import asyncio
import csv
import time
from datetime import timedelta

from crawlee import ConcurrencySettings
from crawlee.crawlers import PlaywrightCrawler
from crawlee.request_loaders import RequestList
from crawlee.configuration import Configuration
from crawlee.events import LocalEventManager
from .routes import get_metrics_snapshot, router

async def load_urls_from_csv(file_path: str):  
    """Read URLs from CSV file."""
    def read_csv():
        with open(file_path, 'r') as file:  
            reader = csv.reader(file)  
            next(reader, None)  # Skip header row
            for row in reader:  
                if row:
                    yield row[0]  # URL is in the first column

    # Convert the synchronous generator into an async-friendly stream
    for url in read_csv():
        yield url
        # Yield control back to the event loop to keep the crawler responsive
        await asyncio.sleep(0)

async def main() -> None:
    """The main function."""
    crawl_started_at = time.monotonic()

    config = Configuration(
        available_memory_ratio=0.55,
        headless=True,
    )

    concurrency = ConcurrencySettings(
        max_concurrency=15,
    )

    event_manager = LocalEventManager.from_config(config)

    request_list = RequestList(load_urls_from_csv('./crawler_test/lists/202601.csv'))  # Load URLs from CSV file

    request_manager = await request_list.to_tandem()  # Convert RequestList to RequestManagerTandem

    async def status_message_callback(current, previous, default_message: str) -> str:
        _ = current
        _ = previous
        snapshot = get_metrics_snapshot()
        elapsed = max(time.monotonic() - crawl_started_at, 0.001)
        return (
            f'{default_message} | rpm={snapshot["requests_per_minute"]} '
            f'avg_handler_ms={snapshot["avg_handler_time_ms"]:.1f} '
            f'processed_mb={snapshot["bytes_processed"] / (1024 * 1024):.2f} '
            f'elapsed_s={elapsed:.1f}'
        )
    
    crawler = PlaywrightCrawler(
        configuration=config,
        concurrency_settings=concurrency,
        event_manager=event_manager,
        request_handler=router,
        request_manager=request_manager,  # Use the RequestManagerTandem for managing requests
        browser_type='chromium',
        browser_launch_options={'chromium_sandbox': False},
        navigation_timeout=timedelta(seconds=15),
        request_handler_timeout=timedelta(seconds=45),
        max_request_retries=2,
        max_crawl_depth=2,
        #max_crawl_depth=1, # necessary to limit crawling only to the link(s) fetched from the main URL
        max_requests_per_crawl=1000, # test limit
        use_incognito_pages=False,
        status_message_logging_interval=timedelta(seconds=15),
        status_message_callback=status_message_callback,
    )

    await crawler.run()

