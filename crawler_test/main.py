import asyncio
import csv
from crawlee.crawlers import PlaywrightCrawler
from crawlee.request_loaders import RequestList
from crawlee.configuration import Configuration
from .routes import router

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
    config = Configuration(
        available_memory_ratio=0.75,
        disable_browser_sandbox=True,
        headless=True,
    )

    request_list = RequestList(load_urls_from_csv('./crawler_test/lists/202601.csv'))  # Load URLs from CSV file

    request_manager = await request_list.to_tandem()  # Convert RequestList to RequestManagerTandem
    
    crawler = PlaywrightCrawler(
        configuration=config,
        request_handler=router,
        request_manager=request_manager,  # Use the RequestManagerTandem for managing requests
        browser_type='chromium',
        max_crawl_depth=1, # necessary to limit crawling only to the link(s) fetched from the main URL
        max_requests_per_crawl=1000, # test limit
        use_incognito_pages=True,
    )

    await crawler.run()

