import csv
from datetime import timedelta
from crawlee.browsers import BrowserPool
from crawlee.crawlers import PlaywrightCrawler
from crawlee.http_clients import ImpitHttpClient
from crawlee.request_loaders import RequestList, RequestManagerTandem
from .routes import router

async def load_urls_from_csv(file_path: str) -> list[str]:  
    """Read URLs from CSV file."""  
    urls = []  
    with open(file_path, 'r') as file:  
        reader = csv.reader(file)  
        next(reader)  # Skip header row
        for row in reader:  
            if row and row[0].strip():  # Skip empty rows  
                urls.append(row[0].strip())  # In the dataset, URL is in first column, so take only that element  
    return urls

async def main() -> None:
    """The main function."""
    urls = await load_urls_from_csv('./crawler_test/lists/202601.csv')  # Load URLs from CSV file
    request_list = RequestList(urls)

    tandem = await RequestList.to_tandem(request_list)  # Convert RequestList to RequestManagerTandem
    
    # In containerized runs, Chromium sandboxing and /dev/shm limits can crash the browser process.
    # These launch options make Playwright startup more reliable in Docker.
    browser_pool = BrowserPool.with_default_plugin(
        operation_timeout=timedelta(seconds=180),
        browser_launch_options={
            'chromium_sandbox': False,
        },
    )

    """The crawler entry point."""
    crawler = PlaywrightCrawler(
        browser_pool=browser_pool,
        request_handler=router,
        request_manager=tandem,  # Use the RequestManagerTandem for managing requests
        headless=True,
        max_crawl_depth=1, # necessary to limit crawling only to the link(s) fetched from the main URL
        max_requests_per_crawl=1000, # test limit
        http_client=ImpitHttpClient(),
    )

    await crawler.run()

