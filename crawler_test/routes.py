import hashlib
import re
import time
from collections.abc import Mapping

from crawlee.crawlers import PlaywrightCrawlingContext
from crawlee.router import Router

router = Router[PlaywrightCrawlingContext]()

META_TAG_PATTERN = re.compile(
    r'<meta\s+[^>]*?(?:name|property)=["\']([^"\']+)["\'][^>]*?content=["\']([^"\']*)["\'][^>]*?>',
    re.IGNORECASE,
)

SCRIPT_SRC_PATTERN = re.compile(r'<script\s+[^>]*?src=["\']([^"\']+)["\'][^>]*?>', re.IGNORECASE)

KNOWN_JS_LIBRARIES: dict[str, str] = {
    'jquery': 'jQuery',
    'react': 'React',
    'vue': 'Vue',
    'angular': 'Angular',
    'bootstrap': 'Bootstrap',
    'lodash': 'Lodash',
    'moment': 'Moment.js',
    'require.js': 'RequireJS',
    'requirejs': 'RequireJS',
    'webpack': 'Webpack runtime',
}

CRAWL_METRICS = {
    'started_at': time.monotonic(),
    'requests_seen': 0,
    'requests_ok': 0,
    'requests_failed': 0,
    'bytes_processed': 0,
    'handler_time_ms_total': 0.0,
}


def get_metrics_snapshot() -> Mapping[str, float | int]:
    elapsed = max(time.monotonic() - CRAWL_METRICS['started_at'], 0.001)
    requests_seen = int(CRAWL_METRICS['requests_seen'])
    requests_per_minute = (requests_seen / elapsed) * 60.0

    return {
        'requests_seen': requests_seen,
        'requests_ok': int(CRAWL_METRICS['requests_ok']),
        'requests_failed': int(CRAWL_METRICS['requests_failed']),
        'bytes_processed': int(CRAWL_METRICS['bytes_processed']),
        'avg_handler_time_ms': (
            float(CRAWL_METRICS['handler_time_ms_total']) / requests_seen if requests_seen else 0.0
        ),
        'requests_per_minute': round(requests_per_minute, 2),
    }


def _extract_meta_tags(html: str) -> dict[str, str]:
    meta_tags: dict[str, str] = {}
    for name, content in META_TAG_PATTERN.findall(html):
        key = name.strip().lower()
        if not key:
            continue
        meta_tags[key] = content.strip()
    return meta_tags


def _detect_js_libraries(html: str) -> list[str]:
    libraries: set[str] = set()
    script_sources = ' '.join(SCRIPT_SRC_PATTERN.findall(html)).lower()
    lowered_html = html.lower()

    for signature, label in KNOWN_JS_LIBRARIES.items():
        if signature in script_sources or signature in lowered_html:
            libraries.add(label)

    return sorted(libraries)


@router.default_handler
async def request_handler(context: PlaywrightCrawlingContext) -> None:
    """Default request handler."""
    started = time.monotonic()
    CRAWL_METRICS['requests_seen'] += 1

    context.log.info(f'Processing {context.request.url} ...')

    if context.response is None:
        CRAWL_METRICS['requests_failed'] += 1
        context.log.warning(f'No HTTP response for {context.request.url}, skipping.')
        return

    # Preparing data to be pushed to the dataset
    url = context.request.url
    status = context.response.status
    headers = {
        'content-type': context.response.headers.get('content-type'),
        'content-length': context.response.headers.get('content-length'),
        'server': context.response.headers.get('server'),
    }
    response_body = await context.response.text()
    response_size = len(response_body.encode('utf-8', errors='ignore'))
    meta_tags = _extract_meta_tags(response_body)
    js_libraries = _detect_js_libraries(response_body)

    CRAWL_METRICS['requests_ok'] += 1
    CRAWL_METRICS['bytes_processed'] += response_size
    CRAWL_METRICS['handler_time_ms_total'] += (time.monotonic() - started) * 1000
    
    await context.push_data(
        {
            'url': url,
            'status': status,
            'headers': headers,
            'body_size_bytes': response_size,
            'body_sha1': hashlib.sha1(response_body.encode('utf-8', errors='ignore')).hexdigest(),
            'meta_tags': meta_tags,
            'js_libraries': js_libraries,
        }
    )

    content_type = (headers.get('content-type') or '').lower()
    if status < 400 and 'text/html' in content_type:
        await context.enqueue_links(
            selector='a',
            strategy='same-domain',
        )

    context.log.debug(
        f'Handled {url} status={status} bytes={response_size} '
        f'meta={len(meta_tags)} js_libs={len(js_libraries)}'
    )

