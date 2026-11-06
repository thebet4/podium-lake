import logging
import time
from collections.abc import Callable, Iterator

import requests

from podium_lake.config import settings

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 100
MAX_RETRIES = 5
MIN_REQUEST_INTERVAL = 0.3  # seconds between requests (max ~3.3 req/s)

_last_request_at: float | None = None


def _throttle() -> None:
    """Sleep as needed so requests are at least MIN_REQUEST_INTERVAL apart."""
    global _last_request_at
    if _last_request_at is not None:
        wait = MIN_REQUEST_INTERVAL - (time.monotonic() - _last_request_at)
        if wait > 0:
            logger.debug("Throttling: sleeping %.2fs", wait)
            time.sleep(wait)
    _last_request_at = time.monotonic()


def _retry_wait(response: requests.Response, attempt: int) -> tuple[float, str]:
    """Return how long to wait after a 429 and where that value came from."""
    retry_after = response.headers.get("Retry-After")
    if retry_after is not None:
        try:
            return float(retry_after), f"Retry-After={retry_after}"
        except ValueError:
            # Retry-After may also be an HTTP date; fall back to backoff
            logger.debug("Ignoring non-numeric Retry-After: %r", retry_after)
    return float(2**attempt), "exponential backoff"


def get_with_retry(url: str, params: dict | None = None) -> requests.Response:
    """GET a URL, retrying on HTTP 429 (rate limit) with backoff.

    Every HTTP request, including retries, is throttled to at least
    MIN_REQUEST_INTERVAL seconds after the previous one.
    """
    started = time.monotonic()

    for attempt in range(MAX_RETRIES):
        _throttle()
        response = requests.get(url, params=params)

        if response.status_code != 429:
            response.raise_for_status()
            if attempt > 0:
                logger.info(
                    "Recovered after %d rate-limited attempt(s) (%.1fs lost): %s params=%s",
                    attempt, time.monotonic() - started, url, params,
                )
            return response

        wait, source = _retry_wait(response, attempt)
        logger.warning(
            "Rate limited (429) on %s params=%s | attempt %d/%d | waiting %.1fs (%s)",
            url, params, attempt + 1, MAX_RETRIES, wait, source,
        )
        time.sleep(wait)

    logger.error(
        "Giving up after %d rate-limited attempts (%.1fs): %s params=%s",
        MAX_RETRIES, time.monotonic() - started, url, params,
    )
    raise RuntimeError(f"Rate limited after {MAX_RETRIES} attempts: {url}")


def paginate(
    path: str,
    params: dict | None = None,
    limit: int = DEFAULT_LIMIT,
    cached: Callable[[int], dict | None] | None = None,
) -> Iterator[tuple[int, dict, bool]]:
    """Yield `(page_number, page, from_cache)` for each page of an endpoint.

    `path` is relative to the API base url (e.g. "2024/drivers"). Pagination
    is driven by the API's own `limit`/`offset`/`total` fields. `total` counts
    the API's rows, which are not necessarily the items in the page (e.g. for
    `results`, rows are race results nested inside races), so pages are
    yielded untouched and the caller decides how to read them.

    `cached(page_number)` lets the caller supply a page it already has: when
    it returns a page, no request is made for it (and `from_cache` is True).
    The cached page's own `total` still drives when to stop, so resuming a
    partly downloaded endpoint costs no requests for what is already saved.
    """
    url = f"{settings.api_url}/{path}"
    offset = 0
    page_number = 0

    while True:
        mr_data = cached(page_number) if cached else None
        from_cache = mr_data is not None

        if mr_data is None:
            page_params = {**(params or {}), "limit": limit, "offset": offset}
            logger.debug("GET %s params=%s", url, page_params)
            mr_data = get_with_retry(url, page_params).json()["MRData"]

        total = int(mr_data["total"])
        if from_cache:
            logger.debug("Cached %s: rows %d-%d of %d", path, offset, min(offset + limit, total), total)
        else:
            logger.info("Fetched %s: rows %d-%d of %d", path, offset, min(offset + limit, total), total)
        yield page_number, mr_data, from_cache

        offset += limit
        page_number += 1
        # Stop when every row was fetched (or the API returned nothing)
        if offset >= total:
            break
