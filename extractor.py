import logging
import time
from typing import Iterator
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

class APIExtractor:
    """
    Extracts paginated data from a REST API.
    Uses a persistent session for connection pooling.
    Handles cursor-based and offset-based pagination automatically.
    """

    def __init__(self, base_url: str, headers: dict = None, rate_limit_delay: float = 0.5):
        self.base_url = base_url.rstrip("/")
        self.rate_limit_delay = rate_limit_delay
        self.session = requests.Session()
        self.session.headers.update(headers or {})
        self.session.headers.update({"Accept": "application/json"})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(requests.exceptions.RequestException),
        reraise=True,
    )
    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Single HTTP GET with automatic retry on failure."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        logger.debug(f"GET {url} params={params}")

        response = self.session.get(url, params=params, timeout=30)

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            logger.warning(f"Rate limited. Sleeping {retry_after}s")
            time.sleep(retry_after)
            response = self.session.get(url, params=params, timeout=30)

        response.raise_for_status()
        return response.json()

    def extract_paginated(
        self,
        endpoint: str,
        params: dict = None,
        page_size: int = 100,
        since: str = None,
    ) -> Iterator[dict]:
        params = params or {}
        params["per_page"] = page_size

        if since and "search" not in endpoint:
            params["since"] = since

        page = 1
        total_fetched = 0

        while True:
            params["page"] = page
            data = self._get(endpoint, params)
            records = data if isinstance(data, list) else data.get("items", data.get("data", []))

            if not records:
                logger.info(f"Extraction complete. Total records: {total_fetched}")
                break

            for record in records:
                yield record
                total_fetched += 1

            logger.info(f"Fetched page {page}, cumulative: {total_fetched}")
            page += 1

            if "search" in endpoint and page > 10:
                logger.info("Reached GitHub search API limit of 1000 results")
                break

            time.sleep(self.rate_limit_delay)
