"""Concurrent fetcher for multiple provider reference URLs."""

import asyncio
import aiohttp
from typing import List, Dict, Any, Optional
import backoff
from ..utils.backoff_logger import get_logger

logger = get_logger(__name__)

class MultiUrlFetcher:
    """Handles concurrent fetching of multiple provider reference URLs."""

    def __init__(self, max_concurrent: int = 10, timeout: int = 30):
        """
        Initialize the fetcher.

        Args:
            max_concurrent: Maximum number of concurrent requests
            timeout: Request timeout in seconds
        """
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.logger = logger
        self._semaphore = asyncio.Semaphore(max_concurrent)

    @backoff.on_exception(
        backoff.expo,
        (aiohttp.ClientError, asyncio.TimeoutError),
        max_tries=3,
        logger=logger
    )
    async def _fetch_url(self, session: aiohttp.ClientSession, url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a single URL with retries and error handling.

        Args:
            session: aiohttp client session
            url: URL to fetch

        Returns:
            Optional[Dict]: JSON response data or None on failure
        """
        try:
            async with self._semaphore:
                async with session.get(url, timeout=self.timeout) as response:
                    response.raise_for_status()
                    return await response.json()
        except Exception as e:
            self.logger.error(f"Error fetching {url}: {str(e)}")
            return None

    async def fetch_urls(self, urls: List[str]) -> Dict[str, Any]:
        """
        Fetch multiple URLs concurrently.

        Args:
            urls: List of URLs to fetch

        Returns:
            Dict[str, Any]: Mapping of URLs to their JSON responses
        """
        results = {}
        timeout = aiohttp.ClientTimeout(total=self.timeout)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [self._fetch_url(session, url) for url in urls]
            responses = await asyncio.gather(*tasks)

            for url, response in zip(urls, responses):
                if response is not None:
                    results[url] = response
                else:
                    self.logger.warning(f"Failed to fetch {url}")

        return results

    def fetch_all(self, urls: List[str]) -> Dict[str, Any]:
        """
        Synchronous wrapper for fetch_urls.

        Args:
            urls: List of URLs to fetch

        Returns:
            Dict[str, Any]: Mapping of URLs to their JSON responses
        """
        return asyncio.run(self.fetch_urls(urls))