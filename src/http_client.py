"""Shared HTTP helpers: retry logic for sync (requests) and async (httpx) calls."""

import asyncio
import logging
import time

import httpx
import requests

log = logging.getLogger(__name__)

MAX_RETRIES = 3
INITIAL_BACKOFF = 1  # seconds

USER_AGENT = "MLE-Pipeline/1.0 (merchant underwriting take-home)"


def fetch_with_retries(
    url: str,
    *,
    rate_limit_wait: int | None = None,
    **kwargs,
) -> requests.Response:
    """GET *url* with exponential back-off.  Optionally handles 429s."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(url, timeout=10, **kwargs)
            if rate_limit_wait and resp.status_code == 429:
                log.warning("Rate limited (429). Waiting %ds before retrying.", rate_limit_wait)
                time.sleep(rate_limit_wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == MAX_RETRIES - 1:
                raise
            wait = INITIAL_BACKOFF * (2 ** attempt)
            log.warning("Attempt %d failed for %s: %s. Retrying in %ds", attempt + 1, url, e, wait)
            time.sleep(wait)


async def async_fetch_with_retries(url: str, **client_kwargs) -> str:
    """GET *url* asynchronously with exponential back-off. Returns response text."""
    headers = client_kwargs.pop("headers", {})
    headers.setdefault("User-Agent", USER_AGENT)
    async with httpx.AsyncClient(
        timeout=15, follow_redirects=True, headers=headers, **client_kwargs,
    ) as client:
        for attempt in range(MAX_RETRIES):
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                return resp.text
            except httpx.HTTPError as e:
                if attempt == MAX_RETRIES - 1:
                    raise
                wait = INITIAL_BACKOFF * (2 ** attempt)
                log.warning("Attempt %d failed for %s: %s. Retrying in %ds", attempt + 1, url, e, wait)
                await asyncio.sleep(wait)
