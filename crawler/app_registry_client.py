"""HTTP client for the App API registry."""

from __future__ import annotations

import logging
from typing import Any

import requests

from crawler import config
from crawler.retry_policy import with_retry

logger = logging.getLogger(__name__)

_REQUEST_TIMEOUT_SECONDS = 10


class AppRegistryClient:
    """Fetch the active application list from the App API registry."""

    def __init__(self, base_url: str | None = None) -> None:
        self.base_url = (base_url or config.get_app_api_base_url()).rstrip("/")

    def get_active_applications(self) -> list[dict[str, Any]]:
        """Return all active applications from the registry API.

        A failed request must never be interpreted as "no active applications":
        the error is logged and re-raised so the caller can skip the cycle
        instead of silently crawling nothing.

        Raises:
            requests.RequestException: on network problems, after retries are
                exhausted.
            ValueError: if the response body is not a JSON list.
        """

        url = f"{self.base_url}/api/applications/"
        params = {"is_active": "true"}

        @with_retry(exceptions=(requests.RequestException,))
        def _fetch() -> list[dict[str, Any]]:
            response = requests.get(url, params=params, timeout=_REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError(
                    f"Expected a JSON list of applications, got {type(payload).__name__}."
                )
            return payload

        try:
            return _fetch()
        except Exception:
            logger.error("Failed to fetch active applications from %s", url, exc_info=True)
            raise
