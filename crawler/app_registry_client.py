"""HTTP client for the App API registry."""

from __future__ import annotations

import logging
from typing import Any, Callable

import requests

from crawler.config import Settings
from crawler.retry_policy import with_retry

logger = logging.getLogger(__name__)


class AppRegistryClient:
    """Fetch the active application list from the App API registry."""

    def __init__(self, base_url: str, settings: Settings) -> None:
        self.base_url = base_url.rstrip("/")
        self._timeout_seconds = settings.registry_request_timeout_seconds
        self._retry: Callable = with_retry(
            max_retries=settings.retry_max_attempts,
            base_delay_seconds=settings.retry_base_delay_seconds,
            exceptions=(requests.RequestException,),
        )

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

        def _fetch() -> list[dict[str, Any]]:
            response = requests.get(
                url,
                params=params,
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError(
                    f"Expected a JSON list of applications, got {type(payload).__name__}."
                )
            return payload

        try:
            return self._retry(_fetch)()
        except Exception:
            logger.error("Failed to fetch active applications from %s", url, exc_info=True)
            raise
