"""Play Store client with shared rate limiting and retry support."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable

import google_play_scraper
from google_play_scraper.exceptions import NotFoundError

from crawler.config import Settings
from crawler.rate_limiter import RateLimiter
from crawler.retry_policy import DEFAULT_RETRYABLE_EXCEPTIONS, with_retry

logger = logging.getLogger(__name__)

_RETRYABLE_EXCEPTIONS = DEFAULT_RETRYABLE_EXCEPTIONS

_APP_FIELDS = (
    "minInstalls",
    "score",
    "ratings",
    "reviews",
    "updated",
    "version",
    "adSupported",
)
_REVIEW_FIELDS = (
    "reviewId",
    "at",
    "userName",
    "thumbsUpCount",
    "score",
    "content",
)


def _normalize_review_value(field: str, value: Any) -> Any:
    """Make review field values JSON-serializable for Kafka."""

    if field == "at" and isinstance(value, datetime):
        return value.isoformat()
    return value


class PlayStoreClient:
    """Fetch Play Store app metadata and reviews, respecting a shared rate limit."""

    def __init__(self, rate_limiter: RateLimiter, settings: Settings) -> None:
        self.rate_limiter = rate_limiter
        # Bind retry wrappers at construction time from injected settings
        # (not at import time / per call), so tests can vary attempts per
        # instance without rebuilding decorators on every request.
        retry = with_retry(
            max_retries=settings.retry_max_attempts,
            base_delay_seconds=settings.retry_base_delay_seconds,
            exceptions=_RETRYABLE_EXCEPTIONS,
        )
        self._get_app_details_with_retry: Callable[..., dict[str, Any]] = retry(
            self._get_app_details_once
        )
        self._get_reviews_with_retry: Callable[..., list[dict[str, Any]]] = retry(
            self._get_reviews_once
        )

    def get_app_details(
        self, package_name: str, country: str, lang: str
    ) -> dict[str, Any]:
        """Return a filtered subset of app details for a package.

        Args:
            package_name: Play Store package identifier.
            country: Two-letter Play Store country code (e.g. "us", "ir").
            lang: Two-letter Play Store language code (e.g. "en", "fa").

        Raises:
            NotFoundError: if the package does not exist on the Play Store
                (not retried).
            requests.exceptions.RequestException: on network problems,
                after retries are exhausted.
        """

        return self._get_app_details_with_retry(package_name, country, lang)

    def _get_app_details_once(
        self, package_name: str, country: str, lang: str
    ) -> dict[str, Any]:
        self.rate_limiter.acquire()
        try:
            raw = google_play_scraper.app(
                package_name, country=country, lang=lang
            )
        except NotFoundError:
            logger.warning("Package not found on Play Store: %s", package_name)
            raise
        except _RETRYABLE_EXCEPTIONS:
            logger.error(
                "Network error fetching app details for %s", package_name, exc_info=True
            )
            raise
        return {field: raw.get(field) for field in _APP_FIELDS}

    def get_reviews(
        self,
        package_name: str,
        count: int,
        country: str,
        lang: str,
    ) -> list[dict[str, Any]]:
        """Return the most recent reviews for a package as plain dicts.

        Args:
            package_name: Play Store package identifier.
            count: Exact number of reviews to fetch.
            country: Two-letter Play Store country code (e.g. "us", "ir").
            lang: Two-letter Play Store language code (e.g. "en", "fa").

        Raises:
            NotFoundError: if the package does not exist on the Play Store
                (not retried).
            requests.exceptions.RequestException: on network problems,
                after retries are exhausted.
        """

        return self._get_reviews_with_retry(package_name, count, country, lang)

    def _get_reviews_once(
        self,
        package_name: str,
        count: int,
        country: str,
        lang: str,
    ) -> list[dict[str, Any]]:
        self.rate_limiter.acquire()
        try:
            raw_reviews, _ = google_play_scraper.reviews(
                package_name, count=count, country=country, lang=lang
            )
        except NotFoundError:
            logger.warning("Package not found on Play Store: %s", package_name)
            raise
        except _RETRYABLE_EXCEPTIONS:
            logger.error(
                "Network error fetching reviews for %s", package_name, exc_info=True
            )
            raise
        return [
            {
                field: _normalize_review_value(field, review.get(field))
                for field in _REVIEW_FIELDS
            }
            for review in raw_reviews
        ]
