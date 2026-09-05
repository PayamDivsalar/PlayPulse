"""Play Store client with shared rate limiting and retry support."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import google_play_scraper
from google_play_scraper.exceptions import NotFoundError

from crawler.rate_limiter import RateLimiter
from crawler.retry_policy import DEFAULT_RETRYABLE_EXCEPTIONS, with_retry

logger = logging.getLogger(__name__)

# NotFoundError means the package simply doesn't exist on the Play Store.
# Retrying it would never succeed, so it must never be treated as a
# transient/retryable error.
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

    def __init__(self, rate_limiter: RateLimiter) -> None:
        self.rate_limiter = rate_limiter

    @with_retry(max_retries=3, base_delay_seconds=2, exceptions=_RETRYABLE_EXCEPTIONS)
    def get_app_details(self, package_name: str) -> dict[str, Any]:
        """Return a filtered subset of app details for a package.

        Raises:
            NotFoundError: if the package does not exist on the Play Store
                (not retried).
            requests.exceptions.RequestException: on network problems,
                after retries are exhausted.
        """
        self.rate_limiter.acquire()
        try:
            raw = google_play_scraper.app(package_name)
        except NotFoundError:
            logger.warning("Package not found on Play Store: %s", package_name)
            raise
        except _RETRYABLE_EXCEPTIONS:
            logger.error(
                "Network error fetching app details for %s", package_name, exc_info=True
            )
            raise
        return {field: raw.get(field) for field in _APP_FIELDS}

    @with_retry(max_retries=3, base_delay_seconds=2, exceptions=_RETRYABLE_EXCEPTIONS)
    def get_reviews(self, package_name: str, count: int = 1000) -> list[dict[str, Any]]:
        """Return the most recent reviews for a package as plain dicts.

        Raises:
            NotFoundError: if the package does not exist on the Play Store
                (not retried).
            requests.exceptions.RequestException: on network problems,
                after retries are exhausted.
        """
        self.rate_limiter.acquire()
        try:
            raw_reviews, _ = google_play_scraper.reviews(package_name, count=count)
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
