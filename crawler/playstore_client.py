"""Play Store client with shared rate limiting and retry support."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable, Union
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import google_play_scraper
import google_play_scraper.features.reviews as gps_reviews
import google_play_scraper.utils.request as gps_request
from google_play_scraper import Sort
from google_play_scraper.constants.element import ElementSpecs
from google_play_scraper.constants.request import Formats
from google_play_scraper.exceptions import ExtraHTTPError, NotFoundError

from crawler.config import Settings
from crawler.exceptions import PlayGatewayError, PlayStoreHTTPError
from crawler.rate_limiter import RateLimiter
from crawler.retry_policy import DEFAULT_RETRYABLE_EXCEPTIONS, with_retry

logger = logging.getLogger(__name__)

# Transient failures only. Permanent HTTP (403/400/…) becomes PlayStoreHTTPError
# and must NOT appear here. NotFoundError is intentionally excluded.
_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    *DEFAULT_RETRYABLE_EXCEPTIONS,
    URLError,
    TimeoutError,
    ExtraHTTPError,
    PlayGatewayError,
)

# Status codes that are safe to retry with exponential backoff.
_RETRYABLE_HTTP_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})

_PLAY_GATEWAY_MARKER = "com.google.play.gateway.proto.PlayGatewayError"

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

# Mutable holder so PlayStoreClient can set timeout without process-wide
# socket.setdefaulttimeout (which would also affect Kafka / other I/O).
_playstore_urlopen_timeout_seconds: float = 60.0
_scraper_request_patches_installed: bool = False


def _map_http_error(error: HTTPError) -> BaseException:
    """Convert urllib HTTPError into a domain error with clear retry semantics."""

    if error.code == 404:
        return NotFoundError("App not found(404).")
    if error.code in _RETRYABLE_HTTP_STATUS_CODES:
        return ExtraHTTPError(
            f"Retryable Play Store HTTP {error.code}."
        )
    return PlayStoreHTTPError(error.code)


def _urlopen_with_timeout(obj: Any) -> str:
    """Drop-in for google_play_scraper.utils.request._urlopen with a read timeout."""

    try:
        resp = urlopen(obj, timeout=_playstore_urlopen_timeout_seconds)
    except HTTPError as e:
        raise _map_http_error(e) from e

    return resp.read().decode("UTF-8")


def _post_once(
    url: str, data: Union[str, bytes], headers: dict[str, str]
) -> str:
    """Single-attempt POST for reviews.

    Replaces google_play_scraper's ``post()``, which retries any Exception up to
    three times with almost no backoff. Owning retries in ``with_retry`` avoids
    stacked attempts (3 × N) and applies one exponential policy for both app
    details and reviews — including PlayGatewayError.
    """

    body = gps_request._urlopen(Request(url, data=data, headers=headers))
    if _PLAY_GATEWAY_MARKER in body:
        raise PlayGatewayError(
            "Play Store gateway error (typically rate limiting)."
        )
    return body


def _collect_play_store_reviews(
    package_name: str,
    *,
    count: int,
    country: str,
    lang: str,
) -> list[dict[str, Any]]:
    """Fetch reviews like google_play_scraper.reviews, but never swallow errors.

    The stock ``reviews()`` catches ``Exception`` and returns partial/empty
    results, which would skip our ``with_retry`` layer entirely.
    """

    sort = Sort.NEWEST.value
    url = Formats.Reviews.build(lang=lang, country=country)
    token: str | None = None
    result: list[dict[str, Any]] = []
    remaining = count

    while remaining > 0:
        fetch_count = min(remaining, gps_reviews.MAX_COUNT_EACH_FETCH)
        review_items, token = gps_reviews._fetch_review_items(
            url,
            package_name,
            sort,
            fetch_count,
            None,
            None,
            token,
        )
        for review in review_items:
            result.append(
                {
                    key: spec.extract_content(review)
                    for key, spec in ElementSpecs.Review.items()
                }
            )
        remaining = count - len(result)
        if isinstance(token, list) or token is None:
            break

    return result


def _ensure_scraper_request_patches(timeout_seconds: float) -> None:
    """Install timed urlopen + single-shot post into google_play_scraper."""

    global _playstore_urlopen_timeout_seconds, _scraper_request_patches_installed
    _playstore_urlopen_timeout_seconds = timeout_seconds
    if _scraper_request_patches_installed:
        return

    gps_request._urlopen = _urlopen_with_timeout  # noqa: SLF001
    gps_request.post = _post_once
    # features.reviews imported ``post`` by name at import time — rebind it.
    gps_reviews.post = _post_once
    _scraper_request_patches_installed = True


def _normalize_review_value(field: str, value: Any) -> Any:
    """Make review field values JSON-serializable for Kafka."""

    if field == "at" and isinstance(value, datetime):
        return value.isoformat()
    return value


class PlayStoreClient:
    """Fetch Play Store app metadata and reviews, respecting a shared rate limit."""

    def __init__(self, rate_limiter: RateLimiter, settings: Settings) -> None:
        self.rate_limiter = rate_limiter
        _ensure_scraper_request_patches(settings.playstore_request_timeout_seconds)
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
            NotFoundError: package missing (not retried).
            PlayStoreHTTPError: non-retryable HTTP status (not retried).
            ExtraHTTPError / PlayGatewayError / URLError / TimeoutError:
                transient failures after retries are exhausted.
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
        except PlayStoreHTTPError:
            logger.error(
                "Non-retryable Play Store HTTP error for app details "
                "package_name=%s",
                package_name,
                exc_info=True,
            )
            raise
        except _RETRYABLE_EXCEPTIONS:
            logger.error(
                "Transient error fetching app details for %s",
                package_name,
                exc_info=True,
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
            NotFoundError: package missing (not retried).
            PlayStoreHTTPError: non-retryable HTTP status (not retried).
            ExtraHTTPError / PlayGatewayError / URLError / TimeoutError:
                transient failures after retries are exhausted.
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
            raw_reviews = _collect_play_store_reviews(
                package_name,
                count=count,
                country=country,
                lang=lang,
            )
        except NotFoundError:
            logger.warning("Package not found on Play Store: %s", package_name)
            raise
        except PlayStoreHTTPError:
            logger.error(
                "Non-retryable Play Store HTTP error for reviews "
                "package_name=%s",
                package_name,
                exc_info=True,
            )
            raise
        except _RETRYABLE_EXCEPTIONS:
            logger.error(
                "Transient error fetching reviews for %s",
                package_name,
                exc_info=True,
            )
            raise
        return [
            {
                field: _normalize_review_value(field, review.get(field))
                for field in _REVIEW_FIELDS
            }
            for review in raw_reviews
        ]
