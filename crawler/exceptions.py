"""Crawler-specific exception types."""

from __future__ import annotations


class CrawlerException(Exception):
    """Base exception for crawler failures."""


class RateLimitExceededException(CrawlerException):
    """Raised when a request is rejected because of rate limiting."""


class CrawlerConfigError(CrawlerException):
    """Raised when a required configuration value is missing."""


class PlayGatewayError(CrawlerException):
    """Play Store response body signaled PlayGatewayError (typically rate limiting)."""


class PlayStoreHTTPError(CrawlerException):
    """Non-retryable Play Store HTTP failure (e.g. 403 Forbidden)."""

    def __init__(self, status_code: int, message: str | None = None) -> None:
        self.status_code = status_code
        super().__init__(message or f"Play Store HTTP {status_code}")
