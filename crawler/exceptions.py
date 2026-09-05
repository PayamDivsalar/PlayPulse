"""Crawler-specific exception types."""

from __future__ import annotations


class CrawlerException(Exception):
    """Base exception for crawler failures."""


class RateLimitExceededException(CrawlerException):
    """Raised when a request is rejected because of rate limiting."""


class CrawlerConfigError(CrawlerException):
    """Raised when a required configuration value is missing."""
