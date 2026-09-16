"""Exception types for the sentiment subsystem."""

from __future__ import annotations


class SentimentError(Exception):
    """Base exception for sentiment subsystem failures."""


class ConfigError(SentimentError):
    """Raised when a required configuration value is missing or invalid."""


class ClassificationError(SentimentError):
    """Raised when the model returns a label that cannot be mapped."""
