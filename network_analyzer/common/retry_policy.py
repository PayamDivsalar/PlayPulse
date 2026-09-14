"""Retry helper with exponential backoff.

A deliberately trimmed counterpart to ``crawler/retry_policy.py``: the analyzer
only needs the stateless ``with_retry`` primitive, because it publishes exactly
one message per pcap file and therefore has no shrinking residual work set to
manage.

The duplication is intentional. Each subsystem is a self-contained deploy unit
with its own image and ``requirements.txt``, and the crawler's source tree is
not present in the analyzer image. Importing across subsystems would couple the
two build contexts for the sake of forty lines.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from functools import wraps
from time import sleep
from typing import ParamSpec, TypeVar

import requests

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
ExceptionTypes = type[BaseException] | tuple[type[BaseException], ...]

# Default: only retry on network-level problems (timeouts, connection errors).
# Anything else (bad data, domain errors, programming bugs) should NOT be
# silently retried.
DEFAULT_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    requests.exceptions.RequestException,
)


def backoff_delay_seconds(attempt: int, base_delay_seconds: float) -> float:
    """Exponential backoff delay for zero-based ``attempt`` index."""

    return base_delay_seconds * (2**attempt)


def with_retry(
    *,
    max_retries: int = 3,
    base_delay_seconds: float = 2.0,
    exceptions: ExceptionTypes = DEFAULT_RETRYABLE_EXCEPTIONS,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Retry a callable with exponential backoff.

    Only exceptions listed in ``exceptions`` trigger a retry. Any other
    exception propagates immediately, since retrying it would just waste time
    on an error that will never succeed (e.g. "application not found").

    Delay sequence: ``base_delay_seconds * (2 ** attempt)``, i.e. 2s, 4s, 8s
    for the defaults.
    """

    if isinstance(exceptions, type):
        exceptions = (exceptions,)

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    if attempt >= max_retries:
                        raise
                    delay = backoff_delay_seconds(attempt, base_delay_seconds)
                    logger.warning(
                        "Retryable error on attempt %s/%s: %s. Retrying in %.2fs.",
                        attempt + 1,
                        max_retries + 1,
                        exc,
                        delay,
                    )
                    sleep(delay)
            raise AssertionError("unreachable")  # pragma: no cover

        return wrapper

    return decorator
