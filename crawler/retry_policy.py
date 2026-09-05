"""Retry helpers with exponential backoff."""

from __future__ import annotations

import logging
from functools import wraps
from time import sleep
from typing import Callable, ParamSpec, TypeVar

import requests

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
ExceptionTypes = type[BaseException] | tuple[type[BaseException], ...]

# Default: only retry on network-level problems (timeouts, connection
# errors, ...). Anything else (bad data, 404s the caller raises as domain
# errors, programming bugs) should NOT be silently retried.
DEFAULT_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    requests.exceptions.RequestException,
)


def with_retry(
    *,
    max_retries: int = 3,
    base_delay_seconds: float = 2,
    exceptions: ExceptionTypes = DEFAULT_RETRYABLE_EXCEPTIONS,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Retry a callable with exponential backoff.

    Only exceptions listed in `exceptions` trigger a retry. Any other
    exception propagates immediately, since retrying it would just waste
    time on an error that will never succeed (e.g. "app not found").

    Delay sequence: base_delay_seconds * (2 ** attempt), i.e. 2s, 4s, 8s
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
                    delay = base_delay_seconds * (2**attempt)
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