"""Retry helpers with exponential backoff.

Two complementary primitives:

* ``with_retry`` — re-invoke the *same* call when it raises a retryable
  exception (stateless; used by Play Store / app-stats paths).
* ``run_residual_retry`` — re-invoke an attempt that returns a shrinking
  residual work set (stateful; used when only failed items should be retried).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from functools import wraps
from time import sleep
from typing import ParamSpec, TypeVar

import requests

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
T = TypeVar("T")
ExceptionTypes = type[BaseException] | tuple[type[BaseException], ...]

# Default: only retry on network-level problems (timeouts, connection
# errors, ...). Anything else (bad data, 404s the caller raises as domain
# errors, programming bugs) should NOT be silently retried.
DEFAULT_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (
    requests.exceptions.RequestException,
)

ResidualAttempt = Callable[[list[T]], tuple[list[T], BaseException | None]]


def backoff_delay_seconds(attempt: int, base_delay_seconds: float) -> float:
    """Exponential backoff delay for zero-based ``attempt`` index."""

    return base_delay_seconds * (2**attempt)


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

    This helper always re-invokes the callable with the original arguments.
    For shrinking work sets (retry only failures), use ``run_residual_retry``.
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


def run_residual_retry(
    attempt: ResidualAttempt[T],
    items: Sequence[T],
    *,
    max_retries: int = 3,
    base_delay_seconds: float = 2.0,
    description: str = "operation",
) -> None:
    """Retry until ``attempt`` reports an empty residual list.

    ``attempt(pending)`` must return ``(residual, error)``:

    * ``residual`` — items that still need work after this attempt
    * ``error`` — last failure reason for this attempt, or ``None`` when
      ``residual`` is empty

    Unlike ``with_retry``, each attempt may partially succeed; only the
    residual set is passed to the next attempt. If residuals remain after
    ``max_retries`` additional attempts, raises ``error`` from the last
    attempt (or ``RuntimeError`` if no error was provided).
    """

    pending = list(items)
    if not pending:
        return

    last_error: BaseException | None = None

    for attempt_index in range(max_retries + 1):
        pending, error = attempt(pending)
        if error is not None:
            last_error = error
        if not pending:
            return
        if attempt_index >= max_retries:
            break
        delay = backoff_delay_seconds(attempt_index, base_delay_seconds)
        logger.warning(
            "Retryable error on %s (%s pending). Attempt %s/%s failed: %s. "
            "Retrying in %.2fs.",
            description,
            len(pending),
            attempt_index + 1,
            max_retries + 1,
            last_error,
            delay,
        )
        sleep(delay)

    if last_error is not None:
        raise last_error
    raise RuntimeError(
        f"{description} still has {len(pending)} pending item(s) after retries"
    )
