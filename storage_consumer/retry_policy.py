"""Retry helper with exponential backoff, plus this subsystem's transient set.

Deliberately duplicated from ``crawler/retry_policy.py`` and
``network_analyzer/retry_policy.py``: each subsystem is a self-contained deploy
unit whose image does not contain the others' source.

What *is* different here is the retryable set. The other two retry HTTP; this
one retries the database, and getting that set wrong is how a consumer loses
data. Failures split three ways:

* **Transient** — the exceptions below. Retry the batch, never dead-letter it.
  Dead-lettering an ``OperationalError`` would discard perfectly good rows
  because Postgres happened to be restarting.
* **Permanent, per message** — ``MessageDecodeError``, and an unregistered
  ``package_name``. Never retried; they go to ``dead_letter_events`` and the
  offset advances.
* **Fatal** — everything else. The process exits non-zero and restarts from
  its last committed offset.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from functools import wraps
from time import sleep
from typing import ParamSpec, TypeVar

import psycopg2
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")
ExceptionTypes = type[BaseException] | tuple[type[BaseException], ...]

# Retrying these is always right, and dead-lettering them is always wrong.
#
# ``OperationalError`` also covers the two serialization faults psycopg2 raises
# as its subclasses -- ``DeadlockDetected`` and ``SerializationFailure`` -- both
# of which succeed on a second attempt by definition. ``InterfaceError`` means
# the connection object itself is unusable, which is why the batch retry path
# reconnects before trying again.
TRANSIENT_DB_EXCEPTIONS: tuple[type[BaseException], ...] = (
    psycopg2.OperationalError,
    psycopg2.InterfaceError,
)

# Offset commits. A failed commit is not data loss -- the rows are already
# durable -- but it does mean the batch will be redelivered, so it is worth a
# few attempts before accepting the duplicate work.
TRANSIENT_KAFKA_EXCEPTIONS: tuple[type[BaseException], ...] = (KafkaError,)


def backoff_delay_seconds(attempt: int, base_delay_seconds: float) -> float:
    """Exponential backoff delay for zero-based ``attempt`` index."""

    return base_delay_seconds * (2**attempt)


def with_retry(
    *,
    max_retries: int = 3,
    base_delay_seconds: float = 1.0,
    exceptions: ExceptionTypes = TRANSIENT_DB_EXCEPTIONS,
    on_retry: Callable[[BaseException], None] | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Retry a callable with exponential backoff.

    Only exceptions listed in ``exceptions`` trigger a retry. Any other
    exception propagates immediately, since retrying it would just waste time
    on an error that will never succeed.

    Delay sequence: ``base_delay_seconds * (2 ** attempt)``, i.e. 1s, 2s, 4s
    for the defaults.

    ``on_retry`` runs before each sleep and receives the exception that caused
    it. The batch write path uses it to drop and re-open the database
    connection: after the server has gone away, every remaining attempt on the
    old connection would fail instantly with ``InterfaceError`` and burn the
    whole retry budget in microseconds.
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
                    if on_retry is not None:
                        on_retry(exc)
                    sleep(delay)
            raise AssertionError("unreachable")  # pragma: no cover

        return wrapper

    return decorator
