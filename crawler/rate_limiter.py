"""Thread-safe request rate limiter."""

from __future__ import annotations

from random import uniform
from threading import Lock
from time import monotonic, sleep


class RateLimiter:
    """Simple token-bucket rate limiter shared across threads."""

    def __init__(self, max_requests: int, per_seconds: float) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests must be greater than zero")
        if per_seconds <= 0:
            raise ValueError("per_seconds must be greater than zero")

        self._max_requests = float(max_requests)
        self._per_seconds = float(per_seconds)
        self._tokens = float(max_requests)
        self._refill_rate = self._max_requests / self._per_seconds
        self._last_refill = monotonic()
        self._lock = Lock()

    def acquire(self) -> None:
        """Block until a request token is available.

        Fractional tokens must be preserved while waiting. Zeroing them (and
        letting every waiter refresh ``_last_refill``) lets concurrent workers
        starve each other: the bucket never reaches a full token again after
        the initial burst is spent.
        """

        while True:
            sleep_seconds = 0.0
            with self._lock:
                now = monotonic()
                elapsed = now - self._last_refill
                if elapsed > 0:
                    self._tokens = min(
                        self._max_requests,
                        self._tokens + elapsed * self._refill_rate,
                    )
                    self._last_refill = now

                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return

                missing_tokens = 1.0 - self._tokens
                sleep_seconds = missing_tokens / self._refill_rate

            jitter = uniform(0.0, 0.5)
            sleep(sleep_seconds + jitter)
