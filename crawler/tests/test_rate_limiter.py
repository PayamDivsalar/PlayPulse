"""Tests for the crawler rate limiter."""

from __future__ import annotations

import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from unittest.mock import patch

from crawler.rate_limiter import RateLimiter


class RateLimiterTests(unittest.TestCase):
    def test_acquire_third_call_waits_for_capacity(self) -> None:
        limiter = RateLimiter(max_requests=2, per_seconds=1)

        with patch("crawler.rate_limiter.uniform", return_value=0.0):
            start = time.monotonic()
            limiter.acquire()
            limiter.acquire()
            limiter.acquire()
            elapsed = time.monotonic() - start

        self.assertGreaterEqual(elapsed, 0.45)
        self.assertLess(elapsed, 2.0)

    def test_acquire_is_thread_safe_under_contention(self) -> None:
        limiter = RateLimiter(max_requests=2, per_seconds=0.5)
        barrier = Barrier(4)
        completion_times: list[float] = []

        def worker() -> None:
            barrier.wait()
            limiter.acquire()
            completion_times.append(time.monotonic())

        with patch("crawler.rate_limiter.uniform", return_value=0.0):
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(worker) for _ in range(4)]
                for future in futures:
                    future.result()

        completion_times.sort()
        self.assertEqual(len(completion_times), 4)
        self.assertGreaterEqual(completion_times[2] - completion_times[0], 0.20)
