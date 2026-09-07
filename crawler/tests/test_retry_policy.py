"""Tests for the retry policy."""

from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from crawler.retry_policy import (
    backoff_delay_seconds,
    run_residual_retry,
    with_retry,
)


class RetryPolicyTests(unittest.TestCase):
    def test_backoff_delay_seconds_is_exponential(self) -> None:
        self.assertEqual(backoff_delay_seconds(0, 5.0), 5.0)
        self.assertEqual(backoff_delay_seconds(1, 5.0), 10.0)
        self.assertEqual(backoff_delay_seconds(2, 5.0), 20.0)

    def test_with_retry_returns_result_after_transient_failures(self) -> None:
        class TransientError(Exception):
            pass

        func = Mock(side_effect=[TransientError("boom"), TransientError("boom"), "done"])

        with patch("crawler.retry_policy.sleep", return_value=None):
            wrapped = with_retry(
                max_retries=3,
                base_delay_seconds=2,
                exceptions=(TransientError,),
            )(func)
            result = wrapped()

        self.assertEqual(result, "done")
        self.assertEqual(func.call_count, 3)

    def test_with_retry_reraises_original_exception_after_exhaustion(self) -> None:
        class PermanentError(Exception):
            pass

        func = Mock(side_effect=PermanentError("still failing"))

        with patch("crawler.retry_policy.sleep", return_value=None):
            wrapped = with_retry(
                max_retries=2,
                base_delay_seconds=2,
                exceptions=(PermanentError,),
            )(func)
            with self.assertRaises(PermanentError):
                wrapped()

        self.assertEqual(func.call_count, 3)

    def test_run_residual_retry_noops_on_empty_items(self) -> None:
        attempt = Mock()
        run_residual_retry(attempt, [], max_retries=2, base_delay_seconds=0)
        attempt.assert_not_called()

    def test_run_residual_retry_returns_when_first_attempt_clears_all(self) -> None:
        attempt = Mock(return_value=([], None))

        run_residual_retry(
            attempt,
            ["a", "b"],
            max_retries=2,
            base_delay_seconds=0,
            description="unit-test",
        )

        attempt.assert_called_once_with(["a", "b"])

    def test_run_residual_retry_only_retries_residual_items(self) -> None:
        class TransientError(Exception):
            pass

        seen: list[list[str]] = []

        def attempt(pending: list[str]) -> tuple[list[str], BaseException | None]:
            seen.append(list(pending))
            if pending == ["a", "b", "c"]:
                return ["b", "c"], TransientError("partial")
            if pending == ["b", "c"]:
                return [], None
            raise AssertionError(f"unexpected pending: {pending!r}")

        with patch("crawler.retry_policy.sleep", return_value=None) as sleep_mock:
            run_residual_retry(
                attempt,
                ["a", "b", "c"],
                max_retries=2,
                base_delay_seconds=5.0,
                description="unit-test",
            )

        self.assertEqual(seen, [["a", "b", "c"], ["b", "c"]])
        sleep_mock.assert_called_once_with(5.0)

    def test_run_residual_retry_reraises_last_error_after_exhaustion(self) -> None:
        class PermanentError(Exception):
            pass

        err = PermanentError("still down")

        def attempt(pending: list[str]) -> tuple[list[str], BaseException | None]:
            return pending, err

        with patch("crawler.retry_policy.sleep", return_value=None):
            with self.assertRaises(PermanentError) as ctx:
                run_residual_retry(
                    attempt,
                    ["x"],
                    max_retries=1,
                    base_delay_seconds=0,
                    description="unit-test",
                )

        self.assertIs(ctx.exception, err)
