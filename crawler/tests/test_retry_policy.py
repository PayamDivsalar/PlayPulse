"""Tests for the retry policy."""

from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from crawler.retry_policy import with_retry


class RetryPolicyTests(unittest.TestCase):
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
