"""Tests for the Play Store client."""

from __future__ import annotations

import time
import unittest
from numbers import Number
from unittest.mock import Mock, patch

import pytest
import requests
from google_play_scraper.exceptions import NotFoundError

from crawler.playstore_client import PlayStoreClient
from crawler.rate_limiter import RateLimiter

_APP_DETAIL_KEYS = (
    "minInstalls",
    "score",
    "ratings",
    "reviews",
    "updated",
    "version",
    "adSupported",
)
_REVIEW_KEYS = (
    "reviewId",
    "at",
    "userName",
    "thumbsUpCount",
    "score",
    "content",
)


class PlayStoreClientTests(unittest.TestCase):
    def test_get_app_details_returns_only_requested_fields(self) -> None:
        rate_limiter = Mock(spec=RateLimiter)
        client = PlayStoreClient(rate_limiter=rate_limiter)
        raw_response = {
            "minInstalls": 123,
            "score": 4.5,
            "ratings": 999,
            "reviews": 456,
            "updated": 1700000000,
            "version": "1.2.3",
            "adSupported": True,
            "extraField": "ignore-me",
        }

        with patch("google_play_scraper.app", return_value=raw_response) as app_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                result = client.get_app_details("com.example.app")

        self.assertEqual(
            result,
            {
                "minInstalls": 123,
                "score": 4.5,
                "ratings": 999,
                "reviews": 456,
                "updated": 1700000000,
                "version": "1.2.3",
                "adSupported": True,
            },
        )
        self.assertEqual(app_mock.call_count, 1)
        rate_limiter.acquire.assert_called_once()

    def test_get_app_details_acquires_rate_limiter_before_library_call(self) -> None:
        rate_limiter = Mock(spec=RateLimiter)
        client = PlayStoreClient(rate_limiter=rate_limiter)

        def app_side_effect(package_name: str) -> dict[str, object]:
            self.assertTrue(rate_limiter.acquire.called)
            self.assertEqual(package_name, "com.example.app")
            return {
                "minInstalls": 1,
                "score": 1.0,
                "ratings": 1,
                "reviews": 1,
                "updated": 1,
                "version": "1",
                "adSupported": False,
            }

        with patch("google_play_scraper.app", side_effect=app_side_effect) as app_mock:
            result = client.get_app_details("com.example.app")

        self.assertEqual(app_mock.call_count, 1)
        self.assertEqual(result["version"], "1")

    def test_get_app_details_retries_network_error_then_reraises(self) -> None:
        rate_limiter = Mock(spec=RateLimiter)
        client = PlayStoreClient(rate_limiter=rate_limiter)

        # Only network errors are retried; the call is attempted 1 + 3 times
        # before the exception propagates.
        with patch(
            "google_play_scraper.app",
            side_effect=requests.ConnectionError("boom"),
        ) as app_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(requests.ConnectionError):
                    client.get_app_details("com.example.app")

        self.assertEqual(app_mock.call_count, 4)
        self.assertEqual(rate_limiter.acquire.call_count, 4)

    def test_get_app_details_does_not_retry_non_network_error(self) -> None:
        rate_limiter = Mock(spec=RateLimiter)
        client = PlayStoreClient(rate_limiter=rate_limiter)

        class PermanentError(Exception):
            pass

        # A non-network error must propagate immediately without retrying,
        # since retrying it would never succeed.
        with patch("google_play_scraper.app", side_effect=PermanentError("boom")) as app_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(PermanentError):
                    client.get_app_details("com.example.app")

        self.assertEqual(app_mock.call_count, 1)
        self.assertEqual(rate_limiter.acquire.call_count, 1)

    def test_get_reviews_returns_expected_structure(self) -> None:
        rate_limiter = Mock(spec=RateLimiter)
        client = PlayStoreClient(rate_limiter=rate_limiter)
        raw_reviews = [
            {
                "reviewId": "r1",
                "at": "2026-09-04T10:00:00Z",
                "userName": "Alice",
                "thumbsUpCount": 3,
                "score": 5,
                "content": "Great app",
                "ignored": "x",
            },
            {
                "reviewId": "r2",
                "at": "2026-09-04T11:00:00Z",
                "userName": "Bob",
                "thumbsUpCount": 1,
                "score": 4,
                "content": "Nice",
                "ignored": "y",
            },
        ]

        with patch("google_play_scraper.reviews", return_value=(raw_reviews, None)) as reviews_mock:
            result = client.get_reviews("com.example.app", count=2)

        self.assertEqual(
            result,
            [
                {
                    "reviewId": "r1",
                    "at": "2026-09-04T10:00:00Z",
                    "userName": "Alice",
                    "thumbsUpCount": 3,
                    "score": 5,
                    "content": "Great app",
                },
                {
                    "reviewId": "r2",
                    "at": "2026-09-04T11:00:00Z",
                    "userName": "Bob",
                    "thumbsUpCount": 1,
                    "score": 4,
                    "content": "Nice",
                },
            ],
        )
        reviews_mock.assert_called_once_with("com.example.app", count=2)
        rate_limiter.acquire.assert_called_once()


@pytest.mark.live
class PlayStoreClientLiveTests(unittest.TestCase):
    """Live Play Store tests. Require internet; do not run in automated CI."""

    def test_live_get_app_details_real_connection(self) -> None:
        """Fetch real WhatsApp details and assert response shape/types only.

        Depends on a live Google Play Store connection. Do not run in CI.
        """

        client = PlayStoreClient(
            rate_limiter=RateLimiter(max_requests=5, per_seconds=60)
        )
        result = client.get_app_details("com.whatsapp")

        for key in _APP_DETAIL_KEYS:
            self.assertIn(key, result)

        self.assertTrue(
            result["minInstalls"] is None or isinstance(result["minInstalls"], int)
        )
        self.assertTrue(
            result["score"] is None or isinstance(result["score"], Number)
        )
        self.assertFalse(isinstance(result["score"], bool))
        self.assertTrue(
            result["ratings"] is None or isinstance(result["ratings"], int)
        )
        self.assertTrue(
            result["reviews"] is None or isinstance(result["reviews"], int)
        )
        self.assertTrue(
            result["updated"] is None or isinstance(result["updated"], int)
        )
        self.assertTrue(
            result["version"] is None or isinstance(result["version"], str)
        )
        self.assertTrue(
            result["adSupported"] is None or isinstance(result["adSupported"], bool)
        )

    def test_live_get_app_details_not_found(self) -> None:
        """NotFoundError must raise immediately for a non-existent package.

        Depends on a live Google Play Store connection. Do not run in CI.
        Retries must not apply (NotFoundError is non-retryable).
        """

        client = PlayStoreClient(
            rate_limiter=RateLimiter(max_requests=5, per_seconds=60)
        )
        start = time.monotonic()
        with self.assertRaises(NotFoundError):
            client.get_app_details("this.package.definitely.does.not.exist.xyz123")
        elapsed = time.monotonic() - start

        # One network round-trip only; no exponential backoff (2s+4s+8s).
        self.assertLess(elapsed, 15.0)

    def test_live_get_reviews_real_connection(self) -> None:
        """Fetch a small batch of real reviews and assert list item shape.

        Depends on a live Google Play Store connection. Do not run in CI.
        """

        client = PlayStoreClient(
            rate_limiter=RateLimiter(max_requests=5, per_seconds=60)
        )
        reviews = client.get_reviews("com.whatsapp", count=5)

        self.assertIsInstance(reviews, list)
        self.assertGreater(len(reviews), 0)
        for review in reviews:
            for key in _REVIEW_KEYS:
                self.assertIn(key, review)
