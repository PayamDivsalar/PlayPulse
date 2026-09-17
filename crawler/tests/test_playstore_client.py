"""Tests for the Play Store client."""

from __future__ import annotations

import time
import unittest
from datetime import datetime
from numbers import Number
from unittest.mock import Mock, patch
from urllib.error import HTTPError

import pytest
import requests
from google_play_scraper.exceptions import ExtraHTTPError, NotFoundError

from crawler.config import Settings
from crawler.exceptions import PlayGatewayError, PlayStoreHTTPError
from crawler import playstore_client as playstore_module
from crawler.playstore_client import PlayStoreClient, _map_http_error, _post_once
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


def _http_error(code: int) -> HTTPError:
    return HTTPError("https://example.test", code, "err", hdrs=None, fp=None)


class MapHttpErrorTests(unittest.TestCase):
    def test_404_maps_to_not_found(self) -> None:
        mapped = _map_http_error(_http_error(404))
        self.assertIsInstance(mapped, NotFoundError)

    def test_retryable_statuses_map_to_extra_http_error(self) -> None:
        for code in (408, 429, 500, 502, 503, 504):
            with self.subTest(code=code):
                mapped = _map_http_error(_http_error(code))
                self.assertIsInstance(mapped, ExtraHTTPError)

    def test_permanent_client_errors_map_to_play_store_http_error(self) -> None:
        for code in (400, 401, 403):
            with self.subTest(code=code):
                mapped = _map_http_error(_http_error(code))
                self.assertIsInstance(mapped, PlayStoreHTTPError)
                self.assertEqual(mapped.status_code, code)


class PostOnceTests(unittest.TestCase):
    def test_post_once_does_not_retry_on_failure(self) -> None:
        with patch.object(
            playstore_module.gps_request,
            "_urlopen",
            side_effect=TimeoutError("slow"),
        ) as urlopen_mock:
            with self.assertRaises(TimeoutError):
                _post_once(
                    "https://example.test",
                    data=b"body",
                    headers={"content-type": "text/plain"},
                )

        self.assertEqual(urlopen_mock.call_count, 1)

    def test_post_once_raises_play_gateway_error_on_marker(self) -> None:
        with patch.object(
            playstore_module.gps_request,
            "_urlopen",
            return_value=f"prefix {playstore_module._PLAY_GATEWAY_MARKER} suffix",
        ):
            with self.assertRaises(PlayGatewayError):
                _post_once(
                    "https://example.test",
                    data=b"body",
                    headers={"content-type": "text/plain"},
                )

    def test_post_once_returns_body_when_healthy(self) -> None:
        with patch.object(
            playstore_module.gps_request,
            "_urlopen",
            return_value="ok-body",
        ):
            self.assertEqual(
                _post_once(
                    "https://example.test",
                    data=b"body",
                    headers={"content-type": "text/plain"},
                ),
                "ok-body",
            )


class PlayStoreClientTests(unittest.TestCase):
    def _client(
        self,
        rate_limiter: Mock | None = None,
        **settings_overrides: object,
    ) -> tuple[PlayStoreClient, Mock, Settings]:
        limiter = rate_limiter or Mock(spec=RateLimiter)
        settings = Settings.for_testing(**settings_overrides)
        return PlayStoreClient(rate_limiter=limiter, settings=settings), limiter, settings

    def test_get_app_details_returns_only_requested_fields(self) -> None:
        client, rate_limiter, _ = self._client()
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
                result = client.get_app_details(
                    "com.example.app", country="de", lang="tr"
                )

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
        app_mock.assert_called_once_with(
            "com.example.app", country="de", lang="tr"
        )
        rate_limiter.acquire.assert_called_once()

    def test_get_app_details_acquires_rate_limiter_before_library_call(self) -> None:
        rate_limiter = Mock(spec=RateLimiter)
        client, _, _ = self._client(rate_limiter=rate_limiter)

        def app_side_effect(
            package_name: str, *, country: str, lang: str
        ) -> dict[str, object]:
            self.assertTrue(rate_limiter.acquire.called)
            self.assertEqual(package_name, "com.example.app")
            self.assertEqual((country, lang), ("us", "en"))
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
            result = client.get_app_details(
                "com.example.app", country="us", lang="en"
            )

        self.assertEqual(app_mock.call_count, 1)
        self.assertEqual(result["version"], "1")

    def test_get_app_details_retries_network_error_then_reraises(self) -> None:
        client, rate_limiter, settings = self._client(retry_max_attempts=3)

        with patch(
            "google_play_scraper.app",
            side_effect=requests.ConnectionError("boom"),
        ) as app_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(requests.ConnectionError):
                    client.get_app_details(
                        "com.example.app", country="us", lang="en"
                    )

        expected_attempts = settings.retry_max_attempts + 1
        self.assertEqual(app_mock.call_count, expected_attempts)
        self.assertEqual(rate_limiter.acquire.call_count, expected_attempts)

    def test_get_app_details_retries_urllib_timeout_then_reraises(self) -> None:
        """Play Store uses urllib; TimeoutError must be retryable after socket timeout."""

        client, rate_limiter, settings = self._client(retry_max_attempts=2)

        with patch(
            "google_play_scraper.app",
            side_effect=TimeoutError("timed out"),
        ) as app_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(TimeoutError):
                    client.get_app_details(
                        "com.example.app", country="us", lang="en"
                    )

        expected_attempts = settings.retry_max_attempts + 1
        self.assertEqual(app_mock.call_count, expected_attempts)
        self.assertEqual(rate_limiter.acquire.call_count, expected_attempts)

    def test_get_app_details_retries_extra_http_error(self) -> None:
        client, rate_limiter, settings = self._client(retry_max_attempts=2)

        with patch(
            "google_play_scraper.app",
            side_effect=ExtraHTTPError("Retryable Play Store HTTP 429."),
        ) as app_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(ExtraHTTPError):
                    client.get_app_details(
                        "com.example.app", country="us", lang="en"
                    )

        self.assertEqual(app_mock.call_count, settings.retry_max_attempts + 1)
        self.assertEqual(rate_limiter.acquire.call_count, settings.retry_max_attempts + 1)

    def test_get_app_details_does_not_retry_not_found(self) -> None:
        client, rate_limiter, _ = self._client(retry_max_attempts=3)

        with patch(
            "google_play_scraper.app",
            side_effect=NotFoundError("missing"),
        ) as app_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(NotFoundError):
                    client.get_app_details(
                        "com.example.app", country="us", lang="en"
                    )

        self.assertEqual(app_mock.call_count, 1)
        self.assertEqual(rate_limiter.acquire.call_count, 1)

    def test_get_app_details_does_not_retry_permanent_http_error(self) -> None:
        client, rate_limiter, _ = self._client(retry_max_attempts=3)

        with patch(
            "google_play_scraper.app",
            side_effect=PlayStoreHTTPError(403),
        ) as app_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(PlayStoreHTTPError):
                    client.get_app_details(
                        "com.example.app", country="us", lang="en"
                    )

        self.assertEqual(app_mock.call_count, 1)
        self.assertEqual(rate_limiter.acquire.call_count, 1)

    def test_client_configures_scraper_urlopen_timeout(self) -> None:
        self._client(playstore_request_timeout_seconds=12.5)

        self.assertEqual(playstore_module._playstore_urlopen_timeout_seconds, 12.5)
        with patch("crawler.playstore_client.urlopen") as urlopen_mock:
            urlopen_mock.return_value.read.return_value = b"ok"
            playstore_module._urlopen_with_timeout("https://example.test")
            urlopen_mock.assert_called_once_with(
                "https://example.test", timeout=12.5
            )

    def test_urlopen_maps_http_status_before_escaping(self) -> None:
        self._client()
        with patch(
            "crawler.playstore_client.urlopen",
            side_effect=_http_error(429),
        ):
            with self.assertRaises(ExtraHTTPError):
                playstore_module._urlopen_with_timeout("https://example.test")

        with patch(
            "crawler.playstore_client.urlopen",
            side_effect=_http_error(403),
        ):
            with self.assertRaises(PlayStoreHTTPError):
                playstore_module._urlopen_with_timeout("https://example.test")

    def test_get_app_details_does_not_retry_non_network_error(self) -> None:
        client, rate_limiter, _ = self._client()

        class PermanentError(Exception):
            pass

        with patch("google_play_scraper.app", side_effect=PermanentError("boom")) as app_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(PermanentError):
                    client.get_app_details(
                        "com.example.app", country="us", lang="en"
                    )

        self.assertEqual(app_mock.call_count, 1)
        self.assertEqual(rate_limiter.acquire.call_count, 1)

    def test_get_reviews_returns_expected_structure(self) -> None:
        client, rate_limiter, _ = self._client()
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

        with patch(
            "crawler.playstore_client._collect_play_store_reviews",
            return_value=raw_reviews,
        ) as reviews_mock:
            result = client.get_reviews(
                "com.example.app", count=2, country="ir", lang="fa"
            )

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
        reviews_mock.assert_called_once_with(
            "com.example.app", count=2, country="ir", lang="fa"
        )
        rate_limiter.acquire.assert_called_once()

    def test_get_reviews_normalizes_datetime_at_field(self) -> None:
        client, _, _ = self._client()
        raw_reviews = [
            {
                "reviewId": "r1",
                "at": datetime(2021, 3, 25, 15, 52, 53),
                "userName": "Alice",
                "thumbsUpCount": 1,
                "score": 5,
                "content": "ok",
            }
        ]

        with patch(
            "crawler.playstore_client._collect_play_store_reviews",
            return_value=raw_reviews,
        ):
            result = client.get_reviews(
                "com.example.app", count=1, country="us", lang="en"
            )

        self.assertEqual(result[0]["at"], "2021-03-25T15:52:53")

    def test_get_reviews_retries_play_gateway_error(self) -> None:
        client, rate_limiter, settings = self._client(retry_max_attempts=2)

        with patch(
            "crawler.playstore_client._collect_play_store_reviews",
            side_effect=PlayGatewayError("rate limited"),
        ) as collect_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(PlayGatewayError):
                    client.get_reviews(
                        "com.example.app", count=10, country="us", lang="en"
                    )

        self.assertEqual(collect_mock.call_count, settings.retry_max_attempts + 1)
        self.assertEqual(rate_limiter.acquire.call_count, settings.retry_max_attempts + 1)

    def test_get_reviews_retries_extra_http_error_without_library_stacking(self) -> None:
        """Our layer retries once-per-attempt; patched post must not multiply calls."""

        client, rate_limiter, settings = self._client(retry_max_attempts=1)
        urlopen_calls = {"n": 0}

        def failing_urlopen(_obj: object) -> str:
            urlopen_calls["n"] += 1
            raise ExtraHTTPError("Retryable Play Store HTTP 503.")

        with patch.object(playstore_module.gps_request, "_urlopen", side_effect=failing_urlopen):
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(ExtraHTTPError):
                    client.get_reviews(
                        "com.example.app", count=5, country="us", lang="en"
                    )

        # with_retry attempts = max_retries+1; each attempt uses single-shot post.
        self.assertEqual(urlopen_calls["n"], settings.retry_max_attempts + 1)
        self.assertEqual(rate_limiter.acquire.call_count, settings.retry_max_attempts + 1)

    def test_get_reviews_does_not_retry_permanent_http_error(self) -> None:
        client, rate_limiter, _ = self._client(retry_max_attempts=3)

        with patch(
            "crawler.playstore_client._collect_play_store_reviews",
            side_effect=PlayStoreHTTPError(403),
        ) as collect_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(PlayStoreHTTPError):
                    client.get_reviews(
                        "com.example.app", count=5, country="us", lang="en"
                    )

        self.assertEqual(collect_mock.call_count, 1)
        self.assertEqual(rate_limiter.acquire.call_count, 1)

    def test_collect_reviews_propagates_errors_instead_of_swallowing(self) -> None:
        self._client()
        with patch.object(
            playstore_module.gps_reviews,
            "_fetch_review_items",
            side_effect=ExtraHTTPError("Retryable Play Store HTTP 500."),
        ):
            with self.assertRaises(ExtraHTTPError):
                playstore_module._collect_play_store_reviews(
                    "com.example.app",
                    count=5,
                    country="us",
                    lang="en",
                )


@pytest.mark.live
class PlayStoreClientLiveTests(unittest.TestCase):
    """Live Play Store tests. Require internet; do not run in automated CI."""

    def _live_client(self) -> PlayStoreClient:
        settings = Settings.for_testing(
            rate_limit_max_requests=5,
            rate_limit_per_seconds=60,
        )
        return PlayStoreClient(
            rate_limiter=RateLimiter(
                max_requests=settings.rate_limit_max_requests,
                per_seconds=settings.rate_limit_per_seconds,
            ),
            settings=settings,
        )

    def test_live_get_app_details_real_connection(self) -> None:
        """Fetch real WhatsApp details and assert response shape/types only.

        Depends on a live Google Play Store connection. Do not run in CI.
        """

        result = self._live_client().get_app_details(
            "com.whatsapp", country="us", lang="en"
        )

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

        start = time.monotonic()
        with self.assertRaises(NotFoundError):
            self._live_client().get_app_details(
                "this.package.definitely.does.not.exist.xyz123",
                country="us",
                lang="en",
            )
        elapsed = time.monotonic() - start

        self.assertLess(elapsed, 15.0)

    def test_live_get_reviews_real_connection(self) -> None:
        """Fetch a small batch of real reviews and assert list item shape.

        Depends on a live Google Play Store connection. Do not run in CI.
        """

        reviews = self._live_client().get_reviews(
            "com.whatsapp", count=5, country="us", lang="en"
        )

        self.assertIsInstance(reviews, list)
        self.assertGreater(len(reviews), 0)
        for review in reviews:
            for key in _REVIEW_KEYS:
                self.assertIn(key, review)
