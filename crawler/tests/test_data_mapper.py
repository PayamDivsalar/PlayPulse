"""Tests for Play Store → Kafka/DB data mapping."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from crawler.data_mapper import map_app_details, map_review

_APP_STATS_KEYS = (
    "package_name",
    "min_installs",
    "score",
    "ratings",
    "reviews_count",
    "version",
    "ad_supported",
    "app_updated_at",
    "crawled_at",
)

_REVIEW_KEYS = (
    "package_name",
    "review_id",
    "user_name",
    "thumbs_up_count",
    "score",
    "content",
    "at",
    "crawled_at",
)


class MapAppDetailsTests(unittest.TestCase):
    def test_map_app_details_produces_expected_keys(self) -> None:
        raw = {
            "minInstalls": 1_000_000,
            "score": 4.5,
            "ratings": 999,
            "reviews": 456,
            "updated": 1_700_000_000,
            "version": "1.2.3",
            "adSupported": True,
        }

        result = map_app_details(raw, "com.example.app")

        self.assertEqual(set(result.keys()), set(_APP_STATS_KEYS))
        self.assertEqual(result["package_name"], "com.example.app")
        self.assertEqual(result["min_installs"], 1_000_000)
        self.assertEqual(result["score"], 4.5)
        self.assertEqual(result["ratings"], 999)
        self.assertEqual(result["reviews_count"], 456)
        self.assertEqual(result["version"], "1.2.3")
        self.assertEqual(result["ad_supported"], True)

    def test_map_app_details_converts_unix_timestamp_to_iso(self) -> None:
        raw = {"updated": 1_700_000_000}
        result = map_app_details(raw, "com.example.app")

        expected = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc).isoformat()
        self.assertEqual(result["app_updated_at"], expected)

    def test_map_app_details_invalid_updated_becomes_none(self) -> None:
        self.assertIsNone(
            map_app_details({"updated": None}, "com.example.app")["app_updated_at"]
        )
        self.assertIsNone(
            map_app_details({"updated": "not-a-timestamp"}, "com.example.app")[
                "app_updated_at"
            ]
        )
        self.assertIsNone(map_app_details({}, "com.example.app")["app_updated_at"])

    def test_map_app_details_crawled_at_is_recent_iso(self) -> None:
        before = datetime.now(timezone.utc) - timedelta(seconds=2)
        result = map_app_details({}, "com.example.app")
        after = datetime.now(timezone.utc) + timedelta(seconds=2)

        crawled_at = datetime.fromisoformat(result["crawled_at"])
        self.assertIsNotNone(crawled_at.tzinfo)
        self.assertGreaterEqual(crawled_at, before)
        self.assertLessEqual(crawled_at, after)


class MapReviewTests(unittest.TestCase):
    def test_map_review_produces_expected_keys(self) -> None:
        raw = {
            "reviewId": "r1",
            "at": "2021-03-25T15:52:53",
            "userName": "Alice",
            "thumbsUpCount": 3,
            "score": 5,
            "content": "Great app",
        }

        result = map_review(raw, "com.example.app")

        self.assertEqual(set(result.keys()), set(_REVIEW_KEYS))
        self.assertEqual(result["package_name"], "com.example.app")
        self.assertEqual(result["review_id"], "r1")
        self.assertEqual(result["user_name"], "Alice")
        self.assertEqual(result["thumbs_up_count"], 3)
        self.assertEqual(result["score"], 5)
        self.assertEqual(result["content"], "Great app")
        self.assertEqual(result["at"], "2021-03-25T15:52:53")

    def test_map_review_passes_through_iso_at_without_double_conversion(self) -> None:
        raw = {"reviewId": "r1", "at": "2021-03-25T15:52:53+00:00"}
        result = map_review(raw, "com.example.app")
        self.assertEqual(result["at"], "2021-03-25T15:52:53+00:00")

    def test_map_review_missing_fields_become_none(self) -> None:
        result = map_review({}, "com.example.app")

        self.assertEqual(result["package_name"], "com.example.app")
        self.assertIsNone(result["review_id"])
        self.assertIsNone(result["user_name"])
        self.assertIsNone(result["thumbs_up_count"])
        self.assertIsNone(result["score"])
        self.assertIsNone(result["content"])
        self.assertIsNone(result["at"])
        self.assertIsInstance(result["crawled_at"], str)

    def test_map_review_crawled_at_is_recent_iso(self) -> None:
        before = datetime.now(timezone.utc) - timedelta(seconds=2)
        result = map_review({"reviewId": "r1"}, "com.example.app")
        after = datetime.now(timezone.utc) + timedelta(seconds=2)

        crawled_at = datetime.fromisoformat(result["crawled_at"])
        self.assertIsNotNone(crawled_at.tzinfo)
        self.assertGreaterEqual(crawled_at, before)
        self.assertLessEqual(crawled_at, after)
