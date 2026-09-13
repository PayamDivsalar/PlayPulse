"""Tests for the event DTOs, focused on the two properties the writes rely on.

``conflict_key`` has to match its table's ``UNIQUE`` constraint exactly, and
``observed_at`` has to pick the fresher of two duplicates. Both are easy to get
subtly wrong and neither shows up until a replay.
"""

from __future__ import annotations

import dataclasses
import unittest
from datetime import datetime, timedelta, timezone

from storage_consumer.core.events import AppStatsEvent, NetworkMetricEvent, ReviewEvent

_NOW = datetime(2026, 9, 7, 14, tzinfo=timezone.utc)


def _network_metric(**overrides: object) -> NetworkMetricEvent:
    values: dict[str, object] = {
        "analysis_id": "8f14e45f-ea6a-4f2b-9c1d-2b3a5c7d9e01",
        "package_name": "com.whatsapp",
        "scenario": "UPLOAD",
        "analyzed_at": _NOW,
        "bytes_transferred_total": 1040,
        "bytes_payload_total": 1000,
        "overhead_ratio": 0.038462,
    }
    values.update(overrides)
    return NetworkMetricEvent(**values)  # type: ignore[arg-type]


class ConflictKeyTests(unittest.TestCase):
    def test_app_stats_is_keyed_on_app_and_crawl_time(self) -> None:
        event = AppStatsEvent(package_name="com.whatsapp", crawled_at=_NOW)

        self.assertEqual(event.conflict_key, ("com.whatsapp", _NOW))

    def test_two_crawls_of_one_app_have_different_keys(self) -> None:
        first = AppStatsEvent(package_name="com.whatsapp", crawled_at=_NOW)
        second = AppStatsEvent(
            package_name="com.whatsapp", crawled_at=_NOW + timedelta(hours=1)
        )

        self.assertNotEqual(first.conflict_key, second.conflict_key)

    def test_two_apps_at_one_instant_have_different_keys(self) -> None:
        first = AppStatsEvent(package_name="com.whatsapp", crawled_at=_NOW)
        second = AppStatsEvent(package_name="com.telegram", crawled_at=_NOW)

        self.assertNotEqual(first.conflict_key, second.conflict_key)

    def test_reviews_are_keyed_on_the_play_store_identifier(self) -> None:
        event = ReviewEvent(
            package_name="com.whatsapp", review_id="gp:AOq", at=_NOW, crawled_at=_NOW
        )

        self.assertEqual(event.conflict_key, "gp:AOq")

    def test_a_resynced_review_keeps_its_key(self) -> None:
        """Which is what makes the second delivery an update, not a new row."""

        first = ReviewEvent(
            package_name="com.whatsapp", review_id="gp:AOq", at=_NOW, crawled_at=_NOW
        )
        second = dataclasses.replace(
            first, crawled_at=_NOW + timedelta(hours=1), thumbs_up_count=9
        )

        self.assertEqual(first.conflict_key, second.conflict_key)

    def test_network_metrics_are_keyed_on_the_analysis_uuid(self) -> None:
        self.assertEqual(
            _network_metric().conflict_key, "8f14e45f-ea6a-4f2b-9c1d-2b3a5c7d9e01"
        )

    def test_conflict_keys_are_hashable(self) -> None:
        """dedupe_by_key puts them in a dict."""

        keys = {
            AppStatsEvent(package_name="a", crawled_at=_NOW).conflict_key,
            _network_metric().conflict_key,
        }

        self.assertEqual(len(keys), 2)


class ObservedAtTests(unittest.TestCase):
    def test_app_stats_freshness_is_its_crawl_time(self) -> None:
        event = AppStatsEvent(package_name="com.whatsapp", crawled_at=_NOW)

        self.assertEqual(event.observed_at, _NOW)

    def test_review_freshness_is_the_sync_time_not_the_review_time(self) -> None:
        """Two messages about one review share `at` but differ in `crawled_at`."""

        event = ReviewEvent(
            package_name="com.whatsapp",
            review_id="gp:AOq",
            at=_NOW - timedelta(days=30),
            crawled_at=_NOW,
        )

        self.assertEqual(event.observed_at, _NOW)

    def test_network_metric_freshness_is_its_analysis_time(self) -> None:
        self.assertEqual(_network_metric().observed_at, _NOW)


class ImmutabilityTests(unittest.TestCase):
    def test_events_cannot_be_mutated_after_decoding(self) -> None:
        event = AppStatsEvent(package_name="com.whatsapp", crawled_at=_NOW)

        with self.assertRaises(dataclasses.FrozenInstanceError):
            event.package_name = "com.telegram"  # type: ignore[misc]


class DefaultsTests(unittest.TestCase):
    def test_review_thumbs_up_defaults_to_the_column_default(self) -> None:
        event = ReviewEvent(
            package_name="com.whatsapp", review_id="gp:AOq", at=_NOW, crawled_at=_NOW
        )

        self.assertEqual(event.thumbs_up_count, 0)

    def test_network_metric_counters_default_to_zero(self) -> None:
        event = _network_metric()

        self.assertEqual(event.retransmission_count, 0)
        self.assertEqual(event.zero_window_count, 0)

    def test_an_absent_handshake_is_none_rather_than_zero(self) -> None:
        """0.0 ms would be a measurement; None means there was none to take."""

        self.assertIsNone(_network_metric().rtt_handshake)


if __name__ == "__main__":
    unittest.main()
