"""Tests for the pure batch transformations.

``dedupe_by_key`` gets the most attention here because forgetting it produces a
failure mode that no ordinary run reveals: Postgres refuses an ``ON CONFLICT DO
UPDATE`` that would touch one row twice, so the bug only appears under replay,
as a crash loop.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from storage_consumer.batching import (
    bind_application_ids,
    coordinates_of,
    decode_batch,
    dedupe_by_key,
)
from storage_consumer.decoders import decode_app_stats, decode_review
from storage_consumer.events import (
    AppStatsEvent,
    BoundRecord,
    DecodedRecord,
    RecordCoordinates,
    ReviewEvent,
)
from storage_consumer.tests.test_decoders import app_stats_payload, review_payload

_BASE_TIME = datetime(2026, 9, 7, 14, tzinfo=timezone.utc)


class FakeRecord:
    """The five attributes of a ``ConsumerRecord`` this subsystem reads."""

    def __init__(
        self,
        value: bytes | None,
        *,
        topic: str = "app-stats",
        partition: int = 0,
        offset: int = 0,
        key: bytes | None = b"com.whatsapp",
    ) -> None:
        self.value = value
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key


def _json(payload: dict[str, object]) -> bytes:
    import json

    return json.dumps(payload).encode("utf-8")


def _decoded(package_name: str, crawled_at: datetime, offset: int = 0) -> DecodedRecord:
    return DecodedRecord(
        coordinates=RecordCoordinates(
            topic="app-stats", partition=0, offset=offset, key=package_name
        ),
        event=AppStatsEvent(package_name=package_name, crawled_at=crawled_at),
    )


def _bound_review(
    review_id: str, crawled_at: datetime, *, thumbs_up_count: int = 0, offset: int = 0
) -> BoundRecord:
    return BoundRecord(
        coordinates=RecordCoordinates(
            topic="reviews", partition=0, offset=offset, key="com.whatsapp"
        ),
        event=ReviewEvent(
            package_name="com.whatsapp",
            review_id=review_id,
            at=_BASE_TIME,
            crawled_at=crawled_at,
            thumbs_up_count=thumbs_up_count,
        ),
        application_id=1,
    )


class DecodeBatchTests(unittest.TestCase):
    def test_good_records_are_decoded(self) -> None:
        records = [FakeRecord(_json(app_stats_payload())) for _ in range(3)]

        decoded, rejected = decode_batch(records, decode_app_stats)

        self.assertEqual(len(decoded), 3)
        self.assertEqual(rejected, [])

    def test_one_bad_record_does_not_take_the_batch_with_it(self) -> None:
        """A single malformed message must not abort 500 good ones."""

        records = [
            FakeRecord(_json(app_stats_payload()), offset=0),
            FakeRecord(b"{not json", offset=1),
            FakeRecord(_json(app_stats_payload()), offset=2),
        ]

        decoded, rejected = decode_batch(records, decode_app_stats)

        self.assertEqual(len(decoded), 2)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0].coordinates.offset, 1)

    def test_rejection_reason_names_the_problem(self) -> None:
        records = [FakeRecord(_json(app_stats_payload(package_name=None)))]

        _, rejected = decode_batch(records, decode_app_stats)

        self.assertIn("package_name", rejected[0].reason)

    def test_rejected_records_keep_their_raw_payload(self) -> None:
        """An operator needs to see exactly what arrived."""

        records = [FakeRecord(b"{not json")]

        _, rejected = decode_batch(records, decode_app_stats)

        self.assertEqual(rejected[0].coordinates.payload, b"{not json")

    def test_an_empty_batch_yields_nothing(self) -> None:
        self.assertEqual(decode_batch([], decode_app_stats), ([], []))


class CoordinateTests(unittest.TestCase):
    def test_kafka_identity_is_captured(self) -> None:
        record = FakeRecord(b"{}", topic="reviews", partition=2, offset=99)

        coordinates = coordinates_of(record)

        self.assertEqual(
            (coordinates.topic, coordinates.partition, coordinates.offset),
            ("reviews", 2, 99),
        )

    def test_a_key_that_is_not_utf8_does_not_raise(self) -> None:
        """Reading a diagnostic must never turn into an unhandled exception."""

        coordinates = coordinates_of(FakeRecord(b"{}", key=b"\xff\xfe"))

        self.assertIsInstance(coordinates.key, str)

    def test_a_missing_key_stays_none(self) -> None:
        self.assertIsNone(coordinates_of(FakeRecord(b"{}", key=None)).key)


class BindApplicationIdsTests(unittest.TestCase):
    def test_known_packages_get_their_foreign_key(self) -> None:
        records = [_decoded("com.whatsapp", _BASE_TIME)]

        bound, rejected = bind_application_ids(records, {"com.whatsapp": 7})

        self.assertEqual(bound[0].application_id, 7)
        self.assertEqual(rejected, [])

    def test_unknown_packages_are_dead_lettered_not_dropped(self) -> None:
        records = [_decoded("com.unregistered", _BASE_TIME, offset=4)]

        bound, rejected = bind_application_ids(records, {})

        self.assertEqual(bound, [])
        self.assertEqual(rejected[0].coordinates.offset, 4)

    def test_the_rejection_explains_how_to_recover(self) -> None:
        records = [_decoded("com.unregistered", _BASE_TIME)]

        _, rejected = bind_application_ids(records, {})

        self.assertIn("com.unregistered", rejected[0].reason)
        self.assertIn("replay", rejected[0].reason)

    def test_one_unknown_package_does_not_block_the_others(self) -> None:
        records = [
            _decoded("com.whatsapp", _BASE_TIME, offset=0),
            _decoded("com.unregistered", _BASE_TIME, offset=1),
            _decoded("com.telegram", _BASE_TIME, offset=2),
        ]

        bound, rejected = bind_application_ids(
            records, {"com.whatsapp": 1, "com.telegram": 2}
        )

        self.assertEqual(len(bound), 2)
        self.assertEqual(len(rejected), 1)


class DedupeTests(unittest.TestCase):
    def test_a_batch_without_duplicates_is_unchanged(self) -> None:
        records = [
            _bound_review("r1", _BASE_TIME, offset=0),
            _bound_review("r2", _BASE_TIME, offset=1),
        ]

        self.assertEqual(dedupe_by_key(records), records)

    def test_duplicates_collapse_to_one_row(self) -> None:
        records = [
            _bound_review("r1", _BASE_TIME, offset=0),
            _bound_review("r1", _BASE_TIME + timedelta(hours=1), offset=1),
        ]

        self.assertEqual(len(dedupe_by_key(records)), 1)

    def test_the_freshest_duplicate_wins(self) -> None:
        records = [
            _bound_review("r1", _BASE_TIME, thumbs_up_count=1, offset=0),
            _bound_review(
                "r1", _BASE_TIME + timedelta(hours=1), thumbs_up_count=9, offset=1
            ),
        ]

        self.assertEqual(dedupe_by_key(records)[0].event.thumbs_up_count, 9)

    def test_an_out_of_order_duplicate_does_not_win(self) -> None:
        """Newest first in the batch must still leave the newest in the result."""

        records = [
            _bound_review(
                "r1", _BASE_TIME + timedelta(hours=1), thumbs_up_count=9, offset=0
            ),
            _bound_review("r1", _BASE_TIME, thumbs_up_count=1, offset=1),
        ]

        self.assertEqual(dedupe_by_key(records)[0].event.thumbs_up_count, 9)

    def test_first_arrival_order_is_preserved(self) -> None:
        records = [
            _bound_review("r2", _BASE_TIME, offset=0),
            _bound_review("r1", _BASE_TIME, offset=1),
            _bound_review("r2", _BASE_TIME + timedelta(hours=1), offset=2),
        ]

        keys = [record.event.review_id for record in dedupe_by_key(records)]

        self.assertEqual(keys, ["r2", "r1"])

    def test_app_stats_dedupes_on_package_and_crawl_time_together(self) -> None:
        """Two crawls of one app in one batch are two rows, not one."""

        records = [
            BoundRecord(
                coordinates=RecordCoordinates("app-stats", 0, offset),
                event=AppStatsEvent(
                    package_name="com.whatsapp",
                    crawled_at=_BASE_TIME + timedelta(hours=offset),
                ),
                application_id=1,
            )
            for offset in range(2)
        ]

        self.assertEqual(len(dedupe_by_key(records)), 2)

    def test_the_same_app_stats_message_twice_collapses(self) -> None:
        records = [
            BoundRecord(
                coordinates=RecordCoordinates("app-stats", 0, offset),
                event=AppStatsEvent(
                    package_name="com.whatsapp", crawled_at=_BASE_TIME
                ),
                application_id=1,
            )
            for offset in range(2)
        ]

        self.assertEqual(len(dedupe_by_key(records)), 1)

    def test_different_apps_at_the_same_instant_both_survive(self) -> None:
        records = [
            BoundRecord(
                coordinates=RecordCoordinates("app-stats", 0, index),
                event=AppStatsEvent(package_name=name, crawled_at=_BASE_TIME),
                application_id=index + 1,
            )
            for index, name in enumerate(("com.whatsapp", "com.telegram"))
        ]

        self.assertEqual(len(dedupe_by_key(records)), 2)

    def test_an_empty_batch_dedupes_to_nothing(self) -> None:
        self.assertEqual(dedupe_by_key([]), [])


class EndToEndPureBatchTests(unittest.TestCase):
    """The three pure steps composed, which is exactly what the worker does."""

    def test_a_mixed_batch_is_partitioned_correctly(self) -> None:
        records = [
            FakeRecord(_json(review_payload(review_id="r1")), offset=0),
            FakeRecord(b"garbage", offset=1),
            FakeRecord(
                _json(review_payload(review_id="r1", thumbs_up_count=99)), offset=2
            ),
            FakeRecord(
                _json(review_payload(package_name="com.unknown", review_id="r2")),
                offset=3,
            ),
        ]

        decoded, rejected = decode_batch(records, decode_review)
        bound, unknown = bind_application_ids(decoded, {"com.whatsapp": 1})
        rows = dedupe_by_key(bound)

        self.assertEqual(len(rejected), 1, "the garbage record")
        self.assertEqual(len(unknown), 1, "the unregistered package")
        self.assertEqual(len(rows), 1, "the two r1 messages collapsed")


if __name__ == "__main__":
    unittest.main()
