"""Integration tests for the write path, against the compose PostgreSQL.

The decisive test in this file is ``ReplayIdempotencyTests``: write a batch,
write the identical batch again, and assert the row counts and the row
contents are unchanged. That is the proof that at-least-once delivery plus
``ON CONFLICT`` really does add up to effectively-once, and it is the property
every other design decision in this subsystem is arranged around.

Run with::

    pytest storage_consumer/tests/test_persistence.py -m integration
"""

from __future__ import annotations

import unittest
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from storage_consumer.core.events import (
    AppStatsEvent,
    BoundRecord,
    NetworkMetricEvent,
    RecordCoordinates,
    RejectedRecord,
    ReviewEvent,
)
from storage_consumer.persistence.app_stats_repository import AppStatsRepository
from storage_consumer.persistence.application_resolver import ApplicationResolver
from storage_consumer.persistence.database import Database
from storage_consumer.persistence.dead_letter_repository import (
    MAX_PAYLOAD_BYTES,
    DeadLetterRepository,
)
from storage_consumer.persistence.migrator import Migrator
from storage_consumer.persistence.network_metric_repository import (
    NetworkMetricRepository,
)
from storage_consumer.persistence.review_repository import ReviewRepository
from storage_consumer.tests import integration_support

_CRAWLED_AT = datetime(2026, 9, 7, 14, 20, 11, 123456, tzinfo=timezone.utc)
_REVIEWED_AT = datetime(2026, 9, 1, 10, tzinfo=timezone.utc)


def _bind(event, application_id: int, offset: int = 0) -> BoundRecord:
    return BoundRecord(
        coordinates=RecordCoordinates(
            topic="test", partition=0, offset=offset, key=event.package_name
        ),
        event=event,
        application_id=application_id,
    )


@pytest.mark.integration
class PersistenceTestCase(unittest.TestCase):
    """Shared setup: a migrated database and one registered application."""

    def setUp(self) -> None:
        self.settings = integration_support.settings_or_skip()
        integration_support.require_postgres(self.settings)
        self.database = Database(
            self.settings, application_name="storage_consumer:test"
        )
        self.addCleanup(self.database.close)
        Migrator(self.database).apply_pending()

        self.package_name = integration_support.unique_package_name()
        with self.database.transaction() as cursor:
            self.application_id = integration_support.register_application(
                cursor, self.package_name
            )
        self.addCleanup(self._cleanup)

    def _cleanup(self) -> None:
        with self.database.transaction() as cursor:
            integration_support.delete_test_applications(cursor)

    def _count(self, table: str) -> int:
        with self.database.transaction() as cursor:
            cursor.execute(
                f"SELECT count(*) FROM {table} WHERE application_id = %s",
                (self.application_id,),
            )
            return cursor.fetchone()[0]

    def _app_stats_event(self, **overrides) -> AppStatsEvent:
        values = {
            "package_name": self.package_name,
            "crawled_at": _CRAWLED_AT,
            "min_installs": 5_000_000_000,
            "score": 4.3,
            "ratings": 178_000_000,
            "reviews_count": 3_400_000,
            "version": "2.24.17.79",
            "ad_supported": False,
            "app_updated_at": _CRAWLED_AT - timedelta(days=6),
        }
        values.update(overrides)
        return AppStatsEvent(**values)

    def _review_event(self, **overrides) -> ReviewEvent:
        values = {
            "package_name": self.package_name,
            "review_id": f"gp:{uuid.uuid4().hex}",
            "at": _REVIEWED_AT,
            "crawled_at": _CRAWLED_AT,
            "user_name": "Sara",
            "thumbs_up_count": 12,
            "score": 5,
            "content": "Works well.",
        }
        values.update(overrides)
        return ReviewEvent(**values)

    def _network_metric_event(self, **overrides) -> NetworkMetricEvent:
        values = {
            "analysis_id": str(uuid.uuid4()),
            "package_name": self.package_name,
            "scenario": "UPLOAD",
            "analyzed_at": _CRAWLED_AT,
            "bytes_transferred_total": 1040,
            "bytes_payload_total": 1000,
            "overhead_ratio": 0.038462,
            "rtt_handshake": 43.123,
            "retransmission_count": 7,
            "source_pcap_filename": "capture.pcap",
        }
        values.update(overrides)
        return NetworkMetricEvent(**values)


class AppStatsWriteTests(PersistenceTestCase):
    def test_a_row_round_trips_with_every_column(self) -> None:
        event = self._app_stats_event()

        with self.database.transaction() as cursor:
            AppStatsRepository().upsert(cursor, [_bind(event, self.application_id)])
            cursor.execute(
                "SELECT min_installs, score, ratings, reviews_count, version, "
                "       ad_supported, app_updated_at, crawled_at "
                "FROM app_stats WHERE application_id = %s",
                (self.application_id,),
            )
            row = cursor.fetchone()

        self.assertEqual(row[0], 5_000_000_000)
        self.assertAlmostEqual(row[1], 4.3)
        self.assertEqual(row[4], "2.24.17.79")
        self.assertIs(row[5], False)
        self.assertEqual(row[7], _CRAWLED_AT)

    def test_a_missing_measurement_is_stored_as_null(self) -> None:
        event = self._app_stats_event(min_installs=None, score=None, version=None)

        with self.database.transaction() as cursor:
            AppStatsRepository().upsert(cursor, [_bind(event, self.application_id)])
            cursor.execute(
                "SELECT min_installs, score, version FROM app_stats "
                "WHERE application_id = %s",
                (self.application_id,),
            )
            row = cursor.fetchone()

        self.assertEqual(row, (None, None, None))

    def test_a_later_crawl_appends_rather_than_replacing(self) -> None:
        """The table's whole purpose is the trend, so both rows must survive."""

        first = self._app_stats_event()
        second = self._app_stats_event(crawled_at=_CRAWLED_AT + timedelta(hours=1))

        with self.database.transaction() as cursor:
            AppStatsRepository().upsert(
                cursor,
                [
                    _bind(first, self.application_id, 0),
                    _bind(second, self.application_id, 1),
                ],
            )

        self.assertEqual(self._count("app_stats"), 2)

    def test_the_outcome_counts_inserts(self) -> None:
        with self.database.transaction() as cursor:
            outcome = AppStatsRepository().upsert(
                cursor, [_bind(self._app_stats_event(), self.application_id)]
            )

        self.assertEqual((outcome.inserted, outcome.updated, outcome.skipped), (1, 0, 0))

    def test_an_empty_batch_touches_nothing(self) -> None:
        with self.database.transaction() as cursor:
            outcome = AppStatsRepository().upsert(cursor, [])

        self.assertEqual(outcome.written, 0)

    def test_a_batch_larger_than_one_page_is_written_whole(self) -> None:
        """psycopg2 splits above PAGE_SIZE; every page must still land."""

        events = [
            self._app_stats_event(crawled_at=_CRAWLED_AT + timedelta(seconds=index))
            for index in range(1500)
        ]

        with self.database.transaction() as cursor:
            outcome = AppStatsRepository().upsert(
                cursor,
                [
                    _bind(event, self.application_id, index)
                    for index, event in enumerate(events)
                ],
            )

        self.assertEqual(outcome.inserted, 1500)
        self.assertEqual(self._count("app_stats"), 1500)


class ReviewWriteTests(PersistenceTestCase):
    def test_a_row_round_trips_with_every_column(self) -> None:
        event = self._review_event()

        with self.database.transaction() as cursor:
            ReviewRepository().upsert(cursor, [_bind(event, self.application_id)])
            cursor.execute(
                "SELECT review_id, user_name, thumbs_up_count, score, content, "
                "       at, last_synced_at, sentiment "
                "FROM reviews WHERE application_id = %s",
                (self.application_id,),
            )
            row = cursor.fetchone()

        self.assertEqual(row[0], event.review_id)
        self.assertEqual(row[1], "Sara")
        self.assertEqual(row[2], 12)
        self.assertEqual(row[3], 5)
        self.assertEqual(row[5], _REVIEWED_AT)
        self.assertEqual(row[6], _CRAWLED_AT, "last_synced_at comes from the message")
        self.assertIsNone(row[7], "sentiment belongs to another subsystem")

    def test_a_newer_sync_updates_the_row_in_place(self) -> None:
        first = self._review_event(thumbs_up_count=1)
        second = self._review_event(
            review_id=first.review_id,
            thumbs_up_count=99,
            crawled_at=_CRAWLED_AT + timedelta(hours=1),
        )

        with self.database.transaction() as cursor:
            ReviewRepository().upsert(cursor, [_bind(first, self.application_id)])
        with self.database.transaction() as cursor:
            outcome = ReviewRepository().upsert(
                cursor, [_bind(second, self.application_id)]
            )
            cursor.execute(
                "SELECT thumbs_up_count FROM reviews WHERE review_id = %s",
                (first.review_id,),
            )
            thumbs_up = cursor.fetchone()[0]

        self.assertEqual(self._count("reviews"), 1)
        self.assertEqual(thumbs_up, 99)
        self.assertEqual((outcome.inserted, outcome.updated), (0, 1))

    def test_a_stale_message_cannot_overwrite_fresher_data(self) -> None:
        """Kafka orders within a partition only; a slow retry can arrive late."""

        fresh = self._review_event(
            thumbs_up_count=99, crawled_at=_CRAWLED_AT + timedelta(hours=1)
        )
        stale = self._review_event(
            review_id=fresh.review_id, thumbs_up_count=1, crawled_at=_CRAWLED_AT
        )

        with self.database.transaction() as cursor:
            ReviewRepository().upsert(cursor, [_bind(fresh, self.application_id)])
        with self.database.transaction() as cursor:
            outcome = ReviewRepository().upsert(
                cursor, [_bind(stale, self.application_id)]
            )
            cursor.execute(
                "SELECT thumbs_up_count FROM reviews WHERE review_id = %s",
                (fresh.review_id,),
            )
            thumbs_up = cursor.fetchone()[0]

        self.assertEqual(thumbs_up, 99, "the stale message must not win")
        self.assertEqual(outcome.skipped, 1)

    def test_first_seen_at_survives_an_upsert(self) -> None:
        first = self._review_event()
        later = self._review_event(
            review_id=first.review_id,
            thumbs_up_count=50,
            crawled_at=_CRAWLED_AT + timedelta(hours=1),
        )

        with self.database.transaction() as cursor:
            ReviewRepository().upsert(cursor, [_bind(first, self.application_id)])
            cursor.execute(
                "SELECT first_seen_at FROM reviews WHERE review_id = %s",
                (first.review_id,),
            )
            original = cursor.fetchone()[0]

        with self.database.transaction() as cursor:
            ReviewRepository().upsert(cursor, [_bind(later, self.application_id)])
            cursor.execute(
                "SELECT first_seen_at FROM reviews WHERE review_id = %s",
                (first.review_id,),
            )
            after = cursor.fetchone()[0]

        self.assertEqual(original, after)

    def test_sentiment_survives_an_upsert(self) -> None:
        """A re-sync must not wipe an analysis the sentiment subsystem wrote."""

        first = self._review_event()
        later = self._review_event(
            review_id=first.review_id, crawled_at=_CRAWLED_AT + timedelta(hours=1)
        )

        with self.database.transaction() as cursor:
            ReviewRepository().upsert(cursor, [_bind(first, self.application_id)])
            cursor.execute(
                "UPDATE reviews SET sentiment = 'POSITIVE' WHERE review_id = %s",
                (first.review_id,),
            )
        with self.database.transaction() as cursor:
            ReviewRepository().upsert(cursor, [_bind(later, self.application_id)])
            cursor.execute(
                "SELECT sentiment FROM reviews WHERE review_id = %s",
                (first.review_id,),
            )
            sentiment = cursor.fetchone()[0]

        self.assertEqual(sentiment, "POSITIVE")

    def test_a_null_score_is_accepted(self) -> None:
        """The column was relaxed from NOT NULL precisely for this case."""

        with self.database.transaction() as cursor:
            outcome = ReviewRepository().upsert(
                cursor, [_bind(self._review_event(score=None), self.application_id)]
            )

        self.assertEqual(outcome.inserted, 1)

    def test_a_naive_review_timestamp_lands_as_utc(self) -> None:
        event = self._review_event(
            at=datetime(2026, 9, 1, 10, tzinfo=timezone.utc)
        )

        with self.database.transaction() as cursor:
            ReviewRepository().upsert(cursor, [_bind(event, self.application_id)])
            cursor.execute(
                "SELECT at AT TIME ZONE 'UTC' FROM reviews WHERE review_id = %s",
                (event.review_id,),
            )
            stored = cursor.fetchone()[0]

        self.assertEqual(stored, datetime(2026, 9, 1, 10))


class NetworkMetricWriteTests(PersistenceTestCase):
    def test_a_row_round_trips_with_every_column(self) -> None:
        event = self._network_metric_event()

        with self.database.transaction() as cursor:
            NetworkMetricRepository().upsert(
                cursor, [_bind(event, self.application_id)]
            )
            cursor.execute(
                "SELECT analysis_id::text, scenario, rtt_handshake, "
                "       retransmission_count, bytes_transferred_total, "
                "       overhead_ratio, source_pcap_filename, analyzed_at "
                "FROM network_metrics WHERE application_id = %s",
                (self.application_id,),
            )
            row = cursor.fetchone()

        self.assertEqual(row[0], event.analysis_id)
        self.assertEqual(row[1], "UPLOAD")
        self.assertAlmostEqual(row[2], 43.123)
        self.assertEqual(row[3], 7)
        self.assertEqual(row[7], _CRAWLED_AT)

    def test_a_rerun_of_the_same_test_makes_a_new_row(self) -> None:
        """A new UUID means a genuinely new observation, not a duplicate."""

        with self.database.transaction() as cursor:
            NetworkMetricRepository().upsert(
                cursor,
                [
                    _bind(self._network_metric_event(), self.application_id, 0),
                    _bind(self._network_metric_event(), self.application_id, 1),
                ],
            )

        self.assertEqual(self._count("network_metrics"), 2)

    def test_an_absent_handshake_is_stored_as_null(self) -> None:
        event = self._network_metric_event(rtt_handshake=None)

        with self.database.transaction() as cursor:
            NetworkMetricRepository().upsert(
                cursor, [_bind(event, self.application_id)]
            )
            cursor.execute(
                "SELECT rtt_handshake FROM network_metrics WHERE analysis_id = %s",
                (event.analysis_id,),
            )
            stored = cursor.fetchone()[0]

        self.assertIsNone(stored)


class ReplayIdempotencyTests(PersistenceTestCase):
    """Consume, replay, and assert nothing changed.

    This is the test the whole idempotency design exists to pass. Redelivery
    happens for two independent reasons -- a crash between the database commit
    and the offset commit, and the producers' own application-level retries --
    so "the same message twice" is a routine event, not an edge case.
    """

    def test_replaying_app_stats_changes_nothing(self) -> None:
        records = [
            _bind(
                self._app_stats_event(
                    crawled_at=_CRAWLED_AT + timedelta(hours=index)
                ),
                self.application_id,
                index,
            )
            for index in range(5)
        ]

        with self.database.transaction() as cursor:
            AppStatsRepository().upsert(cursor, records)
        before = self._count("app_stats")

        with self.database.transaction() as cursor:
            outcome = AppStatsRepository().upsert(cursor, records)

        self.assertEqual(self._count("app_stats"), before)
        self.assertEqual(outcome.inserted, 0)
        self.assertEqual(outcome.skipped, 5)

    def test_replaying_reviews_changes_nothing(self) -> None:
        records = [
            _bind(self._review_event(), self.application_id, index)
            for index in range(5)
        ]

        with self.database.transaction() as cursor:
            ReviewRepository().upsert(cursor, records)
        before = self._snapshot_reviews()

        with self.database.transaction() as cursor:
            outcome = ReviewRepository().upsert(cursor, records)

        self.assertEqual(self._snapshot_reviews(), before)
        self.assertEqual(outcome.skipped, 5, "the guard rejects an equal timestamp")

    def test_replaying_network_metrics_changes_nothing(self) -> None:
        records = [
            _bind(self._network_metric_event(), self.application_id, index)
            for index in range(5)
        ]

        with self.database.transaction() as cursor:
            NetworkMetricRepository().upsert(cursor, records)
        before = self._count("network_metrics")

        with self.database.transaction() as cursor:
            outcome = NetworkMetricRepository().upsert(cursor, records)

        self.assertEqual(self._count("network_metrics"), before)
        self.assertEqual(outcome.inserted, 0)

    def test_replaying_a_dead_letter_changes_nothing(self) -> None:
        rejected = [
            RejectedRecord(
                coordinates=RecordCoordinates(
                    topic="reviews", partition=0, offset=index, payload=b"{bad"
                ),
                reason="Message body is not valid JSON",
            )
            for index in range(3)
        ]

        with self.database.transaction() as cursor:
            DeadLetterRepository().record(cursor, rejected)
        before = self._count_dead_letters("reviews")

        with self.database.transaction() as cursor:
            DeadLetterRepository().record(cursor, rejected)

        self.assertEqual(self._count_dead_letters("reviews"), before)

    def _snapshot_reviews(self) -> list[tuple]:
        with self.database.transaction() as cursor:
            cursor.execute(
                "SELECT review_id, thumbs_up_count, first_seen_at, last_synced_at "
                "FROM reviews WHERE application_id = %s ORDER BY review_id",
                (self.application_id,),
            )
            return cursor.fetchall()

    def _count_dead_letters(self, topic: str) -> int:
        with self.database.transaction() as cursor:
            cursor.execute(
                "SELECT count(*) FROM dead_letter_events WHERE topic = %s",
                (topic,),
            )
            return cursor.fetchone()[0]


class DeadLetterTests(PersistenceTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.topic = f"itest-{uuid.uuid4().hex[:8]}"
        self.addCleanup(self._delete_dead_letters)

    def test_a_rejection_records_its_coordinates_and_reason(self) -> None:
        rejected = RejectedRecord(
            coordinates=RecordCoordinates(
                topic=self.topic,
                partition=2,
                offset=417,
                key="com.whatsapp",
                payload=b'{"truncated',
            ),
            reason="Message body is not valid JSON: unexpected end",
        )

        with self.database.transaction() as cursor:
            DeadLetterRepository().record(cursor, [rejected])
            cursor.execute(
                'SELECT "partition", kafka_offset, kafka_key, raw_payload, '
                "       error_reason FROM dead_letter_events WHERE topic = %s",
                (self.topic,),
            )
            row = cursor.fetchone()

        self.assertEqual(row[0], 2)
        self.assertEqual(row[1], 417)
        self.assertEqual(row[2], "com.whatsapp")
        self.assertEqual(bytes(row[3]), b'{"truncated')
        self.assertIn("not valid JSON", row[4])

    def test_an_invalid_utf8_payload_is_stored_verbatim(self) -> None:
        """BYTEA rather than TEXT exists exactly for this case."""

        rejected = RejectedRecord(
            coordinates=RecordCoordinates(
                topic=self.topic, partition=0, offset=1, payload=b"\xff\xfe\x00"
            ),
            reason="Message body is not valid UTF-8",
        )

        with self.database.transaction() as cursor:
            DeadLetterRepository().record(cursor, [rejected])
            cursor.execute(
                "SELECT raw_payload FROM dead_letter_events WHERE topic = %s",
                (self.topic,),
            )
            stored = bytes(cursor.fetchone()[0])

        self.assertEqual(stored, b"\xff\xfe\x00")

    def test_an_enormous_payload_is_truncated_and_flagged(self) -> None:
        rejected = RejectedRecord(
            coordinates=RecordCoordinates(
                topic=self.topic,
                partition=0,
                offset=2,
                payload=b"x" * (MAX_PAYLOAD_BYTES * 2),
            ),
            reason="Message body is not valid JSON",
        )

        with self.database.transaction() as cursor:
            DeadLetterRepository().record(cursor, [rejected])
            cursor.execute(
                "SELECT length(raw_payload), error_reason FROM dead_letter_events "
                "WHERE topic = %s",
                (self.topic,),
            )
            length, reason = cursor.fetchone()

        self.assertEqual(length, MAX_PAYLOAD_BYTES)
        self.assertIn("truncated", reason)

    def test_a_rejection_commits_with_the_batch_that_produced_it(self) -> None:
        """If the transaction rolls back, so must the dead-letter row."""

        rejected = RejectedRecord(
            coordinates=RecordCoordinates(topic=self.topic, partition=0, offset=3),
            reason="Required field 'review_id' is missing or null.",
        )

        with self.assertRaises(RuntimeError):
            with self.database.transaction() as cursor:
                DeadLetterRepository().record(cursor, [rejected])
                raise RuntimeError("the batch write failed after this point")

        with self.database.transaction() as cursor:
            cursor.execute(
                "SELECT count(*) FROM dead_letter_events WHERE topic = %s",
                (self.topic,),
            )
            self.assertEqual(cursor.fetchone()[0], 0)

    def test_an_empty_rejection_list_writes_nothing(self) -> None:
        with self.database.transaction() as cursor:
            self.assertEqual(DeadLetterRepository().record(cursor, []), 0)

    def _delete_dead_letters(self) -> None:
        with self.database.transaction() as cursor:
            cursor.execute(
                "DELETE FROM dead_letter_events WHERE topic = %s", (self.topic,)
            )


class ResolverIntegrationTests(PersistenceTestCase):
    def test_a_registered_package_resolves_to_its_row(self) -> None:
        resolver = ApplicationResolver(ttl_seconds=300.0)

        with self.database.transaction() as cursor:
            resolved = resolver.resolve(cursor, [self.package_name])

        self.assertEqual(resolved, {self.package_name: self.application_id})

    def test_an_unregistered_package_is_simply_absent(self) -> None:
        resolver = ApplicationResolver(ttl_seconds=300.0)

        with self.database.transaction() as cursor:
            resolved = resolver.resolve(cursor, ["com.definitely.not.registered"])

        self.assertEqual(resolved, {})

    def test_a_deactivated_application_still_resolves(self) -> None:
        """Data already produced for a soft-deleted app must still be stored."""

        resolver = ApplicationResolver(ttl_seconds=300.0)

        with self.database.transaction() as cursor:
            cursor.execute(
                "UPDATE apps_registry_application SET is_active = FALSE WHERE id = %s",
                (self.application_id,),
            )
            resolved = resolver.resolve(cursor, [self.package_name])

        self.assertEqual(resolved, {self.package_name: self.application_id})


class TransactionTests(PersistenceTestCase):
    def test_a_failed_batch_writes_nothing(self) -> None:
        """One batch is one transaction, so a partial write is impossible."""

        with self.assertRaises(RuntimeError):
            with self.database.transaction() as cursor:
                AppStatsRepository().upsert(
                    cursor, [_bind(self._app_stats_event(), self.application_id)]
                )
                raise RuntimeError("something went wrong later in the batch")

        self.assertEqual(self._count("app_stats"), 0)

    def test_the_connection_is_usable_after_a_rollback(self) -> None:
        with self.assertRaises(RuntimeError):
            with self.database.transaction() as cursor:
                cursor.execute("SELECT 1")
                raise RuntimeError("boom")

        with self.database.transaction() as cursor:
            AppStatsRepository().upsert(
                cursor, [_bind(self._app_stats_event(), self.application_id)]
            )

        self.assertEqual(self._count("app_stats"), 1)

    def test_reconnect_gives_a_working_connection(self) -> None:
        self.database.connect()

        self.database.reconnect()

        with self.database.transaction() as cursor:
            cursor.execute("SELECT 1")
            self.assertEqual(cursor.fetchone()[0], 1)

    def test_session_timeouts_are_applied_on_connect(self) -> None:
        """A hung write must not be able to pin a pipeline forever."""

        with self.database.transaction() as cursor:
            cursor.execute("SHOW statement_timeout")
            statement_timeout = cursor.fetchone()[0]
            cursor.execute("SHOW idle_in_transaction_session_timeout")
            idle_timeout = cursor.fetchone()[0]

        self.assertEqual(statement_timeout, "30s")
        self.assertEqual(idle_timeout, "1min")

    def test_the_application_name_is_visible_to_operators(self) -> None:
        with self.database.transaction() as cursor:
            cursor.execute("SELECT current_setting('application_name')")
            name = cursor.fetchone()[0]

        self.assertEqual(name, "storage_consumer:test")


if __name__ == "__main__":
    unittest.main()
