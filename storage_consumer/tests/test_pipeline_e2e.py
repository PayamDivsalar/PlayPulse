"""End-to-end tests over real Kafka and real PostgreSQL.

Each test creates its own topic and consumer group, so runs are isolated from
each other and from the live pipelines.

The decisive test is ``test_rewinding_the_group_and_replaying_changes_nothing``:
produce a batch, consume it, rewind the group's offsets to earliest, consume
again, and assert the row counts are identical. That is the proof that the
idempotency design actually works end to end, and it also exercises the exact
recovery procedure an operator would follow to rebuild a table.

Run with::

    pytest storage_consumer/tests/test_pipeline_e2e.py -m integration
"""

from __future__ import annotations

import json
import logging
import threading
import time
import unittest
import uuid
from datetime import datetime, timedelta, timezone

import pytest
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.structs import OffsetAndMetadata

from storage_consumer.consumer_factory import create_consumer
from storage_consumer.decoders import (
    decode_app_stats,
    decode_network_metric,
    decode_review,
)
from storage_consumer.persistence.app_stats_repository import AppStatsRepository
from storage_consumer.persistence.application_resolver import ApplicationResolver
from storage_consumer.persistence.database import Database
from storage_consumer.persistence.dead_letter_repository import DeadLetterRepository
from storage_consumer.persistence.migrator import Migrator
from storage_consumer.persistence.network_metric_repository import (
    NetworkMetricRepository,
)
from storage_consumer.persistence.review_repository import ReviewRepository
from storage_consumer.pipelines import PipelineSpec
from storage_consumer.supervisor import Supervisor
from storage_consumer.tests import integration_support
from storage_consumer.worker import Worker

# How long a test waits for rows to appear before declaring the pipeline stuck.
_DRAIN_TIMEOUT_SECONDS = 45.0
_PARTITIONS = 3


@pytest.mark.integration
class PipelineTestCase(unittest.TestCase):
    """A migrated database, a registered app, and a throwaway topic."""

    def setUp(self) -> None:
        self.settings = integration_support.settings_or_skip(
            # Short poll timeouts keep shutdown snappy in tests; production
            # defaults trade that for fewer wakeups.
            poll_timeout_ms=250,
            application_cache_ttl_seconds=300.0,
        )
        integration_support.require_postgres(self.settings)
        integration_support.require_kafka(self.settings)

        self.database = Database(
            self.settings, application_name="storage_consumer:e2e"
        )
        self.addCleanup(self.database.close)
        Migrator(self.database).apply_pending()

        self.package_name = integration_support.unique_package_name()
        with self.database.transaction() as cursor:
            self.application_id = integration_support.register_application(
                cursor, self.package_name
            )
        self.addCleanup(self._delete_test_rows)

        self.suffix = uuid.uuid4().hex[:10]
        self.bootstrap = self.settings.kafka_bootstrap_servers.split(",")
        self.admin = KafkaAdminClient(bootstrap_servers=self.bootstrap)
        self.addCleanup(self.admin.close)

    # --- Kafka helpers ---------------------------------------------------

    def create_topic(self, base: str) -> str:
        """Create a throwaway topic with several partitions.

        Multiple partitions on purpose: the compose broker auto-creates topics
        with one, which caps a consumer group at one active member and would
        let a single-partition assumption hide in the tests.
        """

        topic = f"itest-{base}-{self.suffix}"
        self.admin.create_topics(
            [NewTopic(topic, num_partitions=_PARTITIONS, replication_factor=1)]
        )
        self.addCleanup(self._delete_topic, topic)
        return topic

    def _delete_topic(self, topic: str) -> None:
        try:
            self.admin.delete_topics([topic])
        except Exception:  # noqa: BLE001 - cleanup must not fail a green test
            logging.getLogger(__name__).debug(
                "Could not delete %s.", topic, exc_info=True
            )

    def produce(self, topic: str, payloads: list[dict], *, key: str | None = None) -> None:
        """Publish messages the way the real producers do: JSON keyed by package."""

        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(
                "utf-8"
            ),
            acks="all",
        )
        try:
            for payload in payloads:
                producer.send(topic, key=key or self.package_name, value=payload)
            producer.flush()
        finally:
            producer.close()

    def produce_raw(self, topic: str, bodies: list[bytes]) -> None:
        """Publish bodies verbatim, so a test can send something malformed."""

        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            key_serializer=lambda value: value.encode("utf-8"),
            acks="all",
        )
        try:
            for body in bodies:
                producer.send(topic, key=self.package_name, value=body)
            producer.flush()
        finally:
            producer.close()

    def caught_up(self, topic: str, group_id: str) -> bool:
        """True once the group has committed up to the end of every partition.

        Used instead of a fixed sleep so a replay test finishes as soon as the
        pipeline has genuinely re-read the topic. Read through the admin API
        rather than a second consumer, which would join the group and trigger
        a rebalance mid-test.
        """

        committed = self.admin.list_consumer_group_offsets(group_id)
        for partition, end_offset in self._end_offsets(topic).items():
            if end_offset == 0:
                continue
            position = committed.get(partition)
            if position is None or position.offset < end_offset:
                return False
        return True

    def _end_offsets(self, topic: str) -> dict[TopicPartition, int]:
        consumer = KafkaConsumer(bootstrap_servers=self.bootstrap)
        try:
            partitions = [
                TopicPartition(topic, partition)
                for partition in consumer.partitions_for_topic(topic) or ()
            ]
            return consumer.end_offsets(partitions)
        finally:
            consumer.close()

    def rewind_group_to_earliest(self, topic: str, group_id: str) -> None:
        """Reset a consumer group's committed offsets, as an operator would.

        Only safe with no active member in the group, which is why every
        caller stops its worker first.

        The offsets are committed explicitly rather than via
        ``seek_to_beginning()`` followed by a bare ``commit()``. The bare form
        looks right and does nothing: it commits the positions this consumer
        has actually *consumed*, and a consumer that only sought has consumed
        nothing. Getting that wrong makes every replay test pass trivially, so
        the reset is verified below before any caller relies on it.
        """

        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap,
            group_id=group_id,
            enable_auto_commit=False,
        )
        try:
            partitions = [
                TopicPartition(topic, partition)
                for partition in consumer.partitions_for_topic(topic) or ()
            ]
            consumer.assign(partitions)
            consumer.commit(
                {
                    partition: OffsetAndMetadata(offset, "", -1)
                    for partition, offset in consumer.beginning_offsets(
                        partitions
                    ).items()
                }
            )
        finally:
            consumer.close()

        self.assertFalse(
            self.caught_up(topic, group_id),
            f"the offset reset on {topic} did not take effect, so the replay "
            "that follows would prove nothing",
        )

    # --- pipeline helpers ------------------------------------------------

    def spec_for(
        self, name: str, topic: str, *, max_poll_records: int = 200
    ) -> PipelineSpec:
        decoders = {
            "app-stats": decode_app_stats,
            "reviews": decode_review,
            "network-metrics": decode_network_metric,
        }
        repositories = {
            "app-stats": AppStatsRepository,
            "reviews": ReviewRepository,
            "network-metrics": NetworkMetricRepository,
        }
        return PipelineSpec(
            name=name,
            topic=topic,
            group_id=f"itest-{name}-{self.suffix}",
            decoder=decoders[name],
            repository=repositories[name](),
            conflict_key="(test)",
            max_poll_records=max_poll_records,
            poll_timeout_ms=self.settings.poll_timeout_ms,
        )

    def build_worker(self, spec: PipelineSpec, stop_event: threading.Event) -> Worker:
        return Worker(
            spec=spec,
            settings=self.settings,
            consumer=create_consumer(spec, self.settings),
            database=Database(
                self.settings, application_name=f"storage_consumer:{spec.name}"
            ),
            resolver=ApplicationResolver(
                ttl_seconds=self.settings.application_cache_ttl_seconds
            ),
            dead_letters=DeadLetterRepository(),
            stop_event=stop_event,
        )

    def drain(self, specs: list[PipelineSpec], until) -> None:
        """Run the pipelines until ``until()`` is true, then stop them.

        Uses the real ``Supervisor``, so shutdown goes through exactly the path
        SIGTERM takes in production.
        """

        supervisor = Supervisor(tuple(specs), self.settings, self.build_worker)
        thread = threading.Thread(target=supervisor.run, name="supervisor")
        thread.start()
        try:
            deadline = time.monotonic() + _DRAIN_TIMEOUT_SECONDS
            while time.monotonic() < deadline and not until():
                time.sleep(0.25)
        finally:
            supervisor.request_stop("test finished")
            thread.join(timeout=_DRAIN_TIMEOUT_SECONDS)
        self.assertFalse(thread.is_alive(), "the supervisor did not shut down")

    # --- assertions ------------------------------------------------------

    def count(self, table: str) -> int:
        with self.database.transaction() as cursor:
            cursor.execute(
                f"SELECT count(*) FROM {table} WHERE application_id = %s",
                (self.application_id,),
            )
            return cursor.fetchone()[0]

    def count_dead_letters(self, topic: str) -> int:
        with self.database.transaction() as cursor:
            cursor.execute(
                "SELECT count(*) FROM dead_letter_events WHERE topic = %s", (topic,)
            )
            return cursor.fetchone()[0]

    def _delete_test_rows(self) -> None:
        with self.database.transaction() as cursor:
            integration_support.delete_test_applications(cursor)
            cursor.execute(
                "DELETE FROM dead_letter_events WHERE topic LIKE %s",
                (f"itest-%-{self.suffix}",),
            )

    # --- payload builders ------------------------------------------------

    def app_stats_payloads(self, count: int) -> list[dict]:
        base = datetime(2026, 9, 7, 14, tzinfo=timezone.utc)
        return [
            {
                "package_name": self.package_name,
                "min_installs": 5_000_000_000 + index,
                "score": 4.3,
                "ratings": 178_000_000,
                "reviews_count": 3_400_000,
                "version": "2.24.17.79",
                "ad_supported": False,
                "app_updated_at": "2026-09-01T08:00:00+00:00",
                "crawled_at": (base + timedelta(minutes=index)).isoformat(),
            }
            for index in range(count)
        ]

    def review_payloads(self, count: int) -> list[dict]:
        return [
            {
                "package_name": self.package_name,
                "review_id": f"gp:{self.suffix}:{index}",
                "user_name": "Sara",
                "thumbs_up_count": index,
                "score": (index % 5) + 1,
                "content": "Works well.",
                # Naive on purpose: this is what google-play-scraper emits.
                "at": "2026-09-01T10:00:00",
                "crawled_at": "2026-09-07T14:20:11.123456+00:00",
            }
            for index in range(count)
        ]

    def network_metric_payloads(self, count: int) -> list[dict]:
        return [
            {
                "analysis_id": str(uuid.uuid4()),
                "package_name": self.package_name,
                "scenario": "UPLOAD" if index % 2 == 0 else "DOWNLOAD",
                "rtt_handshake": 43.123,
                "retransmission_count": 7,
                "out_of_order_count": 1,
                "spurious_retransmission_count": 2,
                "zero_window_count": 0,
                "tcp_reset_count": 2,
                "bytes_transferred_total": 1040,
                "bytes_payload_total": 1000,
                "overhead_ratio": 0.038462,
                "source_pcap_filename": f"capture-{index}.pcap",
                "analyzed_at": "2026-09-07T14:20:11+00:00",
            }
            for index in range(count)
        ]


class AppStatsPipelineTests(PipelineTestCase):
    def test_produced_messages_land_in_app_stats(self) -> None:
        topic = self.create_topic("app-stats")
        self.produce(topic, self.app_stats_payloads(25))
        spec = self.spec_for("app-stats", topic)

        self.drain([spec], until=lambda: self.count("app_stats") >= 25)

        self.assertEqual(self.count("app_stats"), 25)

    def test_every_column_survives_the_round_trip(self) -> None:
        topic = self.create_topic("app-stats-columns")
        self.produce(topic, self.app_stats_payloads(1))
        spec = self.spec_for("app-stats", topic)

        self.drain([spec], until=lambda: self.count("app_stats") >= 1)

        with self.database.transaction() as cursor:
            cursor.execute(
                "SELECT min_installs, score, version, ad_supported, crawled_at "
                "FROM app_stats WHERE application_id = %s",
                (self.application_id,),
            )
            row = cursor.fetchone()

        self.assertEqual(row[0], 5_000_000_000)
        self.assertEqual(row[2], "2.24.17.79")
        self.assertIs(row[3], False)
        self.assertEqual(row[4], datetime(2026, 9, 7, 14, tzinfo=timezone.utc))

    def test_a_malformed_message_is_dead_lettered_and_the_rest_proceed(self) -> None:
        """One bad record must not stop the partition it sits on."""

        topic = self.create_topic("app-stats-poison")
        good = [json.dumps(p).encode("utf-8") for p in self.app_stats_payloads(4)]
        bodies = [good[0], b"{ this is not json", *good[1:]]
        self.produce_raw(topic, bodies)
        spec = self.spec_for("app-stats", topic)

        self.drain(
            [spec],
            until=lambda: self.count("app_stats") >= 4
            and self.count_dead_letters(topic) >= 1,
        )

        self.assertEqual(self.count("app_stats"), 4)
        self.assertEqual(self.count_dead_letters(topic), 1)

    def test_an_unknown_package_is_dead_lettered(self) -> None:
        topic = self.create_topic("app-stats-unknown")
        payloads = self.app_stats_payloads(1)
        payloads[0]["package_name"] = "com.definitely.not.registered"
        self.produce(topic, payloads, key="com.definitely.not.registered")
        spec = self.spec_for("app-stats", topic)

        self.drain([spec], until=lambda: self.count_dead_letters(topic) >= 1)

        self.assertEqual(self.count("app_stats"), 0)
        self.assertEqual(self.count_dead_letters(topic), 1)

    def test_a_restart_loses_nothing_and_duplicates_nothing(self) -> None:
        """Consume half, stop, produce more, restart: every row exactly once."""

        topic = self.create_topic("app-stats-restart")
        payloads = self.app_stats_payloads(30)
        self.produce(topic, payloads[:15])
        spec = self.spec_for("app-stats", topic)

        self.drain([spec], until=lambda: self.count("app_stats") >= 15)
        self.assertEqual(self.count("app_stats"), 15)

        self.produce(topic, payloads[15:])
        self.drain([spec], until=lambda: self.count("app_stats") >= 30)

        self.assertEqual(self.count("app_stats"), 30)


class ReplayIdempotencyTests(PipelineTestCase):
    def test_rewinding_the_group_and_replaying_changes_nothing(self) -> None:
        """The single test that proves the whole idempotency design.

        This is also the documented recovery procedure: an operator who needs
        to rebuild a table resets that pipeline's group to earliest and lets it
        re-consume. If replay were not a no-op, doing so would double the data.
        """

        topic = self.create_topic("replay")
        self.produce(topic, self.app_stats_payloads(20))
        spec = self.spec_for("app-stats", topic)

        self.drain([spec], until=lambda: self.count("app_stats") >= 20)
        first_pass = self.snapshot()
        self.assertEqual(len(first_pass), 20)

        self.rewind_group_to_earliest(topic, spec.group_id)
        self.drain([spec], until=lambda: self.caught_up(topic, spec.group_id))

        self.assertEqual(self.snapshot(), first_pass)

    def test_replaying_reviews_preserves_first_seen_at(self) -> None:
        topic = self.create_topic("replay-reviews")
        self.produce(topic, self.review_payloads(10))
        spec = self.spec_for("reviews", topic, max_poll_records=500)

        self.drain([spec], until=lambda: self.count("reviews") >= 10)
        before = self.review_snapshot()

        self.rewind_group_to_earliest(topic, spec.group_id)
        self.drain([spec], until=lambda: self.caught_up(topic, spec.group_id))

        self.assertEqual(self.review_snapshot(), before)

    def test_replaying_network_metrics_changes_nothing(self) -> None:
        topic = self.create_topic("replay-metrics")
        self.produce(topic, self.network_metric_payloads(5))
        spec = self.spec_for("network-metrics", topic, max_poll_records=50)

        self.drain([spec], until=lambda: self.count("network_metrics") >= 5)

        self.rewind_group_to_earliest(topic, spec.group_id)
        self.drain([spec], until=lambda: self.caught_up(topic, spec.group_id))

        self.assertEqual(self.count("network_metrics"), 5)

    def test_a_duplicate_message_on_the_topic_makes_one_row(self) -> None:
        """Producer-side retries put the same message on the topic twice."""

        topic = self.create_topic("duplicate")
        payloads = self.app_stats_payloads(5)
        self.produce(topic, payloads + payloads)
        spec = self.spec_for("app-stats", topic)

        self.drain([spec], until=lambda: self.count("app_stats") >= 5)

        self.assertEqual(self.count("app_stats"), 5)

    def test_a_duplicate_review_on_the_topic_makes_one_row(self) -> None:
        """The case that makes ``dedupe_by_key`` mandatory rather than an optimisation.

        Two versions of one review in a single batch would make Postgres refuse
        the whole ``ON CONFLICT DO UPDATE`` statement -- "cannot affect row a
        second time" -- and abort the transaction, putting the pipeline into a
        crash loop that only ever appears under replay.
        """

        topic = self.create_topic("dup-reviews")
        payloads = self.review_payloads(5)
        resynced = [
            dict(payload, thumbs_up_count=99, crawled_at="2026-09-07T15:20:11+00:00")
            for payload in payloads
        ]
        self.produce(topic, payloads + resynced)
        spec = self.spec_for("reviews", topic, max_poll_records=500)

        self.drain([spec], until=lambda: self.caught_up(topic, spec.group_id))

        self.assertEqual(self.count("reviews"), 5)
        with self.database.transaction() as cursor:
            cursor.execute(
                "SELECT DISTINCT thumbs_up_count FROM reviews "
                "WHERE application_id = %s",
                (self.application_id,),
            )
            self.assertEqual(cursor.fetchall(), [(99,)], "the fresher sync won")

    def snapshot(self) -> list[tuple]:
        with self.database.transaction() as cursor:
            cursor.execute(
                "SELECT min_installs, crawled_at FROM app_stats "
                "WHERE application_id = %s ORDER BY crawled_at",
                (self.application_id,),
            )
            return cursor.fetchall()

    def review_snapshot(self) -> list[tuple]:
        with self.database.transaction() as cursor:
            cursor.execute(
                "SELECT review_id, thumbs_up_count, first_seen_at, last_synced_at "
                "FROM reviews WHERE application_id = %s ORDER BY review_id",
                (self.application_id,),
            )
            return cursor.fetchall()


class ConcurrencyIsolationTests(PipelineTestCase):
    """The tests that justify one thread per pipeline.

    Threads here buy isolation, not speed. A single-threaded consumer
    subscribed to all three topics would process one poll at a time, so a
    reviews burst -- up to 200,000 messages over five to fifteen minutes --
    would sit in front of a ``network-metrics`` message and delay it by
    minutes. These tests are the evidence that the threaded version does not.
    """

    #: A network-metrics message must not wait for the reviews backlog. The
    #: bound is generous because CI machines are slow; the failure this catches
    #: is minutes of head-of-line blocking, not tens of milliseconds.
    MAX_ACCEPTABLE_LATENCY_SECONDS = 20.0

    def test_all_three_pipelines_run_together(self) -> None:
        reviews_topic = self.create_topic("all-reviews")
        stats_topic = self.create_topic("all-stats")
        metrics_topic = self.create_topic("all-metrics")

        self.produce(reviews_topic, self.review_payloads(40))
        self.produce(stats_topic, self.app_stats_payloads(10))
        self.produce(metrics_topic, self.network_metric_payloads(5))

        specs = [
            self.spec_for("reviews", reviews_topic, max_poll_records=500),
            self.spec_for("app-stats", stats_topic),
            self.spec_for("network-metrics", metrics_topic, max_poll_records=50),
        ]

        self.drain(
            specs,
            until=lambda: self.count("reviews") >= 40
            and self.count("app_stats") >= 10
            and self.count("network_metrics") >= 5,
        )

        self.assertEqual(
            (
                self.count("reviews"),
                self.count("app_stats"),
                self.count("network_metrics"),
            ),
            (40, 10, 5),
        )

    def test_a_reviews_burst_does_not_delay_network_metrics(self) -> None:
        """Measure how long a metrics message waits behind a reviews backlog."""

        reviews_topic = self.create_topic("burst-reviews")
        metrics_topic = self.create_topic("burst-metrics")

        # Queued before the pipelines start, so the reviews thread has a
        # backlog waiting the moment it is assigned partitions.
        self.produce(reviews_topic, self.review_payloads(5000))

        specs = [
            self.spec_for("reviews", reviews_topic, max_poll_records=500),
            self.spec_for("network-metrics", metrics_topic, max_poll_records=50),
        ]

        supervisor = Supervisor(tuple(specs), self.settings, self.build_worker)
        thread = threading.Thread(target=supervisor.run, name="supervisor")
        thread.start()
        try:
            # Wait for the reviews pipeline to be visibly busy before injecting
            # the metrics message, so the measurement is of contention rather
            # than of startup.
            deadline = time.monotonic() + _DRAIN_TIMEOUT_SECONDS
            while time.monotonic() < deadline and self.count("reviews") < 500:
                time.sleep(0.1)
            self.assertGreaterEqual(
                self.count("reviews"), 500, "the reviews burst never started"
            )

            self.produce(metrics_topic, self.network_metric_payloads(1))
            sent_at = time.monotonic()

            deadline = sent_at + _DRAIN_TIMEOUT_SECONDS
            while time.monotonic() < deadline and self.count("network_metrics") < 1:
                time.sleep(0.05)
            latency = time.monotonic() - sent_at
        finally:
            supervisor.request_stop("test finished")
            thread.join(timeout=_DRAIN_TIMEOUT_SECONDS)

        self.assertEqual(self.count("network_metrics"), 1)
        self.assertLess(
            latency,
            self.MAX_ACCEPTABLE_LATENCY_SECONDS,
            f"a network-metrics message waited {latency:.1f}s behind the reviews "
            "burst, which is the head-of-line blocking the threads exist to "
            "prevent",
        )
        # Reported so the sizing decision can be revisited against real numbers
        # rather than re-derived from scratch.
        logging.getLogger(__name__).info(
            "network-metrics latency during a reviews burst: %.2fs "
            "(reviews stored so far: %s)",
            latency,
            self.count("reviews"),
        )

    def test_each_pipeline_keeps_its_own_offsets(self) -> None:
        """One topic can be replayed without touching the other two."""

        reviews_topic = self.create_topic("offsets-reviews")
        metrics_topic = self.create_topic("offsets-metrics")
        self.produce(reviews_topic, self.review_payloads(10))
        self.produce(metrics_topic, self.network_metric_payloads(4))

        specs = [
            self.spec_for("reviews", reviews_topic, max_poll_records=500),
            self.spec_for("network-metrics", metrics_topic, max_poll_records=50),
        ]
        self.drain(
            specs,
            until=lambda: self.count("reviews") >= 10
            and self.count("network_metrics") >= 4,
        )

        # Rewind only the metrics group. The reviews group must be untouched.
        self.rewind_group_to_earliest(metrics_topic, specs[1].group_id)

        self.assertTrue(self.caught_up(reviews_topic, specs[0].group_id))
        self.assertFalse(self.caught_up(metrics_topic, specs[1].group_id))


if __name__ == "__main__":
    unittest.main()
