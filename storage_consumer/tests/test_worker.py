"""Tests for the poll / write / commit loop, driven by fakes.

Two of these matter more than the rest, because they are the assertions that
stand between this design and data loss:

* ``test_offsets_are_not_committed_when_the_write_fails`` -- the ordering
  guarantee. If offsets could advance past an unwritten batch, the subsystem
  would silently lose data on every database blip.
* ``test_a_transient_database_error_retries_the_batch`` -- the error taxonomy.
  Dead-lettering an ``OperationalError`` would discard perfectly good rows
  because Postgres happened to be restarting.
"""

from __future__ import annotations

import json
import threading
import unittest
from datetime import datetime, timezone
from typing import Any
from unittest import mock

import psycopg2
from kafka.errors import CommitFailedError, KafkaError

from storage_consumer.config import Settings
from storage_consumer.decoders import decode_app_stats
from storage_consumer.persistence.repository import WriteOutcome
from storage_consumer.pipelines import PipelineSpec
from storage_consumer.tests.test_decoders import app_stats_payload
from storage_consumer.worker import MAX_CONSECUTIVE_COMMIT_FAILURES, Worker


class FakeTopicPartition:
    def __init__(self, topic: str, partition: int) -> None:
        self.topic = topic
        self.partition = partition

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))

    def __eq__(self, other: object) -> bool:
        return (self.topic, self.partition) == (other.topic, other.partition)  # type: ignore[attr-defined]


class FakeRecord:
    def __init__(self, value: bytes, offset: int, partition: int = 0) -> None:
        self.value = value
        self.topic = "app-stats"
        self.partition = partition
        self.offset = offset
        self.key = b"com.whatsapp"


class FakeConsumer:
    """Yields a scripted sequence of poll results, then blocks on empty ones."""

    def __init__(self, polls: list[dict[Any, list[FakeRecord]]]) -> None:
        self._polls = list(polls)
        self.commits = 0
        self.closed = False
        self.commit_error: BaseException | None = None
        self.poll_calls = 0

    def poll(self, timeout_ms: int, max_records: int) -> dict[Any, list[FakeRecord]]:
        self.poll_calls += 1
        if self._polls:
            return self._polls.pop(0)
        return {}

    def commit(self) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.commits += 1

    def close(self) -> None:
        self.closed = True


class FakeCursor:
    def __init__(self, registry: dict[str, int]) -> None:
        self.registry = registry
        self._rows: list[tuple[str, int]] = []

    def execute(self, statement: str, parameters: tuple | None = None) -> None:
        requested = (parameters or ([],))[0]
        self._rows = [
            (name, self.registry[name]) for name in requested if name in self.registry
        ]

    def fetchall(self) -> list[tuple[str, int]]:
        return self._rows


class FakeDatabase:
    """A transaction context manager over a fake cursor, with a failure switch."""

    def __init__(self, registry: dict[str, int] | None = None) -> None:
        self.registry = registry if registry is not None else {"com.whatsapp": 1}
        self.transactions = 0
        self.commits = 0
        self.rollbacks = 0
        self.reconnects = 0
        self.closed = False
        #: Exceptions to raise from successive transactions.
        self.failures: list[BaseException] = []

    class _Transaction:
        def __init__(self, database: FakeDatabase) -> None:
            self._database = database

        def __enter__(self) -> FakeCursor:
            return FakeCursor(self._database.registry)

        def __exit__(self, exc_type, exc, tb) -> bool:
            if exc_type is None:
                self._database.commits += 1
            else:
                self._database.rollbacks += 1
            return False

    def transaction(self) -> _Transaction:
        self.transactions += 1
        if self.failures:
            raise self.failures.pop(0)
        return self._Transaction(self)

    def reconnect(self) -> None:
        self.reconnects += 1

    def close(self) -> None:
        self.closed = True


class RecordingRepository:
    def __init__(self) -> None:
        self.batches: list[list[Any]] = []

    def upsert(self, cursor: Any, records: list[Any]) -> WriteOutcome:
        self.batches.append(list(records))
        return WriteOutcome(inserted=len(records))


class RecordingDeadLetters:
    def __init__(self) -> None:
        self.recorded: list[Any] = []

    def record(self, cursor: Any, rejected: list[Any]) -> int:
        self.recorded.extend(rejected)
        return len(rejected)


def _payload(**overrides: object) -> bytes:
    return json.dumps(app_stats_payload(**overrides)).encode("utf-8")


class WorkerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch("storage_consumer.retry_policy.sleep")
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)

        self.settings = Settings.for_testing(
            db_retry_max_attempts=2, db_retry_base_delay_seconds=0.01
        )
        self.repository = RecordingRepository()
        self.spec = PipelineSpec(
            name="app-stats",
            topic="app-stats",
            group_id="storage-consumer.app-stats",
            decoder=decode_app_stats,
            repository=self.repository,  # type: ignore[arg-type]
            conflict_key="(application_id, crawled_at)",
            max_poll_records=200,
            poll_timeout_ms=10,
        )
        self.dead_letters = RecordingDeadLetters()
        self.stop = threading.Event()

    def _worker(self, consumer: FakeConsumer, database: FakeDatabase) -> Worker:
        from storage_consumer.persistence.application_resolver import (
            ApplicationResolver,
        )

        return Worker(
            spec=self.spec,
            settings=self.settings,
            consumer=consumer,
            database=database,  # type: ignore[arg-type]
            resolver=ApplicationResolver(ttl_seconds=300.0),
            dead_letters=self.dead_letters,  # type: ignore[arg-type]
            stop_event=self.stop,
        )

    def _one_batch(self, records: list[FakeRecord]) -> list[dict[Any, list[FakeRecord]]]:
        """A single poll result, after which the worker is told to stop."""

        return [{FakeTopicPartition("app-stats", 0): records}]

    def _run_once(self, consumer: FakeConsumer, worker: Worker) -> None:
        """Run the loop for exactly one non-empty poll."""

        original_poll = consumer.poll

        def poll_then_stop(**kwargs: Any):
            result = original_poll(**kwargs)
            if not result:
                self.stop.set()
            return result

        consumer.poll = poll_then_stop  # type: ignore[method-assign]
        worker.run()


class HappyPathTests(WorkerTestCase):
    def test_a_batch_is_written_and_then_committed(self) -> None:
        consumer = FakeConsumer(
            self._one_batch([FakeRecord(_payload(), offset=0)])
        )
        database = FakeDatabase()

        self._run_once(consumer, self._worker(consumer, database))

        self.assertEqual(len(self.repository.batches), 1)
        self.assertEqual(consumer.commits, 1)

    def test_an_empty_poll_writes_and_commits_nothing(self) -> None:
        consumer = FakeConsumer([])
        database = FakeDatabase()

        self._run_once(consumer, self._worker(consumer, database))

        self.assertEqual(self.repository.batches, [])
        self.assertEqual(consumer.commits, 0)

    def test_records_from_several_partitions_are_written_together(self) -> None:
        """One transaction per poll, not one per partition."""

        consumer = FakeConsumer(
            [
                {
                    FakeTopicPartition("app-stats", 0): [
                        FakeRecord(_payload(), offset=0, partition=0)
                    ],
                    FakeTopicPartition("app-stats", 1): [
                        FakeRecord(
                            _payload(crawled_at="2026-09-07T15:00:00+00:00"),
                            offset=0,
                            partition=1,
                        )
                    ],
                }
            ]
        )
        database = FakeDatabase()

        self._run_once(consumer, self._worker(consumer, database))

        self.assertEqual(len(self.repository.batches), 1)
        self.assertEqual(len(self.repository.batches[0]), 2)

    def test_the_loop_exits_when_the_stop_event_is_set(self) -> None:
        consumer = FakeConsumer([])
        self.stop.set()

        self._worker(consumer, FakeDatabase()).run()

        self.assertEqual(consumer.poll_calls, 0)


class OrderingGuaranteeTests(WorkerTestCase):
    def test_offsets_are_not_committed_when_the_write_fails(self) -> None:
        """The single most important assertion in this suite.

        If offsets advanced past a batch Postgres never accepted, the records
        would never be redelivered and the data would be gone.
        """

        consumer = FakeConsumer(
            self._one_batch([FakeRecord(_payload(), offset=0)])
        )
        database = FakeDatabase()
        database.failures = [psycopg2.OperationalError("down")] * 3

        with self.assertRaises(psycopg2.OperationalError):
            self._run_once(consumer, self._worker(consumer, database))

        self.assertEqual(consumer.commits, 0)

    def test_a_failing_write_is_fatal_rather_than_skipped(self) -> None:
        """Skipping the batch to keep going would be silent data loss."""

        consumer = FakeConsumer(
            self._one_batch([FakeRecord(_payload(), offset=0)])
        )
        database = FakeDatabase()
        database.failures = [psycopg2.OperationalError("down")] * 3

        with self.assertRaises(psycopg2.OperationalError):
            self._run_once(consumer, self._worker(consumer, database))

    def test_the_transaction_is_rolled_back_on_a_repository_error(self) -> None:
        consumer = FakeConsumer(
            self._one_batch([FakeRecord(_payload(), offset=0)])
        )
        database = FakeDatabase()
        self.repository.upsert = mock.Mock(  # type: ignore[method-assign]
            side_effect=RuntimeError("bad SQL")
        )

        with self.assertRaises(RuntimeError):
            self._run_once(consumer, self._worker(consumer, database))

        self.assertEqual(database.rollbacks, 1)
        self.assertEqual(consumer.commits, 0)


class TransientErrorTests(WorkerTestCase):
    def test_a_transient_database_error_retries_the_batch(self) -> None:
        """And does not dead-letter it: the data is fine, the server was not."""

        consumer = FakeConsumer(
            self._one_batch([FakeRecord(_payload(), offset=0)])
        )
        database = FakeDatabase()
        database.failures = [psycopg2.OperationalError("server closed")]

        self._run_once(consumer, self._worker(consumer, database))

        self.assertEqual(len(self.repository.batches), 1)
        self.assertEqual(self.dead_letters.recorded, [])
        self.assertEqual(consumer.commits, 1)

    def test_the_connection_is_replaced_before_retrying(self) -> None:
        """A dead connection would fail every remaining attempt instantly."""

        consumer = FakeConsumer(
            self._one_batch([FakeRecord(_payload(), offset=0)])
        )
        database = FakeDatabase()
        database.failures = [psycopg2.OperationalError("server closed")]

        self._run_once(consumer, self._worker(consumer, database))

        self.assertEqual(database.reconnects, 1)

    def test_a_deadlock_is_retried(self) -> None:
        consumer = FakeConsumer(
            self._one_batch([FakeRecord(_payload(), offset=0)])
        )
        database = FakeDatabase()
        database.failures = [psycopg2.errors.DeadlockDetected("deadlock")]

        self._run_once(consumer, self._worker(consumer, database))

        self.assertEqual(consumer.commits, 1)

    def test_a_programming_error_is_not_retried(self) -> None:
        """Retrying a bug wastes the retry budget and hides the stack trace."""

        consumer = FakeConsumer(
            self._one_batch([FakeRecord(_payload(), offset=0)])
        )
        database = FakeDatabase()
        database.failures = [psycopg2.errors.UndefinedTable("no such table")]

        with self.assertRaises(psycopg2.errors.UndefinedTable):
            self._run_once(consumer, self._worker(consumer, database))

        self.assertEqual(database.transactions, 1, "no retry was attempted")


class DeadLetterRoutingTests(WorkerTestCase):
    def test_a_malformed_record_is_dead_lettered_and_the_batch_proceeds(self) -> None:
        consumer = FakeConsumer(
            self._one_batch(
                [
                    FakeRecord(_payload(), offset=0),
                    FakeRecord(b"{not json", offset=1),
                    FakeRecord(
                        _payload(crawled_at="2026-09-07T16:00:00+00:00"), offset=2
                    ),
                ]
            )
        )

        self._run_once(consumer, self._worker(consumer, FakeDatabase()))

        self.assertEqual(len(self.dead_letters.recorded), 1)
        self.assertEqual(len(self.repository.batches[0]), 2)
        self.assertEqual(consumer.commits, 1, "the offset advances past the bad record")

    def test_an_unknown_package_is_dead_lettered(self) -> None:
        consumer = FakeConsumer(
            self._one_batch(
                [FakeRecord(_payload(package_name="com.unregistered"), offset=0)]
            )
        )

        self._run_once(consumer, self._worker(consumer, FakeDatabase()))

        self.assertEqual(len(self.dead_letters.recorded), 1)
        self.assertIn("com.unregistered", self.dead_letters.recorded[0].reason)

    def test_a_batch_of_nothing_but_bad_records_still_commits(self) -> None:
        """Otherwise a poison batch blocks the partition forever."""

        consumer = FakeConsumer(
            self._one_batch(
                [FakeRecord(b"{not json", offset=0), FakeRecord(b"[]", offset=1)]
            )
        )

        self._run_once(consumer, self._worker(consumer, FakeDatabase()))

        self.assertEqual(len(self.dead_letters.recorded), 2)
        self.assertEqual(consumer.commits, 1)

    def test_duplicates_within_one_batch_are_collapsed_before_writing(self) -> None:
        """Postgres rejects a statement that would touch one row twice."""

        consumer = FakeConsumer(
            self._one_batch(
                [FakeRecord(_payload(), offset=0), FakeRecord(_payload(), offset=1)]
            )
        )

        self._run_once(consumer, self._worker(consumer, FakeDatabase()))

        self.assertEqual(len(self.repository.batches[0]), 1)


class CommitFailureTests(WorkerTestCase):
    def test_a_rebalance_during_the_batch_is_survivable(self) -> None:
        """The rows are durable; the redelivery is a no-op."""

        consumer = FakeConsumer(
            self._one_batch([FakeRecord(_payload(), offset=0)])
        )
        consumer.commit_error = CommitFailedError("rebalanced")

        self._run_once(consumer, self._worker(consumer, FakeDatabase()))

        self.assertEqual(len(self.repository.batches), 1)

    def test_commits_that_never_succeed_become_fatal(self) -> None:
        """A pipeline that reprocesses forever without advancing must crash."""

        polls = [
            {FakeTopicPartition("app-stats", 0): [FakeRecord(_payload(), offset=index)]}
            for index in range(MAX_CONSECUTIVE_COMMIT_FAILURES)
        ]
        consumer = FakeConsumer(polls)
        consumer.commit_error = KafkaError("broker unreachable")

        with self.assertRaises(RuntimeError) as caught:
            self._worker(consumer, FakeDatabase()).run()

        self.assertIn("consecutive offset commits failed", str(caught.exception))

    def test_a_successful_commit_resets_the_failure_count(self) -> None:
        consumer = FakeConsumer(
            self._one_batch([FakeRecord(_payload(), offset=0)])
        )
        worker = self._worker(consumer, FakeDatabase())
        consumer.commit_error = KafkaError("blip")
        worker._commit()
        consumer.commit_error = None

        worker._commit()

        self.assertEqual(worker._consecutive_commit_failures, 0)


class ShutdownTests(WorkerTestCase):
    def test_close_releases_the_consumer_and_the_connection(self) -> None:
        consumer = FakeConsumer([])
        database = FakeDatabase()

        self._worker(consumer, database).close()

        self.assertTrue(consumer.closed)
        self.assertTrue(database.closed)

    def test_close_still_frees_the_connection_when_the_consumer_fails(self) -> None:
        consumer = FakeConsumer([])
        consumer.close = mock.Mock(side_effect=KafkaError("already gone"))
        database = FakeDatabase()

        self._worker(consumer, database).close()

        self.assertTrue(database.closed)


class HeartbeatTests(WorkerTestCase):
    def test_the_heartbeat_is_written_before_the_first_poll(self) -> None:
        """An idle pipeline must not read as a hung one."""

        import tempfile
        from pathlib import Path

        from storage_consumer.heartbeat import Heartbeat
        from storage_consumer.persistence.application_resolver import (
            ApplicationResolver,
        )

        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        heartbeat = Heartbeat(directory.name, "app-stats", interval_seconds=60.0)
        consumer = FakeConsumer([])
        worker = Worker(
            spec=self.spec,
            settings=self.settings,
            consumer=consumer,
            database=FakeDatabase(),  # type: ignore[arg-type]
            resolver=ApplicationResolver(ttl_seconds=300.0),
            dead_letters=self.dead_letters,  # type: ignore[arg-type]
            stop_event=self.stop,
            heartbeat=heartbeat,
        )

        self._run_once(consumer, worker)

        self.assertTrue(Path(heartbeat.path).exists())


class BatchLoggingTests(WorkerTestCase):
    def test_the_offset_range_is_reported_per_partition(self) -> None:
        batches = {
            FakeTopicPartition("app-stats", 3): [
                FakeRecord(_payload(), offset=10),
                FakeRecord(_payload(), offset=14),
            ]
        }

        described = Worker._describe_partitions(batches)

        self.assertEqual(described, "3:10-14")

    def test_an_empty_batch_describes_itself_as_none(self) -> None:
        self.assertEqual(Worker._describe_partitions({}), "none")


class UtcTests(unittest.TestCase):
    def test_the_fixture_payload_is_offset_aware(self) -> None:
        """Guards the fixtures the rest of this module relies on."""

        event = decode_app_stats(app_stats_payload())

        self.assertEqual(
            event.crawled_at,
            datetime(2026, 9, 7, 14, 20, 11, 123456, tzinfo=timezone.utc),
        )


if __name__ == "__main__":
    unittest.main()
