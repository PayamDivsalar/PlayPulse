"""Tests for the retry helper and, more importantly, its retryable set.

The membership assertions matter as much as the mechanics: retrying a
permanent error wedges a partition forever, and dead-lettering a transient one
throws away good data because the database happened to be restarting.
"""

from __future__ import annotations

import unittest
from unittest import mock

import psycopg2
from kafka.errors import CommitFailedError, KafkaError

from storage_consumer.exceptions import MessageDecodeError
from storage_consumer.retry_policy import (
    TRANSIENT_DB_EXCEPTIONS,
    TRANSIENT_KAFKA_EXCEPTIONS,
    backoff_delay_seconds,
    with_retry,
)


class BackoffTests(unittest.TestCase):
    def test_delay_doubles_each_attempt(self) -> None:
        delays = [backoff_delay_seconds(attempt, 1.0) for attempt in range(4)]

        self.assertEqual(delays, [1.0, 2.0, 4.0, 8.0])


class RetryMechanicsTests(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch("storage_consumer.retry_policy.sleep")
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_successful_call_is_not_retried(self) -> None:
        calls = []

        @with_retry(max_retries=3)
        def succeed() -> str:
            calls.append(1)
            return "ok"

        self.assertEqual(succeed(), "ok")
        self.assertEqual(len(calls), 1)
        self.sleep.assert_not_called()

    def test_a_transient_failure_is_retried_until_it_succeeds(self) -> None:
        attempts = []

        @with_retry(max_retries=3, base_delay_seconds=0.01)
        def flaky() -> str:
            attempts.append(1)
            if len(attempts) < 3:
                raise psycopg2.OperationalError("server closed the connection")
            return "ok"

        self.assertEqual(flaky(), "ok")
        self.assertEqual(len(attempts), 3)

    def test_the_last_failure_propagates_once_the_budget_is_spent(self) -> None:
        @with_retry(max_retries=2, base_delay_seconds=0.01)
        def always_fails() -> None:
            raise psycopg2.OperationalError("down")

        with self.assertRaises(psycopg2.OperationalError):
            always_fails()

        self.assertEqual(self.sleep.call_count, 2)

    def test_an_unlisted_exception_is_not_retried(self) -> None:
        """Retrying a decode error would block the partition forever."""

        attempts = []

        @with_retry(max_retries=3)
        def bad_data() -> None:
            attempts.append(1)
            raise MessageDecodeError("missing review_id")

        with self.assertRaises(MessageDecodeError):
            bad_data()

        self.assertEqual(len(attempts), 1)

    def test_on_retry_runs_before_each_sleep(self) -> None:
        """This is where the batch path re-opens a dead connection."""

        recovered: list[BaseException] = []

        @with_retry(
            max_retries=2, base_delay_seconds=0.01, on_retry=recovered.append
        )
        def always_fails() -> None:
            raise psycopg2.OperationalError("gone")

        with self.assertRaises(psycopg2.OperationalError):
            always_fails()

        self.assertEqual(len(recovered), 2)

    def test_on_retry_does_not_run_after_the_final_failure(self) -> None:
        """Reconnecting on the way out would leave an unused open connection."""

        recovered: list[BaseException] = []

        @with_retry(max_retries=1, base_delay_seconds=0.01, on_retry=recovered.append)
        def always_fails() -> None:
            raise psycopg2.OperationalError("gone")

        with self.assertRaises(psycopg2.OperationalError):
            always_fails()

        self.assertEqual(len(recovered), 1)

    def test_zero_retries_calls_once(self) -> None:
        attempts = []

        @with_retry(max_retries=0)
        def once() -> None:
            attempts.append(1)
            raise psycopg2.OperationalError("down")

        with self.assertRaises(psycopg2.OperationalError):
            once()

        self.assertEqual(len(attempts), 1)


class RetryableSetTests(unittest.TestCase):
    def test_a_dropped_connection_is_transient(self) -> None:
        self.assertTrue(
            issubclass(psycopg2.OperationalError, TRANSIENT_DB_EXCEPTIONS)
        )

    def test_an_unusable_connection_object_is_transient(self) -> None:
        self.assertTrue(issubclass(psycopg2.InterfaceError, TRANSIENT_DB_EXCEPTIONS))

    def test_deadlocks_and_serialization_failures_are_transient(self) -> None:
        """Both are psycopg2 subclasses of OperationalError, and both retry clean."""

        for error in (
            psycopg2.errors.DeadlockDetected,
            psycopg2.errors.SerializationFailure,
        ):
            with self.subTest(error=error.__name__):
                self.assertTrue(issubclass(error, TRANSIENT_DB_EXCEPTIONS))

    def test_a_constraint_violation_is_not_transient(self) -> None:
        """Retrying it would loop forever on data that will never be accepted."""

        self.assertFalse(
            issubclass(psycopg2.errors.UniqueViolation, TRANSIENT_DB_EXCEPTIONS)
        )

    def test_a_missing_table_is_not_transient(self) -> None:
        """An unapplied migration is fatal, so the container restarts loudly."""

        self.assertFalse(
            issubclass(psycopg2.errors.UndefinedTable, TRANSIENT_DB_EXCEPTIONS)
        )

    def test_permanent_message_errors_are_not_transient(self) -> None:
        self.assertFalse(issubclass(MessageDecodeError, TRANSIENT_DB_EXCEPTIONS))

    def test_a_failed_commit_is_transient_for_kafka(self) -> None:
        self.assertTrue(issubclass(CommitFailedError, TRANSIENT_KAFKA_EXCEPTIONS))
        self.assertTrue(issubclass(KafkaError, TRANSIENT_KAFKA_EXCEPTIONS))


if __name__ == "__main__":
    unittest.main()
