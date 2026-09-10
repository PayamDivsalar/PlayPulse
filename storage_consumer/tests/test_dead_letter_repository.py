"""Unit tests for dead-letter payload clamping and log volume.

The log-volume tests exist because of a real failure seen on first deployment:
a consumer group reset onto a backlog in an older wire format rejected every
message in a 500-record batch, and one warning per message buried every other
line in the container log.
"""

from __future__ import annotations

import logging
import unittest

from storage_consumer.events import RecordCoordinates, RejectedRecord
from storage_consumer.persistence.dead_letter_repository import (
    MAX_LOGGED_PER_BATCH,
    MAX_PAYLOAD_BYTES,
    DeadLetterRepository,
)


class FakeConnection:
    # execute_values reads the connection encoding to build its statement.
    encoding = "UTF8"


class FakeCursor:
    """The surface ``execute_values`` needs, without a database."""

    def __init__(self) -> None:
        self.connection = FakeConnection()
        self.statements: list[bytes] = []

    def execute(self, statement: bytes, *args: object) -> None:
        self.statements.append(statement)

    def fetchall(self) -> list:
        return []

    def mogrify(self, template: bytes, args: object) -> bytes:
        return str(args).encode("utf-8")


def _rejected(offset: int, *, reason: str = "boom", payload: bytes | None = b"{}"):
    return RejectedRecord(
        coordinates=RecordCoordinates(
            topic="reviews", partition=0, offset=offset, key="com.whatsapp",
            payload=payload,
        ),
        reason=reason,
    )


class PayloadClampingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = DeadLetterRepository()

    def test_a_normal_payload_is_stored_whole(self) -> None:
        payload, truncated = self.repository._clamp(b'{"a": 1}')

        self.assertFalse(truncated)
        self.assertEqual(payload, b'{"a": 1}')

    def test_a_missing_payload_stays_null(self) -> None:
        payload, truncated = self.repository._clamp(None)

        self.assertIsNone(payload)
        self.assertFalse(truncated)

    def test_an_oversized_payload_is_cut_to_the_limit(self) -> None:
        payload, truncated = self.repository._clamp(b"x" * (MAX_PAYLOAD_BYTES + 1))

        self.assertTrue(truncated)
        self.assertEqual(len(payload or b""), MAX_PAYLOAD_BYTES)

    def test_a_payload_exactly_at_the_limit_is_not_truncated(self) -> None:
        _, truncated = self.repository._clamp(b"x" * MAX_PAYLOAD_BYTES)

        self.assertFalse(truncated)

    def test_truncation_is_recorded_in_the_reason(self) -> None:
        """Otherwise the stored bytes look like the whole message."""

        row = self.repository._row(
            _rejected(1, payload=b"x" * (MAX_PAYLOAD_BYTES * 2))
        )

        self.assertIn("truncated", row[5])


class LogVolumeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = DeadLetterRepository()
        self.cursor = FakeCursor()

    def test_a_small_batch_logs_every_rejection(self) -> None:
        rejected = [_rejected(offset) for offset in range(3)]

        with self.assertLogs(
            "storage_consumer.persistence.dead_letter_repository", logging.WARNING
        ) as captured:
            self.repository.record(self.cursor, rejected)

        self.assertEqual(len(captured.records), 3)

    def test_a_mass_rejection_does_not_log_one_line_per_message(self) -> None:
        rejected = [_rejected(offset) for offset in range(500)]

        with self.assertLogs(
            "storage_consumer.persistence.dead_letter_repository", logging.WARNING
        ) as captured:
            self.repository.record(self.cursor, rejected)

        self.assertEqual(len(captured.records), MAX_LOGGED_PER_BATCH + 1)

    def test_the_summary_reports_the_true_total(self) -> None:
        rejected = [_rejected(offset) for offset in range(500)]

        with self.assertLogs(
            "storage_consumer.persistence.dead_letter_repository", logging.WARNING
        ) as captured:
            self.repository.record(self.cursor, rejected)

        summary = captured.output[-1]
        self.assertIn("500", summary)
        self.assertIn("dead_letter_events", summary)

    def test_the_summary_groups_by_reason(self) -> None:
        """A contract change reads as one problem, not hundreds of incidents."""

        rejected = [
            _rejected(offset, reason="Required field 'package_name' is missing")
            for offset in range(50)
        ] + [
            _rejected(offset + 50, reason="Message body is not valid JSON")
            for offset in range(10)
        ]

        with self.assertLogs(
            "storage_consumer.persistence.dead_letter_repository", logging.WARNING
        ) as captured:
            self.repository.record(self.cursor, rejected)

        summary = captured.output[-1]
        self.assertIn("package_name", summary)
        self.assertIn("not valid JSON", summary)

    def test_an_empty_list_writes_and_logs_nothing(self) -> None:
        self.assertEqual(self.repository.record(self.cursor, []), 0)
        self.assertEqual(self.cursor.statements, [])


if __name__ == "__main__":
    unittest.main()
