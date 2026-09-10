"""Writer for ``dead_letter_events``."""

from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Sequence
from typing import Any

import psycopg2
from psycopg2.extras import execute_values

from storage_consumer.events import RejectedRecord
from storage_consumer.persistence.repository import PAGE_SIZE

logger = logging.getLogger(__name__)

# Enough to see what arrived without letting one absurd message bloat the
# table. The three real payloads are a few hundred bytes each, so a body over
# this size is itself evidence that something is wrong.
MAX_PAYLOAD_BYTES = 64 * 1024

# How many rejections get an individual log line before the rest are summarised.
#
# Sized for the case that actually happens: a producer changes a field name, or
# a group is reset onto a backlog in an older format, and every record in a
# 500-message batch is rejected at once. One line each would put 500 warnings
# per batch into the container logs and bury everything else. The full detail
# for every rejection is in the table, which is the point of having it.
MAX_LOGGED_PER_BATCH = 5

_TRUNCATION_NOTE = " [payload truncated]"


class DeadLetterRepository:
    """Records messages this consumer will never be able to store.

    Not a ``Repository`` subclass: it writes Kafka coordinates rather than
    bound events, and its rows are diagnostics rather than data. It shares the
    important part anyway -- the caller's cursor -- so a rejection commits in
    the same transaction as the batch that produced it. That is the property
    a Kafka DLQ topic could not give without transactions: it is impossible
    for the offset to advance past a bad message whose record was lost.
    """

    _STATEMENT = """
        INSERT INTO dead_letter_events
            (topic, "partition", kafka_offset, kafka_key, raw_payload, error_reason)
        VALUES %s
        ON CONFLICT (topic, "partition", kafka_offset) DO NOTHING
    """

    def record(self, cursor: Any, rejected: Sequence[RejectedRecord]) -> int:
        """Write every rejection. Returns how many rows were submitted.

        ``DO NOTHING`` because a Kafka record's coordinates are its identity:
        replaying a partition after an offset reset re-rejects the same bad
        messages, and that must not multiply the rows.
        """

        if not rejected:
            return 0

        self._log(rejected)
        execute_values(
            cursor,
            self._STATEMENT,
            [self._row(record) for record in rejected],
            page_size=PAGE_SIZE,
        )
        return len(rejected)

    def _log(self, rejected: Sequence[RejectedRecord]) -> None:
        """Detail for the first few, then a count and a breakdown by reason."""

        for record in rejected[:MAX_LOGGED_PER_BATCH]:
            coordinates = record.coordinates
            logger.warning(
                "Dead-lettering %s[%s]@%s key=%s: %s",
                coordinates.topic,
                coordinates.partition,
                coordinates.offset,
                coordinates.key,
                record.reason,
            )

        remaining = len(rejected) - MAX_LOGGED_PER_BATCH
        if remaining <= 0:
            return

        # Grouped by reason so a mass rejection reads as one problem rather
        # than as hundreds of separate incidents.
        counts = Counter(record.reason for record in rejected)
        logger.warning(
            "Dead-lettered %s more message(s) from %s in this batch. Reasons: %s. "
            "Full detail for all %s is in dead_letter_events.",
            remaining,
            rejected[0].coordinates.topic,
            "; ".join(
                f"{reason} (x{count})" for reason, count in counts.most_common(5)
            ),
            len(rejected),
        )

    def _row(self, record: RejectedRecord) -> tuple[Any, ...]:
        coordinates = record.coordinates
        payload, truncated = self._clamp(coordinates.payload)
        return (
            coordinates.topic,
            coordinates.partition,
            coordinates.offset,
            coordinates.key,
            # Binary, not a str: the column is BYTEA precisely so that a
            # message rejected for being invalid UTF-8 can still be stored.
            None if payload is None else psycopg2.Binary(payload),
            record.reason + (_TRUNCATION_NOTE if truncated else ""),
        )

    def _clamp(self, payload: bytes | None) -> tuple[bytes | None, bool]:
        """Cut an oversized payload down, reporting whether it was cut."""

        if payload is None or len(payload) <= MAX_PAYLOAD_BYTES:
            return payload, False
        return payload[:MAX_PAYLOAD_BYTES], True
