"""The poll / write / commit loop, shared by all three pipelines.

The loop is short on purpose. Three properties fall out of its shape, and all
three are things you should be able to confirm by reading it:

* **Write before commit.** Offsets advance only after Postgres has durably
  committed, so nothing is ever lost. That gives at-least-once, and the
  ``ON CONFLICT`` clauses make the duplicates harmless.
* **One batch is one transaction.** Data rows and dead-letter rows commit
  together, so a crash can never leave a poisoned message
  recorded-but-unadvanced or advanced-but-unrecorded.
* **No state crosses a poll boundary.** A rebalance mid-batch is therefore
  automatically safe: the in-flight batch is abandoned unwritten and
  redelivered to whoever gets the partition.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from time import perf_counter
from typing import Any

from kafka.errors import CommitFailedError, KafkaError

from storage_consumer.batching import (
    bind_application_ids,
    decode_batch,
    dedupe_by_key,
)
from storage_consumer.config import Settings
from storage_consumer.heartbeat import Heartbeat
from storage_consumer.persistence.application_resolver import ApplicationResolver
from storage_consumer.persistence.database import Database
from storage_consumer.persistence.dead_letter_repository import DeadLetterRepository
from storage_consumer.pipelines import PipelineSpec
from storage_consumer.retry_policy import TRANSIENT_DB_EXCEPTIONS, with_retry

logger = logging.getLogger(__name__)

# How many offset commits may fail back to back before the worker gives up.
# A commit failure on its own is benign -- the rows are already durable and the
# redelivery is idempotent -- but if commits never succeed the pipeline
# reprocesses the same records forever while lag never falls. That livelock is
# far harder to spot than a crash, so it is turned into one.
MAX_CONSECUTIVE_COMMIT_FAILURES = 5


@dataclass(frozen=True, slots=True)
class BatchOutcome:
    """One line's worth of what a batch did."""

    records: int = 0
    decoded: int = 0
    deduplicated: int = 0
    inserted: int = 0
    updated: int = 0
    skipped: int = 0
    dead_lettered: int = 0
    duration_ms: float = 0.0


class Worker:
    """Runs one pipeline until the shared stop event is set.

    Owns its ``KafkaConsumer`` and its ``Database`` outright. Neither is
    thread-safe, and strict one-per-thread ownership is exactly the usage both
    libraries document as safe -- which is why the whole subsystem needs no
    locks. The only things shared with the other pipelines are the immutable
    ``Settings`` and the stop event.
    """

    def __init__(
        self,
        *,
        spec: PipelineSpec,
        settings: Settings,
        consumer: Any,
        database: Database,
        resolver: ApplicationResolver,
        dead_letters: DeadLetterRepository,
        stop_event: threading.Event,
        heartbeat: Heartbeat | None = None,
    ) -> None:
        self._spec = spec
        self._settings = settings
        self._consumer = consumer
        self._database = database
        self._resolver = resolver
        self._dead_letters = dead_letters
        self._stop = stop_event
        self._heartbeat = heartbeat
        self._consecutive_commit_failures = 0
        # Bound once rather than per batch, so the decorator is not rebuilt
        # hundreds of times a minute.
        self._write_batch_with_retry = with_retry(
            max_retries=settings.db_retry_max_attempts,
            base_delay_seconds=settings.db_retry_base_delay_seconds,
            exceptions=TRANSIENT_DB_EXCEPTIONS,
            on_retry=self._recover_connection,
        )(self._write_batch)

    def run(self) -> None:
        """Poll, write and commit until asked to stop.

        Returns normally only on a clean shutdown. Any exception escaping here
        is fatal for the whole process by design: a container that keeps
        running with one of its three pipelines silently dead is the worst
        possible outcome, so it is designed out.
        """

        logger.info("[%s] Worker started.", self._spec.name)
        if self._heartbeat is not None:
            self._heartbeat.touch(force=True)

        while not self._stop.is_set():
            batches = self._consumer.poll(
                timeout_ms=self._spec.poll_timeout_ms,
                max_records=self._spec.max_poll_records,
            )
            if self._heartbeat is not None:
                self._heartbeat.touch()
            if not batches:
                continue

            records = [record for batch in batches.values() for record in batch]
            outcome = self._write_batch_with_retry(records)
            # Only now, with the rows durable, may the offsets move.
            self._commit()
            self._log_batch(batches, outcome)

        logger.info("[%s] Stop requested; worker loop finished.", self._spec.name)

    def close(self) -> None:
        """Release the consumer and the connection.

        Closing the consumer sends a ``LeaveGroup``, which lets the broker
        rebalance immediately instead of waiting out a full session timeout.
        On a rolling restart that is the difference between seconds and most
        of a minute of stalled ingestion.
        """

        try:
            self._consumer.close()
        except KafkaError:
            logger.warning(
                "[%s] Kafka consumer did not close cleanly.",
                self._spec.name,
                exc_info=True,
            )
        self._database.close()
        if self._heartbeat is not None:
            self._heartbeat.remove()

    def _write_batch(self, records: list[Any]) -> BatchOutcome:
        """Decode, resolve, de-duplicate and write one batch, in one transaction.

        Retried as a unit on a transient database fault. That is safe to do
        because it is a pure function of ``records``: nothing is carried over
        from the failed attempt, and the writes are idempotent even if the
        previous attempt had in fact committed before the connection dropped.
        """

        started = perf_counter()
        decoded, rejected = decode_batch(records, self._spec.decoder)

        with self._database.transaction() as cursor:
            application_ids = self._resolver.resolve(
                cursor, {record.event.package_name for record in decoded}
            )
            bound, unknown = bind_application_ids(decoded, application_ids)
            rejected = rejected + unknown

            # Same transaction as the data below: a rejection cannot be lost
            # while its offset advances, and cannot be recorded twice if the
            # batch is retried.
            self._dead_letters.record(cursor, rejected)

            rows = dedupe_by_key(bound)
            write = self._spec.repository.upsert(cursor, rows)

        return BatchOutcome(
            records=len(records),
            decoded=len(decoded),
            deduplicated=len(bound) - len(rows),
            inserted=write.inserted,
            updated=write.updated,
            skipped=write.skipped,
            dead_lettered=len(rejected),
            duration_ms=(perf_counter() - started) * 1000.0,
        )

    def _recover_connection(self, exc: BaseException) -> None:
        """Replace the connection before the next attempt.

        Once the server has gone away the connection object is dead, and every
        remaining attempt on it would fail instantly with ``InterfaceError``,
        burning the whole retry budget in microseconds instead of waiting for
        Postgres to come back.
        """

        logger.warning(
            "[%s] Replacing the database connection after: %s", self._spec.name, exc
        )
        try:
            self._database.reconnect()
        except Exception:
            # The next attempt will try to connect again anyway; failing here
            # would turn a retryable fault into a fatal one.
            logger.warning(
                "[%s] Reconnect failed; will retry with the batch.",
                self._spec.name,
                exc_info=True,
            )

    def _commit(self) -> None:
        """Advance the offsets, tolerating the benign failures.

        A failed commit is not data loss: the rows are already in Postgres, and
        the redelivery it causes is a no-op against the unique constraints. So
        this logs and carries on rather than dying -- up to the point where it
        is clearly not recovering.
        """

        try:
            self._consumer.commit()
        except CommitFailedError:
            # The group rebalanced while we were writing. Our partitions belong
            # to someone else now, and they will redeliver the batch we just
            # wrote. Retrying the commit cannot succeed.
            self._consecutive_commit_failures += 1
            logger.warning(
                "[%s] Offset commit rejected: the group rebalanced during the "
                "batch. The records will be redelivered and re-written "
                "idempotently.",
                self._spec.name,
            )
        except KafkaError as exc:
            self._consecutive_commit_failures += 1
            logger.warning(
                "[%s] Offset commit failed (%s). The batch will be redelivered.",
                self._spec.name,
                exc,
            )
        else:
            self._consecutive_commit_failures = 0
            return

        if self._consecutive_commit_failures >= MAX_CONSECUTIVE_COMMIT_FAILURES:
            raise RuntimeError(
                f"{self._consecutive_commit_failures} consecutive offset commits "
                f"failed on pipeline {self._spec.name}. The pipeline is "
                "reprocessing the same records without ever advancing; exiting "
                "so the container restarts."
            )

    def _log_batch(self, batches: dict[Any, Any], outcome: BatchOutcome) -> None:
        """One INFO line per non-empty batch.

        Reports the insert/update split rather than a single "written" count,
        because that is what tells an operator at a glance whether they are
        watching a backfill or steady state.
        """

        logger.info(
            "[%s] %s",
            self._spec.name,
            " ".join(
                (
                    f"partitions={self._describe_partitions(batches)}",
                    f"records={outcome.records}",
                    f"decoded={outcome.decoded}",
                    f"inserted={outcome.inserted}",
                    f"updated={outcome.updated}",
                    f"skipped={outcome.skipped}",
                    f"deduplicated={outcome.deduplicated}",
                    f"dead_lettered={outcome.dead_lettered}",
                    f"duration_ms={outcome.duration_ms:.1f}",
                )
            ),
        )

    @staticmethod
    def _describe_partitions(batches: dict[Any, Any]) -> str:
        """Render the partitions and offset ranges this batch covered."""

        parts = []
        for partition, records in sorted(
            batches.items(), key=lambda item: item[0].partition
        ):
            if not records:
                continue
            parts.append(
                f"{partition.partition}:{records[0].offset}-{records[-1].offset}"
            )
        return ",".join(parts) or "none"
