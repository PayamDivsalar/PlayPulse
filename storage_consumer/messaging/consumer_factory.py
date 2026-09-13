"""Construct the ``KafkaConsumer`` for one pipeline.

Small module, but every setting in it is load-bearing, so they are grouped here
rather than scattered through the worker.
"""

from __future__ import annotations

import logging
import socket

from kafka import KafkaConsumer
from kafka.consumer.subscription_state import ConsumerRebalanceListener

from storage_consumer.config import Settings
from storage_consumer.messaging.pipelines import PipelineSpec

logger = logging.getLogger(__name__)


class LoggingRebalanceListener(ConsumerRebalanceListener):
    """Logs partition moves. Deliberately does no bookkeeping.

    A rebalance mid-batch needs no cleanup here, and that is a property of the
    loop's shape rather than luck: no state crosses a poll boundary, so an
    in-flight batch is simply abandoned unwritten and redelivered to whoever
    receives the partition. Committing offsets in ``on_partitions_revoked`` --
    the usual reflex -- would be actively wrong, because it could advance past
    records this worker never wrote.
    """

    def __init__(self, pipeline: str) -> None:
        self._pipeline = pipeline

    def on_partitions_revoked(self, revoked) -> None:
        if revoked:
            logger.info(
                "[%s] Partitions revoked: %s",
                self._pipeline,
                ", ".join(str(partition.partition) for partition in revoked),
            )

    def on_partitions_assigned(self, assigned) -> None:
        logger.info(
            "[%s] Partitions assigned: %s",
            self._pipeline,
            ", ".join(str(partition.partition) for partition in assigned) or "none",
        )


def create_consumer(spec: PipelineSpec, settings: Settings) -> KafkaConsumer:
    """Build a consumer for one topic, subscribed and ready to poll."""

    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        group_id=spec.group_id,
        # Non-negotiable. Auto-commit advances offsets on a timer, which can
        # acknowledge records that have not been written yet: silent data loss
        # on any crash. Offsets here move only after Postgres has committed.
        enable_auto_commit=False,
        # A storage subsystem joining a topic for the first time must ingest
        # what has already been produced, not skip to the end and lose it.
        auto_offset_reset=settings.auto_offset_reset,
        max_poll_records=spec.max_poll_records,
        # Must comfortably exceed the worst-case batch time including the
        # database retry window, or the broker evicts us mid-batch and the
        # group livelocks on rebalances. Settings validates the relationship.
        max_poll_interval_ms=settings.max_poll_interval_ms,
        session_timeout_ms=settings.session_timeout_ms,
        heartbeat_interval_ms=settings.heartbeat_interval_ms,
        # Identifies this pipeline in the broker's logs and in kafka-ui.
        client_id=f"{socket.gethostname()}:{spec.name}",
        # NO value_deserializer, and this is the easiest thing here to get
        # wrong. Passing json.loads makes poll() itself raise on a malformed
        # record, from inside the client, with no way to identify or skip the
        # offending offset -- the pipeline simply stops. Keeping raw bytes lets
        # decoders.py route the bad record to dead_letter_events and move on.
    )
    consumer.subscribe([spec.topic], listener=LoggingRebalanceListener(spec.name))
    logger.info(
        "[%s] Subscribed to topic %s as group %s "
        "(max_poll_records=%s, idempotency key %s)",
        spec.name,
        spec.topic,
        spec.group_id,
        spec.max_poll_records,
        spec.conflict_key,
    )
    return consumer
