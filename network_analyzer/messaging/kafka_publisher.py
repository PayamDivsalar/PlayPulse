"""Kafka producer for network metric events."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable

from kafka import KafkaProducer
from kafka.errors import KafkaError

from network_analyzer.config import Settings
from network_analyzer.common.retry_policy import with_retry

logger = logging.getLogger(__name__)

NETWORK_METRICS_TOPIC = "network-metrics"

_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (KafkaError,)


class NetworkMetricsPublisher:
    """Publish analysis results to the ``network-metrics`` topic.

    Durability is layered the same way as the crawler's producer:

    1. Producer-internal retries with ``acks='all'`` and idempotence.
    2. Application-level retries on ``KafkaError`` once layer 1 is exhausted.
    3. Final failure is re-raised, so a batch run can record the file as failed
       rather than reporting a success it did not achieve.

    Every send is confirmed before returning. The analyzer is an interactive
    batch job, so a publish failure has to surface on the operator's terminal
    while they are still looking at it -- not silently at interpreter exit.

    The underlying producer is created on first publish rather than in
    ``__init__``. A capture rejected by the registry check is never published,
    and building a producer for it would mean opening a broker connection, and
    then shutting it down, for nothing.
    """

    def __init__(self, bootstrap_servers: str, settings: Settings) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._settings = settings
        self._get_timeout_seconds = settings.kafka_producer_request_timeout_ms / 1000.0
        # Bound shutdown by the delivery budget: if a record could not be
        # delivered within the time the producer itself was given, waiting
        # longer at exit achieves nothing.
        self._shutdown_timeout_seconds = (
            settings.kafka_producer_delivery_timeout_ms / 1000.0
        )
        retry = with_retry(
            max_retries=settings.kafka_send_retry_max_attempts,
            base_delay_seconds=settings.kafka_send_retry_base_delay_seconds,
            exceptions=_RETRYABLE_EXCEPTIONS,
        )
        # Bind once at construction so each publish does not rebuild a wrapper.
        self._publish_with_retry: Callable[[str, dict[str, Any]], None] = retry(
            self._publish_once
        )
        self._producer: KafkaProducer | None = None

    def publish(self, package_name: str, message: dict[str, Any]) -> None:
        """Send one analysis result and block until the broker confirms it.

        The key is the package name, so every record for one application stays
        in one partition and keeps its relative order downstream.

        An application-level retry may duplicate a message that actually landed
        but whose acknowledgement was lost. The ``analysis_id`` field exists to
        make that harmless: the consumer treats it as an idempotency key.

        Raises:
            KafkaError: if delivery cannot be confirmed after all retries.
        """

        try:
            self._publish_with_retry(package_name, message)
        except KafkaError as exc:
            logger.error(
                "All Kafka send retries exhausted (producer-internal and "
                "application-level) for package_name=%s analysis_id=%s: %s",
                package_name,
                message.get("analysis_id"),
                exc,
            )
            logger.debug("Kafka publish failure detail.", exc_info=True)
            raise

        logger.info(
            "Published network metrics to %s: package_name=%s analysis_id=%s",
            NETWORK_METRICS_TOPIC,
            package_name,
            message.get("analysis_id"),
        )

    def _publish_once(self, package_name: str, message: dict[str, Any]) -> None:
        producer = self._ensure_producer()
        future = producer.send(
            NETWORK_METRICS_TOPIC, key=package_name, value=message
        )
        future.get(timeout=self._get_timeout_seconds)

    def _ensure_producer(self) -> KafkaProducer:
        if self._producer is None:
            settings = self._settings
            self._producer = KafkaProducer(
                bootstrap_servers=self._bootstrap_servers.split(","),
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda value: json.dumps(
                    value, ensure_ascii=False
                ).encode("utf-8"),
                retries=settings.kafka_producer_retries,
                retry_backoff_ms=settings.kafka_producer_retry_backoff_ms,
                acks=settings.kafka_producer_acks,
                enable_idempotence=settings.kafka_producer_enable_idempotence,
                request_timeout_ms=settings.kafka_producer_request_timeout_ms,
                delivery_timeout_ms=settings.kafka_producer_delivery_timeout_ms,
            )
        return self._producer

    def flush(self) -> None:
        """Block until all buffered messages are sent.

        A no-op when nothing was ever published. Errors are logged and
        re-raised rather than swallowed, since a failed flush means data loss.
        """

        if self._producer is None:
            return
        try:
            self._producer.flush(timeout=self._shutdown_timeout_seconds)
        except KafkaError:
            logger.error("Failed to flush Kafka producer.", exc_info=True)
            raise

    def close(self) -> None:
        """Flush and close the underlying Kafka producer.

        The close timeout is bounded on purpose. ``KafkaProducer.close()``
        defaults to waiting indefinitely on its sender thread, and an
        idempotent producer that was created but never used can sit there for
        the full metadata refresh interval -- five minutes of an operator
        staring at a stalled terminal.
        """

        if self._producer is None:
            return
        try:
            self.flush()
        finally:
            self._producer.close(timeout=self._shutdown_timeout_seconds)
            self._producer = None
