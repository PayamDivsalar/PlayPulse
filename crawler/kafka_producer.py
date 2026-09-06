"""Kafka producer for crawler events."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable

from kafka import KafkaProducer
from kafka.errors import KafkaError

from crawler.config import Settings
from crawler.retry_policy import with_retry

logger = logging.getLogger(__name__)

_APP_STATS_TOPIC = "app-stats"
_REVIEWS_TOPIC = "reviews"
_RETRYABLE_EXCEPTIONS: tuple[type[BaseException], ...] = (KafkaError,)


class CrawlerKafkaProducer:
    """Publish crawler output to Kafka topics.

    Synchronous producer, consistent with the project's threading-based (non
    asyncio) architecture. A single instance is safe to share across worker
    threads: ``KafkaProducer`` itself is thread-safe.

    Durability is layered:

    1. Producer-internal retries + ``acks='all'`` + idempotence (Settings).
    2. Application-level ``with_retry`` on Kafka errors after layer 1 fails.
    3. Final failure is re-raised so ``CrawlerService`` can skip that path.
    """

    def __init__(self, bootstrap_servers: str, settings: Settings) -> None:
        self._settings = settings
        self._get_timeout_seconds = settings.kafka_producer_request_timeout_ms / 1000.0
        self._retry: Callable = with_retry(
            max_retries=settings.kafka_send_retry_max_attempts,
            base_delay_seconds=settings.kafka_send_retry_base_delay_seconds,
            exceptions=_RETRYABLE_EXCEPTIONS,
        )
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(
                "utf-8"
            ),
            retries=settings.kafka_producer_retries,
            retry_backoff_ms=settings.kafka_producer_retry_backoff_ms,
            acks=settings.kafka_producer_acks,
            enable_idempotence=settings.kafka_producer_enable_idempotence,
            request_timeout_ms=settings.kafka_producer_request_timeout_ms,
            delivery_timeout_ms=settings.kafka_producer_delivery_timeout_ms,
        )

    def send_app_stats(self, package_name: str, data: dict[str, Any]) -> None:
        """Send one app payload to the app-stats topic.

        The Kafka key is the package name so all events for the same app stay in
        the same partition, which preserves per-app ordering downstream.

        Unlike ``PlayStoreClient`` (which retries HTTP / ``RequestException``),
        this retries on ``KafkaError`` after the producer-internal retry budget
        is exhausted. With ``enable_idempotence=True``, library-internal retries
        do not create duplicate records; application-level retries remain safe
        for the common case where the prior attempt truly failed to land.
        """

        try:
            self._retry(self._send_app_stats_once)(package_name, data)
        except KafkaError:
            logger.error(
                "All Kafka send retries exhausted (producer-internal and "
                "application-level) for app stats package_name=%s",
                package_name,
                exc_info=True,
            )
            raise

    def _send_app_stats_once(self, package_name: str, data: dict[str, Any]) -> None:
        future = self._producer.send(_APP_STATS_TOPIC, key=package_name, value=data)
        self._await_deliveries([future])

    def send_reviews(self, package_name: str, reviews: list[dict[str, Any]]) -> None:
        """Send each review to the reviews topic as an individual message.

        One message per review (rather than a single message holding the whole
        list) keeps the future Storage Consumer simple: a single review is easy
        to ingest, upsert by ``reviewId``, replay, and validate without any
        batch-unpacking semantics. The key is the package name so an app's
        reviews stay in one partition.

        Unlike ``PlayStoreClient`` (which retries HTTP / ``RequestException``),
        this retries on ``KafkaError`` after the producer-internal retry budget
        is exhausted. With ``enable_idempotence=True``, library-internal retries
        do not create duplicate records; application-level retries remain safe
        for the common case where the prior attempt truly failed to land.
        Downstream consumers should still upsert by ``reviewId``.

        Delivery is confirmed in two phases so the shared producer can batch:
        enqueue every review with ``send``, then ``get`` only on this call's
        futures (not ``flush``, which would wait on other workers' messages).
        Application retry still re-runs the whole batch, not a single review.
        """

        try:
            self._retry(self._send_reviews_once)(package_name, reviews)
        except KafkaError:
            logger.error(
                "All Kafka send retries exhausted (producer-internal and "
                "application-level) for reviews package_name=%s",
                package_name,
                exc_info=True,
            )
            raise

    def _send_reviews_once(
        self, package_name: str, reviews: list[dict[str, Any]]
    ) -> None:
        # Phase 1: enqueue all messages so KafkaProducer can batch them.
        futures = [
            self._producer.send(_REVIEWS_TOPIC, key=package_name, value=review)
            for review in reviews
        ]
        # Phase 2: confirm only *this* batch's futures (worker-local attribution).
        self._await_deliveries(futures)

    def _await_deliveries(self, futures: list[Any]) -> None:
        """Block until each future succeeds or raises a ``KafkaError``."""

        for future in futures:
            future.get(timeout=self._get_timeout_seconds)

    def flush(self) -> None:
        """Block until all buffered messages are sent.

        Call before shutdown so pending messages are not lost. Errors are
        logged and re-raised rather than swallowed, since a failed flush means
        data loss.
        """

        try:
            self._producer.flush()
        except KafkaError:
            logger.error("Failed to flush Kafka producer.", exc_info=True)
            raise

    def close(self) -> None:
        """Flush and close the underlying Kafka producer."""

        self.flush()
        self._producer.close()
