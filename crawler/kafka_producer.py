"""Kafka producer for crawler events."""

from __future__ import annotations

import json
import logging
from typing import Any, Callable

from kafka import KafkaProducer
from kafka.errors import KafkaError

from crawler.config import Settings
from crawler.retry_policy import run_residual_retry, with_retry

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
    2. Application-level retries on Kafka errors after layer 1 fails
       (``with_retry`` for stats; ``run_residual_retry`` for reviews).
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
        is exhausted. Delivery uses two phases so the shared producer can batch:
        enqueue the current pending set with ``send``, then ``get`` every future
        for *this* call (not ``flush``, which would wait on other workers).

        Application retry uses ``run_residual_retry``: every future in an attempt
        is awaited and classified; only reviews that failed confirmation are
        retried. The method still fails closed — if any review remains after the
        retry budget, the last ``KafkaError`` is raised so the crawl cycle can
        mark reviews unsuccessful. Downstream consumers should upsert by
        ``reviewId``.
        """

        try:
            run_residual_retry(
                lambda pending: self._publish_reviews_attempt(package_name, pending),
                reviews,
                max_retries=self._settings.kafka_send_retry_max_attempts,
                base_delay_seconds=self._settings.kafka_send_retry_base_delay_seconds,
                description=f"Kafka reviews package_name={package_name}",
            )
        except KafkaError:
            logger.error(
                "All Kafka send retries exhausted (producer-internal and "
                "application-level) for reviews package_name=%s",
                package_name,
                exc_info=True,
            )
            raise

    def _publish_reviews_attempt(
        self, package_name: str, reviews: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], KafkaError | None]:
        """Enqueue ``reviews``, await all futures, return residual failures.

        Every successfully enqueued future is awaited even if earlier confirms
        fail, so already-acked reviews are not unnecessarily retried.
        """

        enqueued: list[tuple[dict[str, Any], Any]] = []
        failed: list[dict[str, Any]] = []
        last_error: KafkaError | None = None

        # Phase 1: enqueue all pending messages so KafkaProducer can batch them.
        for review in reviews:
            try:
                future = self._producer.send(
                    _REVIEWS_TOPIC, key=package_name, value=review
                )
            except KafkaError as exc:
                failed.append(review)
                last_error = exc
            else:
                enqueued.append((review, future))

        # Phase 2: classify every future from this attempt (worker-local only).
        for review, future in enqueued:
            try:
                future.get(timeout=self._get_timeout_seconds)
            except KafkaError as exc:
                failed.append(review)
                last_error = exc

        return failed, last_error

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
