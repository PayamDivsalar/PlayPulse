"""Kafka producer for crawler events."""

from __future__ import annotations

import json
import logging
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

_APP_STATS_TOPIC = "app-stats"
_REVIEWS_TOPIC = "reviews"


class CrawlerKafkaProducer:
    """Publish crawler output to Kafka topics.

    Synchronous producer, consistent with the project's threading-based (non
    asyncio) architecture. A single instance is safe to share across worker
    threads: ``KafkaProducer`` itself is thread-safe.
    """

    def __init__(self, bootstrap_servers: str) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers.split(","),
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode(
                "utf-8"
            ),
        )

    def send_app_stats(self, package_name: str, data: dict[str, Any]) -> None:
        """Send one app payload to the app-stats topic.

        The Kafka key is the package name so all events for the same app stay in
        the same partition, which preserves per-app ordering downstream.
        """

        try:
            self._producer.send(_APP_STATS_TOPIC, key=package_name, value=data)
        except KafkaError:
            logger.error(
                "Failed to send app stats for package_name=%s",
                package_name,
                exc_info=True,
            )
            raise

    def send_reviews(self, package_name: str, reviews: list[dict[str, Any]]) -> None:
        """Send each review to the reviews topic as an individual message.

        One message per review (rather than a single message holding the whole
        list) keeps the future Storage Consumer simple: a single review is easy
        to ingest, upsert by ``reviewId``, replay, and validate without any
        batch-unpacking semantics. The key is the package name so an app's
        reviews stay in one partition.
        """

        try:
            for review in reviews:
                self._producer.send(_REVIEWS_TOPIC, key=package_name, value=review)
        except KafkaError:
            logger.error(
                "Failed to send reviews for package_name=%s",
                package_name,
                exc_info=True,
            )
            raise

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
