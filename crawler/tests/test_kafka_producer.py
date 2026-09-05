"""Tests for the crawler Kafka producer."""

from __future__ import annotations

import json
import time
import unittest
import uuid
from unittest.mock import MagicMock, patch

import pytest
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from crawler import config
from crawler.kafka_producer import CrawlerKafkaProducer


class CrawlerKafkaProducerTests(unittest.TestCase):
    def _build_producer(self) -> tuple[CrawlerKafkaProducer, MagicMock]:
        """Build a producer whose underlying KafkaProducer is mocked."""

        with patch("crawler.kafka_producer.KafkaProducer") as producer_cls:
            producer = CrawlerKafkaProducer(bootstrap_servers="localhost:9092")
        underlying = producer_cls.return_value
        return producer, underlying

    def test_send_app_stats_uses_correct_topic_and_key(self) -> None:
        producer, underlying = self._build_producer()
        data = {"minInstalls": 100, "score": 4.5}

        producer.send_app_stats("com.example.app", data)

        underlying.send.assert_called_once_with(
            "app-stats",
            key="com.example.app",
            value=data,
        )

    def test_value_serializer_encodes_dict_as_utf8_json(self) -> None:
        with patch("crawler.kafka_producer.KafkaProducer") as producer_cls:
            CrawlerKafkaProducer(bootstrap_servers="localhost:9092")

        kwargs = producer_cls.call_args.kwargs
        value_serializer = kwargs["value_serializer"]
        key_serializer = kwargs["key_serializer"]

        payload = {"score": 4.5, "name": "کافه"}
        encoded = value_serializer(payload)

        self.assertIsInstance(encoded, bytes)
        self.assertEqual(json.loads(encoded.decode("utf-8")), payload)
        self.assertEqual(key_serializer("com.example.app"), b"com.example.app")

    def test_send_reviews_sends_one_message_per_review(self) -> None:
        producer, underlying = self._build_producer()
        reviews = [{"reviewId": "r1"}, {"reviewId": "r2"}]

        producer.send_reviews("com.example.app", reviews)

        self.assertEqual(underlying.send.call_count, 2)
        underlying.send.assert_any_call("reviews", key="com.example.app", value=reviews[0])
        underlying.send.assert_any_call("reviews", key="com.example.app", value=reviews[1])

    def test_send_app_stats_propagates_kafka_error(self) -> None:
        producer, underlying = self._build_producer()
        underlying.send.side_effect = KafkaError("broker down")

        with self.assertRaises(KafkaError):
            producer.send_app_stats("com.example.app", {"score": 4.5})

    def test_flush_propagates_kafka_error(self) -> None:
        producer, underlying = self._build_producer()
        underlying.flush.side_effect = KafkaError("flush failed")

        with self.assertRaises(KafkaError):
            producer.flush()


@pytest.mark.live
class CrawlerKafkaProducerLiveTests(unittest.TestCase):
    """Live Kafka tests. Require a running broker; do not run in automated CI."""

    def test_live_send_and_verify_app_stats(self) -> None:
        """Publish a live-test payload to app-stats and read it back.

        Depends on Kafka via ``KAFKA_BOOTSTRAP_SERVERS``. Do not run in CI.
        Uses a clearly-marked ``live-test-`` package name so it does not mix
        with real crawled apps.
        """

        bootstrap = config.get_kafka_bootstrap_servers()
        # Crawler/pytest run on the host; docker-internal DNS (kafka:29092) only
        # works inside the compose network. Fail fast with a clear hint.
        if "kafka:" in bootstrap.split(",")[0]:
            self.fail(
                "KAFKA_BOOTSTRAP_SERVERS is set to a docker-internal address "
                f"({bootstrap!r}). From the host use localhost:9092 "
                "(see crawler/README.md and .env.example)."
            )

        package_name = f"live-test-app-{uuid.uuid4().hex[:12]}"
        payload = {
            "minInstalls": 100,
            "score": 4.5,
            "ratings": 10,
            "reviews": 3,
            "updated": 1700000000,
            "version": "live-test-1.0.0",
            "adSupported": False,
        }

        producer = CrawlerKafkaProducer(bootstrap_servers=bootstrap)
        consumer = KafkaConsumer(
            "app-stats",
            bootstrap_servers=bootstrap.split(","),
            group_id=f"live-test-consumer-{uuid.uuid4().hex}",
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            key_deserializer=lambda key: key.decode("utf-8") if key else None,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        )

        try:
            # Join the group, then seek to the end so we only wait for the
            # message this test publishes (not the whole topic history).
            deadline_assign = time.monotonic() + 10.0
            while not consumer.assignment() and time.monotonic() < deadline_assign:
                consumer.poll(timeout_ms=200)
            self.assertTrue(
                consumer.assignment(),
                "Kafka consumer failed to receive a partition assignment",
            )
            consumer.seek_to_end()
            # seek_to_end is lazy until the next poll/position call
            for partition in consumer.assignment():
                consumer.position(partition)

            producer.send_app_stats(package_name, payload)
            producer.flush()

            found = None
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                records = consumer.poll(timeout_ms=500)
                for batch in records.values():
                    for record in batch:
                        if record.key == package_name:
                            found = record.value
                            break
                    if found is not None:
                        break
                if found is not None:
                    break

            self.assertIsNotNone(
                found,
                f"Timed out waiting for app-stats message key={package_name!r}",
            )
            self.assertEqual(found, payload)
        finally:
            consumer.close()
            producer.close()
