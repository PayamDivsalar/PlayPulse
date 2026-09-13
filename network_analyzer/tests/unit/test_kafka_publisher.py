"""Tests for the network-metrics producer."""

from __future__ import annotations

import json
import unittest
from typing import Any
from unittest.mock import Mock, patch

from kafka.errors import KafkaError, KafkaTimeoutError

from network_analyzer.config import Settings
from network_analyzer.messaging.kafka_publisher import (
    NETWORK_METRICS_TOPIC,
    NetworkMetricsPublisher,
)

_MESSAGE = {
    "analysis_id": "8f14e45f-ea6a-4f2b-9c1d-2b3a5c7d9e01",
    "package_name": "com.whatsapp",
    "scenario": "UPLOAD",
    "rtt_handshake": 43.123,
    "retransmission_count": 7,
    "out_of_order_count": 1,
    "spurious_retransmission_count": 2,
    "zero_window_count": 0,
    "tcp_reset_count": 2,
    "bytes_transferred_total": 1040,
    "bytes_payload_total": 1000,
    "overhead_ratio": 0.038462,
    "source_pcap_filename": "com.whatsapp__upload__20260907T141500.pcap",
    "analyzed_at": "2026-09-07T14:20:11+00:00",
}


def _producer_with_successful_send() -> Mock:
    producer = Mock()
    future = Mock()
    future.get.return_value = Mock()
    producer.send.return_value = future
    return producer


class PublisherTestCase(unittest.TestCase):
    """Builds publishers whose ``KafkaProducer`` stays patched for the test.

    The patch has to outlive construction: the producer is now created lazily
    on first publish, not in ``__init__``.
    """

    def build_publisher(
        self, producer: Mock, **overrides: object
    ) -> NetworkMetricsPublisher:
        # Default to no application-level retries so a failing send raises at once.
        values: dict[str, object] = {
            "kafka_send_retry_max_attempts": 0,
            "kafka_send_retry_base_delay_seconds": 0.0,
        }
        values.update(overrides)
        settings = Settings.for_testing(**values)

        patcher = patch(
            "network_analyzer.messaging.kafka_publisher.KafkaProducer",
            return_value=producer,
        )
        patcher.start()
        self.addCleanup(patcher.stop)

        return NetworkMetricsPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers, settings=settings
        )


class PublishTests(PublisherTestCase):
    def test_sends_to_the_network_metrics_topic(self) -> None:
        producer = _producer_with_successful_send()
        publisher = self.build_publisher(producer)

        publisher.publish("com.whatsapp", _MESSAGE)

        topic = producer.send.call_args[0][0]
        self.assertEqual(topic, NETWORK_METRICS_TOPIC)
        self.assertEqual(NETWORK_METRICS_TOPIC, "network-metrics")

    def test_keys_the_message_by_package_name(self) -> None:
        producer = _producer_with_successful_send()
        publisher = self.build_publisher(producer)

        publisher.publish("com.whatsapp", _MESSAGE)

        self.assertEqual(producer.send.call_args.kwargs["key"], "com.whatsapp")

    def test_sends_the_message_as_the_value(self) -> None:
        producer = _producer_with_successful_send()
        publisher = self.build_publisher(producer)

        publisher.publish("com.whatsapp", _MESSAGE)

        self.assertEqual(producer.send.call_args.kwargs["value"], _MESSAGE)

    def test_waits_for_broker_confirmation(self) -> None:
        """A batch run must not report success for an unconfirmed send."""

        producer = _producer_with_successful_send()
        publisher = self.build_publisher(producer)

        publisher.publish("com.whatsapp", _MESSAGE)

        producer.send.return_value.get.assert_called_once()


def _producer_kwargs(bootstrap_servers: str = "broker:29092") -> dict[str, Any]:
    """Publish once through a patched producer class and return its kwargs."""

    settings = Settings.for_testing()
    with patch(
        "network_analyzer.messaging.kafka_publisher.KafkaProducer"
    ) as mock_producer_class:
        mock_producer_class.return_value = _producer_with_successful_send()
        publisher = NetworkMetricsPublisher(
            bootstrap_servers=bootstrap_servers, settings=settings
        )
        publisher.publish("com.whatsapp", _MESSAGE)
    return mock_producer_class.call_args.kwargs


class LazyConstructionTests(unittest.TestCase):
    """The producer must not connect until there is something to publish.

    A capture the registry rejects is never published, and building a producer
    for it costs a broker connection plus a bounded-but-real shutdown wait.
    """

    def test_no_producer_is_built_before_the_first_publish(self) -> None:
        settings = Settings.for_testing()

        with patch(
            "network_analyzer.messaging.kafka_publisher.KafkaProducer"
        ) as mock_producer_class:
            NetworkMetricsPublisher(
                bootstrap_servers="broker:29092", settings=settings
            )

        mock_producer_class.assert_not_called()

    def test_producer_is_built_on_the_first_publish(self) -> None:
        producer = _producer_with_successful_send()
        settings = Settings.for_testing()

        with patch(
            "network_analyzer.messaging.kafka_publisher.KafkaProducer",
            return_value=producer,
        ) as mock_producer_class:
            publisher = NetworkMetricsPublisher(
                bootstrap_servers="broker:29092", settings=settings
            )
            publisher.publish("com.whatsapp", _MESSAGE)
            publisher.publish("com.whatsapp", _MESSAGE)

        mock_producer_class.assert_called_once()
        self.assertEqual(producer.send.call_count, 2)

    def test_closing_an_unused_publisher_is_a_no_op(self) -> None:
        settings = Settings.for_testing()

        with patch(
            "network_analyzer.messaging.kafka_publisher.KafkaProducer"
        ) as mock_producer_class:
            publisher = NetworkMetricsPublisher(
                bootstrap_servers="broker:29092", settings=settings
            )
            publisher.close()

        mock_producer_class.assert_not_called()

    def test_flushing_an_unused_publisher_is_a_no_op(self) -> None:
        settings = Settings.for_testing()

        with patch(
            "network_analyzer.messaging.kafka_publisher.KafkaProducer"
        ) as mock_producer_class:
            publisher = NetworkMetricsPublisher(
                bootstrap_servers="broker:29092", settings=settings
            )
            publisher.flush()

        mock_producer_class.assert_not_called()


class ShutdownTests(PublisherTestCase):
    def test_close_is_bounded_by_the_delivery_timeout(self) -> None:
        """An unbounded close can stall the CLI for the metadata refresh interval."""

        producer = _producer_with_successful_send()
        publisher = self.build_publisher(
            producer, kafka_producer_delivery_timeout_ms=15000
        )
        publisher.publish("com.whatsapp", _MESSAGE)

        publisher.close()

        producer.close.assert_called_once_with(timeout=15.0)
        producer.flush.assert_called_once_with(timeout=15.0)

    def test_close_still_closes_when_the_flush_fails(self) -> None:
        producer = _producer_with_successful_send()
        producer.flush.side_effect = KafkaTimeoutError("flush failed")
        publisher = self.build_publisher(producer)
        publisher.publish("com.whatsapp", _MESSAGE)

        with self.assertRaises(KafkaError):
            publisher.close()

        producer.close.assert_called_once()

    def test_close_is_idempotent(self) -> None:
        producer = _producer_with_successful_send()
        publisher = self.build_publisher(producer)
        publisher.publish("com.whatsapp", _MESSAGE)

        publisher.close()
        publisher.close()

        producer.close.assert_called_once()


class ProducerConfigurationTests(unittest.TestCase):
    def test_configures_durability_like_the_crawler(self) -> None:
        kwargs = _producer_kwargs()

        self.assertEqual(kwargs["acks"], "all")
        self.assertTrue(kwargs["enable_idempotence"])
        self.assertEqual(kwargs["bootstrap_servers"], ["broker:29092"])

    def test_splits_multiple_bootstrap_servers(self) -> None:
        kwargs = _producer_kwargs("a:9092,b:9092")

        self.assertEqual(kwargs["bootstrap_servers"], ["a:9092", "b:9092"])

    def test_serializers_produce_utf8_json(self) -> None:
        kwargs = _producer_kwargs()

        self.assertEqual(kwargs["key_serializer"]("com.whatsapp"), b"com.whatsapp")
        encoded = kwargs["value_serializer"](_MESSAGE)
        self.assertEqual(json.loads(encoded.decode("utf-8")), _MESSAGE)

    def test_non_ascii_survives_serialization(self) -> None:
        serializer = _producer_kwargs()["value_serializer"]
        payload = {"source_pcap_filename": "گپ__upload__20260907T141500.pcap"}

        decoded = json.loads(serializer(payload).decode("utf-8"))

        self.assertEqual(decoded, payload)


class FailureTests(PublisherTestCase):
    def test_raises_when_confirmation_fails(self) -> None:
        producer = Mock()
        future = Mock()
        future.get.side_effect = KafkaTimeoutError("no ack")
        producer.send.return_value = future
        publisher = self.build_publisher(producer)

        with self.assertRaises(KafkaError):
            publisher.publish("com.whatsapp", _MESSAGE)

    def test_retries_then_succeeds(self) -> None:
        producer = Mock()
        failing = Mock()
        failing.get.side_effect = KafkaTimeoutError("no ack")
        succeeding = Mock()
        succeeding.get.return_value = Mock()
        producer.send.side_effect = [failing, succeeding]
        publisher = self.build_publisher(
            producer,
            kafka_send_retry_max_attempts=2,
            kafka_send_retry_base_delay_seconds=0.0,
        )

        with patch("network_analyzer.common.retry_policy.sleep"):
            publisher.publish("com.whatsapp", _MESSAGE)

        self.assertEqual(producer.send.call_count, 2)

    def test_gives_up_after_the_retry_budget(self) -> None:
        producer = Mock()
        future = Mock()
        future.get.side_effect = KafkaTimeoutError("no ack")
        producer.send.return_value = future
        publisher = self.build_publisher(
            producer,
            kafka_send_retry_max_attempts=2,
            kafka_send_retry_base_delay_seconds=0.0,
        )

        with patch("network_analyzer.common.retry_policy.sleep"):
            with self.assertRaises(KafkaError):
                publisher.publish("com.whatsapp", _MESSAGE)

        self.assertEqual(producer.send.call_count, 3)

    def test_failed_flush_is_raised_not_swallowed(self) -> None:
        producer = _producer_with_successful_send()
        producer.flush.side_effect = KafkaTimeoutError("flush failed")
        publisher = self.build_publisher(producer)
        publisher.publish("com.whatsapp", _MESSAGE)

        with self.assertRaises(KafkaError):
            publisher.flush()

    def test_close_flushes_first(self) -> None:
        producer = _producer_with_successful_send()
        publisher = self.build_publisher(producer)
        publisher.publish("com.whatsapp", _MESSAGE)

        publisher.close()

        producer.flush.assert_called_once()
        producer.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
