"""Live Kafka round-trip for the network-metrics producer.

Publishes to a real broker and reads the message back.

Run manually with ``-m live`` after ``docker compose up -d kafka``.
Override the broker with ``KAFKA_BOOTSTRAP_SERVERS``.

The mocked unit tests live in
``network_analyzer/tests/unit/test_kafka_publisher.py``.
"""

from __future__ import annotations

import json
import os
import unittest
import uuid

import pytest

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


@pytest.mark.live
class LiveKafkaRoundTripTests(unittest.TestCase):
    def test_published_message_can_be_consumed_verbatim(self) -> None:
        from kafka import KafkaConsumer

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        settings = Settings.for_testing(kafka_bootstrap_servers=bootstrap)

        analysis_id = str(uuid.uuid4())
        message = {**_MESSAGE, "analysis_id": analysis_id}

        publisher = NetworkMetricsPublisher(
            bootstrap_servers=bootstrap, settings=settings
        )
        try:
            publisher.publish("com.whatsapp", message)
        finally:
            publisher.close()

        consumer = KafkaConsumer(
            NETWORK_METRICS_TOPIC,
            bootstrap_servers=bootstrap.split(","),
            auto_offset_reset="earliest",
            consumer_timeout_ms=20_000,
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
            key_deserializer=lambda raw: raw.decode("utf-8") if raw else None,
        )
        try:
            received = [
                record
                for record in consumer
                if record.value.get("analysis_id") == analysis_id
            ]
        finally:
            consumer.close()

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].key, "com.whatsapp")
        self.assertEqual(received[0].value, message)


if __name__ == "__main__":
    unittest.main()
