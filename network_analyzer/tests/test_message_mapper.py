"""Tests for the Kafka wire contract.

These assertions are the contract the Data Storage Subsystem will be written
against, so they check exact key names and value shapes rather than just
round-tripping.
"""

from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone

from network_analyzer.message_mapper import map_analysis_result
from network_analyzer.models import AnalysisResult, NetworkMetrics, Scenario

_EXPECTED_KEYS = {
    "analysis_id",
    "package_name",
    "scenario",
    "rtt_handshake",
    "retransmission_count",
    "out_of_order_count",
    "spurious_retransmission_count",
    "zero_window_count",
    "tcp_reset_count",
    "bytes_transferred_total",
    "bytes_payload_total",
    "overhead_ratio",
    "source_pcap_filename",
    "analyzed_at",
}


def _result(**overrides: object) -> AnalysisResult:
    metrics_overrides = overrides.pop("metrics", None)
    metrics = metrics_overrides or NetworkMetrics(
        rtt_handshake_ms=43.123456,
        retransmission_count=7,
        out_of_order_count=1,
        spurious_retransmission_count=2,
        zero_window_count=0,
        tcp_reset_count=2,
        bytes_transferred_total=1040,
        bytes_payload_total=1000,
        packet_count=1,
        handshake_sample_count=1,
    )
    values: dict[str, object] = {
        "analysis_id": "8f14e45f-ea6a-4f2b-9c1d-2b3a5c7d9e01",
        "package_name": "com.whatsapp",
        "scenario": Scenario.UPLOAD,
        "source_pcap_filename": "com.whatsapp__upload__20260907T141500.pcap",
        "analyzed_at": datetime(2026, 9, 7, 14, 20, 11, tzinfo=timezone.utc),
        "metrics": metrics,
    }
    values.update(overrides)
    return AnalysisResult(**values)  # type: ignore[arg-type]


class ContractShapeTests(unittest.TestCase):
    def test_message_holds_exactly_the_contract_keys(self) -> None:
        message = map_analysis_result(_result())

        self.assertEqual(set(message), _EXPECTED_KEYS)

    def test_observability_only_fields_are_not_published(self) -> None:
        """packet_count and handshake_sample_count have no persisted column."""

        message = map_analysis_result(_result())

        self.assertNotIn("packet_count", message)
        self.assertNotIn("handshake_sample_count", message)

    def test_message_is_json_serializable(self) -> None:
        message = map_analysis_result(_result())

        decoded = json.loads(json.dumps(message, ensure_ascii=False))

        self.assertEqual(decoded["package_name"], "com.whatsapp")

    def test_scenario_is_serialized_as_its_persisted_choice(self) -> None:
        message = map_analysis_result(_result(scenario=Scenario.DOWNLOAD))

        self.assertEqual(message["scenario"], "DOWNLOAD")
        self.assertIsInstance(message["scenario"], str)

    def test_analyzed_at_is_iso_8601_with_offset(self) -> None:
        message = map_analysis_result(_result())

        self.assertEqual(message["analyzed_at"], "2026-09-07T14:20:11+00:00")

    def test_source_filename_carries_no_directory(self) -> None:
        message = map_analysis_result(_result())

        self.assertNotIn("/", message["source_pcap_filename"])


class ValueMappingTests(unittest.TestCase):
    def test_metrics_are_copied_verbatim(self) -> None:
        message = map_analysis_result(_result())

        self.assertEqual(message["retransmission_count"], 7)
        self.assertEqual(message["out_of_order_count"], 1)
        self.assertEqual(message["spurious_retransmission_count"], 2)
        self.assertEqual(message["zero_window_count"], 0)
        self.assertEqual(message["tcp_reset_count"], 2)
        self.assertEqual(message["bytes_transferred_total"], 1040)
        self.assertEqual(message["bytes_payload_total"], 1000)

    def test_rtt_is_rounded_to_microsecond_precision(self) -> None:
        message = map_analysis_result(_result())

        self.assertEqual(message["rtt_handshake"], 43.123)

    def test_absent_rtt_becomes_json_null(self) -> None:
        metrics = NetworkMetrics(
            rtt_handshake_ms=None,
            retransmission_count=0,
            out_of_order_count=0,
            spurious_retransmission_count=0,
            zero_window_count=0,
            tcp_reset_count=0,
            bytes_transferred_total=100,
            bytes_payload_total=60,
        )
        message = map_analysis_result(_result(metrics=metrics))

        self.assertIsNone(message["rtt_handshake"])
        self.assertIn('"rtt_handshake": null', json.dumps(message))

    def test_overhead_ratio_is_derived_and_rounded(self) -> None:
        message = map_analysis_result(_result())

        # 40 header bytes out of 1040 total.
        self.assertAlmostEqual(message["overhead_ratio"], round(40 / 1040, 6))

    def test_overhead_ratio_of_empty_capture_is_zero(self) -> None:
        metrics = NetworkMetrics(
            rtt_handshake_ms=None,
            retransmission_count=0,
            out_of_order_count=0,
            spurious_retransmission_count=0,
            zero_window_count=0,
            tcp_reset_count=0,
            bytes_transferred_total=0,
            bytes_payload_total=0,
        )
        message = map_analysis_result(_result(metrics=metrics))

        self.assertEqual(message["overhead_ratio"], 0.0)

    def test_analysis_id_is_passed_through(self) -> None:
        message = map_analysis_result(_result(analysis_id="fixed-id"))

        self.assertEqual(message["analysis_id"], "fixed-id")


if __name__ == "__main__":
    unittest.main()
