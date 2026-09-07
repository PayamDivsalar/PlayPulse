"""Tests for the analyzer domain model."""

from __future__ import annotations

import unittest

from network_analyzer.exceptions import NetworkAnalyzerError
from network_analyzer.models import NetworkMetrics, Scenario, TcpSegment


def _metrics(**overrides: object) -> NetworkMetrics:
    values: dict[str, object] = {
        "rtt_handshake_ms": 12.5,
        "retransmission_count": 0,
        "zero_window_count": 0,
        "tcp_reset_count": 0,
        "bytes_transferred_total": 1000,
        "bytes_payload_total": 800,
    }
    values.update(overrides)
    return NetworkMetrics(**values)  # type: ignore[arg-type]


class ScenarioTests(unittest.TestCase):
    def test_parses_lowercase(self) -> None:
        self.assertEqual(Scenario.parse("upload"), Scenario.UPLOAD)

    def test_parses_with_surrounding_whitespace(self) -> None:
        self.assertEqual(Scenario.parse("  download  "), Scenario.DOWNLOAD)

    def test_rejects_unknown_value(self) -> None:
        with self.assertRaises(NetworkAnalyzerError):
            Scenario.parse("sideload")

    def test_value_matches_persisted_choice(self) -> None:
        self.assertEqual(Scenario.UPLOAD.value, "UPLOAD")
        self.assertEqual(Scenario.DOWNLOAD.value, "DOWNLOAD")


class OverheadRatioTests(unittest.TestCase):
    def test_computes_header_fraction(self) -> None:
        metrics = _metrics(bytes_transferred_total=1000, bytes_payload_total=800)

        self.assertAlmostEqual(metrics.overhead_ratio, 0.2)

    def test_all_header_traffic_yields_one(self) -> None:
        metrics = _metrics(bytes_transferred_total=500, bytes_payload_total=0)

        self.assertAlmostEqual(metrics.overhead_ratio, 1.0)

    def test_empty_capture_yields_zero_instead_of_dividing_by_zero(self) -> None:
        metrics = _metrics(bytes_transferred_total=0, bytes_payload_total=0)

        self.assertEqual(metrics.overhead_ratio, 0.0)


class MetricsValidationTests(unittest.TestCase):
    def test_rejects_negative_counter(self) -> None:
        with self.assertRaises(NetworkAnalyzerError):
            _metrics(retransmission_count=-1)

    def test_rejects_negative_rtt(self) -> None:
        with self.assertRaises(NetworkAnalyzerError):
            _metrics(rtt_handshake_ms=-0.5)

    def test_allows_absent_rtt(self) -> None:
        metrics = _metrics(rtt_handshake_ms=None)

        self.assertIsNone(metrics.rtt_handshake_ms)

    def test_rejects_payload_exceeding_total(self) -> None:
        with self.assertRaises(NetworkAnalyzerError):
            _metrics(bytes_transferred_total=100, bytes_payload_total=101)


class TcpSegmentTests(unittest.TestCase):
    def _segment(self) -> TcpSegment:
        return TcpSegment(
            src_ip="10.0.0.1",
            dst_ip="93.184.216.34",
            src_port=44321,
            dst_port=443,
            seq=100,
            ack=0,
            window=64240,
            payload_bytes=0,
            syn=True,
            ack_flag=False,
            fin=False,
            rst=False,
        )

    def test_direction_key_is_source_to_destination(self) -> None:
        self.assertEqual(
            self._segment().direction_key,
            ("10.0.0.1", 44321, "93.184.216.34", 443),
        )

    def test_reply_key_is_the_mirror_of_direction_key(self) -> None:
        segment = self._segment()

        self.assertEqual(
            segment.reply_key,
            ("93.184.216.34", 443, "10.0.0.1", 44321),
        )
        self.assertNotEqual(segment.reply_key, segment.direction_key)


if __name__ == "__main__":
    unittest.main()
