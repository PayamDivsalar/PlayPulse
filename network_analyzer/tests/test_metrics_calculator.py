"""Tests for the analysis core's public entry point."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from network_analyzer.analysis.metrics_calculator import analyze_capture
from network_analyzer.exceptions import PcapReadError
from network_analyzer.tests.synthetic_pcap import (
    CLIENT_IP,
    CLIENT_PORT,
    LINKTYPE_ETHERNET,
    SERVER_IP,
    SERVER_PORT,
    Capture,
    handshake,
    tcp_packet,
    udp_packet,
    write_pcap,
)


class AnalyzeCaptureTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def write(self, capture: Capture, **kwargs: object) -> Path:
        return write_pcap(self.tmp_path / "capture.pcap", capture, **kwargs)  # type: ignore[arg-type]

    def test_reports_every_required_metric_for_one_capture(self) -> None:
        """A capture with a known count of each anomaly and known byte totals.

        Ground truth by construction:
          handshake  -> 1 sample at 30ms (SYN 40B, SYN-ACK 40B, ACK 40B)
          data       -> 3 unique 500B segments + 1 retransmission (540B each)
          zero-window-> 1 advertisement (40B)
          reset      -> 1 packet (40B)
          udp        -> 1 datagram carrying 1200B payload (1228B total)
        """

        capture = Capture.of(
            *handshake(start=1000.0, rtt_seconds=0.030, client_seq=1000),
            (1000.10, tcp_packet(seq=1001, payload=b"a" * 500)),
            (1000.20, tcp_packet(seq=1501, payload=b"b" * 500)),
            (1000.30, tcp_packet(seq=1001, payload=b"a" * 500)),
            (1000.40, tcp_packet(seq=2001, payload=b"c" * 500)),
            (
                1000.50,
                tcp_packet(
                    src=SERVER_IP,
                    dst=CLIENT_IP,
                    sport=SERVER_PORT,
                    dport=CLIENT_PORT,
                    seq=5001,
                    flags="A",
                    window=0,
                ),
            ),
            (1000.60, tcp_packet(seq=2501, flags="R")),
            (1000.70, udp_packet(payload=b"q" * 1200)),
        )
        path = self.write(capture)

        metrics = analyze_capture(path)

        assert metrics.rtt_handshake_ms is not None
        self.assertAlmostEqual(metrics.rtt_handshake_ms, 30.0, places=2)
        self.assertEqual(metrics.handshake_sample_count, 1)
        self.assertEqual(metrics.retransmission_count, 1)
        self.assertEqual(metrics.zero_window_count, 1)
        self.assertEqual(metrics.tcp_reset_count, 1)

        # 3 handshake + 4 data + 1 zero-window + 1 reset + 1 UDP.
        expected_total = (3 * 40) + (4 * 540) + 40 + 40 + 1228
        expected_payload = (4 * 500) + 1200
        self.assertEqual(metrics.packet_count, 10)
        self.assertEqual(metrics.bytes_transferred_total, expected_total)
        self.assertEqual(metrics.bytes_payload_total, expected_payload)
        self.assertAlmostEqual(
            metrics.overhead_ratio,
            (expected_total - expected_payload) / expected_total,
        )

    def test_empty_capture_yields_all_zeroes(self) -> None:
        path = self.write(Capture.of())

        metrics = analyze_capture(path)

        self.assertIsNone(metrics.rtt_handshake_ms)
        self.assertEqual(metrics.packet_count, 0)
        self.assertEqual(metrics.bytes_transferred_total, 0)
        self.assertEqual(metrics.bytes_payload_total, 0)
        self.assertEqual(metrics.overhead_ratio, 0.0)

    def test_overhead_ratio_stays_within_bounds(self) -> None:
        capture = Capture.of(
            (1.0, tcp_packet(flags="A")),
            (1.1, tcp_packet(seq=1, payload=b"a" * 1460)),
            (1.2, udp_packet(payload=b"q" * 1200)),
        )
        path = self.write(capture)

        metrics = analyze_capture(path)

        self.assertGreaterEqual(metrics.overhead_ratio, 0.0)
        self.assertLessEqual(metrics.overhead_ratio, 1.0)

    def test_capture_format_does_not_change_the_metrics(self) -> None:
        """The same traffic must measure identically in either PCAPdroid mode."""

        capture = Capture.of(
            *handshake(start=1.0, rtt_seconds=0.015),
            (1.5, tcp_packet(seq=1001, payload=b"a" * 300)),
            (1.6, tcp_packet(seq=1001, payload=b"a" * 300)),
            (1.7, tcp_packet(flags="A", window=0)),
            (1.8, tcp_packet(flags="R")),
        )
        raw_path = write_pcap(self.tmp_path / "raw.pcap", capture)
        eth_path = write_pcap(
            self.tmp_path / "eth.pcap",
            capture,
            link_type=LINKTYPE_ETHERNET,
            pcapdroid_trailer=True,
        )

        raw_metrics = analyze_capture(raw_path)
        eth_metrics = analyze_capture(eth_path)

        self.assertEqual(raw_metrics, eth_metrics)

    def test_missing_file_raises(self) -> None:
        with self.assertRaises(PcapReadError):
            analyze_capture(self.tmp_path / "absent.pcap")

    def test_unreadable_file_raises(self) -> None:
        path = self.tmp_path / "garbage.pcap"
        path.write_bytes(b"not a capture at all, just some text")

        with self.assertRaises(PcapReadError):
            analyze_capture(path)


if __name__ == "__main__":
    unittest.main()
