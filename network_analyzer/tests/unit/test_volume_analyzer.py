"""Tests for the data consumption efficiency accumulator."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from network_analyzer.analysis.packet_reader import read_packets
from network_analyzer.analysis.volume_analyzer import VolumeAnalyzer
from network_analyzer.core.models import ParsedPacket
from network_analyzer.tests.synthetic_pcap import (
    Capture,
    tcp_packet,
    udp_packet,
    write_pcap,
)


class VolumeAnalyzerTests(unittest.TestCase):
    def test_empty_capture_reports_zeroes(self) -> None:
        totals = VolumeAnalyzer().result()

        self.assertEqual(totals.packet_count, 0)
        self.assertEqual(totals.bytes_transferred_total, 0)
        self.assertEqual(totals.bytes_payload_total, 0)

    def test_sums_totals_across_packets(self) -> None:
        analyzer = VolumeAnalyzer()
        for _ in range(3):
            analyzer.observe(
                ParsedPacket(timestamp=1.0, ip_bytes=1500, payload_bytes=1448)
            )

        totals = analyzer.result()

        self.assertEqual(totals.packet_count, 3)
        self.assertEqual(totals.bytes_transferred_total, 4500)
        self.assertEqual(totals.bytes_payload_total, 4344)

    def test_result_reflects_only_packets_observed_so_far(self) -> None:
        analyzer = VolumeAnalyzer()
        analyzer.observe(ParsedPacket(timestamp=1.0, ip_bytes=100, payload_bytes=60))

        first = analyzer.result()
        analyzer.observe(ParsedPacket(timestamp=2.0, ip_bytes=100, payload_bytes=60))
        second = analyzer.result()

        self.assertEqual(first.packet_count, 1)
        self.assertEqual(second.packet_count, 2)


class VolumeOverCaptureTests(unittest.TestCase):
    """End-to-end byte accounting from a capture file with derived expectations."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def test_mixed_tcp_and_udp_totals_are_exact(self) -> None:
        # Ground truth, computed by hand from the header layouts:
        #   TCP payload 1000 -> 20 + 20 + 1000 = 1040 total, 1000 payload
        #   TCP payload  400 -> 20 + 20 +  400 =  440 total,  400 payload
        #   pure ACK         -> 20 + 20 +    0 =   40 total,    0 payload
        #   UDP payload 1200 -> 20 +  8 + 1200 = 1228 total, 1200 payload
        capture = Capture.of(
            (1.0, tcp_packet(payload=b"a" * 1000)),
            (1.1, tcp_packet(payload=b"b" * 400)),
            (1.2, tcp_packet(flags="A")),
            (1.3, udp_packet(payload=b"c" * 1200)),
        )
        path = write_pcap(self.tmp_path / "mixed.pcap", capture)

        analyzer = VolumeAnalyzer()
        for packet in read_packets(path):
            analyzer.observe(packet)
        totals = analyzer.result()

        self.assertEqual(totals.packet_count, 4)
        self.assertEqual(totals.bytes_transferred_total, 1040 + 440 + 40 + 1228)
        self.assertEqual(totals.bytes_payload_total, 1000 + 400 + 0 + 1200)

    def test_overhead_ratio_for_a_known_capture(self) -> None:
        """A single 1000-byte TCP payload carries exactly 40 header bytes."""

        capture = Capture.of((1.0, tcp_packet(payload=b"a" * 1000)))
        path = write_pcap(self.tmp_path / "single.pcap", capture)

        analyzer = VolumeAnalyzer()
        for packet in read_packets(path):
            analyzer.observe(packet)
        totals = analyzer.result()

        self.assertEqual(totals.bytes_transferred_total, 1040)
        self.assertEqual(totals.bytes_payload_total, 1000)
        self.assertAlmostEqual(
            (totals.bytes_transferred_total - totals.bytes_payload_total)
            / totals.bytes_transferred_total,
            40 / 1040,
        )

    def test_ack_only_capture_is_pure_overhead(self) -> None:
        capture = Capture.of(
            (1.0, tcp_packet(flags="A")),
            (1.1, tcp_packet(flags="A")),
        )
        path = write_pcap(self.tmp_path / "acks.pcap", capture)

        analyzer = VolumeAnalyzer()
        for packet in read_packets(path):
            analyzer.observe(packet)
        totals = analyzer.result()

        self.assertEqual(totals.bytes_transferred_total, 80)
        self.assertEqual(totals.bytes_payload_total, 0)


if __name__ == "__main__":
    unittest.main()
