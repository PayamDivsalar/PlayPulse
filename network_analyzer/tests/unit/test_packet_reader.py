"""Tests for capture decoding across PCAPdroid's capture formats."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from network_analyzer.analysis.packet_reader import read_packets
from network_analyzer.exceptions import PcapReadError, UnsupportedLinkTypeError
from network_analyzer.tests.synthetic_pcap import (
    CLIENT_IP,
    CLIENT_PORT,
    LINKTYPE_ETHERNET,
    LINKTYPE_RAW_IP,
    SERVER_IP,
    SERVER_PORT,
    Capture,
    icmp_packet,
    tcp6_packet,
    tcp_packet,
    udp_packet,
    write_pcap,
)


class PacketReaderTestCase(unittest.TestCase):
    """Shared temporary-directory plumbing for capture-file tests."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def write(self, capture: Capture, **kwargs: object) -> Path:
        target = self.tmp_path / "capture.pcap"
        return write_pcap(target, capture, **kwargs)  # type: ignore[arg-type]


class LinkTypeDispatchTests(PacketReaderTestCase):
    def test_reads_raw_ip_capture(self) -> None:
        capture = Capture.of((1.0, tcp_packet(payload=b"x" * 100)))
        path = self.write(capture, link_type=LINKTYPE_RAW_IP)

        packets = list(read_packets(path))

        self.assertEqual(len(packets), 1)
        self.assertEqual(packets[0].payload_bytes, 100)

    def test_reads_ethernet_capture(self) -> None:
        capture = Capture.of((1.0, tcp_packet(payload=b"x" * 100)))
        path = self.write(capture, link_type=LINKTYPE_ETHERNET)

        packets = list(read_packets(path))

        self.assertEqual(len(packets), 1)
        self.assertEqual(packets[0].payload_bytes, 100)

    def test_both_formats_report_identical_byte_totals(self) -> None:
        """PCAPdroid's own framing must not inflate the app's traffic volume."""

        capture = Capture.of(
            (1.0, tcp_packet(payload=b"x" * 100)),
            (1.1, udp_packet(payload=b"y" * 250)),
        )
        raw_path = write_pcap(
            self.tmp_path / "raw.pcap", capture, link_type=LINKTYPE_RAW_IP
        )
        eth_path = write_pcap(
            self.tmp_path / "eth.pcap",
            capture,
            link_type=LINKTYPE_ETHERNET,
            pcapdroid_trailer=True,
        )

        raw_packets = list(read_packets(raw_path))
        eth_packets = list(read_packets(eth_path))

        self.assertEqual(
            sum(packet.ip_bytes for packet in raw_packets),
            sum(packet.ip_bytes for packet in eth_packets),
        )
        self.assertEqual(
            sum(packet.payload_bytes for packet in raw_packets),
            sum(packet.payload_bytes for packet in eth_packets),
        )

    def test_pcapdroid_trailer_is_stripped(self) -> None:
        capture = Capture.of((1.0, tcp_packet(payload=b"x" * 100)))
        path = self.write(
            capture, link_type=LINKTYPE_ETHERNET, pcapdroid_trailer=True
        )

        packets = list(read_packets(path))

        self.assertEqual(len(packets), 1)
        self.assertEqual(packets[0].payload_bytes, 100)

    def test_unsupported_link_type_names_the_supported_ones(self) -> None:
        capture = Capture.of((1.0, tcp_packet()))
        path = self.tmp_path / "sll.pcap"
        # 113 is LINKTYPE_LINUX_SLL, which PCAPdroid never writes in PCAP mode.
        write_pcap(path, capture, link_type=LINKTYPE_RAW_IP)
        raw = bytearray(path.read_bytes())
        raw[20:24] = (113).to_bytes(4, "little")
        path.write_bytes(bytes(raw))

        with self.assertRaises(UnsupportedLinkTypeError) as ctx:
            list(read_packets(path))

        message = str(ctx.exception)
        self.assertIn("113", message)
        self.assertIn("101", message)


class FieldExtractionTests(PacketReaderTestCase):
    def test_extracts_tcp_flow_endpoints(self) -> None:
        capture = Capture.of((1.0, tcp_packet(flags="S", seq=1000)))
        path = self.write(capture)

        segment = list(read_packets(path))[0].tcp

        assert segment is not None
        self.assertEqual(segment.src_ip, CLIENT_IP)
        self.assertEqual(segment.dst_ip, SERVER_IP)
        self.assertEqual(segment.src_port, CLIENT_PORT)
        self.assertEqual(segment.dst_port, SERVER_PORT)
        self.assertEqual(segment.seq, 1000)

    def test_extracts_tcp_flags(self) -> None:
        capture = Capture.of(
            (1.0, tcp_packet(flags="S")),
            (1.1, tcp_packet(flags="SA")),
            (1.2, tcp_packet(flags="R")),
            (1.3, tcp_packet(flags="FA")),
        )
        path = self.write(capture)

        segments = [packet.tcp for packet in read_packets(path)]

        assert all(segment is not None for segment in segments)
        syn, syn_ack, reset, fin_ack = segments  # type: ignore[misc]
        self.assertTrue(syn.syn and not syn.ack_flag)
        self.assertTrue(syn_ack.syn and syn_ack.ack_flag)
        self.assertTrue(reset.rst)
        self.assertTrue(fin_ack.fin and fin_ack.ack_flag)

    def test_extracts_advertised_window(self) -> None:
        capture = Capture.of((1.0, tcp_packet(window=0)))
        path = self.write(capture)

        segment = list(read_packets(path))[0].tcp

        assert segment is not None
        self.assertEqual(segment.window, 0)

    def test_preserves_record_timestamps(self) -> None:
        capture = Capture.of(
            (1_700_000_000.0, tcp_packet(flags="S")),
            (1_700_000_000.025, tcp_packet(flags="SA")),
        )
        path = self.write(capture)

        packets = list(read_packets(path))

        self.assertAlmostEqual(packets[0].timestamp, 1_700_000_000.0, places=5)
        self.assertAlmostEqual(packets[1].timestamp, 1_700_000_000.025, places=5)

    def test_udp_packet_has_no_tcp_segment(self) -> None:
        capture = Capture.of((1.0, udp_packet(payload=b"q" * 40)))
        path = self.write(capture)

        packet = list(read_packets(path))[0]

        self.assertIsNone(packet.tcp)
        self.assertEqual(packet.payload_bytes, 40)

    def test_icmp_contributes_bytes_but_no_payload(self) -> None:
        capture = Capture.of((1.0, icmp_packet()))
        path = self.write(capture)

        packet = list(read_packets(path))[0]

        self.assertIsNone(packet.tcp)
        self.assertEqual(packet.payload_bytes, 0)
        self.assertGreater(packet.ip_bytes, 0)

    def test_reads_ipv6_tcp(self) -> None:
        capture = Capture.of((1.0, tcp6_packet(payload=b"z" * 120)))
        path = self.write(capture)

        packet = list(read_packets(path))[0]

        assert packet.tcp is not None
        self.assertEqual(packet.payload_bytes, 120)
        # 40-byte fixed header + 20-byte TCP header + payload.
        self.assertEqual(packet.ip_bytes, 40 + 20 + 120)

    def test_ipv6_addresses_are_rendered_in_v6_notation(self) -> None:
        capture = Capture.of((1.0, tcp6_packet()))
        path = self.write(capture)

        segment = list(read_packets(path))[0].tcp

        assert segment is not None
        self.assertIn(":", segment.src_ip)


class ByteAccountingTests(PacketReaderTestCase):
    def test_ipv4_total_matches_header_arithmetic(self) -> None:
        capture = Capture.of((1.0, tcp_packet(payload=b"x" * 512)))
        path = self.write(capture)

        packet = list(read_packets(path))[0]

        # 20-byte IPv4 header + 20-byte TCP header + payload.
        self.assertEqual(packet.ip_bytes, 20 + 20 + 512)
        self.assertEqual(packet.payload_bytes, 512)

    def test_pure_ack_has_no_payload(self) -> None:
        capture = Capture.of((1.0, tcp_packet(flags="A")))
        path = self.write(capture)

        packet = list(read_packets(path))[0]

        self.assertEqual(packet.payload_bytes, 0)
        self.assertEqual(packet.ip_bytes, 40)

    def test_udp_payload_excludes_the_eight_byte_header(self) -> None:
        capture = Capture.of((1.0, udp_packet(payload=b"q" * 300)))
        path = self.write(capture)

        packet = list(read_packets(path))[0]

        self.assertEqual(packet.payload_bytes, 300)
        self.assertEqual(packet.ip_bytes, 20 + 8 + 300)


class ReaderFailureTests(PacketReaderTestCase):
    def test_missing_file_raises_before_iteration(self) -> None:
        with self.assertRaises(PcapReadError):
            read_packets(self.tmp_path / "absent.pcap")

    def test_non_capture_file_raises(self) -> None:
        path = self.tmp_path / "notes.pcap"
        path.write_bytes(b"this is definitely not a capture file")

        with self.assertRaises(PcapReadError):
            list(read_packets(path))

    def test_empty_file_raises(self) -> None:
        path = self.tmp_path / "empty.pcap"
        path.write_bytes(b"")

        with self.assertRaises(PcapReadError):
            list(read_packets(path))

    def test_capture_with_no_records_yields_nothing(self) -> None:
        path = self.write(Capture.of())

        self.assertEqual(list(read_packets(path)), [])

    def test_truncated_tail_preserves_earlier_records(self) -> None:
        """A capture stopped mid-write must not lose the records it did hold."""

        capture = Capture.of(
            (1.0, tcp_packet(payload=b"a" * 100)),
            (1.1, tcp_packet(payload=b"b" * 100)),
            (1.2, tcp_packet(payload=b"c" * 100)),
        )
        path = self.write(capture)
        raw = path.read_bytes()
        path.write_bytes(raw[:-60])

        packets = list(read_packets(path))

        self.assertGreaterEqual(len(packets), 2)


if __name__ == "__main__":
    unittest.main()
