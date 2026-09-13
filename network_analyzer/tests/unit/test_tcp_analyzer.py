"""Tests for the TCP quality metrics.

Every capture here is constructed so the correct answer is known by counting
the packets that went in, which is what makes the assertions exact rather than
approximate.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from network_analyzer.analysis.packet_reader import read_packets
from network_analyzer.analysis.tcp_analyzer import TcpAnalyzer, TcpMetrics
from network_analyzer.tests.synthetic_pcap import (
    CLIENT_IP,
    CLIENT_PORT,
    SERVER_IP,
    SERVER_PORT,
    Capture,
    handshake,
    tcp_packet,
    udp_packet,
    write_pcap,
)

_SEQ_SPACE = 2**32


class TcpAnalyzerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def analyze(self, capture: Capture) -> TcpMetrics:
        path = write_pcap(self.tmp_path / "capture.pcap", capture)
        analyzer = TcpAnalyzer()
        for packet in read_packets(path):
            analyzer.observe(packet)
        return analyzer.result()


class HandshakeRttTests(TcpAnalyzerTestCase):
    def test_single_handshake_yields_its_exact_rtt(self) -> None:
        capture = Capture.of(*handshake(start=100.0, rtt_seconds=0.025))

        metrics = self.analyze(capture)

        self.assertEqual(metrics.handshake_sample_count, 1)
        assert metrics.rtt_handshake_ms is not None
        self.assertAlmostEqual(metrics.rtt_handshake_ms, 25.0, places=2)

    def test_multiple_handshakes_are_averaged(self) -> None:
        """20ms and 40ms on two separate connections must average to 30ms."""

        capture = Capture.of(
            *handshake(start=100.0, rtt_seconds=0.020, client_port=40001),
            *handshake(start=200.0, rtt_seconds=0.040, client_port=40002),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.handshake_sample_count, 2)
        assert metrics.rtt_handshake_ms is not None
        self.assertAlmostEqual(metrics.rtt_handshake_ms, 30.0, places=2)

    def test_capture_without_handshake_reports_none(self) -> None:
        """Recording started mid-connection, so there is nothing to measure."""

        capture = Capture.of(
            (1.0, tcp_packet(seq=5000, payload=b"a" * 100)),
            (1.1, tcp_packet(seq=5100, payload=b"b" * 100)),
        )

        metrics = self.analyze(capture)

        self.assertIsNone(metrics.rtt_handshake_ms)
        self.assertEqual(metrics.handshake_sample_count, 0)

    def test_unanswered_syn_produces_no_sample(self) -> None:
        capture = Capture.of((1.0, tcp_packet(seq=1000, flags="S")))

        metrics = self.analyze(capture)

        self.assertIsNone(metrics.rtt_handshake_ms)

    def test_duplicate_syn_ack_does_not_add_a_second_sample(self) -> None:
        syn_ack = tcp_packet(
            src=SERVER_IP,
            dst=CLIENT_IP,
            sport=SERVER_PORT,
            dport=CLIENT_PORT,
            seq=5000,
            ack=1001,
            flags="SA",
        )
        capture = Capture.of(
            (100.0, tcp_packet(seq=1000, flags="S")),
            (100.02, syn_ack),
            (100.5, syn_ack),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.handshake_sample_count, 1)
        assert metrics.rtt_handshake_ms is not None
        self.assertAlmostEqual(metrics.rtt_handshake_ms, 20.0, places=2)

    def test_retransmitted_syn_is_measured_from_the_initial_attempt(self) -> None:
        """The spec measures from the *initial* setup packet, so loss counts.

        A SYN sent at t=100 and retransmitted at t=103, answered at t=103.02,
        means the connection took 3.02s to establish. Measuring from the
        retransmission instead would report a healthy 20ms and hide the loss
        entirely. Wireshark's tcp.analysis.ack_rtt agrees; see the oracle test.
        """

        syn = tcp_packet(seq=1000, flags="S")
        syn_ack = tcp_packet(
            src=SERVER_IP,
            dst=CLIENT_IP,
            sport=SERVER_PORT,
            dport=CLIENT_PORT,
            seq=5000,
            ack=1001,
            flags="SA",
        )
        capture = Capture.of(
            (100.0, syn),
            (103.0, syn),
            (103.02, syn_ack),
        )

        metrics = self.analyze(capture)

        assert metrics.rtt_handshake_ms is not None
        self.assertAlmostEqual(metrics.rtt_handshake_ms, 3020.0, places=1)

    def test_syn_ack_for_an_unseen_syn_is_ignored(self) -> None:
        capture = Capture.of(
            (
                1.0,
                tcp_packet(
                    src=SERVER_IP,
                    dst=CLIENT_IP,
                    sport=SERVER_PORT,
                    dport=CLIENT_PORT,
                    seq=5000,
                    ack=999999,
                    flags="SA",
                ),
            )
        )

        metrics = self.analyze(capture)

        self.assertIsNone(metrics.rtt_handshake_ms)


class RetransmissionTests(TcpAnalyzerTestCase):
    def test_clean_transfer_has_no_retransmissions(self) -> None:
        capture = Capture.of(
            (1.0, tcp_packet(seq=1, payload=b"a" * 100)),
            (1.1, tcp_packet(seq=101, payload=b"b" * 100)),
            (1.2, tcp_packet(seq=201, payload=b"c" * 100)),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 0)

    def test_counts_exactly_three_injected_retransmissions(self) -> None:
        capture = Capture.of(
            (1.0, tcp_packet(seq=1, payload=b"a" * 100)),
            (1.1, tcp_packet(seq=101, payload=b"b" * 100)),
            (1.2, tcp_packet(seq=1, payload=b"a" * 100)),
            (1.3, tcp_packet(seq=201, payload=b"c" * 100)),
            (1.4, tcp_packet(seq=101, payload=b"b" * 100)),
            (1.5, tcp_packet(seq=201, payload=b"c" * 100)),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 3)

    def test_repeated_syn_counts_as_a_retransmission(self) -> None:
        syn = tcp_packet(seq=1000, flags="S")
        capture = Capture.of((1.0, syn), (4.0, syn))

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 1)

    def test_repeated_fin_counts_as_a_retransmission(self) -> None:
        fin = tcp_packet(seq=5000, flags="FA")
        capture = Capture.of((1.0, fin), (2.0, fin))

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 1)

    def test_pure_acks_are_never_retransmissions(self) -> None:
        """Duplicate ACKs signal loss but are not themselves retransmissions."""

        ack = tcp_packet(seq=1, ack=500, flags="A")
        capture = Capture.of((1.0, ack), (1.1, ack), (1.2, ack), (1.3, ack))

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 0)

    def test_directions_are_tracked_independently(self) -> None:
        """Both endpoints pick their own sequence numbers from the same space."""

        capture = Capture.of(
            (1.0, tcp_packet(seq=1, payload=b"a" * 100)),
            (
                1.1,
                tcp_packet(
                    src=SERVER_IP,
                    dst=CLIENT_IP,
                    sport=SERVER_PORT,
                    dport=CLIENT_PORT,
                    seq=1,
                    payload=b"b" * 100,
                ),
            ),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 0)

    def test_separate_connections_are_tracked_independently(self) -> None:
        capture = Capture.of(
            (1.0, tcp_packet(sport=40001, seq=1, payload=b"a" * 100)),
            (1.1, tcp_packet(sport=40002, seq=1, payload=b"a" * 100)),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 0)

    def test_sequence_wraparound_is_not_read_as_mass_retransmission(self) -> None:
        """An ISN near 2^32 must not make every later segment look backwards."""

        near_limit = _SEQ_SPACE - 150
        capture = Capture.of(
            (1.0, tcp_packet(seq=near_limit, payload=b"a" * 100)),
            (1.1, tcp_packet(seq=(near_limit + 100) % _SEQ_SPACE, payload=b"b" * 100)),
            (1.2, tcp_packet(seq=(near_limit + 200) % _SEQ_SPACE, payload=b"c" * 100)),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 0)

    def test_retransmission_detected_across_a_wraparound(self) -> None:
        near_limit = _SEQ_SPACE - 150
        capture = Capture.of(
            (1.0, tcp_packet(seq=near_limit, payload=b"a" * 100)),
            (1.1, tcp_packet(seq=(near_limit + 100) % _SEQ_SPACE, payload=b"b" * 100)),
            (1.2, tcp_packet(seq=near_limit, payload=b"a" * 100)),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 1)
        self.assertEqual(metrics.out_of_order_count, 0)
        self.assertEqual(metrics.spurious_retransmission_count, 0)

    def test_out_of_order_fill_is_not_a_retransmission(self) -> None:
        """A late segment that only fills a hole must not look like loss recovery."""

        capture = Capture.of(
            (1.0, tcp_packet(seq=1000, payload=b"a" * 100)),
            (1.1, tcp_packet(seq=1200, payload=b"c" * 100)),
            (1.2, tcp_packet(seq=1100, payload=b"b" * 100)),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 0)
        self.assertEqual(metrics.out_of_order_count, 1)
        self.assertEqual(metrics.spurious_retransmission_count, 0)

    def test_spurious_retransmission_is_excluded_from_real_count(self) -> None:
        """Resending bytes the peer already ACKed is tracked separately."""

        capture = Capture.of(
            (1.0, tcp_packet(seq=1000, payload=b"a" * 100)),
            (
                1.1,
                tcp_packet(
                    src=SERVER_IP,
                    dst=CLIENT_IP,
                    sport=SERVER_PORT,
                    dport=CLIENT_PORT,
                    seq=5000,
                    ack=1100,
                    flags="A",
                ),
            ),
            (1.2, tcp_packet(seq=1000, payload=b"a" * 100)),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 0)
        self.assertEqual(metrics.spurious_retransmission_count, 1)
        self.assertEqual(metrics.out_of_order_count, 0)

    def test_unacked_duplicate_remains_a_real_retransmission(self) -> None:
        """Without a covering peer ACK, a full overlap is loss recovery."""

        capture = Capture.of(
            (1.0, tcp_packet(seq=1000, payload=b"a" * 100)),
            (1.1, tcp_packet(seq=1000, payload=b"a" * 100)),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 1)
        self.assertEqual(metrics.spurious_retransmission_count, 0)

    def test_partial_overlap_counts_as_one_retransmission(self) -> None:
        capture = Capture.of(
            (1.0, tcp_packet(seq=1000, payload=b"a" * 100)),
            (1.1, tcp_packet(seq=1050, payload=b"b" * 100)),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.retransmission_count, 1)
        self.assertEqual(metrics.out_of_order_count, 0)
        self.assertEqual(metrics.spurious_retransmission_count, 0)


class ZeroWindowTests(TcpAnalyzerTestCase):
    def test_counts_exactly_two_injected_zero_windows(self) -> None:
        capture = Capture.of(
            (1.0, tcp_packet(flags="A", window=64240)),
            (1.1, tcp_packet(flags="A", window=0)),
            (1.2, tcp_packet(flags="A", window=0)),
            (1.3, tcp_packet(flags="A", window=64240)),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.zero_window_count, 2)

    def test_syn_with_zero_window_is_not_counted(self) -> None:
        """A SYN advertises an initial window; it is not a buffer-full signal."""

        capture = Capture.of((1.0, tcp_packet(flags="S", window=0)))

        metrics = self.analyze(capture)

        self.assertEqual(metrics.zero_window_count, 0)

    def test_reset_with_zero_window_is_not_counted(self) -> None:
        capture = Capture.of((1.0, tcp_packet(flags="R", window=0)))

        metrics = self.analyze(capture)

        self.assertEqual(metrics.zero_window_count, 0)
        self.assertEqual(metrics.tcp_reset_count, 1)

    def test_healthy_capture_reports_no_zero_windows(self) -> None:
        capture = Capture.of(*handshake(start=1.0, rtt_seconds=0.01))

        metrics = self.analyze(capture)

        self.assertEqual(metrics.zero_window_count, 0)


class ResetTests(TcpAnalyzerTestCase):
    def test_counts_exactly_one_injected_reset(self) -> None:
        capture = Capture.of(
            (1.0, tcp_packet(flags="A")),
            (1.1, tcp_packet(flags="R")),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.tcp_reset_count, 1)

    def test_counts_reset_ack_combination(self) -> None:
        capture = Capture.of((1.0, tcp_packet(flags="RA")))

        metrics = self.analyze(capture)

        self.assertEqual(metrics.tcp_reset_count, 1)

    def test_graceful_close_is_not_a_reset(self) -> None:
        capture = Capture.of(
            (1.0, tcp_packet(seq=100, flags="FA")),
            (
                1.1,
                tcp_packet(
                    src=SERVER_IP,
                    dst=CLIENT_IP,
                    sport=SERVER_PORT,
                    dport=CLIENT_PORT,
                    seq=200,
                    flags="FA",
                ),
            ),
        )

        metrics = self.analyze(capture)

        self.assertEqual(metrics.tcp_reset_count, 0)


class ScopeTests(TcpAnalyzerTestCase):
    def test_udp_traffic_is_ignored_by_every_tcp_metric(self) -> None:
        """Group 1 is TCP-only; QUIC over UDP must not contribute."""

        capture = Capture.of(
            (1.0, udp_packet(payload=b"q" * 1200)),
            (1.1, udp_packet(payload=b"q" * 1200)),
            (1.2, udp_packet(payload=b"q" * 1200)),
        )

        metrics = self.analyze(capture)

        self.assertIsNone(metrics.rtt_handshake_ms)
        self.assertEqual(metrics.retransmission_count, 0)
        self.assertEqual(metrics.out_of_order_count, 0)
        self.assertEqual(metrics.spurious_retransmission_count, 0)
        self.assertEqual(metrics.zero_window_count, 0)
        self.assertEqual(metrics.tcp_reset_count, 0)

    def test_empty_capture_reports_zeroes_and_no_rtt(self) -> None:
        metrics = self.analyze(Capture.of())

        self.assertIsNone(metrics.rtt_handshake_ms)
        self.assertEqual(metrics.retransmission_count, 0)
        self.assertEqual(metrics.out_of_order_count, 0)
        self.assertEqual(metrics.spurious_retransmission_count, 0)
        self.assertEqual(metrics.zero_window_count, 0)
        self.assertEqual(metrics.tcp_reset_count, 0)


class CombinedScenarioTests(TcpAnalyzerTestCase):
    def test_realistic_upload_capture_with_every_anomaly(self) -> None:
        """One capture holding a known count of each anomaly at once.

        Ground truth by construction: 1 handshake at 30ms, 2 retransmissions,
        2 zero-window advertisements, 1 reset.
        """

        capture = Capture.of(
            *handshake(start=1000.0, rtt_seconds=0.030, client_seq=1000),
            (1000.10, tcp_packet(seq=1001, payload=b"a" * 500)),
            (1000.20, tcp_packet(seq=1501, payload=b"b" * 500)),
            (1000.30, tcp_packet(seq=1001, payload=b"a" * 500)),
            (1000.40, tcp_packet(seq=2001, payload=b"c" * 500)),
            (1000.50, tcp_packet(seq=1501, payload=b"b" * 500)),
            (
                1000.60,
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
            (
                1000.70,
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
            (1000.80, tcp_packet(seq=2501, flags="R")),
        )

        metrics = self.analyze(capture)

        assert metrics.rtt_handshake_ms is not None
        self.assertAlmostEqual(metrics.rtt_handshake_ms, 30.0, places=2)
        self.assertEqual(metrics.handshake_sample_count, 1)
        self.assertEqual(metrics.retransmission_count, 2)
        self.assertEqual(metrics.out_of_order_count, 0)
        self.assertEqual(metrics.spurious_retransmission_count, 0)
        self.assertEqual(metrics.zero_window_count, 2)
        self.assertEqual(metrics.tcp_reset_count, 1)


if __name__ == "__main__":
    unittest.main()
