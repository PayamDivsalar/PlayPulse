"""Cross-check analyzer output against Wireshark's expert analysis.

Retransmission detection is the one metric here built on a heuristic rather than
a direct field read, and Wireshark's implementation is the de-facto reference.
Rather than reimplementing its rules, this suite compares the analyzer's numbers
against ``tshark`` on captures whose construction is unambiguous enough that the
two definitions must agree.

Marked ``oracle`` and excluded from ordinary runs, because it needs ``tshark``
on PATH:

    pytest network_analyzer/tests/oracle/test_tshark_oracle.py -m oracle

Install with ``sudo dnf install wireshark-cli`` (Fedora) or
``sudo apt install tshark`` (Debian/Ubuntu).
"""

from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import pytest

from network_analyzer.analysis.metrics_calculator import analyze_capture
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

pytestmark = pytest.mark.oracle

_TSHARK = shutil.which("tshark")
_TSHARK_TIMEOUT_SECONDS = 60


def _count_matching_frames(pcap_path: Path, display_filter: str) -> int:
    """Number of frames in ``pcap_path`` matching a Wireshark display filter."""

    completed = subprocess.run(
        [
            str(_TSHARK),
            "-r",
            str(pcap_path),
            "-Y",
            display_filter,
            "-T",
            "fields",
            "-e",
            "frame.number",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=_TSHARK_TIMEOUT_SECONDS,
    )
    return len([line for line in completed.stdout.splitlines() if line.strip()])


def _mean_syn_ack_rtt_ms(pcap_path: Path) -> float | None:
    """Mean of Wireshark's own ``ack_rtt`` over every SYN-ACK, in milliseconds.

    This is Wireshark's independent measurement of the same quantity the
    analyzer computes for handshake RTT.
    """

    completed = subprocess.run(
        [
            str(_TSHARK),
            "-r",
            str(pcap_path),
            "-Y",
            "tcp.flags.syn==1 && tcp.flags.ack==1",
            "-T",
            "fields",
            "-e",
            "tcp.analysis.ack_rtt",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=_TSHARK_TIMEOUT_SECONDS,
    )
    samples = [
        float(line) for line in completed.stdout.splitlines() if line.strip()
    ]
    if not samples:
        return None
    return (sum(samples) / len(samples)) * 1000.0


@unittest.skipUnless(_TSHARK, "tshark is not installed")
class TsharkOracleTests(unittest.TestCase):
    """Compare the analyzer against tshark on unambiguous captures."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

    def write(self, name: str, capture: Capture) -> Path:
        return write_pcap(self.tmp_path / name, capture)

    def test_reset_count_matches_tshark(self) -> None:
        capture = Capture.of(
            *handshake(start=1.0, rtt_seconds=0.010),
            (1.5, tcp_packet(seq=1001, payload=b"a" * 200)),
            (1.6, tcp_packet(seq=1201, flags="R")),
            (
                1.7,
                tcp_packet(
                    src=SERVER_IP,
                    dst=CLIENT_IP,
                    sport=SERVER_PORT,
                    dport=CLIENT_PORT,
                    seq=5001,
                    flags="RA",
                ),
            ),
        )
        path = self.write("resets.pcap", capture)

        metrics = analyze_capture(path)
        expected = _count_matching_frames(path, "tcp.flags.reset==1")

        self.assertEqual(metrics.tcp_reset_count, expected)
        self.assertEqual(metrics.tcp_reset_count, 2)

    def test_zero_window_count_matches_tshark(self) -> None:
        capture = Capture.of(
            *handshake(start=1.0, rtt_seconds=0.010),
            (1.5, tcp_packet(seq=1001, payload=b"a" * 200)),
            (
                1.6,
                tcp_packet(
                    src=SERVER_IP,
                    dst=CLIENT_IP,
                    sport=SERVER_PORT,
                    dport=CLIENT_PORT,
                    seq=5001,
                    ack=1201,
                    flags="A",
                    window=0,
                ),
            ),
            (
                1.7,
                tcp_packet(
                    src=SERVER_IP,
                    dst=CLIENT_IP,
                    sport=SERVER_PORT,
                    dport=CLIENT_PORT,
                    seq=5001,
                    ack=1201,
                    flags="A",
                    window=0,
                ),
            ),
        )
        path = self.write("zerowindow.pcap", capture)

        metrics = analyze_capture(path)
        expected = _count_matching_frames(path, "tcp.analysis.zero_window")

        self.assertEqual(metrics.zero_window_count, expected)
        self.assertEqual(metrics.zero_window_count, 2)

    def test_retransmission_count_matches_tshark(self) -> None:
        """Plain duplicate data segments, with no ACKs to muddy the picture.

        Without acknowledgements covering the retransmitted ranges, Wireshark
        classifies these as ordinary retransmissions rather than spurious ones,
        so its count and the analyzer's must agree exactly.
        """

        capture = Capture.of(
            *handshake(start=1.0, rtt_seconds=0.010),
            (1.50, tcp_packet(seq=1001, payload=b"a" * 500)),
            (1.60, tcp_packet(seq=1501, payload=b"b" * 500)),
            (1.70, tcp_packet(seq=2001, payload=b"c" * 500)),
            (2.20, tcp_packet(seq=1001, payload=b"a" * 500)),
            (2.70, tcp_packet(seq=1501, payload=b"b" * 500)),
        )
        path = self.write("retransmissions.pcap", capture)

        metrics = analyze_capture(path)
        expected = _count_matching_frames(path, "tcp.analysis.retransmission")

        self.assertEqual(metrics.retransmission_count, expected)
        self.assertEqual(metrics.retransmission_count, 2)
        self.assertEqual(metrics.out_of_order_count, 0)
        self.assertEqual(metrics.spurious_retransmission_count, 0)

    def test_clean_capture_has_no_anomalies_in_either_tool(self) -> None:
        capture = Capture.of(
            *handshake(start=1.0, rtt_seconds=0.010),
            (1.50, tcp_packet(seq=1001, payload=b"a" * 500)),
            (1.60, tcp_packet(seq=1501, payload=b"b" * 500)),
            (1.70, tcp_packet(seq=2001, payload=b"c" * 500)),
        )
        path = self.write("clean.pcap", capture)

        metrics = analyze_capture(path)

        self.assertEqual(
            metrics.retransmission_count,
            _count_matching_frames(path, "tcp.analysis.retransmission"),
        )
        self.assertEqual(
            metrics.zero_window_count,
            _count_matching_frames(path, "tcp.analysis.zero_window"),
        )
        self.assertEqual(metrics.retransmission_count, 0)
        self.assertEqual(metrics.zero_window_count, 0)

    def test_handshake_rtt_matches_tsharks_own_measurement(self) -> None:
        capture = Capture.of(
            *handshake(start=100.0, rtt_seconds=0.020, client_port=40001),
            *handshake(start=200.0, rtt_seconds=0.040, client_port=40002),
        )
        path = self.write("handshakes.pcap", capture)

        metrics = analyze_capture(path)
        expected = _mean_syn_ack_rtt_ms(path)

        assert expected is not None
        assert metrics.rtt_handshake_ms is not None
        self.assertAlmostEqual(metrics.rtt_handshake_ms, expected, places=3)
        self.assertAlmostEqual(metrics.rtt_handshake_ms, 30.0, places=2)

    def test_retransmitted_syn_is_timed_the_same_way_tshark_times_it(self) -> None:
        """Both tools measure from the initial SYN, so a lost one still counts.

        This case is why the oracle exists. Measuring from the retransmitted SYN
        instead looks reasonable and reports a healthy 20ms, but it silently
        hides a 3-second connection setup -- and it contradicts both the
        specification's "initial connection setup packet" and Wireshark.
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
        capture = Capture.of((100.0, syn), (103.0, syn), (103.02, syn_ack))
        path = self.write("syn_retransmit.pcap", capture)

        metrics = analyze_capture(path)
        expected = _mean_syn_ack_rtt_ms(path)

        assert expected is not None
        assert metrics.rtt_handshake_ms is not None
        self.assertAlmostEqual(metrics.rtt_handshake_ms, expected, places=3)

    def test_udp_traffic_is_excluded_from_tcp_metrics_in_both_tools(self) -> None:
        capture = Capture.of(
            (1.0, udp_packet(payload=b"q" * 1200)),
            (1.1, udp_packet(payload=b"q" * 1200)),
            (1.2, udp_packet(payload=b"q" * 1200)),
        )
        path = self.write("quic.pcap", capture)

        metrics = analyze_capture(path)

        self.assertEqual(metrics.retransmission_count, 0)
        self.assertEqual(
            _count_matching_frames(path, "tcp.analysis.retransmission"), 0
        )
        self.assertEqual(_count_matching_frames(path, "udp"), 3)

    def test_total_frame_count_matches_tshark(self) -> None:
        """Sanity check that the fixture writer produces a capture tshark reads."""

        capture = Capture.of(
            *handshake(start=1.0, rtt_seconds=0.010),
            (1.5, tcp_packet(seq=1001, payload=b"a" * 200)),
            (1.6, udp_packet(payload=b"q" * 100)),
        )
        path = self.write("frames.pcap", capture)

        metrics = analyze_capture(path)

        self.assertEqual(metrics.packet_count, _count_matching_frames(path, "ip"))
        self.assertEqual(metrics.packet_count, 5)


if __name__ == "__main__":
    unittest.main()
