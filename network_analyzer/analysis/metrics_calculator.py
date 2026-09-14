"""Compose the metric families into a complete result for one capture file.

This is the entry point of the analysis core, and the boundary the rest of the
subsystem depends on: give it a path, receive a
:class:`~network_analyzer.core.models.NetworkMetrics`. Nothing above this line needs
to know that pcap files or TCP headers exist.
"""

from __future__ import annotations

import logging
from pathlib import Path

from network_analyzer.analysis.packet_reader import read_packets
from network_analyzer.analysis.tcp_analyzer import TcpAnalyzer, TcpMetrics
from network_analyzer.analysis.volume_analyzer import VolumeAnalyzer, VolumeTotals
from network_analyzer.core.models import NetworkMetrics

logger = logging.getLogger(__name__)


def analyze_capture(path: str | Path) -> NetworkMetrics:
    """Analyze one capture file and return its metrics.

    The capture is traversed exactly once, with both accumulators fed from the
    same packet stream, so file size drives runtime linearly and memory stays
    proportional to the number of distinct flows rather than to packet count.

    Raises:
        PcapReadError: if the file is missing, unreadable, or not a capture.
        UnsupportedLinkTypeError: if the link-layer type cannot be decoded.
    """

    volume_analyzer = VolumeAnalyzer()
    tcp_analyzer = TcpAnalyzer()

    for packet in read_packets(path):
        volume_analyzer.observe(packet)
        tcp_analyzer.observe(packet)

    metrics = _combine(volume_analyzer.result(), tcp_analyzer.result())
    _log_summary(Path(path), metrics)
    return metrics


def _combine(volume: VolumeTotals, tcp: TcpMetrics) -> NetworkMetrics:
    return NetworkMetrics(
        rtt_handshake_ms=tcp.rtt_handshake_ms,
        retransmission_count=tcp.retransmission_count,
        out_of_order_count=tcp.out_of_order_count,
        spurious_retransmission_count=tcp.spurious_retransmission_count,
        zero_window_count=tcp.zero_window_count,
        tcp_reset_count=tcp.tcp_reset_count,
        bytes_transferred_total=volume.bytes_transferred_total,
        bytes_payload_total=volume.bytes_payload_total,
        packet_count=volume.packet_count,
        handshake_sample_count=tcp.handshake_sample_count,
    )


def _log_summary(pcap_path: Path, metrics: NetworkMetrics) -> None:
    if metrics.packet_count == 0:
        logger.warning(
            "Capture %s contained no decodable IP packets; all metrics are zero.",
            pcap_path.name,
        )
        return

    if metrics.handshake_sample_count == 0:
        logger.info(
            "Capture %s holds no complete TCP handshake, so rtt_handshake is "
            "null. This is expected when recording started after the "
            "connections were already established.",
            pcap_path.name,
        )

    logger.info(
        "Analyzed %s: packets=%s bytes_total=%s bytes_payload=%s "
        "overhead_ratio=%.4f handshakes=%s",
        pcap_path.name,
        metrics.packet_count,
        metrics.bytes_transferred_total,
        metrics.bytes_payload_total,
        metrics.overhead_ratio,
        metrics.handshake_sample_count,
    )
