"""Project an analysis result onto the Kafka wire contract.

Pure functions only. This module is the single definition of the message the
Data Storage Subsystem consumes, kept separate from both the domain model and
the producer so the contract can be asserted in tests without a broker.

Field names are snake_case and the object is flat, matching the crawler's
existing topics rather than introducing a second message style on the same
cluster.
"""

from __future__ import annotations

from typing import Any

from network_analyzer.core.models import AnalysisResult

# Milliseconds to microsecond precision. Capture timestamps do not justify more,
# and unrounded floats would put values like 24.999999999999996 on the topic.
_RTT_DECIMALS = 3

# Six decimals distinguishes overhead ratios well below a tenth of a percent,
# which is finer than any reporting query needs.
_OVERHEAD_RATIO_DECIMALS = 6


def map_analysis_result(result: AnalysisResult) -> dict[str, Any]:
    """Build the ``network-metrics`` message body for one analyzed capture.

    Deliberately excludes the domain model's ``packet_count`` and
    ``handshake_sample_count``: both exist for operator reporting on stdout and
    have no column in the persisted schema. Publishing them would invite the
    consumer to depend on fields the database cannot hold.
    """

    metrics = result.metrics
    return {
        "analysis_id": result.analysis_id,
        "package_name": result.package_name,
        "scenario": result.scenario.value,
        "rtt_handshake": _round_optional(metrics.rtt_handshake_ms, _RTT_DECIMALS),
        "retransmission_count": metrics.retransmission_count,
        "out_of_order_count": metrics.out_of_order_count,
        "spurious_retransmission_count": metrics.spurious_retransmission_count,
        "zero_window_count": metrics.zero_window_count,
        "tcp_reset_count": metrics.tcp_reset_count,
        "bytes_transferred_total": metrics.bytes_transferred_total,
        "bytes_payload_total": metrics.bytes_payload_total,
        "overhead_ratio": round(metrics.overhead_ratio, _OVERHEAD_RATIO_DECIMALS),
        "source_pcap_filename": result.source_pcap_filename,
        "analyzed_at": result.analyzed_at.isoformat(),
    }


def _round_optional(value: float | None, decimals: int) -> float | None:
    """Round a value that may be absent.

    ``None`` survives as JSON ``null``, which the nullable ``rtt_handshake``
    column expects when a capture holds no complete handshake.
    """

    return None if value is None else round(value, decimals)
