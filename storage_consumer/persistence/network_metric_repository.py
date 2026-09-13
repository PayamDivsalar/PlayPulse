"""Writer for ``network_metrics``."""

from __future__ import annotations

from typing import Any

from storage_consumer.core.events import BoundRecord, NetworkMetricEvent
from storage_consumer.persistence.repository import Repository


class NetworkMetricRepository(Repository):
    """Insert-once on ``analysis_id``.

    ``DO NOTHING`` rather than ``DO UPDATE`` because a capture is analyzed
    once: the same ``analysis_id`` arriving twice is a redelivery, never a
    correction. Re-running the same test genuinely produces a new UUID and
    therefore a new row, which preserves the table's append-only intent.
    """

    table = "network_metrics"
    columns = (
        "application_id",
        "analysis_id",
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
    )
    conflict_clause = (
        "ON CONFLICT (analysis_id) DO NOTHING RETURNING (xmax = 0) AS inserted"
    )

    def _row(self, record: BoundRecord) -> tuple[Any, ...]:
        event = record.event
        assert isinstance(event, NetworkMetricEvent)
        return (
            record.application_id,
            event.analysis_id,
            event.scenario,
            event.rtt_handshake,
            event.retransmission_count,
            event.out_of_order_count,
            event.spurious_retransmission_count,
            event.zero_window_count,
            event.tcp_reset_count,
            event.bytes_transferred_total,
            event.bytes_payload_total,
            event.overhead_ratio,
            event.source_pcap_filename,
            event.analyzed_at,
        )
