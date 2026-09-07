"""Domain model for the network analyzer.

These types are deliberately free of any infrastructure concern: no dpkt, no
Kafka, no HTTP, no database. ``analysis`` builds them, ``message_mapper``
projects them onto the wire contract, and tests can construct them directly.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from network_analyzer.exceptions import NetworkAnalyzerError


class Scenario(str, Enum):
    """The two capture scenarios required by the subsystem specification.

    Values match the ``scenario`` column choices documented for the
    ``network_metrics`` table, so the wire contract needs no translation.
    """

    UPLOAD = "UPLOAD"
    DOWNLOAD = "DOWNLOAD"

    @classmethod
    def parse(cls, raw: str) -> Scenario:
        """Parse a case-insensitive scenario name.

        Raises:
            NetworkAnalyzerError: if ``raw`` is not a known scenario.
        """

        normalized = raw.strip().upper()
        try:
            return cls(normalized)
        except ValueError as exc:
            valid = ", ".join(member.value for member in cls)
            raise NetworkAnalyzerError(
                f"Unknown scenario {raw!r}. Expected one of: {valid}."
            ) from exc


@dataclass(frozen=True, slots=True)
class TcpSegment:
    """A single TCP segment, reduced to the fields the metrics need.

    Flags are stored as explicit booleans rather than a bitmask so both the
    analyzer and its tests read declaratively (``segment.syn`` instead of
    ``segment.flags & dpkt.tcp.TH_SYN``).
    """

    src_ip: str
    dst_ip: str
    src_port: int
    dst_port: int
    seq: int
    ack: int
    window: int
    payload_bytes: int
    syn: bool
    ack_flag: bool
    fin: bool
    rst: bool

    @property
    def direction_key(self) -> tuple[str, int, str, int]:
        """Identify one direction of one TCP flow.

        Directional (not bidirectional) on purpose: sequence-number tracking
        for retransmission detection is per-direction, since each endpoint
        maintains its own independent sequence space.
        """

        return (self.src_ip, self.src_port, self.dst_ip, self.dst_port)

    @property
    def reply_key(self) -> tuple[str, int, str, int]:
        """The ``direction_key`` of the opposite direction of the same flow."""

        return (self.dst_ip, self.dst_port, self.src_ip, self.src_port)


@dataclass(frozen=True, slots=True)
class ParsedPacket:
    """One capture record, normalized across link-layer types.

    ``ip_bytes`` is the size of the IP packet as it appeared on the wire, taken
    from the IP header's own length field rather than the capture record
    length. See ``analysis.packet_reader`` for why.
    """

    timestamp: float
    ip_bytes: int
    payload_bytes: int
    tcp: TcpSegment | None = None


@dataclass(frozen=True, slots=True)
class NetworkMetrics:
    """The metrics required by the subsystem specification, for one pcap file.

    Groups 1 and 2 of the specification differ in scope, which matters when
    reading the numbers: the four TCP counters describe TCP traffic only, while
    the two byte totals cover every IP packet in the capture (including UDP and
    QUIC).

    ``handshake_sample_count`` is not part of the persisted schema. It exists so
    the CLI can report how many handshakes the mean was taken over, which is the
    difference between a trustworthy average and a single outlier.
    """

    rtt_handshake_ms: float | None
    retransmission_count: int
    zero_window_count: int
    tcp_reset_count: int
    bytes_transferred_total: int
    bytes_payload_total: int
    packet_count: int = 0
    handshake_sample_count: int = 0

    def __post_init__(self) -> None:
        if self.rtt_handshake_ms is not None and self.rtt_handshake_ms < 0:
            raise NetworkAnalyzerError("rtt_handshake_ms must be >= 0.")
        for name in (
            "retransmission_count",
            "zero_window_count",
            "tcp_reset_count",
            "bytes_transferred_total",
            "bytes_payload_total",
            "packet_count",
            "handshake_sample_count",
        ):
            if getattr(self, name) < 0:
                raise NetworkAnalyzerError(f"{name} must be >= 0.")
        if self.bytes_payload_total > self.bytes_transferred_total:
            raise NetworkAnalyzerError(
                "bytes_payload_total must not exceed bytes_transferred_total "
                f"({self.bytes_payload_total} > {self.bytes_transferred_total})."
            )

    @property
    def overhead_ratio(self) -> float:
        """Header volume as a fraction of total traffic volume.

        Derived rather than stored, so it can never contradict the two byte
        totals it comes from. ``message_mapper`` serializes it because the
        persisted schema keeps a materialized copy for reporting queries.

        Returns ``0.0`` for an empty capture, where the ratio is undefined.
        """

        if self.bytes_transferred_total <= 0:
            return 0.0
        header_bytes = self.bytes_transferred_total - self.bytes_payload_total
        return header_bytes / self.bytes_transferred_total


@dataclass(frozen=True, slots=True)
class CaptureDescriptor:
    """Which application and scenario a capture file belongs to."""

    package_name: str
    scenario: Scenario
    captured_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class AnalysisResult:
    """A complete analysis of one pcap file, ready to publish."""

    analysis_id: str
    package_name: str
    scenario: Scenario
    source_pcap_filename: str
    analyzed_at: datetime
    metrics: NetworkMetrics
