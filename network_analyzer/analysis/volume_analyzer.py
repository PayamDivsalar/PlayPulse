"""Data consumption efficiency metrics (specification group 2).

A streaming accumulator rather than a function over a list, so a capture is
traversed exactly once no matter how many metric families are being collected.
"""

from __future__ import annotations

from dataclasses import dataclass

from network_analyzer.core.models import ParsedPacket


@dataclass(frozen=True, slots=True)
class VolumeTotals:
    """Byte totals for one capture file."""

    packet_count: int
    bytes_transferred_total: int
    bytes_payload_total: int


class VolumeAnalyzer:
    """Accumulate total and payload byte volumes across a capture.

    Scope note: unlike the TCP metrics, these totals cover every IP packet in
    the file. For messaging applications that means QUIC over UDP is included,
    which is usually where the bulk of a file transfer actually goes.

    The per-packet byte arithmetic lives in ``packet_reader``, which is the only
    module that should know about header layouts. This class is deliberately
    just the summation, so the two concerns stay independently testable.
    """

    def __init__(self) -> None:
        self._packet_count = 0
        self._bytes_transferred_total = 0
        self._bytes_payload_total = 0

    def observe(self, packet: ParsedPacket) -> None:
        """Fold one packet into the running totals."""

        self._packet_count += 1
        self._bytes_transferred_total += packet.ip_bytes
        self._bytes_payload_total += packet.payload_bytes

    def result(self) -> VolumeTotals:
        """Return the totals accumulated so far."""

        return VolumeTotals(
            packet_count=self._packet_count,
            bytes_transferred_total=self._bytes_transferred_total,
            bytes_payload_total=self._bytes_payload_total,
        )
