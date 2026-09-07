"""Network quality and stability metrics (specification group 1).

A streaming accumulator over TCP segments. Three of the four metrics are exact
reads of protocol fields; retransmission detection is a documented heuristic,
described on :meth:`TcpAnalyzer._observe_retransmission`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from network_analyzer.models import ParsedPacket, TcpSegment

logger = logging.getLogger(__name__)

# TCP sequence numbers are 32-bit and wrap around.
_SEQ_SPACE = 2**32

_DirectionKey = tuple[str, int, str, int]


@dataclass(frozen=True, slots=True)
class TcpMetrics:
    """TCP-level metrics for one capture file."""

    rtt_handshake_ms: float | None
    handshake_sample_count: int
    retransmission_count: int
    zero_window_count: int
    tcp_reset_count: int


class TcpAnalyzer:
    """Accumulate TCP quality metrics across a capture.

    State is kept per *flow direction* rather than per connection, because each
    endpoint of a TCP connection maintains its own independent sequence space.
    """

    def __init__(self) -> None:
        self._pending_syns: dict[tuple[_DirectionKey, int], float] = {}
        self._rtt_samples_seconds: list[float] = []
        self._base_seq: dict[_DirectionKey, int] = {}
        self._highest_seq_end: dict[_DirectionKey, int] = {}
        self._seen_control: set[tuple[_DirectionKey, int, str]] = set()
        self._retransmission_count = 0
        self._zero_window_count = 0
        self._tcp_reset_count = 0

    def observe(self, packet: ParsedPacket) -> None:
        """Fold one packet into the running metrics, ignoring non-TCP traffic."""

        segment = packet.tcp
        if segment is None:
            return

        if segment.rst:
            self._tcp_reset_count += 1

        self._observe_zero_window(segment)
        self._observe_handshake(segment, packet.timestamp)
        self._observe_retransmission(segment)

    def result(self) -> TcpMetrics:
        """Return the metrics accumulated so far."""

        samples = self._rtt_samples_seconds
        mean_ms = (sum(samples) / len(samples)) * 1000.0 if samples else None

        return TcpMetrics(
            rtt_handshake_ms=mean_ms,
            handshake_sample_count=len(samples),
            retransmission_count=self._retransmission_count,
            zero_window_count=self._zero_window_count,
            tcp_reset_count=self._tcp_reset_count,
        )

    def _observe_zero_window(self, segment: TcpSegment) -> None:
        """Count receiver-buffer-full notifications.

        SYN and RST packets are excluded: a SYN's window is an initial
        advertisement made before any data has been buffered, and the window
        field of a RST is meaningless.
        """

        if segment.window == 0 and not segment.syn and not segment.rst:
            self._zero_window_count += 1

    def _observe_handshake(self, segment: TcpSegment, timestamp: float) -> None:
        """Match SYN-ACKs to their SYNs and record the elapsed time.

        A SYN is remembered under its own direction and sequence number. The
        SYN-ACK that answers it travels the opposite way and acknowledges
        ``syn_seq + 1``, so the pending entry is looked up by the SYN-ACK's
        reply direction and ``ack - 1``.

        A repeated SYN does *not* overwrite the remembered timestamp: the first
        one wins. The specification asks for the time between "sending the
        initial connection setup packet and receiving its acknowledgment", so
        when a SYN is lost and retransmitted, the measurement covers the whole
        establishment delay including the retransmission timeout, not just the
        round trip of the attempt that happened to get through. That is also
        what Wireshark's ``tcp.analysis.ack_rtt`` reports, which the oracle test
        pins down.
        """

        if not segment.syn:
            return

        if not segment.ack_flag:
            self._pending_syns.setdefault(
                (segment.direction_key, segment.seq), timestamp
            )
            return

        syn_key = (segment.reply_key, (segment.ack - 1) % _SEQ_SPACE)
        syn_timestamp = self._pending_syns.pop(syn_key, None)
        if syn_timestamp is None:
            # The SYN is not in the capture (recording started mid-connection),
            # or this is a duplicate SYN-ACK whose SYN was already matched.
            return

        elapsed = timestamp - syn_timestamp
        if elapsed < 0:
            logger.debug(
                "Discarding handshake sample with negative elapsed time (%.6fs) "
                "for %s; capture records are out of order.",
                elapsed,
                segment.reply_key,
            )
            return
        self._rtt_samples_seconds.append(elapsed)

    def _observe_retransmission(self, segment: TcpSegment) -> None:
        """Detect retransmitted segments.

        Heuristic, and the only metric here that is not a direct field read.
        Per flow direction the analyzer tracks the highest ``seq + length``
        seen. A segment carrying payload whose end does not advance that
        high-water mark is counted as a retransmission, and an exactly
        duplicated SYN or FIN is counted too, since both consume a sequence
        number without carrying payload.

        Sequence numbers are compared relative to the first value seen in that
        direction, using modular arithmetic. TCP's initial sequence number is
        random, so an ordinary transfer can cross the 32-bit boundary mid
        capture; comparing raw values would then read the wrap as a massive
        backwards jump and count every subsequent segment as a retransmission.

        Known limitation: this does not distinguish a true retransmission from
        out-of-order delivery or from a spurious retransmission the way
        Wireshark's expert analysis does, and a partially overlapping segment
        that still advances the high-water mark is not flagged. The oracle test
        in ``tests/test_tshark_oracle.py`` quantifies the difference.
        """

        key = segment.direction_key
        relative_seq = self._relative_seq(key, segment.seq)

        if segment.payload_bytes > 0:
            self._observe_data_segment(key, relative_seq, segment.payload_bytes)
            return

        if segment.syn or segment.fin:
            marker = (key, relative_seq, "SYN" if segment.syn else "FIN")
            if marker in self._seen_control:
                self._retransmission_count += 1
            else:
                self._seen_control.add(marker)

    def _observe_data_segment(
        self, key: _DirectionKey, relative_seq: int, payload_bytes: int
    ) -> None:
        segment_end = relative_seq + payload_bytes
        highest = self._highest_seq_end.get(key)

        if highest is None or segment_end > highest:
            self._highest_seq_end[key] = segment_end
            return
        self._retransmission_count += 1

    def _relative_seq(self, key: _DirectionKey, seq: int) -> int:
        """Sequence number relative to the first one seen in this direction."""

        base = self._base_seq.setdefault(key, seq)
        return (seq - base) % _SEQ_SPACE
