"""Network quality and stability metrics (specification group 1).

A streaming accumulator over TCP segments. Zero-window and RST counts are
exact field reads. Retransmission detection uses covered sequence intervals
so out-of-order delivery is not mistaken for loss recovery, and peer ACK
tracking separates spurious resends from true retransmissions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from network_analyzer.models import ParsedPacket, TcpSegment

logger = logging.getLogger(__name__)

# TCP sequence numbers are 32-bit and wrap around.
_SEQ_SPACE = 2**32

_DirectionKey = tuple[str, int, str, int]
# Half-open byte ranges in relative sequence space: [start, end).
_Interval = tuple[int, int]


@dataclass(frozen=True, slots=True)
class TcpMetrics:
    """TCP-level metrics for one capture file."""

    rtt_handshake_ms: float | None
    handshake_sample_count: int
    retransmission_count: int
    out_of_order_count: int
    spurious_retransmission_count: int
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
        self._covered: dict[_DirectionKey, list[_Interval]] = {}
        self._next_expected: dict[_DirectionKey, int] = {}
        self._peer_ack_abs: dict[_DirectionKey, int] = {}
        self._seen_control: set[tuple[_DirectionKey, int, str]] = set()
        self._retransmission_count = 0
        self._out_of_order_count = 0
        self._spurious_retransmission_count = 0
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
        self._note_peer_ack(segment)
        self._observe_retransmission(segment)

    def result(self) -> TcpMetrics:
        """Return the metrics accumulated so far."""

        samples = self._rtt_samples_seconds
        mean_ms = (sum(samples) / len(samples)) * 1000.0 if samples else None

        return TcpMetrics(
            rtt_handshake_ms=mean_ms,
            handshake_sample_count=len(samples),
            retransmission_count=self._retransmission_count,
            out_of_order_count=self._out_of_order_count,
            spurious_retransmission_count=self._spurious_retransmission_count,
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

    def _note_peer_ack(self, segment: TcpSegment) -> None:
        """Record how far the peer has acknowledged this direction's data.

        An ACK on one direction confirms bytes previously sent on the reverse
        direction. Absolute ACK numbers are stored and converted to relative
        sequence space only when that direction has an established base.
        """

        if not segment.ack_flag:
            return

        data_key = segment.reply_key
        previous = self._peer_ack_abs.get(data_key)
        if previous is None:
            self._peer_ack_abs[data_key] = segment.ack
            return

        # Prefer the numerically greater ACK in absolute space when both fall
        # on the same side of the wrap; otherwise keep the one that advanced
        # relative to the data stream's base once known.
        base = self._base_seq.get(data_key)
        if base is None:
            if segment.ack > previous:
                self._peer_ack_abs[data_key] = segment.ack
            return

        prev_rel = (previous - base) % _SEQ_SPACE
        new_rel = (segment.ack - base) % _SEQ_SPACE
        if new_rel > prev_rel:
            self._peer_ack_abs[data_key] = segment.ack

    def _observe_retransmission(self, segment: TcpSegment) -> None:
        """Classify data and control segments against covered sequence ranges.

        For payload-bearing segments the analyzer keeps a merged interval map
        per direction:

        * entirely new bytes ahead of the contiguous frontier → out-of-order
        * any overlap with already-covered bytes, without a peer ACK past the
          segment end → retransmission (partial overlaps count once)
        * fully covered bytes the peer has already acknowledged → spurious
          retransmission (excluded from ``retransmission_count``)
        * fully covered bytes not yet acknowledged → retransmission

        Exact duplicate SYN or FIN segments also count as retransmissions:
        both consume a sequence number without carrying payload.

        Sequence numbers are compared relative to the first value seen in that
        direction, using modular arithmetic, so a wrap of the 32-bit counter
        does not look like a backwards jump.
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
        start = relative_seq
        end = relative_seq + payload_bytes
        covered = self._covered.setdefault(key, [])

        if _fully_covered(covered, start, end):
            if self._peer_has_acked_through(key, end):
                self._spurious_retransmission_count += 1
            else:
                self._retransmission_count += 1
            return

        if key not in self._next_expected:
            self._next_expected[key] = start

        overlaps = _overlaps(covered, start, end)
        if overlaps:
            self._retransmission_count += 1
        elif start > self._next_expected[key]:
            self._out_of_order_count += 1

        _merge_interval(covered, start, end)
        self._advance_next_expected(key)

    def _peer_has_acked_through(self, key: _DirectionKey, relative_end: int) -> bool:
        abs_ack = self._peer_ack_abs.get(key)
        base = self._base_seq.get(key)
        if abs_ack is None or base is None:
            return False
        return (abs_ack - base) % _SEQ_SPACE >= relative_end

    def _advance_next_expected(self, key: _DirectionKey) -> None:
        next_expected = self._next_expected[key]
        for start, end in self._covered[key]:
            if start > next_expected:
                break
            if end > next_expected:
                next_expected = end
        self._next_expected[key] = next_expected

    def _relative_seq(self, key: _DirectionKey, seq: int) -> int:
        """Sequence number relative to the first one seen in this direction."""

        base = self._base_seq.setdefault(key, seq)
        return (seq - base) % _SEQ_SPACE


def _fully_covered(intervals: list[_Interval], start: int, end: int) -> bool:
    """Return True when every byte in ``[start, end)`` is already covered."""

    if start >= end:
        return True
    cursor = start
    for interval_start, interval_end in intervals:
        if interval_end <= cursor:
            continue
        if interval_start > cursor:
            return False
        cursor = interval_end
        if cursor >= end:
            return True
    return cursor >= end


def _overlaps(intervals: list[_Interval], start: int, end: int) -> bool:
    """Return True when ``[start, end)`` shares any byte with ``intervals``."""

    for interval_start, interval_end in intervals:
        if interval_end <= start:
            continue
        if interval_start >= end:
            return False
        return True
    return False


def _merge_interval(intervals: list[_Interval], start: int, end: int) -> None:
    """Insert ``[start, end)`` into a sorted, disjoint interval list in place."""

    if start >= end:
        return

    merged: list[_Interval] = []
    placed = False
    for interval_start, interval_end in intervals:
        if interval_end < start:
            merged.append((interval_start, interval_end))
            continue
        if interval_start > end:
            if not placed:
                merged.append((start, end))
                placed = True
            merged.append((interval_start, interval_end))
            continue
        start = min(start, interval_start)
        end = max(end, interval_end)

    if not placed:
        merged.append((start, end))
    intervals[:] = merged
