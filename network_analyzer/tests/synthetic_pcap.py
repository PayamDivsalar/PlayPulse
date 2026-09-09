"""Build synthetic capture files with known ground truth.

Analysis tests need captures where the exact number of retransmissions, zero
window advertisements and resets is known by construction, so an assertion can
name a precise expected value instead of eyeballing a real capture.

``scapy`` builds the IP/TCP/UDP bytes; the pcap container is written here with
``struct``. Writing the container directly is what makes the PCAPdroid
"extensions" format reproducible: its records are
``[Ethernet | IP | padding | trailer]``, which no packet-building library will
lay out for you.

Test-only module. Nothing in ``network_analyzer`` imports it.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from pathlib import Path

from scapy.layers.inet import IP, TCP, UDP
from scapy.layers.inet6 import IPv6
from scapy.packet import Packet, Raw

LINKTYPE_ETHERNET = 1
LINKTYPE_RAW_IP = 101

_PCAP_MAGIC = 0xA1B2C3D4
_PCAP_VERSION_MAJOR = 2
_PCAP_VERSION_MINOR = 4
_PCAP_SNAPLEN = 262144

_PCAPDROID_TRAILER_MAGIC = 0x01072021
_PCAPDROID_APPNAME_BYTES = 20

# Fake source/destination MACs, mirroring what PCAPdroid synthesizes when it
# wraps raw IP packets to carry its trailer.
_FAKE_ETHERNET_HEADER = (
    b"\x00\x00\x00\x00\x00\x01" b"\x00\x00\x00\x00\x00\x02" b"\x08\x00"
)
_FAKE_ETHERNET_HEADER_IPV6 = (
    b"\x00\x00\x00\x00\x00\x01" b"\x00\x00\x00\x00\x00\x02" b"\x86\xdd"
)

CLIENT_IP = "10.0.0.2"
SERVER_IP = "93.184.216.34"
CLIENT_IP6 = "2001:db8::2"
SERVER_IP6 = "2001:db8::1"
CLIENT_PORT = 44321
SERVER_PORT = 443


@dataclass(frozen=True)
class Capture:
    """A timestamped sequence of packets destined for one capture file."""

    packets: tuple[tuple[float, Packet], ...]

    @classmethod
    def of(cls, *packets: tuple[float, Packet]) -> Capture:
        return cls(packets=tuple(packets))


def tcp_packet(
    *,
    src: str = CLIENT_IP,
    dst: str = SERVER_IP,
    sport: int = CLIENT_PORT,
    dport: int = SERVER_PORT,
    seq: int = 1,
    ack: int = 0,
    flags: str = "A",
    window: int = 64240,
    payload: bytes = b"",
) -> Packet:
    """Build one IPv4 TCP segment."""

    segment = IP(src=src, dst=dst) / TCP(
        sport=sport, dport=dport, seq=seq, ack=ack, flags=flags, window=window
    )
    if payload:
        segment = segment / Raw(payload)
    return _normalize(segment)


def tcp6_packet(
    *,
    src: str = CLIENT_IP6,
    dst: str = SERVER_IP6,
    sport: int = CLIENT_PORT,
    dport: int = SERVER_PORT,
    seq: int = 1,
    ack: int = 0,
    flags: str = "A",
    window: int = 64240,
    payload: bytes = b"",
) -> Packet:
    """Build one IPv6 TCP segment."""

    segment = IPv6(src=src, dst=dst) / TCP(
        sport=sport, dport=dport, seq=seq, ack=ack, flags=flags, window=window
    )
    if payload:
        segment = segment / Raw(payload)
    return _normalize(segment)


def udp_packet(
    *,
    src: str = CLIENT_IP,
    dst: str = SERVER_IP,
    sport: int = CLIENT_PORT,
    dport: int = 443,
    payload: bytes = b"",
) -> Packet:
    """Build one IPv4 UDP datagram, standing in for QUIC traffic."""

    datagram = IP(src=src, dst=dst) / UDP(sport=sport, dport=dport)
    if payload:
        datagram = datagram / Raw(payload)
    return _normalize(datagram)


def icmp_packet(*, src: str = SERVER_IP, dst: str = CLIENT_IP) -> Packet:
    """Build one ICMP packet, which contributes bytes but no layer-4 payload."""

    from scapy.layers.inet import ICMP

    return _normalize(IP(src=src, dst=dst) / ICMP())


def handshake(
    *,
    start: float,
    rtt_seconds: float,
    client_port: int = CLIENT_PORT,
    client_seq: int = 1000,
    server_seq: int = 5000,
) -> list[tuple[float, Packet]]:
    """Build a SYN / SYN-ACK / ACK exchange with an exact handshake RTT.

    The SYN-ACK acknowledges ``client_seq + 1``, which is what the analyzer
    matches on.
    """

    syn = tcp_packet(seq=client_seq, flags="S", sport=client_port)
    syn_ack = tcp_packet(
        src=SERVER_IP,
        dst=CLIENT_IP,
        sport=SERVER_PORT,
        dport=client_port,
        seq=server_seq,
        ack=client_seq + 1,
        flags="SA",
    )
    ack = tcp_packet(
        seq=client_seq + 1, ack=server_seq + 1, flags="A", sport=client_port
    )
    return [
        (start, syn),
        (start + rtt_seconds, syn_ack),
        (start + rtt_seconds + 0.001, ack),
    ]


def write_pcap(
    path: str | Path,
    capture: Capture,
    *,
    link_type: int = LINKTYPE_RAW_IP,
    pcapdroid_trailer: bool = False,
    uid: int = 10123,
    appname: str = "com.whatsapp",
) -> Path:
    """Write ``capture`` to ``path`` in the requested capture format.

    Args:
        link_type: ``LINKTYPE_RAW_IP`` for PCAPdroid's default output, or
            ``LINKTYPE_ETHERNET`` for its "extensions" output.
        pcapdroid_trailer: append PCAPdroid's metadata trailer to each record.
            Only meaningful with ``LINKTYPE_ETHERNET``, which is how PCAPdroid
            itself emits it.
    """

    pcap_path = Path(path)
    records = [
        (
            timestamp,
            _frame_record(packet, link_type, pcapdroid_trailer, uid, appname),
        )
        for timestamp, packet in capture.packets
    ]

    with pcap_path.open("wb") as handle:
        handle.write(
            struct.pack(
                "<IHHiIII",
                _PCAP_MAGIC,
                _PCAP_VERSION_MAJOR,
                _PCAP_VERSION_MINOR,
                0,
                0,
                _PCAP_SNAPLEN,
                link_type,
            )
        )
        for timestamp, data in records:
            seconds = int(timestamp)
            microseconds = int(round((timestamp - seconds) * 1_000_000))
            handle.write(
                struct.pack("<IIII", seconds, microseconds, len(data), len(data))
            )
            handle.write(data)

    return pcap_path


def _frame_record(
    packet: Packet,
    link_type: int,
    pcapdroid_trailer: bool,
    uid: int,
    appname: str,
) -> bytes:
    ip_bytes = bytes(packet)

    if link_type == LINKTYPE_RAW_IP:
        return ip_bytes

    if link_type != LINKTYPE_ETHERNET:
        raise ValueError(f"Unsupported link type for fixtures: {link_type}")

    header = (
        _FAKE_ETHERNET_HEADER_IPV6
        if isinstance(packet, IPv6)
        else _FAKE_ETHERNET_HEADER
    )
    frame = header + ip_bytes
    if not pcapdroid_trailer:
        return frame

    # PCAPdroid zero-pads so its trailer starts on a 4-byte boundary.
    padding = b"\x00" * (-len(frame) % 4)
    trailer = struct.pack(
        "<Ii20sI",
        _PCAPDROID_TRAILER_MAGIC,
        uid,
        appname.encode("utf-8")[: _PCAPDROID_APPNAME_BYTES - 1],
        0,
    )
    return frame + padding + trailer


def _normalize(packet: Packet) -> Packet:
    """Force scapy to compute lengths and checksums, then reparse.

    Without this, ``bytes(packet)`` is correct but the in-memory object still
    reports unset length fields, which makes expected values in tests harder to
    derive.
    """

    rebuilt = packet.__class__(bytes(packet))
    return rebuilt


def ip_total_bytes(packet: Packet) -> int:
    """Wire size of a built packet's IP layer, for deriving expected totals."""

    return len(bytes(packet))
