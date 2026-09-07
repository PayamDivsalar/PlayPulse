"""Decode pcap capture records into the analyzer's domain model.

This module owns every format-specific detail of reading a capture: container
format sniffing, link-layer dispatch, PCAPdroid trailer removal, and IP/TCP/UDP
field extraction. Everything downstream sees a uniform stream of
:class:`~network_analyzer.models.ParsedPacket`.
"""

from __future__ import annotations

import logging
import socket
import struct
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import dpkt

from network_analyzer.exceptions import PcapReadError, UnsupportedLinkTypeError
from network_analyzer.models import ParsedPacket, TcpSegment

logger = logging.getLogger(__name__)

# Link-layer types exactly as written in the pcap file header.
#
# These are compared as literals on purpose. ``Reader.datalink()`` returns the
# number stored in the file, whereas dpkt's ``DLT_*`` constants are the host
# platform's BSD-style values -- ``dpkt.pcap.DLT_RAW`` is 12, which would never
# match the 101 that PCAPdroid writes.
_LINKTYPE_ETHERNET = 1
_LINKTYPE_RAW_IP = 101

_SUPPORTED_LINK_TYPES = {
    _LINKTYPE_ETHERNET: "Ethernet (PCAPdroid with extensions enabled)",
    _LINKTYPE_RAW_IP: "raw IP (PCAPdroid default)",
}

_PCAPNG_MAGIC = b"\x0a\x0d\x0d\x0a"

# PCAPdroid's "extensions" trailer, appended after the L3 data inside a
# synthetic Ethernet frame: magic(4) + uid(4) + appname(20) + fcs(4).
_PCAPDROID_TRAILER_MAGIC = b"\x01\x07\x20\x21"
_PCAPDROID_TRAILER_BYTES = 32

_IPV6_FIXED_HEADER_BYTES = 40
_UDP_HEADER_BYTES = 8

_IP_CLASSES = (dpkt.ip.IP, dpkt.ip6.IP6)

# A truncated final record is normal in a capture that was cut off mid-write, so
# malformed records are skipped rather than failing the whole analysis. Above
# this share of the file, the input is probably not what it claims to be.
_MALFORMED_WARN_RATIO = 0.01

# dpkt surfaces short reads either as its own error type or, in a few code
# paths, as the underlying struct/index error.
_DECODE_ERRORS: tuple[type[BaseException], ...] = (
    dpkt.dpkt.Error,
    struct.error,
    IndexError,
)


def read_packets(path: str | Path) -> Iterator[ParsedPacket]:
    """Stream decoded packets from a pcap or pcapng capture.

    Packets are yielded lazily so a large capture never has to be held in
    memory at once. Records that cannot be decoded, and records carrying
    neither IPv4 nor IPv6, are skipped.

    Raises:
        PcapReadError: if the file is missing, unreadable, or not a capture.
        UnsupportedLinkTypeError: if the link-layer type cannot be decoded.
    """

    pcap_path = Path(path)
    if not pcap_path.is_file():
        raise PcapReadError(f"Capture file not found: {pcap_path}")
    return _iter_packets(pcap_path)


def _iter_packets(pcap_path: Path) -> Iterator[ParsedPacket]:
    with pcap_path.open("rb") as handle:
        reader = _build_reader(handle, pcap_path)
        decode = _decoder_for(_link_type_of(reader, pcap_path), pcap_path)

        total = 0
        malformed = 0
        for timestamp, buffer in _iter_records(reader, pcap_path):
            total += 1
            try:
                ip_packet = decode(bytes(buffer))
            except _DECODE_ERRORS as exc:
                malformed += 1
                logger.debug(
                    "Skipping undecodable record %s in %s: %s",
                    total,
                    pcap_path.name,
                    exc,
                )
                continue

            if ip_packet is None:
                continue
            yield _build_packet(timestamp, ip_packet)

        if malformed and malformed > total * _MALFORMED_WARN_RATIO:
            logger.warning(
                "Skipped %s of %s records in %s as undecodable.",
                malformed,
                total,
                pcap_path.name,
            )


def _build_reader(handle: Any, pcap_path: Path) -> Any:
    """Pick the pcap or pcapng reader by sniffing the file's leading magic."""

    magic = handle.read(4)
    handle.seek(0)
    if len(magic) < 4:
        raise PcapReadError(
            f"{pcap_path.name} is only {len(magic)} byte(s) long and cannot be "
            "a capture file."
        )

    reader_class = dpkt.pcapng.Reader if magic == _PCAPNG_MAGIC else dpkt.pcap.Reader
    try:
        return reader_class(handle)
    except (dpkt.dpkt.Error, ValueError, KeyError) as exc:
        raise PcapReadError(
            f"{pcap_path.name} is not a readable pcap/pcapng capture: {exc}"
        ) from exc


def _link_type_of(reader: Any, pcap_path: Path) -> int:
    try:
        return int(reader.datalink())
    except (AttributeError, TypeError, ValueError) as exc:
        raise PcapReadError(
            f"Could not determine the link-layer type of {pcap_path.name}: {exc}"
        ) from exc


def _iter_records(reader: Any, pcap_path: Path) -> Iterator[tuple[float, bytes]]:
    """Iterate capture records, tolerating a truncated tail.

    A capture stopped mid-write leaves a partial final record. dpkt raises while
    reading it, which must not discard the records already parsed.
    """

    try:
        yield from reader
    except (*_DECODE_ERRORS, ValueError) as exc:
        logger.warning(
            "Capture %s ends with an incomplete record; analyzing what was "
            "readable. (%s)",
            pcap_path.name,
            exc,
        )


def _decoder_for(
    link_type: int, pcap_path: Path
) -> Callable[[bytes], dpkt.ip.IP | dpkt.ip6.IP6 | None]:
    if link_type == _LINKTYPE_RAW_IP:
        return _decode_raw_ip
    if link_type == _LINKTYPE_ETHERNET:
        return _decode_ethernet

    supported = ", ".join(
        f"{value} ({label})" for value, label in sorted(_SUPPORTED_LINK_TYPES.items())
    )
    raise UnsupportedLinkTypeError(
        f"{pcap_path.name} uses link-layer type {link_type}, which this "
        f"analyzer cannot decode. Supported types: {supported}. Re-export the "
        "capture from PCAPdroid in PCAP format."
    )


def _decode_raw_ip(buffer: bytes) -> dpkt.ip.IP | dpkt.ip6.IP6 | None:
    """Decode a record whose first byte is the start of the IP header."""

    if not buffer:
        return None
    version = buffer[0] >> 4
    if version == 4:
        return dpkt.ip.IP(buffer)
    if version == 6:
        return dpkt.ip6.IP6(buffer)
    return None


def _decode_ethernet(buffer: bytes) -> dpkt.ip.IP | dpkt.ip6.IP6 | None:
    """Decode a record framed in Ethernet, removing any PCAPdroid trailer."""

    frame = dpkt.ethernet.Ethernet(_strip_pcapdroid_trailer(buffer))
    payload = frame.data
    return payload if isinstance(payload, _IP_CLASSES) else None


def _strip_pcapdroid_trailer(buffer: bytes) -> bytes:
    """Remove PCAPdroid's metadata trailer if this frame carries one.

    With "PCAPdroid extensions" enabled, each record becomes
    ``[Ethernet | IP | Payload | padding | trailer]``. The trailer is located
    from the end of the frame, so the variable zero padding in front of it needs
    no handling.

    Removing it is defensive rather than strictly required: IP parsing already
    trims to the header's length field, so trailing bytes are normally ignored.
    It matters for the fallback path that measures a packet by its captured
    length, which would otherwise count PCAPdroid's bookkeeping as app traffic.
    """

    if len(buffer) <= _PCAPDROID_TRAILER_BYTES:
        return buffer
    trailer_start = len(buffer) - _PCAPDROID_TRAILER_BYTES
    if buffer[trailer_start : trailer_start + 4] != _PCAPDROID_TRAILER_MAGIC:
        return buffer
    return buffer[:trailer_start]


def _build_packet(
    timestamp: float, ip_packet: dpkt.ip.IP | dpkt.ip6.IP6
) -> ParsedPacket:
    if isinstance(ip_packet, dpkt.ip.IP):
        ip_bytes = _ipv4_total_bytes(ip_packet)
        header_bytes = ip_packet.hl * 4
        family = socket.AF_INET
    else:
        ip_bytes = _ipv6_total_bytes(ip_packet)
        header_bytes = _IPV6_FIXED_HEADER_BYTES + _ipv6_extension_bytes(ip_packet)
        family = socket.AF_INET6

    ip_payload_bytes = max(0, ip_bytes - header_bytes)
    transport = ip_packet.data

    if isinstance(transport, dpkt.tcp.TCP):
        tcp_header_bytes = transport.off * 4
        payload_bytes = max(0, ip_payload_bytes - tcp_header_bytes)
        segment = _build_tcp_segment(ip_packet, transport, family, payload_bytes)
    elif isinstance(transport, dpkt.udp.UDP):
        # UDP carries its own authoritative length, which covers both IP
        # versions without needing the IP header arithmetic above.
        payload_bytes = max(0, int(transport.ulen) - _UDP_HEADER_BYTES)
        payload_bytes = min(payload_bytes, ip_payload_bytes)
        segment = None
    else:
        # ICMP, IPsec, bare IP and anything else carry no layer-4 payload for
        # the purpose of this metric.
        payload_bytes = 0
        segment = None

    return ParsedPacket(
        timestamp=float(timestamp),
        ip_bytes=ip_bytes,
        payload_bytes=payload_bytes,
        tcp=segment,
    )


def _build_tcp_segment(
    ip_packet: dpkt.ip.IP | dpkt.ip6.IP6,
    transport: dpkt.tcp.TCP,
    family: int,
    payload_bytes: int,
) -> TcpSegment:
    flags = int(transport.flags)
    return TcpSegment(
        src_ip=socket.inet_ntop(family, ip_packet.src),
        dst_ip=socket.inet_ntop(family, ip_packet.dst),
        src_port=int(transport.sport),
        dst_port=int(transport.dport),
        seq=int(transport.seq),
        ack=int(transport.ack),
        window=int(transport.win),
        payload_bytes=payload_bytes,
        syn=bool(flags & dpkt.tcp.TH_SYN),
        ack_flag=bool(flags & dpkt.tcp.TH_ACK),
        fin=bool(flags & dpkt.tcp.TH_FIN),
        rst=bool(flags & dpkt.tcp.TH_RST),
    )


def _ipv4_total_bytes(ip_packet: dpkt.ip.IP) -> int:
    """Wire size of an IPv4 packet, from its own total-length field.

    Falls back to the packet's serialized size when the field reads zero, which
    happens only on captures taken behind hardware segmentation offload.
    """

    declared = int(ip_packet.len)
    return declared if declared > 0 else len(bytes(ip_packet))


def _ipv6_total_bytes(ip_packet: dpkt.ip6.IP6) -> int:
    """Wire size of an IPv6 packet: fixed header plus its payload-length field."""

    declared = int(ip_packet.plen)
    if declared > 0:
        return _IPV6_FIXED_HEADER_BYTES + declared
    return max(len(bytes(ip_packet)), _IPV6_FIXED_HEADER_BYTES)


def _ipv6_extension_bytes(ip_packet: dpkt.ip6.IP6) -> int:
    """Total size of any IPv6 extension headers preceding the layer-4 header.

    These sit between the fixed header and the transport header, so they count
    as header bytes rather than payload.
    """

    headers = getattr(ip_packet, "extension_hdrs", None)
    if not headers:
        return 0
    return sum(
        int(getattr(header, "length", 0) or 0)
        for header in headers.values()
        if header is not None
    )
