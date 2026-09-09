"""Derive capture metadata from a pcap filename.

Captures are produced by hand on a phone, so the filename is the only place
where the operator can record *which* application and *which* scenario a file
represents. Encoding it in the name — rather than requiring two CLI flags on
every invocation — is what makes unattended batch ingestion possible.

Convention::

    <package_name>__<scenario>__<YYYYMMDDTHHMMSS>.pcap
    com.whatsapp__upload__20260907T141500.pcap

The double underscore is the separator because ``.`` and ``_`` are both legal
inside an Android package name, so no single-character separator is safe.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from network_analyzer.exceptions import FilenameConventionError
from network_analyzer.models import CaptureDescriptor, Scenario

_CAPTURE_TIMESTAMP_FORMAT = "%Y%m%dT%H%M%S"

_SUPPORTED_SUFFIXES = frozenset({".pcap", ".pcapng"})

_FILENAME_PATTERN = re.compile(
    r"^(?P<package>[A-Za-z][A-Za-z0-9_]*(?:\.[A-Za-z0-9_]+)+)"
    r"__(?P<scenario>upload|download)"
    r"__(?P<timestamp>\d{8}T\d{6})$",
    re.IGNORECASE,
)

_EXAMPLE = "com.whatsapp__upload__20260907T141500.pcap"


def parse_capture_filename(path: str | Path) -> CaptureDescriptor:
    """Extract package name, scenario, and capture time from a filename.

    Only the final path component is inspected; directories are ignored.

    Raises:
        FilenameConventionError: if the suffix is not a pcap suffix, the stem
            does not match the convention, or the timestamp is not a real
            calendar time.
    """

    name = Path(path).name
    stem = Path(name).stem

    suffix = Path(name).suffix.lower()
    if suffix not in _SUPPORTED_SUFFIXES:
        supported = ", ".join(sorted(_SUPPORTED_SUFFIXES))
        raise FilenameConventionError(
            f"{name!r} does not look like a capture file: expected one of "
            f"{supported}, got {suffix or 'no suffix'!r}."
        )

    match = _FILENAME_PATTERN.match(stem)
    if match is None:
        raise FilenameConventionError(
            f"{name!r} does not follow the capture naming convention "
            f"'<package_name>__<upload|download>__<YYYYMMDDTHHMMSS>{suffix}'. "
            f"Example: {_EXAMPLE}. "
            "Pass --package and --scenario explicitly to analyze a file whose "
            "name cannot be changed."
        )

    raw_timestamp = match.group("timestamp")
    try:
        captured_at = datetime.strptime(raw_timestamp, _CAPTURE_TIMESTAMP_FORMAT)
    except ValueError as exc:
        raise FilenameConventionError(
            f"{name!r} carries {raw_timestamp!r}, which is not a valid "
            "YYYYMMDDTHHMMSS timestamp."
        ) from exc

    return CaptureDescriptor(
        package_name=match.group("package"),
        scenario=Scenario.parse(match.group("scenario")),
        # Recorded as UTC. The value is informational only: it never reaches the
        # wire contract, because the operator's phone clock and timezone are not
        # trustworthy enough to persist as capture time.
        captured_at=captured_at.replace(tzinfo=timezone.utc),
    )


def build_capture_filename(
    package_name: str, scenario: Scenario, captured_at: datetime
) -> str:
    """Build a convention-compliant filename.

    The inverse of :func:`parse_capture_filename`, used by tests and available
    to operators who need to rename an existing capture.
    """

    stamp = captured_at.strftime(_CAPTURE_TIMESTAMP_FORMAT)
    return f"{package_name}__{scenario.value.lower()}__{stamp}.pcap"
