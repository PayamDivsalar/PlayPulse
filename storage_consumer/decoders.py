"""Turn a raw Kafka message body into a validated event, or reject it.

Pure functions over ``dict``, stdlib only. Everything that can go wrong here is
**permanent**: a producer that sends a malformed timestamp will send exactly
the same malformed timestamp on redelivery, so the caller dead-letters the
record and advances the offset rather than retrying.

None of the three topics carries an envelope, headers or a schema-registry id,
so the field sets below *are* the contract. The ``*_FIELDS`` constants exist so
a test can assert exactly which keys each decoder reads, which is what catches
a producer renaming one.

Nullability follows one rule, applied consistently:

* **Identity and key columns** are required. A missing one dead-letters the
  message, because a row without them cannot be written or de-duplicated.
* **Measurement columns** are optional. A missing one is stored as ``NULL``
  (or its documented default), because an absent measurement is not an invalid
  record.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from datetime import datetime, timezone
from typing import Any

from storage_consumer.events import AppStatsEvent, NetworkMetricEvent, ReviewEvent
from storage_consumer.exceptions import MessageDecodeError

logger = logging.getLogger(__name__)

APP_STATS_FIELDS = frozenset(
    {
        "package_name",
        "min_installs",
        "score",
        "ratings",
        "reviews_count",
        "version",
        "ad_supported",
        "app_updated_at",
        "crawled_at",
    }
)

REVIEW_FIELDS = frozenset(
    {
        "package_name",
        "review_id",
        "user_name",
        "thumbs_up_count",
        "score",
        "content",
        "at",
        "crawled_at",
    }
)

NETWORK_METRIC_FIELDS = frozenset(
    {
        "analysis_id",
        "package_name",
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
    }
)

VALID_SCENARIOS = frozenset({"UPLOAD", "DOWNLOAD"})

# Column limits, enforced here rather than left to Postgres.
#
# This looks like belt and braces but it is the difference between one bad
# message and an outage. A string longer than its VARCHAR, or an integer wider
# than its column, raises a DataError that aborts the *entire batch
# transaction* -- taking hundreds of valid rows with it. The batch is then
# retried, fails identically, and the pipeline is in a crash loop. Catching it
# here dead-letters the one offending record and lets the rest commit.
_SMALLINT_MIN, _SMALLINT_MAX = -32_768, 32_767
_INTEGER_MIN, _INTEGER_MAX = -2_147_483_648, 2_147_483_647
_BIGINT_MIN, _BIGINT_MAX = -9_223_372_036_854_775_808, 9_223_372_036_854_775_807

_PACKAGE_NAME_MAX = 255
_REVIEW_ID_MAX = 255
_USER_NAME_MAX = 255
_VERSION_MAX = 50
_PCAP_FILENAME_MAX = 255


def parse_json(raw: bytes | bytearray | str | None) -> dict[str, Any]:
    """Decode a message body into a JSON object.

    Separate from the three decoders because the failure modes are different
    in kind: this is "the bytes are not a JSON object at all", before any field
    has been looked at.

    Raises:
        MessageDecodeError: on an empty body, invalid UTF-8, invalid JSON, or
            valid JSON that is not an object (a bare list or number).
    """

    if raw is None:
        raise MessageDecodeError("Message body is empty (tombstone or null value).")
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise MessageDecodeError(
                f"Message body is not valid UTF-8: {exc}"
            ) from exc
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, ValueError) as exc:
        raise MessageDecodeError(f"Message body is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise MessageDecodeError(
            f"Message body must be a JSON object, got {type(payload).__name__}."
        )
    return payload


def decode_app_stats(payload: dict[str, Any]) -> AppStatsEvent:
    """Decode one ``app-stats`` message."""

    _warn_on_unknown_fields(payload, APP_STATS_FIELDS, "app-stats")
    return AppStatsEvent(
        package_name=_require_str(payload, "package_name", _PACKAGE_NAME_MAX),
        crawled_at=_require_timestamp(payload, "crawled_at"),
        min_installs=_optional_int(payload, "min_installs", _BIGINT_MIN, _BIGINT_MAX),
        score=_optional_float(payload, "score"),
        ratings=_optional_int(payload, "ratings", _BIGINT_MIN, _BIGINT_MAX),
        reviews_count=_optional_int(payload, "reviews_count", _BIGINT_MIN, _BIGINT_MAX),
        version=_optional_str(payload, "version", _VERSION_MAX),
        ad_supported=_optional_bool(payload, "ad_supported"),
        app_updated_at=_optional_timestamp(payload, "app_updated_at"),
    )


def decode_review(payload: dict[str, Any]) -> ReviewEvent:
    """Decode one ``reviews`` message.

    ``at`` is required even though it is a timestamp rather than an id: the
    column is ``NOT NULL`` and it is the axis every review report is drawn
    against, so a review without one has nothing to plot.
    """

    _warn_on_unknown_fields(payload, REVIEW_FIELDS, "reviews")
    return ReviewEvent(
        package_name=_require_str(payload, "package_name", _PACKAGE_NAME_MAX),
        review_id=_require_str(payload, "review_id", _REVIEW_ID_MAX),
        at=_require_timestamp(payload, "at"),
        crawled_at=_require_timestamp(payload, "crawled_at"),
        user_name=_optional_str(payload, "user_name", _USER_NAME_MAX),
        # Absent means zero, which is the column's documented default, not a
        # missing measurement.
        thumbs_up_count=_optional_int(
            payload, "thumbs_up_count", _INTEGER_MIN, _INTEGER_MAX
        )
        or 0,
        score=_optional_int(payload, "score", _SMALLINT_MIN, _SMALLINT_MAX),
        # content is TEXT, so it has no length to check.
        content=_optional_str(payload, "content"),
    )


def decode_network_metric(payload: dict[str, Any]) -> NetworkMetricEvent:
    """Decode one ``network-metrics`` message."""

    _warn_on_unknown_fields(payload, NETWORK_METRIC_FIELDS, "network-metrics")
    return NetworkMetricEvent(
        analysis_id=_require_uuid(payload, "analysis_id"),
        package_name=_require_str(payload, "package_name", _PACKAGE_NAME_MAX),
        scenario=_require_scenario(payload),
        analyzed_at=_require_timestamp(payload, "analyzed_at"),
        bytes_transferred_total=_require_int(
            payload, "bytes_transferred_total", _BIGINT_MIN, _BIGINT_MAX
        ),
        bytes_payload_total=_require_int(
            payload, "bytes_payload_total", _BIGINT_MIN, _BIGINT_MAX
        ),
        overhead_ratio=_require_float(payload, "overhead_ratio"),
        rtt_handshake=_optional_float(payload, "rtt_handshake"),
        retransmission_count=_counter(payload, "retransmission_count"),
        out_of_order_count=_counter(payload, "out_of_order_count"),
        spurious_retransmission_count=_counter(
            payload, "spurious_retransmission_count"
        ),
        zero_window_count=_counter(payload, "zero_window_count"),
        tcp_reset_count=_counter(payload, "tcp_reset_count"),
        source_pcap_filename=_optional_str(
            payload, "source_pcap_filename", _PCAP_FILENAME_MAX
        ),
    )


def _counter(payload: dict[str, Any], field: str) -> int:
    """An ``INTEGER NOT NULL DEFAULT 0`` packet counter."""

    return _optional_int(payload, field, _INTEGER_MIN, _INTEGER_MAX) or 0


def _warn_on_unknown_fields(
    payload: dict[str, Any], known: frozenset[str], topic: str
) -> None:
    """Log, but do not reject, fields the contract does not mention.

    Rejecting them would mean a producer adding one harmless field
    dead-letters every message on the topic, turning a forward-compatible
    change into an outage. Logging them is how contract drift still gets
    noticed.
    """

    unknown = set(payload) - known
    if unknown:
        logger.debug(
            "Ignoring %s field(s) not in the %s contract: %s",
            len(unknown),
            topic,
            ", ".join(sorted(unknown)),
        )


def _require_str(payload: dict[str, Any], field: str, max_length: int) -> str:
    value = _optional_str(payload, field, max_length)
    if value is None:
        raise MessageDecodeError(f"Required field {field!r} is missing or null.")
    if not value.strip():
        raise MessageDecodeError(f"Required field {field!r} is blank.")
    return value


def _optional_str(
    payload: dict[str, Any], field: str, max_length: int | None = None
) -> str | None:
    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise MessageDecodeError(
            f"Field {field!r} must be a string or null, got {type(value).__name__}."
        )
    if max_length is not None and len(value) > max_length:
        raise MessageDecodeError(
            f"Field {field!r} is {len(value)} characters, longer than the "
            f"{max_length} its column holds."
        )
    return value


def _require_uuid(payload: dict[str, Any], field: str) -> str:
    """A string that Postgres will accept into a ``UUID`` column.

    Validated here because ``analysis_id`` is the only thing standing between
    a redelivered ``network-metrics`` message and a duplicate row, and because
    a malformed one would otherwise abort the batch rather than itself.
    """

    value = _require_str(payload, field, 36)
    try:
        uuid.UUID(value)
    except ValueError as exc:
        raise MessageDecodeError(
            f"Field {field!r} is not a valid UUID ({value!r}): {exc}"
        ) from exc
    return value


def _require_int(
    payload: dict[str, Any], field: str, minimum: int, maximum: int
) -> int:
    value = _optional_int(payload, field, minimum, maximum)
    if value is None:
        raise MessageDecodeError(f"Required field {field!r} is missing or null.")
    return value


def _optional_int(
    payload: dict[str, Any],
    field: str,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int | None:
    value = payload.get(field)
    if value is None:
        return None
    # bool is a subclass of int, so `isinstance(True, int)` is True. Letting it
    # through would silently store a flag as the number 1.
    if isinstance(value, bool) or not isinstance(value, int):
        raise MessageDecodeError(
            f"Field {field!r} must be an integer or null, got {type(value).__name__}."
        )
    if (minimum is not None and value < minimum) or (
        maximum is not None and value > maximum
    ):
        raise MessageDecodeError(
            f"Field {field!r} is {value}, outside the range {minimum}..{maximum} "
            "its column holds."
        )
    return value


def _require_float(payload: dict[str, Any], field: str) -> float:
    value = _optional_float(payload, field)
    if value is None:
        raise MessageDecodeError(f"Required field {field!r} is missing or null.")
    return value


def _optional_float(payload: dict[str, Any], field: str) -> float | None:
    value = payload.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MessageDecodeError(
            f"Field {field!r} must be a number or null, got {type(value).__name__}."
        )
    # Python's json module accepts NaN and Infinity, and Postgres accepts them
    # into a double precision column, so nothing downstream would complain --
    # it would just quietly poison every average taken over the column.
    if not math.isfinite(value):
        raise MessageDecodeError(f"Field {field!r} is {value}, which is not a number.")
    # An integral JSON number is a perfectly good float: `"score": 4` and
    # `"score": 4.0` mean the same thing and both belong in a float column.
    return float(value)


def _optional_bool(payload: dict[str, Any], field: str) -> bool | None:
    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, bool):
        raise MessageDecodeError(
            f"Field {field!r} must be a boolean or null, got {type(value).__name__}."
        )
    return value


def _require_scenario(payload: dict[str, Any]) -> str:
    value = _require_str(payload, "scenario", 20)
    if value not in VALID_SCENARIOS:
        raise MessageDecodeError(
            f"Field 'scenario' is {value!r}, expected one of: "
            f"{', '.join(sorted(VALID_SCENARIOS))}."
        )
    return value


def _require_timestamp(payload: dict[str, Any], field: str) -> datetime:
    value = _optional_timestamp(payload, field)
    if value is None:
        raise MessageDecodeError(f"Required field {field!r} is missing or null.")
    return value


def _optional_timestamp(payload: dict[str, Any], field: str) -> datetime | None:
    """Parse an ISO-8601 timestamp, forcing it to be offset-aware.

    Most timestamps on these topics carry a UTC offset, but ``reviews.at`` often
    does not: ``google-play-scraper`` returns naive datetimes and
    ``crawler/playstore_client.py`` calls ``.isoformat()`` on them, so the wire
    value can be ``"2026-09-01T10:00:00"``.

    The columns are all ``TIMESTAMPTZ``. Handing psycopg2 a naive datetime
    would make Postgres interpret it in the session time zone, so the same
    instant would land differently depending on where the container runs. UTC
    is the right assumption because it is what the scraper reports in, and
    attaching it explicitly is what keeps one column from mixing two meanings.
    """

    value = payload.get(field)
    if value is None:
        return None
    if not isinstance(value, str):
        raise MessageDecodeError(
            f"Field {field!r} must be an ISO-8601 string or null, got "
            f"{type(value).__name__}."
        )
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise MessageDecodeError(
            f"Field {field!r} is not a valid ISO-8601 timestamp ({value!r}): {exc}"
        ) from exc
    if parsed.tzinfo is None:
        logger.debug(
            "Field %r arrived without a UTC offset (%r); assuming UTC.", field, value
        )
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
