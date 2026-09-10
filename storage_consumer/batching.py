"""The pure transformations a batch of Kafka records goes through.

Three steps, in order, between ``consumer.poll()`` and the SQL:

1. ``decode_batch`` — bytes to typed events, routing anything unusable to the
   dead-letter list instead of raising.
2. ``bind_application_ids`` — attach the foreign key, dead-lettering names the
   registry does not know.
3. ``dedupe_by_key`` — collapse duplicates within the batch.

Stdlib only, like ``events.py`` and ``decoders.py``. Kafka records are consumed
structurally (``.topic``, ``.partition``, ``.offset``, ``.key``, ``.value``),
so the worker's tests can drive these functions with plain stubs and no broker.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from storage_consumer.decoders import parse_json
from storage_consumer.events import (
    BoundRecord,
    ConflictKey,
    DecodedEvent,
    DecodedRecord,
    RecordCoordinates,
    RejectedRecord,
)
from storage_consumer.exceptions import MessageDecodeError

logger = logging.getLogger(__name__)

Decoder = Callable[[dict[str, Any]], DecodedEvent]

# A key longer than this is not a package name, it is a bug or an attack. The
# column is TEXT so there is no hard limit, but an unbounded key would let one
# malformed record bloat the dead-letter table.
_MAX_KEY_CHARS = 1024


def decode_batch(
    records: Iterable[Any], decoder: Decoder
) -> tuple[list[DecodedRecord], list[RejectedRecord]]:
    """Decode every record, partitioning them into accepted and rejected.

    Never raises on bad input. That is the whole point: a single malformed
    message must not be able to abort a batch of five hundred good ones, and
    it must not be retried forever either, because retrying it would block its
    partition permanently.
    """

    decoded: list[DecodedRecord] = []
    rejected: list[RejectedRecord] = []

    for record in records:
        coordinates = coordinates_of(record)
        try:
            event = decoder(parse_json(record.value))
        except MessageDecodeError as exc:
            rejected.append(RejectedRecord(coordinates=coordinates, reason=str(exc)))
        else:
            decoded.append(DecodedRecord(coordinates=coordinates, event=event))

    return decoded, rejected


def coordinates_of(record: Any) -> RecordCoordinates:
    """Read a Kafka record's identity and body without decoding either."""

    return RecordCoordinates(
        topic=record.topic,
        partition=record.partition,
        offset=record.offset,
        key=_decode_key(record.key),
        payload=_as_bytes(record.value),
    )


def bind_application_ids(
    records: Sequence[DecodedRecord], application_ids: Mapping[str, int]
) -> tuple[list[BoundRecord], list[RejectedRecord]]:
    """Attach the foreign key, rejecting names the registry does not know.

    An unknown ``package_name`` is permanent rather than transient: the row
    cannot be written without a foreign key, and waiting for somebody to
    register the app would stall every other app on the partition. The record
    is dead-lettered and can be replayed once the app exists.
    """

    bound: list[BoundRecord] = []
    rejected: list[RejectedRecord] = []

    for record in records:
        package_name = record.event.package_name
        application_id = application_ids.get(package_name)
        if application_id is None:
            rejected.append(
                RejectedRecord(
                    coordinates=record.coordinates,
                    reason=(
                        f"Unknown package_name {package_name!r}: no row in "
                        "apps_registry_application. Register the application "
                        "and replay this offset to store it."
                    ),
                )
            )
            continue
        bound.append(
            BoundRecord(
                coordinates=record.coordinates,
                event=record.event,
                application_id=application_id,
            )
        )

    return bound, rejected


def dedupe_by_key(records: Sequence[BoundRecord]) -> list[BoundRecord]:
    """Keep one record per conflict key: the one with the latest ``observed_at``.

    **Mandatory, not an optimisation.** Postgres raises "ON CONFLICT DO UPDATE
    command cannot affect row a second time" when the same conflict key appears
    twice in one multi-row statement, so a batch holding two versions of a
    review would abort the whole transaction. The batch is then retried, hits
    the same error, and the pipeline is in a crash loop that only ever shows up
    under replay -- long after the code was reviewed.

    Ties keep the earlier record, so the result is stable for two messages
    stamped at the same instant.
    """

    winners: dict[ConflictKey, BoundRecord] = {}
    order: list[ConflictKey] = []

    for record in records:
        key = record.event.conflict_key
        incumbent = winners.get(key)
        if incumbent is None:
            winners[key] = record
            order.append(key)
        elif record.event.observed_at > incumbent.event.observed_at:
            winners[key] = record

    if len(order) != len(records):
        logger.debug(
            "Collapsed %s duplicate record(s) within the batch.",
            len(records) - len(order),
        )
    return [winners[key] for key in order]


def _decode_key(key: Any) -> str | None:
    """Best-effort rendering of the message key for the dead-letter table.

    Never raises: the key is diagnostic information, and failing to read it
    must not turn a dead-letterable record into an unhandled exception.
    """

    if key is None:
        return None
    if isinstance(key, (bytes, bytearray)):
        text = key.decode("utf-8", errors="replace")
    else:
        text = str(key)
    return text[:_MAX_KEY_CHARS]


def _as_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return str(value).encode("utf-8", errors="replace")
