"""The typed values this subsystem passes between its layers.

Imports stdlib only, on purpose. Together with ``decoders.py`` and
``batching.py`` this module forms the pure core: no Kafka, no psycopg2, no
network. That is what lets the wire contract be asserted in tests without a
broker or a database.

Two ideas run through every event type here:

* ``conflict_key`` — the natural key that makes a redelivered message a no-op.
  It matches the ``UNIQUE`` constraint on the target table exactly, and is what
  ``batching.dedupe_by_key`` collapses on.
* ``observed_at`` — which of two duplicates is the fresher one. Needed because
  ``ON CONFLICT DO UPDATE`` cannot touch the same row twice in one statement,
  so a batch holding the same key twice must pick a winner *before* the write.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

# The natural key of a row, as a hashable value. A tuple for the composite
# key on app_stats, a plain string for the single-column keys.
ConflictKey = str | tuple[object, ...]


@dataclass(frozen=True, slots=True)
class AppStatsEvent:
    """One ``app-stats`` message: an app's Play Store figures at one moment.

    Every field but ``package_name`` and ``crawled_at`` is nullable, because
    each is a measurement rather than an identity: ``minInstalls`` is genuinely
    absent for a newly published app, and discarding the whole row over one
    missing metric would lose the metrics that did arrive.
    """

    package_name: str
    crawled_at: datetime
    min_installs: int | None = None
    score: float | None = None
    ratings: int | None = None
    reviews_count: int | None = None
    version: str | None = None
    ad_supported: bool | None = None
    app_updated_at: datetime | None = None

    @property
    def conflict_key(self) -> ConflictKey:
        """Matches ``UNIQUE (application_id, crawled_at)``.

        Keyed on ``package_name`` rather than ``application_id`` because
        de-duplication happens before the foreign key is resolved, and the two
        are one-to-one.
        """

        return (self.package_name, self.crawled_at)

    @property
    def observed_at(self) -> datetime:
        return self.crawled_at


@dataclass(frozen=True, slots=True)
class ReviewEvent:
    """One ``reviews`` message: a single user review.

    The crawler publishes one message per review rather than a list per app,
    so there is no batch unpacking here.

    ``thumbs_up_count`` is an ``int`` and not ``int | None``: the column is
    ``NOT NULL DEFAULT 0`` and the decoder maps an absent count to zero, which
    is the documented default rather than a guess.
    """

    package_name: str
    review_id: str
    at: datetime
    crawled_at: datetime
    user_name: str | None = None
    thumbs_up_count: int = 0
    score: int | None = None
    content: str | None = None

    @property
    def conflict_key(self) -> ConflictKey:
        """Matches ``UNIQUE (review_id)``."""

        return self.review_id

    @property
    def observed_at(self) -> datetime:
        """When the crawler read this review, which becomes ``last_synced_at``.

        Not the review's own ``at``: two messages about the same review carry
        the same ``at`` but different ``crawled_at``, and it is the sync time
        that decides which one holds the newer ``thumbs_up_count``.
        """

        return self.crawled_at


@dataclass(frozen=True, slots=True)
class NetworkMetricEvent:
    """One ``network-metrics`` message: the analysis of a single pcap capture.

    The counters default to zero to match their ``NOT NULL DEFAULT 0`` columns.
    ``rtt_handshake`` is the one genuinely optional measurement: a capture of
    an already-established connection holds no handshake to time.
    """

    analysis_id: str
    package_name: str
    scenario: str
    analyzed_at: datetime
    bytes_transferred_total: int
    bytes_payload_total: int
    overhead_ratio: float
    rtt_handshake: float | None = None
    retransmission_count: int = 0
    out_of_order_count: int = 0
    spurious_retransmission_count: int = 0
    zero_window_count: int = 0
    tcp_reset_count: int = 0
    source_pcap_filename: str | None = None

    @property
    def conflict_key(self) -> ConflictKey:
        """Matches ``UNIQUE (analysis_id)``."""

        return self.analysis_id

    @property
    def observed_at(self) -> datetime:
        return self.analyzed_at


DecodedEvent = AppStatsEvent | ReviewEvent | NetworkMetricEvent


@dataclass(frozen=True, slots=True)
class RecordCoordinates:
    """Where a message came from, and what it said.

    Carried alongside every decoded event so that a failure discovered *after*
    decoding — an unknown ``package_name`` is the case that matters — can still
    be dead-lettered against the exact Kafka record that caused it.

    ``(topic, partition, offset)`` is the record's identity and the unique key
    of ``dead_letter_events``.
    """

    topic: str
    partition: int
    offset: int
    key: str | None = None
    payload: bytes | None = None


@dataclass(frozen=True, slots=True)
class DecodedRecord:
    """A message that passed decoding, still tied to its Kafka coordinates."""

    coordinates: RecordCoordinates
    event: DecodedEvent


@dataclass(frozen=True, slots=True)
class BoundRecord:
    """A decoded message whose ``package_name`` now has an ``application_id``.

    The last stop before SQL: repositories take these and nothing else.
    """

    coordinates: RecordCoordinates
    event: DecodedEvent
    application_id: int


@dataclass(frozen=True, slots=True)
class RejectedRecord:
    """A message this consumer will never be able to store.

    Written to ``dead_letter_events`` in the same transaction as the batch it
    arrived with, after which the offset advances past it. ``reason`` is the
    only diagnostic an operator gets, so it must name the offending field.
    """

    coordinates: RecordCoordinates
    reason: str
