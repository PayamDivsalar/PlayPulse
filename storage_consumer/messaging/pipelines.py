"""What differs between the three topics, in one table.

Everything else -- the poll loop, the transaction shape, the retry policy, the
shutdown handling -- is topic-agnostic and lives in ``worker.py``. Adding a
fourth topic means adding a repository and one ``_TOPIC_DEFAULTS`` entry,
nothing more.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from storage_consumer.config import Settings
from storage_consumer.core.decoders import (
    decode_app_stats,
    decode_network_metric,
    decode_review,
)
from storage_consumer.core.events import DecodedEvent
from storage_consumer.persistence.app_stats_repository import AppStatsRepository
from storage_consumer.persistence.network_metric_repository import (
    NetworkMetricRepository,
)
from storage_consumer.persistence.repository import Repository
from storage_consumer.persistence.review_repository import ReviewRepository

Decoder = Callable[[dict[str, Any]], DecodedEvent]


@dataclass(frozen=True, slots=True)
class PipelineSpec:
    """One topic's worth of configuration, resolved for this process."""

    name: str
    topic: str
    group_id: str
    decoder: Decoder
    repository: Repository
    #: The natural key that makes redelivery a no-op, logged at startup so an
    #: operator can confirm the idempotency story without reading the SQL.
    conflict_key: str
    max_poll_records: int
    poll_timeout_ms: int


@dataclass(frozen=True, slots=True)
class _TopicDefaults:
    """The half of a spec that is fixed per topic rather than per process."""

    decoder: Decoder
    repository: type[Repository]
    conflict_key: str
    #: Batch size when no STORAGE_MAX_POLL_RECORDS[_<PIPELINE>] override is set.
    max_poll_records: int


# Keyed by pipeline name, which is also the topic name.
#
# The batch sizes are the numbers the "one thread per pipeline" decision exists
# to allow: reviews are a throughput problem (up to 200,000 messages in a 5-15
# minute burst) and want big batches, while network-metrics is a latency
# problem (a handful of messages a day, produced on demand) and wants to write
# what it has rather than wait for a batch to fill. Operators override them
# without touching this table; see Settings.max_poll_records_for.
_TOPIC_DEFAULTS: dict[str, _TopicDefaults] = {
    "app-stats": _TopicDefaults(
        decoder=decode_app_stats,
        repository=AppStatsRepository,
        conflict_key="(application_id, crawled_at)",
        max_poll_records=200,
    ),
    "reviews": _TopicDefaults(
        decoder=decode_review,
        repository=ReviewRepository,
        conflict_key="(review_id)",
        max_poll_records=500,
    ),
    "network-metrics": _TopicDefaults(
        decoder=decode_network_metric,
        repository=NetworkMetricRepository,
        conflict_key="(analysis_id)",
        max_poll_records=50,
    ),
}


def build_pipeline(name: str, settings: Settings) -> PipelineSpec:
    """Resolve one pipeline's spec against the current settings."""

    defaults = _TOPIC_DEFAULTS[name]
    return PipelineSpec(
        name=name,
        topic=name,
        group_id=settings.group_id(name),
        decoder=defaults.decoder,
        # One instance per spec, so nothing is shared between pipeline threads
        # even though repositories hold no state.
        repository=defaults.repository(),
        conflict_key=defaults.conflict_key,
        max_poll_records=settings.max_poll_records_for(
            name, defaults.max_poll_records
        ),
        poll_timeout_ms=settings.poll_timeout_ms,
    )


def build_pipelines(settings: Settings) -> tuple[PipelineSpec, ...]:
    """Build a spec for each pipeline this process was asked to run.

    ``Settings`` has already rejected unknown and repeated names, so anything
    reaching here is a name this module knows.
    """

    return tuple(build_pipeline(name, settings) for name in settings.pipelines)
