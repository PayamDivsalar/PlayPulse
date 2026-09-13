"""Writer for ``app_stats``."""

from __future__ import annotations

from typing import Any

from storage_consumer.core.events import AppStatsEvent, BoundRecord
from storage_consumer.persistence.repository import Repository


class AppStatsRepository(Repository):
    """Append-only inserts, made harmless on redelivery by ``DO NOTHING``.

    There is deliberately no ``DO UPDATE`` branch. A second message carrying
    the same ``(application_id, crawled_at)`` is by definition a redelivery of
    the first -- the crawler stamps ``crawled_at`` per message with microsecond
    precision and emits one message per app per cycle -- so there is nothing
    newer in it to write. A genuine next crawl carries a new ``crawled_at`` and
    inserts a new row, which is what keeps the table append-only.
    """

    table = "app_stats"
    columns = (
        "application_id",
        "min_installs",
        "score",
        "ratings",
        "reviews_count",
        "version",
        "ad_supported",
        "app_updated_at",
        "crawled_at",
    )
    # A conflicting row returns nothing, so it lands in `skipped` and the
    # count of returned rows is exactly the number inserted.
    conflict_clause = (
        "ON CONFLICT (application_id, crawled_at) DO NOTHING "
        "RETURNING (xmax = 0) AS inserted"
    )

    def _row(self, record: BoundRecord) -> tuple[Any, ...]:
        event = record.event
        assert isinstance(event, AppStatsEvent)
        return (
            record.application_id,
            event.min_installs,
            event.score,
            event.ratings,
            event.reviews_count,
            event.version,
            event.ad_supported,
            event.app_updated_at,
            event.crawled_at,
        )
