"""Writer for ``reviews``, the one table in this subsystem that updates rows."""

from __future__ import annotations

from typing import Any

from storage_consumer.events import BoundRecord, ReviewEvent
from storage_consumer.persistence.repository import Repository


class ReviewRepository(Repository):
    """Upsert on ``review_id``, guarded so a stale message cannot win.

    Two columns are deliberately absent from the ``DO UPDATE SET`` list:

    * ``first_seen_at`` keeps its ``DEFAULT now()`` from the insert, so it goes
      on meaning "when we first saw this review" no matter how many times the
      row is re-synced.
    * ``sentiment`` belongs to the future ``sentiment/`` subsystem. Touching it
      here would wipe an analysis every time the crawler re-read the review.
    """

    table = "reviews"
    columns = (
        "application_id",
        "review_id",
        "user_name",
        "thumbs_up_count",
        "score",
        "content",
        "at",
        "last_synced_at",
    )
    # The WHERE is what stops a stale message winning. Without it, two things
    # quietly corrupt the table: a replay rewrites a fresh row with old values,
    # and -- because Kafka only orders within a partition -- a slow retry of an
    # earlier crawl can land after a later one and overwrite the newer
    # thumbs_up_count. With it, both become no-ops counted as skipped.
    conflict_clause = """
        ON CONFLICT (review_id) DO UPDATE SET
            application_id  = EXCLUDED.application_id,
            user_name       = EXCLUDED.user_name,
            thumbs_up_count = EXCLUDED.thumbs_up_count,
            score           = EXCLUDED.score,
            content         = EXCLUDED.content,
            at              = EXCLUDED.at,
            last_synced_at  = EXCLUDED.last_synced_at
        WHERE EXCLUDED.last_synced_at > reviews.last_synced_at
        RETURNING (xmax = 0) AS inserted
    """

    def _row(self, record: BoundRecord) -> tuple[Any, ...]:
        event = record.event
        assert isinstance(event, ReviewEvent)
        return (
            record.application_id,
            event.review_id,
            event.user_name,
            event.thumbs_up_count,
            event.score,
            event.content,
            event.at,
            # last_synced_at is the message's crawled_at, which is what the
            # guard in conflict_clause compares and what makes replay ordering
            # meaningful.
            event.crawled_at,
        )
