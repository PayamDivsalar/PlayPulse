"""Read reviews and write sentiment results in batches."""

from __future__ import annotations

from typing import Any

from psycopg2.extras import execute_values

# Matches storage_consumer's execute_values page size habit.
_PAGE_SIZE = 100

# Keyset pagination by id — every run walks the whole table regardless of the
# current sentiment value. ``after_id`` advances within one job so overwriting
# labels cannot cause an infinite re-fetch loop.
_FETCH_SQL = """
    SELECT id, content
    FROM reviews
    WHERE content IS NOT NULL
      AND content != ''
      AND id > %s
    ORDER BY id
    LIMIT %s
"""

# Batch UPDATE via a VALUES list — one round-trip instead of N statements.
_UPDATE_SQL = """
    UPDATE reviews AS r
    SET sentiment = data.sentiment
    FROM (VALUES %s) AS data(id, sentiment)
    WHERE r.id = data.id::bigint
"""


class ReviewRepository:
    """Cursor-bound access to ``reviews`` for the sentiment job."""

    def fetch_batch(
        self, cursor: Any, batch_size: int, *, after_id: int = 0
    ) -> list[tuple[int, str]]:
        """Return the next ``(id, content)`` page with ``id > after_id``.

        Existing ``sentiment`` values are ignored: every eligible row is
        selected so a later ``update_sentiments`` can overwrite them.
        """

        cursor.execute(_FETCH_SQL, (after_id, batch_size))
        return [(int(row[0]), str(row[1])) for row in cursor.fetchall()]

    def update_sentiments(
        self, cursor: Any, results: list[tuple[int, str]]
    ) -> None:
        """Write ``(id, sentiment)`` pairs with a single ``execute_values`` call."""

        if not results:
            return
        execute_values(
            cursor,
            _UPDATE_SQL,
            results,
            template="(%s::bigint, %s::varchar)",
            page_size=_PAGE_SIZE,
        )
