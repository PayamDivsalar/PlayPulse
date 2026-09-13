"""What every repository has in common: one statement, one outcome.

The three repositories differ only in their table, their column list and their
``ON CONFLICT`` clause. Everything else -- batching the rows into a single
multi-row ``INSERT`` and counting what came back -- is here, so a new topic is
a subclass with three constants and a row builder.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from psycopg2.extras import execute_values

from storage_consumer.core.events import BoundRecord

# How many rows go into one INSERT statement. Larger than the biggest
# max_poll_records (500, for reviews) so a batch is normally one round trip.
# psycopg2 splits anything above this into several statements in the same
# transaction, which is correct but costs an extra round trip each.
PAGE_SIZE = 1000


@dataclass(frozen=True, slots=True)
class WriteOutcome:
    """What one batch write did, in the terms an operator cares about.

    The insert/update split comes from ``RETURNING (xmax = 0)``: Postgres
    leaves ``xmax`` at zero for a freshly inserted tuple and non-zero for one
    an ``ON CONFLICT DO UPDATE`` replaced. It is what makes a log line answer
    "is this a backfill or steady state" at a glance.

    ``skipped`` counts rows the database declined: a conflicting key under
    ``DO NOTHING``, or an update the monotonic guard rejected. Under normal
    operation it is zero; a sustained non-zero value means something is
    replaying.
    """

    inserted: int = 0
    updated: int = 0
    skipped: int = 0

    @property
    def written(self) -> int:
        return self.inserted + self.updated


class Repository(ABC):
    """One table's writer.

    Instances hold no connection and no state: the cursor is passed in by the
    worker so the write joins the batch's transaction rather than opening one
    of its own. That is what lets the data rows and their dead-letter siblings
    commit atomically.
    """

    #: Table this repository writes to.
    table: str
    #: Columns of the INSERT, in the order ``_row`` returns them.
    columns: tuple[str, ...]
    #: The ``ON CONFLICT ...`` text, ending in the RETURNING expression.
    conflict_clause: str

    @abstractmethod
    def _row(self, record: BoundRecord) -> tuple[Any, ...]:
        """Project one bound event onto ``columns``."""

    def upsert(self, cursor: Any, records: Sequence[BoundRecord]) -> WriteOutcome:
        """Write a whole batch in one statement.

        The caller must have run ``dedupe_by_key`` first. Postgres rejects an
        ``ON CONFLICT DO UPDATE`` that would touch the same row twice within
        one statement, so a batch holding a repeated conflict key aborts the
        transaction rather than writing anything.
        """

        if not records:
            return WriteOutcome()

        statement = (
            f"INSERT INTO {self.table} ({', '.join(self.columns)}) "
            f"VALUES %s {self.conflict_clause}"
        )
        returned = execute_values(
            cursor,
            statement,
            [self._row(record) for record in records],
            page_size=PAGE_SIZE,
            fetch=True,
        )

        # Every returned row is `(xmax = 0) AS inserted`. Rows the conflict
        # clause declined return nothing at all, which is what makes the
        # shortfall the skipped count.
        inserted = sum(1 for row in returned if row[0])
        return WriteOutcome(
            inserted=inserted,
            updated=len(returned) - inserted,
            skipped=len(records) - len(returned),
        )
