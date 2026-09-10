"""Resolve ``package_name`` to the ``application_id`` the tables need.

Messages on all three topics carry ``package_name``, never ``application_id``:
the producers deliberately keep database identity out of subsystems whose job
is scraping and packet arithmetic. Turning one into the other is this
subsystem's responsibility because it is the one that owns the schema.

Read straight from Postgres, **not** through the App API endpoint the other two
subsystems use. The consumer already holds database credentials, the lookup is
a single query on a unique index, and putting an HTTP call on the hot write
path would add a failure mode that buys nothing.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from typing import Any

logger = logging.getLogger(__name__)

# The table app_api owns. Read-only from here.
_TABLE = "apps_registry_application"


class ApplicationResolver:
    """A per-thread, time-bounded cache over one lookup query.

    **Per thread, and therefore lock-free.** Each pipeline owns its own
    resolver just as it owns its own consumer and connection; sharing one
    across the three would need a lock on every message for the sake of saving
    a query an hour. Steady state is roughly one query per app per TTL, because
    a whole crawl cycle's worth of messages for an app arrive within minutes of
    each other.

    Misses are not cached. Negative caching would delay picking up an app that
    an operator has just registered, and unknown packages are rare enough that
    re-querying them costs nothing.
    """

    def __init__(
        self, *, ttl_seconds: float, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        # package_name -> (application_id, expires_at)
        self._cache: dict[str, tuple[int, float]] = {}

    def resolve(self, cursor: Any, package_names: Iterable[str]) -> dict[str, int]:
        """Map the given names to ids, querying only those not cached.

        One query per batch for every name it still needs, rather than one
        query per message: at 500 reviews for the same app that is the
        difference between one round trip and five hundred.

        Names with no row are simply absent from the result. The caller
        dead-letters them; raising here would fail a batch over one unknown
        app.
        """

        wanted = set(package_names)
        if not wanted:
            return {}

        now = self._clock()
        resolved: dict[str, int] = {}
        missing: list[str] = []

        for name in wanted:
            cached = self._cache.get(name)
            if cached is not None and cached[1] > now:
                resolved[name] = cached[0]
            else:
                missing.append(name)

        if missing:
            expires_at = now + self._ttl_seconds
            for name, application_id in self._query(cursor, missing).items():
                self._cache[name] = (application_id, expires_at)
                resolved[name] = application_id

        return resolved

    def invalidate(self) -> None:
        """Forget everything. Used by tests and after a reconnect."""

        self._cache.clear()

    def _query(self, cursor: Any, package_names: list[str]) -> dict[str, int]:
        """Look up a batch of names in one statement.

        No ``is_active`` filter, on purpose. Data already produced for an app
        that was just deactivated should still be stored -- the soft delete is
        a statement about what to crawl next, and the schema document is
        explicit that historical data survives it. Activeness is a query-time
        concern.
        """

        cursor.execute(
            f"SELECT package_name, id FROM {_TABLE} WHERE package_name = ANY(%s)",
            (package_names,),
        )
        found = {row[0]: row[1] for row in cursor.fetchall()}

        if len(found) != len(package_names):
            unknown = sorted(set(package_names) - set(found))
            logger.debug(
                "No application row for %s package name(s): %s",
                len(unknown),
                ", ".join(unknown[:10]),
            )
        return found
