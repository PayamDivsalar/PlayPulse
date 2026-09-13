"""Cache behaviour of the package-name resolver, driven by a fake cursor.

The cache is the difference between one query per app per TTL and one query
per message, so its hit, miss and expiry behaviour is worth pinning down
without a database.
"""

from __future__ import annotations

import unittest

from storage_consumer.persistence.application_resolver import ApplicationResolver


class FakeCursor:
    """Answers the resolver's one query from a dict, and counts calls."""

    def __init__(self, registry: dict[str, int] | None = None) -> None:
        self.registry = dict(registry or {})
        self.queries: list[list[str]] = []
        self._rows: list[tuple[str, int]] = []

    def execute(self, statement: str, parameters: tuple) -> None:
        assert "apps_registry_application" in statement
        requested = parameters[0]
        self.queries.append(list(requested))
        self._rows = [
            (name, self.registry[name]) for name in requested if name in self.registry
        ]

    def fetchall(self) -> list[tuple[str, int]]:
        return self._rows


class ManualClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ResolutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()
        self.cursor = FakeCursor({"com.whatsapp": 1, "com.telegram": 2})
        self.resolver = ApplicationResolver(ttl_seconds=300.0, clock=self.clock)

    def test_known_names_resolve_to_their_ids(self) -> None:
        result = self.resolver.resolve(self.cursor, ["com.whatsapp", "com.telegram"])

        self.assertEqual(result, {"com.whatsapp": 1, "com.telegram": 2})

    def test_unknown_names_are_absent_rather_than_raising(self) -> None:
        """One unregistered app must not fail a batch of five hundred rows."""

        result = self.resolver.resolve(self.cursor, ["com.whatsapp", "com.nope"])

        self.assertEqual(result, {"com.whatsapp": 1})

    def test_an_empty_request_queries_nothing(self) -> None:
        self.assertEqual(self.resolver.resolve(self.cursor, []), {})
        self.assertEqual(self.cursor.queries, [])

    def test_a_whole_batch_costs_one_query(self) -> None:
        """500 reviews for one app is one round trip, not five hundred."""

        self.resolver.resolve(self.cursor, ["com.whatsapp"] * 500)

        self.assertEqual(len(self.cursor.queries), 1)

    def test_duplicate_names_are_queried_once(self) -> None:
        self.resolver.resolve(
            self.cursor, ["com.whatsapp", "com.whatsapp", "com.telegram"]
        )

        self.assertEqual(sorted(self.cursor.queries[0]), ["com.telegram", "com.whatsapp"])


class CacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = ManualClock()
        self.cursor = FakeCursor({"com.whatsapp": 1, "com.telegram": 2})
        self.resolver = ApplicationResolver(ttl_seconds=300.0, clock=self.clock)

    def test_a_second_lookup_within_the_ttl_hits_the_cache(self) -> None:
        self.resolver.resolve(self.cursor, ["com.whatsapp"])
        self.resolver.resolve(self.cursor, ["com.whatsapp"])

        self.assertEqual(len(self.cursor.queries), 1)

    def test_only_the_uncached_names_are_queried(self) -> None:
        self.resolver.resolve(self.cursor, ["com.whatsapp"])
        self.resolver.resolve(self.cursor, ["com.whatsapp", "com.telegram"])

        self.assertEqual(self.cursor.queries[1], ["com.telegram"])

    def test_an_expired_entry_is_looked_up_again(self) -> None:
        self.resolver.resolve(self.cursor, ["com.whatsapp"])

        self.clock.advance(301.0)
        self.resolver.resolve(self.cursor, ["com.whatsapp"])

        self.assertEqual(len(self.cursor.queries), 2)

    def test_an_entry_just_inside_the_ttl_still_hits(self) -> None:
        self.resolver.resolve(self.cursor, ["com.whatsapp"])

        self.clock.advance(299.0)
        self.resolver.resolve(self.cursor, ["com.whatsapp"])

        self.assertEqual(len(self.cursor.queries), 1)

    def test_a_renamed_id_is_picked_up_after_the_ttl(self) -> None:
        self.resolver.resolve(self.cursor, ["com.whatsapp"])
        self.cursor.registry["com.whatsapp"] = 99

        self.clock.advance(301.0)
        result = self.resolver.resolve(self.cursor, ["com.whatsapp"])

        self.assertEqual(result["com.whatsapp"], 99)

    def test_misses_are_not_cached(self) -> None:
        """A newly registered app must be visible on the next batch, not in 5 min."""

        self.resolver.resolve(self.cursor, ["com.newapp"])
        self.cursor.registry["com.newapp"] = 42

        result = self.resolver.resolve(self.cursor, ["com.newapp"])

        self.assertEqual(result, {"com.newapp": 42})

    def test_invalidate_empties_the_cache(self) -> None:
        self.resolver.resolve(self.cursor, ["com.whatsapp"])

        self.resolver.invalidate()
        self.resolver.resolve(self.cursor, ["com.whatsapp"])

        self.assertEqual(len(self.cursor.queries), 2)


if __name__ == "__main__":
    unittest.main()
