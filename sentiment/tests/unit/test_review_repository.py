"""Unit tests for ReviewRepository SQL shape and parameters."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from sentiment.persistence.review_repository import ReviewRepository


class FakeConnection:
    encoding = "UTF8"


class FakeCursor:
    """Minimal cursor that records execute / execute_values traffic."""

    def __init__(self) -> None:
        self.connection = FakeConnection()
        self.execute_calls: list[tuple[object, object]] = []
        self._fetchall_rows: list[tuple] = []

    def execute(self, statement: object, params: object = None) -> None:
        self.execute_calls.append((statement, params))

    def fetchall(self) -> list[tuple]:
        return list(self._fetchall_rows)

    def mogrify(self, template: bytes, args: object) -> bytes:
        return str(args).encode("utf-8")


class ReviewRepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repository = ReviewRepository()
        self.cursor = FakeCursor()

    def test_fetch_uses_parameterized_keyset_and_content_filters(self) -> None:
        self.cursor._fetchall_rows = [(1, "nice"), (2, "bad")]

        rows = self.repository.fetch_batch(self.cursor, 50, after_id=0)

        self.assertEqual(rows, [(1, "nice"), (2, "bad")])
        self.assertEqual(len(self.cursor.execute_calls), 1)
        statement, params = self.cursor.execute_calls[0]
        sql = " ".join(str(statement).split())
        self.assertNotIn("sentiment IS NULL", sql)
        self.assertNotIn("sentiment =", sql)
        self.assertIn("content IS NOT NULL", sql)
        self.assertIn("content != ''", sql)
        self.assertIn("id > %s", sql)
        self.assertIn("ORDER BY id", sql)
        self.assertIn("LIMIT %s", sql)
        self.assertEqual(params, (0, 50))

    def test_fetch_passes_after_id_for_pagination(self) -> None:
        self.cursor._fetchall_rows = []
        self.repository.fetch_batch(self.cursor, 10, after_id=42)
        _, params = self.cursor.execute_calls[0]
        self.assertEqual(params, (42, 10))

    def test_update_sentiments_uses_execute_values(self) -> None:
        results = [(10, "POSITIVE"), (11, "NEGATIVE")]

        with patch(
            "sentiment.persistence.review_repository.execute_values"
        ) as mock_ev:
            self.repository.update_sentiments(self.cursor, results)

        mock_ev.assert_called_once()
        args, kwargs = mock_ev.call_args
        self.assertIs(args[0], self.cursor)
        sql = " ".join(str(args[1]).split())
        self.assertIn("UPDATE reviews", sql)
        self.assertIn("VALUES %s", sql)
        self.assertEqual(args[2], results)
        self.assertEqual(kwargs.get("template"), "(%s::bigint, %s::varchar)")

    def test_update_sentiments_skips_empty_results(self) -> None:
        with patch(
            "sentiment.persistence.review_repository.execute_values"
        ) as mock_ev:
            self.repository.update_sentiments(self.cursor, [])

        mock_ev.assert_not_called()


if __name__ == "__main__":
    unittest.main()
