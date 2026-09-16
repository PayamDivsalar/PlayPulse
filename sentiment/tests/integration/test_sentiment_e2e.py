"""Integration test: classify reviews against Postgres and overwrite labels.

The classifier is a deterministic stub so the suite does not download model
weights. The database path (fetch → write → re-read) is real.
"""

from __future__ import annotations

import unittest
import uuid
from datetime import datetime, timezone

import pytest

from sentiment.core.classifier import SentimentClassifier
from sentiment.persistence.database import Database
from sentiment.persistence.review_repository import ReviewRepository
from sentiment.runtime.sentiment_service import SentimentService
from sentiment.tests import integration_support

_VALID = frozenset({"POSITIVE", "NEUTRAL", "NEGATIVE"})
_REVIEWED_AT = datetime(2026, 9, 1, 10, tzinfo=timezone.utc)


class StubClassifier(SentimentClassifier):
    """Avoid loading transformers; map by simple keywords for the e2e path."""

    def __init__(self) -> None:
        # Bypass SentimentClassifier.__init__ (no model download).
        self.model_name = "stub"

    def classify_batch(self, texts):  # type: ignore[override]
        labels: list[str | None] = []
        for text in texts:
            if text is None or not str(text).strip():
                labels.append(None)
                continue
            lowered = str(text).lower()
            if "great" in lowered or "عالی" in lowered:
                labels.append("POSITIVE")
            elif "terrible" in lowered or "افتضاح" in lowered:
                labels.append("NEGATIVE")
            else:
                labels.append("NEUTRAL")
        return labels


@pytest.mark.integration
class SentimentE2ETests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = integration_support.settings_or_skip(batch_size=3)
        integration_support.require_postgres(self.settings)
        self.database = Database(
            self.settings, application_name="sentiment:test"
        )
        self.addCleanup(self.database.close)

        self.package_name = integration_support.unique_package_name()
        with self.database.transaction() as cursor:
            self.application_id = integration_support.register_application(
                cursor, self.package_name
            )
            self.review_ids = self._insert_reviews(cursor)
        self.addCleanup(self._cleanup)

    def _cleanup(self) -> None:
        with self.database.transaction() as cursor:
            integration_support.delete_test_applications(cursor)

    def _insert_reviews(self, cursor) -> list[int]:
        # Include pre-labeled rows to prove existing sentiment is overwritten.
        samples = [
            ("This app is great", None),
            ("terrible experience overall", "POSITIVE"),  # wrong on purpose
            ("نسخه جدید نصب شد", "NEGATIVE"),
        ]
        ids: list[int] = []
        for content, existing in samples:
            cursor.execute(
                """
                INSERT INTO reviews (
                    application_id, review_id, user_name, thumbs_up_count,
                    score, content, at, sentiment, last_synced_at
                ) VALUES (
                    %s, %s, %s, 0, 5, %s, %s, %s, %s
                )
                RETURNING id
                """,
                (
                    self.application_id,
                    f"gp:sentiment-itest:{uuid.uuid4().hex}",
                    "tester",
                    content,
                    _REVIEWED_AT,
                    existing,
                    _REVIEWED_AT,
                ),
            )
            ids.append(int(cursor.fetchone()[0]))
        return ids

    def test_run_batch_cycle_overwrites_sentiment(self) -> None:
        service = SentimentService(
            settings=self.settings,
            classifier=StubClassifier(),
            database=self.database,
            repository=ReviewRepository(),
            dry_run=False,
        )
        # Start just before our serial ids so this stub run does not rewrite
        # unrelated reviews already present in a shared compose database.
        service._after_id = self.review_ids[0] - 1

        self.assertTrue(service.run_batch_cycle())

        with self.database.transaction() as cursor:
            cursor.execute(
                "SELECT id, sentiment FROM reviews WHERE id = ANY(%s) ORDER BY id",
                (self.review_ids,),
            )
            rows = cursor.fetchall()

        by_id = {int(review_id): sentiment for review_id, sentiment in rows}
        self.assertEqual(by_id[self.review_ids[0]], "POSITIVE")
        self.assertEqual(by_id[self.review_ids[1]], "NEGATIVE")
        self.assertEqual(by_id[self.review_ids[2]], "NEUTRAL")


if __name__ == "__main__":
    unittest.main()
