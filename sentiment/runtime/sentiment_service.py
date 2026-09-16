"""Orchestrates one (or many) classify-and-write cycles over ``reviews``.

Decision on unclassifiable text
--------------------------------
When ``SentimentClassifier.classify_batch`` returns ``None`` for a row (empty
or whitespace-only after strip), this service writes ``NEUTRAL`` rather than
leaving ``sentiment`` NULL.

Rationale: empty/whitespace text has no signal worth storing as NULL, and
``NEUTRAL`` is the least assertive label that still satisfies the CHECK
constraint. Pagination advances by ``id``, not by whether sentiment was set,
so writing ``NEUTRAL`` is not required to avoid an infinite loop — it is only
a stable default for unclassifiable content.
"""

from __future__ import annotations

import logging
from collections import Counter

from sentiment.config import Settings
from sentiment.core.classifier import SentimentClassifier
from sentiment.persistence.database import Database
from sentiment.persistence.review_repository import ReviewRepository

logger = logging.getLogger(__name__)

_FALLBACK_LABEL = "NEUTRAL"


class SentimentService:
    """Read a batch → classify → write (unless dry-run).

    Each job starts at ``id = 0`` and walks every review with non-empty content,
    overwriting whatever ``sentiment`` is already stored.
    """

    def __init__(
        self,
        *,
        settings: Settings,
        classifier: SentimentClassifier,
        database: Database,
        repository: ReviewRepository,
        dry_run: bool = False,
    ) -> None:
        self._settings = settings
        self._classifier = classifier
        self._database = database
        self._repository = repository
        self._dry_run = dry_run
        self._after_id = 0

    def run_batch_cycle(self) -> bool:
        """Process one batch. Return ``True`` if work was done, else ``False``.

        ``False`` means there are no more rows after the current id cursor —
        ``main`` stops looping. That is different from "the job failed"; an
        empty fetch is success with nothing left in this run.
        """

        batch_size = self._settings.batch_size
        with self._database.transaction() as cursor:
            rows = self._repository.fetch_batch(
                cursor, batch_size, after_id=self._after_id
            )

        if not rows:
            logger.info("No more reviews to process in this run.")
            return False

        review_ids = [review_id for review_id, _ in rows]
        texts = [content for _, content in rows]
        labels = self._classifier.classify_batch(texts)

        results: list[tuple[int, str]] = []
        for review_id, label in zip(review_ids, labels, strict=True):
            results.append(
                (review_id, _FALLBACK_LABEL if label is None else label)
            )

        # Advance past this page whether or not we write, so dry-run and
        # overwrite runs both terminate after the last id.
        self._after_id = review_ids[-1]

        counts = Counter(sentiment for _, sentiment in results)
        if self._dry_run:
            logger.info(
                "Dry run: would update %s review(s) "
                "(POSITIVE=%s, NEUTRAL=%s, NEGATIVE=%s); no writes performed.",
                len(results),
                counts.get("POSITIVE", 0),
                counts.get("NEUTRAL", 0),
                counts.get("NEGATIVE", 0),
            )
            return True

        with self._database.transaction() as cursor:
            self._repository.update_sentiments(cursor, results)

        logger.info(
            "Updated %s review(s): POSITIVE=%s, NEUTRAL=%s, NEGATIVE=%s",
            len(results),
            counts.get("POSITIVE", 0),
            counts.get("NEUTRAL", 0),
            counts.get("NEGATIVE", 0),
        )
        return True
