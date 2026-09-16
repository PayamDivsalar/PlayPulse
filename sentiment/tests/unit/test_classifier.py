"""Unit tests for SentimentClassifier label mapping and empty-text handling."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from sentiment.core.classifier import LABEL_MAP, SentimentClassifier
from sentiment.exceptions import ClassificationError


class SentimentClassifierTests(unittest.TestCase):
    def _classifier_with_outputs(self, outputs: list[dict]) -> SentimentClassifier:
        pipeline = MagicMock(return_value=outputs)
        factory = MagicMock(return_value=pipeline)
        return SentimentClassifier("fake-model", pipeline_factory=factory)

    def test_maps_lowercase_cardiff_labels(self) -> None:
        classifier = self._classifier_with_outputs(
            [
                {"label": "positive", "score": 0.9},
                {"label": "neutral", "score": 0.5},
                {"label": "negative", "score": 0.8},
            ]
        )

        result = classifier.classify_batch(["great", "ok", "bad"])

        self.assertEqual(result, ["POSITIVE", "NEUTRAL", "NEGATIVE"])

    def test_maps_title_case_and_label_n_aliases(self) -> None:
        classifier = self._classifier_with_outputs(
            [
                {"label": "Positive", "score": 0.9},
                {"label": "LABEL_1", "score": 0.5},
                {"label": "LABEL_0", "score": 0.8},
            ]
        )

        result = classifier.classify_batch(["a", "b", "c"])

        self.assertEqual(result, ["POSITIVE", "NEUTRAL", "NEGATIVE"])
        for raw in ("Positive", "LABEL_1", "LABEL_0"):
            self.assertIn(raw, LABEL_MAP)

    def test_empty_and_none_texts_become_none_without_calling_model(self) -> None:
        pipeline = MagicMock()
        factory = MagicMock(return_value=pipeline)
        classifier = SentimentClassifier("fake-model", pipeline_factory=factory)

        result = classifier.classify_batch([None, "", "   ", "\t"])

        self.assertEqual(result, [None, None, None, None])
        pipeline.assert_not_called()

    def test_mixed_batch_preserves_positions(self) -> None:
        classifier = self._classifier_with_outputs(
            [{"label": "negative", "score": 0.7}]
        )

        result = classifier.classify_batch(["", "terrible app", None])

        self.assertEqual(result, [None, "NEGATIVE", None])
        classifier._pipeline.assert_called_once()
        args, kwargs = classifier._pipeline.call_args
        self.assertEqual(args[0], ["terrible app"])
        self.assertTrue(kwargs.get("truncation"))
        self.assertEqual(kwargs.get("max_length"), 512)

    def test_unknown_label_raises_classification_error(self) -> None:
        classifier = self._classifier_with_outputs(
            [{"label": "mixed", "score": 0.4}]
        )

        with self.assertRaises(ClassificationError):
            classifier.classify_batch(["ambiguous"])

    def test_constructor_loads_via_injected_factory_once(self) -> None:
        pipeline = MagicMock(return_value=[])
        factory = MagicMock(return_value=pipeline)

        SentimentClassifier(
            "cardiffnlp/twitter-xlm-roberta-base-sentiment",
            pipeline_factory=factory,
        )

        factory.assert_called_once_with(
            "sentiment-analysis",
            model="cardiffnlp/twitter-xlm-roberta-base-sentiment",
        )


if __name__ == "__main__":
    unittest.main()
