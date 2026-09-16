"""Thin synchronous wrapper around the Hugging Face sentiment pipeline.

The model is loaded once in the constructor; every batch reuses that pipeline.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from sentiment.exceptions import ClassificationError

# Raw pipeline labels -> values allowed by reviews.sentiment CHECK constraint.
# Cardiff XLM-R typically emits lowercase "positive"/"neutral"/"negative";
# Title-case and LABEL_N forms are included so a config/tokenizer change
# cannot silently break the writer.
LABEL_MAP: Mapping[str, str] = {
    "positive": "POSITIVE",
    "Positive": "POSITIVE",
    "POSITIVE": "POSITIVE",
    "LABEL_2": "POSITIVE",
    "neutral": "NEUTRAL",
    "Neutral": "NEUTRAL",
    "NEUTRAL": "NEUTRAL",
    "LABEL_1": "NEUTRAL",
    "negative": "NEGATIVE",
    "Negative": "NEGATIVE",
    "NEGATIVE": "NEGATIVE",
    "LABEL_0": "NEGATIVE",
}

# Pipeline kwargs applied on every call so long reviews truncate instead of
# raising when they exceed the model's token limit.
_PIPELINE_CALL_KWARGS: Mapping[str, Any] = {
    "truncation": True,
    "max_length": 512,
}


class SentimentClassifier:
    """Classify review texts into POSITIVE / NEUTRAL / NEGATIVE.

    ``model_name`` is injected (default comes from ``Settings`` at the
    composition root) so tests can pass a stub without downloading weights.
    """

    def __init__(
        self,
        model_name: str,
        *,
        pipeline_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.model_name = model_name
        if pipeline_factory is None:
            from transformers import pipeline as hf_pipeline

            pipeline_factory = hf_pipeline
        self._pipeline = pipeline_factory(
            "sentiment-analysis",
            model=model_name,
        )

    def classify_batch(self, texts: Sequence[str | None]) -> list[str | None]:
        """Return one label (or ``None``) per input, preserving order.

        Empty / ``None`` / whitespace-only texts never reach the model; their
        slot is ``None`` so the caller can tell which rows were unclassifiable.
        """

        results: list[str | None] = [None] * len(texts)
        indexed: list[tuple[int, str]] = []
        for index, text in enumerate(texts):
            if text is None:
                continue
            stripped = text.strip()
            if not stripped:
                continue
            indexed.append((index, stripped))

        if not indexed:
            return results

        outputs = self._pipeline(
            [text for _, text in indexed],
            **_PIPELINE_CALL_KWARGS,
        )
        # A single-string call returns a dict; a batch returns a list.
        if isinstance(outputs, dict):
            outputs = [outputs]

        for (index, _), output in zip(indexed, outputs, strict=True):
            raw_label = output["label"]
            mapped = LABEL_MAP.get(raw_label)
            if mapped is None:
                raise ClassificationError(
                    f"Unrecognized model label {raw_label!r}; "
                    f"expected one of {sorted(LABEL_MAP)}."
                )
            results[index] = mapped
        return results
