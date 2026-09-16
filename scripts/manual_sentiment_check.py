#!/usr/bin/env python3
"""Manual smoke check for the Cardiff XLM-R sentiment model.

Not part of pytest. Loads the real model, classifies a few Persian and English
samples, and prints the labels — useful for a quick quality check without a
database.

Usage (from the repository root, with the sentiment venv active)::

    python scripts/manual_sentiment_check.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow `from sentiment...` when run as a script from the repo root.
_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from sentiment.config import DEFAULT_MODEL_NAME
from sentiment.core.classifier import SentimentClassifier

SAMPLES: list[tuple[str, str]] = [
    ("en+", "This app is amazing, I use it every day!"),
    ("en-", "Terrible app, keeps crashing and wasting my time."),
    ("en~", "Installed the new version yesterday."),
    ("fa+", "برنامه عالیه، خیلی راضی‌ام."),
    ("fa-", "افتضاحه، اصلا باز نمیشه."),
    ("fa~", "نسخه جدید رو امروز نصب کردم."),
]


def main() -> int:
    print(f"Loading model: {DEFAULT_MODEL_NAME}")
    classifier = SentimentClassifier(DEFAULT_MODEL_NAME)
    texts = [text for _, text in SAMPLES]
    labels = classifier.classify_batch(texts)

    print()
    print(f"{'tag':<4}  {'label':<10}  text")
    print("-" * 72)
    for (tag, text), label in zip(SAMPLES, labels, strict=True):
        print(f"{tag:<4}  {str(label):<10}  {text}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
