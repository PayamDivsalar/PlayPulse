"""Sentiment analysis batch job for ``reviews.sentiment``.

A periodic CLI job (not a daemon) that classifies review text and writes
``POSITIVE`` / ``NEUTRAL`` / ``NEGATIVE`` into the column left NULL by
``storage_consumer``.
"""
