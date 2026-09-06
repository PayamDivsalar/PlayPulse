"""Tests for crawler Settings loading and validation."""

from __future__ import annotations

import unittest

from crawler.config import Settings
from crawler.exceptions import CrawlerConfigError


class SettingsTests(unittest.TestCase):
    def test_for_testing_builds_valid_defaults(self) -> None:
        settings = Settings.for_testing()
        self.assertEqual(settings.retry_max_attempts, 3)
        self.assertEqual(settings.reviews_fetch_count, 1000)

    def test_rejects_non_positive_workers(self) -> None:
        with self.assertRaises(CrawlerConfigError):
            Settings.for_testing(max_concurrent_workers=0)

    def test_rejects_negative_retry_attempts(self) -> None:
        with self.assertRaises(CrawlerConfigError):
            Settings.for_testing(retry_max_attempts=-1)

    def test_rejects_empty_kafka_bootstrap(self) -> None:
        with self.assertRaises(CrawlerConfigError):
            Settings.for_testing(kafka_bootstrap_servers="   ")
