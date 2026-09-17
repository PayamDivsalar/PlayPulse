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
        self.assertEqual(settings.playstore_request_timeout_seconds, 60.0)

    def test_region_defaults(self) -> None:
        settings = Settings.for_testing()
        self.assertEqual(settings.default_country, "us")
        self.assertEqual(settings.default_lang, "en")
        self.assertEqual(settings.iran_country, "ir")
        self.assertEqual(settings.iran_lang, "fa")

    def test_rejects_empty_region_values(self) -> None:
        for field in (
            "default_country",
            "default_lang",
            "iran_country",
            "iran_lang",
        ):
            with self.subTest(field=field):
                with self.assertRaises(CrawlerConfigError):
                    Settings.for_testing(**{field: "   "})

    def test_rejects_non_positive_workers(self) -> None:
        with self.assertRaises(CrawlerConfigError):
            Settings.for_testing(max_concurrent_workers=0)

    def test_rejects_non_positive_playstore_timeout(self) -> None:
        with self.assertRaises(CrawlerConfigError):
            Settings.for_testing(playstore_request_timeout_seconds=0)

    def test_rejects_negative_retry_attempts(self) -> None:
        with self.assertRaises(CrawlerConfigError):
            Settings.for_testing(retry_max_attempts=-1)

    def test_rejects_empty_kafka_bootstrap(self) -> None:
        with self.assertRaises(CrawlerConfigError):
            Settings.for_testing(kafka_bootstrap_servers="   ")

    def test_kafka_producer_retry_defaults(self) -> None:
        settings = Settings.for_testing()
        self.assertEqual(settings.kafka_producer_retries, 5)
        self.assertEqual(settings.kafka_producer_acks, "all")
        self.assertTrue(settings.kafka_producer_enable_idempotence)
        self.assertEqual(settings.kafka_send_retry_max_attempts, 3)
        self.assertEqual(settings.kafka_send_retry_base_delay_seconds, 5.0)

    def test_rejects_negative_kafka_producer_retries(self) -> None:
        with self.assertRaises(CrawlerConfigError):
            Settings.for_testing(kafka_producer_retries=-1)

    def test_rejects_delivery_timeout_not_greater_than_request(self) -> None:
        with self.assertRaises(CrawlerConfigError):
            Settings.for_testing(
                kafka_producer_request_timeout_ms=5000,
                kafka_producer_delivery_timeout_ms=5000,
            )
