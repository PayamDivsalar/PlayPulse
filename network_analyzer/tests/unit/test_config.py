"""Tests for analyzer settings loading and validation."""

from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from network_analyzer.config import Settings, load_settings
from network_analyzer.exceptions import AnalyzerConfigError

_MISSING_ENV_FILE = Path("/nonexistent/network-analyzer.env")


class SettingsValidationTests(unittest.TestCase):
    def test_defaults_are_valid(self) -> None:
        settings = Settings.for_testing()

        self.assertEqual(settings.kafka_producer_acks, "all")
        self.assertTrue(settings.kafka_producer_enable_idempotence)
        self.assertEqual(settings.retry_max_attempts, 3)

    def test_rejects_empty_bootstrap_servers(self) -> None:
        with self.assertRaises(AnalyzerConfigError):
            Settings.for_testing(kafka_bootstrap_servers="   ")

    def test_rejects_empty_app_api_base_url(self) -> None:
        with self.assertRaises(AnalyzerConfigError):
            Settings.for_testing(app_api_base_url="")

    def test_rejects_non_positive_registry_timeout(self) -> None:
        with self.assertRaises(AnalyzerConfigError):
            Settings.for_testing(registry_request_timeout_seconds=0)

    def test_rejects_negative_retry_attempts(self) -> None:
        with self.assertRaises(AnalyzerConfigError):
            Settings.for_testing(retry_max_attempts=-1)

    def test_rejects_unknown_acks_value(self) -> None:
        with self.assertRaises(AnalyzerConfigError):
            Settings.for_testing(kafka_producer_acks="most")

    def test_rejects_delivery_timeout_not_greater_than_request_timeout(self) -> None:
        with self.assertRaises(AnalyzerConfigError):
            Settings.for_testing(
                kafka_producer_request_timeout_ms=5000,
                kafka_producer_delivery_timeout_ms=5000,
            )

    def test_settings_are_immutable(self) -> None:
        settings = Settings.for_testing()

        with self.assertRaises(Exception):
            settings.kafka_bootstrap_servers = "other:9092"  # type: ignore[misc]


class LoadSettingsTests(unittest.TestCase):
    def test_reads_required_variables_from_environment(self) -> None:
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
            "APP_API_BASE_URL": "http://app-api:8000",
        }
        with patch.dict("os.environ", env, clear=True):
            settings = load_settings(env_file=_MISSING_ENV_FILE)

        self.assertEqual(settings.kafka_bootstrap_servers, "broker:29092")
        self.assertEqual(settings.app_api_base_url, "http://app-api:8000")

    def test_missing_required_variable_raises(self) -> None:
        env = {"APP_API_BASE_URL": "http://app-api:8000"}
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(AnalyzerConfigError):
                load_settings(env_file=_MISSING_ENV_FILE)

    def test_optional_overrides_are_applied(self) -> None:
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
            "APP_API_BASE_URL": "http://app-api:8000",
            "ANALYZER_RETRY_MAX_ATTEMPTS": "7",
            "ANALYZER_RETRY_BASE_DELAY_SECONDS": "0.5",
            "ANALYZER_KAFKA_PRODUCER_ENABLE_IDEMPOTENCE": "false",
        }
        with patch.dict("os.environ", env, clear=True):
            settings = load_settings(env_file=_MISSING_ENV_FILE)

        self.assertEqual(settings.retry_max_attempts, 7)
        self.assertEqual(settings.retry_base_delay_seconds, 0.5)
        self.assertFalse(settings.kafka_producer_enable_idempotence)

    def test_non_numeric_override_raises(self) -> None:
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
            "APP_API_BASE_URL": "http://app-api:8000",
            "ANALYZER_RETRY_MAX_ATTEMPTS": "many",
        }
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(AnalyzerConfigError):
                load_settings(env_file=_MISSING_ENV_FILE)

    def test_non_boolean_override_raises(self) -> None:
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
            "APP_API_BASE_URL": "http://app-api:8000",
            "ANALYZER_KAFKA_PRODUCER_ENABLE_IDEMPOTENCE": "perhaps",
        }
        with patch.dict("os.environ", env, clear=True):
            with self.assertRaises(AnalyzerConfigError):
                load_settings(env_file=_MISSING_ENV_FILE)


if __name__ == "__main__":
    unittest.main()
