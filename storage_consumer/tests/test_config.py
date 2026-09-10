"""Tests for settings validation and environment loading."""

from __future__ import annotations

import os
import unittest
from unittest import mock

from storage_consumer.config import (
    PIPELINE_NAMES,
    Settings,
    load_settings,
    parse_pipelines,
)
from storage_consumer.exceptions import ConfigError

_REQUIRED_ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
    "POSTGRES_DB": "project_db",
    "POSTGRES_USER": "project_user",
    "POSTGRES_PASSWORD": "secret",
    "POSTGRES_HOST": "postgres",
}


class DefaultsTests(unittest.TestCase):
    def test_defaults_run_all_three_pipelines(self) -> None:
        self.assertEqual(Settings.for_testing().pipelines, PIPELINE_NAMES)

    def test_offsets_start_at_the_beginning_by_default(self) -> None:
        """A storage subsystem joining late must ingest history, not skip it."""

        self.assertEqual(Settings.for_testing().auto_offset_reset, "earliest")

    def test_batch_size_defaults_to_the_per_pipeline_tuning(self) -> None:
        settings = Settings.for_testing()

        self.assertIsNone(settings.max_poll_records_override)
        self.assertEqual(dict(settings.max_poll_records_by_pipeline), {})

    def test_settings_are_immutable(self) -> None:
        """Three threads share one instance; a mutable one would be a race."""

        settings = Settings.for_testing()

        with self.assertRaises(Exception):
            settings.poll_timeout_ms = 5  # type: ignore[misc]

        with self.assertRaises(TypeError):
            settings.max_poll_records_by_pipeline["reviews"] = 1  # type: ignore[index]


class MaxPollRecordsResolutionTests(unittest.TestCase):
    """Precedence: per-pipeline env > global env > code default."""

    def test_code_default_when_nothing_is_overridden(self) -> None:
        settings = Settings.for_testing()

        self.assertEqual(settings.max_poll_records_for("reviews", 500), 500)

    def test_global_override_applies_when_no_per_pipeline_value(self) -> None:
        settings = Settings.for_testing(max_poll_records_override=25)

        self.assertEqual(settings.max_poll_records_for("reviews", 500), 25)
        self.assertEqual(settings.max_poll_records_for("app-stats", 200), 25)

    def test_per_pipeline_override_wins_over_global(self) -> None:
        """Raising reviews alone must not flatten network-metrics."""

        settings = Settings.for_testing(
            max_poll_records_override=25,
            max_poll_records_by_pipeline={"reviews": 1000},
        )

        self.assertEqual(settings.max_poll_records_for("reviews", 500), 1000)
        self.assertEqual(settings.max_poll_records_for("network-metrics", 50), 25)

    def test_per_pipeline_override_alone_leaves_siblings_on_defaults(self) -> None:
        settings = Settings.for_testing(
            max_poll_records_by_pipeline={"reviews": 1000}
        )

        self.assertEqual(settings.max_poll_records_for("reviews", 500), 1000)
        self.assertEqual(settings.max_poll_records_for("app-stats", 200), 200)

    def test_unknown_pipeline_in_the_map_is_rejected(self) -> None:
        with self.assertRaises(ConfigError) as caught:
            Settings.for_testing(
                max_poll_records_by_pipeline={"sentiment": 10}
            )

        self.assertIn("sentiment", str(caught.exception))

    def test_non_positive_per_pipeline_value_is_rejected(self) -> None:
        with self.assertRaises(ConfigError):
            Settings.for_testing(max_poll_records_by_pipeline={"reviews": 0})


class GroupIdTests(unittest.TestCase):
    def test_each_pipeline_gets_its_own_group(self) -> None:
        settings = Settings.for_testing()

        groups = {settings.group_id(name) for name in PIPELINE_NAMES}

        self.assertEqual(len(groups), len(PIPELINE_NAMES))

    def test_group_id_uses_the_configured_prefix(self) -> None:
        settings = Settings.for_testing(consumer_group_prefix="staging")

        self.assertEqual(settings.group_id("reviews"), "staging.reviews")


class ValidationTests(unittest.TestCase):
    def test_empty_bootstrap_servers_is_rejected(self) -> None:
        with self.assertRaises(ConfigError):
            Settings.for_testing(kafka_bootstrap_servers="  ")

    def test_empty_postgres_database_is_rejected(self) -> None:
        with self.assertRaises(ConfigError) as caught:
            Settings.for_testing(postgres_db="")

        self.assertIn("postgres_db", str(caught.exception))

    def test_out_of_range_port_is_rejected(self) -> None:
        with self.assertRaises(ConfigError):
            Settings.for_testing(postgres_port=70_000)

    def test_unknown_pipeline_name_is_rejected(self) -> None:
        with self.assertRaises(ConfigError) as caught:
            Settings.for_testing(pipelines=("app-stats", "sentiment"))

        self.assertIn("sentiment", str(caught.exception))

    def test_empty_pipeline_selection_is_rejected(self) -> None:
        with self.assertRaises(ConfigError):
            Settings.for_testing(pipelines=())

    def test_repeated_pipeline_is_rejected(self) -> None:
        """Two threads on one group would rebalance each other forever."""

        with self.assertRaises(ConfigError):
            Settings.for_testing(pipelines=("reviews", "reviews"))

    def test_unknown_auto_offset_reset_is_rejected(self) -> None:
        with self.assertRaises(ConfigError):
            Settings.for_testing(auto_offset_reset="none")

    def test_heartbeat_at_or_above_session_timeout_is_rejected(self) -> None:
        with self.assertRaises(ConfigError) as caught:
            Settings.for_testing(heartbeat_interval_ms=45_000, session_timeout_ms=45_000)

        self.assertIn("heartbeat_interval_ms", str(caught.exception))

    def test_retry_window_longer_than_the_poll_interval_is_rejected(self) -> None:
        """Otherwise the broker evicts us mid-batch and we livelock rebalancing."""

        with self.assertRaises(ConfigError) as caught:
            Settings.for_testing(
                db_retry_max_attempts=10,
                db_retry_base_delay_seconds=30.0,
                max_poll_interval_ms=60_000,
            )

        self.assertIn("max_poll_interval_ms", str(caught.exception))

    def test_the_shipped_defaults_leave_retry_headroom(self) -> None:
        settings = Settings.for_testing()

        self.assertLess(settings.retry_window_seconds * 1000, settings.max_poll_interval_ms)


class PipelineParsingTests(unittest.TestCase):
    def test_absent_selection_means_all(self) -> None:
        self.assertEqual(parse_pipelines(None), PIPELINE_NAMES)

    def test_the_word_all_means_all(self) -> None:
        self.assertEqual(parse_pipelines("all"), PIPELINE_NAMES)

    def test_a_comma_separated_list_is_split_and_stripped(self) -> None:
        self.assertEqual(
            parse_pipelines(" reviews , app-stats "), ("reviews", "app-stats")
        )

    def test_all_cannot_be_combined_with_names(self) -> None:
        with self.assertRaises(ConfigError):
            parse_pipelines("all,reviews")


class LoadSettingsTests(unittest.TestCase):
    def test_required_variables_build_a_valid_settings(self) -> None:
        with mock.patch.dict(os.environ, _REQUIRED_ENV, clear=True):
            settings = load_settings(env_file=self._absent_env_file())

        self.assertEqual(settings.postgres_host, "postgres")
        self.assertEqual(settings.pipelines, PIPELINE_NAMES)

    def test_a_missing_required_variable_names_itself(self) -> None:
        environment = dict(_REQUIRED_ENV)
        del environment["POSTGRES_PASSWORD"]

        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaises(ConfigError) as caught:
                load_settings(env_file=self._absent_env_file())

        self.assertIn("POSTGRES_PASSWORD", str(caught.exception))

    def test_optional_overrides_are_read(self) -> None:
        environment = dict(_REQUIRED_ENV, STORAGE_PIPELINES="reviews")
        environment["STORAGE_MAX_POLL_RECORDS"] = "50"
        environment["STORAGE_MAX_POLL_RECORDS_REVIEWS"] = "1000"
        environment["STORAGE_MAX_POLL_RECORDS_APP_STATS"] = "300"

        with mock.patch.dict(os.environ, environment, clear=True):
            settings = load_settings(env_file=self._absent_env_file())

        self.assertEqual(settings.pipelines, ("reviews",))
        self.assertEqual(settings.max_poll_records_override, 50)
        self.assertEqual(
            dict(settings.max_poll_records_by_pipeline),
            {"app-stats": 300, "reviews": 1000},
        )

    def test_a_non_numeric_per_pipeline_override_names_the_variable(self) -> None:
        environment = dict(_REQUIRED_ENV, STORAGE_MAX_POLL_RECORDS_REVIEWS="big")

        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaises(ConfigError) as caught:
                load_settings(env_file=self._absent_env_file())

        self.assertIn("STORAGE_MAX_POLL_RECORDS_REVIEWS", str(caught.exception))

    def test_a_non_numeric_override_names_the_variable(self) -> None:
        environment = dict(_REQUIRED_ENV, STORAGE_POLL_TIMEOUT_MS="soon")

        with mock.patch.dict(os.environ, environment, clear=True):
            with self.assertRaises(ConfigError) as caught:
                load_settings(env_file=self._absent_env_file())

        self.assertIn("STORAGE_POLL_TIMEOUT_MS", str(caught.exception))

    def _absent_env_file(self):
        from pathlib import Path

        return Path(__file__).resolve().parent / "no-such.env"


if __name__ == "__main__":
    unittest.main()
