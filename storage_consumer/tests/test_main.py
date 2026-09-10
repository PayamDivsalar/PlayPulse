"""Tests for the CLI: argument handling, exit codes and error reporting."""

from __future__ import annotations

import unittest
from unittest import mock

from storage_consumer import main as main_module
from storage_consumer.config import PIPELINE_NAMES, Settings
from storage_consumer.exceptions import ConfigError, MigrationError


class ArgumentTests(unittest.TestCase):
    def test_a_subcommand_is_required(self) -> None:
        with self.assertRaises(SystemExit) as caught:
            main_module._build_parser().parse_args([])

        self.assertEqual(caught.exception.code, 2)

    def test_run_defaults_to_every_pipeline(self) -> None:
        args = main_module._build_parser().parse_args(["run"])

        self.assertIsNone(args.pipeline)

    def test_pipeline_can_be_repeated(self) -> None:
        args = main_module._build_parser().parse_args(
            ["run", "--pipeline", "reviews", "--pipeline", "app-stats"]
        )

        self.assertEqual(args.pipeline, ["reviews", "app-stats"])

    def test_an_unknown_pipeline_is_a_usage_error(self) -> None:
        with self.assertRaises(SystemExit):
            main_module._build_parser().parse_args(["run", "--pipeline", "sentiment"])

    def test_skip_migrate_is_off_by_default(self) -> None:
        self.assertFalse(main_module._build_parser().parse_args(["run"]).skip_migrate)


class PipelineResolutionTests(unittest.TestCase):
    def test_no_flag_leaves_the_environment_selection_alone(self) -> None:
        settings = Settings.for_testing(pipelines=("reviews",))

        resolved = main_module._resolve_pipelines(settings, None)

        self.assertEqual(resolved.pipelines, ("reviews",))

    def test_the_flag_overrides_the_environment(self) -> None:
        settings = Settings.for_testing(pipelines=("reviews",))

        resolved = main_module._resolve_pipelines(settings, ["app-stats"])

        self.assertEqual(resolved.pipelines, ("app-stats",))

    def test_all_selects_every_pipeline(self) -> None:
        settings = Settings.for_testing(pipelines=("reviews",))

        resolved = main_module._resolve_pipelines(settings, ["all"])

        self.assertEqual(resolved.pipelines, PIPELINE_NAMES)

    def test_a_repeated_flag_keeps_the_requested_order(self) -> None:
        resolved = main_module._resolve_pipelines(
            Settings.for_testing(), ["network-metrics", "reviews"]
        )

        self.assertEqual(resolved.pipelines, ("network-metrics", "reviews"))

    def test_a_duplicate_flag_value_is_collapsed(self) -> None:
        """Two threads on one consumer group would rebalance each other."""

        resolved = main_module._resolve_pipelines(
            Settings.for_testing(), ["reviews", "reviews"]
        )

        self.assertEqual(resolved.pipelines, ("reviews",))


class ExitCodeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings.for_testing()
        patcher = mock.patch.object(
            main_module, "load_settings", return_value=self.settings
        )
        self.load_settings = patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_successful_migrate_exits_zero(self) -> None:
        with mock.patch.object(main_module, "_migrate", return_value=0) as migrate:
            self.assertEqual(main_module.main(["migrate"]), 0)

        migrate.assert_called_once()

    def test_a_configuration_error_exits_one(self) -> None:
        self.load_settings.side_effect = ConfigError("POSTGRES_HOST is not set")

        self.assertEqual(main_module.main(["migrate"]), 1)

    def test_a_migration_error_exits_one(self) -> None:
        with mock.patch.object(
            main_module, "_migrate", side_effect=MigrationError("run app_api first")
        ):
            self.assertEqual(main_module.main(["migrate"]), 1)

    def test_run_returns_the_supervisor_exit_code(self) -> None:
        with mock.patch.object(main_module, "_run", return_value=1):
            self.assertEqual(main_module.main(["run"]), 1)

    def test_an_interrupt_exits_one(self) -> None:
        with mock.patch.object(main_module, "_migrate", side_effect=KeyboardInterrupt):
            self.assertEqual(main_module.main(["migrate"]), 1)


class SchemaGateTests(unittest.TestCase):
    def test_run_migrates_by_default(self) -> None:
        with mock.patch.object(main_module, "_migrate") as migrate, mock.patch.object(
            main_module, "Supervisor"
        ) as supervisor, mock.patch.object(main_module.signal, "signal"):
            supervisor.return_value.run.return_value = 0
            main_module._run(Settings.for_testing(), skip_migrate=False)

        migrate.assert_called_once()

    def test_skip_migrate_checks_the_schema_instead(self) -> None:
        """Gating DDL manually must not mean starting against missing tables."""

        with mock.patch.object(main_module, "_migrate") as migrate, mock.patch.object(
            main_module, "_check_schema"
        ) as check, mock.patch.object(
            main_module, "Supervisor"
        ) as supervisor, mock.patch.object(main_module.signal, "signal"):
            supervisor.return_value.run.return_value = 0
            main_module._run(Settings.for_testing(), skip_migrate=True)

        migrate.assert_not_called()
        check.assert_called_once()

    def test_a_missing_table_names_itself_and_the_fix(self) -> None:
        with mock.patch.object(main_module, "Database"), mock.patch.object(
            main_module, "Migrator"
        ) as migrator:
            migrator.return_value.missing_tables.return_value = ("reviews",)

            with self.assertRaises(MigrationError) as caught:
                main_module._check_schema(Settings.for_testing())

        self.assertIn("reviews", str(caught.exception))
        self.assertIn("migrate", str(caught.exception))


class SignalTests(unittest.TestCase):
    def test_sigterm_and_sigint_both_request_a_stop(self) -> None:
        handlers = {}

        def capture(signum, handler):
            handlers[signum] = handler

        with mock.patch.object(main_module, "_migrate"), mock.patch.object(
            main_module, "Supervisor"
        ) as supervisor, mock.patch.object(main_module.signal, "signal", capture):
            supervisor.return_value.run.return_value = 0
            main_module._run(Settings.for_testing(), skip_migrate=False)

            for signum in (main_module.signal.SIGINT, main_module.signal.SIGTERM):
                with self.subTest(signal=signum):
                    handlers[signum](signum, None)

        self.assertEqual(supervisor.return_value.request_stop.call_count, 2)


if __name__ == "__main__":
    unittest.main()
