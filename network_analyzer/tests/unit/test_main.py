"""Tests for the CLI: argument handling, exit codes, and the report."""

from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch

import requests
from kafka.errors import KafkaTimeoutError

from network_analyzer import main as main_module
from network_analyzer.main import (
    EXIT_CAPTURE_UNREADABLE,
    EXIT_ERROR,
    EXIT_INPUT_REJECTED,
    EXIT_OK,
    EXIT_TRANSPORT_FAILURE,
    main,
)
from network_analyzer.core.models import Scenario
from network_analyzer.tests.synthetic_pcap import (
    Capture,
    handshake,
    tcp_packet,
    write_pcap,
)

_CAPTURE_NAME = "com.whatsapp__upload__20260907T141500.pcap"
_ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": "broker:29092",
    "APP_API_BASE_URL": "http://app-api:8000",
}


class CliTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

        self.capture_path = write_pcap(
            self.tmp_path / _CAPTURE_NAME,
            Capture.of(
                *handshake(start=1000.0, rtt_seconds=0.025, client_seq=1000),
                (1000.1, tcp_packet(seq=1001, payload=b"a" * 500)),
                (1000.2, tcp_packet(seq=1001, payload=b"a" * 500)),
                (1000.3, tcp_packet(flags="A", window=0)),
                (1000.4, tcp_packet(flags="R")),
            ),
        )

        self.registry = Mock()
        self.registry.ensure_eligible.return_value = {"id": 1}
        self.publisher = Mock()

        patcher_registry = patch.object(
            main_module, "AppRegistryClient", return_value=self.registry
        )
        patcher_publisher = patch.object(
            main_module, "NetworkMetricsPublisher", return_value=self.publisher
        )
        patcher_env = patch.dict("os.environ", _ENV, clear=True)
        # Point dotenv at a file that cannot exist, so a developer's real
        # network_analyzer/.env never leaks into these assertions.
        patcher_env_file = patch(
            "network_analyzer.config._ANALYZER_ENV_FILE",
            Path("/nonexistent/network-analyzer.env"),
        )
        for patcher in (
            patcher_registry,
            patcher_publisher,
            patcher_env,
            patcher_env_file,
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

    def run_cli(self, *args: str) -> tuple[int, str]:
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = main(list(args))
        return code, buffer.getvalue()


class SuccessTests(CliTestCase):
    def test_analyzes_and_publishes(self) -> None:
        code, _ = self.run_cli("--file", str(self.capture_path))

        self.assertEqual(code, EXIT_OK)
        self.publisher.publish.assert_called_once()

    def test_report_names_every_metric(self) -> None:
        _, output = self.run_cli("--file", str(self.capture_path))

        self.assertIn("Handshake RTT", output)
        self.assertIn("Retransmissions", output)
        self.assertIn("Out-of-order", output)
        self.assertIn("Spurious retran.", output)
        self.assertIn("Zero-window events", output)
        self.assertIn("TCP resets", output)
        self.assertIn("Total bytes", output)
        self.assertIn("Payload bytes", output)
        self.assertIn("Overhead ratio", output)

    def test_report_shows_the_computed_values(self) -> None:
        _, output = self.run_cli("--file", str(self.capture_path))

        self.assertIn("25.00 ms", output)
        self.assertIn(_CAPTURE_NAME, output)
        self.assertIn("com.whatsapp", output)
        self.assertIn("UPLOAD", output)

    def test_report_confirms_publication(self) -> None:
        _, output = self.run_cli("--file", str(self.capture_path))

        self.assertIn("Published to the network-metrics topic.", output)

    def test_overrides_are_forwarded(self) -> None:
        code, output = self.run_cli(
            "--file",
            str(self.capture_path),
            "--package",
            "com.override",
            "--scenario",
            "download",
        )

        self.assertEqual(code, EXIT_OK)
        self.assertIn("com.override", output)
        self.assertIn("DOWNLOAD", output)
        self.registry.ensure_eligible.assert_called_once_with("com.override")

    def test_closes_the_publisher(self) -> None:
        self.run_cli("--file", str(self.capture_path))

        self.publisher.close.assert_called_once()


class DryRunTests(CliTestCase):
    def test_dry_run_publishes_nothing(self) -> None:
        code, output = self.run_cli("--file", str(self.capture_path), "--dry-run")

        self.assertEqual(code, EXIT_OK)
        self.publisher.publish.assert_not_called()
        self.assertIn("Dry run: nothing was published.", output)

    def test_dry_run_still_reports_the_metrics(self) -> None:
        _, output = self.run_cli("--file", str(self.capture_path), "--dry-run")

        self.assertIn("25.00 ms", output)

    def test_dry_run_never_constructs_a_producer(self) -> None:
        with patch.object(main_module, "NetworkMetricsPublisher") as producer_class:
            self.run_cli("--file", str(self.capture_path), "--dry-run")

        producer_class.assert_not_called()

    def test_fully_offline_run_needs_no_connection_settings(self) -> None:
        """A dry run that skips the registry talks to nothing at all."""

        with patch.dict("os.environ", {}, clear=True):
            code, output = self.run_cli(
                "--file",
                str(self.capture_path),
                "--dry-run",
                "--skip-registry-check",
            )

        self.assertEqual(code, EXIT_OK)
        self.assertIn("25.00 ms", output)


class RegistryTests(CliTestCase):
    def test_registry_is_consulted_by_default(self) -> None:
        self.run_cli("--file", str(self.capture_path))

        self.registry.ensure_eligible.assert_called_once_with("com.whatsapp")

    def test_skip_registry_check_bypasses_it(self) -> None:
        with patch.object(main_module, "AppRegistryClient") as client_class:
            code, _ = self.run_cli(
                "--file", str(self.capture_path), "--skip-registry-check"
            )

        self.assertEqual(code, EXIT_OK)
        client_class.assert_not_called()


class ExitCodeTests(CliTestCase):
    def test_nonconforming_filename_is_rejected(self) -> None:
        path = write_pcap(
            self.tmp_path / "phone-capture.pcap",
            Capture.of((1.0, tcp_packet(seq=1, payload=b"a" * 100))),
        )

        code, _ = self.run_cli("--file", str(path))

        self.assertEqual(code, EXIT_INPUT_REJECTED)

    def test_ineligible_application_is_rejected(self) -> None:
        from network_analyzer.exceptions import ApplicationNotEligibleError

        self.registry.ensure_eligible.side_effect = ApplicationNotEligibleError(
            "not registered"
        )

        code, _ = self.run_cli("--file", str(self.capture_path))

        self.assertEqual(code, EXIT_INPUT_REJECTED)

    def test_missing_capture_reports_unreadable(self) -> None:
        absent = self.tmp_path / "com.whatsapp__download__20260101T000000.pcap"

        code, _ = self.run_cli("--file", str(absent))

        self.assertEqual(code, EXIT_CAPTURE_UNREADABLE)

    def test_nonconforming_name_is_rejected_before_the_file_is_opened(self) -> None:
        """Target resolution precedes I/O, so the naming error is the useful one."""

        code, _ = self.run_cli("--file", str(self.tmp_path / "absent.pcap"))

        self.assertEqual(code, EXIT_INPUT_REJECTED)

    def test_garbage_capture_reports_unreadable(self) -> None:
        path = self.tmp_path / _CAPTURE_NAME
        path.write_bytes(b"not a capture file")

        code, _ = self.run_cli("--file", str(path))

        self.assertEqual(code, EXIT_CAPTURE_UNREADABLE)

    def test_unreachable_registry_is_retryable(self) -> None:
        self.registry.ensure_eligible.side_effect = requests.ConnectionError("down")

        code, _ = self.run_cli("--file", str(self.capture_path))

        self.assertEqual(code, EXIT_TRANSPORT_FAILURE)

    def test_unreachable_broker_is_retryable(self) -> None:
        self.publisher.publish.side_effect = KafkaTimeoutError("no ack")

        code, _ = self.run_cli("--file", str(self.capture_path))

        self.assertEqual(code, EXIT_TRANSPORT_FAILURE)

    def test_missing_configuration_is_an_error(self) -> None:
        with patch.dict("os.environ", {}, clear=True):
            code, _ = self.run_cli("--file", str(self.capture_path))

        self.assertEqual(code, EXIT_ERROR)

    def test_registry_rejecting_the_request_is_not_retryable(self) -> None:
        """A 4xx must not leave the capture looping in the inbox forever."""

        from network_analyzer.exceptions import RegistryRequestError

        self.registry.ensure_eligible.side_effect = RegistryRequestError(
            "rejected with HTTP 400"
        )

        code, _ = self.run_cli("--file", str(self.capture_path))

        self.assertEqual(code, EXIT_ERROR)
        self.assertNotEqual(code, EXIT_TRANSPORT_FAILURE)

    def test_unexpected_failure_is_an_error(self) -> None:
        self.registry.ensure_eligible.side_effect = RuntimeError("boom")

        code, _ = self.run_cli("--file", str(self.capture_path))

        self.assertEqual(code, EXIT_ERROR)

    def test_exit_codes_are_distinct(self) -> None:
        """The batch script routes files by these values, so they must differ."""

        codes = [
            EXIT_OK,
            EXIT_ERROR,
            EXIT_INPUT_REJECTED,
            EXIT_CAPTURE_UNREADABLE,
            EXIT_TRANSPORT_FAILURE,
        ]

        self.assertEqual(len(set(codes)), len(codes))
        # 2 belongs to argparse.
        self.assertNotIn(2, codes)


class ArgumentParsingTests(CliTestCase):
    def test_file_is_required(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            main([])

        self.assertEqual(ctx.exception.code, 2)

    def test_scenario_is_case_insensitive(self) -> None:
        code, output = self.run_cli(
            "--file", str(self.capture_path), "--scenario", "DoWnLoAd"
        )

        self.assertEqual(code, EXIT_OK)
        self.assertIn("DOWNLOAD", output)

    def test_invalid_scenario_is_a_usage_error(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            main(["--file", str(self.capture_path), "--scenario", "sideways"])

        self.assertEqual(ctx.exception.code, 2)


class ReportFormattingTests(CliTestCase):
    def test_absent_handshake_is_reported_as_not_available(self) -> None:
        path = write_pcap(
            self.tmp_path / _CAPTURE_NAME,
            Capture.of((1.0, tcp_packet(seq=5000, payload=b"a" * 100))),
        )

        _, output = self.run_cli("--file", str(path))

        self.assertIn("no complete handshake", output)

    def test_handshake_count_is_pluralized(self) -> None:
        path = write_pcap(
            self.tmp_path / _CAPTURE_NAME,
            Capture.of(
                *handshake(start=100.0, rtt_seconds=0.02, client_port=40001),
                *handshake(start=200.0, rtt_seconds=0.04, client_port=40002),
            ),
        )

        _, output = self.run_cli("--file", str(path))

        self.assertIn("mean of 2 handshakes", output)

    def test_large_byte_counts_are_thousands_separated(self) -> None:
        path = write_pcap(
            self.tmp_path / _CAPTURE_NAME,
            Capture.of(
                *[
                    (1.0 + index * 0.01, tcp_packet(seq=1 + index * 1000, payload=b"a" * 1000))
                    for index in range(3)
                ]
            ),
        )

        _, output = self.run_cli("--file", str(path))

        self.assertIn("3,120", output)

    def test_overhead_ratio_is_shown_as_a_percentage(self) -> None:
        _, output = self.run_cli("--file", str(self.capture_path))

        self.assertRegex(output, r"Overhead ratio\s+\d+\.\d{4} %")


if __name__ == "__main__":
    unittest.main()
