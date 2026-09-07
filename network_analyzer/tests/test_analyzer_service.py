"""Tests for the analyze/validate/publish use case."""

from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

from kafka.errors import KafkaTimeoutError

from network_analyzer.analyzer_service import AnalyzerService
from network_analyzer.exceptions import (
    ApplicationNotEligibleError,
    FilenameConventionError,
    PcapReadError,
)
from network_analyzer.models import Scenario
from network_analyzer.tests.synthetic_pcap import (
    Capture,
    handshake,
    tcp_packet,
    write_pcap,
)

_FIXED_TIME = datetime(2026, 9, 7, 14, 20, 11, tzinfo=timezone.utc)
_FIXED_ID = "8f14e45f-ea6a-4f2b-9c1d-2b3a5c7d9e01"


class AnalyzerServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.tmp_path = Path(self._tmp.name)
        self.addCleanup(self._tmp.cleanup)

        self.registry = Mock()
        self.registry.ensure_eligible.return_value = {"id": 1}
        self.publisher = Mock()

    def build_service(self, **overrides: object) -> AnalyzerService:
        kwargs: dict[str, object] = {
            "registry_client": self.registry,
            "publisher": self.publisher,
            "clock": lambda: _FIXED_TIME,
            "analysis_id_factory": lambda: _FIXED_ID,
        }
        kwargs.update(overrides)
        return AnalyzerService(**kwargs)  # type: ignore[arg-type]

    def write_capture(
        self, name: str = "com.whatsapp__upload__20260907T141500.pcap"
    ) -> Path:
        capture = Capture.of(
            *handshake(start=1000.0, rtt_seconds=0.025, client_seq=1000),
            (1000.10, tcp_packet(seq=1001, payload=b"a" * 500)),
            (1000.20, tcp_packet(seq=1001, payload=b"a" * 500)),
            (1000.30, tcp_packet(flags="A", window=0)),
            (1000.40, tcp_packet(flags="R")),
        )
        return write_pcap(self.tmp_path / name, capture)


class HappyPathTests(AnalyzerServiceTestCase):
    def test_derives_target_from_the_filename(self) -> None:
        path = self.write_capture()

        result = self.build_service().analyze_file(path)

        self.assertEqual(result.package_name, "com.whatsapp")
        self.assertEqual(result.scenario, Scenario.UPLOAD)

    def test_stamps_injected_identity_and_time(self) -> None:
        path = self.write_capture()

        result = self.build_service().analyze_file(path)

        self.assertEqual(result.analysis_id, _FIXED_ID)
        self.assertEqual(result.analyzed_at, _FIXED_TIME)

    def test_records_only_the_filename_not_the_path(self) -> None:
        path = self.write_capture()

        result = self.build_service().analyze_file(path)

        self.assertEqual(
            result.source_pcap_filename,
            "com.whatsapp__upload__20260907T141500.pcap",
        )

    def test_computes_the_metrics(self) -> None:
        path = self.write_capture()

        result = self.build_service().analyze_file(path)

        assert result.metrics.rtt_handshake_ms is not None
        self.assertAlmostEqual(result.metrics.rtt_handshake_ms, 25.0, places=2)
        self.assertEqual(result.metrics.retransmission_count, 1)
        self.assertEqual(result.metrics.zero_window_count, 1)
        self.assertEqual(result.metrics.tcp_reset_count, 1)

    def test_publishes_keyed_by_package_name(self) -> None:
        path = self.write_capture()

        self.build_service().analyze_file(path)

        package_name, message = self.publisher.publish.call_args[0]
        self.assertEqual(package_name, "com.whatsapp")
        self.assertEqual(message["analysis_id"], _FIXED_ID)
        self.assertEqual(message["scenario"], "UPLOAD")

    def test_checks_the_registry_before_publishing(self) -> None:
        path = self.write_capture()

        self.build_service().analyze_file(path)

        self.registry.ensure_eligible.assert_called_once_with("com.whatsapp")


class OverrideTests(AnalyzerServiceTestCase):
    def test_explicit_values_win_over_the_filename(self) -> None:
        path = self.write_capture()

        result = self.build_service().analyze_file(
            path, package_name="com.override", scenario=Scenario.DOWNLOAD
        )

        self.assertEqual(result.package_name, "com.override")
        self.assertEqual(result.scenario, Scenario.DOWNLOAD)

    def test_filename_supplies_whichever_value_is_missing(self) -> None:
        path = self.write_capture()

        result = self.build_service().analyze_file(path, scenario=Scenario.DOWNLOAD)

        self.assertEqual(result.package_name, "com.whatsapp")
        self.assertEqual(result.scenario, Scenario.DOWNLOAD)

    def test_full_overrides_allow_a_nonconforming_filename(self) -> None:
        path = write_pcap(
            self.tmp_path / "capture-from-phone.pcap",
            Capture.of((1.0, tcp_packet(seq=1, payload=b"a" * 100))),
        )

        result = self.build_service().analyze_file(
            path, package_name="com.whatsapp", scenario=Scenario.UPLOAD
        )

        self.assertEqual(result.package_name, "com.whatsapp")

    def test_nonconforming_filename_without_overrides_is_rejected(self) -> None:
        path = write_pcap(
            self.tmp_path / "capture-from-phone.pcap",
            Capture.of((1.0, tcp_packet(seq=1, payload=b"a" * 100))),
        )

        with self.assertRaises(FilenameConventionError):
            self.build_service().analyze_file(path)


class OptionalCollaboratorTests(AnalyzerServiceTestCase):
    def test_dry_run_returns_metrics_without_publishing(self) -> None:
        path = self.write_capture()

        result = self.build_service(publisher=None).analyze_file(path)

        self.assertEqual(result.analysis_id, _FIXED_ID)
        self.publisher.publish.assert_not_called()

    def test_dry_run_still_checks_the_registry(self) -> None:
        path = self.write_capture()

        self.build_service(publisher=None).analyze_file(path)

        self.registry.ensure_eligible.assert_called_once_with("com.whatsapp")

    def test_skipping_the_registry_still_publishes(self) -> None:
        path = self.write_capture()

        self.build_service(registry_client=None).analyze_file(path)

        self.registry.ensure_eligible.assert_not_called()
        self.publisher.publish.assert_called_once()

    def test_service_with_neither_collaborator_only_analyzes(self) -> None:
        path = self.write_capture()

        result = self.build_service(
            registry_client=None, publisher=None
        ).analyze_file(path)

        self.assertEqual(result.package_name, "com.whatsapp")
        self.registry.ensure_eligible.assert_not_called()
        self.publisher.publish.assert_not_called()


class FailureOrderingTests(AnalyzerServiceTestCase):
    def test_ineligible_package_is_rejected_before_reading_the_capture(self) -> None:
        """Validation first means an ineligible package costs no parsing work."""

        path = self.write_capture()
        self.registry.ensure_eligible.side_effect = ApplicationNotEligibleError(
            "not registered"
        )

        with self.assertRaises(ApplicationNotEligibleError):
            self.build_service().analyze_file(path)

        self.publisher.publish.assert_not_called()

    def test_unreadable_capture_publishes_nothing(self) -> None:
        path = self.tmp_path / "com.whatsapp__upload__20260907T141500.pcap"
        path.write_bytes(b"not a capture")

        with self.assertRaises(PcapReadError):
            self.build_service().analyze_file(path)

        self.publisher.publish.assert_not_called()

    def test_missing_capture_publishes_nothing(self) -> None:
        path = self.tmp_path / "com.whatsapp__upload__20260907T141500.pcap"

        with self.assertRaises(PcapReadError):
            self.build_service().analyze_file(path)

        self.publisher.publish.assert_not_called()

    def test_publish_failure_propagates(self) -> None:
        """A batch run must be able to record the file as failed."""

        path = self.write_capture()
        self.publisher.publish.side_effect = KafkaTimeoutError("no ack")

        with self.assertRaises(KafkaTimeoutError):
            self.build_service().analyze_file(path)

    def test_bad_filename_is_rejected_before_the_registry_is_consulted(self) -> None:
        path = write_pcap(
            self.tmp_path / "unnamed.pcap",
            Capture.of((1.0, tcp_packet(seq=1, payload=b"a" * 100))),
        )

        with self.assertRaises(FilenameConventionError):
            self.build_service().analyze_file(path)

        self.registry.ensure_eligible.assert_not_called()


class IdentityTests(AnalyzerServiceTestCase):
    def test_each_analysis_gets_a_distinct_id_by_default(self) -> None:
        """Re-analyzing the same file is a new run and a new database row."""

        path = self.write_capture()
        service = AnalyzerService(registry_client=None, publisher=self.publisher)

        first = service.analyze_file(path)
        second = service.analyze_file(path)

        self.assertNotEqual(first.analysis_id, second.analysis_id)

    def test_default_analysis_id_is_a_uuid(self) -> None:
        import uuid

        path = self.write_capture()
        service = AnalyzerService(registry_client=None, publisher=None)

        result = service.analyze_file(path)

        self.assertEqual(uuid.UUID(result.analysis_id).version, 4)

    def test_default_clock_is_timezone_aware_utc(self) -> None:
        path = self.write_capture()
        service = AnalyzerService(registry_client=None, publisher=None)

        result = service.analyze_file(path)

        self.assertIsNotNone(result.analyzed_at.tzinfo)
        self.assertEqual(result.analyzed_at.utcoffset(), timezone.utc.utcoffset(None))


if __name__ == "__main__":
    unittest.main()
