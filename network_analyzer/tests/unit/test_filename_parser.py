"""Tests for the capture filename convention."""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from network_analyzer.exceptions import FilenameConventionError
from network_analyzer.core.filename_parser import (
    build_capture_filename,
    parse_capture_filename,
)
from network_analyzer.core.models import Scenario


class ParseCaptureFilenameTests(unittest.TestCase):
    def test_parses_upload_capture(self) -> None:
        descriptor = parse_capture_filename("com.whatsapp__upload__20260907T141500.pcap")

        self.assertEqual(descriptor.package_name, "com.whatsapp")
        self.assertEqual(descriptor.scenario, Scenario.UPLOAD)
        self.assertEqual(
            descriptor.captured_at,
            datetime(2026, 9, 7, 14, 15, 0, tzinfo=timezone.utc),
        )

    def test_parses_download_capture(self) -> None:
        descriptor = parse_capture_filename("org.telegram.messenger__download__20260101T000000.pcap")

        self.assertEqual(descriptor.package_name, "org.telegram.messenger")
        self.assertEqual(descriptor.scenario, Scenario.DOWNLOAD)

    def test_ignores_leading_directories(self) -> None:
        descriptor = parse_capture_filename(
            "/srv/data/pcap/inbox/com.whatsapp__upload__20260907T141500.pcap"
        )

        self.assertEqual(descriptor.package_name, "com.whatsapp")

    def test_scenario_is_case_insensitive(self) -> None:
        descriptor = parse_capture_filename("com.whatsapp__UPLOAD__20260907T141500.pcap")

        self.assertEqual(descriptor.scenario, Scenario.UPLOAD)

    def test_accepts_pcapng_suffix(self) -> None:
        descriptor = parse_capture_filename("com.whatsapp__upload__20260907T141500.pcapng")

        self.assertEqual(descriptor.package_name, "com.whatsapp")

    def test_package_name_may_contain_underscores(self) -> None:
        descriptor = parse_capture_filename("com.my_app.beta__download__20260907T141500.pcap")

        self.assertEqual(descriptor.package_name, "com.my_app.beta")

    def test_rejects_unknown_suffix(self) -> None:
        with self.assertRaises(FilenameConventionError):
            parse_capture_filename("com.whatsapp__upload__20260907T141500.txt")

    def test_rejects_missing_scenario(self) -> None:
        with self.assertRaises(FilenameConventionError):
            parse_capture_filename("com.whatsapp__20260907T141500.pcap")

    def test_rejects_unknown_scenario(self) -> None:
        with self.assertRaises(FilenameConventionError):
            parse_capture_filename("com.whatsapp__sideways__20260907T141500.pcap")

    def test_rejects_package_without_dot(self) -> None:
        with self.assertRaises(FilenameConventionError):
            parse_capture_filename("whatsapp__upload__20260907T141500.pcap")

    def test_rejects_malformed_timestamp(self) -> None:
        with self.assertRaises(FilenameConventionError):
            parse_capture_filename("com.whatsapp__upload__2026-09-07.pcap")

    def test_rejects_impossible_calendar_date(self) -> None:
        with self.assertRaises(FilenameConventionError):
            parse_capture_filename("com.whatsapp__upload__20261345T141500.pcap")

    def test_error_message_names_the_expected_convention(self) -> None:
        with self.assertRaises(FilenameConventionError) as ctx:
            parse_capture_filename("random.pcap")

        message = str(ctx.exception)
        self.assertIn("--package", message)
        self.assertIn("com.whatsapp__upload__20260907T141500.pcap", message)


class BuildCaptureFilenameTests(unittest.TestCase):
    def test_round_trips_through_the_parser(self) -> None:
        captured_at = datetime(2026, 9, 7, 14, 15, 0, tzinfo=timezone.utc)

        name = build_capture_filename("com.whatsapp", Scenario.DOWNLOAD, captured_at)
        descriptor = parse_capture_filename(name)

        self.assertEqual(name, "com.whatsapp__download__20260907T141500.pcap")
        self.assertEqual(descriptor.package_name, "com.whatsapp")
        self.assertEqual(descriptor.scenario, Scenario.DOWNLOAD)
        self.assertEqual(descriptor.captured_at, captured_at)


if __name__ == "__main__":
    unittest.main()
