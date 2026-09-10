"""Tests for optional pending cycle-report writes."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from crawler.cycle_report import write_pending_cycle_report


class CycleReportTests(unittest.TestCase):
    def test_writes_pending_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            reports = Path(tmp)
            started = datetime(2026, 9, 14, 18, 0, 0, tzinfo=timezone.utc)
            finished = datetime(2026, 9, 14, 18, 2, 14, tzinfo=timezone.utc)
            path = write_pending_cycle_report(
                reports,
                started_at=started,
                finished_at=finished,
                apps_total=16,
                stats_ok=15,
                reviews_ok=16,
                failed_stats=["com.instagram.android"],
                failed_reviews=[],
            )
            self.assertIsNotNone(path)
            assert path is not None
            self.assertTrue(path.is_file())
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["apps_total"], 16)
            self.assertEqual(payload["app_stats"]["ok"], 15)
            self.assertEqual(payload["reviews"]["ok"], 16)
            self.assertEqual(payload["status"], "pending_persist")
            self.assertEqual(payload["app_stats"]["failed"], ["com.instagram.android"])
            self.assertEqual(payload["duration_crawl_s"], 134.0)
