"""Tests for migration discovery and the SQL files themselves.

``discover_migrations`` is pure, so none of this needs a database: it reads the
shipped ``.sql`` files and asserts their properties directly. The integration
tests that need a real server live in
``storage_consumer/tests/integration/test_migrator.py``.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from storage_consumer.exceptions import MigrationError
from storage_consumer.persistence.migrator import OWNED_TABLES, discover_migrations


class DiscoveryTests(unittest.TestCase):
    def test_shipped_migrations_are_discovered_in_version_order(self) -> None:
        migrations = discover_migrations()

        self.assertEqual(
            [m.version for m in migrations], sorted(m.version for m in migrations)
        )

    def test_every_owned_table_has_a_migration(self) -> None:
        names = {m.name for m in discover_migrations()}

        self.assertEqual(names, set(OWNED_TABLES))

    def test_missing_directory_is_reported(self) -> None:
        with self.assertRaises(MigrationError) as caught:
            discover_migrations(self._tmp_path() / "absent")

        self.assertIn("does not exist", str(caught.exception))

    def test_empty_directory_is_reported(self) -> None:
        with self.assertRaises(MigrationError) as caught:
            discover_migrations(self._tmp_path())

        self.assertIn("No migration files", str(caught.exception))

    def test_unnumbered_filename_is_rejected(self) -> None:
        directory = self._tmp_path()
        (directory / "add_a_column.sql").write_text("SELECT 1", encoding="utf-8")

        with self.assertRaises(MigrationError) as caught:
            discover_migrations(directory)

        self.assertIn("NNNN_name.sql", str(caught.exception))

    def test_duplicate_version_is_rejected(self) -> None:
        """Two developers numbering a migration 0005 on separate branches."""

        directory = self._tmp_path()
        (directory / "0005_one.sql").write_text("SELECT 1", encoding="utf-8")
        (directory / "0005_two.sql").write_text("SELECT 1", encoding="utf-8")

        with self.assertRaises(MigrationError) as caught:
            discover_migrations(directory)

        self.assertIn("0005", str(caught.exception))

    def _tmp_path(self) -> Path:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        return Path(directory.name)


class SqlContentTests(unittest.TestCase):
    """Guards on the SQL text, which no unit test can otherwise reach."""

    def test_every_migration_is_re_runnable(self) -> None:
        """A migration interrupted between DDL and its version row must reapply."""

        for migration in discover_migrations():
            with self.subTest(migration=migration.label):
                self.assertIn(
                    "CREATE TABLE IF NOT EXISTS",
                    migration.path.read_text(encoding="utf-8"),
                )

    def test_every_table_declares_its_idempotency_key(self) -> None:
        """The whole design rests on these four natural keys existing."""

        expected = {
            "app_stats": "UNIQUE (application_id, crawled_at)",
            "reviews": "UNIQUE (review_id)",
            "network_metrics": "UNIQUE (analysis_id)",
            "dead_letter_events": 'UNIQUE (topic, "partition", kafka_offset)',
        }
        by_name = {m.name: m for m in discover_migrations()}

        for table, constraint in expected.items():
            with self.subTest(table=table):
                self.assertIn(
                    constraint, by_name[table].path.read_text(encoding="utf-8")
                )


if __name__ == "__main__":
    unittest.main()
