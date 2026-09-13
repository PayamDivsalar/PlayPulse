"""Integration tests for the migration runner against a real PostgreSQL server.

These prove the properties only a live server can show: that the runner is
idempotent, that every owned table exists after it runs, and that it refuses to
run before Django has created the table every owned table references.

The pure discovery and SQL-content tests live in
``storage_consumer/tests/unit/test_migrator.py``.
"""

from __future__ import annotations

import unittest

import pytest

from storage_consumer.exceptions import MigrationError
from storage_consumer.persistence.database import Database
from storage_consumer.persistence.migrator import (
    MIGRATIONS_TABLE,
    Migrator,
    discover_migrations,
)
from storage_consumer.tests import integration_support


@pytest.mark.integration
class MigratorIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = integration_support.settings_or_skip()
        integration_support.require_postgres(self.settings)
        self.database = Database(self.settings, application_name="storage_consumer:test")
        self.addCleanup(self.database.close)

    def test_apply_pending_is_idempotent(self) -> None:
        Migrator(self.database).apply_pending()

        second_run = Migrator(self.database).apply_pending()

        self.assertEqual(second_run, ())

    def test_all_owned_tables_exist_after_migrating(self) -> None:
        Migrator(self.database).apply_pending()

        self.assertEqual(Migrator(self.database).missing_tables(), ())

    def test_every_applied_version_is_recorded(self) -> None:
        Migrator(self.database).apply_pending()

        with self.database.transaction() as cursor:
            cursor.execute(f"SELECT version FROM {MIGRATIONS_TABLE} ORDER BY version")
            recorded = [row[0] for row in cursor.fetchall()]

        self.assertEqual(recorded, [m.version for m in discover_migrations()])

    def test_app_stats_rejects_a_duplicate_application_and_crawl_time(self) -> None:
        """The constraint that makes replay a no-op actually exists."""

        Migrator(self.database).apply_pending()
        package_name = integration_support.unique_package_name()

        with self.database.transaction() as cursor:
            application_id = integration_support.register_application(
                cursor, package_name
            )
            self.addCleanup(self._delete_test_applications)
            cursor.execute(
                "INSERT INTO app_stats (application_id, crawled_at) "
                "VALUES (%s, now())",
                (application_id,),
            )
            cursor.execute(
                "SELECT crawled_at FROM app_stats WHERE application_id = %s",
                (application_id,),
            )
            crawled_at = cursor.fetchone()[0]

        with self.assertRaises(Exception) as caught:
            with self.database.transaction() as cursor:
                cursor.execute(
                    "INSERT INTO app_stats (application_id, crawled_at) "
                    "VALUES (%s, %s)",
                    (application_id, crawled_at),
                )

        self.assertIn(
            "app_stats_application_id_crawled_at_key", str(caught.exception)
        )

    def test_preflight_names_the_app_api_migrations(self) -> None:
        """A database without Django's table must say so, not fail obscurely."""

        probe_database = f"storage_consumer_preflight_{id(self):x}"
        self._create_database(probe_database)
        self.addCleanup(self._drop_database, probe_database)

        probe_settings = integration_support.settings_or_skip(
            postgres_db=probe_database
        )
        probe = Database(probe_settings, application_name="storage_consumer:test")
        self.addCleanup(probe.close)

        with self.assertRaises(MigrationError) as caught:
            Migrator(probe).apply_pending()

        self.assertIn("apps_registry_application", str(caught.exception))
        self.assertIn("manage.py migrate", str(caught.exception))

    def _delete_test_applications(self) -> None:
        with self.database.transaction() as cursor:
            integration_support.delete_test_applications(cursor)

    def _create_database(self, name: str) -> None:
        connection = self.database.connect()
        connection.autocommit = True
        try:
            with connection.cursor() as cursor:
                cursor.execute(f'CREATE DATABASE "{name}"')
        finally:
            connection.autocommit = False

    def _drop_database(self, name: str) -> None:
        connection = self.database.connect()
        connection.autocommit = True
        try:
            with connection.cursor() as cursor:
                cursor.execute(f'DROP DATABASE IF EXISTS "{name}"')
        finally:
            connection.autocommit = False


if __name__ == "__main__":
    unittest.main()
