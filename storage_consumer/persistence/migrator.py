"""Numbered-SQL migration runner for the tables this subsystem owns.

Schema ownership is split: ``app_api`` owns ``apps_registry_application`` through
Django migrations, and this subsystem owns ``app_stats``, ``reviews``,
``network_metrics`` and ``dead_letter_events``. **Nobody must ever add Django
models for those four tables**, or two tools will fight over the same DDL.

No Alembic: it drags in SQLAlchemy, which is a large dependency for a project
with four tables and no ORM. A directory of numbered ``.sql`` files plus the
runner below is the whole mechanism, and the applied state is a table an
operator can read with ``psql``.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import psycopg2

from storage_consumer.exceptions import MigrationError
from storage_consumer.persistence.database import Database

logger = logging.getLogger(__name__)

_SQL_DIRECTORY = Path(__file__).resolve().parent / "sql"
_FILENAME_PATTERN = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")

MIGRATIONS_TABLE = "storage_consumer_migrations"

# The table this subsystem reads but does not own. Every one of its own tables
# has a foreign key to it, so a missing table means the migrations cannot run.
REQUIRED_FOREIGN_TABLE = "apps_registry_application"

# Serialises concurrent replicas. Any constant works as long as it is unique
# within the database; this one is arbitrary and only has to stay stable.
ADVISORY_LOCK_KEY = 8_251_477_301_964_142

# The tables `run` checks for before starting workers, in dependency order.
OWNED_TABLES: tuple[str, ...] = (
    "app_stats",
    "reviews",
    "network_metrics",
    "dead_letter_events",
)


@dataclass(frozen=True, slots=True)
class Migration:
    """One numbered ``.sql`` file."""

    version: int
    name: str
    path: Path

    @property
    def label(self) -> str:
        return f"{self.version:04d}_{self.name}"


def discover_migrations(directory: Path | None = None) -> tuple[Migration, ...]:
    """Load the migration files in version order.

    Raises:
        MigrationError: if the directory is missing, a filename does not follow
            ``NNNN_name.sql``, or two files claim the same version.
    """

    directory = directory or _SQL_DIRECTORY
    if not directory.is_dir():
        raise MigrationError(f"Migration directory {directory} does not exist.")

    migrations: dict[int, Migration] = {}
    for path in sorted(directory.glob("*.sql")):
        match = _FILENAME_PATTERN.match(path.name)
        if match is None:
            raise MigrationError(
                f"Migration file {path.name} does not follow NNNN_name.sql "
                "(four digits, then lowercase words separated by underscores)."
            )
        version = int(match.group(1))
        if version in migrations:
            raise MigrationError(
                f"Migration version {version:04d} is claimed by both "
                f"{migrations[version].path.name} and {path.name}."
            )
        migrations[version] = Migration(
            version=version, name=match.group(2), path=path
        )

    if not migrations:
        raise MigrationError(f"No migration files found in {directory}.")
    return tuple(migrations[version] for version in sorted(migrations))


class Migrator:
    """Applies pending migrations, safely and repeatably.

    Every migration runs in its own transaction together with the row that
    records it, so a failure leaves nothing behind: either the DDL and its
    bookkeeping are both committed, or neither is.
    """

    def __init__(
        self, database: Database, *, sql_directory: Path | None = None
    ) -> None:
        self._database = database
        self._sql_directory = sql_directory

    def apply_pending(self) -> tuple[Migration, ...]:
        """Bring the schema up to date and return what was applied.

        An empty result means the schema was already current, which is the
        normal case on every container restart.
        """

        migrations = discover_migrations(self._sql_directory)
        self._check_prerequisites()
        self._ensure_migrations_table()

        applied: list[Migration] = []
        for migration in migrations:
            if self._apply_one(migration):
                applied.append(migration)

        if applied:
            logger.info(
                "Applied %s migration(s): %s",
                len(applied),
                ", ".join(m.label for m in applied),
            )
        else:
            logger.info(
                "Schema is already up to date at version %s.",
                f"{migrations[-1].version:04d}",
            )
        return tuple(applied)

    def missing_tables(self) -> tuple[str, ...]:
        """Which owned tables do not exist yet.

        Used by ``run --skip-migrate`` to fail fast with a message naming the
        tables, instead of letting the first batch die on an opaque
        "relation does not exist".
        """

        with self._database.transaction() as cursor:
            cursor.execute(
                """
                SELECT name
                  FROM unnest(%s::text[]) AS name
                 WHERE to_regclass(name) IS NULL
                """,
                (list(OWNED_TABLES),),
            )
            return tuple(row[0] for row in cursor.fetchall())

    def _check_prerequisites(self) -> None:
        """Fail with an actionable message if Django has not migrated yet."""

        try:
            with self._database.transaction() as cursor:
                cursor.execute("SELECT to_regclass(%s)", (REQUIRED_FOREIGN_TABLE,))
                exists = cursor.fetchone()[0] is not None
        except psycopg2.Error as exc:
            raise MigrationError(
                f"Could not inspect the database before migrating: {exc}"
            ) from exc

        if not exists:
            raise MigrationError(
                f"Table {REQUIRED_FOREIGN_TABLE} does not exist. Every table "
                "this subsystem owns references it, so run the app_api "
                "migrations first:\n"
                "    cd app_api && python manage.py migrate"
            )

    def _ensure_migrations_table(self) -> None:
        try:
            with self._database.transaction() as cursor:
                cursor.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (
                        version    INTEGER PRIMARY KEY,
                        name       TEXT NOT NULL,
                        applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    )
                    """
                )
        except psycopg2.Error as exc:
            raise MigrationError(
                f"Could not create the {MIGRATIONS_TABLE} table: {exc}"
            ) from exc

    def _apply_one(self, migration: Migration) -> bool:
        """Apply one migration if it is pending. Returns whether it ran.

        The advisory lock is taken *before* reading the applied set, and the
        set is re-read inside the same transaction, so N replicas starting
        simultaneously cannot both decide a migration is pending. The lock is
        transaction-scoped, so it is released by the commit or rollback with no
        unlock call to forget.
        """

        statements = migration.path.read_text(encoding="utf-8")
        try:
            with self._database.transaction() as cursor:
                cursor.execute("SELECT pg_advisory_xact_lock(%s)", (ADVISORY_LOCK_KEY,))
                cursor.execute(
                    f"SELECT 1 FROM {MIGRATIONS_TABLE} WHERE version = %s",
                    (migration.version,),
                )
                if cursor.fetchone() is not None:
                    return False

                logger.info("Applying migration %s", migration.label)
                cursor.execute(statements)
                cursor.execute(
                    f"INSERT INTO {MIGRATIONS_TABLE} (version, name) VALUES (%s, %s)",
                    (migration.version, migration.name),
                )
        except psycopg2.Error as exc:
            raise MigrationError(
                f"Migration {migration.label} failed and was rolled back: {exc}"
            ) from exc
        return True
