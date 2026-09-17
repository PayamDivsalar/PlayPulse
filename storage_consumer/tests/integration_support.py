"""Shared helpers for the tests marked ``integration``.

Not a ``conftest.py`` on purpose: the repository's test suites are plain
``unittest.TestCase`` bodies run under pytest, with no fixture magic, so a
helper anybody can read and import beats an implicit fixture.

Every integration test calls one of the ``*_or_skip`` helpers first, so the
suite stays green on a machine with no containers running.
"""

from __future__ import annotations

import os
import socket
import unittest
import uuid
from contextlib import closing
from pathlib import Path

from dotenv import load_dotenv

from storage_consumer.config import Settings

_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"

# Package names these tests register in apps_registry_application. Prefixed so
# a leftover row from a crashed test is obvious, and so no test can collide
# with a real monitored app.
TEST_PACKAGE_PREFIX = "com.playpulse.itest"


def unique_package_name() -> str:
    """A package name no other test run will use."""

    return f"{TEST_PACKAGE_PREFIX}.{uuid.uuid4().hex[:12]}"


def settings_or_skip(**overrides: object) -> Settings:
    """Build ``Settings`` from ``storage_consumer/.env``, or skip the test.

    Host-side defaults are applied for the two variables whose value differs
    between the host and the container network, so the tests run from a venv
    without the operator editing anything.
    """

    load_dotenv(_ENV_FILE)
    missing = [
        name
        for name in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
        if not os.getenv(name)
    ]
    if missing:
        raise unittest.SkipTest(
            f"{', '.join(missing)} not set; copy storage_consumer/.env.example "
            "to storage_consumer/.env to run the integration tests."
        )

    values: dict[str, object] = {
        "kafka_bootstrap_servers": os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        ),
        "postgres_db": os.environ["POSTGRES_DB"],
        "postgres_user": os.environ["POSTGRES_USER"],
        "postgres_password": os.environ["POSTGRES_PASSWORD"],
        "postgres_host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "postgres_port": int(os.getenv("POSTGRES_PORT", "5432")),
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def require_postgres(settings: Settings) -> None:
    """Skip unless the Postgres port accepts a TCP connection."""

    _require_port(settings.postgres_host, settings.postgres_port, "PostgreSQL")


def require_kafka(settings: Settings) -> None:
    """Skip unless the first bootstrap server accepts a TCP connection."""

    first = settings.kafka_bootstrap_servers.split(",")[0]
    host, _, port = first.rpartition(":")
    _require_port(host or "localhost", int(port), "Kafka")


def _require_port(host: str, port: int, label: str) -> None:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as probe:
        probe.settimeout(2.0)
        if probe.connect_ex((host, port)) != 0:
            raise unittest.SkipTest(
                f"{label} is not reachable at {host}:{port}; "
                "start it with `docker compose up -d`."
            )


def register_application(cursor, package_name: str) -> int:
    """Insert a row into the table ``app_api`` owns and return its id.

    The integration tests need a real foreign-key target, and creating it
    through Django would mean importing the whole framework into this
    subsystem's test suite. Writing the row directly is a test-only exception
    to "app_api owns that table" and is confined to this helper.
    """

    cursor.execute(
        """
        INSERT INTO apps_registry_application
            (package_name, is_messaging_app, is_active, is_iranian_app, created_at, updated_at)
        VALUES (%s, TRUE, TRUE, FALSE, now(), now())
        ON CONFLICT (package_name) DO UPDATE SET updated_at = now()
        RETURNING id
        """,
        (package_name,),
    )
    return cursor.fetchone()[0]


def delete_test_applications(cursor) -> None:
    """Remove every application this test suite created.

    The owned tables cascade from ``apps_registry_application``, so this one
    delete also clears the app_stats / reviews / network_metrics rows the test
    wrote.
    """

    cursor.execute(
        "DELETE FROM apps_registry_application WHERE package_name LIKE %s",
        (f"{TEST_PACKAGE_PREFIX}.%",),
    )
