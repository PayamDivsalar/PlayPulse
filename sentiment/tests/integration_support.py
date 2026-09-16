"""Shared helpers for sentiment integration tests.

Mirrors ``storage_consumer.tests.integration_support``: skip cleanly when
Postgres is not reachable, and never collide with real monitored apps.
"""

from __future__ import annotations

import os
import socket
import unittest
import uuid
from contextlib import closing
from pathlib import Path

from dotenv import load_dotenv

from sentiment.config import Settings

_ENV_CANDIDATES = (
    Path(__file__).resolve().parents[1] / ".env",
    Path(__file__).resolve().parents[2] / "storage_consumer" / ".env",
)

TEST_PACKAGE_PREFIX = "com.playpulse.sentiment.itest"


def unique_package_name() -> str:
    return f"{TEST_PACKAGE_PREFIX}.{uuid.uuid4().hex[:12]}"


def settings_or_skip(**overrides: object) -> Settings:
    """Build Settings from a local ``.env``, or skip the test."""

    for path in _ENV_CANDIDATES:
        if path.is_file():
            load_dotenv(path)
            break

    missing = [
        name
        for name in ("POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD")
        if not os.getenv(name)
    ]
    if missing:
        raise unittest.SkipTest(
            f"{', '.join(missing)} not set; copy sentiment/.env.example "
            "to sentiment/.env to run the integration tests."
        )

    values: dict[str, object] = {
        "postgres_db": os.environ["POSTGRES_DB"],
        "postgres_user": os.environ["POSTGRES_USER"],
        "postgres_password": os.environ["POSTGRES_PASSWORD"],
        "postgres_host": os.getenv("POSTGRES_HOST", "127.0.0.1"),
        "postgres_port": int(os.getenv("POSTGRES_PORT", "5432")),
        "batch_size": 100,
    }
    values.update(overrides)
    return Settings(**values)  # type: ignore[arg-type]


def require_postgres(settings: Settings) -> None:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as probe:
        probe.settimeout(2.0)
        if probe.connect_ex((settings.postgres_host, settings.postgres_port)) != 0:
            raise unittest.SkipTest(
                f"PostgreSQL is not reachable at "
                f"{settings.postgres_host}:{settings.postgres_port}; "
                "start it with `docker compose up -d`."
            )


def register_application(cursor, package_name: str) -> int:
    cursor.execute(
        """
        INSERT INTO apps_registry_application
            (package_name, is_messaging_app, is_active, created_at, updated_at)
        VALUES (%s, TRUE, TRUE, now(), now())
        ON CONFLICT (package_name) DO UPDATE SET updated_at = now()
        RETURNING id
        """,
        (package_name,),
    )
    return cursor.fetchone()[0]


def delete_test_applications(cursor) -> None:
    cursor.execute(
        "DELETE FROM apps_registry_application WHERE package_name LIKE %s",
        (f"{TEST_PACKAGE_PREFIX}.%",),
    )
