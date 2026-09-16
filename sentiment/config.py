"""Central configuration for the sentiment batch job.

Production code loads settings once at the composition root (``main``) via
``load_settings()`` and injects them into collaborators. Docker injects
connection hostnames through the process environment; ``load_dotenv`` never
overrides variables already set (see ``docs/setup.md``).

Postgres variable names match ``storage_consumer`` / ``app_api`` on purpose:
this job writes the same ``reviews`` table those subsystems already own.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from sentiment.exceptions import ConfigError

_SENTIMENT_ENV_FILE = Path(__file__).resolve().parent / ".env"

DEFAULT_MODEL_NAME = "cardiffnlp/twitter-xlm-roberta-base-sentiment"
DEFAULT_BATCH_SIZE = 100


@dataclass(frozen=True, slots=True)
class Settings:
    """Immutable sentiment-job settings — the single runtime source of truth."""

    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int = 5432
    model_name: str = DEFAULT_MODEL_NAME
    batch_size: int = DEFAULT_BATCH_SIZE
    db_connect_timeout_seconds: int = 10
    db_statement_timeout_ms: int = 30_000
    db_idle_in_transaction_timeout_ms: int = 60_000

    def __post_init__(self) -> None:
        for name in ("postgres_db", "postgres_user", "postgres_host"):
            if not str(getattr(self, name)).strip():
                raise ConfigError(f"{name} must not be empty.")
        if not 0 < self.postgres_port < 65536:
            raise ConfigError("postgres_port must be between 1 and 65535.")
        if not self.model_name.strip():
            raise ConfigError("model_name must not be empty.")
        if self.batch_size <= 0:
            raise ConfigError("batch_size must be > 0.")
        if self.db_connect_timeout_seconds <= 0:
            raise ConfigError("db_connect_timeout_seconds must be > 0.")
        if self.db_statement_timeout_ms <= 0:
            raise ConfigError("db_statement_timeout_ms must be > 0.")
        if self.db_idle_in_transaction_timeout_ms <= 0:
            raise ConfigError("db_idle_in_transaction_timeout_ms must be > 0.")

    @classmethod
    def for_testing(cls, **overrides: object) -> Settings:
        """Build settings for unit tests without touching the real ``.env``."""

        values: dict[str, object] = {
            "postgres_db": "project_db",
            "postgres_user": "project_user",
            "postgres_password": "secret",
            "postgres_host": "127.0.0.1",
        }
        values.update(overrides)
        return cls(**values)  # type: ignore[arg-type]


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ConfigError(f"Required environment variable {name} is not set.")
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name} must be an int.") from exc


def load_settings(env_file: Path | None = None) -> Settings:
    """Load ``.env`` (if present) and build a validated ``Settings`` instance."""

    load_dotenv(env_file or _SENTIMENT_ENV_FILE)
    return Settings(
        postgres_db=_require_env("POSTGRES_DB"),
        postgres_user=_require_env("POSTGRES_USER"),
        postgres_password=_require_env("POSTGRES_PASSWORD"),
        postgres_host=_require_env("POSTGRES_HOST"),
        postgres_port=_env_int("POSTGRES_PORT", 5432),
        model_name=os.getenv("SENTIMENT_MODEL_NAME", DEFAULT_MODEL_NAME)
        or DEFAULT_MODEL_NAME,
        batch_size=_env_int("SENTIMENT_BATCH_SIZE", DEFAULT_BATCH_SIZE),
        db_connect_timeout_seconds=_env_int(
            "SENTIMENT_DB_CONNECT_TIMEOUT_SECONDS", 10
        ),
        db_statement_timeout_ms=_env_int("SENTIMENT_DB_STATEMENT_TIMEOUT_MS", 30_000),
        db_idle_in_transaction_timeout_ms=_env_int(
            "SENTIMENT_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS", 60_000
        ),
    )
