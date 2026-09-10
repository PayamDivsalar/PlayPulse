"""Central configuration for the storage consumer subsystem.

Production code loads settings once at the composition root (``main``) via
``load_settings()`` and injects them into collaborators. Docker injects
connection hostnames through the process environment; ``load_dotenv`` never
overrides variables already set (see ``docs/setup.md``).

The Postgres variable names are deliberately the ones already used by
``.env.example`` and ``app_api/.env.example`` rather than new ones: this
subsystem talks to the same database as Django, and two names for one
credential is how environments drift apart.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

from dotenv import load_dotenv

from storage_consumer.exceptions import ConfigError

_STORAGE_ENV_FILE = Path(__file__).resolve().parent / ".env"

# The three topics this subsystem owns. Order is the order pipelines start in.
PIPELINE_NAMES: tuple[str, ...] = ("app-stats", "reviews", "network-metrics")

_VALID_AUTO_OFFSET_RESETS = frozenset({"earliest", "latest"})

# Env suffix for per-pipeline batch sizes: app-stats -> APP_STATS.
_PIPELINE_ENV_SUFFIX = {
    name: name.upper().replace("-", "_") for name in PIPELINE_NAMES
}


@dataclass(frozen=True, slots=True)
class Settings:
    """Immutable storage consumer settings — the single runtime source of truth.

    Shared by every pipeline thread. Frozen and slotted so a worker cannot
    mutate what its siblings are reading: ``Settings`` is the *only* object the
    threads have in common besides the stop event, and that is what makes the
    share-nothing concurrency model safe without a single lock.
    """

    kafka_bootstrap_servers: str
    postgres_db: str
    postgres_user: str
    postgres_password: str
    postgres_host: str
    postgres_port: int = 5432
    pipelines: tuple[str, ...] = PIPELINE_NAMES
    consumer_group_prefix: str = "storage-consumer"
    poll_timeout_ms: int = 1000
    # Global batch-size override. Applied only to pipelines that do not have
    # an entry in ``max_poll_records_by_pipeline``. Prefer the per-pipeline
    # knobs for normal tuning; keep this for "set every unset pipeline to N".
    max_poll_records_override: int | None = None
    # Per-pipeline batch sizes from the environment. Keys are pipeline names
    # (``reviews``, …). Empty means "use the code defaults in pipelines.py".
    # Frozen via MappingProxyType in ``__post_init__`` so threads cannot race
    # a shared Settings by mutating the map.
    max_poll_records_by_pipeline: Mapping[str, int] = field(default_factory=dict)
    max_poll_interval_ms: int = 300_000
    session_timeout_ms: int = 45_000
    heartbeat_interval_ms: int = 3_000
    auto_offset_reset: str = "earliest"
    db_connect_timeout_seconds: int = 10
    db_retry_max_attempts: int = 5
    db_retry_base_delay_seconds: float = 1.0
    db_statement_timeout_ms: int = 30_000
    db_idle_in_transaction_timeout_ms: int = 60_000
    application_cache_ttl_seconds: float = 300.0
    shutdown_timeout_seconds: float = 30.0
    heartbeat_directory: str = "/tmp/storage-consumer"
    heartbeat_interval_seconds: float = 10.0

    def __post_init__(self) -> None:
        if not self.kafka_bootstrap_servers.strip():
            raise ConfigError("kafka_bootstrap_servers must not be empty.")
        for name in ("postgres_db", "postgres_user", "postgres_host"):
            if not str(getattr(self, name)).strip():
                raise ConfigError(f"{name} must not be empty.")
        if not 0 < self.postgres_port < 65536:
            raise ConfigError("postgres_port must be between 1 and 65535.")
        if not self.pipelines:
            raise ConfigError(
                "pipelines must name at least one of: "
                f"{', '.join(PIPELINE_NAMES)}."
            )
        unknown = [name for name in self.pipelines if name not in PIPELINE_NAMES]
        if unknown:
            raise ConfigError(
                f"Unknown pipeline(s): {', '.join(unknown)}. "
                f"Expected any of: {', '.join(PIPELINE_NAMES)}."
            )
        if len(set(self.pipelines)) != len(self.pipelines):
            raise ConfigError("pipelines must not repeat a pipeline name.")
        if not self.consumer_group_prefix.strip():
            raise ConfigError("consumer_group_prefix must not be empty.")
        if self.poll_timeout_ms <= 0:
            raise ConfigError("poll_timeout_ms must be > 0.")
        if (
            self.max_poll_records_override is not None
            and self.max_poll_records_override <= 0
        ):
            raise ConfigError("max_poll_records_override must be > 0 when set.")
        unknown_batch = [
            name
            for name in self.max_poll_records_by_pipeline
            if name not in PIPELINE_NAMES
        ]
        if unknown_batch:
            raise ConfigError(
                f"Unknown pipeline(s) in max_poll_records_by_pipeline: "
                f"{', '.join(unknown_batch)}. Expected any of: "
                f"{', '.join(PIPELINE_NAMES)}."
            )
        for name, value in self.max_poll_records_by_pipeline.items():
            if value <= 0:
                raise ConfigError(
                    f"max_poll_records for pipeline {name!r} must be > 0."
                )
        # Snapshot as a read-only mapping so a frozen Settings cannot grow a
        # mutable hole that three threads would share.
        object.__setattr__(
            self,
            "max_poll_records_by_pipeline",
            MappingProxyType(dict(self.max_poll_records_by_pipeline)),
        )
        if self.max_poll_interval_ms <= 0:
            raise ConfigError("max_poll_interval_ms must be > 0.")
        if self.session_timeout_ms <= 0:
            raise ConfigError("session_timeout_ms must be > 0.")
        if self.heartbeat_interval_ms <= 0:
            raise ConfigError("heartbeat_interval_ms must be > 0.")
        # Kafka's own requirement: the broker evicts a member that has not sent
        # a heartbeat within the session timeout, so the interval must leave
        # room for several attempts. The client library enforces this too, but
        # failing here names the setting instead of raising mid-poll.
        if self.heartbeat_interval_ms >= self.session_timeout_ms:
            raise ConfigError(
                "heartbeat_interval_ms must be < session_timeout_ms."
            )
        if self.auto_offset_reset not in _VALID_AUTO_OFFSET_RESETS:
            raise ConfigError(
                "auto_offset_reset must be one of: earliest, latest."
            )
        if self.db_connect_timeout_seconds <= 0:
            raise ConfigError("db_connect_timeout_seconds must be > 0.")
        if self.db_retry_max_attempts < 0:
            raise ConfigError("db_retry_max_attempts must be >= 0.")
        if self.db_retry_base_delay_seconds < 0:
            raise ConfigError("db_retry_base_delay_seconds must be >= 0.")
        if self.db_statement_timeout_ms <= 0:
            raise ConfigError("db_statement_timeout_ms must be > 0.")
        if self.db_idle_in_transaction_timeout_ms <= 0:
            raise ConfigError("db_idle_in_transaction_timeout_ms must be > 0.")
        if self.application_cache_ttl_seconds < 0:
            raise ConfigError("application_cache_ttl_seconds must be >= 0.")
        if self.shutdown_timeout_seconds <= 0:
            raise ConfigError("shutdown_timeout_seconds must be > 0.")
        if self.heartbeat_interval_seconds <= 0:
            raise ConfigError("heartbeat_interval_seconds must be > 0.")
        # A batch that exhausts its DB retry budget must still leave the worker
        # time to commit before the broker declares it dead and rebalances the
        # partition away mid-write. Livelocking on rebalances is much harder to
        # diagnose than a startup error naming the two settings involved.
        if self.retry_window_seconds * 1000 >= self.max_poll_interval_ms:
            raise ConfigError(
                "max_poll_interval_ms must exceed the worst-case database "
                f"retry window ({self.retry_window_seconds:.0f}s from "
                "db_retry_max_attempts and db_retry_base_delay_seconds)."
            )

    @property
    def retry_window_seconds(self) -> float:
        """Worst-case wall time a single batch can spend retrying the database.

        Sum of the exponential backoff delays plus one statement timeout per
        attempt, i.e. the longest a poll cycle can take before the consumer
        gets back to the broker.
        """

        backoff = self.db_retry_base_delay_seconds * (
            2**self.db_retry_max_attempts - 1
        )
        statements = (self.db_retry_max_attempts + 1) * (
            self.db_statement_timeout_ms / 1000.0
        )
        return backoff + statements

    def group_id(self, pipeline: str) -> str:
        """Consumer group for one pipeline.

        One group per topic, never one group for all three: separate groups are
        what let an operator reset ``reviews`` to earliest to rebuild that
        table without replaying ``app-stats``, and what makes per-topic lag
        visible in kafka-ui.
        """

        return f"{self.consumer_group_prefix}.{pipeline}"

    def max_poll_records_for(self, pipeline: str, default: int) -> int:
        """Resolve one pipeline's batch size.

        Precedence, most specific first:

        1. ``max_poll_records_by_pipeline[pipeline]`` (per-topic env)
        2. ``max_poll_records_override`` (global env, only if no per-topic value)
        3. ``default`` (the tuned constant in ``pipelines.py``)

        That order is what lets an operator raise reviews alone without
        flattening the latency-tuned network-metrics batch, while still
        keeping a single global override for "everything unset becomes N".
        """

        if pipeline in self.max_poll_records_by_pipeline:
            return self.max_poll_records_by_pipeline[pipeline]
        if self.max_poll_records_override is not None:
            return self.max_poll_records_override
        return default

    @classmethod
    def for_testing(cls, **overrides: object) -> Settings:
        """Build settings for unit tests without touching the real ``.env``."""

        values: dict[str, object] = {
            "kafka_bootstrap_servers": "localhost:9092",
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


def _env_optional_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name} must be an int.") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name} must be a float.") from exc


def parse_pipelines(raw: str | None) -> tuple[str, ...]:
    """Parse a pipeline selection like ``"reviews, app-stats"`` or ``"all"``.

    Shared by ``load_settings`` and ``main``'s ``--pipeline`` flag so the flag
    and the environment variable cannot disagree about what a name means.
    """

    if raw is None or not raw.strip():
        return PIPELINE_NAMES
    names = [part.strip() for part in raw.split(",") if part.strip()]
    if not names:
        return PIPELINE_NAMES
    if names == ["all"]:
        return PIPELINE_NAMES
    if "all" in names:
        raise ConfigError("Pipeline selection 'all' cannot be combined with names.")
    return tuple(names)


def _load_max_poll_records_by_pipeline() -> dict[str, int]:
    """Read ``STORAGE_MAX_POLL_RECORDS_<PIPELINE>`` for each known pipeline.

    Only set variables appear in the result, so an unset reviews knob still
    falls through to the global override or the code default.
    """

    found: dict[str, int] = {}
    for name, suffix in _PIPELINE_ENV_SUFFIX.items():
        value = _env_optional_int(f"STORAGE_MAX_POLL_RECORDS_{suffix}")
        if value is not None:
            found[name] = value
    return found


def load_settings(env_file: Path | None = None) -> Settings:
    """Load ``.env`` (if present) and build a validated ``Settings`` instance."""

    load_dotenv(env_file or _STORAGE_ENV_FILE)
    return Settings(
        kafka_bootstrap_servers=_require_env("KAFKA_BOOTSTRAP_SERVERS"),
        postgres_db=_require_env("POSTGRES_DB"),
        postgres_user=_require_env("POSTGRES_USER"),
        postgres_password=_require_env("POSTGRES_PASSWORD"),
        postgres_host=_require_env("POSTGRES_HOST"),
        postgres_port=_env_int("POSTGRES_PORT", 5432),
        pipelines=parse_pipelines(os.getenv("STORAGE_PIPELINES")),
        consumer_group_prefix=os.getenv(
            "STORAGE_CONSUMER_GROUP_PREFIX", "storage-consumer"
        )
        or "storage-consumer",
        poll_timeout_ms=_env_int("STORAGE_POLL_TIMEOUT_MS", 1000),
        max_poll_records_override=_env_optional_int("STORAGE_MAX_POLL_RECORDS"),
        max_poll_records_by_pipeline=_load_max_poll_records_by_pipeline(),
        max_poll_interval_ms=_env_int("STORAGE_MAX_POLL_INTERVAL_MS", 300_000),
        session_timeout_ms=_env_int("STORAGE_SESSION_TIMEOUT_MS", 45_000),
        heartbeat_interval_ms=_env_int("STORAGE_HEARTBEAT_INTERVAL_MS", 3_000),
        auto_offset_reset=os.getenv("STORAGE_AUTO_OFFSET_RESET", "earliest")
        or "earliest",
        db_connect_timeout_seconds=_env_int("STORAGE_DB_CONNECT_TIMEOUT_SECONDS", 10),
        db_retry_max_attempts=_env_int("STORAGE_DB_RETRY_MAX_ATTEMPTS", 5),
        db_retry_base_delay_seconds=_env_float(
            "STORAGE_DB_RETRY_BASE_DELAY_SECONDS", 1.0
        ),
        db_statement_timeout_ms=_env_int("STORAGE_DB_STATEMENT_TIMEOUT_MS", 30_000),
        db_idle_in_transaction_timeout_ms=_env_int(
            "STORAGE_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS", 60_000
        ),
        application_cache_ttl_seconds=_env_float(
            "STORAGE_APPLICATION_CACHE_TTL_SECONDS", 300.0
        ),
        shutdown_timeout_seconds=_env_float("STORAGE_SHUTDOWN_TIMEOUT_SECONDS", 30.0),
        heartbeat_directory=os.getenv(
            "STORAGE_HEARTBEAT_DIRECTORY", "/tmp/storage-consumer"
        )
        or "/tmp/storage-consumer",
        heartbeat_interval_seconds=_env_float(
            "STORAGE_HEARTBEAT_INTERVAL_SECONDS", 10.0
        ),
    )
