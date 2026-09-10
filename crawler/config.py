"""Central configuration for the crawler subsystem.

Production code loads settings once at the composition root (``main``) via
``load_settings()`` and injects them into collaborators. Docker injects
connection hostnames through the process environment; ``load_dotenv`` never
overrides variables already set (see ``docs/setup.md``).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from crawler.exceptions import CrawlerConfigError

_CRAWLER_ENV_FILE = Path(__file__).resolve().parent / ".env"

_VALID_ACKS = frozenset({"all", "0", "1", "-1"})


@dataclass(frozen=True, slots=True)
class Settings:
    """Immutable crawler settings — the single runtime source of truth."""

    kafka_bootstrap_servers: str
    app_api_base_url: str
    rate_limit_max_requests: int = 10
    rate_limit_per_seconds: float = 60.0
    retry_max_attempts: int = 3
    retry_base_delay_seconds: float = 2.0
    max_concurrent_workers: int = 5
    reviews_fetch_count: int = 1000
    crawl_interval_hours: int = 1
    registry_request_timeout_seconds: float = 10.0
    kafka_producer_retries: int = 5
    kafka_producer_retry_backoff_ms: int = 500
    kafka_producer_acks: str = "all"
    kafka_producer_enable_idempotence: bool = True
    kafka_producer_request_timeout_ms: int = 5000
    kafka_producer_delivery_timeout_ms: int = 15000
    kafka_send_retry_max_attempts: int = 3
    kafka_send_retry_base_delay_seconds: float = 5.0
    # Off by default: when true, each finished cycle writes a pending JSON
    # under cycle_reports_dir for scripts/reports/finalize_cycle_reports.py.
    cycle_reports_enabled: bool = False
    cycle_reports_dir: str = "/data/reports"

    def __post_init__(self) -> None:
        if not self.kafka_bootstrap_servers.strip():
            raise CrawlerConfigError("kafka_bootstrap_servers must not be empty.")
        if not self.app_api_base_url.strip():
            raise CrawlerConfigError("app_api_base_url must not be empty.")
        if self.rate_limit_max_requests <= 0:
            raise CrawlerConfigError("rate_limit_max_requests must be > 0.")
        if self.rate_limit_per_seconds <= 0:
            raise CrawlerConfigError("rate_limit_per_seconds must be > 0.")
        if self.retry_max_attempts < 0:
            raise CrawlerConfigError("retry_max_attempts must be >= 0.")
        if self.retry_base_delay_seconds < 0:
            raise CrawlerConfigError("retry_base_delay_seconds must be >= 0.")
        if self.max_concurrent_workers <= 0:
            raise CrawlerConfigError("max_concurrent_workers must be > 0.")
        if self.reviews_fetch_count <= 0:
            raise CrawlerConfigError("reviews_fetch_count must be > 0.")
        if self.crawl_interval_hours <= 0:
            raise CrawlerConfigError("crawl_interval_hours must be > 0.")
        if self.registry_request_timeout_seconds <= 0:
            raise CrawlerConfigError("registry_request_timeout_seconds must be > 0.")
        if self.kafka_producer_retries < 0:
            raise CrawlerConfigError("kafka_producer_retries must be >= 0.")
        if self.kafka_producer_retry_backoff_ms <= 0:
            raise CrawlerConfigError("kafka_producer_retry_backoff_ms must be > 0.")
        if str(self.kafka_producer_acks) not in _VALID_ACKS:
            raise CrawlerConfigError(
                "kafka_producer_acks must be one of: all, 0, 1, -1."
            )
        if self.kafka_producer_request_timeout_ms <= 0:
            raise CrawlerConfigError(
                "kafka_producer_request_timeout_ms must be > 0."
            )
        if self.kafka_producer_delivery_timeout_ms <= 0:
            raise CrawlerConfigError(
                "kafka_producer_delivery_timeout_ms must be > 0."
            )
        if (
            self.kafka_producer_delivery_timeout_ms
            <= self.kafka_producer_request_timeout_ms
        ):
            raise CrawlerConfigError(
                "kafka_producer_delivery_timeout_ms must be > "
                "kafka_producer_request_timeout_ms."
            )
        if self.kafka_send_retry_max_attempts < 0:
            raise CrawlerConfigError("kafka_send_retry_max_attempts must be >= 0.")
        if self.kafka_send_retry_base_delay_seconds < 0:
            raise CrawlerConfigError(
                "kafka_send_retry_base_delay_seconds must be >= 0."
            )
        if self.cycle_reports_enabled and not self.cycle_reports_dir.strip():
            raise CrawlerConfigError(
                "cycle_reports_dir must not be empty when cycle reports are enabled."
            )

    @classmethod
    def for_testing(cls, **overrides: object) -> Settings:
        """Build settings for unit tests without touching the real ``.env``."""

        values: dict[str, object] = {
            "kafka_bootstrap_servers": "localhost:9092",
            "app_api_base_url": "http://api.local",
        }
        values.update(overrides)
        return cls(**values)  # type: ignore[arg-type]


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise CrawlerConfigError(f"Required environment variable {name} is not set.")
    return value


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise CrawlerConfigError(f"Environment variable {name} must be an int.") from exc


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise CrawlerConfigError(f"Environment variable {name} must be a float.") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise CrawlerConfigError(
        f"Environment variable {name} must be a boolean (true/false)."
    )


def load_settings(env_file: Path | None = None) -> Settings:
    """Load ``.env`` (if present) and build a validated ``Settings`` instance."""

    load_dotenv(env_file or _CRAWLER_ENV_FILE)
    return Settings(
        kafka_bootstrap_servers=_require_env("KAFKA_BOOTSTRAP_SERVERS"),
        app_api_base_url=_require_env("APP_API_BASE_URL"),
        rate_limit_max_requests=_env_int("CRAWLER_RATE_LIMIT_MAX_REQUESTS", 10),
        rate_limit_per_seconds=_env_float("CRAWLER_RATE_LIMIT_PER_SECONDS", 60.0),
        retry_max_attempts=_env_int("CRAWLER_RETRY_MAX_ATTEMPTS", 3),
        retry_base_delay_seconds=_env_float("CRAWLER_RETRY_BASE_DELAY_SECONDS", 2.0),
        max_concurrent_workers=_env_int("CRAWLER_MAX_WORKERS", 5),
        reviews_fetch_count=_env_int("CRAWLER_REVIEWS_FETCH_COUNT", 1000),
        crawl_interval_hours=_env_int("CRAWLER_INTERVAL_HOURS", 1),
        kafka_producer_retries=_env_int("CRAWLER_KAFKA_PRODUCER_RETRIES", 5),
        kafka_producer_retry_backoff_ms=_env_int(
            "CRAWLER_KAFKA_PRODUCER_RETRY_BACKOFF_MS", 500
        ),
        kafka_producer_acks=os.getenv("CRAWLER_KAFKA_PRODUCER_ACKS", "all") or "all",
        kafka_producer_enable_idempotence=_env_bool(
            "CRAWLER_KAFKA_PRODUCER_ENABLE_IDEMPOTENCE", True
        ),
        kafka_producer_request_timeout_ms=_env_int(
            "CRAWLER_KAFKA_PRODUCER_REQUEST_TIMEOUT_MS", 5000
        ),
        kafka_producer_delivery_timeout_ms=_env_int(
            "CRAWLER_KAFKA_PRODUCER_DELIVERY_TIMEOUT_MS", 15000
        ),
        kafka_send_retry_max_attempts=_env_int(
            "CRAWLER_KAFKA_SEND_RETRY_MAX_ATTEMPTS", 3
        ),
        kafka_send_retry_base_delay_seconds=_env_float(
            "CRAWLER_KAFKA_SEND_RETRY_BASE_DELAY_SECONDS", 5.0
        ),
        cycle_reports_enabled=_env_bool("CRAWLER_CYCLE_REPORTS_ENABLED", False),
        cycle_reports_dir=(
            os.getenv("CRAWLER_CYCLE_REPORTS_DIR", "/data/reports") or "/data/reports"
        ),
    )
