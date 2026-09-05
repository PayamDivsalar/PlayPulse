"""Central configuration for the crawler subsystem.

Environment variables are loaded once from the project-root ``.env`` file
(mirroring the ``app_api`` convention of a single ``load_dotenv()`` call).
Accessors read from the environment at call time and fail fast with a clear
error when a required value is missing -- there are no silent defaults, because
a wrong Kafka or App API address should surface immediately rather than send
data to the wrong place.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

from crawler.exceptions import CrawlerConfigError

# The project root holds the shared .env (POSTGRES_*, KAFKA_*, APP_API_*).
_PROJECT_ROOT_ENV = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(_PROJECT_ROOT_ENV)


def _require_env(name: str) -> str:
    """Return the value of an environment variable or fail fast."""

    value = os.getenv(name)
    if not value:
        raise CrawlerConfigError(f"Required environment variable {name} is not set.")
    return value


def get_kafka_bootstrap_servers() -> str:
    """Return the Kafka bootstrap servers string (comma-separated)."""

    return _require_env("KAFKA_BOOTSTRAP_SERVERS")


def get_app_api_base_url() -> str:
    """Return the base URL of the App API registry service."""

    return _require_env("APP_API_BASE_URL")
