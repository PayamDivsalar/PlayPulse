"""Map Play Store scrape payloads to the stable Kafka / DB field contract."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _unix_timestamp_to_iso(value: Any) -> str | None:
    """Convert a Unix timestamp to an aware UTC ISO-8601 string, or None."""

    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError, OverflowError):
        return None


def map_app_details(raw: dict[str, Any], package_name: str) -> dict[str, Any]:
    """Map filtered Play Store app details to the ``app_stats`` contract.

    Pure aside from generating ``crawled_at`` at call time. Missing or invalid
    fields become ``None`` rather than raising.
    """

    return {
        "package_name": package_name,
        "min_installs": raw.get("minInstalls"),
        "score": raw.get("score"),
        "ratings": raw.get("ratings"),
        "reviews_count": raw.get("reviews"),
        "version": raw.get("version"),
        "ad_supported": raw.get("adSupported"),
        "app_updated_at": _unix_timestamp_to_iso(raw.get("updated")),
        "crawled_at": _utc_now_iso(),
    }


def map_review(raw: dict[str, Any], package_name: str) -> dict[str, Any]:
    """Map a filtered Play Store review to the ``reviews`` Kafka contract.

    ``at`` is expected to already be an ISO-8601 string from PlayStoreClient.
    Missing fields become ``None`` rather than raising.
    """

    at_value = raw.get("at")
    if isinstance(at_value, datetime):
        at_value = at_value.isoformat()

    return {
        "package_name": package_name,
        "review_id": raw.get("reviewId"),
        "user_name": raw.get("userName"),
        "thumbs_up_count": raw.get("thumbsUpCount"),
        "score": raw.get("score"),
        "content": raw.get("content"),
        "at": at_value,
        "crawled_at": _utc_now_iso(),
    }
