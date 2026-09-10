"""Optional crawl-cycle pending reports for end-to-end finalize.

When enabled, the crawler writes one JSON file per finished cycle under
``<reports_dir>/pending/``. A separate script
(``scripts/reports/finalize_cycle_reports.py``) waits until Kafka consumer lag is
zero, then appends a final line to ``cycles.jsonl``.

Default is off: no files, no behaviour change.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def write_pending_cycle_report(
    reports_dir: Path,
    *,
    started_at: datetime,
    finished_at: datetime,
    apps_total: int,
    stats_ok: int,
    reviews_ok: int,
    failed_stats: list[str],
    failed_reviews: list[str],
) -> Path | None:
    """Atomically write one pending report. Returns the path, or None on skip/error."""

    if apps_total < 0:
        return None

    reports_dir = Path(reports_dir)
    pending_dir = reports_dir / "pending"
    try:
        pending_dir.mkdir(parents=True, exist_ok=True)
    except OSError:
        logger.exception("Cannot create cycle-report pending dir %s", pending_dir)
        return None

    started = started_at.astimezone(timezone.utc)
    finished = finished_at.astimezone(timezone.utc)
    cycle_id = started.strftime("%Y%m%dT%H%M%S%fZ")
    duration_s = max(0.0, (finished - started).total_seconds())

    payload: dict[str, Any] = {
        "cycle_id": cycle_id,
        "started_at": started.isoformat(),
        "finished_at_crawl": finished.isoformat(),
        "duration_crawl_s": round(duration_s, 3),
        "apps_total": apps_total,
        "app_stats": {
            "ok": stats_ok,
            "total": apps_total,
            "failed": list(failed_stats),
        },
        "reviews": {
            "ok": reviews_ok,
            "total": apps_total,
            "failed": list(failed_reviews),
        },
        "status": "pending_persist",
    }

    target = pending_dir / f"{cycle_id}.json"
    tmp = target.with_suffix(".json.tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        tmp.replace(target)
    except OSError:
        logger.exception("Failed to write pending cycle report %s", target)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass
        return None

    logger.info("Wrote pending cycle report %s", target)
    return target
