#!/usr/bin/env python
"""Finalize pending crawl-cycle reports after consumer lag reaches zero.

Crawler (when enabled) writes pending JSON after each cycle. This script
finalizes those files once lag is 0. Mid-cycle lag=0 cannot finalize early
because a pending file exists only after the crawl finishes.

Used by the Compose service ``cycle-reporter`` (mounts this file), or on the host.

    docker compose up -d cycle-reporter
    ./scripts/reports/finalize_cycle_reports.py --loop --interval 60
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

GROUP_TOPICS = (
    ("storage-consumer.app-stats", "app-stats"),
    ("storage-consumer.reviews", "reviews"),
)


def _load_root_env() -> None:
    env_file = REPO_ROOT / ".env"
    if not env_file.is_file():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def _bootstrap() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def consumer_lag(group: str, topic: str) -> int | None:
    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError:
        print("[FAIL] kafka-python is required", file=sys.stderr)
        return None

    consumer = KafkaConsumer(
        bootstrap_servers=_bootstrap().split(","),
        group_id=group,
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
    )
    try:
        parts = consumer.partitions_for_topic(topic)
        if not parts:
            return 0
        tps = [TopicPartition(topic, p) for p in sorted(parts)]
        ends = consumer.end_offsets(tps)
        lag = 0
        for tp in tps:
            committed = consumer.committed(tp)
            end = ends.get(tp, 0)
            lag += end if committed is None else max(0, end - committed)
        return lag
    except Exception as exc:
        print(f"[WARN] lag check failed group={group}: {exc}", file=sys.stderr)
        return None
    finally:
        consumer.close()


def wait_for_lag_zero(
    timeout_s: float, poll_s: float = 2.0
) -> tuple[bool, dict[str, int | None]]:
    deadline = time.monotonic() + timeout_s
    last: dict[str, int | None] = {g: None for g, _ in GROUP_TOPICS}
    while time.monotonic() < deadline:
        last = {g: consumer_lag(g, topic) for g, topic in GROUP_TOPICS}
        if all(v == 0 for v in last.values()):
            return True, last
        time.sleep(poll_s)
    return False, last


def dead_letters_since(iso_started: str) -> int | None:
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    db = os.getenv("POSTGRES_DB")
    host = os.getenv("POSTGRES_HOST", "127.0.0.1")
    port = os.getenv("POSTGRES_PORT", "5432")
    if not user or not db or not password:
        return None
    try:
        import psycopg2
    except ImportError:
        return None
    try:
        with psycopg2.connect(
            host=host,
            port=port,
            dbname=db,
            user=user,
            password=password,
            connect_timeout=5,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM dead_letter_events "
                    "WHERE occurred_at >= %s::timestamptz",
                    (iso_started,),
                )
                row = cur.fetchone()
                return int(row[0]) if row else 0
    except Exception as exc:
        print(f"[WARN] dead-letter query failed: {exc}", file=sys.stderr)
        return None


def verdict_for(pending: dict, lag_ok: bool, dead_letters: int | None) -> str:
    stats = pending["app_stats"]
    reviews = pending["reviews"]
    crawl_perfect = stats["ok"] == stats["total"] and reviews["ok"] == reviews["total"]
    if not lag_ok:
        return "persist_lagging"
    if dead_letters is not None and dead_letters > 0:
        return "partial"
    if crawl_perfect:
        return "ok"
    if stats["ok"] == 0 and reviews["ok"] == 0:
        return "failed"
    return "partial"


def finalize_one(path: Path, reports_dir: Path, lag_timeout: float) -> bool:
    try:
        pending = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"[FAIL] cannot read {path.name}: {exc}", file=sys.stderr)
        return False

    cycle_id = pending.get("cycle_id", path.stem)
    print(f"[..] finalizing {cycle_id} (waiting for lag=0, timeout={int(lag_timeout)}s)")
    lag_ok, lags = wait_for_lag_zero(lag_timeout)
    dead = dead_letters_since(pending.get("started_at", ""))
    finished = datetime.now(timezone.utc)
    started = datetime.fromisoformat(pending["started_at"])
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)

    report = {
        **pending,
        "finished_at_report": finished.isoformat(),
        "duration_e2e_s": round((finished - started).total_seconds(), 3),
        "persist": {
            "lag_ok": lag_ok,
            "lag_app_stats": lags.get("storage-consumer.app-stats"),
            "lag_reviews": lags.get("storage-consumer.reviews"),
            "dead_letters_since_cycle_start": dead,
        },
        "verdict": verdict_for(pending, lag_ok, dead),
        "status": "final",
    }

    cycles_path = reports_dir / "cycles.jsonl"
    done_dir = reports_dir / "done"
    done_dir.mkdir(parents=True, exist_ok=True)
    with cycles_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(report, ensure_ascii=False) + "\n")
    path.replace(done_dir / path.name)
    print(
        f"[OK] {cycle_id} verdict={report['verdict']} "
        f"stats={pending['app_stats']['ok']}/{pending['app_stats']['total']} "
        f"reviews={pending['reviews']['ok']}/{pending['reviews']['total']} "
        f"lag_ok={lag_ok}"
    )
    return True


def run_once(reports_dir: Path, lag_timeout: float) -> int:
    pending_dir = reports_dir / "pending"
    pending_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(pending_dir.glob("*.json"))
    if not files:
        print("[..] no pending cycle reports")
        return 0
    failed = 0
    for path in files:
        if not finalize_one(path, reports_dir, lag_timeout):
            failed += 1
    return 1 if failed else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reports-dir", type=Path, default=None)
    parser.add_argument("--lag-timeout", type=float, default=None)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval", type=float, default=None)
    args = parser.parse_args()
    _load_root_env()

    if args.reports_dir:
        reports_dir = args.reports_dir
    else:
        env_dir = os.getenv("CYCLE_REPORTS_DIR") or os.getenv("CRAWLER_CYCLE_REPORTS_DIR")
        if env_dir:
            reports_dir = Path(env_dir)
        elif os.getenv("POSTGRES_HOST") == "postgres":
            reports_dir = Path("/data/reports")
        else:
            reports_dir = REPO_ROOT / "data" / "reports"
    if not reports_dir.is_absolute():
        reports_dir = REPO_ROOT / reports_dir

    lag_timeout = float(
        args.lag_timeout
        if args.lag_timeout is not None
        else os.getenv("CYCLE_REPORTS_LAG_TIMEOUT", "600")
    )
    interval = float(
        args.interval
        if args.interval is not None
        else os.getenv("CYCLE_REPORTS_INTERVAL_SECONDS", "60")
    )

    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "pending").mkdir(exist_ok=True)
    (reports_dir / "done").mkdir(exist_ok=True)

    if args.loop:
        print(f"[..] loop every {interval}s dir={reports_dir} kafka={_bootstrap()}")
        while True:
            run_once(reports_dir, lag_timeout)
            time.sleep(interval)
    return run_once(reports_dir, lag_timeout)


if __name__ == "__main__":
    raise SystemExit(main())
