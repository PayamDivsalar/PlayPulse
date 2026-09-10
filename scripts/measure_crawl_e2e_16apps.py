#!/usr/bin/env python
"""Real end-to-end measurement: 16-app crawl -> Kafka -> Postgres.

Measures wall-clock crawl time, time until DB catch-up, Kafka on-disk size
(topic log dirs), and logical payload bytes. Writes a JSON report.

Usage (from repo root):

    crawler/venv/bin/python scripts/measure_crawl_e2e_16apps.py
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from crawler.app_registry_client import AppRegistryClient  # noqa: E402
from crawler.config import load_settings  # noqa: E402
from crawler.crawler_service import CrawlerService  # noqa: E402
from crawler.kafka_producer import CrawlerKafkaProducer  # noqa: E402
from crawler.playstore_client import PlayStoreClient  # noqa: E402
from crawler.rate_limiter import RateLimiter  # noqa: E402

logger = logging.getLogger("measure_e2e")

APPS: list[tuple[str, str]] = [
    ("com.whatsapp", "WhatsApp"),
    ("com.instagram.android", "Instagram"),
    ("com.spotify.music", "Spotify"),
    ("com.facebook.katana", "Facebook"),
    ("com.twitter.android", "X"),
    ("com.netflix.mediaclient", "Netflix"),
    ("com.google.android.youtube", "YouTube"),
    ("com.snapchat.android", "Snapchat"),
    ("com.zhiliaoapp.musically", "TikTok"),
    ("com.discord", "Discord"),
    ("com.reddit.frontpage", "Reddit"),
    ("com.ubercab", "Uber"),
    ("com.airbnb.android", "Airbnb"),
    ("com.pinterest", "Pinterest"),
    ("com.linkedin.android", "LinkedIn"),
    ("com.amazon.mShop.android.shopping", "Amazon Shopping"),
]

KAFKA_CONTAINER = "project_kafka"
POSTGRES_CONTAINER = "project_postgres"
BOOTSTRAP = "localhost:9092"
TOPICS = ("app-stats", "reviews")
REPORT_PATH = REPOSITORY_ROOT / "data" / "analytics" / "crawl_e2e_16apps_report.json"


@dataclass
class TopicDiskSample:
    topic: str
    bytes_on_disk: int
    paths: dict[str, int] = field(default_factory=dict)


@dataclass
class Report:
    started_at: str
    finished_at: str | None = None
    app_count: int = 16
    packages: list[str] = field(default_factory=list)

    crawl_seconds: float | None = None
    crawl_stats_ok: int | None = None
    crawl_reviews_ok: int | None = None

    kafka_msg_app_stats: int | None = None
    kafka_msg_reviews: int | None = None
    kafka_payload_bytes_app_stats: int | None = None
    kafka_payload_bytes_reviews: int | None = None
    kafka_payload_bytes_total: int | None = None
    kafka_disk_before: dict[str, dict[str, int]] = field(default_factory=dict)
    kafka_disk_after: dict[str, dict[str, int]] = field(default_factory=dict)
    kafka_log_bytes_delta: dict[str, int] = field(default_factory=dict)
    kafka_log_bytes_delta_total: int | None = None
    kafka_total_bytes_after: dict[str, int] = field(default_factory=dict)
    kafka_index_overhead_bytes_after: int | None = None

    db_app_stats_rows: int | None = None
    db_reviews_rows: int | None = None
    db_dead_letters_new: int | None = None

    persist_lag_drain_seconds: float | None = None
    end_to_end_seconds: float | None = None
    consumer_caught_up_at: str | None = None

    notes: list[str] = field(default_factory=list)


def _env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing env var {name}")
    return value


def _run(cmd: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    logger.info("$ %s", " ".join(cmd))
    return subprocess.run(cmd, check=check, text=True, capture_output=True)


def _docker_exec(container: str, *args: str, check: bool = True) -> str:
    result = _run(["docker", "exec", container, *args], check=check)
    return result.stdout


def _psql(sql: str) -> str:
    return _docker_exec(
        POSTGRES_CONTAINER,
        "psql",
        "-U",
        _env("POSTGRES_USER"),
        "-d",
        _env("POSTGRES_DB"),
        "-v",
        "ON_ERROR_STOP=1",
        "-t",
        "-A",
        "-c",
        sql,
    ).strip()


def _api_request(method: str, path: str, body: dict | None = None) -> tuple[int, object]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        f"http://127.0.0.1:8000{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            payload = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            payload = raw
        return exc.code, payload


def wait_for_api(timeout: float = 60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, _ = _api_request("GET", "/api/applications/?is_active=true")
            if status == 200:
                return
        except Exception:
            pass
        time.sleep(0.5)
    raise RuntimeError("App API did not become ready")


def ensure_sixteen_apps() -> list[str]:
    status, existing = _api_request("GET", "/api/applications/")
    if status != 200 or not isinstance(existing, list):
        raise RuntimeError(f"Failed to list apps: {status} {existing}")

    by_package = {
        row["package_name"]: row for row in existing if isinstance(row, dict)
    }
    wanted = {pkg for pkg, _ in APPS}

    for pkg, name in APPS:
        if pkg in by_package:
            row = by_package[pkg]
            if not row.get("is_active"):
                status, _ = _api_request("POST", f"/api/applications/{row['id']}/reactivate/")
                if status not in (200, 201):
                    raise RuntimeError(f"Failed to reactivate {pkg}: {status}")
            continue
        status, created = _api_request(
            "POST",
            "/api/applications/",
            {"package_name": pkg, "display_name": name, "is_active": True},
        )
        if status not in (200, 201):
            raise RuntimeError(f"Failed to create {pkg}: {status} {created}")

    status, existing = _api_request("GET", "/api/applications/")
    if status != 200 or not isinstance(existing, list):
        raise RuntimeError("Failed to re-list apps after ensure")

    for row in existing:
        pkg = row.get("package_name")
        if pkg in wanted:
            continue
        if row.get("is_active"):
            status, _ = _api_request("POST", f"/api/applications/{row['id']}/deactivate/")
            if status not in (200, 201):
                raise RuntimeError(f"Failed to deactivate {pkg}: {status}")

    status, active = _api_request("GET", "/api/applications/?is_active=true")
    if status != 200 or not isinstance(active, list):
        raise RuntimeError("Failed to list active apps")
    packages = sorted(row["package_name"] for row in active)
    if len(packages) != 16:
        raise RuntimeError(f"Expected 16 active apps, got {len(packages)}: {packages}")
    return packages


def topic_disk_breakdown() -> dict[str, dict[str, int]]:
    """Per-topic Kafka on-disk bytes, split into .log vs index overhead.

    Confluent's broker pre-allocates ~10 MiB ``.index`` and ``.timeindex``
    files per partition. Counting those as "data size" dwarfs a 16-app crawl.
    Operators care about both: ``log_bytes`` is the retained message volume;
    ``index_bytes`` is fixed overhead that scales with partition count.
    Directories marked ``*-delete`` (async topic deletion) are ignored.
    """

    script = r"""
python3 - <<'PY'
import os, json
root = "/var/lib/kafka/data"
out = {}
for name in os.listdir(root):
    path = os.path.join(root, name)
    if not os.path.isdir(path):
        continue
    if "-delete" in name:
        continue
    topic, sep, part = name.rpartition("-")
    if not sep or not part.isdigit():
        continue
    bucket = out.setdefault(topic, {"log_bytes": 0, "index_bytes": 0, "other_bytes": 0, "total_bytes": 0})
    for dirpath, _, filenames in os.walk(path):
        for fn in filenames:
            fp = os.path.join(dirpath, fn)
            try:
                size = os.path.getsize(fp)
            except OSError:
                continue
            bucket["total_bytes"] += size
            if fn.endswith(".log"):
                bucket["log_bytes"] += size
            elif fn.endswith(".index") or fn.endswith(".timeindex"):
                bucket["index_bytes"] += size
            else:
                bucket["other_bytes"] += size
print(json.dumps(out))
PY
"""
    raw = _docker_exec(KAFKA_CONTAINER, "bash", "-lc", script)
    parsed = json.loads(raw)
    return {
        topic: {k: int(v) for k, v in values.items()}
        for topic, values in parsed.items()
    }


def topic_disk_bytes() -> dict[str, int]:
    """Backward-compatible total bytes per topic (log + indexes)."""

    return {
        topic: values.get("total_bytes", 0)
        for topic, values in topic_disk_breakdown().items()
    }

def topic_offsets_sum(topic: str) -> int:
    raw = _docker_exec(
        KAFKA_CONTAINER,
        "kafka-run-class",
        "kafka.tools.GetOffsetShell",
        "--broker-list",
        BOOTSTRAP,
        "--topic",
        topic,
        "--time",
        "-1",
    )
    total = 0
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue
        # topic:partition:offset
        parts = line.rsplit(":", 2)
        if len(parts) != 3:
            continue
        total += int(parts[2])
    return total


def consumer_lag(group: str) -> int | None:
    result = _run(
        [
            "docker",
            "exec",
            KAFKA_CONTAINER,
            "kafka-consumer-groups",
            "--bootstrap-server",
            BOOTSTRAP,
            "--describe",
            "--group",
            group,
        ],
        check=False,
    )
    if result.returncode != 0:
        return None
    lag = 0
    saw = False
    for line in result.stdout.splitlines():
        if line.startswith("GROUP") or not line.strip():
            continue
        cols = line.split()
        # GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG ...
        if len(cols) < 6:
            continue
        saw = True
        value = cols[5]
        if value == "-":
            continue
        lag += int(value)
    return lag if saw else None


def wait_for_catchup(
    *,
    expected_stats: int,
    expected_reviews: int,
    timeout: float = 900.0,
) -> tuple[float, int, int, int]:
    """Wait until lag is 0 and DB row counts reach expected.

    Returns (elapsed_seconds, stats_rows, review_rows, new_dead_letters).
    """

    start = time.monotonic()
    deadline = start + timeout
    baseline_dl = int(_psql("SELECT count(*) FROM dead_letter_events"))

    while time.monotonic() < deadline:
        stats_lag = consumer_lag("storage-consumer.app-stats")
        reviews_lag = consumer_lag("storage-consumer.reviews")
        stats_rows = int(_psql("SELECT count(*) FROM app_stats"))
        review_rows = int(_psql("SELECT count(*) FROM reviews"))
        dl_rows = int(_psql("SELECT count(*) FROM dead_letter_events"))
        new_dl = dl_rows - baseline_dl

        logger.info(
            "catchup stats_lag=%s reviews_lag=%s db_stats=%s/%s db_reviews=%s/%s new_dl=%s",
            stats_lag,
            reviews_lag,
            stats_rows,
            expected_stats,
            review_rows,
            expected_reviews,
            new_dl,
        )

        lags_ok = (
            stats_lag is not None
            and reviews_lag is not None
            and stats_lag == 0
            and reviews_lag == 0
        )
        rows_ok = stats_rows >= expected_stats and review_rows >= expected_reviews
        # If everything landed in dead letters, still stop once lag is drained.
        terminal_dl = lags_ok and (stats_rows + review_rows + new_dl) >= (
            expected_stats + expected_reviews
        )
        if lags_ok and (rows_ok or terminal_dl):
            return time.monotonic() - start, stats_rows, review_rows, new_dl
        time.sleep(2.0)

    raise TimeoutError("Timed out waiting for storage consumer catch-up")


def measure_payload_bytes(topic: str) -> tuple[int, int]:
    """Consume all messages once with a throwaway group; return (count, key+value bytes)."""

    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=BOOTSTRAP,
        group_id=f"measure-payload-{topic}-{int(time.time())}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=15000,
    )
    count = 0
    total = 0
    try:
        for msg in consumer:
            count += 1
            if msg.key:
                total += len(msg.key)
            if msg.value:
                total += len(msg.value)
    finally:
        consumer.close()
    return count, total


def reset_kafka_topics(partitions: int = 3) -> None:
    logger.info("Stopping storage-consumer for topic reset")
    _run(["docker", "compose", "stop", "storage-consumer"], check=False)

    for topic in TOPICS:
        _run(
            [
                "docker",
                "exec",
                KAFKA_CONTAINER,
                "kafka-topics",
                "--bootstrap-server",
                BOOTSTRAP,
                "--delete",
                "--topic",
                topic,
            ],
            check=False,
        )

    # Wait until deleted
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        listed = _docker_exec(
            KAFKA_CONTAINER,
            "kafka-topics",
            "--bootstrap-server",
            BOOTSTRAP,
            "--list",
        )
        remaining = [t for t in TOPICS if t in listed.splitlines()]
        if not remaining:
            break
        time.sleep(1)
    else:
        raise RuntimeError(f"Topics still present after delete: {remaining}")

    time.sleep(2)
    for topic in TOPICS:
        _docker_exec(
            KAFKA_CONTAINER,
            "kafka-topics",
            "--bootstrap-server",
            BOOTSTRAP,
            "--create",
            "--topic",
            topic,
            "--partitions",
            str(partitions),
            "--replication-factor",
            "1",
        )

    # Keep network-metrics; recreate only if missing
    listed = _docker_exec(
        KAFKA_CONTAINER,
        "kafka-topics",
        "--bootstrap-server",
        BOOTSTRAP,
        "--list",
    )
    if "network-metrics" not in listed.splitlines():
        _docker_exec(
            KAFKA_CONTAINER,
            "kafka-topics",
            "--bootstrap-server",
            BOOTSTRAP,
            "--create",
            "--topic",
            "network-metrics",
            "--partitions",
            str(partitions),
            "--replication-factor",
            "1",
        )


def reset_db_tables() -> None:
    _psql(
        "TRUNCATE TABLE app_stats, reviews, dead_letter_events "
        "RESTART IDENTITY CASCADE;"
    )


def run_one_crawl_cycle() -> tuple[float, int, int]:
    settings = load_settings()
    rate_limiter = RateLimiter(
        max_requests=settings.rate_limit_max_requests,
        per_seconds=settings.rate_limit_per_seconds,
    )
    playstore_client = PlayStoreClient(rate_limiter=rate_limiter, settings=settings)
    kafka_producer = CrawlerKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        settings=settings,
    )
    app_registry_client = AppRegistryClient(
        base_url=settings.app_api_base_url,
        settings=settings,
    )
    service = CrawlerService(
        playstore_client=playstore_client,
        kafka_producer=kafka_producer,
        app_registry_client=app_registry_client,
        max_workers=settings.max_concurrent_workers,
        reviews_fetch_count=settings.reviews_fetch_count,
    )

    # Monkey-patch summary counters via wrapping run path: use private tracking
    # by re-running through the public API and reading Kafka offsets afterward.
    t0 = time.monotonic()
    service.run_crawl_cycle()
    elapsed = time.monotonic() - t0
    try:
        kafka_producer.close()
    except Exception:
        logger.exception("producer close failed")

    stats_ok = topic_offsets_sum("app-stats")
    reviews_ok = topic_offsets_sum("reviews")
    return elapsed, stats_ok, reviews_ok


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Load compose/.env for psql user/db
    env_path = REPOSITORY_ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

    report = Report(started_at=datetime.now(timezone.utc).isoformat())
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Waiting for App API")
    wait_for_api()
    packages = ensure_sixteen_apps()
    report.packages = packages
    logger.info("Active packages (%s): %s", len(packages), packages)

    logger.info("Resetting Kafka topics and DB tables for clean measurement")
    reset_kafka_topics(partitions=3)
    reset_db_tables()

    logger.info("Starting storage-consumer")
    _run(["docker", "compose", "up", "-d", "storage-consumer"])

    # Wait until healthy / groups exist
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        health = _run(
            ["docker", "inspect", "-f", "{{.State.Health.Status}}", "project_storage_consumer"],
            check=False,
        ).stdout.strip()
        if health == "healthy":
            break
        time.sleep(2)
    else:
        report.notes.append("storage-consumer never became healthy; continuing anyway")

    # Empty-topic baseline. Index files are preallocated (~10 MiB each); the
    # meaningful crawl cost is the growth of ``.log`` files.
    before = topic_disk_breakdown()
    report.kafka_disk_before = {t: before.get(t, {}) for t in TOPICS}
    logger.info("Kafka disk before: %s", report.kafka_disk_before)

    logger.info("Starting timed crawl cycle for 16 apps")
    crawl_t0 = time.monotonic()
    crawl_seconds, stats_msgs, review_msgs = run_one_crawl_cycle()
    crawl_wall = time.monotonic() - crawl_t0
    report.crawl_seconds = crawl_seconds
    report.notes.append(f"crawl_wall_including_producer_close={crawl_wall:.3f}")
    report.crawl_stats_ok = stats_msgs
    report.crawl_reviews_ok = review_msgs
    report.kafka_msg_app_stats = stats_msgs
    report.kafka_msg_reviews = review_msgs
    logger.info(
        "Crawl finished in %.2fs; kafka offsets app-stats=%s reviews=%s",
        crawl_seconds,
        stats_msgs,
        review_msgs,
    )

    # Force broker to flush segment data to disk before sizing
    time.sleep(3)
    after = topic_disk_breakdown()
    report.kafka_disk_after = {t: after.get(t, {}) for t in TOPICS}
    report.kafka_log_bytes_delta = {
        t: (
            report.kafka_disk_after.get(t, {}).get("log_bytes", 0)
            - report.kafka_disk_before.get(t, {}).get("log_bytes", 0)
        )
        for t in TOPICS
    }
    report.kafka_log_bytes_delta_total = sum(report.kafka_log_bytes_delta.values())
    report.kafka_total_bytes_after = {
        t: report.kafka_disk_after.get(t, {}).get("total_bytes", 0) for t in TOPICS
    }
    report.kafka_index_overhead_bytes_after = sum(
        report.kafka_disk_after.get(t, {}).get("index_bytes", 0) for t in TOPICS
    )

    logger.info("Measuring logical payload bytes via throwaway consumers")
    stats_count, stats_bytes = measure_payload_bytes("app-stats")
    reviews_count, reviews_bytes = measure_payload_bytes("reviews")
    report.kafka_payload_bytes_app_stats = stats_bytes
    report.kafka_payload_bytes_reviews = reviews_bytes
    report.kafka_payload_bytes_total = stats_bytes + reviews_bytes
    report.notes.append(
        f"payload recount app-stats={stats_count} reviews={reviews_count}"
    )

    logger.info("Waiting for storage consumer to persist all messages")
    drain_s, db_stats, db_reviews, new_dl = wait_for_catchup(
        expected_stats=stats_msgs,
        expected_reviews=review_msgs,
        timeout=900.0,
    )
    # e2e = crawl publish wall + remaining time until DB/lag catch-up.
    # The consumer runs during the crawl, so drain_s is only the tail after
    # the producer finished — not the full persist cost in isolation.
    report.persist_lag_drain_seconds = drain_s
    report.end_to_end_seconds = crawl_seconds + drain_s
    report.db_app_stats_rows = db_stats
    report.db_reviews_rows = db_reviews
    report.db_dead_letters_new = new_dl
    report.consumer_caught_up_at = datetime.now(timezone.utc).isoformat()
    report.finished_at = report.consumer_caught_up_at

    final_disk = topic_disk_breakdown()
    report.notes.append(
        "kafka_disk_after_consume="
        + json.dumps({t: final_disk.get(t, {}) for t in TOPICS})
    )
    report.notes.append(
        "rate_limiter_fix=preserved fractional tokens under worker contention"
    )
    REPORT_PATH.write_text(json.dumps(asdict(report), indent=2) + "\n")
    logger.info("Wrote report to %s", REPORT_PATH)

    print("\n========== E2E MEASUREMENT REPORT ==========")
    print(json.dumps(asdict(report), indent=2))
    print("============================================\n")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        logging.exception("measurement failed")
        raise
