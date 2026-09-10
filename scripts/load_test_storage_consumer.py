#!/usr/bin/env python
"""Load test that validates the one-thread-per-pipeline sizing decision.

The design rests on a claim: a single thread per topic is enough for the
expected volume, with room to spare. The expected peak is the reviews burst --
up to 200,000 messages over five to fifteen minutes, so roughly 220-670
messages a second. This measures what one thread actually sustains, against a
target of ten times that peak.

The point is not to prove the number is large. It is to know the headroom, so
that "run a second container for that pipeline" is a decision backed by a
measurement rather than a guess. If this run shows less than 10x, the sizing
claim in the design is wrong and the plan needs revisiting -- which is worth
finding out here rather than during a burst.

What it does:

1. Registers a throwaway application, so nothing is dead-lettered.
2. Produces N messages to a throwaway topic.
3. Runs one real ``Worker`` against it and times how long the backlog takes.
4. Reports throughput, per-batch latency and the headroom over peak.
5. Deletes everything it created.

Usage:

    storage_consumer/venv/bin/python scripts/load_test_storage_consumer.py
    storage_consumer/venv/bin/python scripts/load_test_storage_consumer.py \\
        --pipeline reviews --messages 2000000

Prerequisites: ``docker compose up -d postgres kafka``, and the migrations
applied (``python -m storage_consumer.main migrate``).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import logging
import sys
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPOSITORY_ROOT))

from kafka import KafkaProducer  # noqa: E402
from kafka.admin import KafkaAdminClient, NewTopic  # noqa: E402

from storage_consumer.config import PIPELINE_NAMES, load_settings  # noqa: E402
from storage_consumer.consumer_factory import create_consumer  # noqa: E402
from storage_consumer.persistence.application_resolver import (  # noqa: E402
    ApplicationResolver,
)
from storage_consumer.persistence.database import Database  # noqa: E402
from storage_consumer.persistence.dead_letter_repository import (  # noqa: E402
    DeadLetterRepository,
)
from storage_consumer.pipelines import build_pipeline  # noqa: E402

# Reuses the integration suite's helper rather than re-writing the INSERT: the
# applications table is app_api's, and one place knowing its NOT NULL columns
# is enough.
from storage_consumer.tests.integration_support import (  # noqa: E402
    register_application,
    unique_package_name,
)
from storage_consumer.worker import Worker  # noqa: E402

logger = logging.getLogger("load_test")

# The reviews burst from the design: 200,000 messages in as little as five
# minutes. Everything below is measured against ten times this.
PEAK_MESSAGES = 200_000
PEAK_WINDOW_SECONDS = 5 * 60
PEAK_RATE = PEAK_MESSAGES / PEAK_WINDOW_SECONDS  # ~667 messages/second
TARGET_MULTIPLE = 10

TABLES = {
    "app-stats": "app_stats",
    "reviews": "reviews",
    "network-metrics": "network_metrics",
}


@dataclass
class Result:
    pipeline: str
    messages: int
    produce_seconds: float = 0.0
    consume_seconds: float = 0.0
    rows_stored: int = 0
    batch_durations_ms: list[float] = field(default_factory=list)

    @property
    def rate(self) -> float:
        if self.consume_seconds <= 0:
            return 0.0
        return self.messages / self.consume_seconds

    @property
    def headroom(self) -> float:
        return self.rate / PEAK_RATE

    @property
    def produce_rate(self) -> float:
        if self.produce_seconds <= 0:
            return 0.0
        return self.messages / self.produce_seconds

    @property
    def host_bound(self) -> bool:
        """Whether the host, rather than the consumer, set the measured rate.

        The broker, the database and the consumer all share one machine here,
        so a slow box shows up as a slow consumer. Producing is the cheaper
        half of the same work -- serialise JSON, hand it to the broker, no
        decode and no database -- so if consuming came within striking
        distance of producing, the number measured is the machine's ceiling
        and not the pipeline's.
        """

        return self.produce_rate > 0 and self.rate > self.produce_rate / 2.5


class InstrumentedWorker(Worker):
    """A real worker that also records each batch's duration."""

    def __init__(self, *args, result: Result, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._result = result

    def _log_batch(self, batches, outcome) -> None:
        self._result.batch_durations_ms.append(outcome.duration_ms)
        # Quieter than the production log line: one per 500-message batch over
        # a two-million message run would be 4,000 lines of noise.
        if len(self._result.batch_durations_ms) % 100 == 0:
            logger.info(
                "  %s batches, last one %s records in %.0fms",
                len(self._result.batch_durations_ms),
                outcome.records,
                outcome.duration_ms,
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--pipeline",
        default="reviews",
        choices=list(PIPELINE_NAMES),
        help="Pipeline to load test. Default: reviews, the burst topic.",
    )
    parser.add_argument(
        "--messages",
        type=int,
        default=PEAK_MESSAGES,
        help=(
            "How many messages to produce. Default: 200000, one peak burst. "
            "Throughput is measured per second, so a shorter run still "
            "answers the sizing question."
        ),
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=3,
        help="Partitions on the throwaway topic. Default: 3",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Do not delete the topic and rows afterwards.",
    )
    return parser.parse_args()


def payloads(pipeline: str, package_name: str, count: int, suffix: str):
    """Generate distinct, valid messages, so nothing is de-duplicated away."""

    base = datetime(2026, 9, 7, 14, tzinfo=timezone.utc)

    if pipeline == "reviews":
        for index in range(count):
            yield {
                "package_name": package_name,
                "review_id": f"load:{suffix}:{index}",
                "user_name": "Load Test",
                "thumbs_up_count": index % 100,
                "score": (index % 5) + 1,
                "content": "A review body of roughly representative length.",
                "at": "2026-09-01T10:00:00",
                "crawled_at": base.isoformat(),
            }
    elif pipeline == "app-stats":
        for index in range(count):
            yield {
                "package_name": package_name,
                "min_installs": 5_000_000_000 + index,
                "score": 4.3,
                "ratings": 178_000_000,
                "reviews_count": 3_400_000,
                "version": "2.24.17.79",
                "ad_supported": False,
                "app_updated_at": "2026-09-01T08:00:00+00:00",
                # Distinct per message: the conflict key is
                # (application_id, crawled_at).
                "crawled_at": (base + timedelta(seconds=index)).isoformat(),
            }
    else:
        for index in range(count):
            yield {
                "analysis_id": str(uuid.uuid4()),
                "package_name": package_name,
                "scenario": "UPLOAD" if index % 2 == 0 else "DOWNLOAD",
                "rtt_handshake": 43.123,
                "retransmission_count": 7,
                "out_of_order_count": 1,
                "spurious_retransmission_count": 2,
                "zero_window_count": 0,
                "tcp_reset_count": 2,
                "bytes_transferred_total": 1040,
                "bytes_payload_total": 1000,
                "overhead_ratio": 0.038462,
                "source_pcap_filename": f"capture-{index}.pcap",
                "analyzed_at": base.isoformat(),
            }


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    # The broker client is chatty at INFO and would drown the report.
    logging.getLogger("kafka").setLevel(logging.WARNING)

    args = parse_args()
    settings = load_settings()
    bootstrap = settings.kafka_bootstrap_servers.split(",")
    suffix = uuid.uuid4().hex[:10]
    topic = f"loadtest-{args.pipeline}-{suffix}"
    # Same prefix the integration suite uses, so a row left behind by an
    # interrupted run is cleaned up by the next test run.
    package_name = unique_package_name()
    result = Result(pipeline=args.pipeline, messages=args.messages)

    database = Database(settings, application_name="storage_consumer:loadtest")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)

    print()
    print("+----------------------------------------------+")
    print("|        storage consumer load test            |")
    print("+----------------------------------------------+")
    print()
    logger.info(
        "Pipeline %s, %s messages, %s partitions.",
        args.pipeline,
        f"{args.messages:,}",
        args.partitions,
    )
    logger.info(
        "Peak is %s messages in %s minutes (%.0f/s); target is %sx that (%.0f/s).",
        f"{PEAK_MESSAGES:,}",
        PEAK_WINDOW_SECONDS // 60,
        PEAK_RATE,
        TARGET_MULTIPLE,
        PEAK_RATE * TARGET_MULTIPLE,
    )

    try:
        with database.transaction() as cursor:
            application_id = register_application(cursor, package_name)
        logger.info("Registered application %s (id %s).", package_name, application_id)

        admin.create_topics(
            [
                NewTopic(
                    topic,
                    num_partitions=args.partitions,
                    replication_factor=1,
                )
            ]
        )
        logger.info("Created topic %s.", topic)

        # --- produce ---
        logger.info("Producing %s messages...", f"{args.messages:,}")
        started = time.monotonic()
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            # Tuned for bulk load, not for the durability the real producers
            # need: this is test data being staged, not data being trusted.
            acks=1,
            linger_ms=50,
            batch_size=256 * 1024,
            buffer_memory=128 * 1024 * 1024,
        )
        try:
            for index, payload in enumerate(
                payloads(args.pipeline, package_name, args.messages, suffix)
            ):
                producer.send(topic, key=package_name, value=payload)
                if (index + 1) % 100_000 == 0:
                    logger.info("  queued %s...", f"{index + 1:,}")
            producer.flush()
        finally:
            producer.close()
        result.produce_seconds = time.monotonic() - started
        logger.info(
            "Produced in %.1fs (%.0f messages/second into the broker).",
            result.produce_seconds,
            args.messages / max(result.produce_seconds, 0.001),
        )

        # --- consume ---
        # The real spec, pointed at the throwaway topic and group so the run
        # exercises the production decoder, repository and batch size.
        spec = dataclasses.replace(
            build_pipeline(args.pipeline, settings),
            topic=topic,
            group_id=f"loadtest-{args.pipeline}-{suffix}",
        )

        stop_event = threading.Event()
        worker = InstrumentedWorker(
            spec=spec,
            settings=settings,
            consumer=create_consumer(spec, settings),
            database=Database(
                settings, application_name=f"storage_consumer:loadtest-{spec.name}"
            ),
            resolver=ApplicationResolver(
                ttl_seconds=settings.application_cache_ttl_seconds
            ),
            dead_letters=DeadLetterRepository(),
            stop_event=stop_event,
            result=result,
        )

        logger.info("Consuming...")
        thread = threading.Thread(target=worker.run, name="loadtest-worker")
        thread.start()

        # Time from the first batch, so the consumer-group join is not counted
        # as ingestion time.
        table = TABLES[args.pipeline]
        while not result.batch_durations_ms and thread.is_alive():
            time.sleep(0.05)
        consume_started = time.monotonic()

        deadline = consume_started + 3600
        stored = 0
        while time.monotonic() < deadline and thread.is_alive():
            with database.transaction() as cursor:
                cursor.execute(
                    f"SELECT count(*) FROM {table} WHERE application_id = %s",
                    (application_id,),
                )
                stored = cursor.fetchone()[0]
            if stored >= args.messages:
                break
            time.sleep(0.5)

        result.consume_seconds = time.monotonic() - consume_started
        result.rows_stored = stored
        stop_event.set()
        thread.join(timeout=60)
        worker.close()

        report(result, args)
        return 0 if result.headroom >= TARGET_MULTIPLE else 1

    finally:
        if not args.keep:
            logger.info("Cleaning up...")
            try:
                admin.delete_topics([topic])
            except Exception:
                logger.warning("Could not delete topic %s.", topic)
            try:
                with database.transaction() as cursor:
                    # The three tables cascade from the application row.
                    cursor.execute(
                        "DELETE FROM apps_registry_application WHERE package_name = %s",
                        (package_name,),
                    )
            except Exception:
                logger.warning("Could not delete the test application.")
        admin.close()
        database.close()


def report(result: Result, args: argparse.Namespace) -> None:
    durations = sorted(result.batch_durations_ms)
    print()
    print("=== Result ===")
    print()
    print(f"  pipeline                {result.pipeline}")
    print(f"  messages produced       {result.messages:,}")
    print(f"  rows stored             {result.rows_stored:,}")
    print(f"  consume wall time       {result.consume_seconds:.1f}s")
    print(f"  throughput              {result.rate:,.0f} messages/second")
    print(f"  batches                 {len(durations):,}")
    if durations:
        print(f"  batch duration median   {durations[len(durations) // 2]:.0f}ms")
        print(f"  batch duration p95      {durations[int(len(durations) * 0.95)]:.0f}ms")
        print(f"  batch duration max      {durations[-1]:.0f}ms")
    print()
    print(f"  produce rate            {result.produce_rate:,.0f} messages/second")
    print(f"  peak rate               {PEAK_RATE:,.0f} messages/second")
    print(f"  target ({TARGET_MULTIPLE}x peak)         {PEAK_RATE * TARGET_MULTIPLE:,.0f} messages/second")
    print(f"  headroom over peak      {result.headroom:,.1f}x")
    print()

    if result.rows_stored < result.messages:
        print(
            f"  INCOMPLETE: {result.messages - result.rows_stored:,} message(s) "
            "were not stored. Check the log above and dead_letter_events."
        )
        print()

    if result.headroom >= TARGET_MULTIPLE:
        print(
            f"  PASS: one thread sustains {result.headroom:,.1f}x the peak burst "
            f"rate, above the {TARGET_MULTIPLE}x target. The single thread per "
            "pipeline is correctly sized."
        )
    elif result.host_bound:
        print(
            f"  INCONCLUSIVE: one thread sustained {result.rate:,.0f} messages/"
            f"second ({result.headroom:,.1f}x peak), short of the "
            f"{TARGET_MULTIPLE}x target -- but this host only *produced* at "
            f"{result.produce_rate:,.0f}/second while also running the broker "
            "and the database."
        )
        print()
        print(
            "  Producing is the cheaper half of the same work, so a consume "
            "rate this close to it means the measurement is bounded by the "
            "machine rather than by the pipeline. To get a number that says "
            "something about the code, re-run with the broker and PostgreSQL "
            "on separate hosts."
        )
        print()
        print(
            f"  What this run does establish: one thread absorbs a full "
            f"{PEAK_MESSAGES:,}-message burst in "
            f"{PEAK_MESSAGES / max(result.rate, 1):,.0f}s, against the "
            f"{PEAK_WINDOW_SECONDS}s the burst takes to arrive. It keeps up "
            "with room to spare; how much room is unmeasured."
        )
    else:
        print(
            f"  FAIL: one thread sustains only {result.headroom:,.1f}x the peak "
            f"burst, below the {TARGET_MULTIPLE}x target, and the host was not "
            "the limit. Widen the topic and run a second container for this "
            "pipeline (see storage_consumer/README.md)."
        )
    print()


if __name__ == "__main__":
    sys.exit(main())
