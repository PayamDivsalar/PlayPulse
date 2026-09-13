"""Command-line entry point and composition root for the storage consumer.

Two subcommands::

    python -m storage_consumer.main migrate
    python -m storage_consumer.main run [--pipeline all] [--skip-migrate]

``migrate`` is safe to run repeatedly and from several containers at once (see
``persistence/migrator.py``), which is why the container entrypoint simply runs
it before ``run`` on every start.

``--pipeline`` is what keeps the concurrency decision reversible. The same
image deploys either as one container running three threads (the compose
default) or as three single-pipeline containers with independent resource
limits and restart policies, with no code change either way.
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import signal
import sys
import threading

from storage_consumer.config import PIPELINE_NAMES, Settings, load_settings
from storage_consumer.messaging.consumer_factory import create_consumer
from storage_consumer.exceptions import ConfigError, MigrationError
from storage_consumer.runtime.heartbeat import Heartbeat
from storage_consumer.persistence.application_resolver import ApplicationResolver
from storage_consumer.persistence.database import Database
from storage_consumer.persistence.dead_letter_repository import DeadLetterRepository
from storage_consumer.persistence.migrator import Migrator
from storage_consumer.messaging.pipelines import PipelineSpec, build_pipelines
from storage_consumer.runtime.supervisor import Supervisor
from storage_consumer.runtime.worker import Worker

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_ERROR = 1
# 2 is reserved: argparse uses it for usage errors.


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m storage_consumer.main",
        description=(
            "Consume the app-stats, reviews and network-metrics topics and "
            "persist them to PostgreSQL."
        ),
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser(
        "migrate",
        help="Apply pending SQL migrations for the tables this subsystem owns.",
        description=(
            "Create or update app_stats, reviews, network_metrics and "
            "dead_letter_events. Idempotent, and safe to run concurrently."
        ),
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Consume the topics and write to PostgreSQL until stopped.",
        description=(
            "Runs one thread per selected pipeline, each with its own consumer "
            "group, database connection and offsets."
        ),
    )
    run_parser.add_argument(
        "--pipeline",
        action="append",
        choices=[*PIPELINE_NAMES, "all"],
        metavar="{" + ",".join([*PIPELINE_NAMES, "all"]) + "}",
        help=(
            "Pipeline to run; repeatable. Defaults to all, or to "
            "STORAGE_PIPELINES when that is set."
        ),
    )
    run_parser.add_argument(
        "--skip-migrate",
        action="store_true",
        help=(
            "Do not apply migrations at startup. The schema is still checked, "
            "and a missing table is fatal."
        ),
    )
    return parser


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        # The thread name is what tells three interleaved pipelines apart.
        format="%(asctime)s %(levelname)s [%(name)s] (%(threadName)s) %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def _resolve_pipelines(settings: Settings, selected: list[str] | None) -> Settings:
    """Apply ``--pipeline`` on top of the environment, if it was given."""

    if not selected:
        return settings
    if "all" in selected:
        chosen = PIPELINE_NAMES
    else:
        # dict.fromkeys rather than set(), so --pipeline reviews --pipeline
        # app-stats starts them in the order the operator asked for.
        chosen = tuple(dict.fromkeys(selected))
    return dataclasses.replace(settings, pipelines=chosen)


def _migrate(settings: Settings) -> int:
    database = Database(settings, application_name="storage_consumer:migrate")
    try:
        Migrator(database).apply_pending()
    finally:
        database.close()
    return EXIT_OK


def _check_schema(settings: Settings) -> None:
    """Fail fast when ``--skip-migrate`` was used against an unmigrated database.

    Without this the first batch would die on "relation does not exist" from
    somewhere deep in a repository, which is a much worse way to learn that
    nobody ran the migrations.
    """

    database = Database(settings, application_name="storage_consumer:preflight")
    try:
        missing = Migrator(database).missing_tables()
    finally:
        database.close()
    if missing:
        raise MigrationError(
            f"Missing table(s): {', '.join(missing)}. Run "
            "`python -m storage_consumer.main migrate` or drop --skip-migrate."
        )


def _build_worker(
    spec: PipelineSpec, stop_event: threading.Event, settings: Settings
) -> Worker:
    """Assemble one pipeline's collaborators.

    Everything built here belongs to a single thread: its own consumer, its own
    connection, its own resolver cache. Nothing is shared, so nothing needs a
    lock.
    """

    return Worker(
        spec=spec,
        settings=settings,
        consumer=create_consumer(spec, settings),
        database=Database(
            settings, application_name=f"storage_consumer:{spec.name}"
        ),
        resolver=ApplicationResolver(
            ttl_seconds=settings.application_cache_ttl_seconds
        ),
        dead_letters=DeadLetterRepository(),
        stop_event=stop_event,
        heartbeat=Heartbeat(
            settings.heartbeat_directory,
            spec.name,
            interval_seconds=settings.heartbeat_interval_seconds,
        ),
    )


def _run(settings: Settings, *, skip_migrate: bool) -> int:
    if skip_migrate:
        _check_schema(settings)
    else:
        _migrate(settings)

    supervisor = Supervisor(
        build_pipelines(settings),
        settings,
        lambda spec, stop_event: _build_worker(spec, stop_event, settings),
    )

    def _shutdown(signum: int, _frame: object) -> None:
        supervisor.request_stop(f"signal {signal.Signals(signum).name}")

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    return supervisor.run()


def main(argv: list[str] | None = None) -> int:
    """Dispatch a subcommand. Returns the process exit code."""

    args = _build_parser().parse_args(argv)
    _configure_logging(args.verbose)

    try:
        settings = load_settings()
        if args.command == "migrate":
            return _migrate(settings)
        if args.command == "run":
            settings = _resolve_pipelines(settings, args.pipeline)
            return _run(settings, skip_migrate=args.skip_migrate)
        raise AssertionError(f"unhandled command {args.command!r}")
    except ConfigError as exc:
        logger.error("Configuration error: %s", exc)
        return EXIT_ERROR
    except MigrationError as exc:
        logger.error("%s", exc)
        return EXIT_ERROR
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
