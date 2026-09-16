"""Command-line entry point and composition root for the sentiment job.

Usage::

    python -m sentiment.main run
    python -m sentiment.main run --batch-size 50 --dry-run
"""

from __future__ import annotations

import argparse
import dataclasses
import logging
import sys

from sentiment.config import Settings, load_settings
from sentiment.core.classifier import SentimentClassifier
from sentiment.exceptions import ClassificationError, ConfigError, SentimentError
from sentiment.persistence.database import Database
from sentiment.persistence.review_repository import ReviewRepository
from sentiment.runtime.sentiment_service import SentimentService

logger = logging.getLogger(__name__)

EXIT_OK = 0
EXIT_ERROR = 1
# 2 is reserved: argparse uses it for usage errors.


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m sentiment.main",
        description=(
            "Reclassify every review with non-empty content and overwrite "
            "reviews.sentiment. This is a finishing job, not a daemon."
        ),
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser(
        "run",
        help="Reclassify all reviews and overwrite sentiment labels.",
        description=(
            "Loops run_batch_cycle over the whole reviews table (by id), "
            "overwriting existing labels, then exits."
        ),
    )
    run_parser.add_argument(
        "--batch-size",
        type=int,
        metavar="N",
        help="Override SENTIMENT_BATCH_SIZE / Settings.batch_size for this run.",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Classify and log counts without writing to the database.",
    )
    return parser


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )


def _build_service(
    settings: Settings, *, dry_run: bool
) -> tuple[SentimentService, Database]:
    """Wire the object graph and return the service plus the DB to close."""

    classifier = SentimentClassifier(settings.model_name)
    database = Database(settings, application_name="sentiment")
    repository = ReviewRepository()
    service = SentimentService(
        settings=settings,
        classifier=classifier,
        database=database,
        repository=repository,
        dry_run=dry_run,
    )
    return service, database


def main(argv: list[str] | None = None) -> int:
    """Run the sentiment job. Returns the process exit code."""

    args = _build_parser().parse_args(argv)
    _configure_logging(args.verbose)

    database: Database | None = None
    try:
        settings = load_settings()
        if args.batch_size is not None:
            if args.batch_size <= 0:
                logger.error("--batch-size must be > 0.")
                return EXIT_ERROR
            settings = dataclasses.replace(settings, batch_size=args.batch_size)

        service, database = _build_service(settings, dry_run=args.dry_run)

        batches = 0
        while service.run_batch_cycle():
            batches += 1

        logger.info("Sentiment job finished after %s batch cycle(s).", batches)
        return EXIT_OK
    except ConfigError as exc:
        logger.error("Configuration error: %s", exc)
        return EXIT_ERROR
    except ClassificationError as exc:
        logger.error("Classification error: %s", exc)
        return EXIT_ERROR
    except SentimentError as exc:
        logger.error("%s", exc)
        return EXIT_ERROR
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
        return EXIT_ERROR
    except Exception:
        logger.error("Unexpected failure in the sentiment job.", exc_info=True)
        return EXIT_ERROR
    finally:
        if database is not None:
            database.close()


if __name__ == "__main__":
    raise SystemExit(main())
