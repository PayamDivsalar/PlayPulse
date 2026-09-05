"""Crawler process entrypoint (composition root)."""

from __future__ import annotations

import logging
import signal
import sys

from crawler.app_registry_client import AppRegistryClient
from crawler.crawler_service import CrawlerService
from crawler.kafka_producer import CrawlerKafkaProducer
from crawler.playstore_client import PlayStoreClient
from crawler.rate_limiter import RateLimiter
from crawler.scheduler import build_scheduler

logger = logging.getLogger(__name__)

# Conservative shared budget for Play Store HTTP calls across worker threads.
_RATE_LIMIT_MAX_REQUESTS = 10
_RATE_LIMIT_PER_SECONDS = 60.0


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    """Wire real dependencies, start the hourly scheduler, and block forever."""

    _configure_logging()

    rate_limiter = RateLimiter(
        max_requests=_RATE_LIMIT_MAX_REQUESTS,
        per_seconds=_RATE_LIMIT_PER_SECONDS,
    )
    playstore_client = PlayStoreClient(rate_limiter=rate_limiter)
    kafka_producer = CrawlerKafkaProducer()
    app_registry_client = AppRegistryClient()
    crawler_service = CrawlerService(
        playstore_client=playstore_client,
        kafka_producer=kafka_producer,
        app_registry_client=app_registry_client,
    )
    scheduler = build_scheduler(crawler_service)

    def _shutdown(signum: int, _frame: object) -> None:
        logger.info("Received signal %s; shutting down crawler", signum)
        scheduler.shutdown(wait=False)
        try:
            kafka_producer.close()
        except Exception:
            logger.exception("Failed to close Kafka producer during shutdown")

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info(
        "Starting crawler scheduler (interval=1h, rate_limit=%s/%ss)",
        _RATE_LIMIT_MAX_REQUESTS,
        int(_RATE_LIMIT_PER_SECONDS),
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Crawler stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
