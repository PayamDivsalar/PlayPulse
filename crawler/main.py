"""Crawler process entrypoint (composition root)."""

from __future__ import annotations

import logging
import signal
import sys

from crawler.app_registry_client import AppRegistryClient
from crawler.config import load_settings
from crawler.crawler_service import CrawlerService
from crawler.kafka_producer import CrawlerKafkaProducer
from crawler.playstore_client import PlayStoreClient
from crawler.rate_limiter import RateLimiter
from crawler.scheduler import build_scheduler

logger = logging.getLogger(__name__)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    """Load settings, wire dependencies, start the scheduler, and block forever."""

    _configure_logging()
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
    crawler_service = CrawlerService(
        playstore_client=playstore_client,
        kafka_producer=kafka_producer,
        app_registry_client=app_registry_client,
        max_workers=settings.max_concurrent_workers,
        reviews_fetch_count=settings.reviews_fetch_count,
        default_country=settings.default_country,
        default_lang=settings.default_lang,
        iran_country=settings.iran_country,
        iran_lang=settings.iran_lang,
        cycle_reports_enabled=settings.cycle_reports_enabled,
        cycle_reports_dir=settings.cycle_reports_dir,
    )
    scheduler = build_scheduler(
        crawler_service,
        interval_hours=settings.crawl_interval_hours,
    )

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
        "Starting crawler scheduler (interval=%sh, rate_limit=%s/%ss, workers=%s, "
        "cycle_reports=%s)",
        settings.crawl_interval_hours,
        settings.rate_limit_max_requests,
        int(settings.rate_limit_per_seconds),
        settings.max_concurrent_workers,
        "on" if settings.cycle_reports_enabled else "off",
    )
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Crawler stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
