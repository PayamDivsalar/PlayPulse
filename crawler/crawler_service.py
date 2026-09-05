"""Orchestrates crawl work across the Play Store and Kafka pipeline."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, NamedTuple

from crawler.app_registry_client import AppRegistryClient
from crawler.kafka_producer import CrawlerKafkaProducer
from crawler.playstore_client import PlayStoreClient

logger = logging.getLogger(__name__)


class AppCrawlResult(NamedTuple):
    """Per-app outcome for independently tracked stats and reviews work."""

    stats_ok: bool
    reviews_ok: bool


class CrawlerService:
    """Coordinate app discovery, metadata fetching, and event publishing."""

    def __init__(
        self,
        playstore_client: PlayStoreClient,
        kafka_producer: CrawlerKafkaProducer,
        app_registry_client: AppRegistryClient,
        max_workers: int = 5,
    ) -> None:
        self.playstore_client = playstore_client
        self.kafka_producer = kafka_producer
        self.app_registry_client = app_registry_client
        self.max_workers = max_workers

    def _crawl_single_app(self, app: dict[str, Any]) -> AppCrawlResult:
        """Fetch and publish stats and reviews for one app in the same worker.

        Stats and reviews are independent: failure of one path must not block
        the other. Both still share this thread and the shared rate limiter.
        """

        package_name = self._get_package_name(app)
        if not package_name:
            raise ValueError(f"Application payload missing package name: {app!r}")

        stats_ok = False
        reviews_ok = False

        try:
            app_details = self.playstore_client.get_app_details(package_name)
            self.kafka_producer.send_app_stats(package_name, app_details)
            stats_ok = True
        except Exception:
            logger.error(
                "Failed to fetch/publish app stats package_name=%s",
                package_name,
                exc_info=True,
            )

        try:
            reviews = self.playstore_client.get_reviews(package_name, count=1000)
            self.kafka_producer.send_reviews(package_name, reviews)
            reviews_ok = True
        except Exception:
            logger.error(
                "Failed to fetch/publish reviews package_name=%s",
                package_name,
                exc_info=True,
            )

        return AppCrawlResult(stats_ok=stats_ok, reviews_ok=reviews_ok)

    @staticmethod
    def _get_package_name(app: dict[str, Any]) -> str | None:
        """Read the package name from the common registry field names."""

        for key in ("package_name", "packageName"):
            value = app.get(key)
            if isinstance(value, str) and value:
                return value
        return None

    def run_crawl_cycle(self) -> None:
        """Fetch all active apps, crawl them in parallel, and publish events."""

        apps = self.app_registry_client.get_active_applications()
        if not apps:
            logger.info("No active applications found for crawling; skipping cycle.")
            return

        stats_success = 0
        reviews_success = 0
        total = len(apps)

        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                futures = {
                    executor.submit(self._crawl_single_app, app): app
                    for app in apps
                }

                for future in as_completed(futures):
                    app = futures[future]
                    package_name = self._get_package_name(app)
                    try:
                        result = future.result()
                        if result.stats_ok:
                            stats_success += 1
                        if result.reviews_ok:
                            reviews_success += 1
                    except Exception:
                        logger.error(
                            "Unexpected crawl failure package_name=%s",
                            package_name,
                            exc_info=True,
                        )
        finally:
            # Flush buffered messages even if the cycle errors out, so
            # successfully-sent stats/reviews are not lost on shutdown.
            self.kafka_producer.flush()

        logger.info(
            "Crawl cycle summary: total=%s app_stats=%s/%s success reviews=%s/%s success",
            total,
            stats_success,
            total,
            reviews_success,
            total,
        )
