"""Orchestrates crawl work across the Play Store and Kafka pipeline."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from crawler.app_registry_client import AppRegistryClient
from crawler.kafka_producer import CrawlerKafkaProducer
from crawler.playstore_client import PlayStoreClient

logger = logging.getLogger(__name__)


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

    def _crawl_single_app(self, app: dict[str, Any]) -> None:
        """Fetch an app's stats and reviews and publish both to Kafka.

        Stats are published before reviews are fetched, so a stats payload is
        never lost just because the (larger, slower) reviews call later fails.
        Any failure propagates to ``run_crawl_cycle``, which isolates it to this
        one app.
        """

        package_name = self._get_package_name(app)
        if not package_name:
            raise ValueError(f"Application payload missing package name: {app!r}")

        app_details = self.playstore_client.get_app_details(package_name)
        self.kafka_producer.send_app_stats(package_name, app_details)

        reviews = self.playstore_client.get_reviews(package_name)
        self.kafka_producer.send_reviews(package_name, reviews)

    @staticmethod
    def _get_package_name(app: dict[str, Any]) -> str | None:
        """Read the package name from the common registry field names."""

        for key in ("package_name", "packageName"):
            value = app.get(key)
            if isinstance(value, str) and value:
                return value
        return None

    def run_crawl_cycle(self) -> None:
        """Fetch all active apps, crawl them in parallel, and publish stats."""

        apps = self.app_registry_client.get_active_applications()
        if not apps:
            logger.info("No active applications found for crawling; skipping cycle.")
            return

        success_count = 0
        failure_count = 0

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
                        future.result()
                        success_count += 1
                    except Exception:
                        failure_count += 1
                        logger.error(
                            "Failed to crawl app package_name=%s",
                            package_name,
                            exc_info=True,
                        )
        finally:
            # Flush buffered messages even if the cycle errors out, so
            # successfully-sent stats/reviews are not lost on shutdown.
            self.kafka_producer.flush()

        logger.info(
            "Crawl cycle summary: total=%s success=%s failed=%s",
            len(apps),
            success_count,
            failure_count,
        )
