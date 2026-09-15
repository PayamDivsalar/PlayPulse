"""Orchestrates crawl work across the Play Store and Kafka pipeline."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NamedTuple

from crawler.app_registry_client import AppRegistryClient
from crawler.cycle_report import write_pending_cycle_report
from crawler.data_mapper import map_app_details, map_review
from crawler.kafka_producer import CrawlerKafkaProducer
from crawler.playstore_client import PlayStoreClient

logger = logging.getLogger(__name__)


class AppCrawlResult(NamedTuple):
    """Per-app outcome for independently tracked stats and reviews work."""

    package_name: str
    stats_ok: bool
    reviews_ok: bool


class CrawlerService:
    """Coordinate app discovery, metadata fetching, and event publishing."""

    def __init__(
        self,
        playstore_client: PlayStoreClient,
        kafka_producer: CrawlerKafkaProducer,
        app_registry_client: AppRegistryClient,
        *,
        max_workers: int,
        reviews_fetch_count: int,
        default_country: str = "us",
        default_lang: str = "en",
        iran_country: str = "ir",
        iran_lang: str = "fa",
        cycle_reports_enabled: bool = False,
        cycle_reports_dir: str = "/data/reports",
    ) -> None:
        self.playstore_client = playstore_client
        self.kafka_producer = kafka_producer
        self.app_registry_client = app_registry_client
        self.max_workers = max_workers
        self.reviews_fetch_count = reviews_fetch_count
        self.default_country = default_country
        self.default_lang = default_lang
        self.iran_country = iran_country
        self.iran_lang = iran_lang
        self.cycle_reports_enabled = cycle_reports_enabled
        self.cycle_reports_dir = cycle_reports_dir

    def _crawl_single_app(self, app: dict[str, Any]) -> AppCrawlResult:
        """Fetch and publish stats and reviews for one app in the same worker.

        Stats and reviews are independent: failure of one path must not block
        the other. Both still share this thread and the shared rate limiter.
        """

        package_name = self._get_package_name(app)
        if not package_name:
            raise ValueError(f"Application payload missing package name: {app!r}")

        if app.get('is_iranian_app', False):
            country, lang = self.iran_country, self.iran_lang
        else:
            country, lang = self.default_country, self.default_lang
        logger.debug(
            "Crawling package_name=%s with country=%s lang=%s",
            package_name,
            country,
            lang,
        )

        stats_ok = False
        reviews_ok = False

        try:
            app_details = self.playstore_client.get_app_details(
                package_name, country=country, lang=lang
            )
            mapped_stats = map_app_details(app_details, package_name)
            self.kafka_producer.send_app_stats(package_name, mapped_stats)
            stats_ok = True
        except Exception:
            logger.error(
                "Failed to fetch/publish app stats package_name=%s",
                package_name,
                exc_info=True,
            )

        try:
            reviews = self.playstore_client.get_reviews(
                package_name,
                count=self.reviews_fetch_count,
                country=country,
                lang=lang,
            )
            mapped_reviews = [map_review(review, package_name) for review in reviews]
            self.kafka_producer.send_reviews(package_name, mapped_reviews)
            reviews_ok = True
        except Exception:
            logger.error(
                "Failed to fetch/publish reviews package_name=%s",
                package_name,
                exc_info=True,
            )

        return AppCrawlResult(
            package_name=package_name,
            stats_ok=stats_ok,
            reviews_ok=reviews_ok,
        )

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

        started_at = datetime.now(timezone.utc)
        stats_success = 0
        reviews_success = 0
        failed_stats: list[str] = []
        failed_reviews: list[str] = []
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
                        elif result.package_name:
                            failed_stats.append(result.package_name)
                        if result.reviews_ok:
                            reviews_success += 1
                        elif result.package_name:
                            failed_reviews.append(result.package_name)
                    except Exception:
                        logger.error(
                            "Unexpected crawl failure package_name=%s",
                            package_name,
                            exc_info=True,
                        )
                        if package_name:
                            failed_stats.append(package_name)
                            failed_reviews.append(package_name)
        finally:
            # Flush buffered messages even if the cycle errors out, so
            # successfully-sent stats/reviews are not lost on shutdown.
            self.kafka_producer.flush()

        finished_at = datetime.now(timezone.utc)

        logger.info(
            "Crawl cycle summary: total=%s app_stats=%s/%s success reviews=%s/%s success",
            total,
            stats_success,
            total,
            reviews_success,
            total,
        )

        if self.cycle_reports_enabled:
            write_pending_cycle_report(
                Path(self.cycle_reports_dir),
                started_at=started_at,
                finished_at=finished_at,
                apps_total=total,
                stats_ok=stats_success,
                reviews_ok=reviews_success,
                failed_stats=sorted(set(failed_stats)),
                failed_reviews=sorted(set(failed_reviews)),
            )
