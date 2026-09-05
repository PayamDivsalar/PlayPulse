"""Tests for the crawler orchestration service."""

from __future__ import annotations

import unittest
from unittest.mock import Mock

from crawler.crawler_service import CrawlerService


class CrawlerServiceTests(unittest.TestCase):
    def _build_service(self, apps: list[dict], max_workers: int = 3) -> tuple[
        CrawlerService, Mock, Mock, Mock
    ]:
        playstore_client = Mock()
        kafka_producer = Mock()
        app_registry_client = Mock()
        app_registry_client.get_active_applications.return_value = apps
        playstore_client.get_reviews.return_value = [{"reviewId": "r1"}]

        service = CrawlerService(
            playstore_client=playstore_client,
            kafka_producer=kafka_producer,
            app_registry_client=app_registry_client,
            max_workers=max_workers,
        )
        return service, playstore_client, kafka_producer, app_registry_client

    def test_each_app_is_fetched_and_published(self) -> None:
        apps = [
            {"package_name": "com.a"},
            {"package_name": "com.b"},
            {"package_name": "com.c"},
        ]
        service, playstore, kafka, _ = self._build_service(apps)
        playstore.get_app_details.return_value = {"score": 4.5}

        service.run_crawl_cycle()

        self.assertEqual(playstore.get_app_details.call_count, 3)
        self.assertEqual(kafka.send_app_stats.call_count, 3)
        self.assertEqual(playstore.get_reviews.call_count, 3)
        self.assertEqual(kafka.send_reviews.call_count, 3)
        for _args, kwargs in playstore.get_reviews.call_args_list:
            self.assertEqual(kwargs.get("count"), 1000)

    def test_one_failing_app_does_not_stop_the_others(self) -> None:
        apps = [
            {"package_name": "com.a"},
            {"package_name": "com.b"},
            {"package_name": "com.c"},
        ]
        service, playstore, kafka, _ = self._build_service(apps)

        def details(package_name: str) -> dict:
            if package_name == "com.b":
                raise RuntimeError("play store failed")
            return {"score": 4.5}

        playstore.get_app_details.side_effect = details

        service.run_crawl_cycle()

        # Healthy apps still publish stats; the failing app still attempts reviews.
        self.assertEqual(kafka.send_app_stats.call_count, 2)
        published = {call.args[0] for call in kafka.send_app_stats.call_args_list}
        self.assertEqual(published, {"com.a", "com.c"})
        self.assertEqual(kafka.send_reviews.call_count, 3)

    def test_reviews_failure_does_not_block_app_stats(self) -> None:
        apps = [{"package_name": "com.a"}]
        service, playstore, kafka, _ = self._build_service(apps)
        playstore.get_app_details.return_value = {"score": 4.5}
        playstore.get_reviews.side_effect = RuntimeError("reviews failed")

        service.run_crawl_cycle()

        kafka.send_app_stats.assert_called_once_with("com.a", {"score": 4.5})
        kafka.send_reviews.assert_not_called()

    def test_app_stats_failure_does_not_block_reviews(self) -> None:
        apps = [{"package_name": "com.a"}]
        service, playstore, kafka, _ = self._build_service(apps)
        playstore.get_app_details.side_effect = RuntimeError("details failed")
        reviews = [{"reviewId": "r1"}]
        playstore.get_reviews.return_value = reviews

        service.run_crawl_cycle()

        kafka.send_app_stats.assert_not_called()
        kafka.send_reviews.assert_called_once_with("com.a", reviews)

    def test_empty_app_list_does_not_touch_kafka_or_raise(self) -> None:
        service, playstore, kafka, _ = self._build_service([])

        service.run_crawl_cycle()

        playstore.get_app_details.assert_not_called()
        playstore.get_reviews.assert_not_called()
        kafka.send_app_stats.assert_not_called()
        kafka.send_reviews.assert_not_called()
