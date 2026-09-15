"""Tests for the crawler orchestration service."""

from __future__ import annotations

import unittest
from unittest.mock import Mock

from crawler.config import Settings
from crawler.crawler_service import CrawlerService


class CrawlerServiceTests(unittest.TestCase):
    def _build_service(self, apps: list[dict], max_workers: int = 3) -> tuple[
        CrawlerService, Mock, Mock, Mock, Settings
    ]:
        settings = Settings.for_testing(
            max_concurrent_workers=max_workers,
            reviews_fetch_count=1000,
        )
        playstore_client = Mock()
        kafka_producer = Mock()
        app_registry_client = Mock()
        app_registry_client.get_active_applications.return_value = apps
        playstore_client.get_reviews.return_value = [
            {
                "reviewId": "r1",
                "at": "2021-03-25T15:52:53",
                "userName": "Alice",
                "thumbsUpCount": 1,
                "score": 5,
                "content": "ok",
            }
        ]

        service = CrawlerService(
            playstore_client=playstore_client,
            kafka_producer=kafka_producer,
            app_registry_client=app_registry_client,
            max_workers=settings.max_concurrent_workers,
            reviews_fetch_count=settings.reviews_fetch_count,
        )
        return service, playstore_client, kafka_producer, app_registry_client, settings

    def test_each_app_is_fetched_and_published(self) -> None:
        apps = [
            {"package_name": "com.a"},
            {"package_name": "com.b"},
            {"package_name": "com.c"},
        ]
        service, playstore, kafka, _, settings = self._build_service(apps)
        playstore.get_app_details.return_value = {"score": 4.5}

        service.run_crawl_cycle()

        self.assertEqual(playstore.get_app_details.call_count, 3)
        self.assertEqual(kafka.send_app_stats.call_count, 3)
        self.assertEqual(playstore.get_reviews.call_count, 3)
        self.assertEqual(kafka.send_reviews.call_count, 3)
        for _args, kwargs in playstore.get_reviews.call_args_list:
            self.assertEqual(kwargs.get("count"), settings.reviews_fetch_count)
        for call in playstore.get_app_details.call_args_list:
            self.assertEqual(call.kwargs.get("country"), "us")
            self.assertEqual(call.kwargs.get("lang"), "en")
        for _args, kwargs in playstore.get_reviews.call_args_list:
            self.assertEqual(kwargs.get("country"), "us")
            self.assertEqual(kwargs.get("lang"), "en")

        for call in kafka.send_app_stats.call_args_list:
            package_name, payload = call.args
            self.assertEqual(payload["package_name"], package_name)
            self.assertIn("crawled_at", payload)
            self.assertIn("reviews_count", payload)

    def test_iranian_apps_use_iran_region_others_use_default(self) -> None:
        apps = [
            {"package_name": "com.iranian", "is_iranian_app": True},
            {"package_name": "com.global", "is_iranian_app": False},
            {"package_name": "com.legacy"},  # key missing -> default region
        ]
        service, playstore, kafka, _, _ = self._build_service(apps)
        playstore.get_app_details.return_value = {"score": 4.5}

        service.run_crawl_cycle()

        regions_by_package = {
            call.args[0]: (call.kwargs.get("country"), call.kwargs.get("lang"))
            for call in playstore.get_app_details.call_args_list
        }
        self.assertEqual(
            regions_by_package,
            {
                "com.iranian": ("ir", "fa"),
                "com.global": ("us", "en"),
                "com.legacy": ("us", "en"),
            },
        )
        for call in playstore.get_reviews.call_args_list:
            expected = (
                ("ir", "fa") if call.args[0] == "com.iranian" else ("us", "en")
            )
            self.assertEqual(
                (call.kwargs.get("country"), call.kwargs.get("lang")), expected
            )

    def test_one_failing_app_does_not_stop_the_others(self) -> None:
        apps = [
            {"package_name": "com.a"},
            {"package_name": "com.b"},
            {"package_name": "com.c"},
        ]
        service, playstore, kafka, _, _ = self._build_service(apps)

        def details(package_name: str, *, country: str, lang: str) -> dict:
            if package_name == "com.b":
                raise RuntimeError("play store failed")
            return {"score": 4.5}

        playstore.get_app_details.side_effect = details

        service.run_crawl_cycle()

        self.assertEqual(kafka.send_app_stats.call_count, 2)
        published = {call.args[0] for call in kafka.send_app_stats.call_args_list}
        self.assertEqual(published, {"com.a", "com.c"})
        self.assertEqual(kafka.send_reviews.call_count, 3)

    def test_reviews_failure_does_not_block_app_stats(self) -> None:
        apps = [{"package_name": "com.a"}]
        service, playstore, kafka, _, _ = self._build_service(apps)
        playstore.get_app_details.return_value = {
            "minInstalls": 100,
            "score": 4.5,
            "ratings": 10,
            "reviews": 3,
            "updated": 1_700_000_000,
            "version": "1.0.0",
            "adSupported": False,
        }
        playstore.get_reviews.side_effect = RuntimeError("reviews failed")

        service.run_crawl_cycle()

        kafka.send_app_stats.assert_called_once()
        package_name, payload = kafka.send_app_stats.call_args.args
        self.assertEqual(package_name, "com.a")
        self.assertEqual(payload["package_name"], "com.a")
        self.assertEqual(payload["score"], 4.5)
        self.assertEqual(payload["min_installs"], 100)
        self.assertEqual(payload["reviews_count"], 3)
        self.assertIn("crawled_at", payload)
        self.assertNotIn("minInstalls", payload)
        kafka.send_reviews.assert_not_called()

    def test_app_stats_failure_does_not_block_reviews(self) -> None:
        apps = [{"package_name": "com.a"}]
        service, playstore, kafka, _, _ = self._build_service(apps)
        playstore.get_app_details.side_effect = RuntimeError("details failed")
        playstore.get_reviews.return_value = [
            {
                "reviewId": "r1",
                "at": "2021-03-25T15:52:53",
                "userName": "Alice",
                "thumbsUpCount": 1,
                "score": 5,
                "content": "ok",
            }
        ]

        service.run_crawl_cycle()

        kafka.send_app_stats.assert_not_called()
        kafka.send_reviews.assert_called_once()
        package_name, mapped_reviews = kafka.send_reviews.call_args.args
        self.assertEqual(package_name, "com.a")
        self.assertEqual(len(mapped_reviews), 1)
        review = mapped_reviews[0]
        self.assertEqual(review["package_name"], "com.a")
        self.assertEqual(review["review_id"], "r1")
        self.assertEqual(review["user_name"], "Alice")
        self.assertEqual(review["thumbs_up_count"], 1)
        self.assertEqual(review["score"], 5)
        self.assertEqual(review["content"], "ok")
        self.assertEqual(review["at"], "2021-03-25T15:52:53")
        self.assertIn("crawled_at", review)
        self.assertNotIn("reviewId", review)

    def test_empty_app_list_does_not_touch_kafka_or_raise(self) -> None:
        service, playstore, kafka, _, _ = self._build_service([])

        service.run_crawl_cycle()

        playstore.get_app_details.assert_not_called()
        playstore.get_reviews.assert_not_called()
        kafka.send_app_stats.assert_not_called()
        kafka.send_reviews.assert_not_called()
