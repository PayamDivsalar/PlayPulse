"""Tests for the App API registry client."""

from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import pytest
import requests

from crawler.app_registry_client import AppRegistryClient
from crawler.config import Settings, load_settings


class AppRegistryClientTests(unittest.TestCase):
    def _client(self, **settings_overrides: object) -> AppRegistryClient:
        settings = Settings.for_testing(**settings_overrides)
        return AppRegistryClient(base_url=settings.app_api_base_url, settings=settings)

    def test_get_active_applications_builds_correct_url_and_params(self) -> None:
        client = self._client()
        response = Mock()
        response.json.return_value = []
        response.raise_for_status.return_value = None

        with patch("crawler.app_registry_client.requests.get", return_value=response) as get_mock:
            client.get_active_applications()

        get_mock.assert_called_once_with(
            "http://api.local/api/applications/",
            params={"is_active": "true"},
            timeout=10.0,
        )

    def test_get_active_applications_parses_and_returns_list(self) -> None:
        client = self._client()
        apps = [{"package_name": "com.a"}, {"package_name": "com.b"}]
        response = Mock()
        response.json.return_value = apps
        response.raise_for_status.return_value = None

        with patch("crawler.app_registry_client.requests.get", return_value=response):
            result = client.get_active_applications()

        self.assertEqual(result, apps)

    def test_get_active_applications_retries_then_reraises_on_network_error(self) -> None:
        settings = Settings.for_testing(retry_max_attempts=3)
        client = AppRegistryClient(base_url=settings.app_api_base_url, settings=settings)

        with patch(
            "crawler.app_registry_client.requests.get",
            side_effect=requests.ConnectionError("boom"),
        ) as get_mock:
            with patch("crawler.retry_policy.sleep", return_value=None):
                with self.assertRaises(requests.ConnectionError):
                    client.get_active_applications()

        self.assertEqual(get_mock.call_count, settings.retry_max_attempts + 1)


@pytest.mark.live
class AppRegistryClientLiveTests(unittest.TestCase):
    """Live App API tests. Require a running registry; do not run in CI."""

    def test_live_get_active_applications(self) -> None:
        """Fetch the active app list from a real App API instance.

        Depends on App API via ``APP_API_BASE_URL``. Do not run in CI.
        An empty list is a valid result when no apps are registered yet.
        """

        settings = load_settings()
        client = AppRegistryClient(
            base_url=settings.app_api_base_url,
            settings=settings,
        )
        apps = client.get_active_applications()

        self.assertIsInstance(apps, list)
        for app in apps:
            self.assertIn("package_name", app)
            self.assertIn("is_active", app)
