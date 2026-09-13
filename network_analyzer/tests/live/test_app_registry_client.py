"""Live App API eligibility checks.

Hits a real App API. Run manually with ``-m live``.

Requires the App API on ``APP_API_BASE_URL`` (default http://127.0.0.1:8000)
holding an active messaging app whose package is ``LIVE_PACKAGE_NAME``.

The mocked unit tests live in
``network_analyzer/tests/unit/test_app_registry_client.py``.
"""

from __future__ import annotations

import os
import unittest

import pytest

from network_analyzer.clients.app_registry_client import AppRegistryClient
from network_analyzer.config import Settings


@pytest.mark.live
class LiveAppRegistryTests(unittest.TestCase):
    def test_registry_is_reachable_and_returns_a_list(self) -> None:
        base_url = os.getenv("APP_API_BASE_URL", "http://127.0.0.1:8000")
        settings = Settings.for_testing(app_api_base_url=base_url)
        client = AppRegistryClient(base_url=base_url, settings=settings)

        applications = client._list_applications_once()

        self.assertIsInstance(applications, list)

    def test_known_package_is_eligible(self) -> None:
        package_name = os.getenv("LIVE_PACKAGE_NAME")
        if not package_name:
            self.skipTest("Set LIVE_PACKAGE_NAME to run this check.")

        base_url = os.getenv("APP_API_BASE_URL", "http://127.0.0.1:8000")
        settings = Settings.for_testing(app_api_base_url=base_url)
        client = AppRegistryClient(base_url=base_url, settings=settings)

        application = client.ensure_eligible(package_name)

        self.assertEqual(application["package_name"], package_name)


if __name__ == "__main__":
    unittest.main()
