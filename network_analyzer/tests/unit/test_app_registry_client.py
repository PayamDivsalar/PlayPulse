"""Tests for the App API eligibility check."""

from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import requests

from network_analyzer.clients.app_registry_client import AppRegistryClient
from network_analyzer.config import Settings
from network_analyzer.exceptions import (
    ApplicationNotEligibleError,
    RegistryRequestError,
)


def _client(**overrides: object) -> AppRegistryClient:
    settings = Settings.for_testing(
        retry_max_attempts=0, retry_base_delay_seconds=0.0, **overrides
    )
    return AppRegistryClient(base_url=settings.app_api_base_url, settings=settings)


def _response(payload: object, status_code: int = 200) -> Mock:
    response = Mock()
    response.json.return_value = payload
    response.status_code = status_code
    response.raise_for_status.return_value = None
    return response


_ELIGIBLE = {
    "id": 1,
    "package_name": "com.whatsapp",
    "is_active": True,
    "is_messaging_app": True,
}


class EligibilityTests(unittest.TestCase):
    def test_accepts_active_messaging_app(self) -> None:
        with patch("requests.get", return_value=_response([_ELIGIBLE])):
            application = _client().ensure_eligible("com.whatsapp")

        self.assertEqual(application["id"], 1)

    def test_rejects_unregistered_package(self) -> None:
        with patch("requests.get", return_value=_response([])):
            with self.assertRaises(ApplicationNotEligibleError) as ctx:
                _client().ensure_eligible("com.unknown")

        self.assertIn("not registered", str(ctx.exception))

    def test_rejects_deactivated_package_distinctly_from_missing(self) -> None:
        deactivated = {**_ELIGIBLE, "is_active": False}
        with patch("requests.get", return_value=_response([deactivated])):
            with self.assertRaises(ApplicationNotEligibleError) as ctx:
                _client().ensure_eligible("com.whatsapp")

        self.assertIn("deactivated", str(ctx.exception))

    def test_rejects_non_messaging_app(self) -> None:
        non_messaging = {**_ELIGIBLE, "is_messaging_app": False}
        with patch("requests.get", return_value=_response([non_messaging])):
            with self.assertRaises(ApplicationNotEligibleError) as ctx:
                _client().ensure_eligible("com.whatsapp")

        self.assertIn("messaging", str(ctx.exception))

    def test_selects_the_matching_package_from_a_full_registry(self) -> None:
        registry = [
            {
                "id": 5,
                "package_name": "com.other",
                "is_active": True,
                "is_messaging_app": True,
            },
            _ELIGIBLE,
        ]
        with patch("requests.get", return_value=_response(registry)):
            application = _client().ensure_eligible("com.whatsapp")

        self.assertEqual(application["id"], 1)

    def test_fetches_the_registry_unfiltered(self) -> None:
        """Filtering server-side would hide deactivated apps behind 'missing'."""

        with patch("requests.get", return_value=_response([_ELIGIBLE])) as mock_get:
            _client().ensure_eligible("com.whatsapp")

        _, kwargs = mock_get.call_args
        self.assertNotIn("params", kwargs)

    def test_missing_eligibility_flags_are_treated_as_false(self) -> None:
        with patch(
            "requests.get",
            return_value=_response([{"id": 2, "package_name": "com.whatsapp"}]),
        ):
            with self.assertRaises(ApplicationNotEligibleError):
                _client().ensure_eligible("com.whatsapp")


class TransportFailureTests(unittest.TestCase):
    def test_network_error_propagates_rather_than_rejecting(self) -> None:
        """An unreachable registry is a retry-later condition, not a rejection."""

        with patch("requests.get", side_effect=requests.ConnectionError("down")):
            with self.assertRaises(requests.RequestException):
                _client().ensure_eligible("com.whatsapp")

    def test_server_error_propagates(self) -> None:
        response = _response(None, status_code=503)
        response.raise_for_status.side_effect = requests.HTTPError("503")
        with patch("requests.get", return_value=response):
            with self.assertRaises(requests.RequestException):
                _client().ensure_eligible("com.whatsapp")

    def test_server_error_is_retried(self) -> None:
        """A restarting API may well answer correctly on the next attempt."""

        failing = _response(None, status_code=503)
        failing.raise_for_status.side_effect = requests.HTTPError("503")
        settings = Settings.for_testing(
            retry_max_attempts=2, retry_base_delay_seconds=0.0
        )
        client = AppRegistryClient(
            base_url=settings.app_api_base_url, settings=settings
        )

        with patch(
            "requests.get", side_effect=[failing, failing, _response([_ELIGIBLE])]
        ) as mock_get:
            with patch("network_analyzer.common.retry_policy.sleep"):
                application = client.ensure_eligible("com.whatsapp")

        self.assertEqual(application["id"], 1)
        self.assertEqual(mock_get.call_count, 3)

    def test_client_error_raises_a_registry_request_error(self) -> None:
        with patch("requests.get", return_value=_response(None, status_code=400)):
            with self.assertRaises(RegistryRequestError) as ctx:
                _client().ensure_eligible("com.whatsapp")

        self.assertIn("400", str(ctx.exception))
        self.assertIn("ALLOWED_HOSTS", str(ctx.exception))

    def test_client_error_is_not_retried(self) -> None:
        """A 400 means the server understood and refused; repeating is futile.

        Django answers 400 when the request's Host header is not in
        ALLOWED_HOSTS, which is exactly what a containerized analyzer hits. Left
        retryable, that misconfiguration costs an exponential backoff per
        capture before reporting a problem no wait will fix.
        """

        settings = Settings.for_testing(
            retry_max_attempts=5, retry_base_delay_seconds=0.0
        )
        client = AppRegistryClient(
            base_url=settings.app_api_base_url, settings=settings
        )

        with patch(
            "requests.get", return_value=_response(None, status_code=400)
        ) as mock_get:
            with self.assertRaises(RegistryRequestError):
                client.ensure_eligible("com.whatsapp")

        self.assertEqual(mock_get.call_count, 1)

    def test_not_found_is_not_retried(self) -> None:
        with patch(
            "requests.get", return_value=_response(None, status_code=404)
        ) as mock_get:
            with self.assertRaises(RegistryRequestError):
                _client().ensure_eligible("com.whatsapp")

        self.assertEqual(mock_get.call_count, 1)

    def test_non_list_payload_raises(self) -> None:
        with patch("requests.get", return_value=_response({"results": []})):
            with self.assertRaises(ValueError):
                _client().ensure_eligible("com.whatsapp")

    def test_retries_transient_failures_then_succeeds(self) -> None:
        settings = Settings.for_testing(
            retry_max_attempts=2, retry_base_delay_seconds=0.0
        )
        client = AppRegistryClient(
            base_url=settings.app_api_base_url, settings=settings
        )
        responses = [
            requests.ConnectionError("first"),
            requests.ConnectionError("second"),
            _response([_ELIGIBLE]),
        ]

        with patch("requests.get", side_effect=responses):
            with patch("network_analyzer.common.retry_policy.sleep"):
                application = client.ensure_eligible("com.whatsapp")

        self.assertEqual(application["id"], 1)

    def test_ineligibility_is_not_retried(self) -> None:
        """Retrying a rejected package would only waste the operator's time."""

        with patch("requests.get", return_value=_response([])) as mock_get:
            settings = Settings.for_testing(
                retry_max_attempts=5, retry_base_delay_seconds=0.0
            )
            client = AppRegistryClient(
                base_url=settings.app_api_base_url, settings=settings
            )
            with self.assertRaises(ApplicationNotEligibleError):
                client.ensure_eligible("com.unknown")

        self.assertEqual(mock_get.call_count, 1)


class UrlTests(unittest.TestCase):
    def test_trailing_slash_in_base_url_does_not_double_up(self) -> None:
        settings = Settings.for_testing(app_api_base_url="http://api.local/")
        client = AppRegistryClient(base_url=settings.app_api_base_url, settings=settings)

        with patch("requests.get", return_value=_response([_ELIGIBLE])) as mock_get:
            client.ensure_eligible("com.whatsapp")

        url = mock_get.call_args[0][0]
        self.assertEqual(url, "http://api.local/api/applications/")


if __name__ == "__main__":
    unittest.main()
