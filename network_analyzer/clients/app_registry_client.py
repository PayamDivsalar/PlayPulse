"""HTTP client for the App API registry.

The analyzer consults the registry before publishing, so that a mistyped or
ineligible package name fails on the operator's terminal instead of surfacing
hours later in the storage subsystem's log. This is the mitigation for the one
real drawback of publishing through Kafka rather than writing to the database
directly: asynchronous, remote error reporting.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

import requests

from network_analyzer.config import Settings
from network_analyzer.exceptions import (
    ApplicationNotEligibleError,
    RegistryRequestError,
)
from network_analyzer.common.retry_policy import with_retry

logger = logging.getLogger(__name__)

_APPLICATIONS_PATH = "/api/applications/"


class AppRegistryClient:
    """Verify that an application is eligible for network analysis."""

    def __init__(self, base_url: str, settings: Settings) -> None:
        self.base_url = base_url.rstrip("/")
        self._timeout_seconds = settings.registry_request_timeout_seconds
        retry = with_retry(
            max_retries=settings.retry_max_attempts,
            base_delay_seconds=settings.retry_base_delay_seconds,
            exceptions=(requests.RequestException,),
        )
        # Bind once at construction so each call does not rebuild a wrapper.
        self._list_applications_with_retry: Callable[[], list[dict[str, Any]]] = retry(
            self._list_applications_once
        )

    def ensure_eligible(self, package_name: str) -> dict[str, Any]:
        """Return the registry record for ``package_name``, or explain why not.

        Three outcomes are rejected, all of them operator mistakes rather than
        transient faults, so none is retried:

        * the package is not registered at all
        * the package is soft-deleted (``is_active`` false)
        * the package is not flagged for network analysis
          (``is_messaging_app`` false)

        Raises:
            ApplicationNotEligibleError: on any of the above.
            requests.RequestException: if the registry is unreachable after
                retries are exhausted. Distinguished from ineligibility on
                purpose: an unreachable registry is a reason to try again
                later, not a reason to reject the capture.
        """

        application = self._find_application(package_name)

        if application is None:
            raise ApplicationNotEligibleError(
                f"{package_name!r} is not registered in the App API. Add it "
                f"first: POST {self.base_url}{_APPLICATIONS_PATH}"
            )

        if not application.get("is_active", False):
            raise ApplicationNotEligibleError(
                f"{package_name!r} is registered but deactivated. Reactivate "
                "it before publishing network metrics for it."
            )

        if not application.get("is_messaging_app", False):
            raise ApplicationNotEligibleError(
                f"{package_name!r} is not flagged as a messaging app. Network "
                "analysis is specified only for the messaging/chat category; "
                "set is_messaging_app=true if this app does belong there."
            )

        logger.info(
            "Registry check passed for package_name=%s (application_id=%s).",
            package_name,
            application.get("id"),
        )
        return application

    def _find_application(self, package_name: str) -> dict[str, Any] | None:
        """Look up one application by package name.

        The registry exposes no lookup-by-package-name endpoint, so the full
        list is fetched and matched here. It is fetched *unfiltered* rather
        than with ``?is_active=true``, so that a deactivated application can be
        reported as deactivated instead of as missing. The registry holds one
        row per monitored app, so the cost is negligible.
        """

        url = f"{self.base_url}{_APPLICATIONS_PATH}"
        try:
            applications = self._list_applications_with_retry()
        except Exception:
            # The caller renders the operator-facing message; keeping the
            # traceback at debug level stops a routine "registry is down" from
            # burying that message under a wall of stack frames.
            logger.debug(
                "Failed to fetch the application registry from %s", url, exc_info=True
            )
            raise

        for application in applications:
            if application.get("package_name") == package_name:
                return application
        return None

    def _list_applications_once(self) -> list[dict[str, Any]]:
        url = f"{self.base_url}{_APPLICATIONS_PATH}"
        response = requests.get(url, timeout=self._timeout_seconds)

        if 400 <= response.status_code < 500:
            # Never retried: the server understood the request and refused it,
            # so repeating it verbatim cannot change the answer. A 400 here is
            # usually Django's ALLOWED_HOSTS rejecting the hostname the
            # analyzer was pointed at.
            raise RegistryRequestError(
                f"The registry at {url} rejected the request with HTTP "
                f"{response.status_code}. Check APP_API_BASE_URL, and that the "
                "App API accepts requests for that hostname "
                "(Django's ALLOWED_HOSTS)."
            )

        # 5xx falls through to raise_for_status, which the retry policy treats
        # as retryable: a restarting or overloaded API may well answer next time.
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, list):
            raise ValueError(
                f"Expected a JSON list of applications, got {type(payload).__name__}."
            )
        return payload
