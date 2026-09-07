"""Network analyzer specific exception types."""

from __future__ import annotations


class NetworkAnalyzerError(Exception):
    """Base exception for network analyzer failures."""


class AnalyzerConfigError(NetworkAnalyzerError):
    """Raised when a required configuration value is missing or invalid."""


class FilenameConventionError(NetworkAnalyzerError):
    """Raised when a capture filename does not follow the naming convention."""


class PcapReadError(NetworkAnalyzerError):
    """Raised when a pcap file cannot be opened or its records cannot be read."""


class UnsupportedLinkTypeError(PcapReadError):
    """Raised when a pcap uses a link-layer type the reader cannot decode."""


class RegistryRequestError(NetworkAnalyzerError):
    """Raised when the App API rejects the analyzer's request outright.

    Reserved for 4xx responses, which mean the request itself is wrong: a bad
    base URL, a path the API does not serve, or a host the API refuses to
    answer for. Deliberately not a ``requests.RequestException``, so the retry
    policy leaves it alone -- repeating a request the server has already
    rejected only delays the error the operator needs to see.
    """


class ApplicationNotEligibleError(NetworkAnalyzerError):
    """Raised when the target application must not receive network analysis.

    Covers three registry outcomes that are all operator mistakes rather than
    transient faults: the package is unknown, it is soft-deleted
    (``is_active=False``), or it is not flagged as a messaging app
    (``is_messaging_app=False``). None of them are retryable, so the analyzer
    fails fast instead of publishing a record the storage subsystem could not
    attach to a valid application row.
    """
