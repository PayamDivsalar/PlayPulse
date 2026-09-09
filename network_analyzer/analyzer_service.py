"""Application orchestration for the network analyzer.

Holds the use case -- resolve, validate, analyze, publish -- and nothing else.
It knows the *order* of the steps but none of their mechanics: pcap decoding
lives in ``analysis``, registry lookups in ``app_registry_client``, delivery in
``kafka_publisher``.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path

from network_analyzer.analysis.metrics_calculator import analyze_capture
from network_analyzer.app_registry_client import AppRegistryClient
from network_analyzer.filename_parser import parse_capture_filename
from network_analyzer.kafka_publisher import NetworkMetricsPublisher
from network_analyzer.message_mapper import map_analysis_result
from network_analyzer.models import AnalysisResult, Scenario

logger = logging.getLogger(__name__)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _new_analysis_id() -> str:
    return str(uuid.uuid4())


class AnalyzerService:
    """Analyze capture files and publish the results.

    Both collaborators are optional, and their absence *is* the feature switch:

    * ``registry_client=None`` skips the eligibility check, for working offline
      or against captures whose app is not registered yet.
    * ``publisher=None`` performs a dry run, computing and returning metrics
      without contacting a broker. The composition root omits the publisher
      rather than passing a flag, so a dry run never opens a Kafka connection
      at all.

    ``clock`` and ``analysis_id_factory`` are injected so tests can assert on
    exact timestamps and identifiers.
    """

    def __init__(
        self,
        *,
        registry_client: AppRegistryClient | None = None,
        publisher: NetworkMetricsPublisher | None = None,
        clock: Callable[[], datetime] = _utc_now,
        analysis_id_factory: Callable[[], str] = _new_analysis_id,
    ) -> None:
        self._registry_client = registry_client
        self._publisher = publisher
        self._clock = clock
        self._analysis_id_factory = analysis_id_factory

    def analyze_file(
        self,
        pcap_path: str | Path,
        *,
        package_name: str | None = None,
        scenario: Scenario | None = None,
    ) -> AnalysisResult:
        """Analyze one capture file, publishing unless configured not to.

        ``package_name`` and ``scenario`` override what the filename encodes.
        Either may be supplied alone; the filename supplies whichever is
        missing, and is not consulted at all when both are given.

        Ordering matters here. The registry check runs *before* the analysis so
        an ineligible package costs nothing, and publication runs last so a
        failed analysis never emits a partial record.

        Raises:
            FilenameConventionError: if a value is neither given nor derivable
                from the filename.
            ApplicationNotEligibleError: if the registry rejects the package.
            PcapReadError: if the capture cannot be read.
            KafkaError: if publication cannot be confirmed.
        """

        path = Path(pcap_path)
        resolved_package, resolved_scenario = self._resolve_target(
            path, package_name, scenario
        )

        if self._registry_client is not None:
            self._registry_client.ensure_eligible(resolved_package)
        else:
            logger.warning(
                "Registry check skipped for package_name=%s; the storage "
                "subsystem will reject this record if the package is unknown.",
                resolved_package,
            )

        logger.info(
            "Analyzing %s as package_name=%s scenario=%s",
            path.name,
            resolved_package,
            resolved_scenario.value,
        )
        metrics = analyze_capture(path)

        result = AnalysisResult(
            analysis_id=self._analysis_id_factory(),
            package_name=resolved_package,
            scenario=resolved_scenario,
            # Filename only: the absolute path is an artifact of whichever
            # machine ran the analysis and would not be reproducible.
            source_pcap_filename=path.name,
            analyzed_at=self._clock(),
            metrics=metrics,
        )

        if self._publisher is not None:
            self._publisher.publish(result.package_name, map_analysis_result(result))
        else:
            logger.info(
                "Dry run: analysis of %s complete, nothing published.", path.name
            )

        return result

    def _resolve_target(
        self,
        path: Path,
        package_name: str | None,
        scenario: Scenario | None,
    ) -> tuple[str, Scenario]:
        if package_name is not None and scenario is not None:
            return package_name, scenario

        descriptor = parse_capture_filename(path)
        return (
            package_name if package_name is not None else descriptor.package_name,
            scenario if scenario is not None else descriptor.scenario,
        )
