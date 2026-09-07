"""Command-line entry point and composition root for the network analyzer.

Analyzes a single capture file. Batch ingestion over a directory is the job of
``scripts/analyze_pcaps.sh``, which drives this command once per file and uses
its exit code to decide where the file goes next.

Usage::

    python -m network_analyzer.main --file data/pcap/inbox/com.whatsapp__upload__20260907T141500.pcap
    python -m network_analyzer.main --file capture.pcap --package com.whatsapp --scenario UPLOAD
    python -m network_analyzer.main --file capture.pcap --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import requests
from kafka.errors import KafkaError

from network_analyzer.analyzer_service import AnalyzerService
from network_analyzer.app_registry_client import AppRegistryClient
from network_analyzer.config import Settings, load_settings
from network_analyzer.exceptions import (
    AnalyzerConfigError,
    ApplicationNotEligibleError,
    FilenameConventionError,
    NetworkAnalyzerError,
    PcapReadError,
    RegistryRequestError,
)
from network_analyzer.kafka_publisher import NetworkMetricsPublisher
from network_analyzer.models import AnalysisResult, Scenario

logger = logging.getLogger(__name__)

# Exit codes. The batch script depends on these: it retries a transport failure
# by leaving the capture in the inbox, but quarantines an input the analyzer
# will never accept no matter how often it is retried.
EXIT_OK = 0
EXIT_ERROR = 1
# 2 is reserved: argparse uses it for usage errors.
EXIT_INPUT_REJECTED = 3
EXIT_CAPTURE_UNREADABLE = 4
EXIT_TRANSPORT_FAILURE = 5


def _scenario_arg(raw: str) -> Scenario:
    try:
        return Scenario.parse(raw)
    except NetworkAnalyzerError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m network_analyzer.main",
        description=(
            "Analyze one PCAPdroid capture and publish its network metrics to "
            "the network-metrics Kafka topic."
        ),
        epilog=(
            "Captures named <package>__<upload|download>__<YYYYMMDDTHHMMSS>.pcap "
            "need no --package or --scenario."
        ),
    )
    parser.add_argument(
        "--file",
        required=True,
        type=Path,
        metavar="PATH",
        help="Capture file to analyze (.pcap or .pcapng).",
    )
    parser.add_argument(
        "--package",
        metavar="NAME",
        help="Application package name, overriding what the filename encodes.",
    )
    parser.add_argument(
        "--scenario",
        type=_scenario_arg,
        metavar="{upload,download}",
        help="Capture scenario, overriding what the filename encodes.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and print the metrics without publishing anything.",
    )
    parser.add_argument(
        "--skip-registry-check",
        action="store_true",
        help=(
            "Do not verify the package against the App API. The storage "
            "subsystem will reject the record if the package is unknown."
        ),
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser


def _configure_logging(verbose: bool) -> None:
    """Send logs to stderr, keeping stdout clean for the metrics report."""

    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )


def _build_service(
    settings: Settings | None, *, dry_run: bool, skip_registry_check: bool
) -> tuple[AnalyzerService, NetworkMetricsPublisher | None]:
    """Wire the object graph, omitting collaborators that are switched off.

    Returns the publisher alongside the service so the caller can close it.
    """

    registry_client = None
    publisher = None

    if not skip_registry_check:
        assert settings is not None
        registry_client = AppRegistryClient(
            base_url=settings.app_api_base_url, settings=settings
        )

    if not dry_run:
        assert settings is not None
        publisher = NetworkMetricsPublisher(
            bootstrap_servers=settings.kafka_bootstrap_servers, settings=settings
        )

    service = AnalyzerService(registry_client=registry_client, publisher=publisher)
    return service, publisher


def _format_report(result: AnalysisResult, *, published: bool) -> str:
    metrics = result.metrics

    if metrics.rtt_handshake_ms is None:
        rtt_line = "n/a (no complete handshake in this capture)"
    else:
        handshakes = metrics.handshake_sample_count
        plural = "handshake" if handshakes == 1 else "handshakes"
        rtt_line = f"{metrics.rtt_handshake_ms:.2f} ms (mean of {handshakes} {plural})"

    lines = [
        "",
        f"Capture          {result.source_pcap_filename}",
        f"Application      {result.package_name}",
        f"Scenario         {result.scenario.value}",
        f"Analysis ID      {result.analysis_id}",
        f"Analyzed at      {result.analyzed_at.isoformat()}",
        "",
        "Network quality and stability (TCP only)",
        f"  Handshake RTT       {rtt_line}",
        f"  Retransmissions     {metrics.retransmission_count:,}",
        f"  Zero-window events  {metrics.zero_window_count:,}",
        f"  TCP resets          {metrics.tcp_reset_count:,}",
        "",
        "Data consumption efficiency (all IP traffic)",
        f"  Packets             {metrics.packet_count:,}",
        f"  Total bytes         {metrics.bytes_transferred_total:,}",
        f"  Payload bytes       {metrics.bytes_payload_total:,}",
        f"  Overhead ratio      {metrics.overhead_ratio * 100:.4f} %",
        "",
    ]

    if published:
        lines.append("Published to the network-metrics topic.")
    else:
        lines.append("Dry run: nothing was published.")
    lines.append("")

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    """Analyze one capture file. Returns the process exit code."""

    args = _build_parser().parse_args(argv)
    _configure_logging(args.verbose)

    # A dry run that also skips the registry talks to nothing, so it must not
    # demand connection settings the operator has no use for.
    needs_settings = not (args.dry_run and args.skip_registry_check)

    publisher: NetworkMetricsPublisher | None = None
    try:
        settings = load_settings() if needs_settings else None
        service, publisher = _build_service(
            settings,
            dry_run=args.dry_run,
            skip_registry_check=args.skip_registry_check,
        )
        result = service.analyze_file(
            args.file, package_name=args.package, scenario=args.scenario
        )
    except AnalyzerConfigError as exc:
        logger.error("Configuration error: %s", exc)
        return EXIT_ERROR
    except RegistryRequestError as exc:
        # Not EXIT_TRANSPORT_FAILURE: the registry answered, and it said no.
        # Leaving the capture in the inbox to retry would loop forever.
        logger.error("%s", exc)
        return EXIT_ERROR
    except FilenameConventionError as exc:
        logger.error("%s", exc)
        return EXIT_INPUT_REJECTED
    except ApplicationNotEligibleError as exc:
        logger.error("%s", exc)
        return EXIT_INPUT_REJECTED
    except PcapReadError as exc:
        logger.error("%s", exc)
        return EXIT_CAPTURE_UNREADABLE
    except (requests.RequestException, KafkaError) as exc:
        logger.error(
            "Could not reach a required service (%s). The capture was left "
            "unprocessed and can be retried.",
            exc,
            exc_info=args.verbose,
        )
        return EXIT_TRANSPORT_FAILURE
    except KeyboardInterrupt:
        logger.warning("Interrupted.")
        return EXIT_ERROR
    except Exception:
        logger.error("Unexpected failure while analyzing the capture.", exc_info=True)
        return EXIT_ERROR
    finally:
        if publisher is not None:
            try:
                publisher.close()
            except KafkaError:
                logger.error("Failed to close the Kafka producer cleanly.")

    print(_format_report(result, published=not args.dry_run))
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
