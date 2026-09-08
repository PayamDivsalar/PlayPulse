#!/usr/bin/env bash
# =============================================================================
# docker-entrypoint.sh
#
# Dispatcher for the network-analyzer image. One image, two operating modes —
# the same pattern official images (Postgres, MySQL, …) use for a default job
# that can still be overridden with explicit arguments.
#
#   --batch [analyze_pcaps.sh options…]
#       Process every capture under /data/pcap/inbox (the compose bind mount).
#       This is the image default (see Dockerfile CMD).
#
#   anything else
#       Passed straight through to `python -m network_analyzer.main`, e.g.
#         --file /data/pcap/inbox/<name>.pcap
#         --file … --dry-run --skip-registry-check
#         --help
# =============================================================================

set -euo pipefail

readonly PCAP_ROOT="${ANALYZER_PCAP_ROOT:-/data/pcap}"

print_usage() {
    cat <<EOF
network-analyzer container

Usage:
  docker compose run --rm network-analyzer
      Batch-analyze every capture in ${PCAP_ROOT}/inbox (default).

  docker compose run --rm network-analyzer --batch [options]
      Same as the default, with optional flags forwarded to analyze_pcaps.sh
      (for example --dry-run --skip-registry-check --keep --verbose).

  docker compose run --rm network-analyzer --file ${PCAP_ROOT}/inbox/<name>.pcap [options]
      Analyze one capture via network_analyzer.main.

  docker compose run --rm network-analyzer --help
      Show the single-file analyzer help.

Captures are bind-mounted from the host at ./data/pcap -> ${PCAP_ROOT}.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && [[ $# -eq 1 ]]; then
    print_usage
    echo ""
    echo "----- single-file analyzer (--file / …) -----"
    echo ""
    exec python -m network_analyzer.main --help
fi

if [[ $# -eq 0 || "${1:-}" == "--batch" ]]; then
    if [[ "${1:-}" == "--batch" ]]; then
        shift
    fi
    exec analyze_pcaps.sh \
        --inbox "${PCAP_ROOT}/inbox" \
        --processed "${PCAP_ROOT}/processed" \
        --failed "${PCAP_ROOT}/failed" \
        "$@"
fi

exec python -m network_analyzer.main "$@"
