#!/usr/bin/env bash

# =============================================================================
# verify_network_metrics.sh
#
# Reads messages back off the `network-metrics` Kafka topic so an operator can
# confirm the network analyzer actually published what it reported.
#
# This exists because the Data Storage Subsystem is not implemented yet: until
# something consumes the topic and writes to PostgreSQL, there are no database
# rows to check, and the topic is the only evidence available.
#
# Usage:
#   ./scripts/verify/verify_network_metrics.sh [options]
#   ./scripts/verify/verify_network_metrics.sh --help
#
# Prerequisites: the Kafka container running via
#   docker compose up -d zookeeper kafka
# =============================================================================

set -uo pipefail

# --- Appearance --------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- Defaults ----------------------------------------------------------------
KAFKA_CONTAINER="project_kafka"
TOPIC="network-metrics"
MAX_MESSAGES=10
TIMEOUT_SECONDS=20
FROM_BEGINNING=true
COUNT_ONLY=false

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

FAILED_CHECKS=0

# --- Helpers -----------------------------------------------------------------

print_header() {
    echo ""
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_ok() {
    echo -e "  ${GREEN}[OK]${NC} $1"
}

print_fail() {
    echo -e "  ${RED}[FAIL]${NC} $1"
    FAILED_CHECKS=$((FAILED_CHECKS + 1))
}

print_info() {
    echo -e "  ${YELLOW}[..]${NC} $1"
}

usage() {
    cat <<'EOF'
Read network-metrics messages back off Kafka to verify the analyzer published.

Usage:
  ./scripts/verify/verify_network_metrics.sh [options]

Options:
  --max-messages N   Stop after N messages. Default: 10
  --timeout SECONDS  Give up waiting after this long. Default: 20
  --latest           Show only messages published from now on, instead of
                     replaying the topic from the beginning.
  --count-only       Report how many messages the topic holds, without
                     printing them.
  --topic NAME       Topic to read. Default: network-metrics
  --container NAME   Kafka container name. Default: project_kafka
  -h, --help         Show this help and exit.

Exit status:
  0  the topic exists and the requested check succeeded
  1  the broker or topic was unreachable, or the topic held no messages

Examples:
  # Did anything get published at all?
  ./scripts/verify/verify_network_metrics.sh

  # Watch for a message while running the analyzer in another terminal.
  ./scripts/verify/verify_network_metrics.sh --latest --max-messages 1 --timeout 60

  # How many analyses are on the topic?
  ./scripts/verify/verify_network_metrics.sh --count-only
EOF
}

# --- Argument parsing --------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --max-messages)
            [[ $# -ge 2 ]] || { echo "--max-messages needs a number." >&2; exit 2; }
            MAX_MESSAGES="$2"
            shift 2
            ;;
        --timeout)
            [[ $# -ge 2 ]] || { echo "--timeout needs a number." >&2; exit 2; }
            TIMEOUT_SECONDS="$2"
            shift 2
            ;;
        --latest)
            FROM_BEGINNING=false
            shift
            ;;
        --count-only)
            COUNT_ONLY=true
            shift
            ;;
        --topic)
            [[ $# -ge 2 ]] || { echo "--topic needs a name." >&2; exit 2; }
            TOPIC="$2"
            shift 2
            ;;
        --container)
            [[ $# -ge 2 ]] || { echo "--container needs a name." >&2; exit 2; }
            KAFKA_CONTAINER="$2"
            shift 2
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Run with --help to see the available options." >&2
            exit 2
            ;;
    esac
done

echo -e "${BLUE}"
echo "+----------------------------------------------+"
echo "|        network-metrics topic inspection      |"
echo "+----------------------------------------------+"
echo -e "${NC}"

# --- Broker reachability -----------------------------------------------------

print_header "Broker"

if ! command -v docker >/dev/null 2>&1; then
    print_fail "docker is not on PATH; this script talks to Kafka through the container."
    exit 1
fi

if docker inspect -f '{{.State.Running}}' "$KAFKA_CONTAINER" 2>/dev/null | grep -q "true"; then
    print_ok "Container $KAFKA_CONTAINER is running."
else
    print_fail "Container $KAFKA_CONTAINER is not running."
    echo "      Start it with: docker compose up -d zookeeper kafka"
    exit 1
fi

if docker exec "$KAFKA_CONTAINER" kafka-broker-api-versions \
    --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    print_ok "Broker is accepting connections."
else
    print_fail "Broker did not respond."
    exit 1
fi

# --- Topic existence ---------------------------------------------------------

print_header "Topic"

TOPIC_LIST=$(docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server localhost:9092 --list 2>/dev/null)

if echo "$TOPIC_LIST" | grep -qx "$TOPIC"; then
    print_ok "Topic $TOPIC exists."
else
    print_fail "Topic $TOPIC does not exist yet."
    echo "      It is created automatically the first time the analyzer publishes."
    echo "      Run: ./scripts/network/analyze_pcaps.sh"
    exit 1
fi

# Sum the end offsets across partitions to get the message count. The topic is
# append-only and never compacted, so the end offset is the total published.
OFFSETS=$(docker exec "$KAFKA_CONTAINER" kafka-run-class \
    kafka.tools.GetOffsetShell \
    --bootstrap-server localhost:9092 \
    --topic "$TOPIC" 2>/dev/null)

MESSAGE_COUNT=$(echo "$OFFSETS" | awk -F: '{ total += $3 } END { print total + 0 }')
print_info "Topic holds $MESSAGE_COUNT message(s)."

if [[ "$COUNT_ONLY" == true ]]; then
    print_header "Result"
    if [[ "$MESSAGE_COUNT" -gt 0 ]]; then
        print_ok "$MESSAGE_COUNT analysis record(s) published."
        exit 0
    fi
    print_fail "The topic is empty."
    exit 1
fi

# --- Consume -----------------------------------------------------------------

print_header "Messages"

CONSUMER_ARGS=(
    --bootstrap-server localhost:9092
    --topic "$TOPIC"
    --max-messages "$MAX_MESSAGES"
    --property print.key=true
    --property key.separator=" | "
)
if [[ "$FROM_BEGINNING" == true ]]; then
    CONSUMER_ARGS+=(--from-beginning)
    print_info "Replaying up to $MAX_MESSAGES message(s) from the beginning..."
else
    print_info "Waiting up to ${TIMEOUT_SECONDS}s for new message(s)..."
fi

MESSAGES=$(timeout "$TIMEOUT_SECONDS" docker exec "$KAFKA_CONTAINER" \
    kafka-console-consumer "${CONSUMER_ARGS[@]}" 2>/dev/null)

if [[ -z "$MESSAGES" ]]; then
    print_fail "No messages were read."
    if [[ "$FROM_BEGINNING" != true ]]; then
        echo "      Nothing was published during the wait. Run the analyzer, or"
        echo "      drop --latest to replay what is already on the topic."
    fi
    exit 1
fi

# Pretty-print each record as "<key> | <indented json>" when a Python is
# available, and fall back to the raw line when it is not.
PYTHON_BIN="${ANALYZER_PYTHON:-}"
if [[ -z "$PYTHON_BIN" ]]; then
    for candidate in \
        "$PROJECT_ROOT/.venv-analyzer/bin/python" \
        "$PROJECT_ROOT/.venv/bin/python" \
        "$PROJECT_ROOT/venv/bin/python"; do
        [[ -x "$candidate" ]] && PYTHON_BIN="$candidate" && break
    done
fi
[[ -z "$PYTHON_BIN" ]] && PYTHON_BIN="$(command -v python3 || true)"

RECEIVED=0
while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    RECEIVED=$((RECEIVED + 1))
    key="${line%% | *}"
    value="${line#* | }"

    echo ""
    echo -e "  ${GREEN}#$RECEIVED${NC} key=${key}"
    if [[ -n "$PYTHON_BIN" ]]; then
        if ! printf '%s' "$value" \
            | "$PYTHON_BIN" -m json.tool --indent 2 2>/dev/null \
            | sed 's/^/      /'; then
            printf '      %s\n' "$value"
        fi
    else
        printf '      %s\n' "$value"
    fi
done <<< "$MESSAGES"

# --- Summary -----------------------------------------------------------------

print_header "Result"

if [[ $RECEIVED -gt 0 ]]; then
    print_ok "Read $RECEIVED message(s) from $TOPIC."
    echo ""
    echo "  These are the records the Data Storage Subsystem will consume and"
    echo "  insert into the network_metrics table."
    exit 0
fi

print_fail "No messages could be parsed."
exit 1
