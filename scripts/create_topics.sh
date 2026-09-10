#!/usr/bin/env bash

# =============================================================================
# create_topics.sh
#
# Creates the three PlayPulse topics with a chosen partition count, and reports
# any that are narrower than that.
#
# Why bother, when the broker auto-creates topics? Because auto-created topics
# get one partition, and a topic's partition count can never be reduced. One
# partition caps every consumer group at one active member, so the day the
# reviews burst outgrows a single consumer, the fix is a topic rebuild rather
# than a second container. Creating them wide up front costs nothing.
#
# The compose `kafka-init` service does the same thing on every `up`. This
# script exists for the case that one cannot handle: widening topics that
# already exist.
#
# Usage:
#   ./scripts/create_topics.sh [options]
#   ./scripts/create_topics.sh --help
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
BOOTSTRAP="localhost:9092"
PARTITIONS=3
TOPICS=("app-stats" "reviews" "network-metrics")
ALTER=false

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
Create the PlayPulse Kafka topics with a chosen partition count.

Usage:
  ./scripts/create_topics.sh [options]

Options:
  --partitions N     Partition count for new topics. Default: 3
  --alter            Also widen existing topics that have fewer partitions.
                     Off by default because it rewrites the partitioning of a
                     live topic; see the warning it prints.
  --topic NAME       Only act on this topic; repeatable. Default: all three.
  --container NAME   Kafka container name. Default: project_kafka
  -h, --help         Show this help and exit.

Exit status:
  0  every requested topic exists with at least the requested partition count
  1  the broker was unreachable, or a topic could not be created

Examples:
  # First-time setup, before anything produces.
  ./scripts/create_topics.sh

  # Widen reviews for a second consumer container.
  ./scripts/create_topics.sh --topic reviews --partitions 6 --alter
EOF
}

# --- Argument parsing --------------------------------------------------------

explicit_topics=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --partitions)
            [[ $# -ge 2 ]] || { echo "--partitions needs a number." >&2; exit 2; }
            PARTITIONS="$2"
            shift 2
            ;;
        --alter)
            ALTER=true
            shift
            ;;
        --topic)
            [[ $# -ge 2 ]] || { echo "--topic needs a name." >&2; exit 2; }
            explicit_topics+=("$2")
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

if [[ ${#explicit_topics[@]} -gt 0 ]]; then
    TOPICS=("${explicit_topics[@]}")
fi

if ! [[ "$PARTITIONS" =~ ^[0-9]+$ ]] || [[ "$PARTITIONS" -lt 1 ]]; then
    echo "--partitions must be a positive integer." >&2
    exit 2
fi

echo -e "${BLUE}"
echo "+----------------------------------------------+"
echo "|             Kafka topic setup                |"
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
    --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; then
    print_ok "Broker is accepting connections."
else
    print_fail "Broker did not respond."
    exit 1
fi

# --- Create / inspect --------------------------------------------------------

partition_count() {
    docker exec "$KAFKA_CONTAINER" kafka-topics \
        --bootstrap-server "$BOOTSTRAP" --describe --topic "$1" 2>/dev/null \
        | grep -c $'\tPartition:'
}

topic_exists() {
    docker exec "$KAFKA_CONTAINER" kafka-topics \
        --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | grep -qx "$1"
}

print_header "Topics (target: ${PARTITIONS} partition(s))"

for topic in "${TOPICS[@]}"; do
    if ! topic_exists "$topic"; then
        if docker exec "$KAFKA_CONTAINER" kafka-topics \
            --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
            --topic "$topic" --partitions "$PARTITIONS" \
            --replication-factor 1 >/dev/null 2>&1; then
            print_ok "Created $topic with $PARTITIONS partition(s)."
        else
            print_fail "Could not create $topic."
        fi
        continue
    fi

    existing=$(partition_count "$topic")
    if [[ "$existing" -ge "$PARTITIONS" ]]; then
        print_ok "$topic already has $existing partition(s)."
        continue
    fi

    if [[ "$ALTER" != true ]]; then
        print_info "$topic has $existing partition(s), fewer than $PARTITIONS."
        echo "      Re-run with --alter to widen it. Partition counts can be"
        echo "      increased but never reduced, so this is one-way."
        continue
    fi

    echo ""
    echo -e "  ${YELLOW}Widening $topic from $existing to $PARTITIONS partition(s).${NC}"
    echo "  Messages already on the topic stay where they are, and new messages"
    echo "  for a given key may land on a different partition than before. That"
    echo "  breaks per-key ordering across the change."
    echo ""
    echo "  Safe for these three topics, because none of the consumers depend on"
    echo "  it: every write is keyed by a natural id (review_id, analysis_id,"
    echo "  application_id + crawled_at) and reviews carry a monotonic"
    echo "  last_synced_at guard, so an out-of-order redelivery is a no-op"
    echo "  rather than a lost update."
    echo ""

    if docker exec "$KAFKA_CONTAINER" kafka-topics \
        --bootstrap-server "$BOOTSTRAP" --alter \
        --topic "$topic" --partitions "$PARTITIONS" >/dev/null 2>&1; then
        print_ok "$topic now has $(partition_count "$topic") partition(s)."
    else
        print_fail "Could not widen $topic."
    fi
done

# --- Summary -----------------------------------------------------------------

print_header "Result"

if [[ $FAILED_CHECKS -eq 0 ]]; then
    print_ok "Topic setup complete."
    echo ""
    docker exec "$KAFKA_CONTAINER" kafka-topics \
        --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null \
        | grep -v '^__' | sed 's/^/      /'
    exit 0
fi

print_fail "$FAILED_CHECKS topic operation(s) failed."
exit 1
