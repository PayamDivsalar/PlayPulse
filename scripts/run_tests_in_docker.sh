#!/usr/bin/env bash

# =============================================================================
# run_tests_in_docker.sh
#
# Bring up the shared Docker infrastructure (Postgres, Kafka, app-api), then
# run every subsystem test service on the compose network via the ``test``
# profile. Collects all exit codes and prints a PASS / FAIL / SKIPPED summary.
#
# Usage (from repo root):
#   ./scripts/run_tests_in_docker.sh
#   ./scripts/run_tests_in_docker.sh --no-build
#
# Prerequisites: root .env present; Docker Engine + Compose plugin.
#
# Notes:
#   - crawler-tests-live includes Play Store hits (outbound internet); Kafka /
#     App API live cases in that suite still FAIL if the compose network is down.
#   - network-analyzer-tests-live is compose-network only (never SKIPPED for
#     "missing internet").
#   - network-analyzer-tests-oracle needs tshark (Dockerfile.test).
#   - sentiment-* is never cold-built by this script; if playpulse-sentiment:latest
#     is missing, those rows are SKIPPED.
# =============================================================================

set -uo pipefail

# --- Appearance --------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# --- Paths -------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

# --- Defaults ----------------------------------------------------------------
NO_BUILD=false

POSTGRES_CONTAINER="project_postgres"
KAFKA_CONTAINER="project_kafka"
APP_API_CONTAINER="project_app_api"
KAFKA_INIT_CONTAINER="project_kafka_init"
SENTIMENT_IMAGE="playpulse-sentiment:latest"

# Full sequence (continue on failure; summary at the end).
TEST_SERVICES=(
    app-api-tests
    crawler-tests
    crawler-tests-live
    network-analyzer-tests
    network-analyzer-tests-live
    network-analyzer-tests-oracle
    storage-consumer-tests
    storage-consumer-tests-integration
    sentiment-tests
    sentiment-tests-integration
)

# Parallel arrays: name, exit code (-1 = skipped before run), result label, note
declare -a RESULT_NAMES=()
declare -a RESULT_CODES=()
declare -a RESULT_LABELS=()
declare -a RESULT_NOTES=()

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
}

print_skip() {
    echo -e "  ${CYAN}[SKIP]${NC} $1"
}

print_info() {
    echo -e "  ${YELLOW}[..]${NC} $1"
}

die() {
    echo -e "${RED}$1${NC}" >&2
    exit 1
}

usage() {
    cat <<'EOF'
Run PlayPulse subsystem tests inside the Docker compose network.

Usage:
  ./scripts/run_tests_in_docker.sh [options]

Options:
  --no-build   Reuse images already built (skip --build on compose up/run)
  -h, --help   Show this help

Starts postgres, zookeeper, kafka, kafka-init, and app-api, waits until they
are ready, then runs each *-tests service with:
  docker compose --profile test run --rm <service>

Summary labels:
  PASS     suite exited 0
  FAIL     suite failed for a real reason
  SKIPPED  Play Store unreachable (crawler-tests-live), tshark missing
           (oracle), or sentiment image not built (this script never
           cold-builds sentiment)
EOF
}

wait_postgres() {
    local retries=30
    print_info "Waiting for Postgres ($POSTGRES_CONTAINER)..."
    until docker exec "$POSTGRES_CONTAINER" \
        pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || return 1
        sleep 2
    done
    return 0
}

wait_kafka() {
    local retries=45
    print_info "Waiting for Kafka ($KAFKA_CONTAINER)..."
    until docker exec "$KAFKA_CONTAINER" \
        kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || return 1
        sleep 2
    done
    return 0
}

wait_kafka_init() {
    local retries=30
    print_info "Waiting for kafka-init to finish..."
    until [[ "$(docker inspect -f '{{.State.Status}}' "$KAFKA_INIT_CONTAINER" 2>/dev/null || echo missing)" == "exited" ]]; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || return 1
        sleep 2
    done
    local code
    code="$(docker inspect -f '{{.State.ExitCode}}' "$KAFKA_INIT_CONTAINER")"
    [[ "$code" == "0" ]] || return 1
    return 0
}

wait_app_api() {
    local retries=60
    print_info "Waiting for app-api health..."
    until [[ "$(docker inspect -f '{{.State.Health.Status}}' "$APP_API_CONTAINER" 2>/dev/null || echo starting)" == "healthy" ]]; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || return 1
        sleep 2
    done
    return 0
}

is_sentiment_service() {
    case "$1" in
        sentiment-tests|sentiment-tests-integration) return 0 ;;
        *) return 1 ;;
    esac
}

is_playstore_live_service() {
    # Only this suite hits the public Play Store. Other *-live services exercise
    # Kafka / App API on the compose network and must not be SKIPPED as "no internet".
    [[ "$1" == "crawler-tests-live" ]]
}

is_oracle_service() {
    [[ "$1" == "network-analyzer-tests-oracle" ]]
}

# Shared tag with tools-profile ``sentiment`` (see compose ``image:``).
sentiment_image_ready() {
    docker image inspect "$SENTIMENT_IMAGE" >/dev/null 2>&1
}

# True when the failure looks like Play Store egress/DNS, not Kafka/App API.
log_looks_like_missing_play_store() {
    local log="$1"
    grep -qiE 'google_play_scraper|play\.google\.com|googleapis\.com' "$log" \
        || return 1
    grep -qiE \
        'Failed to establish a new connection|Name or service not known|Temporary failure in name resolution|Network is unreachable|Network unreachable|gaierror|nodename nor servname|Could not resolve host|SSLError|URLError|ConnectionError|Max retries exceeded|Timed out|Read timed out|Connection reset|Connection refused|HTTPSConnectionPool' \
        "$log"
}

log_looks_like_missing_tshark() {
    local log="$1"
    # unittest.skipUnless → pytest "skipped"; or binary missing at runtime.
    grep -qiE \
        'tshark is not installed|skipped.*tshark|tshark: not found|No such file or directory: .*tshark|executable.*tshark' \
        "$log" \
        || { grep -qE 'skipped=[1-9]' "$log" && grep -qiE 'tshark' "$log"; }
}

record_result() {
    local name="$1" code="$2" label="$3" note="${4:-}"
    RESULT_NAMES+=("$name")
    RESULT_CODES+=("$code")
    RESULT_LABELS+=("$label")
    RESULT_NOTES+=("$note")
}

classify_and_record() {
    local service="$1" code="$2" log="$3"
    if [[ $code -eq 0 ]]; then
        # Oracle suite may exit 0 with every test skipped when tshark is absent
        # (e.g. wrong image). Dockerfile.test normally has tshark.
        if is_oracle_service "$service" && log_looks_like_missing_tshark "$log"; then
            record_result "$service" "$code" "SKIPPED" "requires tshark"
            print_skip "$service — requires tshark"
            return
        fi
        record_result "$service" "$code" "PASS" ""
        print_ok "$service exited 0"
        return
    fi
    if is_playstore_live_service "$service" && log_looks_like_missing_play_store "$log"; then
        record_result "$service" "$code" "SKIPPED" "requires outbound internet (Play Store)"
        print_skip "$service — requires outbound internet (Play Store)"
        return
    fi
    if is_oracle_service "$service" && log_looks_like_missing_tshark "$log"; then
        record_result "$service" "$code" "SKIPPED" "requires tshark"
        print_skip "$service — requires tshark"
        return
    fi
    record_result "$service" "$code" "FAIL" ""
    print_fail "$service exited $code"
}

run_suite() {
    local service="$1"
    local log
    local code
    local run_build_flag=()
    print_info "Running $service..."
    if is_sentiment_service "$service"; then
        # Never cold-build torch/HF here — operator must supply the image.
        if ! sentiment_image_ready; then
            record_result "$service" -1 "SKIPPED" \
                "sentiment image not built (skip cold build)"
            print_skip "$service — sentiment image not built (skip cold build)"
            return
        fi
        run_build_flag=()
    else
        run_build_flag=("${BUILD_FLAG[@]}")
    fi
    log="$(mktemp)"
    "${COMPOSE[@]}" --profile test run --rm "${run_build_flag[@]}" "$service" \
        2>&1 | tee "$log"
    code=${PIPESTATUS[0]}
    classify_and_record "$service" "$code" "$log"
    rm -f "$log"
}

# --- Args --------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-build) NO_BUILD=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "Unknown option: $1 (try --help)" ;;
    esac
done

cd "$PROJECT_ROOT"

if [[ ! -f "$ENV_FILE" ]]; then
    die "Missing $ENV_FILE — copy .env.example to .env first."
fi

# shellcheck disable=SC1090
set -a
source "$ENV_FILE"
set +a

COMPOSE=(docker compose)
BUILD_FLAG=()
if [[ "$NO_BUILD" != true ]]; then
    BUILD_FLAG=(--build)
fi

echo -e "${BLUE}"
echo "╔══════════════════════════════════════════════╗"
echo "║     PlayPulse tests (Docker network)         ║"
echo "╚══════════════════════════════════════════════╝"
echo -e "${NC}"

print_info "crawler-tests-live: Play Store needs outbound internet."
print_info "network-analyzer-tests-live: Kafka/App API on compose network only."
print_info "network-analyzer-tests-oracle needs tshark (Dockerfile.test)."
print_info "sentiment-* skipped unless ${SENTIMENT_IMAGE} exists."

# =============================================================================
# 1. Base infrastructure + app-api
# =============================================================================
print_header "Infrastructure"

print_info "Starting postgres zookeeper kafka kafka-init app-api..."
"${COMPOSE[@]}" up -d "${BUILD_FLAG[@]}" \
    postgres zookeeper kafka app-api

# Always recreate the one-shot topic initializer so a stale exited container
# cannot mask a wiped Kafka data dir (topics would otherwise be missing).
print_info "Recreating kafka-init..."
"${COMPOSE[@]}" up -d --force-recreate --no-deps kafka-init

if wait_postgres; then
    print_ok "Postgres is ready."
else
    die "Postgres did not become ready. Check: docker compose logs postgres"
fi

if wait_kafka; then
    print_ok "Kafka is ready."
else
    die "Kafka did not become ready. Check: docker compose logs kafka"
fi

if wait_kafka_init; then
    print_ok "kafka-init finished successfully."
else
    die "kafka-init failed. Check: docker compose logs kafka-init"
fi

if wait_app_api; then
    print_ok "app-api is healthy."
else
    die "app-api did not become healthy. Check: docker compose logs app-api"
fi

# =============================================================================
# 2. Run each test service (continue on failure)
# =============================================================================
print_header "Test suites"

for service in "${TEST_SERVICES[@]}"; do
    run_suite "$service"
done

# =============================================================================
# 3. Summary
# =============================================================================
print_header "Summary"

printf "  %-42s %-8s %s\n" "subsystem" "result" "note"
printf "  %-42s %-8s %s\n" "------------------------------------------" "--------" "----"

failed=0
skipped=0
for i in "${!RESULT_NAMES[@]}"; do
    name="${RESULT_NAMES[$i]}"
    label="${RESULT_LABELS[$i]}"
    note="${RESULT_NOTES[$i]}"
    case "$label" in
        PASS)
            printf "  %-42s ${GREEN}%-8s${NC} %s\n" "$name" "$label" "$note"
            ;;
        SKIPPED)
            printf "  %-42s ${CYAN}%-8s${NC} %s\n" "$name" "$label" "$note"
            skipped=$((skipped + 1))
            ;;
        *)
            printf "  %-42s ${RED}%-8s${NC} %s\n" "$name" "$label" "$note"
            failed=$((failed + 1))
            ;;
    esac
done

echo ""
if [[ $failed -eq 0 ]]; then
    if [[ $skipped -gt 0 ]]; then
        echo -e "  ${GREEN}[OK]${NC} Required suites passed ($skipped skipped for external prerequisites)."
    else
        echo -e "  ${GREEN}[OK]${NC} All Docker test suites passed."
    fi
    exit 0
else
    echo -e "  ${RED}[FAIL]${NC} $failed suite(s) failed (skipped=$skipped)."
    exit 1
fi
