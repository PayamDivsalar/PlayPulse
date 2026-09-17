#!/usr/bin/env bash

# =============================================================================
# run_tests_in_docker.sh
#
# Bring up an *isolated* Docker test stack (Postgres, Kafka, app-api), then
# run every subsystem test service via the ``test`` profile. Collects exit
# codes and prints a PASS / FAIL / SKIPPED summary.
#
# Isolation (does not touch the live / prod compose stack):
#   - COMPOSE_PROJECT_NAME=playpulse_test  → separate network + volumes
#   - --env-file .env.test                 → not the live root .env
#   - -f docker-compose.test.yml           → no fixed container names / host ports
#
# Usage (from repo root):
#   ./scripts/run_tests_in_docker.sh
#   ./scripts/run_tests_in_docker.sh --no-build
#   ./scripts/run_tests_in_docker.sh --keep          # leave stack up after run
#   ./scripts/run_tests_in_docker.sh --down          # teardown only
#
# If .env.test is missing, it is created once from .env.test.example (never
# overwritten). By default the playpulse_test stack (containers + volumes) is
# torn down when the script exits — including after failures or an early abort
# once infra was started. Use --keep to iterate without rebuilding infra.
# Full guide: docs/docker-tests.md
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
ENV_FILE="$PROJECT_ROOT/.env.test"
ENV_EXAMPLE="$PROJECT_ROOT/.env.test.example"
COMPOSE_FILE="$PROJECT_ROOT/docker-compose.yml"
COMPOSE_TEST_FILE="$PROJECT_ROOT/docker-compose.test.yml"

# Fixed project name: keeps test volumes/networks away from the live stack.
COMPOSE_PROJECT="playpulse_test"
SENTIMENT_IMAGE="playpulse-sentiment:latest"

# --- Defaults ----------------------------------------------------------------
NO_BUILD=false
DO_DOWN=false
KEEP=false
# Set true once a full test run has started infra (so EXIT tears it down).
TEARDOWN_ON_EXIT=false

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
    cat <<EOF
Run PlayPulse subsystem tests on an isolated Docker Compose project.

Usage:
  ./scripts/run_tests_in_docker.sh [options]

Options:
  --no-build   Reuse images already built (skip --build on compose up/run)
  --keep       Leave the playpulse_test stack running after the run
  --down       Tear down the playpulse_test stack (and volumes) and exit
  -h, --help   Show this help

By default the test stack is removed (containers + volumes) when this script
exits. Use --keep for faster local re-runs, then --down when finished.

If .env.test is missing, it is created from .env.test.example (never overwritten).

Always uses:
  project     ${COMPOSE_PROJECT}
  env file    .env.test
  compose     docker-compose.yml + docker-compose.test.yml

This never uses the live root .env or fixed names like project_postgres.
Do not run on a production host as a substitute for health checks — use
scripts/infra/check_infra.sh there. See docs/docker-tests.md.

Summary labels:
  PASS     suite exited 0
  FAIL     suite failed for a real reason
  SKIPPED  Play Store unreachable (crawler-tests-live), tshark missing
           (oracle), or sentiment image not built (this script never
           cold-builds sentiment)
EOF
}

# Resolve a running (or recently created) container id for a compose service.
service_cid() {
    local service="$1"
    "${COMPOSE[@]}" ps -aq "$service" 2>/dev/null | head -n1
}

wait_postgres() {
    local retries=30
    print_info "Waiting for Postgres (compose service postgres)..."
    until "${COMPOSE[@]}" exec -T postgres \
        pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || return 1
        sleep 2
    done
    return 0
}

wait_kafka() {
    local retries=45
    print_info "Waiting for Kafka (compose service kafka)..."
    # Inside the broker container the host listener is still :9092 even when
    # no host port is published (docker-compose.test.yml clears ports).
    until "${COMPOSE[@]}" exec -T kafka \
        kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || return 1
        sleep 2
    done
    return 0
}

wait_kafka_init() {
    local retries=30
    local cid=""
    local status=""
    local code=""
    print_info "Waiting for kafka-init to finish..."
    until cid="$(service_cid kafka-init)" && [[ -n "$cid" ]]; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || return 1
        sleep 2
    done
    until [[ "$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || echo missing)" == "exited" ]]; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || return 1
        sleep 2
        cid="$(service_cid kafka-init)"
        [[ -n "$cid" ]] || return 1
    done
    code="$(docker inspect -f '{{.State.ExitCode}}' "$cid")"
    [[ "$code" == "0" ]] || return 1
    return 0
}

wait_app_api() {
    local retries=60
    local cid=""
    print_info "Waiting for app-api health..."
    until cid="$(service_cid app-api)" && [[ -n "$cid" ]] \
        && [[ "$(docker inspect -f '{{.State.Health.Status}}' "$cid" 2>/dev/null || echo starting)" == "healthy" ]]; do
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

sentiment_image_ready() {
    docker image inspect "$SENTIMENT_IMAGE" >/dev/null 2>&1
}

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

down_test_stack() {
    print_header "Teardown"
    print_info "Stopping project ${COMPOSE_PROJECT} (including volumes)..."
    "${COMPOSE[@]}" --profile test down -v --remove-orphans
    print_ok "Test stack removed (volumes deleted)."
}

# Tear down on any exit after infra was started, unless --keep.
# Preserves the script's exit code (PASS/FAIL summary or die()).
cleanup_on_exit() {
    local ec=$?
    trap - EXIT
    if [[ "$KEEP" != true && "$TEARDOWN_ON_EXIT" == true ]]; then
        down_test_stack || true
    elif [[ "$KEEP" == true && "$TEARDOWN_ON_EXIT" == true ]]; then
        print_info "Leaving playpulse_test running (--keep). Tear down later with: ./scripts/run_tests_in_docker.sh --down"
    fi
    exit "$ec"
}

# --- Args --------------------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-build) NO_BUILD=true; shift ;;
        --keep) KEEP=true; shift ;;
        --down) DO_DOWN=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *) die "Unknown option: $1 (try --help)" ;;
    esac
done

cd "$PROJECT_ROOT"

[[ -f "$COMPOSE_FILE" ]] || die "Missing $COMPOSE_FILE"
[[ -f "$COMPOSE_TEST_FILE" ]] || die "Missing $COMPOSE_TEST_FILE"

if [[ ! -f "$ENV_FILE" ]]; then
    [[ -f "$ENV_EXAMPLE" ]] || die "Missing $ENV_EXAMPLE — cannot create .env.test."
    cp "$ENV_EXAMPLE" "$ENV_FILE"
    print_info "Created .env.test from .env.test.example (existing file is never overwritten)."
fi

# shellcheck disable=SC1090
set -a
source "$ENV_FILE"
set +a

: "${POSTGRES_USER:?POSTGRES_USER must be set in .env.test}"
: "${POSTGRES_DB:?POSTGRES_DB must be set in .env.test}"

COMPOSE=(
    docker compose
    -p "$COMPOSE_PROJECT"
    --env-file "$ENV_FILE"
    -f "$COMPOSE_FILE"
    -f "$COMPOSE_TEST_FILE"
)

BUILD_FLAG=()
if [[ "$NO_BUILD" != true ]]; then
    BUILD_FLAG=(--build)
fi

if [[ "$DO_DOWN" == true ]]; then
    if [[ "$KEEP" == true ]]; then
        die "Cannot combine --down and --keep"
    fi
    down_test_stack
    exit 0
fi

echo -e "${BLUE}"
echo "╔══════════════════════════════════════════════╗"
echo "║     PlayPulse tests (isolated Docker)        ║"
echo "╚══════════════════════════════════════════════╝"
echo -e "${NC}"

print_info "Compose project: ${COMPOSE_PROJECT} (not the live stack)"
print_info "Env file: .env.test"
if [[ "$KEEP" == true ]]; then
    print_info "Stack will be left running after the run (--keep)."
else
    print_info "Stack will be torn down automatically when this script exits."
fi
print_info "crawler-tests-live: Play Store needs outbound internet."
print_info "network-analyzer-tests-live: Kafka/App API on compose network only."
print_info "network-analyzer-tests-oracle needs tshark (Dockerfile.test)."
print_info "sentiment-* skipped unless ${SENTIMENT_IMAGE} exists."

# =============================================================================
# 1. Base infrastructure + app-api
# =============================================================================
print_header "Infrastructure"

# From here on, EXIT tears the test stack down (unless --keep).
TEARDOWN_ON_EXIT=true
trap cleanup_on_exit EXIT

print_info "Starting postgres zookeeper kafka app-api..."
"${COMPOSE[@]}" up -d "${BUILD_FLAG[@]}" \
    postgres zookeeper kafka app-api

# Always recreate the one-shot topic initializer so a stale exited container
# cannot mask a wiped Kafka data dir (topics would otherwise be missing).
print_info "Recreating kafka-init..."
"${COMPOSE[@]}" up -d --force-recreate --no-deps kafka-init

if wait_postgres; then
    print_ok "Postgres is ready."
else
    die "Postgres did not become ready. Check: docker compose -p ${COMPOSE_PROJECT} logs postgres"
fi

if wait_kafka; then
    print_ok "Kafka is ready."
else
    die "Kafka did not become ready. Check: docker compose -p ${COMPOSE_PROJECT} logs kafka"
fi

if wait_kafka_init; then
    print_ok "kafka-init finished successfully."
else
    die "kafka-init failed. Check: docker compose -p ${COMPOSE_PROJECT} logs kafka-init"
fi

if wait_app_api; then
    print_ok "app-api is healthy."
else
    die "app-api did not become healthy. Check: docker compose -p ${COMPOSE_PROJECT} logs app-api"
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
