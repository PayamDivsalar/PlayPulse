#!/usr/bin/env bash

# =============================================================================
# run_tests_in_docker.sh
#
# Bring up the shared Docker infrastructure (Postgres, Kafka, app-api), then
# run every subsystem's `-tests` service on the compose network via the
# ``test`` profile. Collects all exit codes and prints a PASS/FAIL summary.
#
# Usage (from repo root):
#   ./scripts/run_tests_in_docker.sh
#   ./scripts/run_tests_in_docker.sh --no-build
#
# Prerequisites: root .env present; Docker Engine + Compose plugin.
# =============================================================================

set -uo pipefail

# --- Appearance --------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
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

# Order matters: unit/contract suites first, then integration, then Django.
# sentiment-tests is omitted by default: the image bakes torch + the HF model
# and a cold build can take many minutes. Run it manually when needed:
#   docker compose --profile test run --rm sentiment-tests
TEST_SERVICES=(
    crawler-tests
    network-analyzer-tests
    storage-consumer-tests
    storage-consumer-tests-integration
    app-api-tests
)

declare -a RESULT_NAMES=()
declare -a RESULT_CODES=()

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
    until [[ "$(docker inspect -f '{{.State.Status}}' project_kafka_init 2>/dev/null || echo missing)" == "exited" ]]; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || return 1
        sleep 2
    done
    local code
    code="$(docker inspect -f '{{.State.ExitCode}}' project_kafka_init)"
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

# =============================================================================
# 1. Base infrastructure + app-api
# =============================================================================
print_header "Infrastructure"

print_info "Starting postgres zookeeper kafka kafka-init app-api..."
"${COMPOSE[@]}" up -d "${BUILD_FLAG[@]}" \
    postgres zookeeper kafka kafka-init app-api

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
    print_info "Running $service..."
    "${COMPOSE[@]}" --profile test run --rm "${BUILD_FLAG[@]}" "$service"
    code=$?
    RESULT_NAMES+=("$service")
    RESULT_CODES+=("$code")
    if [[ $code -eq 0 ]]; then
        print_ok "$service exited 0"
    else
        print_fail "$service exited $code"
    fi
done

# =============================================================================
# 3. Summary
# =============================================================================
print_header "Summary"

printf "  %-40s %s\n" "subsystem" "result"
printf "  %-40s %s\n" "----------------------------------------" "------"

failed=0
for i in "${!RESULT_NAMES[@]}"; do
    name="${RESULT_NAMES[$i]}"
    code="${RESULT_CODES[$i]}"
    if [[ $code -eq 0 ]]; then
        printf "  %-40s ${GREEN}%s${NC}\n" "$name" "PASS"
    else
        printf "  %-40s ${RED}%s${NC}\n" "$name" "FAIL"
        failed=$((failed + 1))
    fi
done

echo ""
if [[ $failed -eq 0 ]]; then
    echo -e "  ${GREEN}[OK]${NC} All Docker test suites passed."
    exit 0
else
    echo -e "  ${RED}[FAIL]${NC} $failed suite(s) failed."
    exit 1
fi
