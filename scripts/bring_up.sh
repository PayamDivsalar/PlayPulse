#!/usr/bin/env bash

# =============================================================================
# bring_up.sh
#
# Project bootstrap for PlayPulse: prepare env files, start Compose services,
# wait until they are usable, and run the existing infra smoke check.
#
# This is the shell-script half of "install and bring up". Ansible (under
# ansible/) is intentionally separate and not used here.
#
# What this script does:
#   1. Checks that Docker and Compose are available
#   2. Creates missing .env files from *.env.example (never overwrites)
#   3. Fills blank Postgres credentials in the root .env if needed
#   4. Mirrors those credentials into host-side app_api / storage_consumer envs
#   5. Starts the Compose stack (infra + app-api + storage-consumer + crawler
#      + network-analyzer via Compose profile ``tools``)
#   6. Waits for Postgres / Kafka / app-api / storage-consumer
#   7. Runs scripts/infra/check_infra.sh
#
# What it does not do:
#   - Install Docker/Compose on the OS (leave that to the operator or Ansible)
#
# Note: network-analyzer is a batch job (processes data/pcap/inbox), not a
# long-running daemon. Bring-up starts it once with the rest of the stack; an
# empty inbox usually means it exits quickly after start.
#
# Usage:
#   ./scripts/bring_up.sh
#   ./scripts/bring_up.sh --infra-only
#   ./scripts/bring_up.sh --skip-check
#   ./scripts/bring_up.sh --help
#
# Prerequisites: Docker Engine + Compose plugin, run from a clone of this repo.
# =============================================================================

set -euo pipefail

# --- Appearance --------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# --- Paths -------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# --- Defaults ----------------------------------------------------------------
INFRA_ONLY=false
SKIP_CHECK=false
NO_BUILD=false
WITH_CYCLE_REPORTS=false

INFRA_SERVICES=(postgres zookeeper kafka kafka-init kafka-ui)
# network-analyzer lives behind Compose profile ``tools``; bring_up enables
# that profile when starting the full stack (see Compose up below).
APP_SERVICES=(app-api storage-consumer crawler network-analyzer)

POSTGRES_CONTAINER="project_postgres"
KAFKA_CONTAINER="project_kafka"
APP_API_CONTAINER="project_app_api"
CONSUMER_CONTAINER="project_storage_consumer"
ANALYZER_CONTAINER="project_network_analyzer"

# --- Helpers -----------------------------------------------------------------

print_header() {
    echo ""
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_ok() {
    echo -e "  ${GREEN}[OK]${NC} $1"
}

print_fail() {
    echo -e "  ${RED}[FAIL]${NC} $1" >&2
}

print_info() {
    echo -e "  ${YELLOW}[..]${NC} $1"
}

usage() {
    cat <<'EOF'
Bring up PlayPulse with Docker Compose.

Usage:
  ./scripts/bring_up.sh [options]

Options:
  --infra-only          Start only Postgres, Zookeeper, Kafka, kafka-init, Kafka UI
  --skip-check          Skip scripts/infra/check_infra.sh after startup
  --no-build            Pass --no-build to docker compose up (reuse existing images)
  --with-cycle-reports  Enable crawl+persist cycle reports
                        (crawler pending files + cycle-reporter Compose service)
  -h, --help            Show this help and exit

Examples:
  # Full stack (infra + app-api + storage-consumer + crawler + network-analyzer)
  ./scripts/bring_up.sh

  # Same + cycle-reporter (runs scripts/reports/finalize_cycle_reports.py in a loop)
  ./scripts/bring_up.sh --with-cycle-reports

  # Broker + database only (for host/venv App API + crawler development)
  ./scripts/bring_up.sh --infra-only
EOF
}

die() {
    print_fail "$1"
    exit 1
}

# Read KEY=value from a dotenv file without sourcing (avoids executing content).
env_get() {
    local file="$1"
    local key="$2"
    [[ -f "$file" ]] || return 0
    # Take the last matching assignment; ignore comments and blank values' quotes.
    local line
    line="$(grep -E "^${key}=" "$file" | tail -n1 || true)"
    [[ -n "$line" ]] || return 0
    printf '%s' "${line#*=}"
}

# Set or replace KEY=value in a dotenv file. Creates the file if missing.
env_set() {
    local file="$1"
    local key="$2"
    local value="$3"
    local tmp
    tmp="$(mktemp)"
    if [[ -f "$file" ]] && grep -qE "^${key}=" "$file"; then
        # Escape & and \ for sed replacement safety; values are simple tokens.
        local escaped
        escaped="$(printf '%s' "$value" | sed -e 's/[&\\]/\\&/g')"
        sed -E "s|^${key}=.*|${key}=${escaped}|" "$file" >"$tmp"
        mv "$tmp" "$file"
    else
        {
            [[ -f "$file" ]] && cat "$file"
            printf '%s=%s\n' "$key" "$value"
        } >"$tmp"
        mv "$tmp" "$file"
    fi
}

copy_example_if_missing() {
    local example="$1"
    local target="$2"
    if [[ -f "$target" ]]; then
        print_ok "Exists: ${target#"$PROJECT_ROOT"/}"
        return 0
    fi
    [[ -f "$example" ]] || die "Missing example file: $example"
    cp "$example" "$target"
    print_ok "Created ${target#"$PROJECT_ROOT"/} from $(basename "$example")"
}

# --- Argument parsing --------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --infra-only) INFRA_ONLY=true; shift ;;
        --skip-check) SKIP_CHECK=true; shift ;;
        --no-build) NO_BUILD=true; shift ;;
        --with-cycle-reports) WITH_CYCLE_REPORTS=true; shift ;;
        -h|--help) usage; exit 0 ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Run with --help to see available options." >&2
            exit 2
            ;;
    esac
done

cd "$PROJECT_ROOT"

echo -e "${BLUE}"
echo "+----------------------------------------------+"
echo "|           PlayPulse bring-up                 |"
echo "+----------------------------------------------+"
echo -e "${NC}"

# =============================================================================
# 1. Prerequisites
# =============================================================================
print_header "Prerequisites"

command -v docker >/dev/null 2>&1 || die "docker is not on PATH. Install Docker Engine first."

if docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
else
    die "Docker Compose is not available (need \`docker compose\` or docker-compose)."
fi

if docker info >/dev/null 2>&1; then
    print_ok "Docker daemon is reachable."
else
    die "Docker daemon is not reachable. Is the service running, and is your user in the docker group?"
fi

print_ok "Compose: ${COMPOSE[*]}"

# =============================================================================
# 2. Environment files
# =============================================================================
print_header "Environment files"

copy_example_if_missing "$PROJECT_ROOT/.env.example" "$PROJECT_ROOT/.env"

# Sensible defaults when the operator has not filled the root compose .env yet.
ROOT_ENV="$PROJECT_ROOT/.env"
db="$(env_get "$ROOT_ENV" POSTGRES_DB)"
user="$(env_get "$ROOT_ENV" POSTGRES_USER)"
pass="$(env_get "$ROOT_ENV" POSTGRES_PASSWORD)"
port="$(env_get "$ROOT_ENV" POSTGRES_PORT)"

if [[ -z "$db" ]]; then
    env_set "$ROOT_ENV" POSTGRES_DB "project_db"
    print_info "Set POSTGRES_DB=project_db in .env"
fi
if [[ -z "$user" ]]; then
    env_set "$ROOT_ENV" POSTGRES_USER "project_user"
    print_info "Set POSTGRES_USER=project_user in .env"
fi
if [[ -z "$pass" ]]; then
    # openssl is usual on Ubuntu servers; fall back to /dev/urandom.
    if command -v openssl >/dev/null 2>&1; then
        pass="$(openssl rand -hex 16)"
    else
        pass="$(tr -dc 'a-zA-Z0-9' </dev/urandom | head -c 32)"
    fi
    env_set "$ROOT_ENV" POSTGRES_PASSWORD "$pass"
    print_info "Generated POSTGRES_PASSWORD in .env (keep this file private)."
fi
if [[ -z "$port" ]]; then
    env_set "$ROOT_ENV" POSTGRES_PORT "5432"
fi

# Re-read after possible fills.
db="$(env_get "$ROOT_ENV" POSTGRES_DB)"
user="$(env_get "$ROOT_ENV" POSTGRES_USER)"
pass="$(env_get "$ROOT_ENV" POSTGRES_PASSWORD)"
port="$(env_get "$ROOT_ENV" POSTGRES_PORT)"
[[ -n "$db" && -n "$user" && -n "$pass" ]] || die "Root .env is missing Postgres credentials."

# Host/venv env files (see docs/setup.md). Compose does not use these for
# app-api/crawler/storage-consumer containers; they matter for local runs.
copy_example_if_missing \
    "$PROJECT_ROOT/app_api/.env.example" \
    "$PROJECT_ROOT/app_api/.env"
copy_example_if_missing \
    "$PROJECT_ROOT/crawler/.env.example" \
    "$PROJECT_ROOT/crawler/.env"
copy_example_if_missing \
    "$PROJECT_ROOT/storage_consumer/.env.example" \
    "$PROJECT_ROOT/storage_consumer/.env"
copy_example_if_missing \
    "$PROJECT_ROOT/network_analyzer/.env.example" \
    "$PROJECT_ROOT/network_analyzer/.env"

# Keep host-side Postgres settings aligned with the compose database when blank.
for host_env in \
    "$PROJECT_ROOT/app_api/.env" \
    "$PROJECT_ROOT/storage_consumer/.env"
do
    [[ -z "$(env_get "$host_env" POSTGRES_DB)" ]] && env_set "$host_env" POSTGRES_DB "$db"
    [[ -z "$(env_get "$host_env" POSTGRES_USER)" ]] && env_set "$host_env" POSTGRES_USER "$user"
    [[ -z "$(env_get "$host_env" POSTGRES_PASSWORD)" ]] && env_set "$host_env" POSTGRES_PASSWORD "$pass"
    [[ -z "$(env_get "$host_env" POSTGRES_PORT)" ]] && env_set "$host_env" POSTGRES_PORT "$port"
    # Host processes must use localhost, never the compose service name.
    host_val="$(env_get "$host_env" POSTGRES_HOST)"
    if [[ -z "$host_val" || "$host_val" == "postgres" ]]; then
        env_set "$host_env" POSTGRES_HOST "127.0.0.1"
    fi
done

print_ok "Env files ready (existing files were left unchanged except blank Postgres fields)."

if [[ "$WITH_CYCLE_REPORTS" == true ]]; then
    print_header "Cycle reports"
    env_set "$ROOT_ENV" CRAWLER_CYCLE_REPORTS_ENABLED "true"
    # Container path; host bind is ./data/reports via compose.
    if [[ -z "$(env_get "$ROOT_ENV" CRAWLER_CYCLE_REPORTS_DIR)" ]]; then
        env_set "$ROOT_ENV" CRAWLER_CYCLE_REPORTS_DIR "/data/reports"
    fi
    mkdir -p "$PROJECT_ROOT/data/reports/pending" "$PROJECT_ROOT/data/reports/done"
    print_ok "Enabled CRAWLER_CYCLE_REPORTS_ENABLED=true in .env"
    print_info "Pending/final files: data/reports/ (mounted into the crawler)"
fi

# =============================================================================
# 3. Compose up
# =============================================================================
print_header "Docker Compose"

SERVICES=("${INFRA_SERVICES[@]}")
if [[ "$INFRA_ONLY" != true ]]; then
    SERVICES+=("${APP_SERVICES[@]}")
fi
if [[ "$WITH_CYCLE_REPORTS" == true ]]; then
    SERVICES+=(cycle-reporter)
fi

# Profile ``tools`` is required for network-analyzer (see docker-compose.yml).
UP_ARGS=()
if [[ "$INFRA_ONLY" != true ]]; then
    UP_ARGS+=(--profile tools)
fi
UP_ARGS+=(up -d)
if [[ "$NO_BUILD" != true ]]; then
    UP_ARGS+=(--build)
fi

print_info "Starting: ${SERVICES[*]}"
"${COMPOSE[@]}" "${UP_ARGS[@]}" "${SERVICES[@]}"
print_ok "Compose up completed."

# =============================================================================
# 4. Wait until ready
# =============================================================================
print_header "Readiness"

wait_postgres() {
    local retries=30
    print_info "Waiting for Postgres ($POSTGRES_CONTAINER)..."
    until docker exec "$POSTGRES_CONTAINER" \
        pg_isready -U "$user" -d "$db" >/dev/null 2>&1; do
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

wait_storage_consumer() {
    local retries=60
    print_info "Waiting for storage-consumer health..."
    until [[ "$(docker inspect -f '{{.State.Health.Status}}' "$CONSUMER_CONTAINER" 2>/dev/null || echo starting)" == "healthy" ]]; do
        retries=$((retries - 1))
        [[ $retries -gt 0 ]] || return 1
        sleep 2
    done
    return 0
}

if wait_postgres; then
    print_ok "Postgres is accepting connections."
else
    die "Postgres did not become ready in time. Check: docker compose logs postgres"
fi

if wait_kafka; then
    print_ok "Kafka broker is reachable."
else
    die "Kafka did not become ready in time. Check: docker compose logs kafka"
fi

if wait_kafka_init; then
    print_ok "kafka-init exited successfully (topics created or already present)."
else
    print_info "kafka-init did not finish cleanly; topics may still be created by auto-create or scripts/infra/create_topics.sh."
fi

if [[ "$INFRA_ONLY" != true ]]; then
    if wait_app_api; then
        print_ok "app-api is healthy."
    else
        die "app-api did not become healthy. Check: docker compose logs app-api"
    fi

    if wait_storage_consumer; then
        print_ok "storage-consumer is healthy."
    else
        die "storage-consumer did not become healthy. Check: docker compose logs storage-consumer"
    fi

    # Batch job: "started" means the container was created and has either
    # finished the inbox pass or is still running. Do not require healthy.
    analyzer_status="$(docker inspect -f '{{.State.Status}}' "$ANALYZER_CONTAINER" 2>/dev/null || echo missing)"
    if [[ "$analyzer_status" == "running" || "$analyzer_status" == "exited" ]]; then
        print_ok "network-analyzer was started (status=$analyzer_status; batch over data/pcap/inbox)."
    else
        print_info "network-analyzer status=$analyzer_status — check: docker compose --profile tools logs network-analyzer"
    fi
fi

# =============================================================================
# 5. Smoke check
# =============================================================================
if [[ "$SKIP_CHECK" != true ]]; then
    print_header "Infra smoke check"
    CHECK_INFRA="$SCRIPT_DIR/infra/check_infra.sh"
    if [[ -x "$CHECK_INFRA" ]]; then
        "$CHECK_INFRA"
    else
        bash "$CHECK_INFRA"
    fi
else
    print_info "Skipped scripts/infra/check_infra.sh (--skip-check)."
fi

# =============================================================================
# 6. Summary
# =============================================================================
print_header "Status"

if [[ "$INFRA_ONLY" != true ]]; then
    "${COMPOSE[@]}" --profile tools ps -a
else
    "${COMPOSE[@]}" ps
fi

KAFKA_UI_PORT="$(env_get "$ROOT_ENV" KAFKA_UI_PORT)"
KAFKA_UI_PORT="${KAFKA_UI_PORT:-8080}"
APP_API_PORT="$(env_get "$ROOT_ENV" APP_API_PORT)"
APP_API_PORT="${APP_API_PORT:-8000}"

echo ""
echo -e "${GREEN}Bring-up finished.${NC}"
echo ""
echo "Useful URLs / commands:"
echo "  App API:    http://localhost:${APP_API_PORT}/swagger/"
echo "  Kafka UI:   http://localhost:${KAFKA_UI_PORT}"
echo "  Compose:    docker compose --profile tools ps -a"
echo "  Consumer:   ./scripts/verify/verify_storage_consumer.sh"
echo "  Analyzer:   docker compose --profile tools logs network-analyzer"
echo "              (drop pcaps in data/pcap/inbox, then re-run the service)"
if [[ "$WITH_CYCLE_REPORTS" == true ]] || [[ "$(env_get "$ROOT_ENV" CRAWLER_CYCLE_REPORTS_ENABLED)" == "true" ]]; then
    echo "  Reports:    docker compose logs -f cycle-reporter"
    echo "              tail -n 20 data/reports/cycles.jsonl"
fi
echo ""
echo "Host/venv notes: docs/setup.md"
echo "Ansible automation (later): ansible/"
echo ""
