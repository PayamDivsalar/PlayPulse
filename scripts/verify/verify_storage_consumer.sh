#!/usr/bin/env bash

# =============================================================================
# verify_storage_consumer.sh
#
# End-to-end check that the storage consumer is actually persisting what the
# producers publish. Answers, in order:
#
#   1. Are the schema objects this subsystem owns present?
#   2. Is the consumer running and healthy?
#   3. Is it keeping up, or is lag growing?
#   4. What has it stored, and what did it reject?
#
# Complements verify_network_metrics.sh, which reads the Kafka topic. That one
# proves the analyzer published; this one proves the rows landed.
#
# Usage:
#   ./scripts/verify/verify_storage_consumer.sh [options]
#   ./scripts/verify/verify_storage_consumer.sh --help
#
# Prerequisites:
#   docker compose up -d postgres kafka storage-consumer
# =============================================================================

set -uo pipefail

# --- Appearance --------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- Defaults ----------------------------------------------------------------
POSTGRES_CONTAINER="project_postgres"
KAFKA_CONTAINER="project_kafka"
CONSUMER_CONTAINER="project_storage_consumer"
CONSUMER_GROUP_PREFIX="storage-consumer"
SHOW_DEAD_LETTERS=5
LAG_WARN_THRESHOLD=1000

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

FAILED_CHECKS=0
WARNINGS=0

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

print_warn() {
    echo -e "  ${YELLOW}[WARN]${NC} $1"
    WARNINGS=$((WARNINGS + 1))
}

print_info() {
    echo -e "  ${YELLOW}[..]${NC} $1"
}

usage() {
    cat <<'EOF'
Verify the storage consumer is persisting Kafka messages to PostgreSQL.

Usage:
  ./scripts/verify/verify_storage_consumer.sh [options]

Options:
  --dead-letters N     Show N recent dead-lettered messages. Default: 5
                       Use 0 to show only the count.
  --lag-threshold N    Warn when a pipeline's lag exceeds N. Default: 1000
  --group-prefix NAME  Consumer group prefix. Default: storage-consumer
                       Must match STORAGE_CONSUMER_GROUP_PREFIX.
  --container NAME     Consumer container name. Default: project_storage_consumer
  -h, --help           Show this help and exit.

Exit status:
  0  the schema exists, the consumer is healthy and rows are present
  1  a required check failed

Examples:
  # Routine check after starting the stack.
  ./scripts/verify/verify_storage_consumer.sh

  # Investigate rejected messages.
  ./scripts/verify/verify_storage_consumer.sh --dead-letters 25
EOF
}

# --- Argument parsing --------------------------------------------------------

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dead-letters)
            [[ $# -ge 2 ]] || { echo "--dead-letters needs a number." >&2; exit 2; }
            SHOW_DEAD_LETTERS="$2"
            shift 2
            ;;
        --lag-threshold)
            [[ $# -ge 2 ]] || { echo "--lag-threshold needs a number." >&2; exit 2; }
            LAG_WARN_THRESHOLD="$2"
            shift 2
            ;;
        --group-prefix)
            [[ $# -ge 2 ]] || { echo "--group-prefix needs a name." >&2; exit 2; }
            CONSUMER_GROUP_PREFIX="$2"
            shift 2
            ;;
        --container)
            [[ $# -ge 2 ]] || { echo "--container needs a name." >&2; exit 2; }
            CONSUMER_CONTAINER="$2"
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

# Credentials come from the same .env compose reads, so this script and the
# container always talk to the same database.
if [[ -f "$PROJECT_ROOT/.env" ]]; then
    # shellcheck disable=SC1091
    set -a && . "$PROJECT_ROOT/.env" && set +a
fi
PG_USER="${POSTGRES_USER:-project_user}"
PG_DB="${POSTGRES_DB:-project_db}"

psql_value() {
    docker exec "$POSTGRES_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" \
        -tAc "$1" 2>/dev/null | tr -d '[:space:]'
}

psql_table() {
    docker exec "$POSTGRES_CONTAINER" psql -U "$PG_USER" -d "$PG_DB" \
        -c "$1" 2>/dev/null
}

echo -e "${BLUE}"
echo "+----------------------------------------------+"
echo "|        storage consumer verification         |"
echo "+----------------------------------------------+"
echo -e "${NC}"

# --- Prerequisites -----------------------------------------------------------

print_header "Containers"

if ! command -v docker >/dev/null 2>&1; then
    print_fail "docker is not on PATH."
    exit 1
fi

for container in "$POSTGRES_CONTAINER" "$KAFKA_CONTAINER"; do
    if docker inspect -f '{{.State.Running}}' "$container" 2>/dev/null | grep -q true; then
        print_ok "$container is running."
    else
        print_fail "$container is not running."
        echo "      Start it with: docker compose up -d postgres kafka"
        exit 1
    fi
done

# --- Schema ------------------------------------------------------------------

print_header "Schema"

# Django owns this one. Without it the consumer cannot resolve a package_name
# to an application_id and would dead-letter everything.
if [[ "$(psql_value "SELECT to_regclass('apps_registry_application') IS NOT NULL")" == "t" ]]; then
    print_ok "apps_registry_application exists (owned by app_api)."
else
    print_fail "apps_registry_application is missing."
    echo "      The consumer needs it to resolve package_name -> application_id."
    echo "      Run Django's migrations first: python manage.py migrate"
fi

for table in app_stats reviews network_metrics dead_letter_events; do
    if [[ "$(psql_value "SELECT to_regclass('$table') IS NOT NULL")" == "t" ]]; then
        print_ok "$table exists."
    else
        print_fail "$table is missing."
        echo "      Run: docker compose run --rm storage-consumer migrate"
    fi
done

applied=$(psql_value "SELECT coalesce(max(version)::text, 'none') FROM storage_consumer_migrations")
if [[ -n "$applied" && "$applied" != "none" ]]; then
    print_ok "Migrations applied up to version $applied."
fi

registered=$(psql_value "SELECT count(*) FROM apps_registry_application")
if [[ "${registered:-0}" -gt 0 ]]; then
    print_ok "$registered application(s) registered."
else
    print_warn "No applications are registered."
    echo "      Every message will be dead-lettered for an unknown package_name."
fi

# --- Consumer process --------------------------------------------------------

print_header "Consumer"

if docker inspect -f '{{.State.Running}}' "$CONSUMER_CONTAINER" 2>/dev/null | grep -q true; then
    print_ok "$CONSUMER_CONTAINER is running."

    health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' \
        "$CONSUMER_CONTAINER" 2>/dev/null)
    case "$health" in
        healthy)
            print_ok "Healthcheck reports healthy (every pipeline is polling)."
            ;;
        starting)
            print_info "Healthcheck is still in its start period."
            ;;
        unhealthy)
            print_fail "Healthcheck reports unhealthy: a pipeline has stopped polling."
            docker exec "$CONSUMER_CONTAINER" healthcheck.sh 2>&1 | sed 's/^/      /'
            ;;
        *)
            print_info "No healthcheck is configured on this container."
            ;;
    esac

    restarts=$(docker inspect -f '{{.RestartCount}}' "$CONSUMER_CONTAINER" 2>/dev/null)
    if [[ "${restarts:-0}" -gt 0 ]]; then
        print_warn "The container has restarted ${restarts} time(s)."
        echo "      Recent errors:"
        docker logs --tail 200 "$CONSUMER_CONTAINER" 2>&1 \
            | grep -E 'ERROR|CRITICAL' | tail -5 | sed 's/^/        /'
    fi
else
    print_warn "$CONSUMER_CONTAINER is not running."
    echo "      Start it with: docker compose up -d storage-consumer"
    echo "      Continuing with the checks that do not need it."
fi

# --- Lag ---------------------------------------------------------------------

print_header "Consumer lag"

for pipeline in app-stats reviews network-metrics; do
    group="${CONSUMER_GROUP_PREFIX}.${pipeline}"
    described=$(docker exec "$KAFKA_CONTAINER" kafka-consumer-groups \
        --bootstrap-server localhost:9092 --describe --group "$group" 2>/dev/null)

    if ! echo "$described" | grep -q "$pipeline"; then
        print_info "$pipeline: the group has not committed any offsets yet."
        continue
    fi

    # LAG is column 6 of kafka-consumer-groups --describe; "-" means no
    # committed offset for that partition yet.
    lag=$(echo "$described" | awk '$6 ~ /^[0-9]+$/ { total += $6 } END { print total + 0 }')

    if [[ "$lag" -eq 0 ]]; then
        print_ok "$pipeline: caught up (lag 0)."
    elif [[ "$lag" -lt "$LAG_WARN_THRESHOLD" ]]; then
        print_ok "$pipeline: lag $lag."
    else
        print_warn "$pipeline: lag $lag exceeds $LAG_WARN_THRESHOLD."
        echo "      Normal during a reviews burst. Sustained growth means the"
        echo "      single thread for this pipeline is no longer keeping up:"
        echo "      widen the topic and run a second container for it."
    fi
done

# --- Stored rows -------------------------------------------------------------

print_header "Stored rows"

for table in app_stats reviews network_metrics; do
    count=$(psql_value "SELECT count(*) FROM $table")
    if [[ "${count:-0}" -gt 0 ]]; then
        print_ok "$table holds ${count} row(s)."
    else
        print_info "$table is empty."
    fi
done

print_info "Most recent row per table:"
psql_table "
    SELECT 'app_stats' AS table_name, max(crawled_at)::text AS latest FROM app_stats
    UNION ALL
    SELECT 'reviews', max(last_synced_at)::text FROM reviews
    UNION ALL
    SELECT 'network_metrics', max(analyzed_at)::text FROM network_metrics
" | sed 's/^/      /'

# --- Dead letters ------------------------------------------------------------

print_header "Dead-lettered messages"

dead_count=$(psql_value "SELECT count(*) FROM dead_letter_events")

if [[ "${dead_count:-0}" -eq 0 ]]; then
    print_ok "No messages have been rejected."
else
    print_warn "${dead_count} message(s) could not be stored."
    echo ""
    echo "      Rejections by topic and reason:"
    psql_table "
        SELECT topic,
               left(error_reason, 60) AS reason,
               count(*) AS events,
               max(occurred_at)::text AS most_recent
          FROM dead_letter_events
         GROUP BY topic, left(error_reason, 60)
         ORDER BY count(*) DESC
         LIMIT 10
    " | sed 's/^/      /'

    if [[ "${SHOW_DEAD_LETTERS:-0}" -gt 0 ]]; then
        echo "      Most recent ${SHOW_DEAD_LETTERS}:"
        psql_table "
            SELECT topic, \"partition\", kafka_offset, kafka_key,
                   left(error_reason, 70) AS reason
              FROM dead_letter_events
             ORDER BY occurred_at DESC
             LIMIT ${SHOW_DEAD_LETTERS}
        " | sed 's/^/      /'
    fi

    echo "      See storage_consumer/README.md for the dead-letter runbook."
fi

# --- Summary -----------------------------------------------------------------

print_header "Result"

if [[ $FAILED_CHECKS -eq 0 && $WARNINGS -eq 0 ]]; then
    print_ok "Everything checks out."
    exit 0
fi

if [[ $FAILED_CHECKS -eq 0 ]]; then
    print_ok "No failures, but ${WARNINGS} warning(s) worth a look."
    exit 0
fi

print_fail "${FAILED_CHECKS} check(s) failed, ${WARNINGS} warning(s)."
exit 1
