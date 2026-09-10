#!/usr/bin/env bash
# =============================================================================
# docker-entrypoint.sh
#
# Entrypoint for the storage-consumer image.
#
#   run [--pipeline NAME]…    Apply migrations, then consume until stopped.
#                             This is the image default (see Dockerfile CMD).
#   migrate                   Apply migrations and exit.
#   anything else             Passed straight through to storage_consumer.main
#                             (for example --help).
#
# Migrations run before consuming, on every start, because they are idempotent
# and guarded by an advisory lock -- so three single-pipeline containers racing
# to migrate is safe (see persistence/migrator.py). The alternative, a separate
# one-shot migrate service, adds an ordering dependency for no benefit.
#
# Note there is no `wait for Kafka` loop here. If the broker is not up yet the
# consumer fails, the container exits non-zero, and `restart: unless-stopped`
# retries -- which is the same outcome as a wait loop, without a second
# implementation of backoff that nothing tests. Postgres does get waited on,
# because migrations run before the retry-capable consumer loop exists.
# =============================================================================

set -euo pipefail

readonly PYTHON_MODULE="storage_consumer.main"
readonly PG_WAIT_TIMEOUT_SECONDS="${STORAGE_PG_WAIT_TIMEOUT_SECONDS:-60}"

log() {
    echo "$(date -u '+%Y-%m-%d %H:%M:%S') entrypoint: $*"
}

# Wait for Postgres to accept a connection.
#
# Not a substitute for compose's `depends_on: condition: service_healthy`, but
# a backstop for the case that leaves: the container is healthy while Postgres
# is still replaying WAL after an unclean shutdown. Without this the migration
# fails on the first connection and the container restart-loops noisily for a
# few seconds. Uses Python because psycopg2 is already installed and pg_isready
# is not.
wait_for_postgres() {
    local deadline=$((SECONDS + PG_WAIT_TIMEOUT_SECONDS))

    while true; do
        if python - <<'PY' 2>/dev/null
import sys
from storage_consumer.config import load_settings
from storage_consumer.persistence.database import Database

database = Database(load_settings(), application_name="storage_consumer:wait")
try:
    database.connect()
finally:
    database.close()
PY
        then
            log "PostgreSQL is accepting connections."
            return 0
        fi

        if (( SECONDS >= deadline )); then
            log "PostgreSQL did not accept a connection within ${PG_WAIT_TIMEOUT_SECONDS}s."
            return 1
        fi
        log "Waiting for PostgreSQL..."
        sleep 2
    done
}

case "${1:-run}" in
    run)
        shift || true
        wait_for_postgres
        # `exec` matters: it makes Python PID 1 so that SIGTERM from
        # `docker stop` reaches the supervisor's handler directly. Without it
        # bash would receive the signal, ignore it, and every stop would take
        # the full 10-second grace period before a SIGKILL cut a batch short.
        exec python -m "$PYTHON_MODULE" run "$@"
        ;;
    migrate)
        shift
        wait_for_postgres
        exec python -m "$PYTHON_MODULE" migrate "$@"
        ;;
    *)
        exec python -m "$PYTHON_MODULE" "$@"
        ;;
esac
