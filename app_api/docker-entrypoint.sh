#!/usr/bin/env bash
# =============================================================================
# docker-entrypoint.sh
#
# Entrypoint for the app-api image.
#
#   run                       Wait for Postgres, migrate, then serve on :8000.
#                             This is the image default (see Dockerfile CMD).
#   migrate                   Wait for Postgres, apply migrations, exit.
#   anything else             Passed to manage.py (for example: test, shell).
#
# Migrations run on every start because they are idempotent. storage-consumer
# refuses to start until apps_registry_application exists, so applying here
# before the HTTP server binds is what makes `depends_on: service_healthy`
# meaningful for the rest of the stack.
#
# runserver is intentional for this compose-as-local stack: it serves admin
# and swagger static files with DEBUG on, matching the host/venv workflow.
# =============================================================================

set -euo pipefail

readonly PG_WAIT_TIMEOUT_SECONDS="${APP_API_PG_WAIT_TIMEOUT_SECONDS:-60}"

log() {
    echo "$(date -u '+%Y-%m-%d %H:%M:%S') entrypoint: $*"
}

# Wait for Postgres to accept a connection.
#
# Not a substitute for compose's `depends_on: condition: service_healthy`, but
# a backstop when Postgres is still replaying WAL after an unclean shutdown.
# Uses Python because psycopg2 is already installed and pg_isready is not.
wait_for_postgres() {
    local deadline=$((SECONDS + PG_WAIT_TIMEOUT_SECONDS))

    while true; do
        if python - <<'PY' 2>/dev/null
import os
import sys

import psycopg2

try:
    conn = psycopg2.connect(
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        connect_timeout=3,
    )
except Exception:
    sys.exit(1)
else:
    conn.close()
    sys.exit(0)
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
        log "Applying Django migrations..."
        python manage.py migrate --noinput
        # `exec` makes runserver PID 1 so SIGTERM from `docker stop` reaches it.
        exec python manage.py runserver 0.0.0.0:8000 "$@"
        ;;
    migrate)
        shift || true
        wait_for_postgres
        exec python manage.py migrate --noinput "$@"
        ;;
    *)
        wait_for_postgres
        exec python manage.py "$@"
        ;;
esac
