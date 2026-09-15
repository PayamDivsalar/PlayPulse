#!/usr/bin/env bash
# =============================================================================
# 01-create-metabase-db.sh
#
# Creates Metabase's dedicated application database (metabase_app_db).
#
# The official postgres image runs everything in /docker-entrypoint-initdb.d
# exactly once: on the very first startup against an empty data directory
# (fresh postgres_data volume). On an existing volume this script is never
# executed -- by design of the image -- which is why scripts/bring_up.sh
# also ensures this database exists idempotently on every bring-up.
# =============================================================================

set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE metabase_app_db;
EOSQL
