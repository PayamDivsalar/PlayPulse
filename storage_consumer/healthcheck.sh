#!/usr/bin/env bash
# =============================================================================
# healthcheck.sh
#
# Container healthcheck. Reports healthy only when *every* pipeline this
# process was asked to run has touched its heartbeat file recently.
#
# Checking per pipeline is the whole point. A process whose reviews thread has
# wedged is still a running process with three open sockets, so anything that
# merely checks liveness of PID 1 -- or of the Kafka connection -- would call
# it healthy while reviews silently stopped arriving. The supervisor is built
# to crash rather than limp, and this is the check that notices if it ever
# fails to.
#
# Exit status: 0 healthy, 1 unhealthy.
# =============================================================================

set -uo pipefail

readonly HEARTBEAT_DIR="${STORAGE_HEARTBEAT_DIRECTORY:-/tmp/storage-consumer}"
readonly ALL_PIPELINES="app-stats reviews network-metrics"

# A heartbeat is stale after this long. Must exceed the worst-case time between
# two touches, which is one poll timeout plus a full batch including its
# database retry budget -- so it is generous on purpose. A tighter bound would
# restart containers that were merely waiting for Postgres to come back, which
# is exactly when losing the consumer is least helpful.
readonly MAX_AGE_SECONDS="${STORAGE_HEARTBEAT_MAX_AGE_SECONDS:-120}"

# Which pipelines to expect, mirroring how config.py reads STORAGE_PIPELINES.
pipelines="${STORAGE_PIPELINES:-all}"
if [[ "$pipelines" == "all" || -z "$pipelines" ]]; then
    pipelines="$ALL_PIPELINES"
else
    pipelines="${pipelines//,/ }"
fi

now=$(date +%s)
unhealthy=0

for pipeline in $pipelines; do
    pipeline="${pipeline// /}"
    [[ -z "$pipeline" ]] && continue

    file="${HEARTBEAT_DIR}/${pipeline}.heartbeat"
    if [[ ! -f "$file" ]]; then
        echo "unhealthy: no heartbeat file for pipeline '${pipeline}' at ${file}"
        unhealthy=1
        continue
    fi

    modified=$(stat -c %Y "$file" 2>/dev/null || echo 0)
    age=$((now - modified))
    if (( age > MAX_AGE_SECONDS )); then
        echo "unhealthy: pipeline '${pipeline}' last polled ${age}s ago (limit ${MAX_AGE_SECONDS}s)"
        unhealthy=1
    fi
done

if (( unhealthy )); then
    exit 1
fi

echo "healthy: ${pipelines// /, }"
exit 0
