#!/usr/bin/env bash
# =============================================================================
# healthcheck.sh
#
# Compose healthcheck for app-api. Hits /healthz/ on the local loopback so we
# verify the HTTP server is up AND Postgres is reachable from inside the
# process — the same readiness signal bring_up and sibling services wait on.
# =============================================================================

set -euo pipefail

python - <<'PY'
import sys
import urllib.error
import urllib.request

url = "http://127.0.0.1:8000/healthz/"
try:
    with urllib.request.urlopen(url, timeout=5) as response:
        sys.exit(0 if response.status == 200 else 1)
except (urllib.error.URLError, TimeoutError, OSError):
    sys.exit(1)
PY
