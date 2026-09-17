#!/usr/bin/env bash

# =============================================================================
# seed_apps.sh
#
# Idempotently register the initial PlayPulse application list via the App API
# (POST /api/applications/). Intended to run on the host after app-api is
# healthy — typically from scripts/bring_up.sh, before crawler starts.
#
# HTTP outcomes per app:
#   201 → [OK]   created
#   409 → [SKIP] already registered
#   else → [FAIL]
#
# Note: App API create ignores is_active (always starts active). After a
# successful POST/409, this script enforces is_active via the deactivate /
# reactivate endpoints so seeded flags match the table below.
#
# Usage:
#   ./scripts/seed_apps.sh
#   APP_API_BASE_URL=http://localhost:8000 ./scripts/seed_apps.sh
#   APP_API_PORT=8000 ./scripts/seed_apps.sh
#
# Defaults: APP_API_BASE_URL=http://localhost:${APP_API_PORT:-8000}
# =============================================================================

set -uo pipefail

# --- Appearance (same style as scripts/infra/check_infra.sh) ------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

FAILED=0

print_header() {
    echo ""
    echo -e "${BLUE}=== $1 ===${NC}"
}

print_ok() {
    echo -e "  ${GREEN}[OK]${NC} $1"
}

print_skip() {
    echo -e "  ${CYAN}[SKIP]${NC} $1"
}

print_fail() {
    echo -e "  ${RED}[FAIL]${NC} $1" >&2
    FAILED=$((FAILED + 1))
}

print_info() {
    echo -e "  ${YELLOW}[..]${NC} $1"
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Prefer an explicit base URL; otherwise build from APP_API_PORT (host-side).
APP_API_BASE_URL="${APP_API_BASE_URL:-http://localhost:${APP_API_PORT:-8000}}"
APP_API_BASE_URL="${APP_API_BASE_URL%/}"

command -v curl >/dev/null 2>&1 || {
    echo -e "${RED}curl is required but not on PATH.${NC}" >&2
    exit 1
}

# package_name|display_name|category|is_messaging_app|is_active|is_iranian_app
APPS=(
    "org.telegram.messenger|Telegram|Messaging|false|true|false"
    "com.whatsapp|WhatsApp|Messaging|false|true|false"
    "com.instagram.android|Instagram|Social Network|false|true|false"
    "com.facebook.katana|Facebook|Social Network|false|true|false"
    "com.zhiliaoapp.musically|TikTok|Social Network|false|true|false"
    "com.myirancell|Myirancell|Operator|false|true|true"
    "ir.mci.ecareapp|MyMCI|Operator|false|true|true"
    "ir.rightel.myrightel|MyRightel|Operator|false|false|true"
    "com.shatelland.namava.mobile|Namava|Video Streaming|false|true|true"
    "com.likotv|Lenz|Video Streaming|false|true|true"
    "ir.tamashakhonehtv|Tamasha Khoneh|Video Streaming|false|true|true"
    "com.plus9.fandogh|Fandogh|Word Game|false|true|true"
    "com.BrainLadder.AmirzaGP|Amirza|Word Game|false|true|true"
    "com.plus9.samavar|Samavar|Word Game|false|true|true"
    "ir.android.baham|Baham|Dating Chat|true|true|true"
    "app.pinno|Pinno|Dating Chat|true|true|true"
)

print_header "Seed applications"
print_info "POST ${APP_API_BASE_URL}/api/applications/ (${#APPS[@]} apps)"

# Resolve application id by package_name from GET /api/applications/.
lookup_app_id() {
    local package_name="$1"
    curl -sS "${APP_API_BASE_URL}/api/applications/" 2>/dev/null \
        | python3 -c '
import json, sys
pkg = sys.argv[1]
try:
    apps = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for app in apps:
    if app.get("package_name") == pkg:
        print(app["id"])
        sys.exit(0)
sys.exit(1)
' "$package_name"
}

# App API create always leaves apps active; enforce desired flag via soft endpoints.
enforce_active_flag() {
    local package_name="$1"
    local want_active="$2"
    local app_id="$3"

    if [[ -z "$app_id" ]]; then
        app_id="$(lookup_app_id "$package_name" || true)"
    fi
    if [[ -z "$app_id" ]]; then
        print_fail "${package_name} (could not resolve id to set is_active=${want_active})"
        return 1
    fi

    local action http_code
    if [[ "$want_active" == "true" ]]; then
        action="reactivate"
    else
        action="deactivate"
    fi

    http_code="$(
        curl -sS -o /dev/null -w '%{http_code}' \
            -X POST "${APP_API_BASE_URL}/api/applications/${app_id}/${action}/" \
            2>/dev/null || echo "000"
    )"
    if [[ "$http_code" != "200" ]]; then
        print_fail "${package_name} (${action} HTTP ${http_code})"
        return 1
    fi
    return 0
}

seed_one() {
    local package_name="$1"
    local display_name="$2"
    local category="$3"
    local is_messaging_app="$4"
    local is_active="$5"
    local is_iranian_app="$6"

    local payload body_file http_code app_id
    body_file="$(mktemp)"
    payload="$(printf \
        '{"package_name":"%s","display_name":"%s","category":"%s","is_messaging_app":%s,"is_active":%s,"is_iranian_app":%s}' \
        "$package_name" "$display_name" "$category" \
        "$is_messaging_app" "$is_active" "$is_iranian_app")"

    http_code="$(
        curl -sS -o "$body_file" -w '%{http_code}' \
            -X POST "${APP_API_BASE_URL}/api/applications/" \
            -H 'Content-Type: application/json' \
            -d "$payload" \
            2>/dev/null || echo "000"
    )"

    app_id=""
    case "$http_code" in
        201)
            app_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("id",""))' "$body_file" 2>/dev/null || true)"
            print_ok "${package_name} (HTTP ${http_code})"
            ;;
        409)
            print_skip "${package_name} (HTTP ${http_code} duplicate)"
            ;;
        *)
            print_fail "${package_name} (HTTP ${http_code})"
            rm -f "$body_file"
            return 0
            ;;
    esac
    rm -f "$body_file"

    # Always enforce is_active so re-seeds and create-ignores stay correct.
    enforce_active_flag "$package_name" "$is_active" "$app_id" || true
}

for row in "${APPS[@]}"; do
    IFS='|' read -r package_name display_name category is_messaging_app is_active is_iranian_app <<<"$row"
    seed_one "$package_name" "$display_name" "$category" \
        "$is_messaging_app" "$is_active" "$is_iranian_app"
done

echo ""
if [[ "$FAILED" -gt 0 ]]; then
    echo -e "${RED}Seed finished with ${FAILED} failure(s).${NC}" >&2
    exit 1
fi

echo -e "${GREEN}Seed finished successfully.${NC}"
exit 0
