#!/usr/bin/env bash

# =============================================================================
# seed_apps.sh
#
# Idempotently register the initial PlayPulse application list via the App API
# (POST /api/applications/). Runs on the host after app-api is healthy —
# typically from scripts/bring_up.sh, before crawler starts.
#
# Per app:  201 → [OK]   created
#           409 → [SKIP] already registered
#           else → [FAIL]
# Exit 1 if any FAIL (SKIP does not count as failure).
#
# Usage:
#   ./scripts/seed_apps.sh
#   APP_API_BASE_URL=http://localhost:8000 ./scripts/seed_apps.sh
#   APP_API_PORT=8000 ./scripts/seed_apps.sh
#
# Default: APP_API_BASE_URL=http://localhost:${APP_API_PORT:-8000}
# =============================================================================

set -uo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

FAILED=0

print_header() { echo ""; echo -e "${BLUE}=== $1 ===${NC}"; }
print_ok()     { echo -e "  ${GREEN}[OK]${NC} $1"; }
print_skip()   { echo -e "  ${CYAN}[SKIP]${NC} $1"; }
print_fail()   { echo -e "  ${RED}[FAIL]${NC} $1" >&2; FAILED=$((FAILED + 1)); }
print_info()   { echo -e "  ${YELLOW}[..]${NC} $1"; }

APP_API_BASE_URL="${APP_API_BASE_URL:-http://localhost:${APP_API_PORT:-8000}}"
APP_API_BASE_URL="${APP_API_BASE_URL%/}"

command -v curl >/dev/null 2>&1 || {
    echo -e "${RED}curl is required but not on PATH.${NC}" >&2
    exit 1
}

# package_name|display_name|category|is_messaging_app|is_iranian_app
# All seeded apps are active (App API create default).
APPS=(
    "org.telegram.messenger|Telegram|Messaging|false|false"
    "com.whatsapp|WhatsApp|Messaging|false|false"
    "com.instagram.android|Instagram|Social Network|false|false"
    "com.facebook.katana|Facebook|Social Network|false|false"
    "com.zhiliaoapp.musically|TikTok|Social Network|false|false"
    "com.myirancell|Myirancell|Operator|false|true"
    "ir.mci.ecareapp|MyMCI|Operator|false|true"
    "com.shatelland.namava.mobile|Namava|Video Streaming|false|true"
    "com.likotv|Lenz|Video Streaming|false|true"
    "ir.tamashakhonehtv|Tamasha Khoneh|Video Streaming|false|true"
    "com.plus9.fandogh|Fandogh|Word Game|false|true"
    "com.BrainLadder.AmirzaGP|Amirza|Word Game|false|true"
    "com.plus9.samavar|Samavar|Word Game|false|true"
    "ir.android.baham|Baham|Dating Chat|true|true"
    "app.pinno|Pinno|Dating Chat|true|true"
)

print_header "Seed applications"
print_info "POST ${APP_API_BASE_URL}/api/applications/ (${#APPS[@]} apps)"

for row in "${APPS[@]}"; do
    IFS='|' read -r package_name display_name category is_messaging_app is_iranian_app <<<"$row"

    payload="$(printf \
        '{"package_name":"%s","display_name":"%s","category":"%s","is_messaging_app":%s,"is_iranian_app":%s}' \
        "$package_name" "$display_name" "$category" \
        "$is_messaging_app" "$is_iranian_app")"

    http_code="$(
        curl -sS -o /dev/null -w '%{http_code}' \
            -X POST "${APP_API_BASE_URL}/api/applications/" \
            -H 'Content-Type: application/json' \
            -d "$payload" \
            2>/dev/null
    )" || http_code="000"

    case "$http_code" in
        201) print_ok   "${package_name} (HTTP ${http_code})" ;;
        409) print_skip "${package_name} (HTTP ${http_code} duplicate)" ;;
        *)   print_fail "${package_name} (HTTP ${http_code})" ;;
    esac
done

echo ""
if [[ "$FAILED" -gt 0 ]]; then
    echo -e "${RED}Seed finished with ${FAILED} failure(s).${NC}" >&2
    exit 1
fi

echo -e "${GREEN}Seed finished successfully.${NC}"
exit 0
