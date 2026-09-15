#!/usr/bin/env bash

# =============================================================================
# check_infra.sh
#
# اسکریپت Smoke Test برای بررسی سلامت زیرساخت پایه‌ی پروژه (Postgres و Kafka
# و Metabase)
#
# این اسکریپت هیچ فرضی درباره‌ی صحت داده نمی‌کند؛ فقط بررسی می‌کند که سرویس‌ها
# بالا آمده‌اند، در دسترس هستند، و می‌توان به آن‌ها متصل شد و روی آن‌ها عملیات
# پایه (write/read) انجام داد.
#
# نحوه‌ی اجرا:
#   ./scripts/infra/check_infra.sh
#
# پیش‌نیاز: فایل .env در ریشه‌ی پروژه موجود باشد و سرویس‌ها با
#   docker compose up -d postgres zookeeper kafka
# بالا آمده باشند.
# =============================================================================

set -uo pipefail

# --- تنظیمات ظاهری (رنگ‌ها برای خوانایی بهتر خروجی) -------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# --- متغیرهای وضعیت کلی ------------------------------------------------------
FAILED_CHECKS=0

# --- توابع کمکی --------------------------------------------------------------

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

print_info() {
    echo -e "  ${YELLOW}[..]${NC} $1"
}

# بررسی این‌که آیا یک کانتینر در حال اجراست
container_is_running() {
    local container_name="$1"
    docker inspect -f '{{.State.Running}}' "$container_name" 2>/dev/null | grep -q "true"
}

# --- بارگذاری متغیرهای محیطی -------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ENV_FILE="$PROJECT_ROOT/.env"

if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    set -a
    source "$ENV_FILE"
    set +a
else
    echo -e "${RED}فایل .env در مسیر $ENV_FILE پیدا نشد.${NC}"
    echo "ابتدا دستور زیر را اجرا کنید و مقادیر را تنظیم کنید:"
    echo "  cp .env.example .env"
    exit 1
fi

POSTGRES_CONTAINER="project_postgres"
KAFKA_CONTAINER="project_kafka"
METABASE_CONTAINER="project_metabase"
TEST_TOPIC="infra-smoke-test"
TEST_MESSAGE="hello-from-check-infra-$(date +%s)"

echo -e "${BLUE}"
echo "╔══════════════════════════════════════════════╗"
echo "║        بررسی سلامت زیرساخت پروژه            ║"
echo "╚══════════════════════════════════════════════╝"
echo -e "${NC}"

# =============================================================================
# بخش ۱: بررسی PostgreSQL
# =============================================================================
print_header "PostgreSQL"

if container_is_running "$POSTGRES_CONTAINER"; then
    print_ok "کانتینر $POSTGRES_CONTAINER در حال اجراست."
else
    print_fail "کانتینر $POSTGRES_CONTAINER در حال اجرا نیست."
fi

print_info "در حال بررسی آماده بودن Postgres برای اتصال (pg_isready)..."
RETRIES=10
until docker exec "$POSTGRES_CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1 || [[ $RETRIES -eq 0 ]]; do
    sleep 2
    RETRIES=$((RETRIES - 1))
done

if docker exec "$POSTGRES_CONTAINER" pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" >/dev/null 2>&1; then
    print_ok "Postgres آماده‌ی پذیرش اتصال است."
else
    print_fail "Postgres پس از چند بار تلاش هنوز آماده نیست."
fi

print_info "در حال تست ایجاد و خواندن یک جدول موقت..."
CREATE_TEST_SQL="
CREATE TABLE IF NOT EXISTS infra_smoke_test (id SERIAL PRIMARY KEY, note TEXT);
INSERT INTO infra_smoke_test (note) VALUES ('smoke-test-ok');
SELECT note FROM infra_smoke_test ORDER BY id DESC LIMIT 1;
DROP TABLE infra_smoke_test;
"

TEST_RESULT=$(docker exec -i "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c "$CREATE_TEST_SQL" 2>&1)

if echo "$TEST_RESULT" | grep -q "smoke-test-ok"; then
    print_ok "عملیات نوشتن و خواندن در Postgres با موفقیت انجام شد."
else
    print_fail "عملیات نوشتن/خواندن در Postgres ناموفق بود."
    echo "$TEST_RESULT" | sed 's/^/      /'
fi

# =============================================================================
# بخش ۲: بررسی Kafka
# =============================================================================
print_header "Kafka"

if container_is_running "$KAFKA_CONTAINER"; then
    print_ok "کانتینر $KAFKA_CONTAINER در حال اجراست."
else
    print_fail "کانتینر $KAFKA_CONTAINER در حال اجرا نیست."
fi

print_info "در حال بررسی در دسترس بودن بروکر Kafka..."
RETRIES=10
until docker exec "$KAFKA_CONTAINER" kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 || [[ $RETRIES -eq 0 ]]; do
    sleep 2
    RETRIES=$((RETRIES - 1))
done

if docker exec "$KAFKA_CONTAINER" kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    print_ok "بروکر Kafka در دسترس است."
else
    print_fail "بروکر Kafka پس از چند بار تلاش در دسترس نیست."
fi

print_info "در حال ساخت topic تستی ($TEST_TOPIC)..."
docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$TEST_TOPIC" \
    --partitions 1 \
    --replication-factor 1 >/dev/null 2>&1

TOPIC_LIST=$(docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server localhost:9092 --list 2>&1)
if echo "$TOPIC_LIST" | grep -q "$TEST_TOPIC"; then
    print_ok "Topic تستی با موفقیت ساخته شد."
else
    print_fail "ساخت topic تستی ناموفق بود."
fi

print_info "در حال ارسال یک پیام تستی به Kafka (Produce)..."
echo "$TEST_MESSAGE" | docker exec -i "$KAFKA_CONTAINER" kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic "$TEST_TOPIC" >/dev/null 2>&1

if [[ $? -eq 0 ]]; then
    print_ok "پیام تستی با موفقیت ارسال شد."
else
    print_fail "ارسال پیام تستی به Kafka ناموفق بود."
fi

print_info "در حال خواندن پیام تستی از Kafka (Consume)..."
CONSUMED_MESSAGE=$(timeout 15 docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "$TEST_TOPIC" \
    --from-beginning \
    --max-messages 1 2>/dev/null)

if [[ "$CONSUMED_MESSAGE" == *"$TEST_MESSAGE"* ]]; then
    print_ok "پیام تستی با موفقیت خوانده شد و با مقدار ارسالی مطابقت دارد."
else
    print_fail "پیام خوانده‌شده با پیام ارسالی مطابقت ندارد یا خوانده نشد."
    echo "      دریافت شد: $CONSUMED_MESSAGE"
fi

print_info "در حال حذف topic تستی (پاکسازی)..."
docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --delete --topic "$TEST_TOPIC" >/dev/null 2>&1
print_ok "پاکسازی انجام شد."

# =============================================================================
# بخش ۳: بررسی Metabase
# =============================================================================
print_header "Metabase"

if container_is_running "$METABASE_CONTAINER"; then
    print_ok "کانتینر $METABASE_CONTAINER در حال اجراست."
else
    print_fail "کانتینر $METABASE_CONTAINER در حال اجرا نیست."
fi

print_info "در حال بررسی سلامت Metabase (GET /api/health)..."
METABASE_HEALTH=$(docker exec "$METABASE_CONTAINER" curl -fsS http://localhost:3000/api/health 2>&1)

if echo "$METABASE_HEALTH" | grep -q '"status":"ok"'; then
    print_ok "Metabase سالم است و به درخواست‌ها پاسخ می‌دهد."
else
    print_fail "پاسخ /api/health سالم نیست (یا در دسترس نیست)."
    echo "      دریافت شد: $METABASE_HEALTH"
fi

# =============================================================================
# جمع‌بندی نهایی
# =============================================================================
print_header "نتیجه نهایی"

if [[ $FAILED_CHECKS -eq 0 ]]; then
    echo -e "  ${GREEN}تمام بررسی‌ها با موفقیت انجام شد. زیرساخت آماده است.${NC}"
    exit 0
else
    echo -e "  ${RED}$FAILED_CHECKS مورد از بررسی‌ها ناموفق بود. لطفاً لاگ‌های بالا را بررسی کنید.${NC}"
    exit 1
fi