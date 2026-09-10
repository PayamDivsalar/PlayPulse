# تست دستی End-to-End زیرسیستم Crawler

این سند یک مسیر دستی برای تأیید کل جریان crawler است (نه تست خودکار CI).
هدف: ثبت چند اپ در App API، اجرای یک دور crawl، و مشاهده‌ی پیام‌ها در Kafka.

## پیش‌نیاز

- `.env` ریشه فقط برای `docker compose` (نام/رمز Postgres و پورت‌ها) — جزئیات در [`docs/setup.md`](setup.md)
- `crawler/.env` برای اجرای host/venv کراولر (`KAFKA_BOOTSTRAP_SERVERS=localhost:9092`، `APP_API_BASE_URL=http://127.0.0.1:8000`)
- venv مربوط به `crawler` و `app_api`
- اینترنت برای دسترسی به Google Play

## ۱. بالا آوردن زیرساخت

از ریشه پروژه:

```bash
docker compose up -d postgres zookeeper kafka kafka-ui
```

اختیاری — smoke test زیرساخت:

```bash
./scripts/infra/check_infra.sh
```

UI کافکا بعد از بالا آمدن در آدرس زیر در مرورگر باز می‌شود:

- [http://localhost:8080](http://localhost:8080)

(اگر پورت را در `.env` با `KAFKA_UI_PORT` عوض کرده‌اید، همان را باز کنید.)

## ۲. اجرای App API و ثبت اپ‌ها

در یک ترمینال:

```bash
cd app_api
source venv/bin/activate   # یا مسیر venv خودتان
python manage.py migrate
python manage.py runserver 0.0.0.0:8000
```

حداقل ۲–۳ اپ واقعی ثبت کنید (Swagger در `/swagger/` یا curl):

```bash
curl -s -X POST http://localhost:8000/api/applications/ \
  -H 'Content-Type: application/json' \
  -d '{"package_name":"com.whatsapp","display_name":"WhatsApp"}'

curl -s -X POST http://localhost:8000/api/applications/ \
  -H 'Content-Type: application/json' \
  -d '{"package_name":"com.instagram.android","display_name":"Instagram"}'

curl -s -X POST http://localhost:8000/api/applications/ \
  -H 'Content-Type: application/json' \
  -d '{"package_name":"com.spotify.music","display_name":"Spotify"}'

curl -s 'http://localhost:8000/api/applications/?is_active=true'
```

## ۳. اجرای crawler روی host (برای مشاهده‌ی لاگ)

از ریشه پروژه، با `crawler/.env` میزبان (`localhost:9092`):

```bash
cd /path/to/PlayPulse
source crawler/venv/bin/activate
pip install -r crawler/requirements.txt   # در صورت نیاز
python -m crawler.main
```

انتظار در لاگ:

- شروع scheduler
- یک دور فوری crawl (بدون انتظار یک ساعته)
- خلاصه‌ای شبیه: `app_stats=N/N success reviews=N/N success`
- با `Ctrl+C` خاموشی تمیز

> برای این تست E2E لازم نیست crawler را داخل Docker اجرا کنید؛ اجرای host لاگ را خواناتر می‌کند.

## ۴. بررسی پیام‌ها در Kafka

### روش پیشنهادی: Kafka UI

1. مرورگر را باز کنید: [http://localhost:8080](http://localhost:8080)
2. از منوی چپ **Topics** را بزنید؛ باید `app-stats` و `reviews` را ببینید (بعد از اولین produce ساخته می‌شوند).
3. روی topic `app-stats` کلیک کنید → تب **Messages**.
4. اگر خالی بود، **Seek to** را روی `Beginning` / earliest بگذارید و Refresh کنید.
5. برای هر پیام چک کنید:
   - **Key** برابر `package_name` اپ باشد (مثلاً `com.whatsapp`)
   - **Value** JSON با فیلدهای `minInstalls`, `score`, `ratings`, `reviews`, `updated`, `version`, `adSupported`
6. همین کار را برای topic `reviews` تکرار کنید:
   - Key = `package_name`
   - Value شامل `reviewId`, `at`, `userName`, `thumbsUpCount`, `score`, `content`
   - `at` باید رشتهٔ ISO باشد (مثلاً `2021-03-25T15:52:53`)

نکته: تعداد پیام‌های `reviews` خیلی بیشتر از `app-stats` است (تا حدود ۱۰۰۰ تا به ازای هر اپ). در UI می‌توانید با فیلتر key یا محدود کردن تعداد نمایش، سریع‌تر مرور کنید.

### روش جایگزین: kafka-console-consumer

در ترمینال جدا، topic آمار اپ‌ها:

```bash
docker exec -it project_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic app-stats \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | " \
  --timeout-ms 15000
```

topic ریویوها (خروجی می‌تواند زیاد باشد؛ با timeout قطع می‌شود):

```bash
docker exec -it project_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic reviews \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | " \
  --timeout-ms 15000
```

الگو مشابه [`scripts/infra/check_infra.sh`](../scripts/infra/check_infra.sh) است: consumer داخل کانتینر `project_kafka` و bootstrap روی `localhost:9092`.

## ۵. چک‌لیست تأیید

- [ ] به ازای هر اپ active ثبت‌شده، حداقل یک پیام در `app-stats` با key برابر `package_name` دیده می‌شود
- [ ] پیام‌های `app-stats` شامل فیلدهایی مثل `minInstalls`, `score`, `ratings`, `reviews`, `updated`, `version`, `adSupported` هستند
- [ ] در topic `reviews` پیام‌هایی با key همان `package_name`ها وجود دارد
- [ ] هر پیام review شامل `reviewId`, `at`, `userName`, `thumbsUpCount`, `score`, `content` است و `at` به صورت رشته (ISO) است نه object غیرقابل‌خواندن
- [ ] در لاگ crawler خطای مکرر rate-limit / ban دیده نمی‌شود
- [ ] اگر یک اپ روی Play Store پیدا نشود، بقیه اپ‌ها همچنان پردازش می‌شوند
