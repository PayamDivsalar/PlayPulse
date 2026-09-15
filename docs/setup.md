# تنظیمات محیطی پروژه (venv در برابر Docker)

این سند تصمیم آگاهانه‌ی تفکیک فایل‌های `.env` را توضیح می‌دهد تا آدرس‌های
شبکه بین اجرای محلی و کانتینر قاطی نشوند.

## دو نوع متغیر

1. **مقادیر یکسان در همه محیط‌ها** (نام دیتابیس، رمز، پورت publish شده)  
   این‌ها فقط برای جایگزینی `${VAR}` داخل `docker-compose.yml` لازم‌اند و در
   **`.env` ریشه پروژه** نگه داشته می‌شوند. خودِ `docker compose` به‌طور
   پیش‌فرض فقط همین فایل کنار compose را می‌خواند.

2. **آدرس‌های وابسته به محیط اجرا** (hostname سرویس‌ها)  
   این‌ها بین venv و Docker فرق می‌کنند و **نباید** از یک فایل مشترک برای هر
   دو محیط خوانده شوند:
   - روی host/venv: `localhost` / `127.0.0.1`
   - داخل شبکه compose: نام سرویس (`kafka`, `postgres`, ...)

## چیدمان فایل‌ها

```text
project-root/
  .env                 # فقط جایگزینی compose (POSTGRES_*, KAFKA_*_PORT, APP_API_PORT, ...)
  app_api/.env         # فقط اجرای venv جنگو روی host (مثلاً POSTGRES_HOST=127.0.0.1)
                       # کانتینر app-api این فایل را نمی‌خواند
  crawler/.env         # فقط اجرای venv کراولر
                       #   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
                       #   APP_API_BASE_URL=http://127.0.0.1:8000
  network_analyzer/.env  # فقط اجرای venv آنالایزر
  storage_consumer/.env  # فقط اجرای venv مصرف‌کننده‌ی استوریج
                       #   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
                       #   POSTGRES_HOST=127.0.0.1
```

نمونه‌ها: `.env.example` در ریشه، `crawler/.env.example`،
`app_api/.env.example`، `network_analyzer/.env.example`،
`storage_consumer/.env.example`.

`storage_consumer` تنها زیرسیستمی است که **هم** به Kafka و **هم** به
PostgreSQL وصل می‌شود، پس هر دو دسته آدرس را دارد و همین آن را به بهترین
مثال این تفکیک تبدیل می‌کند: روی host مقدار `localhost:9092` و
`127.0.0.1` و داخل compose مقدار `kafka:29092` و `postgres`.

نام متغیرهای `POSTGRES_*` عیناً همان‌های `app_api` هستند؛ این عمدی است چون
هر دو به یک دیتابیس وصل می‌شوند. مالکیت جدول‌ها اما تفکیک‌شده است:
جنگو مالک `applications` است و `storage_consumer` مالک چهار جدول خودش
(برای جزئیات: `storage_consumer/README.md`).

## Docker چطور override می‌کند؟

برای سرویس‌های `app-api`، `crawler` و `storage-consumer`، مقادیر مخصوص
کانتینر **مستقیم** در `docker-compose.yml` → `environment:` تعریف شده‌اند
(مثلاً `POSTGRES_HOST: postgres`، `KAFKA_BOOTSTRAP_SERVERS: kafka:29092`،
`APP_API_BASE_URL: http://app-api:8000`). از `env_file: */.env` استفاده
**نمی‌کنیم**، چون همان فایل برای host است و hostname اشتباه به کانتینر
می‌دهد.

`python-dotenv` متغیرهایی را که از قبل در محیط process ست شده‌اند
بازنویسی نمی‌کند؛ پس داخل کانتینر، مقادیر compose مقدم‌اند.

`app-api` روی استارت مهاجرت‌های Django را اعمال می‌کند و `/healthz/` را
برای readiness در اختیار compose می‌گذارد. کراولر و `storage-consumer`
با `depends_on: condition: service_healthy` منتظر آن می‌مانند.

## تنظیمات runtime کراولر

`main.py` با `load_settings()` یک شیء immutable به نام `Settings` می‌سازد و
آن را به clientها تزریق می‌کند. پیش‌فرض tunables داخل همین dataclass است؛
فقط آدرس‌های اتصال (Kafka / App API) برای اجرای host در `crawler/.env`
الزامی‌اند.

```bash
cp .env.example .env
cp crawler/.env.example crawler/.env
cp app_api/.env.example app_api/.env
# سپس مقادیر هر فایل را پر کنید
```

اجرای کراولر روی host:

```bash
python -m crawler.main
```

اجرای کراولر در Docker (آدرس‌ها از compose می‌آیند):

```bash
docker compose up -d --build crawler
```

## Metabase

متابیس در `docker-compose.yml` تعریف شده و دیتابیس اختصاصی برنامه‌ی خودش
(`metabase_app_db`) بدون هیچ قدم دستی ساخته می‌شود، از دو مسیر مکمل:

- **volume کاملاً جدید:** `postgres/init/01-create-metabase-db.sh` که به
  `/docker-entrypoint-initdb.d` ماونت شده، طبق رفتار مستند ایمیج رسمی postgres
  فقط در اولین استارتِ کانتینر با دیتای خالی اجرا می‌شود و دیتابیس را می‌سازد.
- **volume موجود** (مثلاً سروری که پیش از وجود این اسکریپت مستقر شده و
  دیتابیس در آن دستی ساخته شده بود): گام idempotent در `scripts/bring_up.sh`
  بعد از سالم شدن Postgres، وجود دیتابیس را بررسی و در صورت نبود می‌سازد؛
  روی volume تازه هم بی‌اثر است (فقط تأیید می‌کند).

آدرس دسترسی: **http://localhost:3000** (با `METABASE_PORT` در `.env` قابل تغییر).

متابیس بخشی از توالی استاندارت راه‌اندازی است: `scripts/bring_up.sh` آن را در
کنار سرویس‌های زیرساخت (پایگاه‌داده، Kafka و...) بالا می‌آورد (حتی در حالت
`--infra-only`)، بعد از اطمینان از سالم بودن Postgres و موجود بودن
`metabase_app_db`، منتظر سالم شدن خودِ متابیس هم می‌ماند. healthcheck آن در
`docker-compose.yml` روی نقطه‌ی `GET /api/health` تعریف شده است (و `start_period`
بافر مناسبی برای استارت JVM و مهاجرت‌های اولیه‌ی Liquibase دارد)؛ اولین اجرا ممکن
است یکی-دو دقیقه طول بکشد تا وضعیت `healthy` را نشان دهد.

دو کار اولیه باید **دستی و از طریق مرورگر** انجام شود، چون متابیس برای
مرحله‌ی setup اولیه API عمومی ندارد:

1. ساخت اولین اکانت Admin در اولین ورود.
2. اتصال متابیس به **دیتابیس اصلی پروژه** (نه دیتابیس داخلی خودش، یعنی نه
   `metabase_app_db`) با پارامترهای اتصال زیر:

   | پارامتر | مقدار |
   |---|---|
   | Host | `postgres` |
   | Port | `5432` |
   | Database | مقدار `POSTGRES_DB` در `.env` |
   | User | مقدار `POSTGRES_USER` در `.env` |
   | Password | مقدار `POSTGRES_PASSWORD` در `.env` |

   توجه: این آدرس‌ها فقط داخل شبکه compose معتبرند؛ متابیس خودش داخل همان
   شبکه اجرا می‌شود.
