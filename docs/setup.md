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
  .env                 # فقط جایگزینی compose (POSTGRES_*, KAFKA_*_PORT, ...)
  app_api/.env         # فقط اجرای venv جنگو (مثلاً POSTGRES_HOST=127.0.0.1)
  crawler/.env         # فقط اجرای venv کراولر
                       #   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
                       #   APP_API_BASE_URL=http://127.0.0.1:8000
```

نمونه‌ها: `.env.example` در ریشه، `crawler/.env.example`، `app_api/.env.example`.

## Docker چطور override می‌کند؟

برای سرویس `crawler`، مقادیر مخصوص کانتینر **مستقیم** در
`docker-compose.yml` → `environment:` تعریف شده‌اند (مثلاً
`KAFKA_BOOTSTRAP_SERVERS: kafka:29092`). از `env_file: crawler/.env`
استفاده **نمی‌کنیم**، چون همان فایل برای host است و hostname اشتباه به
کانتینر می‌دهد.

`python-dotenv` متغیرهایی را که از قبل در محیط process ست شده‌اند
بازنویسی نمی‌کند؛ پس داخل کانتینر، مقادیر compose مقدم‌اند.

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
