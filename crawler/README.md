# Crawler

Play Store crawler subsystem: fetches active apps from App API, scrapes
stats and reviews, and publishes them to Kafka.

## Running tests

Unit tests (mocked; no external services required):

```bash
pytest crawler/tests/ -v -m "not live"
```

Live tests (real network / Kafka / App API; run manually only):

```bash
pytest crawler/tests/ -v -m "live"
```

Live tests expect:

- Internet access (Play Store)
- Kafka up via docker compose, and `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
  in the project-root `.env` (host clients cannot resolve `kafka:29092`)
- App API reachable via `APP_API_BASE_URL` (for example
  `http://localhost:8000` with `python manage.py runserver`)
