# Crawler

Play Store crawler subsystem: fetches active apps from App API, scrapes
stats and reviews, and publishes them to Kafka on an hourly schedule.

## Run locally

From the project root (with host-side `.env`: `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`):

```bash
python -m crawler.main
```

Manual end-to-end checklist: [docs/crawler_e2e_test.md](../docs/crawler_e2e_test.md).

## Docker

Infra + Kafka UI:

```bash
docker compose up -d postgres zookeeper kafka kafka-ui
```

Kafka UI: [http://localhost:8080](http://localhost:8080) — browse topics `app-stats` / `reviews`.

Crawler container (optional for E2E; host `python -m crawler.main` is easier for logs):

```bash
docker compose up -d --build crawler
```

Inside compose, Kafka is `kafka:29092`. App API is reached via
`host.docker.internal:8000` until Django is added as a compose service.

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
