# Crawler

Play Store crawler subsystem: fetches active apps from App API, scrapes
stats and reviews, and publishes them to Kafka on an hourly schedule.

## Run locally

Copy env for host/venv (not Docker hostnames):

```bash
cp crawler/.env.example crawler/.env
```

From the project root:

```bash
python -m crawler.main
```

`crawler/.env` should use `KAFKA_BOOTSTRAP_SERVERS=localhost:9092` and
`APP_API_BASE_URL=http://127.0.0.1:8000`. Why this differs from Docker:
[docs/setup.md](../docs/setup.md).

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

Container addresses come from `docker-compose.yml` `environment:`
(`kafka:29092`, `host.docker.internal:8000`) — not from `crawler/.env`.

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
- Kafka up via docker compose, and `crawler/.env` with
  `KAFKA_BOOTSTRAP_SERVERS=localhost:9092`
- App API reachable via `APP_API_BASE_URL` in `crawler/.env`
  (for example `http://127.0.0.1:8000` with `python manage.py runserver`)
