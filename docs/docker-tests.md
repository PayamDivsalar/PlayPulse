# Docker tests (isolated stack)

How to run PlayPulse’s subsystem tests in Docker **without touching** the live
or production Compose stack (its volumes, containers, or root `.env`).

For day-to-day deploy health on a server, use `scripts/infra/check_infra.sh`
(and Ansible). Do **not** use this test runner as a production health check.

---

## What we set up

Three pieces keep tests away from the live stack:

| Piece | Role |
| --- | --- |
| `COMPOSE_PROJECT_NAME=playpulse_test` | Separate Docker network and volumes (e.g. `playpulse_test_postgres_data`, not the live `postgres_data`) |
| `.env.test` | Credentials / `${VAR}` substitution for the test stack only — never the root `.env` used by `bring_up` / Ansible |
| `docker-compose.test.yml` | Clears fixed `container_name`s (`project_postgres`, …) and published host ports so the test stack can sit beside a live stack on the same machine |

The same `docker-compose.yml` and `test`-profile services are reused. There is
no second copy of the stack definition and no Testcontainers dependency.

`scripts/run_tests_in_docker.sh` always wires those three together. Waiting for
readiness uses `docker compose … exec` / service ids under `playpulse_test`,
not the live names `project_postgres` / `project_kafka`.

---

## One-time setup

`.env.test` is created automatically on first run if missing:

```bash
./scripts/run_tests_in_docker.sh
# → copies .env.test.example → .env.test (never overwrites an existing file)
```

You can still create it yourself with `cp .env.test.example .env.test`. The
example defaults (`playpulse_test` / `playpulse_test`) are fine for local and
CI. They do not need to match production secrets. `.env.test` is gitignored.

Optional (heavy): build the sentiment image once if you want those suites to
run instead of SKIPPED:

```bash
docker compose --profile tools build sentiment
```

---

## Usage

### Run all Docker test suites

```bash
./scripts/run_tests_in_docker.sh
```

When the script exits (success, failure, or abort after infra started), it
**automatically** tears down the `playpulse_test` stack — containers and
volumes. Nothing is left running.

Reuse images already built:

```bash
./scripts/run_tests_in_docker.sh --no-build
```

### Keep the stack for faster re-runs

Leave infra up (e.g. while debugging one suite):

```bash
./scripts/run_tests_in_docker.sh --keep
# … iterate …
./scripts/run_tests_in_docker.sh --no-build --keep
# when done:
./scripts/run_tests_in_docker.sh --down
```

### Tear down only

```bash
./scripts/run_tests_in_docker.sh --down
```

### Run a single suite (same isolation)

```bash
docker compose -p playpulse_test \
  --env-file .env.test \
  -f docker-compose.yml -f docker-compose.test.yml \
  --profile test run --rm storage-consumer-tests
```

Prefer the script for the full matrix; use the manual form when debugging one
service. Remember that a manual `run` does not start or tear down infra —
bring infra up with `--keep` first, or start the needed services yourself.

---

## What runs

The script starts only test infra under `playpulse_test`:

- `postgres`, `zookeeper`, `kafka`, `kafka-init`, `app-api`

Then it runs each `*-tests` service (profile `test`), continuing after
failures, and prints PASS / FAIL / SKIPPED:

| Label | Meaning |
| --- | --- |
| PASS | Suite exited 0 |
| FAIL | Real failure |
| SKIPPED | Play Store unreachable (`crawler-tests-live`), tshark missing (oracle), or `playpulse-sentiment:latest` not built |

Notes:

- `crawler-tests-live` may hit the public Play Store (needs egress).
- `network-analyzer-tests-live` is compose-network only (Kafka / App API).
- `network-analyzer-tests-oracle` needs the tshark image (`Dockerfile.test`).
- Integration suites write into the **test** Postgres/Kafka only.

---

## Production / Ansible

| Environment | What to run |
| --- | --- |
| Dev / CI | `./scripts/run_tests_in_docker.sh` |
| Production server | `./scripts/bring_up.sh` + `./scripts/infra/check_infra.sh` only |

Ansible deploy already follows the production path. Do not add this test script
to the deploy role.

Even with isolation, avoid running long write-heavy suites on a busy prod host
(CPU/disk contention). Prefer CI or a developer machine.

---

## How to confirm you are not on the live stack

While tests are running:

```bash
docker compose -p playpulse_test ps
docker volume ls | grep playpulse_test
```

You should **not** see the test script creating or exec’ing `project_postgres`
(that name belongs to the live stack from the main compose file without the
test override).

---

## Files touched by this design

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Shared service definitions (including profile `test`) |
| `docker-compose.test.yml` | Isolation override (names / ports / restart) |
| `.env.test.example` | Template for the test env file |
| `.env.test` | Local/CI secrets (gitignored) |
| `scripts/run_tests_in_docker.sh` | Orchestrator |
| `docs/docker-tests.md` | This guide |
