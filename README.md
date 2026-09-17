# PlayPulse

> **Persian documentation series:** [https://payamdivsalar.github.io/PlayPulse/](https://payamdivsalar.github.io/PlayPulse/)

PlayPulse is a modular analytics pipeline for Google Play applications. It crawls store metadata and reviews, analyzes PCAPdroid network captures, persists everything through Kafka into PostgreSQL, and optionally labels review sentiment — with Docker Compose for local/dev stacks and Ansible for production Ubuntu deployment.

---

## Architecture

```
                    ┌─────────────┐
  App registry ────►│   App API   │◄──── package validation
  (Django/REST)     └──────┬──────┘
                           │
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                 ▼
   ┌──────────┐    ┌───────────────┐   ┌────────────┐
   │ Crawler  │    │   Network     │   │ Sentiment  │
   │ (hourly) │    │   Analyzer    │   │ (batch)    │
   └────┬─────┘    └───────┬───────┘   └─────┬──────┘
        │                  │                 │
        ▼                  ▼                 │
   Kafka topics:     network-metrics         │
   app-stats / reviews                       │
        │                  │                 │
        └────────┬─────────┘                 │
                 ▼                           ▼
        ┌─────────────────┐           PostgreSQL
        │ Storage Consumer│──────────► (apps, stats,
        └─────────────────┘             reviews, network)
                 │
                 ▼
            Metabase (BI)
```

| Subsystem | Role |
| --- | --- |
| **App API** (`app_api/`) | Application registry — source of truth for registered packages |
| **Crawler** (`crawler/`) | Scrapes Play Store stats & reviews → Kafka (`app-stats`, `reviews`) |
| **Network Analyzer** (`network_analyzer/`) | PCAP → quality/efficiency metrics → Kafka (`network-metrics`) |
| **Storage Consumer** (`storage_consumer/`) | Kafka → PostgreSQL with at-least-once delivery + idempotent writes |
| **Sentiment** (`sentiment/`) | Batch NLP job writing `reviews.sentiment` (POSITIVE / NEUTRAL / NEGATIVE) |

Design highlights:

- **Kafka as the integration bus** — producers never write analytics tables directly
- **Single writer per schema concern** — storage consumer owns its tables; sentiment alone owns `reviews.sentiment`
- **Idempotent persistence** — natural keys + `ON CONFLICT`; offsets commit only after a successful DB commit
- **Dead-letter handling** — poison messages are recorded and skipped without stalling a topic

---

## Prerequisites

- Docker Engine + Compose plugin
- (Optional) Python 3.11+ for host/venv development
- (Production) Ansible on a control node; SSH + sudo on an Ubuntu target

---

## Quick start

From the repository root:

```bash
./scripts/bring_up.sh
```

The script prepares missing `.env` files (never overwrites existing ones), starts the Compose stack in stages, waits for readiness, and runs an infrastructure smoke check.

Useful variants:

```bash
./scripts/bring_up.sh --infra-only          # Postgres / ZooKeeper / Kafka / Kafka UI / Metabase
./scripts/bring_up.sh --with-cycle-reports  # enable crawl+persist cycle reporting
./scripts/bring_up.sh --skip-check          # skip infra smoke check
./scripts/bring_up.sh --help
```

| After bring-up | URL / command |
| --- | --- |
| App API (Swagger) | http://localhost:8000/swagger/ |
| Kafka UI | http://localhost:8080 |
| Metabase | http://localhost:3000 |
| Infra smoke check | `./scripts/infra/check_infra.sh` |
| Storage consumer verify | `./scripts/verify/verify_storage_consumer.sh` |

Seed a few apps into the registry:

```bash
./scripts/seed_apps.sh
```

---

## Repository layout

| Path | Purpose |
| --- | --- |
| `app_api/` | Django application registry |
| `crawler/` | Play Store crawler |
| `network_analyzer/` | PCAP analysis → Kafka |
| `storage_consumer/` | Kafka → PostgreSQL consumer |
| `sentiment/` | Review sentiment batch job |
| `scripts/` | Bring-up, infra, verify, PCAP batch, reports, Docker tests |
| `ansible/` | Production deploy (Docker install, clone, `.env` from Vault, bring-up) |
| `docs/` | Setup notes, Docker test guide, Persian HTML docs source |
| `docker-compose.yml` | Full stack definition |
| `docker-compose.test.yml` | Isolated test overlay (`playpulse_test`) |

Environment rules (host `localhost` vs Compose service hostnames) are documented in [`docs/setup.md`](docs/setup.md).

---

## Common operations

```bash
# Kafka topics (also created by kafka-init on compose up)
./scripts/infra/create_topics.sh

# Batch-analyze PCAPdroid captures in data/pcap/inbox
./scripts/network/analyze_pcaps.sh

# Confirm network-metrics messages landed in Kafka
./scripts/verify/verify_network_metrics.sh --latest

# Sentiment job (Compose profile: tools)
docker compose --profile tools build sentiment
docker compose run --rm sentiment run
```

Operator script reference: [`scripts/README.md`](scripts/README.md).

---

## Testing

Isolated Docker tests — separate Compose project (`playpulse_test`), separate `.env.test`, no touch of the live stack:

```bash
./scripts/run_tests_in_docker.sh
./scripts/run_tests_in_docker.sh --keep     # leave the test stack up for iteration
./scripts/run_tests_in_docker.sh --down     # tear down a kept stack
```

Details: [`docs/docker-tests.md`](docs/docker-tests.md).

Per-subsystem unit tests can also be run from a host venv (see each subsystem’s README).

---

## Production deployment

Ansible installs Docker, syncs the repo, renders `.env` from an encrypted Vault file, then runs the same bring-up and smoke-check scripts used locally.

```bash
cd ansible
cp inventory.ini.example inventory.ini   # set server IP / SSH user
# configure group_vars/vault.yml from vault.yml.example
ansible-playbook playbook.yml
```

Full guide: [`ansible/README.md`](ansible/README.md).

---

## Documentation

| Resource | Description |
| --- | --- |
| **[Persian doc series](https://payamdivsalar.github.io/PlayPulse/)** | Architecture, schema, crawler, network analyzer, storage, sentiment, Ansible, scripts |
| [`docs/setup.md`](docs/setup.md) | Host vs Docker environment layout |
| [`docs/docker-tests.md`](docs/docker-tests.md) | Isolated test stack |
| Subsystem READMEs | `crawler/`, `storage_consumer/`, `network_analyzer/`, `sentiment/`, `ansible/`, `scripts/` |
