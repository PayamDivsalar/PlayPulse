# Scripts

Operator helpers for PlayPulse. Run them from the **repo root**.

| Folder | What it’s for |
| --- | --- |
| [`bring_up.sh`](bring_up.sh) | First-time / everyday stack start |
| [`infra/`](infra/) | Postgres + Kafka health, topic layout |
| [`verify/`](verify/) | Prove crawls / analyzer output actually landed |
| [`network/`](network/) | Batch-ingest PCAPdroid captures |
| [`reports/`](reports/) | Finalize hourly crawl+persist cycle reports |

---

## `bring_up.sh`

**What it does:** Prepares missing `.env` files (never overwrites existing ones), fills blank Postgres credentials, starts Docker Compose, waits until services are ready, then runs the infra smoke check.

**When to use:** Fresh machine, server install, or whenever you want the full stack up with one command.

```bash
./scripts/bring_up.sh                      # full stack
./scripts/bring_up.sh --infra-only         # Postgres / ZooKeeper / Kafka / UI only
./scripts/bring_up.sh --with-cycle-reports # also enable crawler pending reports + cycle-reporter
./scripts/bring_up.sh --skip-check         # skip infra smoke check
./scripts/bring_up.sh --no-build           # reuse images already built
./scripts/bring_up.sh --help
```

---

## `infra/check_infra.sh`

**What it does:** Smoke-tests that Postgres and Kafka are up and that basic write/read works. It does **not** check application data quality.

**When to use:** After `bring_up`, after infra restarts, or before blaming the crawler/consumer.

```bash
./scripts/infra/check_infra.sh
```

Needs root `.env` and containers already running (`postgres`, `zookeeper`, `kafka`).

---

## `infra/create_topics.sh`

**What it does:** Creates `app-stats`, `reviews`, and `network-metrics` with a chosen partition count (default **3**). Compose `kafka-init` already does this on `up`; this script is for **widening** topics later (partition count can only go up).

**When to use:** Manual topic setup, or scaling reviews/network to more partitions before adding consumers.

```bash
./scripts/infra/create_topics.sh
./scripts/infra/create_topics.sh --topic reviews --partitions 6 --alter
./scripts/infra/create_topics.sh --help
```

Needs the Kafka container (`project_kafka`) running.

---

## `verify/verify_storage_consumer.sh`

**What it does:** End-to-end check that storage-consumer is healthy and persisting: schema present, container healthy, Kafka lag, row counts, recent dead letters.

**When to use:** After a crawl cycle, after deploying storage-consumer, or when investigating “Kafka has messages but DB looks empty.”

```bash
./scripts/verify/verify_storage_consumer.sh
./scripts/verify/verify_storage_consumer.sh --dead-letters 25
./scripts/verify/verify_storage_consumer.sh --help
```

Needs `postgres`, `kafka`, and `storage-consumer` up.

---

## `verify/verify_network_metrics.sh`

**What it does:** Reads messages from the `network-metrics` Kafka topic so you can confirm the analyzer published something (payload dump or count-only).

**When to use:** After running `analyze_pcaps.sh` (or the Compose network-analyzer batch), before digging into DB/consumer issues.

```bash
./scripts/verify/verify_network_metrics.sh
./scripts/verify/verify_network_metrics.sh --count-only
./scripts/verify/verify_network_metrics.sh --latest --max-messages 1 --timeout 60
./scripts/verify/verify_network_metrics.sh --help
```

Needs Kafka running.

---

## `network/analyze_pcaps.sh`

**What it does:** Batch-runs the network analyzer over every capture in an inbox. On success → `processed/`; on rejected input → `failed/` (+ `.log`); on retryable errors (e.g. broker down) → left in the inbox.

Expected filename form:

` <package>__<upload|download>__<YYYYMMDDTHHMMSS>.pcap `

**When to use:** Host-side batch over `data/pcap/inbox` (Compose also ships a `network-analyzer` service that does the same job in-container).

```bash
./scripts/network/analyze_pcaps.sh
./scripts/network/analyze_pcaps.sh --dry-run --skip-registry-check --keep
./scripts/network/analyze_pcaps.sh --inbox ~/Downloads/PCAPdroid
./scripts/network/analyze_pcaps.sh --help
```

Needs analyzer Python deps; for a real publish, Kafka + App API must be reachable unless you pass `--dry-run` / `--skip-registry-check`.

---

## `reports/finalize_cycle_reports.py`

**What it does:** Watches `data/reports/pending/*.json` (written by the crawler when cycle reports are enabled). For each file it waits until storage-consumer Kafka lag is **0**, appends one line to `data/reports/cycles.jsonl`, and moves the file to `done/`.

**When to use:** Usually you don’t run this by hand — Compose service `cycle-reporter` mounts and loops it. Enable with:

```bash
./scripts/bring_up.sh --with-cycle-reports
docker compose logs -f cycle-reporter
tail -n 20 data/reports/cycles.jsonl
```

Host one-shot / loop (stack must be up; Kafka on `localhost:9092` from the host):

```bash
./scripts/reports/finalize_cycle_reports.py
./scripts/reports/finalize_cycle_reports.py --loop --interval 60
```

---

## Typical flows

**Fresh server / full stack**

```bash
./scripts/bring_up.sh --with-cycle-reports
./scripts/infra/check_infra.sh          # already run by bring_up unless --skip-check
./scripts/verify/verify_storage_consumer.sh
```

**After a crawl**

```bash
docker logs project_crawler 2>&1 | grep 'Crawl cycle summary'
./scripts/verify/verify_storage_consumer.sh
tail -n 5 data/reports/cycles.jsonl    # if cycle reports enabled
```

**Network captures**

```bash
# drop files into data/pcap/inbox, then either:
./scripts/network/analyze_pcaps.sh
# or: docker compose --profile tools run --rm network-analyzer
./scripts/verify/verify_network_metrics.sh --count-only
```
