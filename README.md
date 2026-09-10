# PlayPulse

Play Store analytics pipeline: App API, crawler, Kafka, storage consumer, and an on-demand network analyzer.

## Quick start (Compose)

Prerequisites: Docker Engine and the Compose plugin.

```bash
./scripts/bring_up.sh
```

That script prepares `.env` files, starts the Compose stack, waits for readiness, and runs the infra smoke check. See `./scripts/bring_up.sh --help` for `--infra-only` and other options.

| After bring-up | URL / command |
| --- | --- |
| App API (Swagger) | http://localhost:8000/swagger/ |
| Kafka UI | http://localhost:8080 |
| Infra check | `./scripts/infra/check_infra.sh` |
| Storage consumer | `./scripts/verify/verify_storage_consumer.sh` |
| Cycle reports (optional) | `./scripts/bring_up.sh --with-cycle-reports` → `cycle-reporter` service |

App API runs as the `app-api` Compose service (migrations on start). For a host/venv Django process instead, use `./scripts/bring_up.sh --infra-only` and see `docs/setup.md`.

### Optional cycle reports

Off by default. One script (`scripts/reports/finalize_cycle_reports.py`) is mounted into a
Compose service that reuses the storage-consumer image — not a new subsystem.

```bash
./scripts/bring_up.sh --with-cycle-reports
docker compose logs -f cycle-reporter
tail -n 20 data/reports/cycles.jsonl
```

## Layout

| Path | Role |
| --- | --- |
| `scripts/bring_up.sh` | Shell install / bring-up |
| `scripts/` | Ops helpers — see [`scripts/README.md`](scripts/README.md) |
| `scripts/infra/` | Topics + infra smoke check |
| `scripts/verify/` | Storage / network verify helpers |
| `scripts/network/` | PCAP batch ingestion |
| `scripts/reports/` | Cycle-report finalizer |
| `ansible/` | Future Ansible automation (not used yet) |
| `docs/setup.md` | Host vs Docker env rules |

## Subsystems

- `app_api/` — application registry (Django)
- `crawler/` — Play Store scrape → Kafka
- `storage_consumer/` — Kafka → Postgres
- `network_analyzer/` — pcap → Kafka (started by `bring_up.sh` via Compose profile `tools`; also `docker compose --profile tools up network-analyzer`)
