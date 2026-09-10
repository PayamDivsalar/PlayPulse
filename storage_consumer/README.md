# Storage Consumer Subsystem

Consumes the `app-stats`, `reviews` and `network-metrics` Kafka topics and
writes them to PostgreSQL. It is the only subsystem that writes to the four
tables it owns, and the point at which the pipeline's data becomes durable and
queryable.

```
crawler ──────► app-stats ───────┐
                reviews  ────────┤
network-analyzer ► network-metrics ┤──► storage_consumer ──► PostgreSQL
                                        (one thread per topic)
```

## Contents

- [What it guarantees](#what-it-guarantees)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Schema ownership](#schema-ownership)
- [How it works](#how-it-works)
- [Operations](#operations)
  - [Verifying it works](#verifying-it-works)
  - [Dead-letter runbook](#dead-letter-runbook)
  - [Replaying a topic](#replaying-a-topic)
  - [Scaling a pipeline](#scaling-a-pipeline)
- [Development](#development)

## What it guarantees

**Nothing published is lost.** Offsets are committed only after PostgreSQL has
committed the rows. A crash anywhere in between means the batch is redelivered,
never skipped.

**Redelivery does not duplicate.** Every table has a `UNIQUE` constraint on the
message's natural key, and every write is an `INSERT … ON CONFLICT`. Kafka
gives at-least-once; the constraints turn that into effectively-once.

**One bad message cannot stop a topic.** A message that can never be stored —
malformed JSON, a missing required field, an unregistered `package_name` — is
recorded in `dead_letter_events` in the same transaction as the batch it
arrived with, after which the offset advances past it.

**A partial pipeline is not a running pipeline.** If any of the three threads
fails, the process exits and the container restarts. A container that keeps
running with one topic silently unconsumed is the failure mode this design
rules out.

## Quick start

With Docker Compose, from the repository root:

```bash
# The consumer needs the applications table, which Django owns.
cd app_api && python manage.py migrate && cd ..

# Create the topics with several partitions (see kafka-init in compose).
./scripts/create_topics.sh

docker compose up -d storage-consumer
docker compose logs -f storage-consumer

./scripts/verify_storage_consumer.sh
```

The container applies its own migrations on every start, so there is no
separate setup step. Both `migrate` and `run` are idempotent.

To run against the stack from the host instead:

```bash
python -m venv storage_consumer/venv
storage_consumer/venv/bin/pip install -r storage_consumer/requirements.txt
cp storage_consumer/.env.example storage_consumer/.env   # then fill it in

storage_consumer/venv/bin/python -m storage_consumer.main migrate
storage_consumer/venv/bin/python -m storage_consumer.main run
```

`.env` is for host runs only and uses `localhost:9092` / `127.0.0.1`. The
container gets `kafka:29092` / `postgres` from `docker-compose.yml`, because the
hostnames differ. Same split as `crawler/.env` and `network_analyzer/.env`; see
`docs/setup.md`.

## Configuration

Required, and shared with `app_api` because this writes to the database Django
owns the `applications` table in:

| Variable | Example |
| --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| `POSTGRES_DB` | `project_db` |
| `POSTGRES_USER` | `project_user` |
| `POSTGRES_PASSWORD` | |
| `POSTGRES_HOST` | `postgres` |
| `POSTGRES_PORT` | `5432` |

Everything else has a default in `config.py`; `.env.example` lists them all with
the reasoning. The ones worth knowing about:

- `STORAGE_PIPELINES` — which topics this process consumes (`all`, or a
  comma-separated list). Equivalent to `--pipeline`.
- `STORAGE_AUTO_OFFSET_RESET` — `earliest` by default. A storage subsystem
  joining a topic must ingest the history already on it rather than skip to the
  end. See the note in [Dead-letter runbook](#dead-letter-runbook) about what
  that means on a topic with an old backlog.
- Batch size (`max_poll_records`) — code defaults are per topic in
  `pipelines.py` (reviews 500, app-stats 200, network-metrics 50). Override
  without a code change:

  | Want | Set |
  | --- | --- |
  | Raise reviews alone | `STORAGE_MAX_POLL_RECORDS_REVIEWS=1000` |
  | Raise app-stats alone | `STORAGE_MAX_POLL_RECORDS_APP_STATS=300` |
  | Raise network-metrics alone | `STORAGE_MAX_POLL_RECORDS_NETWORK_METRICS=100` |
  | Default for every *unset* topic | `STORAGE_MAX_POLL_RECORDS=100` |

  Per-topic wins over the global variable. In Docker Compose, put these under
  the `storage-consumer` service `environment:` block (or export them for a
  host/venv run).

## Schema ownership

Two subsystems write DDL to this database, and they own strictly disjoint sets
of tables.

| Owner | Tables | Mechanism |
| --- | --- | --- |
| `app_api` | `apps_registry_application` | Django migrations |
| `storage_consumer` | `app_stats`, `reviews`, `network_metrics`, `dead_letter_events` | numbered `.sql` in `persistence/sql/` |

`storage_consumer` reads `apps_registry_application` and never writes to it.
There are deliberately **no Django models** for the four tables above: two
migration systems believing they own one table is how a schema gets corrupted.
`docs/raw_md/database_schema.md` is the reference for both.

Migrations are plain SQL applied by `persistence/migrator.py`, tracked in
`storage_consumer_migrations`, and serialised with a Postgres advisory lock — so
several containers starting at once is safe.

## How it works

```
poll() ─► decode ─► resolve package_name ─► dedupe ─► INSERT … ON CONFLICT ─► commit()
             │              │                              │
             └── malformed  └── unknown app ──► dead_letter_events (same transaction)
```

Each pipeline is one thread with its own consumer, consumer group, database
connection and `package_name` cache. Nothing is shared but immutable settings
and a stop event, so there are no locks anywhere in the subsystem.

Threads are for isolation, not throughput. A reviews burst is up to 200,000
messages over five to fifteen minutes; a single consumer subscribed to all three
topics would put that backlog in front of a `network-metrics` message and delay
it by minutes. Measured with the burst test in
`tests/test_pipeline_e2e.py`, a `network-metrics` message lands in well under a
second while 3,000 reviews are ingesting concurrently.

The per-topic differences all live in `pipelines.py`:

| Pipeline | Table | Idempotency key | Batch | On conflict |
| --- | --- | --- | --- | --- |
| `app-stats` | `app_stats` | `(application_id, crawled_at)` | 200 | `DO NOTHING` |
| `reviews` | `reviews` | `(review_id)` | 500 | `DO UPDATE` if newer |
| `network-metrics` | `network_metrics` | `(analysis_id)` | 50 | `DO NOTHING` |

`reviews` is the only table that updates rows, because a review's
`thumbs_up_count` changes over time. The update is guarded with
`WHERE EXCLUDED.last_synced_at > reviews.last_synced_at`, so a replayed or
out-of-order message cannot overwrite fresher data with older values.
`first_seen_at` and `sentiment` are never touched by an update — the latter
belongs to a future subsystem.

## Operations

### Verifying it works

```bash
./scripts/verify_storage_consumer.sh
```

Checks the schema, the container's health, consumer lag per pipeline, row
counts, and any dead-lettered messages. `--help` lists the options.

Health is reported per pipeline, via a heartbeat file each worker touches every
poll (`healthcheck.sh`). A process whose reviews thread has wedged is not
healthy just because PID 1 is alive.

### Dead-letter runbook

A row in `dead_letter_events` means a message could never be stored. The offset
has already advanced past it, so the pipeline is healthy — but the data is not
in the database and will not arrive without action.

Start with what the reasons are:

```sql
SELECT topic, left(error_reason, 60) AS reason, count(*), max(occurred_at)
  FROM dead_letter_events
 GROUP BY topic, left(error_reason, 60)
 ORDER BY count(*) DESC;
```

**`Unknown package_name '…'`** — the app is not in `apps_registry_application`.
Register it through the App API, then replay those offsets (below). This is the
one dead-letter reason that is routinely recoverable.

**`Required field 'x' is missing or null`, on every message at once** — the
producer's contract changed. Compare `crawler/data_mapper.py` or
`network_analyzer/message_mapper.py` against the `*_FIELDS` constants in
`decoders.py`, fix whichever is wrong, then replay.

> **Expected on first deployment.** Any messages produced before commit
> `90d244c` ("map Play Store payloads to stable Kafka field contracts") are in
> the raw scraper format: camelCase keys, `package_name` only in the Kafka
> message key, and no `crawled_at` at all. Because `auto_offset_reset` is
> `earliest`, a first start reads that whole backlog and rejects all of it.
> That is correct behaviour and it is not a bug in either subsystem, but it does
> mean one dead-letter row per legacy message. To skip a known-legacy backlog
> instead, seek the group to the end before the first start:
>
> ```bash
> docker compose stop storage-consumer
> docker exec project_kafka kafka-consumer-groups \
>   --bootstrap-server localhost:9092 --group storage-consumer.reviews \
>   --topic reviews --reset-offsets --to-latest --execute
> docker compose start storage-consumer
> ```
>
> Then clear the noise: `DELETE FROM dead_letter_events WHERE topic = 'reviews';`

**`Message body is not valid JSON` / `not valid UTF-8`** — a corrupt or
non-JSON message. `raw_payload` holds the original bytes:

```sql
SELECT topic, "partition", kafka_offset, convert_from(raw_payload, 'UTF8')
  FROM dead_letter_events ORDER BY occurred_at DESC LIMIT 10;
```

**`Field 'x' is N characters, longer than …` / `outside the range …`** — a value
that would not fit its column. These are caught at decode time on purpose: left
to PostgreSQL, one such value would abort the whole batch transaction and take
every valid row in it down too.

To replay specific offsets after fixing the cause, reset that pipeline's group
to the earliest affected offset. Replay is always safe (see below).

### Replaying a topic

Rebuilding a table from the topic is a supported operation, because replay is a
no-op for rows that are already correct:

```bash
docker compose stop storage-consumer

docker exec project_kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group storage-consumer.reviews --topic reviews \
  --reset-offsets --to-earliest --execute

docker compose start storage-consumer
```

Each pipeline has its own consumer group, so this affects one topic and leaves
the other two alone. The end state is asserted by
`test_rewinding_the_group_and_replaying_changes_nothing`.

### Scaling a pipeline

One thread per pipeline is sized for the expected volume with room to spare.
Measured with `scripts/load_test_storage_consumer.py` on a 4-vCPU VM running
the broker, PostgreSQL and the consumer together:

| | |
| --- | --- |
| Worst-case burst | 200,000 reviews arriving over 300s |
| Time to absorb it | **46s**, all 200,000 rows stored, nothing dead-lettered |
| Sustained rate | ~4,400 messages/second (6.6x the 667/s peak) |
| Batch write latency | 51ms median, 65ms p95 for 500 rows |

The measured rate is short of the 10x target the design set, but that number is
bounded by the test machine rather than by the pipeline: the same host only
*produced* at 10,700 messages/second while running the broker and database, and
producing is the cheaper half of the same work. Re-run with the broker and
PostgreSQL on separate hosts for a figure that says something about the code.
What the run does settle is the question that matters — one thread clears the
worst-case burst about six times faster than it arrives.

If one pipeline's lag grows steadily rather than in bursts, it has outgrown
that:

1. Widen the topic — `./scripts/create_topics.sh --topic reviews --partitions 6
   --alter`. A partition count can be increased but never reduced.
2. Run a second container for that pipeline only, by duplicating the
   `storage-consumer` service with `STORAGE_PIPELINES: reviews`. Members of one
   consumer group split the partitions between them.

No code change is needed for either step, which is why `--pipeline` exists.

To re-measure after any change to the write path:

```bash
storage_consumer/venv/bin/python scripts/load_test_storage_consumer.py \
    --pipeline reviews --messages 200000
```

It registers a throwaway app, produces to a throwaway topic, runs one real
worker against it, and deletes everything afterwards.

## Development

```bash
storage_consumer/venv/bin/python -m pytest storage_consumer/tests -m "not integration"
```

The suite splits in two. The default run needs no broker and no database: the
core (`config.py`, `events.py`, `decoders.py`, `batching.py`,
`retry_policy.py`) imports nothing but the standard library, so the wire
contract for all three topics can be asserted directly.

Tests marked `integration` need the compose stack running:

```bash
docker compose up -d postgres kafka
storage_consumer/venv/bin/python -m pytest storage_consumer/tests -m integration
```

They create their own topics and applications and clean up after themselves, so
they can run against a live stack.

| Module | Responsibility |
| --- | --- |
| `config.py` | Settings, from the environment, validated |
| `events.py` | Typed events; `conflict_key` and `observed_at` |
| `decoders.py` | Bytes to events; the wire contract |
| `batching.py` | Decode, bind foreign keys, de-duplicate |
| `retry_policy.py` | Backoff, and which errors are transient |
| `pipelines.py` | The per-topic table of differences |
| `consumer_factory.py` | `KafkaConsumer` construction |
| `worker.py` | The poll / write / commit loop |
| `supervisor.py` | Threads, shutdown, exit code |
| `persistence/` | Connection, migrations, repositories |
| `main.py` | CLI and composition root |

### Reading order

Six files explain the whole subsystem; the rest is detail you can look up when
you need it.

1. `events.py` — the vocabulary. Everything downstream moves these values.
2. `pipelines.py` — the one table of per-topic differences. Read it next and
   the three topics stop being three separate stories.
3. `worker.py` — the poll / write / commit loop, where the no-data-loss
   ordering is visible in about thirty lines.
4. `persistence/repository.py` — how one batch becomes one `INSERT`, and how
   `WriteOutcome` reports what the database did.
5. `supervisor.py` — the thread lifecycle and the "no half-alive process" rule.
6. `main.py` — the composition root, which shows how the above are wired.

For the tests, `tests/test_batching.py` is the best starting point: it exercises
the pure core with no broker and no database, so it reads as executable
documentation of the decode / bind / dedupe contract.
