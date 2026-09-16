# Sentiment Analysis Subsystem

Periodic **batch/CLI job** that classifies review text and writes
`reviews.sentiment` (`POSITIVE` / `NEUTRAL` / `NEGATIVE`).
`storage_consumer` never touches that column on upsert, so crawler re-syncs
do not wipe analysis results — only this job writes them.

This is not a daemon: each run walks **every** review with non-empty content
(by `id`), recomputes labels with the model, **overwrites** existing values,
then exits.

## Model

Single model, no language routing:

`cardiffnlp/twitter-xlm-roberta-base-sentiment`

### Known limitation (Persian)

The model used is **not** officially fine-tuned for Persian; its accuracy is
based on cross-lingual transfer of the XLM-RoBERTa base. Manual routing testing
with a specialized Persian model showed no significant improvement, so the
single-model architecture was kept for simplicity.

## Layout

```
sentiment/
  main.py                 # CLI + composition root
  config.py               # frozen Settings
  exceptions.py
  core/classifier.py      # transformers wrapper (no psycopg2)
  persistence/            # Database + ReviewRepository
  runtime/sentiment_service.py
  tests/unit|integration/
```

## Run

Host / venv — run from the **repository root** (`PlayPulse/`), not from
`sentiment/`. The package name is `sentiment`, so Python must see the parent
directory on `sys.path` (same pattern as `storage_consumer` / `network_analyzer`).

```bash
cd ~/PlayPulse
source sentiment/.venv/bin/activate

pip install torch --index-url https://download.pytorch.org/whl/cpu
pip install -r sentiment/requirements.txt
# fill POSTGRES_* in sentiment/.env (see .env.example)

python -m sentiment.main run
python -m sentiment.main run --batch-size 50 --dry-run
```

Docker (profile `tools` — not started by `docker compose up`):

```bash
docker compose --profile tools build sentiment
docker compose run --rm sentiment run
docker compose run --rm sentiment run --batch-size 50 --dry-run
```

Manual model smoke check (no database):

```bash
python scripts/manual_sentiment_check.py
```

## Tests

```bash
pytest sentiment/tests/unit -v
pytest sentiment/tests/integration -m integration -v   # needs compose Postgres
```

## Unclassifiable text

Empty / whitespace-only content is never sent to the model (`None` in the
classifier output). The service writes `NEUTRAL` as a stable default for those
rows. Selection does **not** filter on the current `sentiment` value; pagination
advances by `id` within a run.
