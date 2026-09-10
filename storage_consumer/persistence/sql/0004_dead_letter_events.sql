-- dead_letter_events: Kafka records this consumer refused and skipped past.
--
-- In Postgres rather than on a Kafka dead-letter topic for two reasons. It
-- commits in the same transaction as the batch that rejected it, so a record
-- can never be lost while its offset advances; and it is queryable from
-- Metabase and psql with no extra tooling.
--
-- Deliberately has no foreign key to apps_registry_application: the single
-- most common reason to land here is an unknown package_name, which by
-- definition has no row to reference.

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id             BIGSERIAL PRIMARY KEY,
    topic          VARCHAR(255) NOT NULL,
    -- "partition" is quoted throughout: unquoted it reads as the start of a
    -- PARTITION BY clause to anyone skimming, and quoting removes the doubt.
    "partition"    INTEGER NOT NULL,
    -- "offset" is a reserved word, so the column carries the kafka_ prefix
    -- rather than needing quotes in every query that touches it.
    kafka_offset   BIGINT NOT NULL,
    kafka_key      TEXT,
    -- The undecoded message body, so an operator can see exactly what arrived.
    -- BYTEA rather than TEXT because the payload may not be valid UTF-8 --
    -- that is one of the reasons a record ends up here.
    raw_payload    BYTEA,
    error_reason   TEXT NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- The coordinates of a Kafka record are its identity, so re-processing the
    -- same bad message after an offset reset updates nothing and adds nothing.
    CONSTRAINT dead_letter_events_coordinates_key
        UNIQUE (topic, "partition", kafka_offset)
);

-- The operational query is always "what has been rejected recently".
CREATE INDEX IF NOT EXISTS dead_letter_events_occurred_at_idx
    ON dead_letter_events (occurred_at DESC);
