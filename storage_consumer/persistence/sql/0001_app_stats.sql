-- app_stats: hourly Play Store statistics, one row per app per crawl cycle.
--
-- Append-only by design (docs/raw_md/database_schema.md section 2): every
-- crawl inserts a new row instead of updating the previous one, because the
-- point of the table is the trend over time.
--
-- application_id is BIGINT to match apps_registry_application.id, which Django
-- creates as a bigint identity column. A narrower type here would fail to
-- create the foreign key.

CREATE TABLE IF NOT EXISTS app_stats (
    id              BIGSERIAL PRIMARY KEY,
    application_id  BIGINT NOT NULL
                    REFERENCES apps_registry_application (id) ON DELETE CASCADE,
    min_installs    BIGINT,
    score           DOUBLE PRECISION,
    ratings         BIGINT,
    reviews_count   BIGINT,
    version         VARCHAR(50),
    ad_supported    BOOLEAN,
    app_updated_at  TIMESTAMPTZ,
    crawled_at      TIMESTAMPTZ NOT NULL,

    -- The idempotency key. Kafka delivery is at-least-once and both producers
    -- retry at the application level, so without this constraint a replay
    -- silently duplicates rows and corrupts every trend chart built on the
    -- table. crawled_at is stamped per message with microsecond precision and
    -- there is one message per app per cycle, so only a redelivery can collide.
    --
    -- Its btree index is also the composite (application_id, crawled_at) index
    -- the schema document asks for, so no separate index is created.
    CONSTRAINT app_stats_application_id_crawled_at_key
        UNIQUE (application_id, crawled_at)
);

-- Metabase also slices the table by time across all apps ("score distribution
-- last week"), which the composite index above cannot serve because
-- application_id is its leading column.
CREATE INDEX IF NOT EXISTS app_stats_crawled_at_idx
    ON app_stats (crawled_at);
