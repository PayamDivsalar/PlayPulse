-- reviews: user reviews, upserted on the Play Store review identifier.
--
-- The one table in this subsystem that updates rows rather than appending
-- them: the requirement is to keep the latest version of each review (its
-- thumbs_up_count changes over time), not a history of its edits.

CREATE TABLE IF NOT EXISTS reviews (
    id               BIGSERIAL PRIMARY KEY,
    application_id   BIGINT NOT NULL
                     REFERENCES apps_registry_application (id) ON DELETE CASCADE,
    review_id        VARCHAR(255) NOT NULL,
    user_name        VARCHAR(255),
    thumbs_up_count  INTEGER NOT NULL DEFAULT 0,
    -- Nullable, unlike the schema document: the scraper legitimately returns
    -- no score for some reviews, and one absent measurement must not discard
    -- an otherwise valid review.
    score            SMALLINT,
    content          TEXT,
    at               TIMESTAMPTZ NOT NULL,
    -- Written by the future sentiment/ subsystem; created here so that adding
    -- it later needs no migration against a large table.
    sentiment        VARCHAR(20),
    -- Never touched on conflict, so it keeps meaning "when we first saw this
    -- review" across any number of upserts.
    first_seen_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Taken from the message's crawled_at rather than a row-touch timestamp.
    -- That is both the more honest semantics (when the data was synced from
    -- Play Store, not when our row was written) and what makes the monotonic
    -- replay guard in review_repository.py possible.
    last_synced_at   TIMESTAMPTZ NOT NULL,

    -- The idempotency key, and the only way to implement the required upsert.
    CONSTRAINT reviews_review_id_key UNIQUE (review_id),
    CONSTRAINT reviews_sentiment_check
        CHECK (sentiment IS NULL
               OR sentiment IN ('POSITIVE', 'NEUTRAL', 'NEGATIVE'))
);

-- The main reporting query is "score trend for one app over time". This table
-- grows to (apps x up to 1000 reviews) and keeps growing, so unlike
-- network_metrics it genuinely needs the composite index.
CREATE INDEX IF NOT EXISTS reviews_application_id_at_idx
    ON reviews (application_id, at);

-- Cross-app queries by review date, which the composite index cannot serve.
CREATE INDEX IF NOT EXISTS reviews_at_idx
    ON reviews (at);
