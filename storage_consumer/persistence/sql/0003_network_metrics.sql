-- network_metrics: one row per analyzed pcap capture.
--
-- Append-only like app_stats, and deliberately so: genuinely re-running the
-- same upload test in different conditions is a new observation, not a
-- correction of the old one.

CREATE TABLE IF NOT EXISTS network_metrics (
    id                             BIGSERIAL PRIMARY KEY,
    application_id                 BIGINT NOT NULL
                                   REFERENCES apps_registry_application (id)
                                   ON DELETE CASCADE,
    -- The idempotency key. The analyzer generates one UUID per analyzed file
    -- for exactly this purpose (network_analyzer/README.md), which lets a
    -- redelivered message be a no-op while a real re-run, carrying a new UUID,
    -- still produces a new row.
    analysis_id                    UUID NOT NULL,
    scenario                       VARCHAR(20) NOT NULL,
    rtt_handshake                  DOUBLE PRECISION,
    retransmission_count           INTEGER NOT NULL DEFAULT 0,
    out_of_order_count             INTEGER NOT NULL DEFAULT 0,
    spurious_retransmission_count  INTEGER NOT NULL DEFAULT 0,
    zero_window_count              INTEGER NOT NULL DEFAULT 0,
    tcp_reset_count                INTEGER NOT NULL DEFAULT 0,
    bytes_transferred_total        BIGINT NOT NULL,
    bytes_payload_total            BIGINT NOT NULL,
    overhead_ratio                 DOUBLE PRECISION NOT NULL,
    source_pcap_filename           VARCHAR(255),
    -- Stamped by the analyzer and written verbatim, so the column records when
    -- the analysis ran rather than when this consumer happened to read the
    -- message.
    analyzed_at                    TIMESTAMPTZ NOT NULL,

    CONSTRAINT network_metrics_analysis_id_key UNIQUE (analysis_id),
    CONSTRAINT network_metrics_scenario_check
        CHECK (scenario IN ('UPLOAD', 'DOWNLOAD'))
);

-- The unique constraint above indexes analysis_id, not application_id, so the
-- foreign key still needs an index of its own. Making it composite with
-- analyzed_at costs nothing and serves the per-app reporting query too.
CREATE INDEX IF NOT EXISTS network_metrics_application_id_analyzed_at_idx
    ON network_metrics (application_id, analyzed_at);
