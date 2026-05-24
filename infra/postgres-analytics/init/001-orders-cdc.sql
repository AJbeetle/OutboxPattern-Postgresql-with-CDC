-- =============================================================================
-- postgres-analytics: 001_orders_cdc.sql
--
-- This is the CDC sink table. The Confluent JDBC Sink connector writes here
-- via INSERT ... ON CONFLICT (order_id) DO UPDATE (upsert).
--
-- Unlike ClickHouse's append-only ReplacingMergeTree, standard Postgres
-- upserts mean this table always holds exactly ONE row per order — the
-- current state. No FINAL keyword or argMax() needed.
--
-- Every Kafka message from outbox.event.orders maps to one upsert here.
-- The JDBC connector maps JSON payload fields → columns by name.
-- =============================================================================

CREATE TABLE IF NOT EXISTS orders_cdc (
    -- Primary key — JDBC sink upserts on this field (pk.fields = "order_id")
    order_id            UUID            PRIMARY KEY,

    -- Business fields — reflect current state of the order
    customer_id         UUID            NOT NULL,
    status              TEXT            NOT NULL,
    total_amount_cents  BIGINT          NOT NULL DEFAULT 0,
    currency            CHAR(3)         NOT NULL DEFAULT 'USD',

    -- JSONB for structured querying (e.g. find all orders with product X)
    line_items          JSONB           NOT NULL DEFAULT '[]',
    shipping_address    JSONB,
    metadata            JSONB           NOT NULL DEFAULT '{}',
    changed_fields      JSONB,

    -- Soft delete flag — set to TRUE when event_type = ORDER_DELETED
    is_deleted          BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Version from the source order — monotonically increases with each change
    -- Useful for detecting out-of-order delivery (Kafka guarantees per-partition
    -- ordering, but double-check: if incoming version < current, skip)
    version             INTEGER         NOT NULL DEFAULT 1,

    -- CDC metadata — which event last updated this row
    last_event_type     TEXT,           -- ORDER_CREATED | ORDER_UPDATED | ORDER_DELETED
    event_created_at    TIMESTAMPTZ,    -- when the outbox event was written in the source DB
    order_created_at    TEXT,
    order_updated_at    TEXT,

    -- Audit — when this analytics row was last touched
    ingested_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Indexes for common analytical queries
CREATE INDEX idx_orders_cdc_customer   ON orders_cdc (customer_id);
CREATE INDEX idx_orders_cdc_status     ON orders_cdc (status) WHERE is_deleted = FALSE;
CREATE INDEX idx_orders_cdc_deleted    ON orders_cdc (is_deleted);
CREATE INDEX idx_orders_cdc_ingested   ON orders_cdc (ingested_at DESC);

COMMENT ON TABLE orders_cdc IS
  'CDC sink — current state of every order, kept in sync via Kafka JDBC sink connector. One row per order_id.';

COMMENT ON COLUMN orders_cdc.version IS
  'Mirrors orders.version from source DB. Use to detect out-of-order CDC delivery.';

COMMENT ON COLUMN orders_cdc.event_created_at IS
  'Timestamp from source outbox_events.created_at — when the change happened, not when it arrived here.';

-- =============================================================================
-- Monitoring view — lag detection
-- The difference between event_created_at and ingested_at is your CDC lag.
-- In a healthy system this should be sub-second.
-- =============================================================================
CREATE OR REPLACE VIEW cdc_lag_monitor AS
SELECT
    COUNT(*)                                                        AS total_orders,
    COUNT(*) FILTER (WHERE is_deleted = FALSE)                      AS active_orders,
    COUNT(*) FILTER (WHERE is_deleted = TRUE)                       AS deleted_orders,
    MAX(ingested_at)                                                AS last_ingested_at,
    EXTRACT(EPOCH FROM (NOW() - MAX(ingested_at)))                  AS seconds_since_last_ingest,
    AVG(EXTRACT(EPOCH FROM (ingested_at - event_created_at)))       AS avg_cdc_lag_seconds,
    MAX(EXTRACT(EPOCH FROM (ingested_at - event_created_at)))       AS max_cdc_lag_seconds
FROM orders_cdc;

COMMENT ON VIEW cdc_lag_monitor IS
  'CDC health dashboard. avg_cdc_lag_seconds should be <1s in steady state. seconds_since_last_ingest helps detect stalled pipelines.';