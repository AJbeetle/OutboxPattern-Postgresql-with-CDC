-- =============================================================================
-- ClickHouse Schema — orders_cdc
--
-- ENGINE: ReplacingMergeTree(version)
-- ClickHouse does NOT update rows in-place. Instead:
--   - Every CDC event is INSERTed as a new row
--   - ReplacingMergeTree deduplicates by ORDER BY key, keeping the row
--     with the highest `version` value
--   - Deduplication happens asynchronously during merges
--   - Use FINAL keyword or argMax() in queries for consistent reads
--
-- This pattern gives us a full CDC audit trail AND current-state queries.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------------
-- orders_cdc — receives all order domain events from Kafka
-- Each row represents one CDC event (create, update, or delete)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.orders_cdc
(
    -- Event metadata (from Kafka message / outbox_events columns)
    event_id            String,             -- outbox_events.id (UUID as string)
    event_type          String,             -- ORDER_CREATED | ORDER_UPDATED | ORDER_DELETED
    schema_version      UInt8,
    correlation_id      Nullable(String),
    trace_id            Nullable(String),
    event_created_at    DateTime64(3, 'UTC'),

    -- Order fields (from the event payload)
    order_id            String,             -- orders.id (UUID as string)
    customer_id         String,
    status              String,
    total_amount_cents  Int64,
    currency            FixedString(3),
    line_items          String,             -- JSON string (ClickHouse JSON type is experimental)
    shipping_address    Nullable(String),   -- JSON string
    metadata            String,
    
    -- Soft delete flag
    is_deleted          UInt8 DEFAULT 0,    -- 1 when event_type = ORDER_DELETED
    
    -- Version for ReplacingMergeTree deduplication
    -- Higher version = newer state. Use orders.version from payload.
    version             UInt32,
    
    -- ClickHouse ingestion timestamp
    ingested_at         DateTime64(3, 'UTC') DEFAULT now64()
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(event_created_at)     -- partition by month for efficient range scans
ORDER BY (order_id, event_created_at)        -- dedup key: latest event per order
SETTINGS
    index_granularity = 8192,
    merge_with_ttl_timeout = 3600;

-- ---------------------------------------------------------------------------
-- Materialized view — current order state (deduplicated)
-- Uses argMax to get the latest value of each field per order_id.
-- Avoids FINAL keyword performance penalty on the raw table.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.orders_current_state
(
    order_id            String,
    customer_id         String,
    status              String,
    total_amount_cents  Int64,
    currency            FixedString(3),
    line_items          String,
    is_deleted          UInt8,
    version             UInt32,
    last_event_type     String,
    last_updated_at     DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
ORDER BY order_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.orders_current_state_mv
TO analytics.orders_current_state
AS
SELECT
    order_id,
    argMax(customer_id, version)            AS customer_id,
    argMax(status, version)                 AS status,
    argMax(total_amount_cents, version)     AS total_amount_cents,
    argMax(currency, version)               AS currency,
    argMax(line_items, version)             AS line_items,
    argMax(is_deleted, version)             AS is_deleted,
    max(version)                            AS version,
    argMax(event_type, version)             AS last_event_type,
    max(event_created_at)                   AS last_updated_at
FROM analytics.orders_cdc
GROUP BY order_id;

-- ---------------------------------------------------------------------------
-- orders_by_status — summary view for dashboards
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics.orders_by_status_summary
(
    status              String,
    date                Date,
    order_count         UInt64,
    total_revenue_cents Int64
)
ENGINE = SummingMergeTree((order_count, total_revenue_cents))
ORDER BY (status, date);

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.orders_by_status_mv
TO analytics.orders_by_status_summary
AS
SELECT
    status,
    toDate(event_created_at)        AS date,
    countIf(event_type = 'ORDER_CREATED') AS order_count,
    sumIf(total_amount_cents, event_type = 'ORDER_CREATED') AS total_revenue_cents
FROM analytics.orders_cdc
GROUP BY status, date;