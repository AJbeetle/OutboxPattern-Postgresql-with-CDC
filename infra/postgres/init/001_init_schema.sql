-- =============================================================================
-- 001_init_schema.sql
-- Run order: first. Creates extensions, types, and core tables.
-- PostgreSQL logical replication requires wal_level=logical (set in docker-compose).
-- =============================================================================

-- Enable UUID generation (pgcrypto gives gen_random_uuid())
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Enable pg_stat_statements for query performance monitoring
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ---------------------------------------------------------------------------
-- ENUM types — stored as strings in outbox payload for forward compatibility
-- ---------------------------------------------------------------------------

CREATE TYPE order_status AS ENUM (
    'PENDING',
    'CONFIRMED',
    'PROCESSING',
    'SHIPPED',
    'DELIVERED',
    'CANCELLED',
    'REFUNDED'
);

CREATE TYPE outbox_event_type AS ENUM (
    'ORDER_CREATED',
    'ORDER_UPDATED',
    'ORDER_DELETED'
);

CREATE TYPE outbox_status AS ENUM (
    'PENDING',      -- written, not yet picked up by CDC
    'PROCESSED',    -- CDC confirmed delivery to Kafka
    'FAILED'        -- CDC failed; used for alerting/dead-letter
);

-- ---------------------------------------------------------------------------
-- orders — the business table. This is the source of truth for order state.
-- IMPORTANT: We do NOT embed event-publishing logic here. The outbox table
-- handles that concern separately, within the same transaction.
-- ---------------------------------------------------------------------------

CREATE TABLE orders (
    id                  UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id         UUID            NOT NULL,
    status              order_status    NOT NULL DEFAULT 'PENDING',
    
    -- Line items stored as JSONB for schema flexibility without EAV complexity
    -- Schema: [{"product_id": "uuid", "quantity": int, "unit_price_cents": int}]
    line_items          JSONB           NOT NULL DEFAULT '[]'::jsonb,
    
    -- Monetary values in cents to avoid floating-point precision issues
    total_amount_cents  BIGINT          NOT NULL DEFAULT 0 CHECK (total_amount_cents >= 0),
    currency            CHAR(3)         NOT NULL DEFAULT 'USD',
    
    -- Shipping information
    shipping_address    JSONB,          -- {street, city, state, zip, country}
    
    -- Metadata
    metadata            JSONB           NOT NULL DEFAULT '{}'::jsonb,
    
    -- Soft delete — we never hard-delete orders for audit/compliance
    deleted_at          TIMESTAMPTZ,
    
    -- Optimistic locking — increment on every update to detect concurrent writes
    version             INTEGER         NOT NULL DEFAULT 1,
    
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX idx_orders_customer_id     ON orders (customer_id);
CREATE INDEX idx_orders_status          ON orders (status) WHERE deleted_at IS NULL;
CREATE INDEX idx_orders_created_at      ON orders (created_at DESC);
CREATE INDEX idx_orders_deleted_at      ON orders (deleted_at) WHERE deleted_at IS NOT NULL;

-- GIN index for JSONB line_items queries (e.g. find orders containing product X)
CREATE INDEX idx_orders_line_items      ON orders USING GIN (line_items);
CREATE INDEX idx_orders_metadata        ON orders USING GIN (metadata);

COMMENT ON TABLE orders IS 'Core order business table. Never hard-delete rows; use deleted_at for logical deletion.';
COMMENT ON COLUMN orders.version IS 'Optimistic lock version. Increment on every UPDATE. Caller must pass current version to detect concurrent modification.';
COMMENT ON COLUMN orders.line_items IS 'JSON array: [{product_id, quantity, unit_price_cents}]';
COMMENT ON COLUMN orders.total_amount_cents IS 'Sum of (quantity * unit_price_cents) for all line_items, in cents.';

-- ---------------------------------------------------------------------------
-- outbox_events — transactional outbox table.
--
-- DESIGN: Every business operation (create/update/delete order) writes BOTH
-- to `orders` AND inserts a row here, within a SINGLE database transaction.
-- This guarantees atomicity: either both writes succeed or neither does.
--
-- Debezium watches this table via WAL logical replication. When it detects
-- a new INSERT here, it reads the payload and publishes to Kafka.
-- The `status` column is updated back to PROCESSED after Kafka ACK.
--
-- WHY NOT TRIGGER-BASED? Triggers are implicit and hard to test/debug.
-- Explicit outbox writes in application code are inspectable and controllable.
-- ---------------------------------------------------------------------------

CREATE TABLE outbox_events (
    -- Monotonic UUID v7 (time-ordered) for deterministic ordering within a transaction
    -- Falls back to gen_random_uuid() here; upgrade to uuid_generate_v7() with pgx extension
    id                  UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- aggregate_type: the domain entity type (maps to Kafka topic prefix)
    -- e.g. "orders" → Debezium routes to topic "outbox.event.orders"
    aggregate_type      TEXT            NOT NULL,
    
    -- aggregate_id: the PK of the business entity this event is about
    aggregate_id        UUID            NOT NULL,
    
    -- event_type: what happened (maps to Kafka message key suffix / event schema version)
    event_type          outbox_event_type NOT NULL,
    
    -- payload: full event body as JSONB. Consumers deserialize this.
    -- Versioned with schema_version for forward/backward compatibility.
    payload             JSONB           NOT NULL,
    
    -- schema_version: bump when payload shape changes; consumers can branch on this
    schema_version      INTEGER         NOT NULL DEFAULT 1,
    
    -- tracing: propagate distributed trace context through the event
    trace_id            TEXT,           -- OpenTelemetry trace ID
    span_id             TEXT,           -- OpenTelemetry span ID
    
    -- correlation: link events from the same user request (e.g. saga steps)
    correlation_id      UUID,
    causation_id        UUID,           -- ID of the event that caused this one
    
    -- processing state (updated by CDC relay after Kafka ACK)
    status              outbox_status   NOT NULL DEFAULT 'PENDING',
    
    -- error_details: populated if status = FAILED
    error_details       TEXT,
    
    -- retry_count: how many times CDC relay has attempted delivery
    retry_count         INTEGER         NOT NULL DEFAULT 0,
    
    -- processed_at: set by CDC relay after confirmed Kafka ACK
    processed_at        TIMESTAMPTZ,
    
    -- created_at: event creation time. This is immutable.
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

-- Debezium reads WAL; it doesn't need indexes for its own reads.
-- These indexes serve operational queries: monitor pending events, debug failures.
CREATE INDEX idx_outbox_status          ON outbox_events (status) WHERE status = 'PENDING';
CREATE INDEX idx_outbox_aggregate       ON outbox_events (aggregate_type, aggregate_id);
CREATE INDEX idx_outbox_created_at      ON outbox_events (created_at DESC);
CREATE INDEX idx_outbox_correlation     ON outbox_events (correlation_id) WHERE correlation_id IS NOT NULL;

COMMENT ON TABLE outbox_events IS 'Transactional outbox. Written atomically with business entity changes. Read by Debezium CDC via WAL logical replication.';
COMMENT ON COLUMN outbox_events.aggregate_type IS 'Domain entity type. Used by Debezium Outbox Event Router SMT to determine target Kafka topic.';
COMMENT ON COLUMN outbox_events.schema_version IS 'Payload schema version. Increment when payload structure changes. Consumers must handle multiple versions during rolling deploys.';
COMMENT ON COLUMN outbox_events.trace_id IS 'OpenTelemetry trace ID for distributed tracing across service boundaries.';

-- ---------------------------------------------------------------------------
-- updated_at trigger — auto-maintain updated_at on orders
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- Replication publication — tell Postgres which tables to expose via WAL
--
-- Debezium uses a PUBLICATION to know which tables to stream.
-- We publish ONLY outbox_events (not orders) — Debezium only needs to see the outbox table, not every column of every business table.
-- This reduces WAL fan-out and keeps CDC concerns separated.
-- ---------------------------------------------------------------------------

CREATE PUBLICATION outbox_publication
    FOR TABLE outbox_events
    WITH (publish = 'insert');   -- outbox rows are INSERT-only from CDC perspective

COMMENT ON PUBLICATION outbox_publication IS 'Logical replication publication for Debezium CDC. Publishes INSERT events on outbox_events only.';