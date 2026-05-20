-- =============================================================================
-- 002_replication_setup.sql
-- Creates the logical replication slot and grants required permissions.
-- Debezium connects as a replication user; we keep it separate from the app user.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Replication role — Debezium uses this, NOT the application user.
-- Principle of least privilege: replication user can only replicate.
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium_user') THEN
        CREATE ROLE debezium_user WITH
            LOGIN
            REPLICATION                     -- required: allows logical replication connections
            PASSWORD 'debezium_pass'
            CONNECTION LIMIT 5;
    END IF;
END
$$;

-- Grant SELECT on the tables Debezium needs to read during snapshot phase
GRANT SELECT ON TABLE outbox_events TO debezium_user;
GRANT SELECT ON TABLE orders TO debezium_user;

-- Grant USAGE on the schema
GRANT USAGE ON SCHEMA public TO debezium_user;

-- Grant permission to create replication slots
-- (In PG14+ this requires pg_create_logical_replication_slot privilege)
GRANT pg_replication_origin_write TO debezium_user;

-- ---------------------------------------------------------------------------
-- Logical replication slot — named cursor in WAL.
-- Postgres guarantees WAL segments are retained until this slot's LSN is ACKed.
-- 
-- IMPORTANT: Only create slot ONCE. Debezium will create it automatically
-- using the slot_name in its connector config. This script is here for
-- documentation and manual setup if needed.
--
-- Using pgoutput (built-in) rather than wal2json (requires extension install).
-- ---------------------------------------------------------------------------

-- Uncomment to pre-create the slot manually (Debezium will also do this):
-- SELECT pg_create_logical_replication_slot('debezium_outbox_slot', 'pgoutput');

-- ---------------------------------------------------------------------------
-- Monitoring view — operational visibility into outbox health
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW outbox_monitor AS
SELECT
    status,
    COUNT(*)                                            AS total_events,
    MIN(created_at)                                     AS oldest_event,
    MAX(created_at)                                     AS newest_event,
    AVG(EXTRACT(EPOCH FROM (processed_at - created_at)))
        FILTER (WHERE processed_at IS NOT NULL)         AS avg_processing_seconds,
    COUNT(*) FILTER (WHERE retry_count > 0)             AS events_with_retries,
    COUNT(*) FILTER (WHERE retry_count >= 3)            AS events_max_retries
FROM outbox_events
GROUP BY status;

COMMENT ON VIEW outbox_monitor IS 'Operational dashboard for outbox health. Query this to detect CDC lag or delivery failures.';

-- Pending event lag alert view (events pending > 60s indicate CDC is stuck)
CREATE OR REPLACE VIEW outbox_pending_lag AS
SELECT
    id,
    aggregate_type,
    event_type,
    created_at,
    EXTRACT(EPOCH FROM (NOW() - created_at))    AS lag_seconds,
    retry_count
FROM outbox_events
WHERE status = 'PENDING'
  AND created_at < NOW() - INTERVAL '60 seconds'
ORDER BY created_at ASC;

COMMENT ON VIEW outbox_pending_lag IS 'Events pending for >60s — indicates CDC or Kafka connectivity issues.';