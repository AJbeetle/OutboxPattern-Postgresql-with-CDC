# Production-Grade Outbox Pattern + CDC with PostgreSQL WAL

## Architecture

```
FastAPI → PostgreSQL (WAL logical) → Debezium → Kafka → PostgreSQL Analytics (JDBC Sink)
                                              ↘ Downstream Services
```

## Prerequisites

- Docker + Docker Compose v2
- Python 3.12+ (for local dev)
- `curl`, `jq` (for scripts)

## Quick Start

```bash
# 1. Clone and set up env
cp .env.example .env

# 2. Start all services (this takes ~2min on first run — Debezium downloads connectors)
docker compose up -d

# 3. Wait for all services to be healthy
docker compose ps

# 4. Register Debezium + JDBC Sink connectors
chmod +x scripts/register-connectors.sh
./scripts/register-connectors.sh

# 5. Run the smoke test
chmod +x scripts/smoke-test.sh
./scripts/smoke-test.sh
```

## Service URLs

| Service | URL | Notes |
|---|---|---|
| FastAPI | http://localhost:8000/docs | Swagger UI |
| Kafka UI | http://localhost:8080 | Topic/consumer management |
| Kafka Connect | http://localhost:8083 | Connector REST API |
| Schema Registry | http://localhost:8081 | Schema management |
| PostgreSQL Analytics | localhost:5433 | CDC Sink |

## API Endpoints

### Create Order
```bash
curl -X POST http://localhost:8000/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "X-Trace-Id: my-trace-001" \
  -d '{
    "customer_id": "550e8400-e29b-41d4-a716-446655440000",
    "line_items": [
      {"product_id": "660e8400-e29b-41d4-a716-446655440000", "quantity": 2, "unit_price_cents": 4999}
    ],
    "currency": "USD"
  }'
```

### Update Order (requires version for optimistic locking)
```bash
curl -X PUT http://localhost:8000/api/v1/orders/{order_id} \
  -H "Content-Type: application/json" \
  -d '{"status": "CONFIRMED", "version": 1}'
```

### Delete Order
```bash
curl -X DELETE http://localhost:8000/api/v1/orders/{order_id}
```

### Monitor Outbox Health
```bash
curl http://localhost:8000/health/outbox
curl http://localhost:8000/health/outbox/lag   # events stuck >60s
```

## Key Design Decisions

### Why outbox_events is INSERT-only
The `CREATE PUBLICATION` uses `publish = 'insert'` — Debezium only watches for
new rows in the outbox table, not updates. The `status` column (PENDING →
PROCESSED) is updated by a separate process after Kafka ACK, but Debezium
doesn't care about those updates. This keeps WAL volume minimal.

### Why we don't hard-delete orders
Orders are soft-deleted (deleted_at + status=CANCELLED) for:
- Audit compliance (GDPR Article 17 requires tracking deletion requests)
- Analytics database CDC integrity (hard deletes create tombstones that need special handling)
- Debugging (you can always see what existed)

### Optimistic locking
Every `PUT /orders/{id}` requires a `version` field. The service checks
`order.version == req.version` before applying updates. If a concurrent
request updated the order first, the version won't match → 409 Conflict.
The client fetches the latest order and retries.

### Debezium Outbox Event Router SMT
Without SMT, Debezium would emit raw CDC envelopes to a single topic.
With SMT, it:
1. Extracts the `payload` column as the Kafka message body
2. Uses `aggregate_id` as the Kafka message key (ensures ordering per order)
3. Routes to topic `outbox.event.{aggregate_type}` based on the `aggregate_type` column

Result: consumers receive clean domain events on per-entity topics.

### PostgreSQL Analytics JDBC Sink
We use a PostgreSQL replica for analytics. The JDBC Sink connector handles CDC upserts by:
1. Using `insert.mode = upsert`
2. Setting `pk.mode = record_value` and `pk.fields = order_id`
3. Applying updates to the target table (`orders_cdc`) based on the primary key

```sql
-- Current state query (consistent)
SELECT order_id, status, version, last_event_type 
FROM orders_cdc 
WHERE order_id = '...';

-- Check average CDC lag via the view:
SELECT * FROM cdc_lag_monitor;
```

## Monitoring

### Check Debezium connector status
```bash
curl http://localhost:8083/connectors/outbox-postgres-source/status | jq
```

### Check WAL slot lag
```bash
docker exec outbox_postgres psql -U orderuser -d ordersdb -c "
  SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes
  FROM pg_replication_slots;
"
```

### Check Kafka topic messages
```bash
docker exec outbox_kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic outbox.event.orders \
  --from-beginning \
  --max-messages 5
```

### Query PostgreSQL Analytics
```bash
docker exec outbox_postgres_analytics psql \
  -U analyticsuser -d analyticsdb -c \
  "SELECT last_event_type, COUNT(*) FROM orders_cdc GROUP BY last_event_type;"
```

## Running Tests

```bash
cd api
pip install -r requirements.txt
pytest tests/ -v --cov=app --cov-report=term-missing
```

## Project Structure

```
outbox-cdc-project/
├── docker-compose.yaml             # Full stack: PG, Kafka, Debezium, PG Analytics, API
├── .env.example                    # Environment variables template
│
├── api/                            # FastAPI application
│   ├── app/
│   │   ├── main.py                 # App factory, middleware, routers
│   │   ├── core/
│   │   │   ├── config.py           # Pydantic settings
│   │   │   └── logging.py          # Structured logging (structlog)
│   │   ├── db/
│   │   │   └── session.py          # Async SQLAlchemy engine + session factory
│   │   ├── models/
│   │   │   ├── orm.py              # SQLAlchemy ORM (Order, OutboxEvent)
│   │   │   └── schemas.py          # Pydantic request/response schemas
│   │   ├── services/
│   │   │   ├── order_service.py    # Business logic + atomic outbox writes
│   │   │   └── outbox_service.py   # Outbox event construction + session.add()
│   │   └── routers/
│   │       ├── orders.py           # CRUD endpoints
│   │       └── health.py           # Health + outbox monitoring
│   └── tests/
│       └── test_order_service.py   # Unit tests for outbox atomicity
│
├── infra/
│   ├── postgres/
│   │   └── init/
│   │       ├── 001_init_schema.sql     # Tables, enums, indexes, publication
│   │       └── 002_replication_setup.sql # Replication role, monitoring views
│   ├── debezium/
│   │   ├── Dockerfile                  # Debezium + JDBC connector install
│   │   └── connectors/
│   │       ├── 01-postgres-source.json # WAL reader + Outbox Event Router SMT
│   │       ├── 02-clickhouse-sink.json # Kafka → ClickHouse sync (deprecated)
│   │       └── 03-jdbc-sink.json       # Kafka → Postgres Analytics sync
│   └── postgres-analytics/
│       └── 001-orders-cdc.sql          # Upsert table + lag monitor views
│
└── scripts/
    ├── project-setup.sh            # Project initial setup script
    ├── register-connectors.sh      # Register connectors via Connect REST API
    └── smoke-test.sh               # End-to-end pipeline validation
```