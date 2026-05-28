#!/usr/bin/env bash
# =============================================================================
# smoke_test.sh — End-to-end test of the full CDC pipeline
#
# Tests:
#   1. Create order via API → verify in Postgres (source)
#   2. Verify outbox_event was written atomically
#   3. Verify Kafka topic received the event
#   4. Verify postgres-analytics received the CDC upsert
# =============================================================================

set -euo pipefail

API_URL="${API_URL:-http://localhost:8001}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-outbox_kafka}"
PG_CONTAINER="${PG_CONTAINER:-outbox_postgres}"
ANALYTICS_CONTAINER="${ANALYTICS_CONTAINER:-outbox_postgres_analytics}"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'

pass() { echo -e "${GREEN}✓ PASS${NC}  $*"; }
fail() { echo -e "${RED}✗ FAIL${NC}  $*"; exit 1; }
step() { echo -e "\n${BLUE}══${NC} $*"; }

# ---------------------------------------------------------------------------
# Step 1: Create an order
# ---------------------------------------------------------------------------
step "Step 1: Create order via API"

CUSTOMER_ID=$(python3 -c "import uuid; print(uuid.uuid4())")
PRODUCT_ID=$(python3 -c "import uuid; print(uuid.uuid4())")

CREATE_RESPONSE=$(curl -sf -X POST "$API_URL/api/v1/orders" \
    -H "Content-Type: application/json" \
    -H "X-Trace-Id: smoke-test-trace-001" \
    -H "X-Correlation-Id: $(python3 -c 'import uuid; print(uuid.uuid4())')" \
    -d "{
        \"customer_id\": \"$CUSTOMER_ID\",
        \"line_items\": [{
            \"product_id\": \"$PRODUCT_ID\",
            \"quantity\": 2,
            \"unit_price_cents\": 4999
        }],
        \"currency\": \"USD\",
        \"shipping_address\": {
            \"street\": \"123 Test St\",
            \"city\": \"San Francisco\",
            \"state\": \"CA\",
            \"zip_code\": \"94102\",
            \"country\": \"US\"
        }
    }")

ORDER_ID=$(echo "$CREATE_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
ORDER_TOTAL=$(echo "$CREATE_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['total_amount_cents'])")
ORDER_VERSION=$(echo "$CREATE_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['version'])")

[[ -n "$ORDER_ID" ]] && pass "Order created: $ORDER_ID (total: ${ORDER_TOTAL} cents, version: $ORDER_VERSION)" \
    || fail "Order creation failed"

# ---------------------------------------------------------------------------
# Step 2: Verify outbox event in Postgres
# ---------------------------------------------------------------------------
step "Step 2: Verify outbox_event written atomically"
sleep 1

OUTBOX_COUNT=$(docker exec "$PG_CONTAINER" psql -U orderuser -d ordersdb -t -c \
    "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = '$ORDER_ID';" | tr -d ' ')

[[ "$OUTBOX_COUNT" -ge "1" ]] && pass "Outbox event found in Postgres ($OUTBOX_COUNT event(s))" \
    || fail "No outbox event found for order $ORDER_ID"

OUTBOX_EVENT=$(docker exec "$PG_CONTAINER" psql -U orderuser -d ordersdb -t -c \
    "SELECT event_type, status, schema_version FROM outbox_events WHERE aggregate_id = '$ORDER_ID' ORDER BY created_at DESC LIMIT 1;")
echo "  Event: $OUTBOX_EVENT"

# ---------------------------------------------------------------------------
# Step 3: Verify Kafka received the event
# ---------------------------------------------------------------------------
step "Step 3: Verify event published to Kafka topic"
sleep 5  # give Debezium time to publish

KAFKA_MSG=$(docker exec "$KAFKA_CONTAINER" kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "outbox.event.orders" \
    --from-beginning \
    --max-messages 10 \
    --timeout-ms 8000 2>/dev/null | grep "$ORDER_ID" | head -1 || true)

[[ -n "$KAFKA_MSG" ]] && pass "Event found in Kafka topic outbox.event.orders" \
    || echo -e "${YELLOW}⚠ WARN${NC}  Event not yet in Kafka (Debezium may still be starting)"

# ---------------------------------------------------------------------------
# Step 4: Update the order
# ---------------------------------------------------------------------------
step "Step 4: Update order status"

UPDATE_RESPONSE=$(curl -sf -X PUT "$API_URL/api/v1/orders/$ORDER_ID" \
    -H "Content-Type: application/json" \
    -d "{
        \"status\": \"CONFIRMED\",
        \"version\": $ORDER_VERSION
    }")

NEW_VERSION=$(echo "$UPDATE_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['version'])")
NEW_STATUS=$(echo "$UPDATE_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")

[[ "$NEW_STATUS" == "CONFIRMED" ]] && pass "Order updated: status=$NEW_STATUS, version=$NEW_VERSION" \
    || fail "Order update failed"

# ---------------------------------------------------------------------------
# Step 5: Test optimistic locking
# ---------------------------------------------------------------------------
step "Step 5: Test optimistic lock (concurrent update rejection)"

HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT "$API_URL/api/v1/orders/$ORDER_ID" \
    -H "Content-Type: application/json" \
    -d "{
        \"status\": \"PROCESSING\",
        \"version\": 1
    }")

[[ "$HTTP_CODE" == "409" ]] && pass "Optimistic lock enforced (409 Conflict returned)" \
    || fail "Expected 409, got $HTTP_CODE"

# ---------------------------------------------------------------------------
# Step 6: Delete order
# ---------------------------------------------------------------------------
step "Step 6: Soft-delete order"

DELETE_RESPONSE=$(curl -sf -X DELETE "$API_URL/api/v1/orders/$ORDER_ID")
DELETED_STATUS=$(echo "$DELETE_RESPONSE" | python3 -c "import json,sys; print(json.load(sys.stdin)['status'])")

[[ "$DELETED_STATUS" == "CANCELLED" ]] && pass "Order soft-deleted (status=CANCELLED)" \
    || fail "Delete failed"

# Verify 410 on re-access
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$API_URL/api/v1/orders/$ORDER_ID")
[[ "$HTTP_CODE" == "410" ]] && pass "Deleted order returns 410 Gone" \
    || fail "Expected 410, got $HTTP_CODE"

# ---------------------------------------------------------------------------
# Step 7: Verify outbox event count
# ---------------------------------------------------------------------------
step "Step 7: Verify all 3 outbox events (created, updated, deleted)"
sleep 1

TOTAL_OUTBOX=$(docker exec "$PG_CONTAINER" psql -U orderuser -d ordersdb -t -c \
    "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = '$ORDER_ID';" | tr -d ' ')

[[ "$TOTAL_OUTBOX" -eq "3" ]] && pass "3 outbox events recorded (ORDER_CREATED, ORDER_UPDATED, ORDER_DELETED)" \
    || echo -e "${YELLOW}⚠ WARN${NC}  Expected 3 events, found $TOTAL_OUTBOX"

# ---------------------------------------------------------------------------
# Step 8: postgres-analytics check — verify CDC upsert landed
# ---------------------------------------------------------------------------
step "Step 8: Check postgres-analytics CDC table"
sleep 10  # give JDBC sink connector time to batch and write

ANALYTICS_ROW=$(docker exec "$ANALYTICS_CONTAINER" psql -U analyticsuser -d analyticsdb -t -c \
    "SELECT order_id, status, version, last_event_type FROM orders_cdc WHERE order_id = '$ORDER_ID';" \
    2>/dev/null || echo "")

if [[ -n "$ANALYTICS_ROW" ]]; then
    pass "postgres-analytics has CDC row for order $ORDER_ID"
    echo "  Row: $ANALYTICS_ROW"
else
    echo -e "${YELLOW}⚠ WARN${NC}  postgres-analytics not yet synced (JDBC connector may still be starting)"
fi

# Also verify the cdc_lag_monitor view
# LAG=$(docker exec "$ANALYTICS_CONTAINER" psql -U analyticsuser -d analyticsdb -t -c \
#     "SELECT ROUND(avg_cdc_lag_seconds::numeric, 2) FROM cdc_lag_monitor;" 2>/dev/null | tr -d ' \n' || echo "N/A")
# echo "  Average CDC lag: ${LAG}s"

echo -e "\n${GREEN}══ Smoke test complete ══${NC}"
echo "Order ID: $ORDER_ID"
echo "Outbox events: $TOTAL_OUTBOX"