#!/usr/bin/env bash
# =============================================================================
# scripts/register_connectors.sh
#
# WHAT THIS DOES:
#   Kafka Connect stores connector configs in an internal Kafka topic
#   (_connect-configs). To add a connector you POST its config JSON to the
#   Kafka Connect REST API at http://localhost:8083/connectors.
#
#   This script does that for all JSON files in infra/debezium/connectors/,
#   in filename order (01-... before 02-...).
#
# WHEN TO RUN:
#   After `docker compose up -d` and after kafka-connect is healthy.
#   Wait ~60-90s on first boot (connector JARs install during build).
#   You can check readiness: curl -s http://localhost:8083/connectors
#
# USAGE:
#   ./scripts/register_connectors.sh           # register/update connectors
#   ./scripts/register_connectors.sh --reset   # delete all first, then register
# =============================================================================

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTORS_DIR="$(cd "$(dirname "$0")/../infra/debezium/connectors" && pwd)"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERR]${NC}   $*" >&2; }
step() { echo -e "\n${BLUE}══${NC} $*"; }

# ---------------------------------------------------------------------------
# Wait for Kafka Connect REST API to respond
# ---------------------------------------------------------------------------
wait_for_connect() {
    step "Waiting for Kafka Connect at $CONNECT_URL"
    local attempts=0
    until curl -sf "$CONNECT_URL/connectors" > /dev/null 2>&1; do
        attempts=$((attempts + 1))
        if [[ $attempts -ge 40 ]]; then
            err "Kafka Connect not ready after $((attempts * 3))s. Check: docker logs outbox_kafka_connect"
            exit 1
        fi
        echo -n "."
        sleep 3
    done
    echo ""
    log "Kafka Connect is ready"
}

# ---------------------------------------------------------------------------
# Register or update one connector from a JSON file
# ---------------------------------------------------------------------------
register_connector() {
    local file="$1"
    local name
    # Extract connector name from JSON
    name=$(python3 -c "import json,sys; print(json.load(open('$file'))['name'])")

    log "Registering: $name ($(basename "$file"))"

    local payload
    payload=$(python3 -c "import json,sys; print(json.dumps(json.load(open('$file'))))")

    # Check if connector already exists
    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/$name")

    if [[ "$status" == "200" ]]; then
        # PUT to update existing connector config
        log "  → updating existing connector"
        local config
        config=$(python3 -c "import json,sys; d=json.load(open('$file')); print(json.dumps(d['config']))")
        curl -sf -X PUT \
            -H "Content-Type: application/json" \
            --data "$config" \
            "$CONNECT_URL/connectors/$name/config" | python3 -m json.tool
    else
        # POST to create new connector
        log "  → creating new connector"
        curl -sf -X POST \
            -H "Content-Type: application/json" \
            --data "$payload" \
            "$CONNECT_URL/connectors" | python3 -m json.tool
    fi
}

# ---------------------------------------------------------------------------
# Print connector status summary
# ---------------------------------------------------------------------------
show_status() {
    step "Connector status"
    local connectors
    connectors=$(curl -sf "$CONNECT_URL/connectors" | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))" 2>/dev/null || echo "")

    if [[ -z "$connectors" ]]; then
        warn "No connectors registered"
        return
    fi

    for name in $connectors; do
        local state
        state=$(curl -sf "$CONNECT_URL/connectors/$name/status" \
            | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['connector']['state'])" 2>/dev/null || echo "UNKNOWN")
        if [[ "$state" == "RUNNING" ]]; then
            echo -e "  ${GREEN}●${NC} $name ($state)"
        else
            echo -e "  ${RED}●${NC} $name ($state)"
        fi
    done
}

# ---------------------------------------------------------------------------
# Delete all connectors (--reset)
# ---------------------------------------------------------------------------
reset_connectors() {
    step "Deleting all existing connectors"
    local connectors
    connectors=$(curl -sf "$CONNECT_URL/connectors" | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))" 2>/dev/null || echo "")
    for name in $connectors; do
        log "  Deleting: $name"
        curl -sf -X DELETE "$CONNECT_URL/connectors/$name"
    done
    log "Done"
    sleep 2
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
wait_for_connect

if [[ "${1:-}" == "--reset" ]]; then
    reset_connectors
fi

step "Registering connectors from $CONNECTORS_DIR"
for file in "$CONNECTORS_DIR"/*.json; do
    [[ -f "$file" ]] && register_connector "$file"
    echo ""
done

sleep 5
show_status

echo ""
log "Done. Open http://localhost:8080 (Kafka UI) to inspect topics and messages."
log "Use: docker logs -f outbox_consumer  to watch the Python consumer."