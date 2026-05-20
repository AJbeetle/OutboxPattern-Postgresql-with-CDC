#!/usr/bin/env bash
# =============================================================================
# register_connectors.sh
#
# Registers Debezium source + ClickHouse sink connectors via Kafka Connect REST API.
# Run this AFTER `docker-compose up` and after kafka-connect is healthy.
#
# Usage:
#   ./scripts/register_connectors.sh
#   ./scripts/register_connectors.sh --reset   # delete existing connectors first
# =============================================================================

set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONNECTORS_DIR="$(dirname "$0")/../infra/debezium/connectors"

# Colors
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ---------------------------------------------------------------------------
# Wait for Kafka Connect to be ready
# ---------------------------------------------------------------------------
wait_for_connect() {
    log "Waiting for Kafka Connect to be ready at $CONNECT_URL ..."
    local retries=30
    while [[ $retries -gt 0 ]]; do
        if curl -sf "$CONNECT_URL/connectors" > /dev/null 2>&1; then
            log "Kafka Connect is ready."
            return 0
        fi
        retries=$((retries - 1))
        echo -n "."
        sleep 3
    done
    err "Kafka Connect did not become ready in time."
    exit 1
}

# ---------------------------------------------------------------------------
# Register or update a connector from a JSON file
# Strips JSON comments (lines starting with //) before posting
# ---------------------------------------------------------------------------
register_connector() {
    local file="$1"
    local name
    name=$(python3 -c "
import re, sys
content = open('$file').read()
# Remove // comment lines
content = re.sub(r'^\s*//.*$', '', content, flags=re.MULTILINE)
import json; print(json.load(open('/dev/stdin') if False else __import__('io').StringIO(content))['name'])
")

    log "Registering connector: $name (from $(basename "$file"))"

    # Strip // comments and POST to Connect REST API
    local payload
    payload=$(python3 -c "
import re, json, sys
content = open('$file').read()
content = re.sub(r'^\s*//.*$', '', content, flags=re.MULTILINE)
print(json.dumps(json.loads(content)))
")

    # Check if connector already exists
    local http_status
    http_status=$(curl -s -o /dev/null -w "%{http_code}" "$CONNECT_URL/connectors/$name")

    if [[ "$http_status" == "200" ]]; then
        # Update existing connector config
        log "  Updating existing connector: $name"
        local config
        config=$(echo "$payload" | python3 -c "import json,sys; d=json.load(sys.stdin); print(json.dumps(d['config']))")
        curl -sf -X PUT \
            -H "Content-Type: application/json" \
            --data "$config" \
            "$CONNECT_URL/connectors/$name/config" | python3 -m json.tool
    else
        # Create new connector
        log "  Creating connector: $name"
        curl -sf -X POST \
            -H "Content-Type: application/json" \
            --data "$payload" \
            "$CONNECT_URL/connectors" | python3 -m json.tool
    fi

    echo ""
}

# ---------------------------------------------------------------------------
# Delete all existing connectors (--reset flag)
# ---------------------------------------------------------------------------
reset_connectors() {
    warn "Deleting all existing connectors..."
    local connectors
    connectors=$(curl -sf "$CONNECT_URL/connectors" | python3 -c "import json,sys; print('\n'.join(json.load(sys.stdin)))")
    for name in $connectors; do
        log "  Deleting: $name"
        curl -sf -X DELETE "$CONNECT_URL/connectors/$name"
    done
    log "All connectors deleted."
}

# ---------------------------------------------------------------------------
# Show connector status
# ---------------------------------------------------------------------------
show_status() {
    log "Connector status:"
    local connectors
    connectors=$(curl -sf "$CONNECT_URL/connectors?expand=status" | python3 -m json.tool)
    echo "$connectors"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    if [[ "${1:-}" == "--reset" ]]; then
        wait_for_connect
        reset_connectors
    fi

    wait_for_connect

    # Register all connector JSON files in order (01-, 02-, etc.)
    for file in "$CONNECTORS_DIR"/*.json; do
        if [[ -f "$file" ]]; then
            register_connector "$file"
        fi
    done

    echo ""
    log "All connectors registered. Waiting 5s for status..."
    sleep 5
    show_status
}

main "$@"