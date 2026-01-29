#!/bin/bash
#
# Simulate infrastructure failures for testing the diagnostic and predictive alerts tools
#
# Usage:
#   ./simulate_failure.sh <action> <target> [options]
#
# Actions:
#   block      - Block connections/stop the service (hard failure - no telemetry)
#   unblock    - Restore connections/start the service
#   degrade    - Introduce performance degradation (slow queries, partial failures)
#   restore    - Remove degradation
#   inject     - Inject failures via otel-demo API
#   status     - Check current status
#
# Targets for block/unblock/degrade:
#   postgres  - PostgreSQL database
#   redis     - Redis cache
#   kafka     - Kafka message broker
#   <service> - Any docker compose service name
#
# Targets for inject (otel-demo API):
#   payment-failure    - Partial payment failures (10%, 25%, 50%, 75%, 90%, 100%)
#   slow-images        - Slow loading images (5sec, 10sec)
#   cart-failure       - Cart service failures
#   ad-failure         - Ad service failures
#   memory-leak        - Email service memory leak (1x, 10x, 100x, 1000x)
#   kafka-problems     - Kafka queue problems
#   recommendation-cache - Cache failures
#
# Examples:
#   # Hard failures (produces no telemetry - hard to detect)
#   ./simulate_failure.sh block postgres
#   ./simulate_failure.sh unblock postgres
#
#   # Degraded performance (produces telemetry patterns - detectable by root cause monitoring)
#   ./simulate_failure.sh degrade postgres slow        # Slow queries
#   ./simulate_failure.sh degrade postgres memory      # Memory pressure
#   ./simulate_failure.sh restore postgres
#
#   # Inject application failures via otel-demo API
#   ./simulate_failure.sh inject payment-failure 50%   # 50% payment failures
#   ./simulate_failure.sh inject slow-images 5sec
#   ./simulate_failure.sh inject memory-leak 100x
#   ./simulate_failure.sh inject payment-failure off   # Restore
#

set -e

ACTION=${1:-status}
TARGET=${2:-postgres}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# PostgreSQL specific functions
postgres_block() {
    log_info "Blocking PostgreSQL connections..."
    docker compose exec -T postgresql psql -U root -d "${POSTGRES_DB:-otel}" -c \
        "REVOKE CONNECT ON DATABASE ${POSTGRES_DB:-otel} FROM PUBLIC;" 2>/dev/null || true

    log_info "Terminating existing connections..."
    docker compose exec -T postgresql psql -U root -d "${POSTGRES_DB:-otel}" -c \
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${POSTGRES_DB:-otel}' AND pid <> pg_backend_pid();" 2>/dev/null || true

    log_info "PostgreSQL connections blocked. Services will fail to connect."
}

postgres_unblock() {
    log_info "Restoring PostgreSQL connections..."
    docker compose exec -T postgresql psql -U root -d "${POSTGRES_DB:-otel}" -c \
        "GRANT CONNECT ON DATABASE ${POSTGRES_DB:-otel} TO PUBLIC;" 2>/dev/null || true

    log_info "PostgreSQL connections restored."
}

postgres_status() {
    log_info "Checking PostgreSQL status..."
    if docker compose exec -T postgresql psql -U root -c "SELECT 1;" > /dev/null 2>&1; then
        echo -e "PostgreSQL: ${GREEN}RUNNING${NC}"

        # Check connection permissions
        CONNECT_PRIV=$(docker compose exec -T postgresql psql -U root -d "${POSTGRES_DB:-otel}" -t -c \
            "SELECT has_database_privilege('public', '${POSTGRES_DB:-otel}', 'CONNECT');" 2>/dev/null | tr -d ' ')

        if [ "$CONNECT_PRIV" = "t" ]; then
            echo -e "Connections: ${GREEN}ALLOWED${NC}"
        else
            echo -e "Connections: ${RED}BLOCKED${NC}"
        fi

        # Check for slow query simulation
        SLOW_QUERY_FUNC=$(docker compose exec -T postgresql psql -U root -d "${POSTGRES_DB:-otel}" -t -c \
            "SELECT COUNT(*) FROM pg_proc WHERE proname = 'simulate_slow_query';" 2>/dev/null | tr -d ' ')

        if [ "$SLOW_QUERY_FUNC" = "1" ]; then
            echo -e "Slow query simulation: ${YELLOW}ACTIVE${NC}"
        else
            echo -e "Slow query simulation: ${GREEN}INACTIVE${NC}"
        fi
    else
        echo -e "PostgreSQL: ${RED}DOWN${NC}"
    fi
}

# PostgreSQL degradation functions (produces telemetry patterns)
postgres_degrade() {
    MODE=${3:-slow}
    case "$MODE" in
        slow)
            postgres_degrade_slow
            ;;
        memory)
            postgres_degrade_memory
            ;;
        *)
            log_error "Unknown degradation mode: $MODE (use 'slow' or 'memory')"
            exit 1
            ;;
    esac
}

postgres_degrade_slow() {
    log_info "Enabling slow query simulation..."

    # Create a function that adds artificial delay to queries
    # This will be called by a trigger on the most common table
    docker compose exec -T postgresql psql -U root -d "${POSTGRES_DB:-otel}" <<'EOF'
-- Create a function that simulates slow queries by adding random delay
CREATE OR REPLACE FUNCTION simulate_slow_query()
RETURNS trigger AS $$
BEGIN
    -- Add 100-500ms delay to simulate slow queries
    PERFORM pg_sleep(0.1 + random() * 0.4);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Also reduce work_mem to increase query times
ALTER SYSTEM SET work_mem = '1MB';
SELECT pg_reload_conf();
EOF

    log_info "Slow query simulation enabled."
    log_info "Queries will now have 100-500ms artificial delay."
    log_info "This should trigger 'db_slow_queries' alerts in root cause monitoring."
}

postgres_degrade_memory() {
    log_info "Simulating PostgreSQL memory pressure..."

    # Reduce shared_buffers and work_mem to create memory pressure
    docker compose exec -T postgresql psql -U root -d "${POSTGRES_DB:-otel}" <<'EOF'
-- Reduce work memory to create pressure
ALTER SYSTEM SET work_mem = '512kB';
ALTER SYSTEM SET maintenance_work_mem = '16MB';

-- Increase temp_buffers to consume memory differently
ALTER SYSTEM SET temp_buffers = '32MB';

SELECT pg_reload_conf();

-- Create a table to consume shared memory
CREATE TABLE IF NOT EXISTS _memory_pressure (
    id SERIAL PRIMARY KEY,
    data TEXT
);

-- Fill it with some data
INSERT INTO _memory_pressure (data)
SELECT repeat('x', 10000) FROM generate_series(1, 1000);
EOF

    log_info "Memory pressure simulation enabled."
    log_info "PostgreSQL is now running with reduced memory settings."
}

postgres_restore() {
    log_info "Restoring PostgreSQL to normal operation..."

    docker compose exec -T postgresql psql -U root -d "${POSTGRES_DB:-otel}" <<'EOF'
-- Remove slow query function
DROP FUNCTION IF EXISTS simulate_slow_query() CASCADE;

-- Reset memory settings to defaults
ALTER SYSTEM RESET work_mem;
ALTER SYSTEM RESET maintenance_work_mem;
ALTER SYSTEM RESET temp_buffers;

SELECT pg_reload_conf();

-- Clean up memory pressure table
DROP TABLE IF EXISTS _memory_pressure;
EOF

    log_info "PostgreSQL restored to normal operation."
}

# Redis specific functions
redis_block() {
    log_info "Pausing Redis container..."
    docker compose pause redis 2>/dev/null || docker compose pause valkey-cart 2>/dev/null || {
        log_error "Could not find Redis container (tried 'redis' and 'valkey-cart')"
        exit 1
    }
    log_info "Redis paused. Services will timeout on Redis operations."
}

redis_unblock() {
    log_info "Unpausing Redis container..."
    docker compose unpause redis 2>/dev/null || docker compose unpause valkey-cart 2>/dev/null || {
        log_error "Could not find Redis container"
        exit 1
    }
    log_info "Redis restored."
}

redis_status() {
    log_info "Checking Redis status..."
    # Try both common redis container names
    for name in redis valkey-cart; do
        STATUS=$(docker compose ps --format json 2>/dev/null | grep -o "\"$name\"[^}]*" | head -1)
        if [ -n "$STATUS" ]; then
            if echo "$STATUS" | grep -q "paused"; then
                echo -e "Redis ($name): ${YELLOW}PAUSED${NC}"
            elif echo "$STATUS" | grep -q "running"; then
                echo -e "Redis ($name): ${GREEN}RUNNING${NC}"
            else
                echo -e "Redis ($name): ${RED}DOWN${NC}"
            fi
            return
        fi
    done
    echo -e "Redis: ${RED}NOT FOUND${NC}"
}

# =============================================================================
# otel-demo API injection functions
# =============================================================================

OTEL_DEMO_HOST=${OTEL_DEMO_HOST:-localhost}
OTEL_DEMO_PORT=${OTEL_DEMO_PORT:-8080}
OTEL_DEMO_ADMIN_URL="http://${OTEL_DEMO_HOST}:${OTEL_DEMO_PORT}/feature"

inject_failure() {
    FAILURE_TYPE=$2
    VALUE=${3:-on}

    # Map friendly names to otel-demo API method names
    case "$FAILURE_TYPE" in
        payment-failure|payment)
            METHOD="paymentServiceFailure"
            # Convert percentage to otel-demo format
            case "$VALUE" in
                off)   VALUE="0" ;;
                10%)   VALUE="10" ;;
                25%)   VALUE="25" ;;
                50%)   VALUE="50" ;;
                75%)   VALUE="75" ;;
                90%)   VALUE="90" ;;
                100%|on) VALUE="100" ;;
            esac
            ;;
        slow-images|slow-image)
            METHOD="imageSlowLoad"
            # Convert to otel-demo format
            case "$VALUE" in
                off)   VALUE="0" ;;
                5sec)  VALUE="5000" ;;
                10sec) VALUE="10000" ;;
            esac
            ;;
        cart-failure|cart)
            METHOD="cartServiceFailure"
            case "$VALUE" in
                off) VALUE="0" ;;
                on)  VALUE="1" ;;
            esac
            ;;
        ad-failure|ad)
            METHOD="adServiceFailure"
            case "$VALUE" in
                off) VALUE="0" ;;
                on)  VALUE="1" ;;
            esac
            ;;
        memory-leak|memleak)
            METHOD="recommendationServiceCacheFailure"
            # This causes recommendation service to consume memory
            case "$VALUE" in
                off)    VALUE="0" ;;
                1x)     VALUE="1" ;;
                10x)    VALUE="10" ;;
                100x)   VALUE="100" ;;
                1000x)  VALUE="1000" ;;
                10000x) VALUE="10000" ;;
            esac
            ;;
        kafka-problems|kafka-queue)
            METHOD="kafkaQueueProblems"
            case "$VALUE" in
                off) VALUE="0" ;;
                on)  VALUE="1" ;;
            esac
            ;;
        recommendation-cache|rec-cache)
            METHOD="recommendationServiceCacheFailure"
            case "$VALUE" in
                off) VALUE="0" ;;
                on)  VALUE="1" ;;
            esac
            ;;
        product-failure|product)
            METHOD="productCatalogFailure"
            case "$VALUE" in
                off) VALUE="0" ;;
                on)  VALUE="1" ;;
            esac
            ;;
        *)
            log_error "Unknown failure type: $FAILURE_TYPE"
            echo "Available types: payment-failure, slow-images, cart-failure, ad-failure,"
            echo "                 memory-leak, kafka-problems, recommendation-cache, product-failure"
            exit 1
            ;;
    esac

    log_info "Injecting failure via otel-demo API: $METHOD=$VALUE"

    # Call the otel-demo feature flag API
    RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
        "${OTEL_DEMO_ADMIN_URL}/${METHOD}/${VALUE}" \
        -H "Content-Type: application/json" 2>/dev/null || echo "error")

    HTTP_CODE=$(echo "$RESPONSE" | tail -1)
    BODY=$(echo "$RESPONSE" | head -n -1)

    if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "204" ]; then
        log_info "Successfully set $METHOD=$VALUE"
        if [ "$VALUE" = "0" ] || [ "$VALUE" = "off" ]; then
            echo -e "Failure injection: ${GREEN}DISABLED${NC}"
        else
            echo -e "Failure injection: ${YELLOW}ACTIVE${NC} ($METHOD=$VALUE)"
        fi
    else
        log_warn "Could not reach otel-demo API at $OTEL_DEMO_ADMIN_URL"
        log_warn "HTTP $HTTP_CODE: $BODY"
        log_info "Make sure otel-demo is running and OTEL_DEMO_HOST/OTEL_DEMO_PORT are set correctly"
    fi
}

inject_status() {
    log_info "Checking otel-demo feature flags..."

    RESPONSE=$(curl -s "${OTEL_DEMO_ADMIN_URL}" 2>/dev/null || echo "error")

    if [ "$RESPONSE" = "error" ]; then
        log_warn "Could not reach otel-demo API at $OTEL_DEMO_ADMIN_URL"
        return
    fi

    echo "$RESPONSE" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    for key, value in data.items():
        if value and value != '0' and value != 0:
            print(f'  {key}: \033[1;33mACTIVE\033[0m ({value})')
        else:
            print(f'  {key}: inactive')
except:
    print('Could not parse response')
" 2>/dev/null || echo "  (Could not parse feature flag status)"
}

# =============================================================================
# Generic service functions (stop/start)
# =============================================================================

service_block() {
    log_info "Stopping $TARGET..."
    docker compose stop "$TARGET"
    log_info "$TARGET stopped."
}

service_unblock() {
    log_info "Starting $TARGET..."
    docker compose start "$TARGET"
    log_info "$TARGET started."
}

service_status() {
    log_info "Checking $TARGET status..."
    STATUS=$(docker compose ps "$TARGET" --format "{{.Status}}" 2>/dev/null)
    if [ -z "$STATUS" ]; then
        echo -e "$TARGET: ${RED}NOT FOUND${NC}"
    elif echo "$STATUS" | grep -qi "up"; then
        echo -e "$TARGET: ${GREEN}RUNNING${NC}"
    elif echo "$STATUS" | grep -qi "paused"; then
        echo -e "$TARGET: ${YELLOW}PAUSED${NC}"
    else
        echo -e "$TARGET: ${RED}DOWN${NC} ($STATUS)"
    fi
}

# =============================================================================
# Main logic
# =============================================================================

# Handle 'inject' action separately (works differently)
if [ "$ACTION" = "inject" ]; then
    if [ -z "$TARGET" ] || [ "$TARGET" = "status" ]; then
        inject_status
    else
        inject_failure "$ACTION" "$TARGET" "$3"
    fi
    echo ""
    log_info "Done. Wait 30-60 seconds for effects to propagate."
    log_info "Check the Predictive Alerts page for root cause detection."
    exit 0
fi

# Handle infrastructure targets
case "$TARGET" in
    postgres|postgresql)
        case "$ACTION" in
            block)   postgres_block ;;
            unblock) postgres_unblock ;;
            degrade) postgres_degrade "$@" ;;
            restore) postgres_restore ;;
            status)  postgres_status ;;
            *)       log_error "Unknown action: $ACTION"; exit 1 ;;
        esac
        ;;
    redis|valkey|valkey-cart)
        case "$ACTION" in
            block)   redis_block ;;
            unblock) redis_unblock ;;
            status)  redis_status ;;
            *)       log_error "Unknown action: $ACTION (redis only supports block/unblock/status)"; exit 1 ;;
        esac
        ;;
    *)
        # Generic service handling
        case "$ACTION" in
            block)   service_block ;;
            unblock) service_unblock ;;
            status)  service_status ;;
            *)       log_error "Unknown action: $ACTION"; exit 1 ;;
        esac
        ;;
esac

echo ""
log_info "Done. Wait 30-60 seconds for effects to propagate."
echo ""
echo "Root cause monitoring should detect:"
case "$ACTION" in
    degrade)
        echo "  - DB_SLOW_QUERIES: Database query latency increased"
        echo "  - Possibly ERROR_SPIKE if queries start timing out"
        ;;
    block)
        echo "  - DB_CONNECTION_FAILURE: Database connection errors"
        echo "  - DEPENDENCY_FAILURE: Downstream services failing"
        echo "  - SERVICE_DOWN: If telemetry stops completely"
        ;;
    inject)
        echo "  - DEPENDENCY_FAILURE: Service call failures"
        echo "  - LATENCY_DEGRADATION: Slow service responses"
        echo "  - EXCEPTION_SURGE: Increased exception rates"
        ;;
esac
echo ""
log_info "Check the Predictive Alerts page for root cause detection."
