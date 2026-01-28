#!/bin/bash
#
# Simulate infrastructure failures for testing the diagnostic tool
#
# Usage:
#   ./simulate_failure.sh <action> <target>
#
# Actions:
#   block   - Block connections/stop the service
#   unblock - Restore connections/start the service
#   status  - Check current status
#
# Targets:
#   postgres  - PostgreSQL database
#   redis     - Redis cache
#   kafka     - Kafka message broker
#   <service> - Any docker compose service name
#
# Examples:
#   ./simulate_failure.sh block postgres
#   ./simulate_failure.sh unblock postgres
#   ./simulate_failure.sh status postgres
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
    else
        echo -e "PostgreSQL: ${RED}DOWN${NC}"
    fi
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

# Generic service functions (stop/start)
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

# Main logic
case "$TARGET" in
    postgres|postgresql)
        case "$ACTION" in
            block)   postgres_block ;;
            unblock) postgres_unblock ;;
            status)  postgres_status ;;
            *)       log_error "Unknown action: $ACTION"; exit 1 ;;
        esac
        ;;
    redis|valkey|valkey-cart)
        case "$ACTION" in
            block)   redis_block ;;
            unblock) redis_unblock ;;
            status)  redis_status ;;
            *)       log_error "Unknown action: $ACTION"; exit 1 ;;
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
log_info "Done. Wait 30-60 seconds for effects to propagate, then check the diagnostic tool."
