# Integration Instructions for Claude Session

## Context

This is a fork of the [OpenTelemetry Demo](https://github.com/open-telemetry/opentelemetry-demo) with VAST Data observability assets in the `vast/` folder. The goal is to integrate these assets into the otel-demo so everything runs with a single `docker compose up`.

## Current State

The `vast/` folder contains:
- `otel_ingester.py` - Kafka consumer that writes OTEL data to VastDB
- `web_ui.py` - Flask web dashboard (port 5001) with diagnostic chat, predictive alerts, service health monitoring
- `predictive_alerts.py` - Automated anomaly detection with LLM-powered investigation
- `diagnostic_chat.py` - Interactive LLM-powered diagnosis via Trino SQL
- `otel_query.py` - Simple VastDB query utility
- `otelcol-config.yml` - Custom OpenTelemetry Collector config that exports to Kafka (for VastDB ingestion)
- `ddl.sql` - Database schema for baselines/alerts tables
- `requirements.txt` - Python dependencies (vastdb, kafka-python, pyarrow, anthropic, trino, flask, scikit-learn, numpy)
- `scripts/simulate_failure.sh` - Fault injection tooling
- `scripts/kafka_sizing.py` - Kafka throughput estimator
- `templates/index.html` - Web UI dashboard template
- `.env-template` - Environment variable template
- `failure_simulator.md` - Documentation for failure injection

## Tasks

### 1. Create `vast/Dockerfile`

Create a Dockerfile for the observability agent:
- Base image: `python:3.12-slim`
- Install `curl` system dependency
- Install Python deps from `requirements.txt`
- Copy `*.py`, `ddl.sql`, `templates/`
- Expose port 5001
- Default CMD: `python web_ui.py`

### 2. Add services to `docker-compose.yml`

Add these services to `docker-compose.yml`:

```yaml
  # *************************
  # VAST Observability Agent
  # *************************
  # Web UI + Predictive Alerts + Diagnostic Chat
  observability-agent:
    build:
      context: ./vast
      dockerfile: Dockerfile
    container_name: observability-agent
    deploy:
      resources:
        limits:
          memory: 512M
    restart: unless-stopped
    ports:
      - "${OBSERVABILITY_AGENT_PORT}"
    environment:
      - TRINO_HOST
      - TRINO_PORT
      - TRINO_USER
      - TRINO_CATALOG
      - TRINO_SCHEMA
      - TRINO_VERIFY
      - ANTHROPIC_API_KEY
      - VASTDB_ENDPOINT
      - VASTDB_ACCESS_KEY
      - VASTDB_SECRET_KEY
      - VASTDB_BUCKET
      - VASTDB_SCHEMA
      - KAFKA_BOOTSTRAP_SERVERS
      - POSTGRES_HOST
      - POSTGRES_PORT
      - POSTGRES_DB
      - POSTGRES_PASSWORD
      - DETECTION_INTERVAL
      - BASELINE_INTERVAL
      - BASELINE_WINDOW_HOURS
      - ANOMALY_THRESHOLD
      - ERROR_RATE_WARNING
      - ERROR_RATE_CRITICAL
      - MAX_INVESTIGATIONS_PER_HOUR
      - INVESTIGATION_SERVICE_COOLDOWN_MINUTES
      - INVESTIGATE_CRITICAL_ONLY
    depends_on:
      otel-collector:
        condition: service_started
    logging: *logging

  # Data ingester: Kafka → VastDB
  observability-ingester:
    build:
      context: ./vast
      dockerfile: Dockerfile
    container_name: observability-ingester
    deploy:
      resources:
        limits:
          memory: 256M
    restart: unless-stopped
    command: ["python", "otel_ingester.py"]
    environment:
      - VASTDB_ENDPOINT
      - VASTDB_ACCESS_KEY
      - VASTDB_SECRET_KEY
      - VASTDB_BUCKET
      - VASTDB_SCHEMA
      - KAFKA_BOOTSTRAP_SERVERS
    depends_on:
      otel-collector:
        condition: service_started
    logging: *logging

  # PostgreSQL latency proxy for fault injection testing
  pg-latency-proxy:
    image: alpine/socat
    container_name: pg-latency-proxy
    deploy:
      resources:
        limits:
          memory: 32M
    cap_add:
      - NET_ADMIN
    command: TCP-LISTEN:5432,fork,reuseaddr TCP:postgresql-direct:5432
    profiles:
      - fault-injection
    logging: *logging
```

### 3. Add environment variables to `.env`

Add these variables to the `.env` file (after the Postgres section):

```
# *************************
# VAST Observability Agent
# *************************
OBSERVABILITY_AGENT_PORT=5001
```

Note: The remaining env vars (TRINO_*, VASTDB_*, ANTHROPIC_API_KEY, etc.) should be set by the user in `.env.override` or sourced from `vast/.env-template`. They should NOT have default values in `.env` since they contain deployment-specific credentials.

### 4. Replace the OpenTelemetry Collector config

Replace `src/otel-collector/otelcol-config.yml` with the contents of `vast/otelcol-config.yml`. This custom config adds:
- Kafka exporter for logs, traces, and metrics (3 topics: otel-logs, otel-traces, otel-metrics)
- PostgreSQL receiver for database metrics
- Redis receiver for cache metrics
- Host metrics receiver
- Docker stats receiver

**Important**: The Kafka broker address in the config may need updating. Check for hardcoded IPs and replace with the `KAFKA_BOOTSTRAP_SERVERS` env var reference.

### 5. Update `vast/scripts/simulate_failure.sh`

The simulate_failure.sh script has a `postgres_degrade_slow()` function that uses a TCP proxy approach for PostgreSQL latency injection. Since the `pg-latency-proxy` service is now defined in `docker-compose.yml`:

- The `degrade postgres slow` command should use `docker compose --profile fault-injection up -d pg-latency-proxy` to start the proxy
- It swaps PostgreSQL's DNS alias from `postgresql` to `postgresql-direct`, then gives the proxy the `postgresql` alias
- The `restore postgres` command stops the proxy and restores the original alias
- The `status postgres` command checks if the proxy is running and measures actual query latency

### 6. Key architecture notes

**How services connect to PostgreSQL:**
- All services use `POSTGRES_HOST` env var (defaults to `postgresql`)
- Three connection string formats are used across services:
  - .NET (accounting): `Host=${POSTGRES_HOST};Username=otelu;Password=otelp;Database=${POSTGRES_DB}`
  - Go (product-catalog): `postgres://otelu:otelp@${POSTGRES_HOST}/${POSTGRES_DB}?sslmode=disable`
  - Python (product-reviews): `host=${POSTGRES_HOST} user=otelu password=otelp dbname=${POSTGRES_DB}`

**Docker network:** All services are on the `opentelemetry-demo` bridge network.

**Data flow:**
```
otel-demo services → otel-collector → Kafka (3 topics)
                                         ↓
                              observability-ingester
                                         ↓
                                      VastDB
                                         ↓
                                   Trino (queries)
                                         ↓
                              observability-agent (web UI)
```

### 7. Verify the integration

After making changes:
1. `docker compose build observability-agent observability-ingester` should succeed
2. `docker compose up -d` should start all services including the new ones
3. The observability agent dashboard should be accessible on port 5001
4. `vast/scripts/simulate_failure.sh status postgres` should work from the repo root
