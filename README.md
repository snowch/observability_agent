# Observability Agent

## Prerequisites

- VAST Kafka Broker with three topics
  - otel-logs
  - otel-traces
  - otel-metrics
- VAST DB bucket/schema access credentials

## Instructions

- Deploy [otel demo](https://opentelemetry.io/ecosystem/demo/)
- Replace the otel demo src/otel-collector/otelcol-config.yml with [otelcol-config.yml](./otelcol-config.yml)
- Modify the kafka broker name in `otelcol-config.yml`
- Restart otel demo
- Run the script [otel_ingester.py](./otel_ingester.py) using nohup/screen/tmux

## Connect Trino to VAST DB

- Connect Trino to VAST DB

## Diagnostic Chat Tool

An interactive LLM-powered chat interface for support engineers to diagnose issues by querying observability data via Trino.

### Features

- Natural language queries like "ad service is slow" or "show me errors in checkout"
- Iterative diagnosis - the LLM runs multiple queries to find root causes
- Correlates logs, metrics, and traces automatically
- Full SQL support via Trino (JOINs, GROUP BY, aggregations, etc.)

### Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

Set environment variables:

```bash
export ANTHROPIC_API_KEY=your_api_key
export TRINO_HOST=trino.example.com
export TRINO_PORT=443
export TRINO_USER=your_user
export TRINO_CATALOG=vast
export TRINO_SCHEMA=otel
```

### Usage

```bash
python diagnostic_chat.py
```

### Example Queries

| Query | What it does |
|-------|--------------|
| "ad service is slow" | Investigates latency in the ad service |
| "what errors occurred in the last hour?" | Finds recent errors across all services |
| "show me failed checkouts" | Finds checkout failures with traces |
| "trace request abc123" | Shows full trace for a specific request |
| "why is the frontend timing out?" | Diagnoses timeout issues |

### Commands (CLI)

- `/clear` - Clear conversation history
- `/help` - Show help message
- `/quit` - Exit the chat

### Web UI

A browser-based interface with real-time system status monitoring.

```bash
python web_ui.py
```

Then open http://localhost:5000 in your browser.

**Features:**
- Chat interface for diagnosing issues
- Real-time service health dashboard
- Database status monitoring
- Recent errors feed
- Query result visualization

## Testing with Simulated Failures

To test the diagnostic capabilities, you can simulate infrastructure failures in the OpenTelemetry demo.

### Simulate PostgreSQL Failure

The PostgreSQL database is used by the Accounting service. To simulate a database outage:

```bash
# Enter the PostgreSQL container
docker compose exec -it postgresql /bin/bash

# Block new connections AND terminate existing ones
psql -U root -d ${POSTGRES_DB} -c "REVOKE CONNECT ON DATABASE ${POSTGRES_DB} FROM PUBLIC;"
psql -U root -d ${POSTGRES_DB} -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '${POSTGRES_DB}' AND pid <> pg_backend_pid();"
```

This will cause:
- Services depending on PostgreSQL to fail with connection errors
- Cascading failures in dependent services
- The diagnostic tool should identify PostgreSQL as the root cause

**To restore:**
```bash
psql -U root -d ${POSTGRES_DB} -c "GRANT CONNECT ON DATABASE ${POSTGRES_DB} TO PUBLIC;"
```

### Simulate Complete Service Outage

To completely stop a service (e.g., PostgreSQL):

```bash
# From the host machine (outside containers)
docker compose stop postgresql

# To restore
docker compose start postgresql
```

### What to Look For

When testing, ask the diagnostic chat:
- "Show me errors in the last 5 minutes"
- "What's wrong with the system?"
- "Diagnose the frontend issues"

The AI should:
1. Check infrastructure health first (database spans, metrics)
2. Identify missing telemetry from the affected service
3. Trace cascading errors back to the root cause
4. Recommend checking the specific failed component
