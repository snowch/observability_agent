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

To test the diagnostic capabilities, you can simulate infrastructure failures in the OpenTelemetry demo using the provided script.

### Using the Simulation Script

```bash
# From the opentelemetry-demo directory:
cd /path/to/opentelemetry-demo

# Copy the script or run from the observability_agent directory
./scripts/simulate_failure.sh <action> <target>

# Actions: block, unblock, status
# Targets: postgres, redis, kafka, or any docker compose service name
```

### Examples

```bash
# Block PostgreSQL (gracefully blocks connections)
./scripts/simulate_failure.sh block postgres

# Check status
./scripts/simulate_failure.sh status postgres

# Restore PostgreSQL
./scripts/simulate_failure.sh unblock postgres

# Pause Redis (simulates timeout/hang)
./scripts/simulate_failure.sh block redis

# Stop any service completely
./scripts/simulate_failure.sh block checkoutservice
```

### Manual Testing

You can also manually simulate failures:

```bash
# PostgreSQL - block connections
docker compose exec postgresql psql -U root -d otel -c "REVOKE CONNECT ON DATABASE otel FROM PUBLIC;"

# PostgreSQL - restore
docker compose exec postgresql psql -U root -d otel -c "GRANT CONNECT ON DATABASE otel TO PUBLIC;"

# Any service - stop/start
docker compose stop <service>
docker compose start <service>

# Any service - pause/unpause (simulates hang)
docker compose pause <service>
docker compose unpause <service>
```

### What to Look For

After simulating a failure, wait 30-60 seconds for effects to propagate, then ask the diagnostic chat:
- "Show me errors in the last 5 minutes"
- "What's wrong with the system?"
- "Diagnose the frontend issues"

The AI should follow the SysAdmin diagnostic process:
1. Check infrastructure health first (databases, hosts, services)
2. Look for connection errors, timeouts, and missing telemetry
3. Trace errors through the dependency chain
4. Identify the root cause component
