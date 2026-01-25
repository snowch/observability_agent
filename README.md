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

An interactive LLM-powered chat interface for support engineers to diagnose issues by querying observability data.

### Features

- Natural language queries like "ad service is slow" or "show me errors in checkout"
- Iterative diagnosis - the LLM runs multiple queries to find root causes
- Correlates logs, metrics, and traces automatically
- Supports both Trino (full SQL) and VastDB direct (basic queries)

### Setup

Install dependencies:

```bash
pip install -r requirements.txt
```

Set environment variables:

```bash
# Required
export ANTHROPIC_API_KEY=your_api_key

# Option 1: Use Trino (recommended for complex queries)
export TRINO_HOST=trino.example.com
export TRINO_PORT=443
export TRINO_USER=your_user
export TRINO_CATALOG=vast
export TRINO_SCHEMA=otel

# Option 2: Use VastDB directly (simpler setup)
export VASTDB_ENDPOINT=http://vastdb:8080
export VASTDB_ACCESS_KEY=your_access_key
export VASTDB_SECRET_KEY=your_secret_key
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

### Commands

- `/clear` - Clear conversation history
- `/help` - Show help message
- `/quit` - Exit the chat
