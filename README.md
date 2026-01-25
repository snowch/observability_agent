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
