#!/usr/bin/env python3
"""
Web UI for Observability Diagnostic Chat

A web-based interface for support engineers to diagnose issues
and monitor system status.

Usage:
    export ANTHROPIC_API_KEY=your_api_key
    export TRINO_HOST=trino.example.com
    python web_ui.py

Then open http://localhost:5000 in your browser.
"""

import urllib3
import warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore", message=".*model.*is deprecated.*")

import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List

from flask import Flask, render_template, request, jsonify, Response
import anthropic

try:
    from trino.dbapi import connect as trino_connect
    from trino.auth import BasicAuthentication
    TRINO_AVAILABLE = True
except ImportError:
    TRINO_AVAILABLE = False

# =============================================================================
# Configuration
# =============================================================================

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-haiku-20241022")

TRINO_HOST = os.getenv("TRINO_HOST")
TRINO_PORT = int(os.getenv("TRINO_PORT", "443"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "vast")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "otel")
TRINO_HTTP_SCHEME = os.getenv("TRINO_HTTP_SCHEME", "https")

MAX_QUERY_ROWS = 100

app = Flask(__name__)

# Import the system prompt from diagnostic_chat
from diagnostic_chat import SYSTEM_PROMPT

# =============================================================================
# Trino Query Executor
# =============================================================================

class TrinoQueryExecutor:
    """Executes SQL queries against VastDB via Trino."""

    def __init__(self):
        if not TRINO_AVAILABLE:
            raise ImportError("trino package not installed")

        auth = None
        if TRINO_PASSWORD:
            auth = BasicAuthentication(TRINO_USER, TRINO_PASSWORD)

        self.conn = trino_connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog=TRINO_CATALOG,
            schema=TRINO_SCHEMA,
            http_scheme=TRINO_HTTP_SCHEME,
            auth=auth,
            verify=False,
        )

    def execute_query(self, sql: str) -> Dict[str, Any]:
        """Execute a SQL query via Trino."""
        sql = sql.strip()

        if not sql.lower().startswith("select"):
            return {"success": False, "error": "Only SELECT queries are supported", "rows": [], "columns": []}

        sql_lower = sql.lower()
        if "limit" not in sql_lower:
            sql = sql.rstrip(";") + f" LIMIT {MAX_QUERY_ROWS}"
        else:
            match = re.search(r'\blimit\s+(\d+)', sql_lower)
            if match and int(match.group(1)) > MAX_QUERY_ROWS:
                sql = re.sub(r'\blimit\s+\d+', f'LIMIT {MAX_QUERY_ROWS}', sql, flags=re.IGNORECASE)

        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            raw_rows = cursor.fetchall()

            rows = []
            for raw_row in raw_rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    val = raw_row[i]
                    if hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    row_dict[col] = val
                rows.append(row_dict)

            return {"success": True, "rows": rows, "columns": columns, "row_count": len(rows)}

        except Exception as e:
            return {"success": False, "error": f"{type(e).__name__}: {str(e)}", "rows": [], "columns": []}


# Global instances
query_executor = None
anthropic_client = None

def get_query_executor():
    global query_executor
    if query_executor is None:
        query_executor = TrinoQueryExecutor()
    return query_executor

def get_anthropic_client():
    global anthropic_client
    if anthropic_client is None:
        anthropic_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    return anthropic_client


# =============================================================================
# Chat Session Management
# =============================================================================

chat_sessions = {}

def get_or_create_session(session_id: str) -> List[Dict]:
    if session_id not in chat_sessions:
        chat_sessions[session_id] = []
    return chat_sessions[session_id]


# =============================================================================
# Routes
# =============================================================================

@app.route('/')
def index():
    return render_template('index.html')


def get_chat_tools():
    """Return the tools definition for the chat endpoint."""
    return [{
        "name": "execute_sql",
        "description": "Execute a SQL query against the observability database",
        "input_schema": {
            "type": "object",
            "properties": {"sql": {"type": "string", "description": "The SQL SELECT query"}},
            "required": ["sql"]
        }
    }, {
        "name": "generate_chart",
        "description": """Generate a chart/graph to visualize data. Use this when the user asks for visualizations, trends, or graphs.

The chart will be rendered in the chat interface. Supported chart types:
- line: For time series data (latency over time, error rates over time)
- bar: For comparing values across categories (errors by service)
- doughnut: For showing proportions (request distribution)

IMPORTANT: Always provide data sorted by the x-axis (usually time) for line charts.""",
        "input_schema": {
            "type": "object",
            "properties": {
                "chart_type": {
                    "type": "string",
                    "enum": ["line", "bar", "doughnut"],
                    "description": "Type of chart to generate"
                },
                "title": {
                    "type": "string",
                    "description": "Chart title"
                },
                "labels": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "X-axis labels (categories or time points)"
                },
                "datasets": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "label": {"type": "string", "description": "Dataset name (shown in legend)"},
                            "data": {"type": "array", "items": {"type": "number"}, "description": "Data values"},
                            "color": {"type": "string", "description": "Color (optional, e.g., '#00d9ff' or 'red')"}
                        },
                        "required": ["label", "data"]
                    },
                    "description": "One or more data series to plot"
                }
            },
            "required": ["chart_type", "title", "labels", "datasets"]
        }
    }]


@app.route('/api/chat/stream', methods=['POST'])
def chat_stream():
    """Handle chat messages with streaming progress updates via SSE."""
    data = request.json
    user_message = data.get('message', '')
    session_id = data.get('session_id', 'default')

    if not user_message:
        return jsonify({'error': 'No message provided'}), 400

    def generate():
        conversation_history = get_or_create_session(session_id)
        conversation_history.append({"role": "user", "content": user_message})

        # Keep history manageable
        if len(conversation_history) > 20:
            conversation_history = conversation_history[-20:]
            chat_sessions[session_id] = conversation_history

        client = get_anthropic_client()
        executor = get_query_executor()
        tools = get_chat_tools()

        executed_queries = []
        generated_charts = []
        iteration = 0
        max_iterations = 10  # Safety limit

        try:
            # Send initial status
            yield f"data: {json.dumps({'type': 'status', 'message': 'Analyzing your question...', 'step': 1})}\n\n"

            response = client.messages.create(
                model=ANTHROPIC_MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=tools,
                messages=conversation_history
            )

            # Handle tool use loop
            while response.stop_reason == "tool_use" and iteration < max_iterations:
                iteration += 1
                tool_results = []

                # Count tools to execute
                tool_count = sum(1 for cb in response.content if cb.type == "tool_use")
                tool_index = 0

                for content_block in response.content:
                    if content_block.type == "tool_use":
                        tool_index += 1

                        if content_block.name == "execute_sql":
                            sql = content_block.input.get("sql", "")
                            # Send query status
                            yield f"data: {json.dumps({'type': 'status', 'message': f'Executing query {tool_index}/{tool_count}...', 'step': iteration + 1, 'detail': sql[:80] + '...' if len(sql) > 80 else sql})}\n\n"

                            result = executor.execute_query(sql)
                            executed_queries.append({"sql": sql, "result": result})

                            row_count = result.get('row_count', 0) if result.get('success') else 0
                            yield f"data: {json.dumps({'type': 'query_result', 'query_index': len(executed_queries) - 1, 'row_count': row_count, 'success': result.get('success', False)})}\n\n"

                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": content_block.id,
                                "content": json.dumps(result, default=str)
                            })

                        elif content_block.name == "generate_chart":
                            yield f"data: {json.dumps({'type': 'status', 'message': f'Generating chart: {content_block.input.get(\"title\", \"Chart\")}...', 'step': iteration + 1})}\n\n"

                            chart_data = {
                                "chart_type": content_block.input.get("chart_type", "line"),
                                "title": content_block.input.get("title", "Chart"),
                                "labels": content_block.input.get("labels", []),
                                "datasets": content_block.input.get("datasets", [])
                            }
                            generated_charts.append(chart_data)
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": content_block.id,
                                "content": json.dumps({"success": True, "message": "Chart generated successfully"})
                            })

                conversation_history.append({"role": "assistant", "content": response.content})
                conversation_history.append({"role": "user", "content": tool_results})

                # Send analyzing status before next API call
                yield f"data: {json.dumps({'type': 'status', 'message': 'Analyzing results...', 'step': iteration + 1})}\n\n"

                response = client.messages.create(
                    model=ANTHROPIC_MODEL,
                    max_tokens=4096,
                    system=SYSTEM_PROMPT,
                    tools=tools,
                    messages=conversation_history
                )

            # Extract final response
            yield f"data: {json.dumps({'type': 'status', 'message': 'Preparing response...', 'step': iteration + 2})}\n\n"

            final_response = ""
            for content_block in response.content:
                if hasattr(content_block, 'text'):
                    final_response += content_block.text

            conversation_history.append({"role": "assistant", "content": final_response})

            # Send final result
            yield f"data: {json.dumps({'type': 'complete', 'response': final_response, 'queries': executed_queries, 'charts': generated_charts})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return Response(generate(), mimetype='text/event-stream', headers={
        'Cache-Control': 'no-cache',
        'X-Accel-Buffering': 'no'
    })


@app.route('/api/chat', methods=['POST'])
def chat():
    """Handle chat messages (non-streaming fallback)."""
    data = request.json
    user_message = data.get('message', '')
    session_id = data.get('session_id', 'default')

    if not user_message:
        return jsonify({'error': 'No message provided'}), 400

    conversation_history = get_or_create_session(session_id)
    conversation_history.append({"role": "user", "content": user_message})

    # Keep history manageable
    if len(conversation_history) > 20:
        conversation_history = conversation_history[-20:]
        chat_sessions[session_id] = conversation_history

    client = get_anthropic_client()
    executor = get_query_executor()
    tools = get_chat_tools()

    executed_queries = []
    generated_charts = []

    try:
        response = client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=tools,
            messages=conversation_history
        )

        # Handle tool use loop
        while response.stop_reason == "tool_use":
            tool_results = []

            for content_block in response.content:
                if content_block.type == "tool_use":
                    if content_block.name == "execute_sql":
                        sql = content_block.input.get("sql", "")
                        result = executor.execute_query(sql)
                        executed_queries.append({"sql": sql, "result": result})
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": content_block.id,
                            "content": json.dumps(result, default=str)
                        })
                    elif content_block.name == "generate_chart":
                        chart_data = {
                            "chart_type": content_block.input.get("chart_type", "line"),
                            "title": content_block.input.get("title", "Chart"),
                            "labels": content_block.input.get("labels", []),
                            "datasets": content_block.input.get("datasets", [])
                        }
                        generated_charts.append(chart_data)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": content_block.id,
                            "content": json.dumps({"success": True, "message": "Chart generated successfully"})
                        })

            conversation_history.append({"role": "assistant", "content": response.content})
            conversation_history.append({"role": "user", "content": tool_results})

            response = client.messages.create(
                model=ANTHROPIC_MODEL,
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=tools,
                messages=conversation_history
            )

        # Extract final response
        final_response = ""
        for content_block in response.content:
            if hasattr(content_block, 'text'):
                final_response += content_block.text

        conversation_history.append({"role": "assistant", "content": final_response})

        return jsonify({
            'response': final_response,
            'queries': executed_queries,
            'charts': generated_charts
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/clear', methods=['POST'])
def clear_session():
    """Clear chat session."""
    data = request.json
    session_id = data.get('session_id', 'default')
    chat_sessions[session_id] = []
    return jsonify({'status': 'cleared'})


@app.route('/api/status', methods=['GET'])
def system_status():
    """Get current system status."""
    executor = get_query_executor()
    time_param = request.args.get('time', '5m')

    # Parse time parameter
    time_value = int(time_param[:-1])
    time_unit = time_param[-1]

    if time_unit == 's':
        interval = f"'{time_value}' SECOND"
    elif time_unit == 'm':
        interval = f"'{time_value}' MINUTE"
    elif time_unit == 'h':
        interval = f"'{time_value}' HOUR"
    else:
        interval = "'5' MINUTE"

    status = {
        'services': [],
        'databases': [],
        'recent_errors': [],
        'error_summary': {},
        'timestamp': datetime.utcnow().isoformat()
    }

    # Get service health - discover from 1 hour, but calculate stats for selected window
    # Use two queries and merge in Python for better compatibility

    # First: discover all services from the last hour
    all_services_query = """
    SELECT DISTINCT service_name
    FROM traces_otel_analytic
    WHERE start_time > NOW() - INTERVAL '1' HOUR
    """
    all_services = set()
    result = executor.execute_query(all_services_query)
    if result['success']:
        all_services = {row['service_name'] for row in result['rows']}

    # Second: get stats for the selected time window
    stats_query = f"""
    SELECT service_name,
           COUNT(*) as total_spans,
           SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) as errors,
           ROUND(100.0 * SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as error_pct,
           ROUND(AVG(duration_ns / 1000000.0), 2) as avg_latency_ms
    FROM traces_otel_analytic
    WHERE start_time > NOW() - INTERVAL {interval}
    GROUP BY service_name
    ORDER BY total_spans DESC
    """
    stats_by_service = {}
    result = executor.execute_query(stats_query)
    if result['success']:
        for row in result['rows']:
            stats_by_service[row['service_name']] = row

    # Merge: all discovered services with their stats (or zeros if no recent activity)
    services_list = []
    for svc in all_services:
        if svc in stats_by_service:
            services_list.append(stats_by_service[svc])
        else:
            # Service exists but has no activity in selected time window
            services_list.append({
                'service_name': svc,
                'total_spans': 0,
                'errors': 0,
                'error_pct': None,
                'avg_latency_ms': None
            })

    # Sort by total_spans descending
    services_list.sort(key=lambda x: x['total_spans'], reverse=True)
    status['services'] = services_list

    # Get database status
    db_query = """
    SELECT db_system,
           COUNT(*) as span_count,
           MAX(start_time) as last_seen,
           ROUND(AVG(duration_ns / 1000000.0), 2) as avg_latency_ms
    FROM traces_otel_analytic
    WHERE db_system IS NOT NULL AND db_system != ''
      AND start_time > NOW() - INTERVAL '10' MINUTE
    GROUP BY db_system
    """
    result = executor.execute_query(db_query)
    if result['success']:
        status['databases'] = result['rows']

    # Get error summary stats
    error_summary_query = """
    SELECT
        COUNT(*) as total_errors,
        COUNT(DISTINCT service_name) as affected_services
    FROM traces_otel_analytic
    WHERE status_code = 'ERROR'
      AND start_time > NOW() - INTERVAL '5' MINUTE
    """
    result = executor.execute_query(error_summary_query)
    if result['success'] and result['rows']:
        status['error_summary'] = result['rows'][0]

    # Get recent errors with trace_id and span_id for drill-down
    error_query = """
    SELECT trace_id, span_id, service_name, span_name, status_code,
           duration_ns / 1000000.0 as duration_ms,
           start_time
    FROM traces_otel_analytic
    WHERE status_code = 'ERROR'
      AND start_time > NOW() - INTERVAL '5' MINUTE
    ORDER BY start_time DESC
    LIMIT 10
    """
    result = executor.execute_query(error_query)
    if result['success']:
        status['recent_errors'] = result['rows']

    return jsonify(status)


@app.route('/api/query', methods=['POST'])
def execute_query():
    """Execute a custom SQL query."""
    data = request.json
    sql = data.get('sql', '')

    if not sql:
        return jsonify({'error': 'No SQL provided'}), 400

    executor = get_query_executor()
    result = executor.execute_query(sql)
    return jsonify(result)


@app.route('/api/service/<service_name>', methods=['GET'])
def service_details(service_name):
    """Get detailed metrics for a specific service."""
    executor = get_query_executor()
    time_range = request.args.get('range', '1')  # hours

    data = {
        'service_name': service_name,
        'latency_history': [],
        'error_history': [],
        'throughput_history': [],
        'recent_errors': [],
        'top_operations': []
    }

    # Latency over time (1-minute buckets)
    latency_query = f"""
    SELECT
        date_trunc('minute', start_time) as time_bucket,
        ROUND(AVG(duration_ns / 1000000.0), 2) as avg_latency_ms,
        ROUND(MAX(duration_ns / 1000000.0), 2) as max_latency_ms,
        COUNT(*) as request_count
    FROM traces_otel_analytic
    WHERE service_name = '{service_name}'
      AND start_time > NOW() - INTERVAL '{time_range}' HOUR
    GROUP BY date_trunc('minute', start_time)
    ORDER BY time_bucket
    """
    result = executor.execute_query(latency_query)
    if result['success']:
        data['latency_history'] = result['rows']

    # Error rate over time
    error_query = f"""
    SELECT
        date_trunc('minute', start_time) as time_bucket,
        COUNT(*) as total,
        SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) as errors,
        ROUND(100.0 * SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) / COUNT(*), 2) as error_pct
    FROM traces_otel_analytic
    WHERE service_name = '{service_name}'
      AND start_time > NOW() - INTERVAL '{time_range}' HOUR
    GROUP BY date_trunc('minute', start_time)
    ORDER BY time_bucket
    """
    result = executor.execute_query(error_query)
    if result['success']:
        data['error_history'] = result['rows']

    # Recent errors for this service
    recent_errors_query = f"""
    SELECT span_name, status_code, start_time,
           duration_ns / 1000000.0 as duration_ms
    FROM traces_otel_analytic
    WHERE service_name = '{service_name}'
      AND status_code = 'ERROR'
      AND start_time > NOW() - INTERVAL '{time_range}' HOUR
    ORDER BY start_time DESC
    LIMIT 10
    """
    result = executor.execute_query(recent_errors_query)
    if result['success']:
        data['recent_errors'] = result['rows']

    # Top operations by volume
    top_ops_query = f"""
    SELECT span_name,
           COUNT(*) as call_count,
           ROUND(AVG(duration_ns / 1000000.0), 2) as avg_latency_ms,
           ROUND(100.0 * SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) / COUNT(*), 2) as error_pct
    FROM traces_otel_analytic
    WHERE service_name = '{service_name}'
      AND start_time > NOW() - INTERVAL '{time_range}' HOUR
    GROUP BY span_name
    ORDER BY call_count DESC
    LIMIT 10
    """
    result = executor.execute_query(top_ops_query)
    if result['success']:
        data['top_operations'] = result['rows']

    return jsonify(data)


@app.route('/api/error/<trace_id>/<span_id>', methods=['GET'])
def error_details(trace_id, span_id):
    """Get detailed information about a specific error."""
    executor = get_query_executor()

    data = {
        'error_info': None,
        'exception': None,
        'trace': []
    }

    # Get error span info
    error_query = f"""
    SELECT service_name, span_name, span_kind, status_code,
           duration_ns / 1000000.0 as duration_ms,
           start_time, db_system
    FROM traces_otel_analytic
    WHERE trace_id = '{trace_id}' AND span_id = '{span_id}'
    LIMIT 1
    """
    result = executor.execute_query(error_query)
    if result['success'] and result['rows']:
        data['error_info'] = result['rows'][0]

    # Get exception details from span_events
    exception_query = f"""
    SELECT exception_type, exception_message
    FROM span_events_otel_analytic
    WHERE trace_id = '{trace_id}' AND span_id = '{span_id}'
      AND exception_type IS NOT NULL AND exception_type != ''
    LIMIT 1
    """
    result = executor.execute_query(exception_query)
    if result['success'] and result['rows']:
        data['exception'] = result['rows'][0]

    # Get full trace for context
    trace_query = f"""
    SELECT service_name, span_name, span_kind, status_code,
           duration_ns / 1000000.0 as duration_ms,
           start_time, parent_span_id
    FROM traces_otel_analytic
    WHERE trace_id = '{trace_id}'
    ORDER BY start_time
    """
    result = executor.execute_query(trace_query)
    if result['success']:
        data['trace'] = result['rows']

    return jsonify(data)


@app.route('/api/service/<service_name>/operations', methods=['GET'])
def service_operations(service_name):
    """Get top operations for a service with configurable time window."""
    executor = get_query_executor()
    time_param = request.args.get('time', '5m')

    # Parse time parameter (e.g., "10s", "1m", "5m", "1h")
    time_value = int(time_param[:-1])
    time_unit = time_param[-1]

    if time_unit == 's':
        interval = f"'{time_value}' SECOND"
    elif time_unit == 'm':
        interval = f"'{time_value}' MINUTE"
    elif time_unit == 'h':
        interval = f"'{time_value}' HOUR"
    else:
        interval = "'5' MINUTE"  # default

    top_ops_query = f"""
    SELECT span_name,
           COUNT(*) as call_count,
           ROUND(AVG(duration_ns / 1000000.0), 2) as avg_latency_ms,
           ROUND(100.0 * SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as error_pct
    FROM traces_otel_analytic
    WHERE service_name = '{service_name}'
      AND start_time > NOW() - INTERVAL {interval}
    GROUP BY span_name
    ORDER BY call_count DESC
    LIMIT 10
    """
    result = executor.execute_query(top_ops_query)

    if result['success']:
        return jsonify({'operations': result['rows']})
    else:
        return jsonify({'operations': [], 'error': result.get('error')})


@app.route('/api/database/<db_system>', methods=['GET'])
def database_details(db_system):
    """Get detailed metrics for a specific database system."""
    executor = get_query_executor()
    time_range = request.args.get('range', '1')  # hours

    data = {
        'db_system': db_system,
        'latency_history': [],
        'error_history': [],
        'slow_queries': []
    }

    # Query latency and volume over time (1-minute buckets)
    latency_query = f"""
    SELECT
        date_trunc('minute', start_time) as time_bucket,
        ROUND(AVG(duration_ns / 1000000.0), 2) as avg_latency_ms,
        ROUND(MAX(duration_ns / 1000000.0), 2) as max_latency_ms,
        COUNT(*) as query_count
    FROM traces_otel_analytic
    WHERE db_system = '{db_system}'
      AND start_time > NOW() - INTERVAL '{time_range}' HOUR
    GROUP BY date_trunc('minute', start_time)
    ORDER BY time_bucket
    """
    result = executor.execute_query(latency_query)
    if result['success']:
        data['latency_history'] = result['rows']

    # Error rate over time
    error_query = f"""
    SELECT
        date_trunc('minute', start_time) as time_bucket,
        COUNT(*) as total,
        SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) as errors,
        ROUND(100.0 * SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as error_pct
    FROM traces_otel_analytic
    WHERE db_system = '{db_system}'
      AND start_time > NOW() - INTERVAL '{time_range}' HOUR
    GROUP BY date_trunc('minute', start_time)
    ORDER BY time_bucket
    """
    result = executor.execute_query(error_query)
    if result['success']:
        data['error_history'] = result['rows']

    # Slowest queries by service/operation
    slow_queries_query = f"""
    SELECT service_name, span_name,
           COUNT(*) as call_count,
           ROUND(AVG(duration_ns / 1000000.0), 2) as avg_latency_ms,
           ROUND(100.0 * SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as error_pct
    FROM traces_otel_analytic
    WHERE db_system = '{db_system}'
      AND start_time > NOW() - INTERVAL '{time_range}' HOUR
    GROUP BY service_name, span_name
    ORDER BY avg_latency_ms DESC
    LIMIT 10
    """
    result = executor.execute_query(slow_queries_query)
    if result['success']:
        data['slow_queries'] = result['rows']

    return jsonify(data)


# =============================================================================
# Main
# =============================================================================

if __name__ == '__main__':
    # Validate config
    errors = []
    if not ANTHROPIC_API_KEY:
        errors.append("ANTHROPIC_API_KEY is required")
    if not TRINO_HOST:
        errors.append("TRINO_HOST is required")
    if not TRINO_AVAILABLE:
        errors.append("trino package not installed")

    if errors:
        print("Configuration errors:")
        for e in errors:
            print(f"  - {e}")
        exit(1)

    print("Starting Observability Diagnostic Web UI...")
    print(f"Trino: {TRINO_HOST}:{TRINO_PORT}")
    print(f"Model: {ANTHROPIC_MODEL}")
    print("\nOpen http://localhost:5000 in your browser\n")

    app.run(host='0.0.0.0', port=5000, debug=True)
