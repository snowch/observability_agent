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


@app.route('/api/chat', methods=['POST'])
def chat():
    """Handle chat messages."""
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

    tools = [{
        "name": "execute_sql",
        "description": "Execute a SQL query against the observability database",
        "input_schema": {
            "type": "object",
            "properties": {"sql": {"type": "string", "description": "The SQL SELECT query"}},
            "required": ["sql"]
        }
    }]

    executed_queries = []

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
            'queries': executed_queries
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

    status = {
        'services': [],
        'databases': [],
        'recent_errors': [],
        'timestamp': datetime.utcnow().isoformat()
    }

    # Get service health
    service_query = """
    SELECT service_name,
           COUNT(*) as total_spans,
           SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) as errors,
           ROUND(100.0 * SUM(CASE WHEN status_code = 'ERROR' THEN 1 ELSE 0 END) / COUNT(*), 2) as error_pct,
           ROUND(AVG(duration_ns / 1000000.0), 2) as avg_latency_ms
    FROM traces_otel_analytic
    WHERE start_time > NOW() - INTERVAL '5' MINUTE
    GROUP BY service_name
    ORDER BY total_spans DESC
    """
    result = executor.execute_query(service_query)
    if result['success']:
        status['services'] = result['rows']

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

    # Get recent errors
    error_query = """
    SELECT service_name, span_name, status_code,
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
