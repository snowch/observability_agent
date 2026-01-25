#!/usr/bin/env python3
"""
Diagnostic Chat Tool for Support Engineers

An interactive chat interface that uses an LLM to help diagnose issues
by querying observability data (logs, metrics, traces) stored in VastDB via Trino.

Usage:
    export ANTHROPIC_API_KEY=your_api_key
    export TRINO_HOST=trino.example.com
    export TRINO_PORT=443
    export TRINO_USER=your_user
    export TRINO_CATALOG=vast
    export TRINO_SCHEMA=otel

    python diagnostic_chat.py

Example queries:
    - "ad service ui is slow"
    - "what errors occurred in the last hour?"
    - "show me failed requests for the checkout service"
    - "trace the request with id abc123"
"""

import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

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
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-20250514")

# Trino configuration
TRINO_HOST = os.getenv("TRINO_HOST")
TRINO_PORT = int(os.getenv("TRINO_PORT", "443"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "vast")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "otel")
TRINO_HTTP_SCHEME = os.getenv("TRINO_HTTP_SCHEME", "https")

# Maximum rows to return from queries to avoid overwhelming context
MAX_QUERY_ROWS = 100

# =============================================================================
# Database Schema Information
# =============================================================================

SCHEMA_INFO = """
## Available Tables in VastDB

### 1. logs_otel_analytic
Log records from all services.
Columns:
- timestamp (timestamp) - When the log was emitted
- service_name (varchar) - Name of the service (e.g., 'adservice', 'frontend', 'checkoutservice')
- severity_number (integer) - Numeric severity (1-24, where higher = more severe)
- severity_text (varchar) - Severity level ('DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL')
- body_text (varchar) - The log message content
- trace_id (varchar) - Associated trace ID for correlation
- span_id (varchar) - Associated span ID
- attributes_json (varchar) - JSON string of additional attributes

### 2. metrics_otel_analytic
Time-series metrics from all services.
Columns:
- timestamp (timestamp) - When the metric was recorded
- service_name (varchar) - Name of the service
- metric_name (varchar) - Name of the metric (e.g., 'http.server.duration', 'runtime.cpython.cpu_time')
- metric_unit (varchar) - Unit of measurement ('ms', 's', 'By', '1')
- value_double (double) - The metric value
- attributes_flat (varchar) - Comma-separated key=value pairs of attributes

### 3. traces_otel_analytic
Distributed trace spans showing request flow across services.
Columns:
- trace_id (varchar) - Unique trace identifier (groups related spans)
- span_id (varchar) - Unique span identifier
- parent_span_id (varchar) - Parent span ID (empty for root spans)
- start_time (timestamp) - When the span started
- duration_ns (bigint) - Duration in nanoseconds
- service_name (varchar) - Service that created this span
- span_name (varchar) - Operation name (e.g., 'GET /api/products', 'SELECT')
- span_kind (varchar) - Type: 'SERVER', 'CLIENT', 'INTERNAL', 'PRODUCER', 'CONSUMER'
- status_code (varchar) - 'OK', 'ERROR', or 'UNSET'
- http_status (integer) - HTTP response status code (if applicable)
- db_system (varchar) - Database system if this is a DB span (e.g., 'redis', 'postgresql')

### 4. span_events_otel_analytic
Events attached to spans, including exceptions.
Columns:
- timestamp (timestamp) - When the event occurred
- trace_id (varchar) - Associated trace ID
- span_id (varchar) - Associated span ID
- service_name (varchar) - Service name
- span_name (varchar) - Parent span's operation name
- event_name (varchar) - Event name (e.g., 'exception', 'message')
- event_attributes_json (varchar) - JSON attributes
- exception_type (varchar) - Exception class name (if exception event)
- exception_message (varchar) - Exception message
- exception_stacktrace (varchar) - Full stack trace
- gen_ai_system (varchar) - GenAI system if applicable
- gen_ai_operation (varchar) - GenAI operation name
- gen_ai_request_model (varchar) - Model used
- gen_ai_usage_prompt_tokens (integer) - Prompt tokens used
- gen_ai_usage_completion_tokens (integer) - Completion tokens used

### 5. span_links_otel_analytic
Links between spans (e.g., async message producers/consumers).
Columns:
- trace_id (varchar) - Source trace ID
- span_id (varchar) - Source span ID
- service_name (varchar) - Service name
- span_name (varchar) - Span operation name
- linked_trace_id (varchar) - Linked trace ID
- linked_span_id (varchar) - Linked span ID
- linked_trace_state (varchar) - W3C trace state
- link_attributes_json (varchar) - JSON attributes

## Common Service Names (OpenTelemetry Demo)
- frontend - Web frontend
- adservice - Advertisement service
- cartservice - Shopping cart
- checkoutservice - Checkout processing
- currencyservice - Currency conversion
- emailservice - Email notifications
- paymentservice - Payment processing
- productcatalogservice - Product catalog
- recommendationservice - Product recommendations
- shippingservice - Shipping calculations
- quoteservice - Quote generation

## Query Tips
- Use duration_ns / 1000000.0 to convert to milliseconds
- Filter by time: timestamp > NOW() - INTERVAL '1' HOUR
- For slow requests: ORDER BY duration_ns DESC
- For errors: WHERE status_code = 'ERROR' OR severity_text = 'ERROR'
- Join traces with logs using trace_id for full context
"""

SYSTEM_PROMPT = f"""You are an expert Site Reliability Engineer (SRE) assistant helping support engineers diagnose issues in a distributed system. You have access to observability data (logs, metrics, and traces) stored in VastDB.

{SCHEMA_INFO}

## Your Approach

When a user reports an issue (e.g., "ad service is slow"), follow this diagnostic methodology:

1. **Understand the Problem**: Clarify the issue if needed. What service? What symptoms? When did it start?

2. **Start Broad, Then Narrow**:
   - First, check for obvious errors or anomalies
   - Look at recent traces to understand the request flow
   - Examine metrics for performance patterns
   - Dive into logs for detailed error messages

3. **Correlate Signals**:
   - Use trace_id to correlate logs, spans, and events
   - Compare durations across services to find bottlenecks
   - Look for patterns in error messages

4. **Provide Actionable Insights**:
   - Summarize findings clearly
   - Identify the root cause when possible
   - Suggest next steps or remediation

## Query Guidelines

- Always limit queries (use LIMIT) to avoid overwhelming results
- Use appropriate time filters to focus on relevant data
- When looking for slow operations, sort by duration descending
- When investigating errors, filter by status_code = 'ERROR' or severity_text = 'ERROR'
- For the current time, use NOW() or CURRENT_TIMESTAMP

## Important Notes

- Be conversational but focused on diagnosis
- Show your reasoning as you investigate
- If you need more information, ask clarifying questions
- Always explain what you're looking for with each query
- Summarize findings after each query result

You have access to a tool called `execute_sql` that runs SQL queries against the VastDB database via Trino. Use it to investigate issues.
"""


# =============================================================================
# Trino Query Executor
# =============================================================================

class TrinoQueryExecutor:
    """Executes SQL queries against VastDB via Trino."""

    def __init__(self):
        if not TRINO_AVAILABLE:
            raise ImportError("trino package not installed. Run: pip install trino")

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
        )

    def get_backend_name(self) -> str:
        return f"Trino ({TRINO_HOST}:{TRINO_PORT})"

    def execute_query(self, sql: str) -> Dict[str, Any]:
        """Execute a SQL query via Trino."""
        sql = sql.strip()

        if not sql.lower().startswith("select"):
            return {
                "success": False,
                "error": "Only SELECT queries are supported",
                "rows": [],
                "columns": []
            }

        # Enforce limit
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

            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            # Fetch results
            raw_rows = cursor.fetchall()

            # Convert to list of dicts
            rows = []
            for raw_row in raw_rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    val = raw_row[i]
                    # Convert timestamps to strings
                    if hasattr(val, 'isoformat'):
                        val = val.isoformat()
                    row_dict[col] = val
                rows.append(row_dict)

            return {
                "success": True,
                "message": "Query executed successfully",
                "rows": rows,
                "columns": columns,
                "row_count": len(rows)
            }

        except Exception as e:
            return {
                "success": False,
                "error": f"{type(e).__name__}: {str(e)}",
                "rows": [],
                "columns": []
            }


# =============================================================================
# Claude Chat Interface
# =============================================================================

class DiagnosticChat:
    """Interactive chat interface using Claude for diagnosis."""

    def __init__(self):
        self.client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        self.query_executor = TrinoQueryExecutor()
        self.conversation_history: List[Dict] = []

        # Define the SQL execution tool
        self.tools = [
            {
                "name": "execute_sql",
                "description": """Execute a SQL query against the VastDB observability database via Trino.

Use this tool to query logs, metrics, traces, span events, and span links.

Available tables:
- logs_otel_analytic: Log records with timestamp, service_name, severity_text, body_text, trace_id
- metrics_otel_analytic: Metrics with timestamp, service_name, metric_name, value_double
- traces_otel_analytic: Trace spans with trace_id, span_id, service_name, span_name, duration_ns, status_code
- span_events_otel_analytic: Span events including exceptions with exception_type, exception_message
- span_links_otel_analytic: Links between spans

Always include a LIMIT clause to avoid returning too many results.
Results are limited to 100 rows maximum.""",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "sql": {
                            "type": "string",
                            "description": "The SQL SELECT query to execute"
                        }
                    },
                    "required": ["sql"]
                }
            }
        ]

    def chat(self, user_message: str) -> str:
        """Send a message and get a response, potentially with tool use."""

        # Add user message to history
        self.conversation_history.append({
            "role": "user",
            "content": user_message
        })

        # Keep conversation history manageable
        if len(self.conversation_history) > 20:
            self.conversation_history = self.conversation_history[-20:]

        # Initial API call
        response = self._call_api()

        # Handle tool use loop
        while response.stop_reason == "tool_use":
            # Process tool calls
            tool_results = self._process_tool_calls(response)

            # Add assistant response and tool results to history
            self.conversation_history.append({
                "role": "assistant",
                "content": response.content
            })
            self.conversation_history.append({
                "role": "user",
                "content": tool_results
            })

            # Continue the conversation
            response = self._call_api()

        # Extract final text response
        final_response = self._extract_text(response)

        # Add to history
        self.conversation_history.append({
            "role": "assistant",
            "content": final_response
        })

        return final_response

    def _call_api(self):
        """Make an API call to Claude."""
        return self.client.messages.create(
            model=ANTHROPIC_MODEL,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=self.tools,
            messages=self.conversation_history
        )

    def _process_tool_calls(self, response) -> List[Dict]:
        """Process tool calls from the response."""
        tool_results = []

        for content_block in response.content:
            if content_block.type == "tool_use":
                tool_name = content_block.name
                tool_input = content_block.input
                tool_use_id = content_block.id

                if tool_name == "execute_sql":
                    sql = tool_input.get("sql", "")
                    print(f"\n[Executing SQL]\n{sql}\n")

                    result = self.query_executor.execute_query(sql)

                    # Format result for display
                    if result["success"]:
                        print(f"[Query returned {result['row_count']} rows]")
                        if result["rows"]:
                            # Show preview of first few rows
                            preview_count = min(3, len(result["rows"]))
                            for i, row in enumerate(result["rows"][:preview_count]):
                                print(f"  Row {i+1}: {self._format_row_preview(row)}")
                            if len(result["rows"]) > preview_count:
                                print(f"  ... and {len(result['rows']) - preview_count} more rows")
                    else:
                        print(f"[Query Error: {result['error']}]")

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": json.dumps(result, default=str)
                    })
                else:
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": tool_use_id,
                        "content": json.dumps({"error": f"Unknown tool: {tool_name}"})
                    })

        return tool_results

    def _format_row_preview(self, row: Dict) -> str:
        """Format a row for preview display."""
        parts = []
        for k, v in list(row.items())[:4]:  # Show first 4 columns
            v_str = str(v)[:50]  # Truncate long values
            parts.append(f"{k}={v_str}")
        return ", ".join(parts)

    def _extract_text(self, response) -> str:
        """Extract text content from response."""
        text_parts = []
        for content_block in response.content:
            if hasattr(content_block, 'text'):
                text_parts.append(content_block.text)
        return "\n".join(text_parts)

    def clear_history(self):
        """Clear conversation history."""
        self.conversation_history = []
        print("Conversation history cleared.")


# =============================================================================
# Main CLI Interface
# =============================================================================

def print_banner():
    """Print welcome banner."""
    print("=" * 70)
    print("  Observability Diagnostic Chat")
    print("  Powered by Claude + Trino + VastDB")
    print("=" * 70)
    print()
    print("Describe your issue and I'll help diagnose it by querying")
    print("logs, metrics, and traces from your observability data.")
    print()
    print("Example queries:")
    print("  - 'ad service is slow'")
    print("  - 'what errors occurred in the last hour?'")
    print("  - 'show me failed checkouts'")
    print("  - 'trace request abc123'")
    print()
    print("Commands:")
    print("  /clear  - Clear conversation history")
    print("  /help   - Show this help message")
    print("  /quit   - Exit the chat")
    print()
    print("-" * 70)


def validate_config():
    """Validate required configuration."""
    errors = []

    if not ANTHROPIC_API_KEY:
        errors.append("ANTHROPIC_API_KEY is required")

    if not TRINO_HOST:
        errors.append("TRINO_HOST is required")

    if not TRINO_AVAILABLE:
        errors.append("trino package not installed. Run: pip install trino")

    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        print()
        return False

    return True


def main():
    """Main entry point."""
    print_banner()

    if not validate_config():
        return 1

    try:
        print("Initializing...")
        chat = DiagnosticChat()
        print(f"Connected to: {chat.query_executor.get_backend_name()}")
        print(f"Using model: {ANTHROPIC_MODEL}")
        print()
    except Exception as e:
        print(f"Error initializing: {type(e).__name__}: {e}")
        return 1

    print("Ready! Type your question or describe the issue.\n")

    while True:
        try:
            # Get user input
            user_input = input("You: ").strip()

            if not user_input:
                continue

            # Handle commands
            if user_input.lower() == "/quit":
                print("Goodbye!")
                break
            elif user_input.lower() == "/clear":
                chat.clear_history()
                continue
            elif user_input.lower() == "/help":
                print_banner()
                continue

            # Get response from Claude
            print()
            response = chat.chat(user_input)
            print(f"\nAssistant: {response}\n")
            print("-" * 70)
            print()

        except KeyboardInterrupt:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\nError: {type(e).__name__}: {e}\n")
            continue

    return 0


if __name__ == "__main__":
    sys.exit(main())
