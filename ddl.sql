-- vast."csnow-db|otel".logs_otel_analytic definition

CREATE TABLE vast."csnow-db|otel".logs_otel_analytic (
   timestamp timestamp(9),
   service_name varchar,
   severity_number integer,
   severity_text varchar,
   body_text varchar,
   trace_id varchar,
   span_id varchar,
   attributes_json varchar
);

-- vast."csnow-db|otel".metrics_otel_analytic definition

CREATE TABLE vast."csnow-db|otel".metrics_otel_analytic (
   timestamp timestamp(9),
   service_name varchar,
   metric_name varchar,
   metric_unit varchar,
   value_double double,
   attributes_flat varchar
);

-- vast."csnow-db|otel".span_events_otel_analytic definition

CREATE TABLE vast."csnow-db|otel".span_events_otel_analytic (
   timestamp timestamp(9),
   trace_id varchar,
   span_id varchar,
   service_name varchar,
   span_name varchar,
   event_name varchar,
   event_attributes_json varchar,
   exception_type varchar,
   exception_message varchar,
   exception_stacktrace varchar,
   gen_ai_system varchar,
   gen_ai_operation varchar,
   gen_ai_request_model varchar,
   gen_ai_usage_prompt_tokens integer,
   gen_ai_usage_completion_tokens integer
);

-- vast."csnow-db|otel".span_links_otel_analytic definition

CREATE TABLE vast."csnow-db|otel".span_links_otel_analytic (
   trace_id varchar,
   span_id varchar,
   service_name varchar,
   span_name varchar,
   linked_trace_id varchar,
   linked_span_id varchar,
   linked_trace_state varchar,
   link_attributes_json varchar
);

-- vast."csnow-db|otel".traces_otel_analytic definition

CREATE TABLE vast."csnow-db|otel".traces_otel_analytic (
   trace_id varchar,
   span_id varchar,
   parent_span_id varchar,
   start_time timestamp(9),
   duration_ns bigint,
   service_name varchar,
   span_name varchar,
   span_kind varchar,
   status_code varchar,
   http_status integer,
   db_system varchar
);

