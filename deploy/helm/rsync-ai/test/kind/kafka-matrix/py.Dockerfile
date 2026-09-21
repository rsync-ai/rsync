# Test-only image for the Kafka security matrix. Never published.
#
# Runs two things, both loaded from the checkout at run time (never copied in,
# so the image cannot go stale against the code under test):
#   probe.py                -- the Python runtime: kafka-python + llm-service's
#                              src/utils/kafka_security.py (stdlib only)
#   schema_history_props.py -- the Debezium connector's schema-history builder
#                              (connector.py imports httpx at module level)
# Base and pins come from the services themselves; run.py passes them in.
ARG PYTHON_BASE=python:3.11-slim
FROM ${PYTHON_BASE}
ARG KAFKA_PYTHON_SPEC
ARG HTTPX_SPEC
RUN test -n "$KAFKA_PYTHON_SPEC" && test -n "$HTTPX_SPEC" \
 && pip install --no-cache-dir "$KAFKA_PYTHON_SPEC" "$HTTPX_SPEC"
