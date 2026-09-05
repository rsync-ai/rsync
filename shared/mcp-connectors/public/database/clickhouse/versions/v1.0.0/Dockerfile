# Dockerfile for the ClickHouse MCP connector.
# ClickHouse is accessed over its HTTP interface via clickhouse-connect
# (pure-Python; no ODBC/native system libraries required).

FROM python:3.13-slim

LABEL org.opencontainers.image.title="ClickHouse MCP Connector"
LABEL org.opencontainers.image.description="ClickHouse batch source + destination (OLAP columnar warehouse, HTTP interface)."
LABEL org.opencontainers.image.version="1.0.0"
LABEL ai.rsync.connector.type="clickhouse"
LABEL ai.rsync.connector.category="relational_db"
LABEL mcp.managed="true"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Requirements first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/shared/mcp-connectors

# Copy entire (small) version directory
COPY . /app/

# Canonical->DDL authority (single source of truth), shipped from the `shared`
# build context (shared/mcp-connectors/public) exactly like warehouse_adapters.py.
# The `shared` named context is provided unconditionally by mcp_generate_compose.py.
COPY --from=shared canonical_types.py /app/canonical_types.py

# Resolve runtime imports to the shared base_connector (fix once, apply everywhere)
RUN if [ -f /app/base_connector.py ]; then \
    cp /app/base_connector.py /app/shared/mcp-connectors/; \
    rm -f /app/base_connector.py; \
    fi

ENV PYTHONPATH="/app/shared/mcp-connectors:/app:${PYTHONPATH}"

# HTTP mode for Docker
ENV MCP_HTTP_MODE=true
ENV DOCKER_CONTAINER=true
ENV MCP_PORT=8000
ENV PORT=8000

# Connector configuration (populated at runtime)
ENV CLICKHOUSE_HOST=""
ENV CLICKHOUSE_PORT="8123"
ENV CLICKHOUSE_DATABASE="default"
ENV CLICKHOUSE_USER="default"
ENV CLICKHOUSE_PASSWORD=""
ENV CLICKHOUSE_SECURE="false"

# Run as an unprivileged user (defense-in-depth: connectors process untrusted
# upstream data, so least privilege limits blast radius). uid 1000 matches the
# connector-fs-init convention; port 8000 is unprivileged so no extra caps needed.
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "connector.py"]
