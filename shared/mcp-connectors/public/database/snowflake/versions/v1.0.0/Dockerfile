# Dockerfile for the Snowflake MCP connector.
# Snowflake uses a PEP-249 DB-API driver (snowflake-connector-python); no adapter.

FROM python:3.13-slim

LABEL org.opencontainers.image.title="Snowflake MCP Connector"
LABEL org.opencontainers.image.description="Snowflake batch source + destination (cloud data warehouse)."
LABEL org.opencontainers.image.version="1.0.0"
LABEL ai.rsync.connector.type="snowflake"
LABEL ai.rsync.connector.category="data_warehouse"
LABEL mcp.managed="true"

# snowflake-connector-python ships wheels, but keep build tooling for edge cases.
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
ENV SNOWFLAKE_ACCOUNT=""
ENV SNOWFLAKE_USER=""
ENV SNOWFLAKE_PASSWORD=""
ENV SNOWFLAKE_WAREHOUSE=""
ENV SNOWFLAKE_DATABASE=""
ENV SNOWFLAKE_SCHEMA="PUBLIC"
ENV SNOWFLAKE_ROLE=""
ENV SNOWFLAKE_PRIVATE_KEY_FILE=""
# Optimized bulk write flag (COPY-INTO-from-stage is dormant in v1.0.0; default-off)
ENV RSYNC_SNOWFLAKE_COPY_STAGE="false"

# Run as an unprivileged user (defense-in-depth: connectors process untrusted
# upstream data, so least privilege limits blast radius). uid 1000 matches the
# connector-fs-init convention; port 8000 is unprivileged so no extra caps needed.
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "connector.py"]
