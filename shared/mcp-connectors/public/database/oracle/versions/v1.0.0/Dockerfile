# Dockerfile for the Oracle MCP connector
#
# Production-ready MCP Connector Image.
# python-oracledb runs in THIN mode (pure Python) — no Oracle Instant Client
# and no system packages are required, so this stays on the slim base with only
# pip deps (unlike the SQL Server connector, which needs the msodbcsql ODBC
# driver). Thick mode / Instant Client is intentionally NOT installed.

FROM python:3.13-slim

# Labels
LABEL org.opencontainers.image.title="Oracle MCP Connector"
LABEL org.opencontainers.image.description="Connector for Oracle Database (batch source + destination + Debezium LogMiner CDC)."
LABEL org.opencontainers.image.version="1.0.0"
LABEL ai.rsync.connector.type="oracle"
LABEL ai.rsync.connector.category="relational_db"
LABEL mcp.managed="true"

# curl is needed for the container HEALTHCHECK; no DB client libraries required
# (oracledb thin mode is pure Python).
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create shared directory structure
RUN mkdir -p /app/shared/mcp-connectors

# Copy entire build context (safe: per-connector version directory is small).
# This makes optional directories (like oauth/) work without Dockerfile conditionals.
COPY . /app/

# Canonical type authority (single source of truth), shipped from the `shared`
# build context (shared/mcp-connectors/public) exactly like warehouse_adapters.py.
# Discovery imports it as `canonical_types` from /app (on PYTHONPATH).
COPY --from=shared canonical_types.py /app/canonical_types.py

# Copy base_connector to shared location if present
RUN if [ -f /app/base_connector.py ]; then \
    cp /app/base_connector.py /app/shared/mcp-connectors/; \
    # Ensure runtime imports always resolve to the shared base_connector, not a per-connector copy. \
    rm -f /app/base_connector.py; \
    fi && \
    if [ -d /app/oauth ]; then \
      mkdir -p /app/shared/mcp-connectors/oauth && cp -r /app/oauth/* /app/shared/mcp-connectors/oauth/; \
    fi

# Add shared connectors to Python path
ENV PYTHONPATH="/app/shared/mcp-connectors:/app:${PYTHONPATH}"

# Enable HTTP mode for Docker
ENV MCP_HTTP_MODE=true
ENV DOCKER_CONTAINER=true
ENV MCP_PORT=8000
ENV PORT=8000

# Environment variables for connector configuration
ENV ORACLE_HOST=""
ENV ORACLE_PORT=""
ENV ORACLE_SERVICE_NAME=""
ENV ORACLE_USER=""
ENV ORACLE_PASSWORD=""

# Run as an unprivileged user (defense-in-depth: connectors process untrusted
# upstream data, so least privilege limits blast radius). uid 1000 matches the
# connector-fs-init convention; port 8000 is unprivileged so no extra caps needed.
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Default port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Run connector
CMD ["python", "connector.py"]
