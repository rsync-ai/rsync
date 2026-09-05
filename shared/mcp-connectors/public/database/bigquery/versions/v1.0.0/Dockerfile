# Dockerfile for the BigQuery MCP connector (hand-curated).
# Production-ready MCP Connector Image; standard Python base for portability.

FROM python:3.13-slim

# Labels
LABEL org.opencontainers.image.title="BigQuery MCP Connector"
LABEL org.opencontainers.image.description="Connector for Google BigQuery (optimized batch source + destination)."
LABEL org.opencontainers.image.version="1.0.0"
LABEL ai.rsync.connector.type="bigquery"
LABEL ai.rsync.connector.category="data_warehouse"
LABEL mcp.managed="true"

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
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
COPY . /app/

# Pull shared warehouse_adapters.py from the single canonical source (public/ root)
# via the `shared` BuildKit named context — see docker-compose.mcp.yml
# build.additional_contexts. connector.py imports it (guarded) to delegate the
# BigQuery warehouse operations; no physical copy lives in this version dir.
COPY --from=shared warehouse_adapters.py /app/warehouse_adapters.py

# Copy base_connector to shared location if present, then remove the per-connector
# copy so imports always resolve to the shared library (fix-once semantics).
RUN if [ -f /app/base_connector.py ]; then \
    cp /app/base_connector.py /app/shared/mcp-connectors/; \
    rm -f /app/base_connector.py; \
    fi

# Add shared connectors to Python path
ENV PYTHONPATH="/app/shared/mcp-connectors:/app:${PYTHONPATH}"

# Enable HTTP mode for Docker
ENV MCP_HTTP_MODE=true
ENV DOCKER_CONTAINER=true
ENV MCP_PORT=8000
ENV PORT=8000

# Environment variables for connector configuration
ENV BIGQUERY_PROJECT_ID=""
ENV BIGQUERY_DATASET_ID=""
ENV BIGQUERY_SERVICE_ACCOUNT_JSON=""
ENV BIGQUERY_LOCATION=""
# Default-off optimized bulk write path (NDJSON load jobs); streaming when unset.
ENV RSYNC_BQ_LOAD_JOB=""

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
