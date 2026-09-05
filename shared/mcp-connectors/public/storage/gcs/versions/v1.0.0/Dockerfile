# Dockerfile for GCS (Google Cloud Storage) connector (hand-built)
#
# Production-ready MCP Connector Image
FROM python:3.13-slim

LABEL org.opencontainers.image.title="GCS MCP Connector"
LABEL org.opencontainers.image.description="Connector for Google Cloud Storage — first-class source (batch + last-modified incremental) and destination."
LABEL org.opencontainers.image.version="1.0.0"
LABEL ai.rsync.connector.type="gcs"
LABEL ai.rsync.connector.category="cloud_storage"
LABEL mcp.managed="true"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/shared/mcp-connectors

COPY . /app/

# Shared rsync_protocol/ (single canonical source) via the `shared` BuildKit
# named context — see docker-compose.mcp.yml build.additional_contexts. The
# shared source pipeline (ObjectStorageSourceMixin) lives here.
COPY --from=shared rsync_protocol /app/rsync_protocol

# Move base_connector to the shared location so runtime imports resolve to it
RUN if [ -f /app/base_connector.py ]; then \
    cp /app/base_connector.py /app/shared/mcp-connectors/; \
    rm -f /app/base_connector.py; \
    fi && \
    if [ -d /app/oauth ]; then \
      mkdir -p /app/shared/mcp-connectors/oauth && cp -r /app/oauth/* /app/shared/mcp-connectors/oauth/; \
    fi

ENV PYTHONPATH="/app/shared/mcp-connectors:/app:${PYTHONPATH}"
ENV MCP_HTTP_MODE=true
ENV DOCKER_CONTAINER=true
ENV MCP_PORT=8000
ENV PORT=8000

# GCS connector configuration (never reads MINIO_* — INV-1)
ENV GCS_SERVICE_ACCOUNT_JSON=""
ENV GCS_PROJECT_ID=""

# Run as an unprivileged user (defense-in-depth: connectors process untrusted
# upstream data, so least privilege limits blast radius). uid 1000 matches the
# connector-fs-init convention; port 8000 is unprivileged so no extra caps needed.
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "connector.py"]
