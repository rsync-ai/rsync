# Auto-generated Dockerfile for MySQL connector
# Generated: 2026-01-04T08:50:06.069464
#
# Production-ready MCP Connector Image
# Uses standard Python base image for portability

FROM python:3.13-slim

# Labels
LABEL org.opencontainers.image.title="MySQL MCP Connector"
LABEL org.opencontainers.image.description="Connector for MySQL relational database."
LABEL org.opencontainers.image.version="1.0.0"
LABEL ai.rsync.connector.type="mysql"
LABEL ai.rsync.connector.category="relational_db"
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
# This makes optional directories (like oauth/) work without Dockerfile conditionals.
COPY . /app/

# Canonical->DDL authority (single source of truth) from the `shared` BuildKit
# named context (public/ root) — see docker-compose.mcp.yml build.additional_contexts.
# The sink's _ddl_for delegates its canonical branch to it; no physical copy
# lives in this version dir.
COPY --from=shared canonical_types.py /app/canonical_types.py

# Copy base_connector to shared location if present
RUN if [ -f /app/base_connector.py ]; then \
    cp /app/base_connector.py /app/shared/mcp-connectors/; \
    # Ensure runtime imports always resolve to the shared base_connector, not a per-connector copy. \
    # This lets us fix auth/normalization/pagination once in the shared library and have it apply \
    # across all connectors without regenerating each connector's Python code. \
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
ENV MYSQL_HOST=""
ENV MYSQL_PORT=""
ENV MYSQL_DATABASE=""
ENV MYSQL_USER=""
ENV MYSQL_PASSWORD=""

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
