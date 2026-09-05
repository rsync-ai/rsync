# Dockerfile for the Amazon Redshift MCP connector.
# Redshift uses the PostgreSQL wire protocol (psycopg2); no warehouse adapter.

FROM python:3.13-slim

LABEL org.opencontainers.image.title="Amazon Redshift MCP Connector"
LABEL org.opencontainers.image.description="Amazon Redshift batch source + destination (PostgreSQL-wire)."
LABEL org.opencontainers.image.version="1.0.0"
LABEL ai.rsync.connector.type="redshift"
LABEL ai.rsync.connector.category="relational_db"
LABEL mcp.managed="true"

# psycopg2-binary ships wheels, but keep build tooling available for edge cases.
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
ENV REDSHIFT_HOST=""
ENV REDSHIFT_PORT="5439"
ENV REDSHIFT_DATABASE=""
ENV REDSHIFT_USER=""
ENV REDSHIFT_PASSWORD=""
ENV REDSHIFT_SCHEMA="public"
# Empty (not "require"): a baked non-empty value would pre-fill sslmode and
# defeat the connector's verify-by-default TLS decision (_get_config). Left unset
# so remote hosts default to verify-full; a user/runtime REDSHIFT_SSLMODE still wins.
ENV REDSHIFT_SSLMODE=""
# Optimized bulk write flag (COPY-from-S3 is dormant in v1.0.0; default-off)
ENV RSYNC_REDSHIFT_COPY_S3="false"

# Run as an unprivileged user (defense-in-depth: connectors process untrusted
# upstream data, so least privilege limits blast radius). uid 1000 matches the
# connector-fs-init convention; port 8000 is unprivileged so no extra caps needed.
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "connector.py"]
