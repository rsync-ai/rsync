# Dockerfile for shopify-admin-graphql MCP connector.
#
# Modeled on the auto-generated Dockerfile pattern used by the versioned
# database connectors (e.g. public/postgresql/versions/v1.0.0/Dockerfile).
# shopify-admin-graphql uses a flat layout — connector.py and metadata.json
# live directly in this directory rather than under versions/<v>/. The
# build context is this directory; base_connector.py is snapshotted next
# to connector.py so it can be copied into /app/shared/mcp-connectors at
# image build time, matching the runtime layout the other connectors expect.

FROM python:3.13-slim

LABEL org.opencontainers.image.title="Shopify Admin GraphQL MCP Connector"
LABEL org.opencontainers.image.description="GraphQL connector for Shopify Admin API"
LABEL org.opencontainers.image.version="1.0.1"
LABEL ai.rsync.connector.type="shopify-admin-graphql"
LABEL ai.rsync.connector.category="api_saas"
LABEL mcp.managed="true"

# curl is needed for the healthcheck.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/shared/mcp-connectors

# Copy the whole build context (connector.py + metadata.json + base_connector.py + etc).
COPY . /app/

# Move the snapshotted base_connector.py to the shared location and drop the
# per-connector copy so runtime imports resolve to the shared library only.
RUN if [ -f /app/base_connector.py ]; then \
      cp /app/base_connector.py /app/shared/mcp-connectors/; \
      rm -f /app/base_connector.py; \
    fi

ENV PYTHONPATH="/app/shared/mcp-connectors:/app:${PYTHONPATH}"
ENV MCP_HTTP_MODE=true
ENV DOCKER_CONTAINER=true
ENV MCP_PORT=8000
ENV PORT=8000

# Run as an unprivileged user (defense-in-depth: connectors process untrusted
# upstream data, so least privilege limits blast radius). uid 1000 matches the
# connector-fs-init convention; port 8000 is unprivileged so no extra caps needed.
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "connector.py"]
