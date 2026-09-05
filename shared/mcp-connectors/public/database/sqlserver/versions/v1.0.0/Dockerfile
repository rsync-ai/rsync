# Auto-generated Dockerfile for SQL Server connector
# Generated: 2026-07-04T00:00:00.000000
#
# Production-ready MCP Connector Image
# Uses standard Python base image for portability

FROM python:3.13-slim

# Labels
LABEL org.opencontainers.image.title="SQL Server MCP Connector"
LABEL org.opencontainers.image.description="Connector for Microsoft SQL Server / Azure SQL Database."
LABEL org.opencontainers.image.version="1.0.0"
LABEL ai.rsync.connector.type="sqlserver"
LABEL ai.rsync.connector.category="relational_db"
LABEL mcp.managed="true"

# Install system dependencies + Microsoft ODBC Driver 18 for SQL Server.
# pyodbc needs an ODBC driver + unixodbc at runtime; Driver 18 defaults to
# Encrypt=yes which Azure SQL Database requires. ACCEPT_EULA is mandatory.
# msodbcsql18 supports amd64 + arm64. The prod.list must track the BASE's Debian
# release, which is NOT what the python tag says: python:3.13-slim -- and
# python:3.11-slim before it -- is Debian 13 (trixie). This read `debian/12` under
# a comment asserting the base was bookworm; that had been untrue for some time,
# so bookworm packages were being installed onto trixie. `debian/13` carries
# msodbcsql18 for both amd64 and arm64 (18.6.x, vs bookworm's 18.3.x).
# Re-check this line whenever the python base moves.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl gnupg2 apt-transport-https ca-certificates \
    # Microsoft's prod.list pins signed-by=/usr/share/keyrings/microsoft-prod.gpg,
    # so the dearmored key MUST live at exactly that path or apt rejects the repo
    # as unsigned (the sqv verifier reads that keyring by path).
    #
    # BOTH keys, because Microsoft signs the two feeds differently and the
    # prod.list files do not say so -- they name the identical keyring path.
    # bookworm is signed by microsoft.asc (EB3E94ADBE1229CF); trixie is signed by
    # microsoft-2025.asc (EE4D7792F748182B), and shipping only the classic key
    # fails `apt-get update` with "Missing key EE4D7792F748182B ... is not
    # signed". Dearmoring both into one keyring covers either side of the
    # rotation, so this does not break again when MS moves the next release.
    && { curl -fsSL https://packages.microsoft.com/keys/microsoft.asc; \
         curl -fsSL https://packages.microsoft.com/keys/microsoft-2025.asc; } \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/13/prod.list \
        -o /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
        msodbcsql18 unixodbc-dev \
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
# The sink's ensure_table imports it as `canonical_types` from /app (on PYTHONPATH).
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
ENV SQLSERVER_HOST=""
ENV SQLSERVER_PORT=""
ENV SQLSERVER_DATABASE=""
ENV SQLSERVER_USER=""
ENV SQLSERVER_PASSWORD=""

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
