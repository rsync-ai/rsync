# Debezium MCP Connector (Kafka Connect manager)
FROM python:3.13-slim

LABEL org.opencontainers.image.title="Debezium MCP Connector"
LABEL org.opencontainers.image.description="Internal CDC engine that provisions Debezium connectors in Kafka Connect."
LABEL org.opencontainers.image.version="1.0.0"
LABEL ai.rsync.connector.type="debezium"
LABEL ai.rsync.connector.category="streaming"
LABEL mcp.managed="true"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/

ENV MCP_HTTP_MODE=true
ENV DOCKER_CONTAINER=true
ENV MCP_PORT=8000
ENV PORT=8000
# Deterministic str hashing across restarts. connector.py._stable_server_id uses
# hashlib (seed-independent), but pin the seed too so any residual hash()-based
# path can't re-roll the MySQL database.server.id and collide with a live stream.
ENV PYTHONHASHSEED=0

# Default in-cluster URLs (override via env if needed)
ENV KAFKA_CONNECT_URL="http://kafka-connect:8083"
ENV KAFKA_BOOTSTRAP_SERVERS="kafka:29092"

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "connector.py"]


