FROM golang:1.25-alpine AS worker-builder
ENV GONOSUMDB=* GONOSUMCHECK=* GOPROXY=https://proxy.golang.org,direct

WORKDIR /src

# Build context is repo root; keep the directory layout intact so go.mod replace paths resolve.
COPY shared/go/transforms/ /src/shared/go/transforms/
COPY shared/go/kafkaclient/ /src/shared/go/kafkaclient/
COPY shared/go/pgdriver/ /src/shared/go/pgdriver/
COPY shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/ /src/shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/

WORKDIR /src/shared/mcp-connectors/internal/kafka-mcp-sink/worker-src
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o kafka-sink-worker ./cmd/kafka-sink-worker

FROM python:3.13-slim

LABEL org.opencontainers.image.title="Kafka MCP Sink"
LABEL org.opencontainers.image.description="Kafka → Destination sink connector (spawns Go worker)"
LABEL org.opencontainers.image.version="1.0.0"
LABEL ai.rsync.connector.type="kafka-mcp-sink"
LABEL ai.rsync.connector.category="streaming"
LABEL mcp.managed="true"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Root copies were removed (PR #183/#184) — single source of truth is
# versions/<current_version>/. current_version = v1.0.0 (latest.json).
COPY shared/mcp-connectors/internal/kafka-mcp-sink/versions/v1.0.0/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY shared/mcp-connectors/internal/kafka-mcp-sink/versions/v1.0.0/connector.py /app/connector.py
COPY shared/mcp-connectors/internal/kafka-mcp-sink/versions/v1.0.0/base_connector.py /app/base_connector.py
COPY shared/mcp-connectors/internal/kafka-mcp-sink/versions /app/versions

# Build-time Go worker (overrides any checked-in binary).
COPY --from=worker-builder /src/shared/mcp-connectors/internal/kafka-mcp-sink/worker-src/kafka-sink-worker /app/worker/kafka-sink-worker

ENV MCP_HTTP_MODE=true
ENV DOCKER_CONTAINER=true
ENV MCP_PORT=8000
ENV PORT=8000

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

CMD ["python", "connector.py"]

