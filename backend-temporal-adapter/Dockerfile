# Build stage
FROM golang:1.25-alpine AS builder
ENV GONOSUMDB=* GONOSUMCHECK=* GOPROXY=https://proxy.golang.org,direct

WORKDIR /build

# Build context is the repo root (docker-compose.yml, docker-publish.yml) so the
# `replace` in go.mod resolves. Copy the shared module first — it changes far
# less often than the adapter, so it stays cached across adapter edits.
COPY shared/go/kafkaclient /build/shared/go/kafkaclient
COPY shared/go/pgdriver /build/shared/go/pgdriver

COPY backend-temporal-adapter/go.mod backend-temporal-adapter/go.sum* /build/backend-temporal-adapter/
WORKDIR /build/backend-temporal-adapter
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY backend-temporal-adapter/ /build/backend-temporal-adapter/

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux go build -o temporal-adapter ./cmd/adapter

# Runtime stage
FROM alpine:3.24

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Install ca-certificates for HTTPS and curl for health checks
RUN apk --no-cache add ca-certificates curl

# Copy binary from builder
COPY --from=builder /build/backend-temporal-adapter/temporal-adapter .

RUN chown appuser:appgroup /app/temporal-adapter

USER appuser

# Expose port (if needed for health checks)
EXPOSE 8082

# Run the adapter
CMD ["./temporal-adapter"]

