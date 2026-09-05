FROM golang:1.25-alpine AS builder
# Fix go mod download failures on new VMs (rate limits / sum DB connectivity)
ENV GONOSUMDB=* GONOSUMCHECK=* GOPROXY=https://proxy.golang.org,direct

WORKDIR /build

# Copy shared modules first (for replace directive)
COPY shared/go/crypto /build/shared/go/crypto
COPY shared/go/transforms /build/shared/go/transforms
COPY shared/go/naming /build/shared/go/naming
COPY shared/go/kafkaclient /build/shared/go/kafkaclient
COPY shared/go/pgdriver /build/shared/go/pgdriver

# Copy backend-orchestrator so its `pkg/` paths (diagnose) are reachable
# via the replace directive in api-gateway/go.mod. The chat assistant
# reuses Phase 3's diagnoser this way — single source of truth.
COPY backend-orchestrator /build/backend-orchestrator

# Copy api-gateway module
COPY api-gateway/go.mod api-gateway/go.sum /build/api-gateway/
WORKDIR /build/api-gateway
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy source code
COPY api-gateway/ /build/api-gateway/

# Build — mount Go build cache so incremental rebuilds reuse prior compiler output.
# On the prod VM this cuts the go build step from ~180s to ~20s after the first build.
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    # ./cmd/server, NOT cmd/server/main.go. Naming the file compiles that ONE file as
    # an ad-hoc `command-line-arguments` package, so every sibling in package main is
    # invisible to it. #820 added cmd/server/ready.go and this image stopped building
    # entirely -- `undefined: readinessVerdict` -- while CI stayed green, because every
    # Go job runs `go build ./...` (the package form) and only the PR smoke gate builds
    # this image. Prod could not be redeployed for two days as a result, which also
    # stranded two pending migrations. Every other Dockerfile here already uses the
    # package form; this was the last single-file build.
    go build -o bin/server ./cmd/server

FROM alpine:3.24

RUN addgroup -S appgroup && adduser -S appuser -G appgroup \
    && apk --no-cache add ca-certificates curl

WORKDIR /app

COPY --from=builder /build/api-gateway/bin/server .
COPY --from=builder /build/api-gateway/migrations ./migrations

RUN chown -R appuser:appgroup /app

USER appuser

EXPOSE 8080

CMD ["./server"]
