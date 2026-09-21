# --platform=$BUILDPLATFORM: the compiler runs natively on the build host and
# cross-compiles for $TARGETARCH, so a multi-arch publish does not run `go build`
# under QEMU. CGO is off (no C toolchain in this image), which is what makes the
# cross-compile a pure GOARCH switch.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ENV GONOSUMDB=* GONOSUMCHECK=* GOPROXY=https://proxy.golang.org,direct

WORKDIR /build

# Copy shared modules first (for replace directives)
COPY shared/go/crypto /build/shared/go/crypto
COPY shared/go/correlation /build/shared/go/correlation
COPY shared/go/transforms /build/shared/go/transforms
COPY shared/go/naming /build/shared/go/naming
COPY shared/go/kafkaclient /build/shared/go/kafkaclient
COPY shared/go/pgdriver /build/shared/go/pgdriver

# Copy backend-orchestrator module
COPY backend-orchestrator/go.mod backend-orchestrator/go.sum /build/backend-orchestrator/
WORKDIR /build/backend-orchestrator
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy source code
COPY backend-orchestrator/ /build/backend-orchestrator/

ARG TARGETOS
ARG TARGETARCH
# Build — mount Go build cache so incremental rebuilds reuse prior compiler output.
# On the prod VM this cuts the go build step from ~180s to ~20s after the first build.
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -o bin/orchestrator ./cmd/orchestrator

FROM alpine:3.24

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app
# Install python3 for MCP servers that use python; curl for health checks
RUN apk add --no-cache python3 py3-pip ca-certificates curl

COPY --from=builder /build/backend-orchestrator/bin/orchestrator .

RUN chown appuser:appgroup /app/orchestrator

USER appuser

EXPOSE 8080

# Build info for GET /version, which the admin drift check compares across
# services. docker-compose.yml passes these as build args (from the caller's env;
# scripts/deploy-service.sh exports them) and docker-publish.yml passes the
# release sha. Declared last so a new commit rebuilds only this metadata layer.
ARG GIT_COMMIT=dev
ARG BUILD_TIME=unknown
ENV GIT_COMMIT=${GIT_COMMIT} BUILD_TIME=${BUILD_TIME}

CMD ["./orchestrator"]
