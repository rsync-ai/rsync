# Build stage
# --platform=$BUILDPLATFORM: the compiler runs natively on the build host and
# cross-compiles for $TARGETARCH, so a multi-arch publish does not run `go build`
# under QEMU. CGO is off (no C toolchain in this image), which is what makes the
# cross-compile a pure GOARCH switch.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ENV GONOSUMDB=* GONOSUMCHECK=* GOPROXY=https://proxy.golang.org,direct

WORKDIR /build

# Build context is the repo root (docker-compose.yml, docker-publish.yml) so the
# `replace` in go.mod resolves. Copy the shared module first — it changes far
# less often than the adapter, so it stays cached across adapter edits.
COPY shared/go/kafkaclient /build/shared/go/kafkaclient
COPY shared/go/pgdriver /build/shared/go/pgdriver
COPY shared/go/memlimit /build/shared/go/memlimit

COPY backend-temporal-adapter/go.mod backend-temporal-adapter/go.sum* /build/backend-temporal-adapter/
WORKDIR /build/backend-temporal-adapter
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY backend-temporal-adapter/ /build/backend-temporal-adapter/

ARG TARGETOS
ARG TARGETARCH
# -trimpath keeps build-host paths out of the binary; -s -w drops the symbol table and
# DWARF. Panics still print file:line (that is pclntab, which stays), only a debugger loses.
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath -ldflags "-s -w" -o temporal-adapter ./cmd/adapter \
    # `go version -m` rejects an incomplete binary; the hash is checked again after
    # the COPY in the runtime stage (llm-service/tests/test_go_images_verify_their_binary.py).
    && go version -m temporal-adapter >/dev/null \
    && sha256sum temporal-adapter > temporal-adapter.sha256

# Runtime stage
FROM alpine:3.24

RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Install ca-certificates for HTTPS and curl for health checks
RUN apk --no-cache add ca-certificates curl

# Copy binary from builder
COPY --from=builder --chown=appuser:appgroup /build/backend-temporal-adapter/temporal-adapter .
# The binary this stage received is the one the builder built, byte for byte.
COPY --from=builder /build/backend-temporal-adapter/temporal-adapter.sha256 /tmp/
RUN sha256sum -c /tmp/temporal-adapter.sha256 && rm /tmp/temporal-adapter.sha256

USER appuser

# Expose port (if needed for health checks)
EXPOSE 8082

# Build info for GET /version, which the admin drift check compares across
# services. docker-compose.yml passes these as build args (from the caller's env;
# scripts/deploy-service.sh exports them) and docker-publish.yml passes the
# release sha. Declared last so a new commit rebuilds only this metadata layer.
ARG GIT_COMMIT=dev
ARG BUILD_TIME=unknown
ENV GIT_COMMIT=${GIT_COMMIT} BUILD_TIME=${BUILD_TIME}

# Run the adapter
CMD ["./temporal-adapter"]

