# --platform=$BUILDPLATFORM: the compiler runs natively on the build host and
# cross-compiles for $TARGETARCH, so a multi-arch publish does not run `go build`
# under QEMU. CGO is off (no C toolchain in this image), which is what makes the
# cross-compile a pure GOARCH switch.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
# Fix go mod download failures on new VMs (rate limits / sum DB connectivity)
ENV GONOSUMDB=* GONOSUMCHECK=* GOPROXY=https://proxy.golang.org,direct

WORKDIR /build

# Copy shared modules first (for replace directive)
COPY shared/go/crypto /build/shared/go/crypto
COPY shared/go/transforms /build/shared/go/transforms
COPY shared/go/naming /build/shared/go/naming
COPY shared/go/kafkaclient /build/shared/go/kafkaclient
COPY shared/go/pgdriver /build/shared/go/pgdriver
COPY shared/go/memlimit /build/shared/go/memlimit

# The replace directive in api-gateway/go.mod points at ../backend-orchestrator,
# so `go mod download` needs that module's go.mod/go.sum to resolve the build
# list -- but NOT a line of its source. Only the two manifests come in here.
# This file used to COPY the whole orchestrator tree at this point, which keyed
# the download layer below to every .go file in another service: any orchestrator
# edit re-ran `go mod download` even when no dependency had changed.
COPY backend-orchestrator/go.mod backend-orchestrator/go.sum /build/backend-orchestrator/

# Copy api-gateway module
COPY api-gateway/go.mod api-gateway/go.sum /build/api-gateway/
WORKDIR /build/api-gateway
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# NOW the orchestrator source, which the build genuinely does compile: the chat
# assistant reuses Phase 3's diagnoser through its `pkg/` paths -- single source
# of truth. Below the download layer, so it can only invalidate the build itself.
COPY backend-orchestrator /build/backend-orchestrator

# Copy source code
COPY api-gateway/ /build/api-gateway/

ARG TARGETOS
ARG TARGETARCH
# Build — mount Go build cache so incremental rebuilds reuse prior compiler output.
# On the prod VM this cuts the go build step from ~180s to ~20s after the first build.
# -trimpath keeps build-host paths out of the binary; -s -w drops the symbol table and
# DWARF. Panics still print file:line (that is pclntab, which stays), only a debugger loses.
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
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -trimpath -ldflags "-s -w" -o bin/server ./cmd/server \
    # `go version -m` rejects an incomplete binary; the hash is checked again after
    # the COPY in the runtime stage (llm-service/tests/test_go_images_verify_their_binary.py).
    && go version -m bin/server >/dev/null \
    && cd bin && sha256sum server > server.sha256

FROM alpine:3.24

RUN addgroup -S appgroup && adduser -S appuser -G appgroup \
    && apk --no-cache add ca-certificates curl

WORKDIR /app

COPY --from=builder --chown=appuser:appgroup /build/api-gateway/bin/server .
COPY --from=builder --chown=appuser:appgroup /build/api-gateway/migrations ./migrations
# The binary this stage received is the one the builder built, byte for byte.
COPY --from=builder /build/api-gateway/bin/server.sha256 /tmp/
RUN sha256sum -c /tmp/server.sha256 && rm /tmp/server.sha256

USER appuser

EXPOSE 8080

# Build info for GET /version, which the admin drift check compares across
# services. docker-compose.yml passes these as build args (from the caller's env;
# scripts/deploy-service.sh exports them) and docker-publish.yml passes the
# release sha. Declared last so a new commit rebuilds only this metadata layer.
ARG GIT_COMMIT=dev
ARG BUILD_TIME=unknown
ENV GIT_COMMIT=${GIT_COMMIT} BUILD_TIME=${BUILD_TIME}

CMD ["./server"]
