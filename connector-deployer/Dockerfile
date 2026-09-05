# connector-deployer — the small, trusted socket-holder (SEC-H-02).
#
# It runs as root and mounts /var/run/docker.sock BY DESIGN: concentrating the Docker
# privilege in this tiny no-LLM service (whose only input is DATA, turned into a
# hardcoded-safe HostConfig by internal/spec) is the whole point of the split. The
# runtime image bundles the `docker` CLI (legacy in-daemon build, BUILDKIT_HOST unset)
# AND `buildctl` (rootless build against the buildkitd sidecar, BUILDKIT_HOST set —
# SEC-H-02 increment 2), both needing BuildKit named contexts (COPY --from=shared ...).

# BUILDKIT_VERSION pins buildctl to the SAME moby/buildkit release as the buildkitd
# sidecar in docker-compose.prod.yml (compose passes this as a build-arg), so the
# client and daemon can never drift. Bump both together to the current stable.
ARG BUILDKIT_VERSION=v0.31.2

# ---- buildctl source ----
# A named stage so BUILDKIT_VERSION can parameterize the image: `COPY --from=<image>`
# does NOT support variable expansion, but `FROM <image:$ARG> AS <stage>` does. We copy
# buildctl out of this stage below. The non-rootless image ships the identical client.
FROM moby/buildkit:${BUILDKIT_VERSION} AS buildkit

# ---- build ----
FROM golang:1.25 AS build
WORKDIR /src

# Cache modules first.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags "-s -w" -o /out/deployer ./cmd/deployer

# ---- runtime ----
# docker:27-cli is Alpine-based and bundles the docker CLI + buildx (needed for
# `docker build --build-context`). Keep the tag in step with the host daemon.
FROM docker:29-cli AS runtime
RUN apk add --no-cache ca-certificates
COPY --from=build /out/deployer /usr/local/bin/deployer
# buildctl for the rootless build path — version-locked to the buildkitd sidecar via
# the BUILDKIT_VERSION-parameterized `buildkit` stage above.
COPY --from=buildkit /usr/bin/buildctl /usr/local/bin/buildctl

ENV PORT=5011
EXPOSE 5011

# Override the docker image's default entrypoint with our binary.
ENTRYPOINT ["/usr/local/bin/deployer"]
