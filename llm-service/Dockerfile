# Pinned by manifest-list digest (an OCI index, so the pin stays multi-arch:
# amd64 and arm64 both resolve through it). The tag alone floats, and every
# layer below here -- apt, pip, spacy -- is invalidated the moment upstream
# republishes it. Bump deliberately, not by accident.
FROM python:3.11-slim@sha256:da047cb8f9d1d98e5c070f5300ba9f7274e33b8fc0e5be5ed88740aed1b95ba9

# Install Docker CLI + buildx plugin via apt (avoids curl|sh supply chain risk).
# buildx is required for BuildKit named contexts (--build-context shared=...), which
# the tool-generator's JIT connector build uses to satisfy generated Dockerfiles'
# `COPY --from=shared ...`. Without it, on-demand builds of freshly generated
# connectors fail with "invalid from flag value shared".
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    gnupg \
    && install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg \
    && chmod a+r /etc/apt/keyrings/docker.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian bookworm stable" \
       > /etc/apt/sources.list.d/docker.list \
    && apt-get update && apt-get install -y --no-install-recommends docker-ce-cli docker-buildx-plugin \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user; add to docker group so it can reach the mounted socket.
# The GID 999 matches the docker group on most Linux hosts — override at runtime
# with `--group-add $(stat -c '%g' /var/run/docker.sock)` if your host differs.
RUN groupadd -g 999 docker 2>/dev/null || true \
    && useradd -m -u 1000 -G docker appuser

WORKDIR /app

# Install dependencies.
#
# The cache mount is what makes a cache MISS here cheap, and a miss is the
# normal case: the PR smoke gate reuses a warm per-machine BuildKit cache that
# was seeded by whichever PR last built this image, so the `COPY requirements.txt`
# key is keyed to THAT branch's file, not to main's. Measured on the 2026-09-24
# gate run: base + apt layers CACHED, then COPY requirements.txt missed and this
# step ran for 400s on a PR that changed no dependency at all -- 400s of the
# gate's 663s image-build phase, and this image is the phase's critical path.
#
# The mount survives layer-cache key drift and is shared across branches, so a
# miss reinstalls from local wheels instead of re-resolving and re-downloading
# from PyPI. `--no-cache-dir` MUST stay off for that to work -- it existed to
# keep wheels out of the layer, which the mount does better (the cache is never
# part of the image). `sharing=locked` because two builds writing one pip cache
# can corrupt it; nothing is lost by serialising, the gate holds a repo-wide
# lock anyway.
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip,sharing=locked \
    pip install -r requirements.txt

# Pre-install the spaCy English model used by Presidio.
# Without this, the FIRST suggestions call after a fresh container start
# triggers an inline download from GitHub (en_core_web_sm ~13MB + a fallback
# en_core_web_lg ~400MB), which historically tripped the 30s gateway timeout
# and produced the intermittent "Failed to generate suggestions" error.
# Baked in here means startup-time pay, not request-time hang.
RUN python -m spacy download en_core_web_sm

# Copy source code
COPY src/ ./src/
COPY prompts/ ./prompts/

RUN chown -R appuser:appuser /app

USER appuser

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Build info for GET /version, which the admin drift check compares across
# services. docker-compose.yml passes these as build args (from the caller's env;
# scripts/deploy-service.sh exports them) and docker-publish.yml passes the
# release sha. Declared last so a new commit rebuilds only this metadata layer.
ARG GIT_COMMIT=dev
ARG BUILD_TIME=unknown
ENV GIT_COMMIT=${GIT_COMMIT} BUILD_TIME=${BUILD_TIME}

# Default command
CMD ["python", "-m", "src.gateway.main"]

