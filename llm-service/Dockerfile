FROM python:3.11-slim

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

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

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

# Default command
CMD ["python", "-m", "src.gateway.main"]

