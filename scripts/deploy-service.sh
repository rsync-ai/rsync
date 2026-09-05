#!/usr/bin/env bash
# deploy-service.sh — rebuild and restart a single prod service.
# Run this ON THE PROD VM to deploy a specific service without touching others.
#
# Usage:
#   ./scripts/deploy-service.sh api-gateway
#   ./scripts/deploy-service.sh frontend
#   ./scripts/deploy-service.sh orchestrator llm-service   # multiple at once
#
# Prerequisites on prod VM:
#   - .env.prod exists in ~/rsync-ai
#   - docker compose v2 installed
#   - git remote origin set to your GitHub repo

set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILES="-f docker-compose.yml -f docker-compose.prod.yml"
ENV_FILE="--env-file .env.prod"

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <service> [service2 ...]"
  echo "Available services: api-gateway frontend orchestrator llm-service tool-generator planner temporal-adapter"
  exit 1
fi

cd "$REPO_DIR"

echo "=== git pull ==="
git pull origin main

for SERVICE in "$@"; do
  echo ""
  echo "=== building $SERVICE ==="
  # No --no-cache: Docker layer cache + BuildKit mount caches make rebuilds fast.
  # First build after this change will be slow (cold cache); all subsequent builds
  # will reuse the Go build cache and unchanged layers — ~20-30s instead of ~180s.
  DOCKER_BUILDKIT=1 docker compose $COMPOSE_FILES $ENV_FILE build "$SERVICE"

  echo "=== restarting $SERVICE ==="
  docker compose $COMPOSE_FILES $ENV_FILE up -d --no-deps --force-recreate "$SERVICE"

  echo "=== waiting for $SERVICE to settle (10s) ==="
  sleep 10

  STATUS=$(docker compose $COMPOSE_FILES $ENV_FILE ps "$SERVICE" --format json 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Health', d.get('State', 'unknown')))" 2>/dev/null || echo "unknown")
  echo "=== $SERVICE status: $STATUS ==="
done

echo ""
echo "=== running smoke test ==="
DIRECT=1 "$REPO_DIR/scripts/smoke-test.sh"
