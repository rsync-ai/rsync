#!/usr/bin/env bash
# REAL /v1/deploy runtime smoke for the OSS connector-lifecycle image.
#
# Where oss-leak-proof-test.sh proves the image carries none of the moat AND that `docker
# buildx` is *present*, it never actually POSTs /v1/deploy — so it is green-but-blind over
# the one path the self-host data plane calls (backend-orchestrator -> TOOL_GENERATOR_URL
# /v1/deploy for self-heal / pinned-version JIT builds). This script closes that gap by
# standing up ONLY the OSS lifecycle container in isolation and driving a connector all the
# way to a running container.
#
# ── WHY THIS EXISTS (the F1 class) ───────────────────────────────────────────────────────
# docker_builder.py is fully gated on the `docker` Python SDK (DOCKER_SDK_AVAILABLE): with
# the SDK absent, DockerBuilder._check_docker_available() is False and every /v1/deploy
# returns the error_message "Docker not available" — an OSS image that imports fine but
# cannot deploy a single connector (dead-on-arrival). PR #500 added `docker>=7.0.0` to
# llm-service/requirements-oss.txt to fix it. This smoke is the guard that would have
# caught it.
#
# ── NEGATIVE CONTROL (how we know this smoke actually catches F1) ─────────────────────────
# Remove the `docker>=7.0.0` line from llm-service/requirements-oss.txt, rebuild the image,
# and re-run this smoke: it MUST fail at assertion (a) with the response carrying
# "Docker not available". That failure is the proof this smoke would have caught the F1
# dead-on-arrival. (Assertions (b)/(c) never even get a chance — the build never starts.)
#
# ── ISOLATION / SAFETY ───────────────────────────────────────────────────────────────────
# The box runs a shared `rsync-ai` + `rsync-ai-mcp` stack. This smoke NEVER touches those:
# it uses its own throwaway network / volume / container names (rsync-oss-smoke-*) and a
# private host port, aborts up-front if the target connector container already exists
# (so it can't clobber a shared-stack one), and a trap tears down everything it created.
#
# ── HOST-PATH ALIASING (mirror the compose wiring exactly) ───────────────────────────────
# When the lifecycle container serves /v1/deploy with build_if_missing, it shells out to
# `docker build <ctx> --build-context shared=<public/>` (docker_builder.py
# _cli_build_with_shared_context) against the MOUNTED docker socket — i.e. the HOST daemon.
# For that build context + the JIT-started container's network DNS to line up, we mirror
# docker-compose.oss.yml / docker-compose.quickstart.yml precisely:
#   * seeded connectors live on a named volume mounted at the SAME in-container path the
#     compose files use: /app/shared/mcp-connectors, with TOOLS_DIR pointed there;
#   * the lifecycle container AND every JIT connector it starts share ONE user-defined
#     network, so the lifecycle -> connector /health probe resolves by container name;
#   * DOCKER_NETWORK and MCP_SHARED_NETWORK are both pinned to that isolated network, so the
#     JIT container is NEVER attached to the shared `rsync-ai-mcp` network (default would);
#   * OAUTH_TOKENS_VOLUME_NAME is blanked so start_container mounts no shared token volume.
#
# Usage:  bash scripts/oss-deploy-smoke.sh
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

IMG="rsync-oss-lifecycle:smoke"
CTX="$ROOT/llm-service"                       # Dockerfile.oss build context
SEED_SRC="$ROOT/shared/mcp-connectors"        # host source for the throwaway seed

# Isolated, throwaway resources — the `rsync-oss-smoke-` prefix keeps them clearly distinct
# from the shared `rsync-ai` / `rsync-ai-mcp` projects; the trap removes each on exit.
NET="rsync-oss-smoke-net"
VOL="rsync-oss-smoke-connectors"
LIFECYCLE_CN="rsync-oss-smoke-lifecycle"
HOST_PORT="15010"                             # private host port -> lifecycle :5010

# Lightweight seed connector (no DB driver, no OAuth). petstore is a flat public connector
# whose versioned dir is a self-contained build context (Dockerfile + connector.py +
# requirements.txt + base_connector.py) and whose Dockerfile pulls shared libs via the
# `shared` named context (public/rsync_protocol + public/warehouse_adapters.py).
CONNECTOR="petstore"

FAIL=0
say() { printf '\n=== %s ===\n' "$1"; }
ok()  { printf '  ✓ %s\n' "$1"; }
bad() { printf '  ✗ %s\n' "$1"; FAIL=1; }

# Extract one field from a JSON object on stdin. Booleans come back lowercased
# ("true"/"false"); missing/null -> "".
jget() {
  python3 -c 'import sys,json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
v = d.get(sys.argv[1])
print("" if v is None else (str(v).lower() if isinstance(v, bool) else v))' "$1"
}

# Resolve the connector's concrete current version -> versioned container name that
# docker_builder starts (rsync-ai-<id>-vX-Y-Z-mcp).
CV="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["current_version"])' \
        "$SEED_SRC/public/$CONNECTOR/latest.json" 2>/dev/null)"
if [ -z "${CV:-}" ]; then echo "cannot resolve $CONNECTOR current_version"; exit 1; fi
VERSION_PART="$(printf '%s' "$CV" | sed 's/^v//; s/\./-/g')"
CONTAINER="rsync-ai-${CONNECTOR}-v${VERSION_PART}-mcp"     # e.g. rsync-ai-petstore-v1-0-3-mcp
IMAGE_REF="mcp-${CONNECTOR}:${CV}"                         # e.g. mcp-petstore:v1.0.3

cleanup() {
  # Best-effort teardown of ONLY the resources this smoke created. Never targets the shared
  # rsync-ai / rsync-ai-mcp projects.
  docker rm -f "$LIFECYCLE_CN" >/dev/null 2>&1 || true
  docker rm -f "$CONTAINER"    >/dev/null 2>&1 || true
  docker network rm "$NET"     >/dev/null 2>&1 || true
  docker volume rm  "$VOL"     >/dev/null 2>&1 || true
  docker rmi -f "$IMAGE_REF"   >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ── preflight ────────────────────────────────────────────────────────────────────────────
say "PREFLIGHT"
if ! docker info >/dev/null 2>&1; then echo "docker daemon not reachable"; exit 1; fi
# Refuse to run if the target JIT connector container already exists — it may belong to the
# shared stack, and start_container would reuse/remove it. Fail loud instead of clobbering.
if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  echo "  ✗ $CONTAINER already exists on this host — aborting to avoid touching a shared-stack container"
  exit 1
fi
ok "docker reachable; $CONTAINER not present (safe to proceed)"

# ── build the OSS lifecycle image ────────────────────────────────────────────────────────
say "BUILD (Dockerfile.oss -> $IMG)"
docker build -f "$CTX/Dockerfile.oss" -t "$IMG" "$CTX" || { echo "build failed"; exit 1; }

# ── seed a throwaway mcp_connectors volume with ONE connector + its shared deps ───────────
# Mirrors Dockerfile.seed's allowlist, scoped to just $CONNECTOR: the connector root, the
# shared rsync_protocol package, warehouse_adapters.py, and the root base_connector.py.
say "SEED throwaway volume ($VOL) with $CONNECTOR + shared libs"
docker volume create "$VOL" >/dev/null || { echo "volume create failed"; exit 1; }
docker run --rm \
  -v "$SEED_SRC":/src:ro \
  -v "$VOL":/target \
  busybox:1.36 sh -c '
    set -e
    mkdir -p /target/public
    cp -a /src/public/'"$CONNECTOR"' /target/public/
    cp -a /src/public/rsync_protocol /target/public/
    cp -a /src/public/warehouse_adapters.py /target/public/
    cp -a /src/base_connector.py /target/
  ' || { echo "seed failed"; exit 1; }
ok "seeded /public/$CONNECTOR (+ rsync_protocol, warehouse_adapters.py, base_connector.py)"

# ── isolated network + lifecycle container (mirrors docker-compose.oss.yml tool-generator) ─
say "RUN lifecycle container in isolation"
docker network create "$NET" >/dev/null || { echo "network create failed"; exit 1; }
docker run -d \
  --name "$LIFECYCLE_CN" \
  --network "$NET" \
  --user "0:0" \
  -p "127.0.0.1:${HOST_PORT}:5010" \
  -e PORT=5010 \
  -e TOOLS_DIR=/app/shared/mcp-connectors \
  -e RSYNC_MANAGED_CONNECTORS=true \
  -e DOCKER_NETWORK="$NET" \
  -e MCP_SHARED_NETWORK="$NET" \
  -e OAUTH_TOKENS_VOLUME_NAME= \
  -e RSYNC_EDITION=community \
  -e LOG_LEVEL=info \
  -v "$VOL":/app/shared/mcp-connectors \
  -v /var/run/docker.sock:/var/run/docker.sock \
  "$IMG" >/dev/null || { echo "lifecycle run failed"; exit 1; }

# ── wait for /health ─────────────────────────────────────────────────────────────────────
say "WAIT for /health"
HEALTH_URL="http://127.0.0.1:${HOST_PORT}/health"
HEALTHY=0
for _ in $(seq 1 30); do
  if curl -fsS "$HEALTH_URL" >/dev/null 2>&1; then HEALTHY=1; break; fi
  sleep 1
done
if [ "$HEALTHY" = 1 ]; then
  ok "lifecycle /health is up"
else
  bad "lifecycle /health never came up"
  docker logs --tail 40 "$LIFECYCLE_CN" 2>&1 || true
  say "RESULT"; echo "  ❌ FAIL — lifecycle did not start"; exit "$FAIL"
fi

# ── POST /v1/deploy {build_if_missing:true} + poll (the JIT-build request shape) ────────
say "DEPLOY $CONNECTOR ($CV) via POST /v1/deploy"
DEPLOY_URL="http://127.0.0.1:${HOST_PORT}/v1/deploy"
PAYLOAD="{\"connector_name\":\"${CONNECTOR}\",\"version\":\"latest\",\"build_if_missing\":true}"

# First call: image is missing, so the handler kicks off a background JIT build and returns
# success=true, building=true, started=false. We then re-POST (idempotent; the in-flight
# guard prevents duplicate builds) until the background build+start completes and the
# start-only fast path reports started=true. A cold build (base image pull + pip install)
# can take minutes, so the poll deadline is generous.
RESP="$(curl -sS -X POST "$DEPLOY_URL" -H 'Content-Type: application/json' -d "$PAYLOAD" 2>/dev/null)"
printf '  first response: %s\n' "$RESP"

DEADLINE=$(( $(date +%s) + 420 ))   # up to 7 min for a cold build
STARTED=""; BUILT=""; SUCCESS=""
while :; do
  # Assertion (a) is checked on EVERY response — the F1 symptom must never appear.
  if printf '%s' "$RESP" | grep -q 'Docker not available'; then
    break
  fi
  STARTED="$(printf '%s' "$RESP" | jget started)"
  BUILT="$(printf '%s' "$RESP" | jget built)"
  SUCCESS="$(printf '%s' "$RESP" | jget success)"
  if [ "$STARTED" = "true" ] || [ "$BUILT" = "true" ]; then break; fi
  if [ "$(date +%s)" -ge "$DEADLINE" ]; then break; fi
  sleep 4
  RESP="$(curl -sS -X POST "$DEPLOY_URL" -H 'Content-Type: application/json' -d "$PAYLOAD" 2>/dev/null)"
done
printf '  final response: %s\n' "$RESP"

# ── assertions ───────────────────────────────────────────────────────────────────────────
# (a) NOT the F1 symptom — passing this alone proves the docker SDK gate is satisfied (F1 fixed).
say "ASSERT (a) response is NOT the F1 'Docker not available' symptom"
if printf '%s' "$RESP" | grep -q 'Docker not available'; then
  bad "response carries 'Docker not available' — the docker SDK gate FAILED (F1 dead-on-arrival). Is docker>=7.0.0 in requirements-oss.txt?"
else
  ok "no 'Docker not available' — docker SDK gate satisfied (F1 fixed)"
fi

# (b) deploy actually succeeded and the connector was started (or built).
say "ASSERT (b) success==true and (started==true or built==true)"
SUCCESS="$(printf '%s' "$RESP" | jget success)"
STARTED="$(printf '%s' "$RESP" | jget started)"
BUILT="$(printf '%s' "$RESP" | jget built)"
if [ "$SUCCESS" = "true" ] && { [ "$STARTED" = "true" ] || [ "$BUILT" = "true" ]; }; then
  ok "success=$SUCCESS started=$STARTED built=$BUILT"
else
  ERR="$(printf '%s' "$RESP" | jget error_message)"
  bad "deploy did not report a started/built success (success=$SUCCESS started=$STARTED built=$BUILT error=${ERR:-none})"
fi

# (c) the daemon really has the connector container running.
say "ASSERT (c) docker ps shows $CONTAINER running"
if docker ps --filter "name=^/${CONTAINER}$" --filter "status=running" --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  ok "$CONTAINER is running"
else
  bad "$CONTAINER is not running"
  docker logs --tail 40 "$LIFECYCLE_CN" 2>&1 || true
fi

say "RESULT"
if [ "$FAIL" = 0 ]; then
  echo "  ✅ PASS — OSS lifecycle image deployed $CONNECTOR end-to-end via /v1/deploy"
else
  echo "  ❌ FAIL — see above"
fi
exit "$FAIL"
