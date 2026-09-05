#!/usr/bin/env bash
# preflight-e2e-runtime.sh — verify the RUNNING shared containers are wired for
# LOCAL e2e/base BEFORE the e2e gate trusts them. The mirror of
# scripts/preflight-staging-runtime.sh (which guards the OTHER direction).
#
# Why this exists (the reverse-leak this closes):
#   The CI e2e gate and manual staging testing intentionally SHARE one Docker
#   stack: both drive compose project `rsync-ai` with the same hard-pinned
#   container_names, and neither tears the shared services down. Each side must
#   therefore RECONCILE the wiring to its own target at start-up — that is the
#   stack's documented idempotent-reconcile model (see ci.yml data-pipeline-gate
#   comment: "reconcile the stack idempotently on each run").
#
#   `preflight-staging-runtime.sh` handles "a gate run left base/e2e wiring →
#   re-wire for Azure staging." This script handles the SYMMETRIC reverse:
#   a prior STAGING run left `kafka-mcp-sink` / `orchestrator` / `api-gateway`
#   wired to AZURE `staging` (+ real S3, real GitHub). If the gate then runs
#   against that, e2e pipelines write to AZURE instead of the local stack —
#   either polluting staging data or failing outright. This guard force-recreates
#   any such service back to LOCAL base/e2e wiring before the gate proceeds.
#
# What it checks (running container env vs the base+e2e RENDER):
#   • DB identity — POSTGRES_URL / DATABASE_URL (host+db+sslmode) and
#     DB_HOST / DB_NAME / DB_SSLMODE — must be the LOCAL `postgres/pipeline_db`,
#     not an Azure host. (Catches the sink/orchestrator/api-gateway reverse leak.)
#   • e2e overlay markers — orchestrator MINIO_ENDPOINT_URL must be the in-stack
#     `http://minio:9000`, api-gateway GITHUB_TOKEN_URL must be the in-stack
#     `http://mock-github:8080/...` (the offline-auth mock). A staging run points
#     these at real S3 / real GitHub; the gate's deterministic tests need the
#     local mocks.
#
# REDIS auth is deliberately NOT checked here: it is a staging-only concern
# (staging Redis runs --requirepass; local/e2e Redis does not).
#
# Usage (run from anywhere in the repo or a worktree):
#   scripts/preflight-e2e-runtime.sh          # report drift, exit 1 on any
#   scripts/preflight-e2e-runtime.sh --fix    # recreate drifted services, re-verify
#
# Exit codes: 0 = wired for local e2e · 1 = drift (gate not safe) · 2 = setup error.

set -euo pipefail

FIX=0
for arg in "$@"; do
  case "$arg" in
    --fix) FIX=1 ;;
    -h|--help) sed -n '2,46p' "$0"; exit 0 ;;
    *) echo "unknown arg: $arg (use --fix or --help)" >&2; exit 2 ;;
  esac
done

# Resolve the MAIN repo root (not the worktree): the shared running stack is the
# main-root stack. Mirrors preflight-staging-runtime.sh's resolution.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMMON_DIR="$(git -C "$SCRIPT_DIR" rev-parse --git-common-dir)"
case "$COMMON_DIR" in /*) ;; *) COMMON_DIR="$SCRIPT_DIR/$COMMON_DIR" ;; esac
MAIN_ROOT="$(cd "$COMMON_DIR/.." && pwd)"
cd "$MAIN_ROOT"

say()  { printf '\n\033[1m▶ %s\033[0m\n' "$*"; }
ok()   { printf '\033[32m✅ %s\033[0m\n' "$*"; }
die()  { printf '\033[31m🛑 %s\033[0m\n' "$*" >&2; exit 2; }

# Base + e2e overlay = exactly what e2e/run_gate.sh's dc_main brings up. No
# --env-file: the gate runs on local/base defaults.
# STACK_PREFIX (exported by run_gate.sh): rsync-ai (default, shared stack) or an
# isolated CI prefix (e.g. rsync-ci). Layer the SAME container-name/network
# override dc_main uses + pin the project, so this preflight targets the SAME
# project + containers the gate just brought up. Exported so the python heredoc
# below (SERVICES container names) resolves the identical prefix.
export STACK_PREFIX="${STACK_PREFIX:-rsync-ai}"
if [[ "${STACK_PREFIX}" == "rsync-ai" ]]; then CI_MAIN=(); else CI_MAIN=(-f docker-compose.ci-isolate.yml); fi
COMPOSE=(-p "${STACK_PREFIX}" -f docker-compose.yml -f docker-compose.e2e.yml ${CI_MAIN[@]+"${CI_MAIN[@]}"})
REMEDY_BASE="docker compose ${COMPOSE[*]} up -d --no-deps --force-recreate"

RENDER_JSON="$(mktemp)"
trap 'rm -f "$RENDER_JSON"' EXIT
say "Rendering base+e2e config (docker-compose.yml + docker-compose.e2e.yml)"
if ! docker compose "${COMPOSE[@]}" config --format json >"$RENDER_JSON" 2>/dev/null; then
  die "'docker compose config' failed — fix compose errors first."
fi

# ── Comparison (running container env vs render) ────────────────────────────
run_check() {
  python3 - "$RENDER_JSON" <<'PY'
import json, subprocess, sys, re, os

render_path = sys.argv[1]
render = json.load(open(render_path), strict=False)

# service compose-key -> running container name (STACK_PREFIX-aware: matches the
# container_name override dc_main applies — rsync-ai-* by default, rsync-ci-* in CI)
_PFX = os.environ.get("STACK_PREFIX", "rsync-ai")
SERVICES = {
    "kafka-mcp-sink-mcp": f"{_PFX}-kafka-mcp-sink-v1-0-0-mcp",
    "api-gateway":        f"{_PFX}-api-gateway",
    "orchestrator":       f"{_PFX}-orchestrator",
}
URL_KEYS     = {"POSTGRES_URL", "DATABASE_URL"}                       # compare host+db+sslmode
LITERAL_KEYS = {"DB_HOST", "DB_NAME", "DB_SSLMODE",                   # local DB identity
                "MINIO_ENDPOINT_URL", "GITHUB_TOKEN_URL"}            # e2e overlay markers

def err(*a): print(*a, file=sys.stderr)

def url_triple(u):
    """(host, db, sslmode) from a postgres URL — ignores user/password noise."""
    if not u:
        return (None, None, None)
    host = None
    m = re.search(r'@([^/:?]+)', u)
    if m: host = m.group(1)
    db = None
    m = re.search(r'@[^/]*/([^?]+)', u)
    if m: db = m.group(1)
    ssl = None
    m = re.search(r'sslmode=([^&\s]+)', u)
    if m: ssl = m.group(1)
    return (host, db, ssl)

def running_env(container):
    try:
        out = subprocess.run(
            ["docker", "inspect", container, "--format", "{{json .Config.Env}}"],
            capture_output=True, text=True, check=True).stdout
    except subprocess.CalledProcessError:
        return None  # not running
    env = {}
    for item in json.loads(out):
        k, _, v = item.partition("=")
        env[k] = v
    return env

drifted = []
not_running = []

for svc, container in SERVICES.items():
    desired = (render["services"].get(svc, {}) or {}).get("environment", {}) or {}
    actual = running_env(container)
    if actual is None:
        not_running.append((svc, container))
        continue

    svc_drift = False

    for k in URL_KEYS:
        if k in desired:
            want, got = url_triple(desired[k]), url_triple(actual.get(k, ""))
            if want != got:
                err(f"❌ DRIFT  {svc} ({container})  {k}")
                err(f"     expected host/db/sslmode: {want}")
                err(f"     running  host/db/sslmode: {got}")
                svc_drift = True

    for k in LITERAL_KEYS:
        if k in desired:
            want, got = desired[k], actual.get(k, "")
            if want != got:
                err(f"❌ DRIFT  {svc} ({container})  {k}: expected '{want}', running '{got}'")
                svc_drift = True

    if svc_drift:
        drifted.append(svc)
    else:
        err(f"   ok    {svc} ({container})")

for svc, container in not_running:
    err(f"   ⚠️  {svc} ({container}) not running — skipped")

for svc in drifted:
    print(f"FIX\t{svc}")

sys.exit(1 if drifted else 0)
PY
}

verify_and_collect() {
  # prints FIX lines to the named file; returns python's exit code
  local out="$1"
  set +e
  run_check >"$out"
  local rc=$?
  set -e
  return $rc
}

DRIFT_OUT="$(mktemp)"
trap 'rm -f "$RENDER_JSON" "$DRIFT_OUT"' EXIT

# KI-E2E-GATE-CONNECTOR-NET: MCP connectors come up only on the rsync-ai-mcp
# network, but the e2e DB fixtures (docker-compose.e2e.dbs.yml) and the
# data-pipeline gate's connector-reachability probe live on rsync-ai_default.
# Attach every running connector to rsync-ai_default (idempotent; e2e-only — does
# NOT touch the generated prod compose, which would force this external network
# onto every connector in prod and break bring-up where it is absent) so the gate
# can reach the connectors and the connectors can reach the e2e DBs. Without this
# the gate fails with "❌ <connector> MCP not reachable on rsync-ai_default" and
# the batch/CDC tests time out. Runs on every invocation (bring-up + reuse paths).
bridge_connectors_to_e2e_net() {
  local net="${STACK_PREFIX:-rsync-ai}_default" c n=0
  docker network inspect "$net" >/dev/null 2>&1 || { say "   ⚠️  ${net} absent — skipping connector bridge"; return 0; }
  for c in $(docker network inspect "${STACK_PREFIX:-rsync-ai}-mcp" -f '{{range .Containers}}{{.Name}}{{"\n"}}{{end}}' 2>/dev/null); do
    [[ "$c" == *-mcp ]] || continue
    if docker network connect "$net" "$c" >/dev/null 2>&1; then n=$((n + 1)); fi
  done
  say "connector→${net} bridge ensured (${n} newly attached)"
}
bridge_connectors_to_e2e_net

say "Checking running container wiring (local DB identity + e2e overlay markers)"
if verify_and_collect "$DRIFT_OUT"; then
  ok "runtime wiring parity passed — shared containers are wired for local e2e."
  exit 0
fi

# Drift found. (portable read — macOS ships bash 3.2, which lacks `mapfile`.)
DRIFTED=()
while IFS=$'\t' read -r _tag _svc; do
  [[ "$_tag" == "FIX" && -n "$_svc" ]] && DRIFTED+=("$_svc")
done < "$DRIFT_OUT"

if [[ "$FIX" -ne 1 ]]; then
  printf '\n\033[31m🛑 runtime wiring drift — the e2e gate is NOT safe to run.\033[0m\n' >&2
  echo "   Most likely a manual staging run left the shared containers wired to Azure." >&2
  echo "   Heal it with:" >&2
  for svc in "${DRIFTED[@]}"; do
    echo "     $REMEDY_BASE $svc" >&2
  done
  echo "   …or re-run this guard with --fix to do it automatically." >&2
  exit 1
fi

# --fix: recreate each drifted service, then re-verify once.
say "--fix: recreating drifted services for local e2e"
for svc in "${DRIFTED[@]}"; do
  printf '   recreating %s …\n' "$svc"
  # shellcheck disable=SC2086
  docker compose "${COMPOSE[@]}" up -d --no-deps --force-recreate "$svc" >/dev/null
done

say "Re-verifying after --fix"
if verify_and_collect "$DRIFT_OUT"; then
  ok "drift healed — shared containers now wired for local e2e."
  exit 0
fi
die "drift persists after --fix — inspect the services above manually."
