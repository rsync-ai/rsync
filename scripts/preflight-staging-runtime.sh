#!/usr/bin/env bash
# preflight-staging-runtime.sh — verify the RUNNING staging containers are
# wired for Azure BEFORE you trust a staging test run.
#
# Why this exists (the bug it closes):
#   The CI e2e gate and manual staging testing intentionally SHARE one Docker
#   stack: both drive compose project `rsync-ai` with the same hard-pinned
#   container_names, and `e2e/run_gate.sh` brings the stack up once and never
#   tears it down (tests reuse it; chaos tests restart the sink in place). So
#   after a CI e2e run, the shared containers are left wired to BASE/e2e defaults.
#
#   The casualty is `kafka-mcp-sink`: base env points its exactly-once ACK ledger
#   (`pipeline_batch_acks`) at the LOCAL `postgres:5432/pipeline_db?sslmode=disable`,
#   while staging pipelines live in Azure `staging`. The parent
#   pipeline row is then absent in the sink's DB →
#     ack persist failed: ... violates foreign key constraint "fk_batch_acks_pipeline"
#   and EVERY staging BATCH pipeline silently stalls at executor 80%, landing
#   no data. (Verified 2026-06-13 on pipeline f3a7d280.)
#
#   `scripts/preflight-prod-config.sh` only checks the compose RENDER; it never
#   inspects the RUNNING containers — which is exactly where this drift hides.
#   This guard closes that gap.
#
# What it checks (running container env vs the staging render):
#   • DB identity — POSTGRES_URL / DATABASE_URL (host + db + sslmode) and
#     DB_HOST / DB_NAME / DB_SSLMODE — must match the Azure staging target.
#     (Catches the sink-collision class above.)
#   • Temporal DB — the temporal server's POSTGRES_SEEDS must be the Azure host,
#     not the local `postgres`; otherwise staging workflow state persists locally
#     while pipelines run on Azure. (--fix recreates temporal.)
#   • No local postgres — the `local-db`-profile `postgres` container must NOT be
#     running. A bare `docker compose up` (without the staging -f files +
#     --env-file) starts it and silently rewires services to local `pipeline_db`,
#     and the RSYNC_REQUIRE_REMOTE_DB guard never arms. (--fix removes it.)
#   • REDIS auth  — orchestrator + temporal-adapter REDIS_PASSWORD must be
#     NON-EMPTY. staging Redis runs with --requirepass; a blank password
#     NOAUTH-crashes IntentActivityV2 (the #216 class). Compared as
#     present/absent, NOT against the render — the multi-file merge renders this
#     key empty even though the real value is set via interpolation.
#
# Usage (run from anywhere in the repo or a worktree):
#   scripts/preflight-staging-runtime.sh          # report drift, exit 1 on any
#   scripts/preflight-staging-runtime.sh --fix    # recreate drifted services, re-verify
#
# Override the env file (default: <main-repo-root>/.env.staging, which is
# gitignored and absent in worktrees, so we resolve the main repo root for you):
#   ENV_FILE=/path/to/.env.staging scripts/preflight-staging-runtime.sh
#
# Exit codes: 0 = wired correctly · 1 = drift (not safe to test) · 2 = setup error.

set -euo pipefail

FIX=0
for arg in "$@"; do
  case "$arg" in
    --fix) FIX=1 ;;
    -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
    *) echo "unknown arg: $arg (use --fix or --help)" >&2; exit 2 ;;
  esac
done

# Resolve the MAIN repo root (not the worktree): the shared running stack is the
# main-root stack, and .env.staging lives only there.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# --git-common-dir may be relative (e.g. "../.git" from a worktree, or ".git");
# resolve it against SCRIPT_DIR before taking its parent so this works whether
# invoked from the main checkout or a linked worktree.
COMMON_DIR="$(git -C "$SCRIPT_DIR" rev-parse --git-common-dir)"
case "$COMMON_DIR" in /*) ;; *) COMMON_DIR="$SCRIPT_DIR/$COMMON_DIR" ;; esac
MAIN_ROOT="$(cd "$COMMON_DIR/.." && pwd)"
cd "$MAIN_ROOT"

ENV_FILE="${ENV_FILE:-$MAIN_ROOT/.env.staging}"

say()  { printf '\n\033[1m▶ %s\033[0m\n' "$*"; }
ok()   { printf '\033[32m✅ %s\033[0m\n' "$*"; }
die()  { printf '\033[31m🛑 %s\033[0m\n' "$*" >&2; exit 2; }

[[ -f "$ENV_FILE" ]] || die "ENV_FILE '$ENV_FILE' not found — run this on the staging machine, or set ENV_FILE=..."

COMPOSE=(-f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.staging.yml --env-file "$ENV_FILE")
REMEDY_BASE="docker compose -f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.staging.yml --env-file $ENV_FILE up -d --no-deps --force-recreate"

# Render the desired staging config once. (docker compose may emit raw control
# chars inside multi-line command/entrypoint values, so we parse with Python's
# json strict=False rather than jq, which rejects them.)
RENDER_JSON="$(mktemp)"
trap 'rm -f "$RENDER_JSON"' EXIT
say "Rendering staging config against $ENV_FILE"
if ! docker compose "${COMPOSE[@]}" config --format json >"$RENDER_JSON" 2>/dev/null; then
  die "'docker compose config' failed — fix compose/env errors first."
fi

# ── Comparison (running container env vs render) ────────────────────────────
# Python does the parsing and per-key comparison; it prints a human report to
# stderr and one `FIX\t<service-key>` line per drifted service to stdout.
run_check() {
  python3 - "$RENDER_JSON" <<'PY'
import json, subprocess, sys, re

render_path = sys.argv[1]
render = json.load(open(render_path), strict=False)

# service compose-key -> running container name
SERVICES = {
    "kafka-mcp-sink-mcp": "rsync-ai-kafka-mcp-sink-v1-0-0-mcp",
    "api-gateway":        "rsync-ai-api-gateway",
    "orchestrator":       "rsync-ai-orchestrator",
    "temporal-adapter":   "rsync-ai-temporal-adapter",
    # temporal SERVER persistence — POSTGRES_SEEDS must be the Azure host, not the
    # local `postgres`. (Container name is hard-pinned to `temporal`.)
    "temporal":           "temporal",
}
# DB-identity keys to compare running-vs-render (reliable in the render).
URL_KEYS    = {"POSTGRES_URL", "DATABASE_URL"}          # compare host+db+sslmode
# POSTGRES_SEEDS is the temporal server's DB host (present only in temporal's
# desired env, so the `if k in desired` guard skips it for every other service).
LITERAL_KEYS = {"DB_HOST", "DB_NAME", "DB_SSLMODE", "POSTGRES_SEEDS"}  # compare exact string
# REDIS_PASSWORD is checked as present/absent on these (render is unreliable here).
REDIS_NONEMPTY = {"orchestrator", "temporal-adapter"}

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

drifted = []     # service keys needing recreate
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

    if svc in REDIS_NONEMPTY:
        if not actual.get("REDIS_PASSWORD", ""):
            err(f"❌ DRIFT  {svc} ({container})  REDIS_PASSWORD is EMPTY "
                f"(staging Redis requires auth — NOAUTH will crash IntentActivityV2)")
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

# ---------------------------------------------------------------------------
# Redis auth gate — MUST run before the per-service wiring check.
#
# CI/e2e brings the SHARED redis container up with NO password (e2e has an empty
# REDIS_PASSWORD); staging services all carry a non-empty REDIS_PASSWORD. A
# no-auth redis then silently breaks EVERY redis-dependent: orchestrator
# FATAL-crashes ("AUTH called without any password configured"), and
# temporal-adapter nil-Stores its correlation client → IntentActivityV2
# nil-deref. The reverse also bites (authed redis + e2e services).
#
# The trap here: recreating a single dependent --no-deps against a no-auth redis
# just re-breaks it, so healing them one at a time loops forever. The container's
# requirepass is the single source of truth — so heal REDIS FIRST, wait for it to
# actually require+accept the staging password, THEN recreate ALL dependents in
# one ordered pass.
REDIS_CONTAINER="${REDIS_CONTAINER:-redis}"
# Every service that carries REDIS_PASSWORD in the rendered staging config.
REDIS_DEPENDENTS=(api-gateway llm-service orchestrator planner temporal-adapter tool-generator kafka-mcp-sink-mcp)
REDIS_PW="$(grep -E '^REDIS_PASSWORD=' "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d '"'"'"'\r')"

redis_auth_ok() {
  # No staging password configured → nothing to enforce.
  [[ -n "$REDIS_PW" ]] || return 0
  docker inspect "$REDIS_CONTAINER" >/dev/null 2>&1 || return 1   # not running
  # Authed ping must succeed…
  docker exec "$REDIS_CONTAINER" redis-cli -a "$REDIS_PW" --no-auth-warning ping 2>/dev/null | grep -q PONG || return 1
  # …and an UNauthenticated ping must be rejected (proves requirepass is active).
  ! docker exec "$REDIS_CONTAINER" redis-cli ping 2>/dev/null | grep -q PONG
}

if ! redis_auth_ok; then
  echo "❌ DRIFT  $REDIS_CONTAINER  not requiring staging auth (CI/e2e likely left it NOAUTH)" >&2
  if [[ "$FIX" -ne 1 ]]; then
    printf '\n\033[31m🛑 redis auth drift — staging is NOT safe to test.\033[0m\n' >&2
    echo "   A no-auth shared redis silently breaks every redis-dependent service." >&2
    echo "   Re-run with --fix to recreate redis (with --requirepass) + all dependents." >&2
    exit 1
  fi
  say "--fix: recreating redis with staging auth, then all redis-dependents (ordered)"
  # shellcheck disable=SC2086
  docker compose "${COMPOSE[@]}" up -d --no-deps --force-recreate "$REDIS_CONTAINER" >/dev/null
  for _i in $(seq 1 20); do redis_auth_ok && break; sleep 2; done
  redis_auth_ok || die "redis did not come up requiring the staging password — inspect '$REDIS_CONTAINER'."
  for svc in "${REDIS_DEPENDENTS[@]}"; do
    printf '   recreating %s …\n' "$svc"
    # shellcheck disable=SC2086
    docker compose "${COMPOSE[@]}" up -d --no-deps --force-recreate "$svc" >/dev/null
  done
  ok "redis auth healed + all redis-dependents recreated."
fi

# ---------------------------------------------------------------------------
# Internal service-secret gate.
#
# INTERNAL_SERVICE_SECRET authenticates service-to-service calls (api-gateway →
# orchestrator: /agent/test-connection connection tests, executor OAuth refresh).
# docker-compose.prod.yml passes it as ${INTERNAL_SERVICE_SECRET:-}, so an
# UNDEFINED var interpolates to EMPTY — and the orchestrator, running
# ENVIRONMENT=production, then fails requirePrincipal closed with HTTP 401
# (auth_middleware.go: the internal-secret branch is skipped when the secret is
# ""). Symptom: connection tests + OAuth refresh 401. Historically the key was in
# no .env.*.example, so a fresh .env.staging silently omitted it.
#
# Enforce non-empty in ENV_FILE AND on the running api-gateway + orchestrator
# (a container started before the key was added carries an empty value even once
# ENV_FILE is fixed — recreate to pick it up).
INTERNAL_SECRET_SERVICES=(api-gateway orchestrator)
INTERNAL_SECRET_CONTAINERS=(rsync-ai-api-gateway rsync-ai-orchestrator)
INTERNAL_SECRET="$(grep -E '^INTERNAL_SERVICE_SECRET=' "$ENV_FILE" | head -1 | cut -d= -f2- | tr -d '"'"'"'\r')"

internal_secret_running_ok() {
  local c val
  for c in "${INTERNAL_SECRET_CONTAINERS[@]}"; do
    docker inspect "$c" >/dev/null 2>&1 || continue   # not running → skip (per-service loop reports absence elsewhere)
    val="$(docker inspect "$c" --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null \
            | grep -E '^INTERNAL_SERVICE_SECRET=' | head -1 | cut -d= -f2-)"
    [[ -n "$val" ]] || return 1
  done
  return 0
}

recreate_internal_secret_services() {
  local svc
  for svc in "${INTERNAL_SECRET_SERVICES[@]}"; do
    printf '   recreating %s …\n' "$svc"
    # shellcheck disable=SC2086
    docker compose "${COMPOSE[@]}" up -d --no-deps --force-recreate "$svc" >/dev/null
  done
}

if [[ -z "$INTERNAL_SECRET" ]]; then
  echo "❌ DRIFT  INTERNAL_SERVICE_SECRET is EMPTY in $ENV_FILE" >&2
  if [[ "$FIX" -ne 1 ]]; then
    printf '\n\033[31m🛑 internal service secret missing — staging is NOT safe to test.\033[0m\n' >&2
    echo "   S2S calls (connection tests, OAuth refresh) will 401 — the orchestrator fails closed." >&2
    echo "   Add it:  echo \"INTERNAL_SERVICE_SECRET=\$(openssl rand -hex 32)\" >> $ENV_FILE" >&2
    echo "   (or re-run scripts/setup-staging-db.sh), then re-run this guard with --fix." >&2
    exit 1
  fi
  say "--fix: generating INTERNAL_SERVICE_SECRET into $ENV_FILE, then recreating api-gateway + orchestrator"
  printf 'INTERNAL_SERVICE_SECRET=%s\n' "$(openssl rand -hex 32)" >> "$ENV_FILE"
  recreate_internal_secret_services
  internal_secret_running_ok || die "INTERNAL_SERVICE_SECRET still empty on a running service after recreate — inspect api-gateway/orchestrator."
  ok "INTERNAL_SERVICE_SECRET generated + api-gateway/orchestrator recreated."
elif ! internal_secret_running_ok; then
  echo "❌ DRIFT  INTERNAL_SERVICE_SECRET set in $ENV_FILE but EMPTY on a running service" >&2
  if [[ "$FIX" -ne 1 ]]; then
    printf '\n\033[31m🛑 internal service secret not applied — staging is NOT safe to test.\033[0m\n' >&2
    echo "   A service was started before the secret was set; recreate it to pick the value up." >&2
    echo "   Re-run with --fix, or:  $REMEDY_BASE api-gateway orchestrator" >&2
    exit 1
  fi
  say "--fix: recreating api-gateway + orchestrator to apply INTERNAL_SERVICE_SECRET"
  recreate_internal_secret_services
  internal_secret_running_ok || die "INTERNAL_SERVICE_SECRET still empty on a running service after recreate — inspect api-gateway/orchestrator."
  ok "api-gateway/orchestrator recreated with INTERNAL_SERVICE_SECRET."
fi

# ---------------------------------------------------------------------------
# E2E contamination gate — the claim-check `minio` DNS-alias split-brain.
#
# The e2e overlay (compose project `rsync-ai-e2e`, docker-compose.e2e.dbs.yml)
# attaches its containers to the SHARED rsync-ai_default network, and its `minio`
# service claims the `minio` alias — colliding with the main stack's
# rsync-ai-minio. Docker DNS then round-robins `minio:9000` across two MinIO
# stores, so a claim-check object written to one store is read from the other →
# NoSuchKey → DLQ → SILENT data drop (size-dependent: bigger tables lose more
# often). `make e2e-start` brings these up WITHOUT taking the stack lock, so the
# leak survives into staging runs. Verified 2026-06-16 on pipeline b4465471
# (customers 1703 rows → 0 landed). Reclaim any e2e leftover before trusting a run.
e2e_leftovers() {
  docker ps --filter "label=com.docker.compose.project=rsync-ai-e2e" --format '{{.Names}}' 2>/dev/null
}
E2E_LEFT="$(e2e_leftovers)"
if [[ -n "$E2E_LEFT" ]]; then
  echo "❌ DRIFT  rsync-ai-e2e containers share the staging network (claim-check 'minio' split-brain):" >&2
  # shellcheck disable=SC2001
  echo "$E2E_LEFT" | sed 's/^/     /' >&2
  if [[ "$FIX" -ne 1 ]]; then
    printf '\n\033[31m🛑 e2e leftovers contaminate staging — NOT safe to test.\033[0m\n' >&2
    echo "   Their minio hijacks the 'minio' DNS alias → claim-check reads can NoSuchKey → silent drop." >&2
    echo "   Re-run with --fix to reclaim them, or: make e2e-clean" >&2
    exit 1
  fi
  say "--fix: reclaiming rsync-ai-e2e leftovers (frees the 'minio' alias)"
  # shellcheck disable=SC2086
  docker rm -f $E2E_LEFT >/dev/null 2>&1 || true
  if [[ -z "$(e2e_leftovers)" ]]; then
    ok "e2e leftovers reclaimed — 'minio' now resolves to a single store."
  else
    die "e2e leftovers persist after --fix — remove them manually (make e2e-clean)."
  fi
fi

# ---------------------------------------------------------------------------
# Stray local-postgres gate.
#
# docker-compose.staging.yml is explicit (the temporal block + comment ~87-111):
# there is intentionally NO `postgres:` override, so the base `postgres` service
# (gated behind profiles: ["local-db"] in prod.yml) NEVER starts in a correct
# staging launch. A bare `docker compose up` — without the three staging -f files
# + --env-file — auto-includes docker-compose.override.yml and starts it anyway,
# silently rewiring services to the local `pipeline_db`; and because the env-file
# isn't loaded, the RSYNC_REQUIRE_REMOTE_DB guard never arms to catch it. If the
# container exists at all, the divergence is present — remove it.
stray_postgres() {
  docker ps --filter "label=com.docker.compose.project=rsync-ai" \
            --filter "label=com.docker.compose.service=postgres" \
            --format '{{.Names}}' 2>/dev/null
}

# Report (and in --fix, remove) the stray postgres. Returns non-zero only in
# report mode when present, so the caller exits 1. MUST run AFTER the wiring heal
# below: in --fix it removes the container only once no running service still
# resolves the local `postgres` host.
handle_stray_postgres() {
  local stray; stray="$(stray_postgres || true)"
  [[ -z "$stray" ]] && return 0
  echo "❌ DRIFT  local-db postgres container running: $stray" >&2
  echo "     staging must have NONE (docker-compose.staging.yml:87-90)." >&2
  if [[ "$FIX" -ne 1 ]]; then
    printf '\n\033[31m🛑 stray local postgres — staging is NOT safe to test.\033[0m\n' >&2
    echo "   A bare 'docker compose up' (no staging -f/--env-file) starts it and" >&2
    echo "   silently rewires services to local pipeline_db. Re-run with --fix," >&2
    echo "   or remove it: docker rm -f $stray" >&2
    return 1
  fi
  # Only safe once nothing resolves the local `postgres` host any more.
  local refs="" name
  for name in $(docker ps --filter "label=com.docker.compose.project=rsync-ai" --format '{{.Names}}'); do
    [[ "$name" == "$stray" ]] && continue
    if docker exec "$name" printenv 2>/dev/null \
         | grep -qiE '(POSTGRES_HOST|DB_HOST|PGHOST|DATABASE_HOST|POSTGRES_SEEDS)=postgres([:[:space:]]|$)|@postgres:'; then
      refs="$refs $name"
    fi
  done
  [[ -n "$refs" ]] && die "stray postgres still referenced by:$refs — heal those first, then re-run --fix."
  say "--fix: removing stray local postgres container ($stray)"
  docker rm -f "$stray" >/dev/null 2>&1 || true
  [[ -z "$(stray_postgres || true)" ]] || die "stray postgres persists after 'docker rm -f' — remove it manually."
  ok "stray local postgres removed."
  return 0
}

say "Checking running container wiring (DB identity + temporal seeds + Redis auth)"
if ! verify_and_collect "$DRIFT_OUT"; then
  # Drift found. (portable read — macOS ships bash 3.2, which lacks `mapfile`.)
  DRIFTED=()
  while IFS=$'\t' read -r _tag _svc; do
    [[ "$_tag" == "FIX" && -n "$_svc" ]] && DRIFTED+=("$_svc")
  done < "$DRIFT_OUT"

  if [[ "$FIX" -ne 1 ]]; then
    printf '\n\033[31m🛑 runtime wiring drift — staging is NOT safe to test.\033[0m\n' >&2
    echo "   Most likely a CI e2e run left the shared containers on base/e2e defaults." >&2
    echo "   Heal it with:" >&2
    for svc in "${DRIFTED[@]}"; do
      echo "     $REMEDY_BASE $svc" >&2
    done
    echo "   …or re-run this guard with --fix to do it automatically." >&2
    echo "   …or relaunch the whole stack from the MAIN checkout: make staging-up" >&2
    exit 1
  fi

  # --fix: recreate each drifted service (incl. temporal), then re-verify once.
  say "--fix: recreating drifted services for Azure staging"
  for svc in "${DRIFTED[@]}"; do
    printf '   recreating %s …\n' "$svc"
    # shellcheck disable=SC2086
    docker compose "${COMPOSE[@]}" up -d --no-deps --force-recreate "$svc" >/dev/null
  done

  say "Re-verifying after --fix"
  verify_and_collect "$DRIFT_OUT" || die "drift persists after --fix — inspect the services above manually."
fi

# Wiring parity is GREEN here (first pass or after heal). The local-db postgres
# is removed LAST — now that no service resolves the local host.
handle_stray_postgres || exit 1

ok "runtime wiring parity passed — shared containers are wired for Azure staging."
exit 0
