#!/usr/bin/env bash
#
# Data-pipeline correctness GATE (RELIABILITY_PLAN.md Phase 0).
#
# Brings up the full stack ONCE (batch + CDC, incl. Debezium/Kafka-Connect),
# then runs the curated data-correctness + chaos e2e suite against the shared
# stack with E2E_SKIP_UP=1 so the tests do NOT each re-bring-up. Aggregates
# results, NEVER stops on the first failure, and exits non-zero if ANY test
# fails -- so CI can use this single script as a required, merge-blocking check.
#
# Design notes (correctness-critical):
#   * Bring-up is COPIED from the proven golden script
#     (test_golden_type_preservation_mysql_to_postgres_batch.sh) and EXTENDED
#     with the three CDC services the golden path omits:
#       kafka-init, schema-registry, kafka-connect (Debezium :8083), debezium-mcp.
#   * Chaos tests run LAST: they intentionally kill redis / the sink worker
#     (each restarts it in a `finally`), so running them before the correctness
#     tests could perturb a shared stack.
#   * Only DETERMINISTIC, LOCAL-ONLY tests are gated. Tests needing real S3,
#     live OAuth (shopify), or external SaaS are deliberately excluded -- a
#     flaky gate is a useless gate. See the EXCLUDED note at the bottom.
#   * Quarantine: list a test filename (one per line) in e2e/QUARANTINE.md under
#     the `<!-- quarantine -->` marker to skip it WITHOUT failing the gate. A
#     quarantined test is tracked debt, never a silent deletion.
#
# Env knobs:
#   E2E_SKIP_UP=1     skip bring-up (stack already running)
#   E2E_SKIP_HEALTH=1 skip the health-wait (operator has already verified health,
#                     or for unit-testing the harness itself)
#   E2E_BUILD=1       pass --build to the bring-up (rebuild images)
#   GATE_ONLY="a b"   run only the named tests (space-separated filenames)
#   GATE_MODE=smoke   FAST per-PR gate: run only SMOKE_TESTS (one batch + one CDC)
#                     against the already-running stack. Defaults E2E_SKIP_UP=1
#                     (reuse the warm dev/staging stack); pass E2E_SKIP_UP=0 to
#                     force a cold bring-up. Default (unset/"full") = the full
#                     batch + CDC[+chaos] matrix, unchanged.
#   SMOKE_BUILD_MAIN  (smoke only) space-separated docker-compose.yml service names
#                     to rebuild + recreate before the run (e.g. "orchestrator
#                     api-gateway kafka-mcp-sink-mcp"). Lets the per-PR gate
#                     exercise the PR's CODE, not stale warm-stack images, while
#                     rebuilding ONLY what changed (the rest of the warm stack is
#                     reused untouched). Empty => nothing rebuilt (e.g. a
#                     test-script-only PR).
#   SMOKE_BUILD_MCP   (smoke only) space-separated connector IDs (e.g. "mysql
#                     postgresql") to rebuild + recreate in the rsync-ai-mcp
#                     project. Versioned service names resolve here via the shared
#                     mcp_service_name() helper -- never hardcode them in CI.
#   E2E_PARALLELISM=N max concurrent isolated CDC tests (default 2; =1 forces the
#                     original fully-serial run). Only name-isolated tests run
#                     concurrently; fixed-name .sh CDC + chaos stay serial.
#   E2E_INCLUDE_OAUTH=1  (full mode) additionally bring up mock-github + the
#                     github-rest MCP (private-net overlay) and run the opt-in
#                     OAUTH_TESTS bucket (deterministic generated-connector OAuth2
#                     token-refresh proof). Off by default so the per-PR/nightly
#                     surface is unchanged; intended for a nightly/on-demand lane.
#   E2E_INCLUDE_GRAPHQL=1  (opt-in) additionally bring up mock-graphql + the
#                     widgets-graphql MCP (private-net overlay) and run the opt-in
#                     GRAPHQL_TESTS bucket (deterministic generated-GraphQL
#                     connector -> Postgres pipeline proof). Same treatment as
#                     E2E_INCLUDE_OAUTH: off by default, nightly/on-demand.
#
# Exit: 0 = every non-quarantined test passed; 1 = at least one failed (or
#       bring-up/health failed).

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
E2E_DIR="${ROOT_DIR}/e2e"
cd "${ROOT_DIR}"

# --- Isolated-stack support (STACK_PREFIX) ----------------------------------
# Default (unset) => STACK_PREFIX=rsync-ai: BYTE-IDENTICAL to the historical
# single-shared-stack behavior (no override files, current ports, bare names).
# CI sets STACK_PREFIX=rsync-ci to run a FULLY ISOLATED stack (own compose
# project + container names + host ports + networks) that coexists with a
# developer's local rsync-ai dev/staging stack on the SAME Docker daemon.
STACK_PREFIX="${STACK_PREFIX:-rsync-ai}"
MAIN_PROJECT="${STACK_PREFIX}"
E2E_PROJECT="${STACK_PREFIX}-e2e"
MCP_PROJECT="${STACK_PREFIX}-mcp"
if [[ "${STACK_PREFIX}" == "rsync-ai" ]]; then
  # default: shared single-stack behavior, unchanged
  CI_MAIN=(); CI_DBS=(); CI_MCP=()
  ORCH_PG_CONTAINER="postgres"; KAFKA_CONNECT_CONTAINER="kafka-connect"
  API_GATEWAY_URL="${API_GATEWAY_URL:-http://localhost:5001}"
  CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
  PLANNER_URL="${PLANNER_URL:-http://localhost:8001}"
else
  # isolated CI stack: layer the container-name/network overrides, remap every
  # host port to a distinct value, and thread the prefixed names into the
  # subprocess tests (each reads these as env-overridable defaults).
  CI_MAIN=(-f "${ROOT_DIR}/docker-compose.ci-isolate.yml")
  CI_DBS=(-f "${ROOT_DIR}/docker-compose.e2e.dbs.ci-isolate.yml")
  CI_MCP=(-f "${ROOT_DIR}/docker-compose.mcp.ci-isolate.yml")
  ORCH_PG_CONTAINER="${STACK_PREFIX}-postgres"; KAFKA_CONNECT_CONTAINER="${STACK_PREFIX}-kafka-connect"
  # distinct host ports (default+10000) so a dev rsync-ai stack never clashes
  export RSYNC_HP_API_GATEWAY=15001 RSYNC_HP_CONNECT=18083 RSYNC_HP_POSTGRES=15432 \
    RSYNC_HP_KAFKA=19092 RSYNC_HP_SCHEMA_REGISTRY=18085 RSYNC_HP_TEMPORAL=17233 \
    RSYNC_HP_TEMPORAL_UI=18233 RSYNC_HP_TEMPORAL_ADAPTER=18082 RSYNC_HP_CONTEXT7=18087 \
    RSYNC_HP_REDIS=16379 RSYNC_HP_ORCHESTRATOR=18081 RSYNC_HP_PLANNER=18001 \
    RSYNC_HP_FRONTEND=13000 RSYNC_HP_FLUENT=34224 RSYNC_HP_OTEL_GRPC=24317 \
    RSYNC_HP_OTEL_HTTP=24318 RSYNC_HP_OTEL_FLUENTFWD=18006 RSYNC_HP_OTEL_HEALTH=23133 \
    RSYNC_HP_MYSQL_E2E=13307 RSYNC_HP_POSTGRES_E2E=15433 RSYNC_HP_MINIO=19000 \
    RSYNC_HP_MINIO_CONSOLE=19001 RSYNC_HP_MOCK_GITHUB=18099 RSYNC_HP_MOCK_GRAPHQL=18098
  API_GATEWAY_URL="http://localhost:${RSYNC_HP_API_GATEWAY}"
  CONNECT_URL="http://localhost:${RSYNC_HP_CONNECT}"
  PLANNER_URL="http://localhost:${RSYNC_HP_PLANNER}"
  # subprocess tests read these as ${VAR:-<default>}; exporting overrides the default
  export STACK_PREFIX API_GATEWAY_URL CONNECT_URL ORCH_PG_CONTAINER \
    NETWORK="${STACK_PREFIX}_default" \
    MYSQL_CONTAINER="${STACK_PREFIX}-mysql-e2e" \
    PG_CONTAINER="${STACK_PREFIX}-postgres-e2e"
  # per-prefix lock + marker so CI NEVER serializes against staging's lock
  export STACK_LOCK_DIR="/tmp/${STACK_PREFIX}-stack.lock" \
    STAGING_HOLD_FILE="/tmp/${STACK_PREFIX}-stack.staging-hold"
fi
RESULTS_DIR="${RESULTS_DIR:-/tmp/e2e-gate-$(date +%Y%m%d-%H%M%S)}"
mkdir -p "${RESULTS_DIR}"

log()  { printf '\n==> %s\n' "$*"; }
warn() { printf 'WARN: %s\n' "$*" >&2; }
die()  { printf 'GATE FAIL (setup): %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }

need docker; need curl; need jq; need python3

# --- setup-failure diagnostics ---------------------------------------------
# Every `die` in the health wait below used to print one assertion line and
# exit. That is how three consecutive "Kafka Connect (Debezium) not healthy"
# failures produced ZERO evidence about why: the fixtures are torn down on the
# way out, the containers are gone by the time anyone looks, and the CI log
# held nothing but the assertion. A gate that cannot say why it failed cannot
# be trusted to say a change is safe -- so dump the container's state and tail,
# plus the host's disk/memory, BEFORE dying.
#
# Redaction: a log line is dropped WHOLE when it looks like it carries a
# credential. Connect logs connector configs, JDBC URLs and broker SASL
# settings, and this output lands in a CI log that becomes public with the
# repo. Masking in place would leave the surrounding text and risk a partial
# leak; dropping the line cannot.
diag_container() {
  local ctr="$1" lines="${2:-80}"
  printf '\n--- diagnostics: container %s ---\n' "${ctr}" >&2
  docker inspect -f 'state={{.State.Status}} exit={{.State.ExitCode}} oomkilled={{.State.OOMKilled}} restarts={{.RestartCount}} health={{if .State.Health}}{{.State.Health.Status}}{{else}}<none>{{end}}' \
    "${ctr}" 2>&1 | sed 's/^/  /' >&2 || true
  printf '  --- last %s log lines ---\n' "${lines}" >&2
  docker logs --tail "${lines}" "${ctr}" 2>&1 \
    | sed -E 's/.*([Pp][Aa][Ss][Ss][Ww][Oo][Rr][Dd]|[Ss][Ee][Cc][Rr][Ee][Tt]|[Tt][Oo][Kk][Ee][Nn]|[Aa][Pp][Ii][-_]?[Kk][Ee][Yy]|[Aa][Uu][Tt][Hh][Oo][Rr][Ii][Zz][Aa][Tt][Ii][Oo][Nn]).*/[line redacted: possible credential]/' \
    | sed 's/^/  /' >&2 || true
}

# Host-level pressure. A JVM (Kafka Connect) that never opens its REST port on
# a box also running the dev/staging stack is far more often starved of RAM or
# disk than broken -- and neither shows up in the container's own log.
diag_host() {
  printf '\n--- diagnostics: host pressure ---\n' >&2
  df -h / 2>&1 | sed 's/^/  /' >&2 || true
  docker system df 2>&1 | sed 's/^/  /' >&2 || true
  docker stats --no-stream --format '  {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}' 2>&1 >&2 || true
}

# die, but explain first. Container name is resolved by the caller because the
# default (rsync-ai) and isolated (rsync-ci) stacks name them differently.
die_diag() { local ctr="$1"; shift; diag_container "${ctr}"; diag_host; die "$@"; }

# --- compose helpers (verbatim from the golden script) ----------------------
# E2E_INCLUDE_OAUTH=1 opt-in: layer the OAuth-refresh overlays so the app tier
# gets a shared INTERNAL_SERVICE_SECRET and the github-rest MCP can reach the
# private mock-github host. Flag unset (normal PR/nightly gate) => the extra -f
# is absent and the rendered stacks are byte-identical to before.
dc_main() {
  local oauth=()
  [[ "${E2E_INCLUDE_OAUTH:-}" == "1" ]] && oauth=(-f "${ROOT_DIR}/docker-compose.e2e.oauth.yml")
  # NB: ${arr[@]+"${arr[@]}"} idiom — an EMPTY oauth array must not trip `set -u`
  # (unbound variable) on the runner's bash 3.2 (see the SELECTED-bucket loop).
  docker compose -p "${MAIN_PROJECT}" -f "${ROOT_DIR}/docker-compose.yml" -f "${ROOT_DIR}/docker-compose.e2e.yml" ${CI_MAIN[@]+"${CI_MAIN[@]}"} ${oauth[@]+"${oauth[@]}"} "$@"
}
dc_e2e() {
  docker compose -p "${E2E_PROJECT}" -f "${ROOT_DIR}/docker-compose.e2e.dbs.yml" ${CI_DBS[@]+"${CI_DBS[@]}"} "$@"
}
dc_mcp() {
  local oauth=() graphql=()
  [[ "${E2E_INCLUDE_OAUTH:-}" == "1" ]] && oauth=(-f "${ROOT_DIR}/docker-compose.mcp.e2e.yml")
  # E2E_INCLUDE_GRAPHQL=1 opt-in: layer the widgets-graphql private-net overlay so
  # the generated GraphQL connector can reach the private mock-graphql host.
  [[ "${E2E_INCLUDE_GRAPHQL:-}" == "1" ]] && graphql=(-f "${ROOT_DIR}/docker-compose.mcp.graphql.e2e.yml")
  # NB: ${arr[@]+"${arr[@]}"} — empty-array-safe under bash 3.2 `set -u`.
  docker compose -p "${MCP_PROJECT}" -f "${ROOT_DIR}/docker-compose.mcp.yml" ${CI_MCP[@]+"${CI_MCP[@]}"} ${oauth[@]+"${oauth[@]}"} ${graphql[@]+"${graphql[@]}"} "$@"
}

# Warm-runner self-heal for `up -d` dying on a stale compose network: a
# container from an earlier run can outlive the project network (a teardown /
# recreate swaps the network while the stopped container still pins the dead
# id), and the next plain `up -d` then fails with "failed to set up container
# networking: network <id> not found" before a single test runs (observed on
# the PR smoke gate: stale `postgres` killed the whole run in 37s). The daemon
# error names the network, not the container, and the stale container is
# typically a DEPENDENCY of the requested services — so the retry must
# recreate the named services AND their dependency closure
# (--always-recreate-deps), not just re-run the same no-op `up`. Only this
# exact daemon error triggers the single retry; any other failure propagates
# unchanged so a real bring-up error still fails the gate loudly.
compose_up_selfheal() {
  local dc="$1"; shift
  local out rc
  out=$("$dc" up -d "$@" 2>&1); rc=$?
  [[ -n "${out}" ]] && printf '%s\n' "${out}"
  [[ ${rc} -eq 0 ]] && return 0
  if grep -qE 'failed to set up container networking|network [0-9a-f]{12,} not found' <<<"${out}"; then
    warn "stale compose network on warm stack; retrying '${dc} up -d $*' with --force-recreate --always-recreate-deps"
    "$dc" up -d --force-recreate --always-recreate-deps "$@"
    return $?
  fi
  return "${rc}"
}

# --- RAM hygiene: reclaim leftover e2e Debezium connectors ------------------
# The .py CDC tests register connectors named `debug-<kind>-<db>-<ts>` with a
# fresh timestamp per run and (by design) only pre-delete their OWN exact name
# before creating — they never delete on exit. Across runs these pile up (76
# observed on the self-hosted box), each pinning kafka-connect heap PLUS a live
# source replication-slot / binlog reader. On an 18 GB box already deep in swap
# that degrades every later test, and the heaviest namespace test runs LAST
# (SERIAL_TAIL) on the most-degraded stack — which is exactly what pushes it
# past E2E_PIPELINE_TIMEOUT_S. Reclaim the test-owned `debug-` namespace up front
# so each gate starts on a clean connect cluster. Best-effort: never fails the
# gate. Scope is the `debug-` prefix ONLY — never a prod/manual connector.
# Opt out with E2E_SKIP_CONNECTOR_RECLAIM=1.
#
# NB: defined HERE, above on_exit()/the EXIT trap, on purpose. bash resolves
# function names at call time, so on_exit (which calls reclaim_e2e_connection_rows)
# can fire on an EARLY setup failure — e.g. the stack-lock acquire timing out —
# before execution ever reaches a definition placed lower in the file. Both
# reclaim_* helpers must precede the trap install so that early-exit teardown
# path resolves them instead of dying with `command not found` (and silently
# skipping the connections-table reclaim). Keep them above `trap on_exit EXIT`.
reclaim_e2e_connectors() {
  [[ "${E2E_SKIP_CONNECTOR_RECLAIM:-}" == "1" ]] && { log "connector reclaim: skipped (E2E_SKIP_CONNECTOR_RECLAIM=1)"; return 0; }
  local removed
  removed="$(docker exec "${KAFKA_CONNECT_CONTAINER}" bash -lc '
    n=0
    for c in $(curl -s localhost:8083/connectors 2>/dev/null | tr ",[]\"" "\n" | grep "^debug-"); do
      curl -s -X DELETE "localhost:8083/connectors/${c}" >/dev/null 2>&1 && n=$((n+1))
    done
    echo "${n}"' 2>/dev/null || echo 0)"
  if [[ "${removed:-0}" -gt 0 ]]; then
    log "connector reclaim: removed ${removed} leftover debug-* connector(s) (freed connect heap + source CDC readers)"
  else
    log "connector reclaim: no leftover debug-* connectors"
  fi
}

# --- DB hygiene: reclaim leftover e2e/gate CONNECTION ROWS ------------------
# Sibling to reclaim_e2e_connectors, but for the api-gateway `connections` table
# in the SHARED rsync-ai pipeline_db. The batch/CDC tests INSERT connection rows
# (golden-*, e2e-*, pgns-*, gipk-*, ...) and -- unlike the throwaway e2e DB
# fixtures torn down in on_exit -- never delete them. Across runs they pile up
# (324 observed) and clutter the Data Explorer connection list on the shared
# stack that manual staging also reads. Reap ONLY the gate's own generated name
# prefixes; never a manual/prod connection (e.g. azure-*). FK rules make this
# safe: pipelines.*_connection_id = SET NULL (pipelines survive), per-connection
# metadata (access logs / cdc_resources / checkpoints / pii_scan_jobs) = CASCADE.
# Best-effort: never fails the gate. Opt out with E2E_SKIP_CONNECTION_ROW_RECLAIM=1.
reclaim_e2e_connection_rows() {
  [[ "${E2E_SKIP_CONNECTION_ROW_RECLAIM:-}" == "1" ]] && { log "connection-row reclaim: skipped (E2E_SKIP_CONNECTION_ROW_RECLAIM=1)"; return 0; }
  local removed
  removed="$(docker exec "${ORCH_PG_CONTAINER}" psql -U "${PGUSER:-user}" -d "${PGDATABASE:-pipeline_db}" -tAc "
    WITH del AS (
      DELETE FROM connections
      WHERE split_part(name,'-',1) IN
        ('golden','e2e','pgns','gipk','nomkey','pgleak','flat','diag','debug')
      RETURNING 1)
    SELECT count(*) FROM del;" 2>/dev/null | tr -d '[:space:]')"
  if [[ "${removed:-0}" =~ ^[0-9]+$ && "${removed}" -gt 0 ]]; then
    log "connection-row reclaim: removed ${removed} leftover e2e/gate connection row(s) from pipeline_db"
  else
    log "connection-row reclaim: no leftover e2e/gate connection rows"
  fi
}

# --- single-stack serialization + self-cleaning lifecycle -------------------
# The gate and manual staging share ONE Docker stack (project rsync-ai). Take a
# host-wide mutex so they can never drive it concurrently, and tear down the
# throwaway e2e DB fixtures on exit so they don't sit on the memory-constrained
# box between runs.
# shellcheck source=../scripts/_stack_lock.sh
source "${ROOT_DIR}/scripts/_stack_lock.sh"

# STAGING-OWNERSHIP GUARD (fail-closed). If manual staging currently owns the
# shared `rsync-ai` stack (scripts/staging-up.sh left a durable marker), this
# gate must NOT reconcile it back to local e2e wiring — that force-recreate
# (preflight-e2e-runtime.sh --fix) is exactly what clobbers a live staging stack.
# Refuse loudly and exit BEFORE registering the teardown trap or taking the
# mutex. Self-healing: if the marker is stale (a manual `docker compose down`
# left it behind and api-gateway is no longer Azure-wired), clear it and proceed
# so a forgotten marker can never wedge CI forever. Escape hatch:
# STAGING_HOLD_OVERRIDE=1 (you then accept the clobber).
if [[ "${STACK_PREFIX}" == "rsync-ai" ]] && staging_hold_active && [[ "${STAGING_HOLD_OVERRIDE:-}" != "1" ]]; then
  gate_dburl="$(docker inspect rsync-ai-api-gateway \
    --format '{{range .Config.Env}}{{println .}}{{end}}' 2>/dev/null \
    | grep -E '^DATABASE_URL=' || true)"
  if printf '%s' "$gate_dburl" | grep -qi 'azure'; then
    die "staging owns the shared rsync-ai stack [$(staging_hold_info)] — refusing to run: reconciling to local wiring would clobber the live Azure-wired staging stack. Run 'scripts/staging-down.sh' to release it, or set STAGING_HOLD_OVERRIDE=1 to force."
  else
    warn "stale staging-hold marker found but api-gateway is not Azure-wired — clearing it and proceeding."
    clear_staging_hold
  fi
fi

on_exit() {
  local rc=$?
  # RAM hygiene: remove ONLY the rsync-ai-e2e fixtures — never the shared
  # rsync-ai stack, whose volumes the self-hosted runner relies on (see the
  # ci.yml "we do NOT down -v" note). The fixtures are recreated with
  # --renew-anon-volumes every run, so this loses nothing. E2E_KEEP=1 leaves
  # them up for post-run inspection / fast local iteration.
  if [[ "${E2E_KEEP:-}" != "1" ]]; then
    log "Tearing down e2e DB fixtures (set E2E_KEEP=1 to keep them)"
    dc_e2e down -v >/dev/null 2>&1 || warn "e2e fixture teardown returned non-zero"
    # Also reap the connection ROWS the tests wrote into the shared pipeline_db
    # (the e2e DB fixtures above are separate Docker containers). Best-effort.
    reclaim_e2e_connection_rows
  fi
  release_stack_lock
  exit $rc
}
trap on_exit EXIT

acquire_stack_lock "e2e-gate (PID $$)" \
  || die "another rsync-ai stack owner is active (staging?) — refusing to run the gate"

resolve_current_version() {
  local connector_id="$1"
  python3 - "${connector_id}" <<'PY'
import glob, json, sys
cid = sys.argv[1].strip()
root = "shared/mcp-connectors/public"
candidates = []
direct = f"{root}/{cid}/latest.json"
if glob.glob(direct):
    candidates.append(direct)
candidates.extend(glob.glob(f"{root}/**/{cid}/latest.json", recursive=True))
if not candidates:
    raise SystemExit(f"latest.json not found for connector: {cid}")
def score(p):
    p2 = p.replace("\\", "/")
    return (1 if "/database/" in p2 else 0, len(p2))
best = sorted(set(candidates), key=score)[0]
latest = json.load(open(best, "r"))
v = (latest.get("current_version") or "").strip()
if not v:
    p = (latest.get("path") or "").strip()
    if p.startswith("versions/"):
        v = p[len("versions/"):].strip()
if not v:
    v = (latest.get("version") or "").strip()
if not v:
    raise SystemExit(f"missing version in {best}")
print(v if v.startswith("v") else "v" + v)
PY
}
to_version_part() { local v="$1"; v="${v#v}"; echo "${v//./-}"; }
mcp_service_name() { echo "$1-v$(to_version_part "$(resolve_current_version "$1")")-mcp"; }

# --- bring up the full stack ONCE (batch + CDC) -----------------------------
bring_up_stack() {
  local build_opt=""
  if [[ "${E2E_BUILD:-}" == "1" ]]; then
    # Build images in a SEPARATE phase BEFORE any `up`, then bring the stack up
    # from cached images (build_opt stays empty). Why: `up -d --build` builds the
    # heavy kafka-connect image (maven + debezium SMT compile) CONCURRENTLY with
    # kafka starting. kafka has only a 512m heap and a ~130s health window; on a
    # loaded self-hosted runner the build starves it and it misses health →
    # "dependency failed to start: container rsync-ai-kafka is unhealthy" and the
    # gate dies at setup (intermittent). Pre-building means kafka boots on an
    # unloaded box and wins its window deterministically.
    # Build ONLY the services this gate actually brings up — NOT `build` with no
    # args, which compiles the ENTIRE compose stack (frontend Next.js/TypeScript,
    # llm-service, tool-generator) that the data plane never uses and which can
    # hang for ~tens of minutes on the runner. Keep this list in lockstep with
    # the `up` service lists below.
    log "Pre-building gate images (decoupled from kafka startup to avoid health starvation)"
    dc_main build \
      kafka-connect debezium-mcp temporal-adapter api-gateway orchestrator \
      kafka-mcp-sink-mcp minio-mcp \
      || die "image pre-build (core) failed"
    dc_mcp  build "$(mcp_service_name mysql)" "$(mcp_service_name postgresql)" "$(mcp_service_name mongodb)" \
      || die "image pre-build (mcp connectors) failed"
  fi

  log "Bringing up core + CDC stack (this can take a few minutes on a cold runner)"
  # Core + batch (from golden) PLUS the CDC trio the golden path omits:
  #   kafka-init schema-registry kafka-connect debezium-mcp
  compose_up_selfheal dc_main ${build_opt} \
    postgres redis kafka kafka-init schema-registry \
    temporal temporal-adapter \
    kafka-connect debezium-mcp \
    api-gateway orchestrator \
    || die "core stack bring-up failed"
  # planner (the batch PLANNING service): batch pipelines call PlannerActivityV2 ->
  # http://planner:5011/plan, so a batch run FAILS at planning ("dial tcp: lookup
  # planner ... no such host") if planner isn't up. The gate historically assumed
  # planner persisted from a warm dev/staging stack; on a cold runner (or right after
  # scripts/staging-down.sh tore the stack down) it's absent and every batch test
  # dies at planning. Bring it up explicitly — in its OWN call after the core stack
  # is healthy so a rare from-scratch build (planner shares the heavy llm-service
  # image) can't starve kafka's health window. CDC-only runs don't need it; batch does.
  compose_up_selfheal dc_main ${build_opt} planner || die "planner bring-up failed"
  compose_up_selfheal dc_main ${build_opt} kafka-mcp-sink-mcp minio-mcp minio || die "sink/minio bring-up failed"
  compose_up_selfheal dc_mcp ${build_opt} "$(mcp_service_name mysql)" "$(mcp_service_name postgresql)" "$(mcp_service_name mongodb)" || die "mcp connectors bring-up failed"
  # --renew-anon-volumes: the mysql/postgres images declare an anonymous VOLUME
  # at their data dir, which survives plain `up`/`--force-recreate`. On the
  # persistent self-hosted runner that means e2e/<db>/init/*.sql run ONCE ever
  # (at first volume creation) and never again — so seed/auth changes silently
  # never land, and the golden batch test was flaky (e2e_user stuck on
  # caching_sha2_password; cold-cache go-sql-driver auth → "2061 Authentication
  # requires secure connection"). Renewing the anon volumes makes each gate run
  # a fresh, deterministic DB so init is authoritative every time.
  dc_e2e  up -d --force-recreate --renew-anon-volumes mysql-e2e postgres-e2e || die "e2e DBs bring-up failed"
  # MongoDB replica set for the MongoDB CDC test (mongo-e2e-init runs rs.initiate, idempotent).
  dc_e2e  up -d mongo-e2e mongo-e2e-init || warn "mongo-e2e bring-up returned non-zero (continuing; MongoDB CDC test will fail if unavailable)"
  dc_e2e  up -d --no-deps minio-init || warn "minio-init bring-up returned non-zero (continuing)"

  # OAuth2-refresh e2e (opt-in via E2E_INCLUDE_OAUTH=1): the app tier brought up
  # above already carries the shared INTERNAL_SERVICE_SECRET (dc_main folds in
  # docker-compose.e2e.oauth.yml when the flag is set). Here we add the two extra
  # containers the test needs: the mock GitHub provider, and the github-rest MCP
  # rendered with docker-compose.mcp.e2e.yml (MCP_ALLOW_PRIVATE_NETWORKS + the app
  # network) so it can reach http://mock-github:8080 by DNS. --build because the
  # connector image is not in the default gate build set; safe here since kafka is
  # already healthy (no startup-starvation risk). test_github_rest_oauth_refresh.py
  # self-skips if these are absent, so a warm-stack (E2E_SKIP_UP) run degrades to a
  # skip rather than a hard failure.
  if [[ "${E2E_INCLUDE_OAUTH:-}" == "1" ]]; then
    log "OAuth e2e opt-in: bringing up mock-github + github-rest MCP (private-net overlay)"
    dc_e2e up -d mock-github || die "mock-github bring-up failed"
    dc_mcp up -d --build github-rest-v1-0-0-mcp || die "github-rest MCP bring-up failed"
  fi

  # Generated-GraphQL e2e (opt-in via E2E_INCLUDE_GRAPHQL=1): bring up the mock
  # GraphQL server + the widgets-graphql MCP rendered with
  # docker-compose.mcp.graphql.e2e.yml (MCP_ALLOW_PRIVATE_NETWORKS + the app
  # network) so it can reach http://mock-graphql:8080 by DNS. --build because the
  # connector image is not in the default gate build set; safe here since kafka is
  # already healthy. test_graphql_connector_to_postgres_batch.py self-skips if
  # these are absent, so a warm-stack (E2E_SKIP_UP) run degrades to a skip.
  if [[ "${E2E_INCLUDE_GRAPHQL:-}" == "1" ]]; then
    log "GraphQL e2e opt-in: bringing up mock-graphql + widgets-graphql MCP (private-net overlay)"
    dc_e2e up -d mock-graphql || die "mock-graphql bring-up failed"
    dc_mcp up -d --build widgets-graphql-v1-0-0-mcp || die "widgets-graphql MCP bring-up failed"
  fi
}

# --- smoke: rebuild ONLY the PR-changed data-plane images -------------------
# The fast per-PR gate reuses the WARM dev stack -- which is NOT "built from
# main", as this comment used to claim. It is built from whichever PR last ran
# this gate, and a FAILED or CLOSED PR's images persist under the same
# `${STACK_PREFIX}-*:latest` tags (see log_stack_image_provenance below). Without
# this a smoke test would exercise those images and false-green a regression in
# the changed service. The caller (CI) passes which areas changed; resolution of
# compose service names -- including versioned connector names -- stays HERE via
# the same dc_main/dc_mcp/mcp_service_name helpers the full bring-up uses (single
# source of truth; never duplicate the v1-0-0-mcp naming in YAML). build+up are
# self-contained so this is correct under BOTH the warm-reuse (E2E_SKIP_UP=1) and
# ensure-up (E2E_SKIP_UP=0) paths: it recreates the changed containers itself, and
# a later bring_up_stack just reconciles them as no-ops. No-op when nothing was
# passed (e.g. a test-script-only PR -- scripts run from the checkout, no image).

smoke_selective_build() {
  local built=0
  if [[ -n "${SMOKE_BUILD_MAIN:-}" ]]; then
    log "Smoke: rebuilding + recreating changed core image(s): ${SMOKE_BUILD_MAIN}"
    dc_main build ${SMOKE_BUILD_MAIN} || die "smoke core image rebuild failed"
    compose_up_selfheal dc_main ${SMOKE_BUILD_MAIN} || die "smoke core image recreate failed"
    built=1
  fi
  if [[ -n "${SMOKE_BUILD_MCP:-}" ]]; then
    local svcs=() c
    for c in ${SMOKE_BUILD_MCP}; do svcs+=("$(mcp_service_name "$c")"); done
    log "Smoke: rebuilding + recreating changed connector image(s): ${svcs[*]}"
    dc_mcp build "${svcs[@]}" || die "smoke connector image rebuild failed"
    compose_up_selfheal dc_mcp "${svcs[@]}" || die "smoke connector image recreate failed"
    built=1
  fi
  [[ "${built}" == "1" ]] || log "Smoke: no PR-changed images to rebuild (test-script-only change)"
}

# --- warm-stack image provenance (diagnostics only; NEVER fails the gate) ----
# The warm stack is not built from main; it is built from whichever PR last ran
# this gate, and a failed or closed PR's images survive under the same
# `${STACK_PREFIX}-*:latest` tags. #897 was closed leaving two of them installed
# -- kafka-connect, then planner -- and they went on to fail #902 and #904 for
# about four hours. Nothing recorded when any image was built, so each failure
# looked like a fresh bug in the PR under test rather than a stale image.
#
# One table fixes that. The tell is an image minted minutes-to-hours ago on a
# service the PR under test never touched: main does not rebuild these, so a
# recent timestamp there means some OTHER PR left it behind. Everything here is
# best-effort -- an unreadable image must not fail a gate that would pass.
log_stack_image_provenance() {
  local ids meta shas tags created_map tag_map
  ids=$(docker ps --filter "name=${STACK_PREFIX}-" --format '{{.ID}}' 2>/dev/null) || return 0
  [[ -n "${ids}" ]] || { warn "image provenance: no ${STACK_PREFIX}-* containers running"; return 0; }

  # Resolve each container to the image SHA it is ACTUALLY running, not the tag
  # `docker ps` prints. Those disagree more often than they look like they could:
  # a rebuild moves the tag to a new image while the running container stays on
  # the old one, and a prune can delete that old image out from under it. The
  # container then keeps serving code that no longer exists on disk while still
  # reporting a familiar tag. Reading `docker ps` alone cannot see this.
  meta=$(docker inspect ${ids} --format '{{.Name}}&{{.Config.Image}}&{{.Image}}' 2>/dev/null) || return 0
  shas=$(printf '%s\n' "${meta}" | awk -F'&' '{print $3}' | sort -u)
  tags=$(printf '%s\n' "${meta}" | awk -F'&' '{print $2}' | sort -u)
  # Both inspects are best-effort: a missing image is exactly the case we are
  # hunting, so it must not abort the table. Docker prints the ones it finds.
  created_map=$(docker image inspect ${shas} --format 'S&{{.Id}}&{{.Created}}' 2>/dev/null)
  tag_map=$(docker image inspect ${tags} --format 'T&{{.Id}}&{{.Created}}&{{index .RepoTags 0}}' 2>/dev/null)

  # One awk pass over all three streams, told apart by a leading marker -- awk's
  # -v cannot carry embedded newlines.
  log "Warm-stack image provenance (newest first; a recent build on a service the PR never touched is the tell)"
  printf '%s\n%s\n%s\n' "${created_map}" "${tag_map}" "${meta}" | awk -F'&' '
    $1 == "S" { C[$2] = $3; next }
    $1 == "T" { if ($4 != "") { TS[$4] = $2; TC[$4] = $3 } next }
    NF == 3 {
      name = $1; sub(/^\//, "", name)
      tag = $2; sha = $3
      when = ((sha in C) ? substr(C[sha], 1, 19) : "unknown")
      note = ""
      # The tag resolving to a different image than the container runs means the
      # container is stale no matter which of the two dates looks reassuring.
      if ((tag in TS) && TS[tag] != sha)
        note = "  <-- DRIFT: this tag now names a different image, built " substr(TC[tag], 1, 19)
      else if (when == "unknown")
        note = "  <-- its image is gone from the daemon"
      printf "%s\t%s\t%s%s\n", when, name, tag, note
    }' | sort -r | awk -F'\t' '{ printf "  %-19s  %-42s  %s\n", $1, $2, $3 }'
}

# --- smoke: lightweight ensure-up (reuse a warm stack; bring up only what's down) ---
# The fast per-PR gate must be ROBUST (don't false-fail if a service is down) WITHOUT
# the full bring_up_stack's churn. The key difference vs bring_up_stack: the e2e DBs
# come up with a PLAIN `up -d` (reuse if already healthy) instead of
# `--force-recreate --renew-anon-volumes`, which recreates the containers and RE-SEEDS
# the 100k-row big_table EVERY run (~20-40s). The smoke subset is the golden 3-row
# batch + one small CDC table — it never touches big_table, so that re-seed is pure
# wasted wall-clock on the PR critical path. `up -d` is a no-op for already-running,
# correctly-configured services, so on the warm dev runner this is near-instant; on a
# cold box it still brings everything up (first volume creation runs the seed once).
smoke_ensure_up() {
  log "Smoke: ensuring stack is up (reuse warm; no destructive recreate / re-seed)"
  # NB: do NOT redirect these to /dev/null — compose_up_selfheal prints the
  # compose stdout/stderr, and swallowing it hides the real bring-up error on
  # failure (a bare-container-name collision under an isolated STACK_PREFIX cost
  # a long blind investigation). On a warm reuse this is near-silent anyway.
  # planner is REQUIRED for batch pipelines (orchestrator -> http://planner:5011/plan).
  # Historically it persisted from a warm dev/staging stack; a cold isolated
  # rsync-ci stack has none, so a batch run failed at planning with
  # "lookup planner ... no such host". bring_up_stack already includes it (see
  # the planner block below) — the smoke path must too.
  compose_up_selfheal dc_main postgres redis kafka kafka-init schema-registry \
    temporal temporal-adapter kafka-connect debezium-mcp \
    api-gateway orchestrator planner || die "smoke core ensure-up failed"
  compose_up_selfheal dc_main kafka-mcp-sink-mcp minio-mcp minio || die "smoke sink/minio ensure-up failed"
  compose_up_selfheal dc_mcp "$(mcp_service_name mysql)" "$(mcp_service_name postgresql)" \
    || die "smoke mcp connectors ensure-up failed"
  compose_up_selfheal dc_e2e mysql-e2e postgres-e2e || die "smoke e2e DB ensure-up failed"
  dc_e2e  up -d --no-deps minio-init >/dev/null 2>&1 || warn "smoke minio-init returned non-zero (continuing)"
}

wait_healthy() {
  log "Waiting for stack health (api-gateway, e2e DBs, Kafka Connect, planner)"
  local i

  for i in $(seq 1 60); do
    curl -fsS "${API_GATEWAY_URL%/}/health" >/dev/null 2>&1 && break
    sleep 2
  done
  curl -fsS "${API_GATEWAY_URL%/}/health" >/dev/null 2>&1 \
    || die_diag "${STACK_PREFIX}-api-gateway" "api-gateway not healthy at ${API_GATEWAY_URL}"

  # MySQL needs 10-30s after container start to accept connections; poll like
  # every other dependency below (a single immediate ping races the daemon and
  # fails the gate at setup -- the omitted loop here was a real harness bug).
  for i in $(seq 1 60); do
    dc_e2e exec -T mysql-e2e mysqladmin ping -h 127.0.0.1 -uroot -prootpassword --silent >/dev/null 2>&1 && break
    sleep 2
  done
  dc_e2e exec -T mysql-e2e mysqladmin ping -h 127.0.0.1 -uroot -prootpassword --silent >/dev/null 2>&1 \
    || die_diag "${MYSQL_CONTAINER:-${STACK_PREFIX}-mysql-e2e}" "e2e mysql not ready"
  for i in $(seq 1 60); do
    dc_e2e exec -T postgres-e2e pg_isready -U e2e_user -d e2e_db >/dev/null 2>&1 && break
    sleep 2
  done
  dc_e2e exec -T postgres-e2e pg_isready -U e2e_user -d e2e_db >/dev/null 2>&1 \
    || die_diag "${PG_CONTAINER:-${STACK_PREFIX}-postgres-e2e}" "e2e postgres not ready"

  # CDC tests die immediately if Kafka Connect (Debezium) is not up. Debezium
  # takes the longest to accept REST traffic, so it gets the longest budget.
  for i in $(seq 1 90); do
    curl -fsS "${CONNECT_URL}/" >/dev/null 2>&1 && break
    sleep 2
  done
  curl -fsS "${CONNECT_URL}/" >/dev/null 2>&1 \
    || die_diag "${KAFKA_CONNECT_CONTAINER}" "Kafka Connect (Debezium) not healthy at ${CONNECT_URL}"

  # Every batch pipeline is planned before it runs, so a dead planner fails the
  # suite -- but it used to fail 30s INTO a test, as an opaque "Pipeline <uuid>
  # failed", with the real cause (connection refused to planner:5011) buried in
  # a 5KB JSON status blob. Waiting on it here turns that into a setup failure
  # that prints the planner's own traceback. It is a Python service behind a
  # heavy import graph, so it gets Debezium's budget rather than the DBs'.
  for i in $(seq 1 90); do
    curl -fsS "${PLANNER_URL%/}/health" >/dev/null 2>&1 && break
    sleep 2
  done
  curl -fsS "${PLANNER_URL%/}/health" >/dev/null 2>&1 \
    || die_diag "${STACK_PREFIX}-planner" "planner not healthy at ${PLANNER_URL}"

  log "Stack healthy."
}

# --- curated, deterministic, local-only test list ---------------------------
# Order matters: BATCH -> CDC -> CHAOS (chaos kills services; run last).
BATCH_TESTS=(
  test_golden_type_preservation_mysql_to_postgres_batch.sh
  test_mysql_batch_to_postgres_via_minio.sh
  test_batch_reload_resume.sh
  test_pg_nonpublic_schema_namespaced_batch.sh
  test_namespaced_batch_to_postgres.sh
  # Regression net for the PG-source non-public-schema silent-drop class fixed in
  # #230/#231 (resolveDestTableName). It was on disk but in NO bucket, so the
  # data-loss fix it guards has never actually been gated.
  test_pg_nonpublic_schema_to_postgres_batch.sh
)
CDC_TESTS=(
  test_mysql_cdc_to_postgres.sh
  test_mysql_cdc_hybrid_topics_to_postgres.sh
  test_mysql_cdc_namespace_to_postgres.sh
  test_delete_correctness_mysql_to_postgres_cdc.py
  test_delete_correctness_postgres_to_mysql_cdc.py
  test_type_fidelity_mysql_to_postgres_cdc.py
  test_type_fidelity_postgres_to_mysql_cdc.py
  test_schema_evolution_mysql_to_postgres_cdc.py
  test_schema_evolution_postgres_to_mysql_cdc.py
  test_mongodb_cdc_to_postgres.py
  test_postgres_to_mongodb_dest_cdc.py
  test_mysql_to_mongodb_dest_cdc.py
  test_sqlserver_cdc_to_postgres.py
  # Env-gated exactly like the sqlserver test above: exits 77 (-> SKIP, never
  # PASS) unless ORA_LIVE_HOST names a provisioned Oracle. Gating it costs
  # ~0.1s today and makes the lane live the moment Oracle is provisioned.
  test_oracle_cdc_to_postgres.py
)
CHAOS_TESTS=(
  test_chaos_dlq_down_fail_closed.py
  test_chaos_dest_down_dlq_healthy.py
  test_chaos_sink_worker_kill_auto_recovers.py
)
# OAUTH: opt-in, deterministic (mock-backed) generated-connector OAuth2
# token-refresh proof. NOT in the per-PR/nightly default suite -- appended to
# SELECTED only when E2E_INCLUDE_OAUTH=1 (nightly/on-demand), because it needs
# extra bring-up (mock-github + the github-rest MCP on the app net) and the
# OAuth overlays (INTERNAL_SERVICE_SECRET + MCP_ALLOW_PRIVATE_NETWORKS). Runs
# serially in SERIAL_TAIL. See the opt-in block at the end of bring_up_stack()
# and the two docker-compose.e2e.oauth.yml / docker-compose.mcp.e2e.yml overlays.
OAUTH_TESTS=(
  test_github_rest_oauth_refresh.py
)
# GRAPHQL: opt-in, deterministic (mock-backed) generated-GraphQL-connector
# pipeline proof. NOT in the per-PR/nightly default suite -- appended to SELECTED
# only when E2E_INCLUDE_GRAPHQL=1, because it needs extra bring-up (mock-graphql +
# the widgets-graphql MCP on the app net) and the private-net overlay
# (MCP_ALLOW_PRIVATE_NETWORKS). Runs serially in SERIAL_TAIL. See the opt-in block
# at the end of bring_up_stack() and the docker-compose.mcp.graphql.e2e.yml overlay.
GRAPHQL_TESTS=(
  test_graphql_connector_to_postgres_batch.py
)
# UNGATED: on disk, deliberately NOT run by this gate. One entry per file with a
# one-line reason. This list is not decoration -- check_test_coverage() below
# fails the gate when a test_*.{sh,py} in e2e/ is in neither a bucket above nor
# this list, so "forgot to add it" can no longer mean "never runs".
# (test_github_rest_oauth_refresh.py + test_graphql_connector_to_postgres_batch.py
#  are NOT here -- they are the opt-in OAUTH_TESTS / GRAPHQL_TESTS buckets above.)
UNGATED_TESTS=(
  # -- external network / real cloud credentials (would make the gate flaky) --
  test_mysql_cdc_to_real_s3.sh                 # real AWS S3 bucket + credentials
  test_blob_passthrough_objstore.sh            # real aws-s3 + azure-blob object stores
  test_shopify_to_postgres_batch.py            # live Shopify OAuth app
  test_github_rest_to_postgres_batch.py        # public GitHub API; nightly/runnable, not merge-blocking
  test_github_rest_to_s3_batch.py              # public GitHub API + object store; nightly/runnable

  # -- needs a service or emulator the gate stack does not bring up --
  test_clickhouse_batch_roundtrip.py           # needs a ClickHouse service (not in the gate compose)
  test_db_cdc_to_local_minio.sh                # needs the external-MinIO overlay bring-up
  test_db_cdc_to_emulated_gcs_azure.sh         # needs fake-gcs-server + Azurite emulator overlay
  test_pg_bulk_copy_parity.py                  # needs the copytest-pg fixture + a mounted edited connector.py
  test_pg_bulk_copy_merge.py                   # needs the copytest-pg fixture + RSYNC_PG_BULK_COPY overlay
  test_claim_check_gzip.py                     # needs the RSYNC_CLAIM_CHECK_GZIP opt-in overlay
  test_mysql_load_strategy.py                  # connector-source contract check; needs the mounted connector tree

  # -- non-deterministic: asserts on LLM output or wall-clock timing --
  test_pipeline_full.py                        # broad LLM-planner flow; not deterministic
  test_agentic_chat_pipeline_flow.py           # /chat LLM flow; HITL answers are best-effort, not deterministic
  test_response_accuracy.py                    # asserts chat-assistant wording against system state
  test_ui_response_accuracy.py                 # needs the frontend + browser; asserts rendered UI text
  test_performance_benchmarks.py               # wall-clock load/render timings; machine-dependent
  test_websocket_realtime.py                   # timing-sensitive websocket fan-out against localhost:5001

  # -- superseded: the product-level test covers the same hop --
  test_mysql_cdc_debezium.sh                   # raw Debezium/Kafka-Connect probe; test_mysql_cdc_to_postgres.sh gates the product path
)

# --- coverage guard ---------------------------------------------------------
# Every test_*.{sh,py} in e2e/ MUST be named in a run bucket or in UNGATED_TESTS.
# Adding a test file and forgetting to list it used to leave it silently unrun
# forever -- the same enumerate-by-name gap that hid four llm-service test files
# from CI until #784. This turns that omission into an immediate, named failure.
# It also catches the reverse (a listed name that no longer exists on disk),
# which otherwise only surfaces as a MISS line deep in a 40-minute run.
# Skipped under GATE_ONLY, which is the explicit "vet one test by hand" path.
check_test_coverage() {
  local known unclassified missing f
  known=$(printf '%s\n' \
    "${BATCH_TESTS[@]}" "${CDC_TESTS[@]}" "${CHAOS_TESTS[@]}" \
    "${OAUTH_TESTS[@]}" "${GRAPHQL_TESTS[@]}" "${SMOKE_TESTS[@]}" \
    "${UNGATED_TESTS[@]}" | sort -u)

  unclassified=""
  for f in "${E2E_DIR}"/test_*.sh "${E2E_DIR}"/test_*.py; do
    [[ -e "${f}" ]] || continue
    printf '%s\n' "${known}" | grep -qxF "${f##*/}" \
      || unclassified="${unclassified}  ${f##*/}"$'\n'
  done

  missing=""
  while IFS= read -r f; do
    [[ -n "${f}" ]] || continue
    [[ -f "${E2E_DIR}/${f}" ]] || missing="${missing}  ${f}"$'\n'
  done <<<"${known}"

  if [[ -n "${unclassified}" ]]; then
    printf 'GATE FAIL (coverage): e2e test file(s) named nowhere in run_gate.sh:\n%s' "${unclassified}" >&2
    printf 'Add each to a run bucket (BATCH/CDC/CHAOS/OAUTH/GRAPHQL_TESTS) or to\nUNGATED_TESTS with a one-line reason. An unlisted test never runs.\n' >&2
    exit 1
  fi
  if [[ -n "${missing}" ]]; then
    printf 'GATE FAIL (coverage): run_gate.sh names test file(s) that do not exist:\n%s' "${missing}" >&2
    printf 'Drop the stale entry (or restore the file) -- a named-but-absent test is\nonly reported MISS once the run reaches it.\n' >&2
    exit 1
  fi
  log "Coverage guard OK: all $(printf '%s\n' "${known}" | grep -c .) e2e test files are gated or explicitly ungated."
}

# SMOKE: minimal merge-blocking subset for the FAST per-PR gate (GATE_MODE=smoke).
# One batch (pipeline-API + table-selection HITL + executor + dest DDL/types) and
# one CDC (Debezium -> Kafka -> kafka-mcp-sink -> Postgres apply) -- together they
# cover both data-movement contracts. They land in different execution buckets
# (golden=BG_OVERLAP background, cdc=SERIAL_CHAIN) so at the default parallelism
# they OVERLAP -> wall-clock ~= the slower of the two. Run against the warm stack
# for a sub-10-min PR signal; the full batch + CDC[+chaos] matrix still gates
# post-merge + nightly.
SMOKE_TESTS=(
  test_golden_type_preservation_mysql_to_postgres_batch.sh
  test_mysql_cdc_to_postgres.sh
)

is_quarantined() {
  local name="$1" qfile="${E2E_DIR}/QUARANTINE.md"
  [[ -f "${qfile}" ]] || return 1
  # Lines after the `<!-- quarantine -->` marker, first whitespace token each.
  awk '/<!-- quarantine -->/{f=1; next} f && $1 !~ /^#/ {print $1}' "${qfile}" \
    | grep -qxF "${name}"
}

# run_one records its verdict to a per-test status file (PASS/FAIL/QUAR/MISS)
# rather than appending to a shared array, so it is safe to invoke in a
# background subshell for parallel execution. Results are aggregated from these
# files after all tests finish.
run_one() {
  local name="$1" path="${E2E_DIR}/$1" rc=0
  local statusf="${RESULTS_DIR}/${name}.status"
  if is_quarantined "${name}"; then
    printf '  SKIP (quarantined): %s\n' "${name}"; echo QUAR >"${statusf}"; return
  fi
  if [[ ! -f "${path}" ]]; then
    printf '  MISS (not found):   %s\n' "${name}"; echo MISS >"${statusf}"; return
  fi
  log "RUN ${name}"
  local logf="${RESULTS_DIR}/${name}.log"
  # Every test reuses the already-running stack.
  export E2E_SKIP_UP=1
  case "${name}" in
    *.sh) bash "${path}"   >"${logf}" 2>&1; rc=$? ;;
    *.py) python3 "${path}" >"${logf}" 2>&1; rc=$? ;;
    *)    warn "unknown test type: ${name}"; rc=2 ;;
  esac
  if [[ ${rc} -eq 77 ]]; then
    # rc=77 (autotools convention) = the test gated itself off because a required
    # env var is absent. Counting that as PASS is the trivy --exit-code 0 class of
    # false green: test_sqlserver_cdc_to_postgres.py "passed" in 0.077s every
    # night without ever connecting to a SQL Server.
    printf '  SKIP (env-gated): %s\n' "${name}"; echo SKIP >"${statusf}"
  elif [[ ${rc} -eq 0 ]]; then
    printf '  PASS: %s\n' "${name}"; echo PASS >"${statusf}"
  else
    printf '  FAIL (rc=%s): %s  (log: %s)\n' "${rc}" "${name}" "${logf}"
    tail -15 "${logf}" | sed 's/^/      | /'
    echo FAIL >"${statusf}"
  fi
}

# --- main -------------------------------------------------------------------
# Static consistency check FIRST: it costs milliseconds and needs no stack, so a
# misclassified test fails here rather than 40 minutes into a bring-up.
if [[ -z "${GATE_ONLY:-}" ]]; then
  check_test_coverage
fi

# GATE_MODE=smoke reuses the already-running (dev/staging) stack by default -- the
# fast per-PR path. The warm stack on this single self-hosted box IS the dev stack
# (project rsync-ai + rsync-ai-mcp); smoke just needs it healthy + the e2e DBs up.
# Pass E2E_SKIP_UP=0 to force a cold bring-up instead.
if [[ "${GATE_MODE:-full}" == "smoke" ]]; then
  # E2E_SKIP_UP=1 so smoke NEVER runs the heavy full bring_up_stack (which
  # --force-recreates the e2e DBs + re-seeds the 100k big_table every run). Instead:
  #   1) smoke_selective_build — rebuild+recreate ONLY the PR-changed images, so the
  #      smoke test exercises the PR's code (no-op for a test-script-only PR).
  #   2) smoke_ensure_up — bring up anything that's DOWN but REUSE a warm stack as-is.
  # The wiring guard + health wait below still run, so this stays robust.
  : "${E2E_SKIP_UP:=1}"
  smoke_selective_build
  smoke_ensure_up
  log_stack_image_provenance
fi

if [[ "${E2E_SKIP_UP:-}" != "1" ]]; then
  bring_up_stack
fi

# Reconcile shared-service wiring to LOCAL e2e/base BEFORE the health wait (and
# before any test). A prior manual staging run can leave kafka-mcp-sink/
# orchestrator/api-gateway pointed at Azure staging; --fix force-recreates them
# back to base. This MUST precede wait_healthy: an Azure-wired api-gateway can
# fail the localhost health probe, so healing the wiring first is what makes the
# probe meaningful. Symmetric to staging's Gate 0 (preflight-staging-runtime.sh).
# Also covers the E2E_SKIP_UP reuse path.
if [[ "${E2E_SKIP_WIRING_GUARD:-}" != "1" ]]; then
  log "Reconciling shared-service wiring for local e2e (preflight-e2e-runtime.sh --fix)"
  bash "${ROOT_DIR}/scripts/preflight-e2e-runtime.sh" --fix \
    || die "shared-service wiring could not be reconciled for local e2e (would write to the wrong DB)"
fi

if [[ "${E2E_SKIP_HEALTH:-}" != "1" ]]; then
  wait_healthy
else
  log "E2E_SKIP_HEALTH=1 -> skipping health wait (operator-asserted)"
fi

# RAM hygiene: clear leftover debug-* test connectors BEFORE running tests so the
# heaviest test (SERIAL_TAIL) starts on a kafka-connect that prior runs have not
# bloated. Each leftover connector pins connect heap + a live source CDC reader
# on the memory-constrained box. Best-effort; see reclaim_e2e_connectors().
reclaim_e2e_connectors
# Same idea for the connections TABLE: clear any rows older runs left in the
# shared pipeline_db (e.g. a crashed run that skipped on_exit teardown) so every
# gate -- and any manual staging session after it -- starts on a clean list.
reclaim_e2e_connection_rows

# Allow GATE_ONLY="t1 t2" to scope a run (e.g. when vetting a single test).
if [[ -n "${GATE_ONLY:-}" ]]; then
  SELECTED=(${GATE_ONLY})
elif [[ "${GATE_MODE:-full}" == "smoke" ]]; then
  # Fast per-PR gate: the minimal merge-blocking subset (one batch + one CDC),
  # run against the warm stack. The full matrix still gates post-merge + nightly.
  SELECTED=("${SMOKE_TESTS[@]}")
else
  # PR gate = batch + CDC correctness (the deterministic, merge-blocking surface).
  # Chaos tests kill services and wait for recovery within SLA — the slowest part
  # of the suite and not needed to gate every PR. The nightly schedule sets
  # E2E_INCLUDE_CHAOS=1 to run the full batch + CDC + chaos suite.
  SELECTED=("${BATCH_TESTS[@]}" "${CDC_TESTS[@]}")
  if [[ "${E2E_INCLUDE_CHAOS:-}" == "1" ]]; then
    SELECTED+=("${CHAOS_TESTS[@]}")
  fi
  if [[ "${E2E_INCLUDE_OAUTH:-}" == "1" ]]; then
    if python3 -c 'import pytest' 2>/dev/null; then
      SELECTED+=("${OAUTH_TESTS[@]}")
    else
      warn "E2E_INCLUDE_OAUTH=1 but python3 has no pytest module — skipping OAUTH_TESTS (pip install pytest to enable the OAuth lane)"
    fi
  fi
  if [[ "${E2E_INCLUDE_GRAPHQL:-}" == "1" ]]; then
    if python3 -c 'import pytest' 2>/dev/null; then
      SELECTED+=("${GRAPHQL_TESTS[@]}")
    else
      warn "E2E_INCLUDE_GRAPHQL=1 but python3 has no pytest module — skipping GRAPHQL_TESTS (pip install pytest to enable the GraphQL lane)"
    fi
  fi
fi

# --- bounded-parallel execution --------------------------------------------
# Only tests VERIFIED to use uuid/timestamp-unique resource names (pipeline_id,
# consumer_group, source/dest table, topic, connector prefix) may run
# concurrently. Everything else stays serial. Buckets:
#   BG_OVERLAP   long batch test(s) (golden) — orchestrator/MinIO path, no
#                Debezium, fully name-isolated; run in the background so their
#                ~160s overlaps the whole CDC phase instead of adding to it.
#   SERIAL_CHAIN the two .sh CDC tests use FIXED names (cdc_test/cdc_test_dest,
#                UNIFIED_TOPIC) so they must never overlap each other; ordered,
#                and the first also warms Debezium before the pool fans out.
#   POOL         uuid/ts-isolated python CDC tests — run at E2E_PARALLELISM.
#   SERIAL_TAIL  chaos (kills shared services) + anything unrecognized — serial,
#                last. Unknown tests default here so new tests are safe-by-default.
# Set E2E_PARALLELISM=1 to force the original fully-serial behaviour. Default is 2
# (was 3): on the memory-constrained self-hosted runner, golden-batch(bg)+3 POOL
# CDC tests overlapping the base stack peaked high enough to OOM-kill core services
# (api-gateway/postgres-e2e), cascading the serial heavy-batch tail into red.
PARALLELISM="${E2E_PARALLELISM:-2}"
BG_OVERLAP=(); SERIAL_CHAIN=(); POOL=(); SERIAL_TAIL=()
for t in "${SELECTED[@]}"; do
  case "${t}" in
    test_golden_type_preservation_mysql_to_postgres_batch.sh) BG_OVERLAP+=("${t}") ;;
    test_mysql_cdc_to_postgres.sh|test_mysql_cdc_hybrid_topics_to_postgres.sh|test_mysql_cdc_namespace_to_postgres.sh) SERIAL_CHAIN+=("${t}") ;;
    test_delete_correctness_*_cdc.py|test_type_fidelity_*_cdc.py|test_schema_evolution_*_cdc.py) POOL+=("${t}") ;;
    *) SERIAL_TAIL+=("${t}") ;;
  esac
done

log "Gate suite: ${#SELECTED[@]} test(s) (mode=${GATE_MODE:-full}, parallelism=${PARALLELISM}). Results dir: ${RESULTS_DIR}"

if [[ "${PARALLELISM}" -le 1 ]]; then
  for t in "${SELECTED[@]}"; do run_one "${t}"; done
else
  # NB: arrays are expanded with the ${arr[@]+"${arr[@]}"} idiom so an EMPTY
  # bucket does not trip `set -u` (unbound variable) on bash 3.2.
  bg_pids=()
  for t in ${BG_OVERLAP[@]+"${BG_OVERLAP[@]}"}; do run_one "${t}" & bg_pids+=($!); done   # 1. background batch
  for t in ${SERIAL_CHAIN[@]+"${SERIAL_CHAIN[@]}"}; do run_one "${t}"; done               # 2. ordered .sh CDC (warms Debezium)
  wave=()                                                                                  # 3. isolated CDC, bounded waves
  for t in ${POOL[@]+"${POOL[@]}"}; do
    run_one "${t}" & wave+=($!)
    if (( ${#wave[@]} >= PARALLELISM )); then wait "${wave[@]}"; wave=(); fi
  done
  (( ${#wave[@]} > 0 )) && wait "${wave[@]}"
  (( ${#bg_pids[@]} > 0 )) && wait "${bg_pids[@]}"                                          # 4. join background batch
  # De-load before the heaviest serial-tail tests: the POOL .py CDC tests leave
  # their connectors registered (no on-exit delete) and nothing else runs past
  # the barrier above, so reclaim now → the heaviest test (e.g. the namespaced
  # batch) starts on a clean connect cluster, not one bloated by this run's pool.
  reclaim_e2e_connectors
  # Core-service health-gate before the heavy serial-tail batch tests. The
  # background-batch + POOL peak can OOM-kill api-gateway/orchestrator/postgres-e2e
  # on a memory-starved self-hosted runner; the serial heavy-batch tail would then
  # hit a dead :5001 ("connection refused") or "postgres not ready" and cascade-fail
  # (observed post-merge 2026-06-21). RAM has freed after the barriers above, so a
  # plain `up -d` revives any OOM-killed core service (no-op if already healthy);
  # then re-probe. Non-fatal: if still unhealthy the tail runs and fails naturally,
  # preserving the per-test summary. Honors E2E_SKIP_HEALTH for operator-asserted skips.
  if [[ "${E2E_SKIP_HEALTH:-}" != "1" ]] && (( ${#SERIAL_TAIL[@]} > 0 )); then
    log "Re-ensuring core services healthy before heavy serial-tail tests"
    dc_main up -d api-gateway orchestrator kafka-mcp-sink-mcp minio-mcp minio >/dev/null 2>&1 || true
    dc_e2e  up -d postgres-e2e mysql-e2e >/dev/null 2>&1 || true
    for _ in $(seq 1 30); do
      curl -fsS "${API_GATEWAY_URL%/}/health" >/dev/null 2>&1 \
        && dc_e2e exec -T postgres-e2e pg_isready -U e2e_user -d e2e_db >/dev/null 2>&1 \
        && { log "Core services healthy before serial-tail."; break; }
      sleep 2
    done
  fi
  for t in ${SERIAL_TAIL[@]+"${SERIAL_TAIL[@]}"}; do run_one "${t}"; done                  # 5. chaos / unknown, serial last
fi

# --- aggregate verdicts from per-test status files --------------------------
PASS=(); FAIL=(); QUAR=(); SKIP=()
for t in "${SELECTED[@]}"; do
  s="$(cat "${RESULTS_DIR}/${t}.status" 2>/dev/null || echo MISS)"
  case "${s}" in
    PASS) PASS+=("${t}") ;;
    SKIP) SKIP+=("${t}") ;;
    QUAR) QUAR+=("${t}") ;;
    MISS) FAIL+=("${t} [missing]") ;;
    *)    FAIL+=("${t}") ;;
  esac
done

# --- summary ----------------------------------------------------------------
echo
echo "================ DATA-PIPELINE GATE SUMMARY ================"
printf '  PASS:       %s\n' "${#PASS[@]}"
printf '  FAIL:       %s\n' "${#FAIL[@]}"
printf '  SKIP:       %s\n' "${#SKIP[@]}"
[[ ${#SKIP[@]} -gt 0 ]] && printf '    - %s (env-gated, never ran)\n' "${SKIP[@]}"
printf '  QUARANTINE: %s\n' "${#QUAR[@]}"
[[ ${#QUAR[@]} -gt 0 ]] && printf '    - %s\n' "${QUAR[@]}"
if [[ ${#FAIL[@]} -gt 0 ]]; then
  echo "  FAILED TESTS:"
  printf '    - %s\n' "${FAIL[@]}"
  echo "==========================================================="
  echo "GATE RESULT: FAIL"
  exit 1
fi
echo "==========================================================="
echo "GATE RESULT: PASS"
exit 0
