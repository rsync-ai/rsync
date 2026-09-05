#!/usr/bin/env bash
# preflight-prod-config.sh — verify rendered prod compose config BEFORE go-live.
#
# Why this exists:
#   staging kept passing while prod failed because nobody DIFFED the two
#   rendered configs. The base docker-compose.yml hardcodes some connection
#   strings with sslmode=disable (correct for the local dev `postgres`
#   container). Prod points those at Azure PostgreSQL, which REQUIRES TLS. If a
#   single service's URL is left at sslmode=disable on prod, that service can't
#   reach the DB — and the failure is often silent (crash-loop, 0 rows landed).
#   This script renders the actual prod config and FAILS LOUDLY if any DB /
#   connector connection string is not sslmode=require, so you catch drift
#   before deploying instead of from a failed pipeline.
#
# Usage (run from repo root, on a host that has .env.prod):
#   scripts/preflight-prod-config.sh
#
# Exit codes: 0 = safe to deploy · 1 = drift found (DO NOT deploy) · 2 = setup error.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

ENV_FILE="${ENV_FILE:-.env.prod}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: $ENV_FILE not found — run this on the prod VM (or set ENV_FILE=...)." >&2
  exit 2
fi

# DB_SSLMODE is the canonical knob; prod must set it to require (Azure needs TLS).
DB_SSLMODE_VAL="$(grep -E '^DB_SSLMODE=' "$ENV_FILE" | tail -1 | cut -d= -f2- || true)"
if [[ "$DB_SSLMODE_VAL" != "require" ]]; then
  echo "❌ $ENV_FILE has DB_SSLMODE='$DB_SSLMODE_VAL' (expected 'require'). Azure PostgreSQL rejects non-TLS connections." >&2
  exit 1
fi

# Temporal reads NEITHER DB_SSLMODE nor any sslmode= in a DSN. It has its own
# switch (POSTGRES_TLS_ENABLED -> SQL_TLS / SQL_TLS_ENABLED), and it is really
# two programs -- the schema tool and the server -- that this one key feeds
# together. So DB_SSLMODE=require alone is HALF-ON: every Go service gets TLS
# while Temporal's metadata connection carries the database password in the
# clear, and nothing errors. The compose defaults are TLS-on, so an ABSENT key
# is correct; only an explicit false is the half-on state.
TLS_VAL="$(grep -E '^POSTGRES_TLS_ENABLED=' "$ENV_FILE" | tail -1 | cut -d= -f2- || true)"
if [[ -n "$TLS_VAL" && "$TLS_VAL" != "true" ]]; then
  echo "❌ $ENV_FILE has DB_SSLMODE=require but POSTGRES_TLS_ENABLED='$TLS_VAL'." >&2
  echo "     Half-on: Temporal would reach the metadata DB in PLAINTEXT while every" >&2
  echo "     other service uses TLS, and the deploy would look healthy. Set it to" >&2
  echo "     'true', or delete the line to inherit the TLS-on compose default." >&2
  exit 1
fi

echo "→ Rendering prod compose config (this resolves all \${VAR} interpolation)…"
RENDERED="$(docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file "$ENV_FILE" config 2>/dev/null)" || {
  echo "ERROR: 'docker compose config' failed — fix compose/env errors first." >&2
  exit 2
}

# ── Positive control on the render, BEFORE anything is asserted about it ─────
# Checks 1-3 below are ABSENCE assertions ("no rendered URL says sslmode=
# disable"), and an absence assertion over an empty string passes. The render
# is captured with `2>/dev/null`, so a compose failure that still exits 0, or a
# short read, would reach those checks as the empty set and this script would
# print "Safe to deploy" having proved nothing -- the same empty-set-reads-as-a-
# pass this repo keeps paying for. So measure the denominator first and refuse a
# zero. Both figures are derived from the render itself rather than hard-coded,
# except the >10 service floor, which is the established shape here: a 24-service
# project must never be believed to have rendered 0. The `/^[A-Za-z]/{f=0}` reset
# matters -- without it the walk runs on past `services:` into the top-level
# `networks:`/`volumes:` blocks, whose children are indented the same two spaces,
# and reports 51. A denominator that is silently too large is a weaker floor, not
# a stronger one; this one is cross-checked against `config --services | wc -l`.
SVC_COUNT="$(awk '/^services:/{f=1;next} /^[A-Za-z]/{f=0} f && /^  [A-Za-z0-9_.-]+:/{n++} END{print n+0}' <<< "$RENDERED")"
DSN_COUNT="$( { grep -oE 'postgres(ql)?://[^[:space:]"]+' <<< "$RENDERED" || true; } | wc -l | tr -d ' ')"
if [[ "$SVC_COUNT" -le 10 || "$DSN_COUNT" -lt 1 ]]; then
  echo "ERROR: the render is not a usable subject — $SVC_COUNT services, $DSN_COUNT Postgres URLs." >&2
  echo "     Every TLS check below is an absence assertion and would pass vacuously" >&2
  echo "     on this input. Re-run the render without 2>/dev/null to see why." >&2
  exit 2
fi
echo "   render: $SVC_COUNT services, $DSN_COUNT Postgres URLs to check."

fail=0

# 1) No Postgres connection string may use sslmode=disable in the prod render.
#    Catches POSTGRES_URL / DATABASE_URL on every app service AND the kafka sink ledger.
if grep -nE 'postgres(ql)?://[^[:space:]]*sslmode=disable' <<< "$RENDERED" >/dev/null; then
  echo "❌ DRIFT: a Postgres URL is rendered with sslmode=disable on prod:" >&2
  grep -nE 'postgres(ql)?://[^[:space:]]*sslmode=disable' <<< "$RENDERED" | sed 's/^/     /' >&2
  fail=1
fi

# 2) Specifically assert the kafka-mcp-sink ledger URL is sslmode=require.
#    (This is the exact line that crash-looped prod when it drifted.)
SINK_URL="$(grep -E 'POSTGRES_URL=postgres' <<< "$RENDERED" | grep -i 'sslmode' | head -1 || true)"
if [[ -n "$SINK_URL" ]] && ! echo "$SINK_URL" | grep -q 'sslmode=require'; then
  echo "❌ DRIFT: kafka-mcp-sink POSTGRES_URL is not sslmode=require:" >&2
  echo "     $SINK_URL" >&2
  fail=1
fi

# 3) Every rendered Postgres URL that has an sslmode must be =require (belt & suspenders).
while IFS= read -r line; do
  if grep -qE 'sslmode=' <<< "$line" && ! grep -q 'sslmode=require' <<< "$line"; then
    echo "❌ DRIFT: non-require sslmode on a Postgres URL:" >&2
    echo "     $line" >&2
    fail=1
  fi
done < <(grep -oE 'postgres(ql)?://[^[:space:]"]+' <<< "$RENDERED" || true)

# 4) SEC-H-02 (#643) regression guard. tool-generator dropped root and runs as
#    uid 1000, but it still WRITES generated connectors into ./shared/mcp-connectors
#    (public/<slug>/versions/… + oauth/providers.json). On a root-owned checkout
#    (prod = /root/rsync-ai) that tree is root:… 0755, so uid 1000 can't create
#    public/<slug> and connector GENERATION fails with PermissionError — the exact
#    regression found on prod 2026-07-20. The connector-fs-init one-shot chowns the
#    tree to 1000 before tool-generator starts; assert it survives in the render so
#    it can't be silently dropped from a future compose edit.
if ! grep -q 'container_name: rsync-ai-connector-fs-init' <<< "$RENDERED"; then
  echo "❌ MISSING: connector-fs-init one-shot absent from the prod render." >&2
  echo "     Without it, connector generation fails as uid 1000 on a root-owned" >&2
  echo "     checkout (SEC-H-02 #643 regression, 2026-07-20). Restore the service" >&2
  echo "     in docker-compose.yml + tool-generator's depends_on gate on it." >&2
  fail=1
fi

if [[ "$fail" -ne 0 ]]; then
  echo >&2
  echo "🛑 Config preflight FAILED — do NOT deploy. Fix the drift above first." >&2
  exit 1
fi

echo "✅ Config preflight passed: all Postgres connection strings render with sslmode=require,"
echo "   and the connector-fs-init one-shot is wired (connector generation writable as uid 1000)."
echo "   Safe to deploy."
