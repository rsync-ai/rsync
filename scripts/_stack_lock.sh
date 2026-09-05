#!/usr/bin/env bash
# _stack_lock.sh — advisory mutex for the SINGLE shared `rsync-ai` Docker stack.
#
# Why: the e2e gate (e2e/run_gate.sh) and manual staging testing
# (scripts/staging-up.sh) both drive compose project `rsync-ai` with the same
# hard-pinned container_names. Run at the same time on one host they clobber each
# other's wiring (the fk_batch_acks_pipeline class). We keep them SERIALIZED
# rather than isolating into a second stack (the box can't fit two — see the
# e2e plan). This file makes "serialized" ENFORCED, not assumed.
#
# Source it, then `acquire_stack_lock "<owner>"` early and `release_stack_lock`
# in an EXIT trap. macOS ships no flock(1), so this is an atomic mkdir mutex with
# PID-staleness reclaim — portable across the macOS runner and Linux.
#
# Tunables (env): STACK_LOCK_DIR (default /tmp/rsync-ai-stack.lock),
# STACK_LOCK_TIMEOUT seconds to wait before giving up (default 300; 0 = forever).

STACK_LOCK_DIR="${STACK_LOCK_DIR:-/tmp/rsync-ai-stack.lock}"

acquire_stack_lock() {
  local who="${1:-$$}" timeout="${STACK_LOCK_TIMEOUT:-300}" waited=0
  while ! mkdir "$STACK_LOCK_DIR" 2>/dev/null; do
    # Reclaim a lock whose holder PID is gone (crash / kill -9).
    local holder; holder="$(cat "$STACK_LOCK_DIR/pid" 2>/dev/null || true)"
    if [[ -n "$holder" ]] && ! kill -0 "$holder" 2>/dev/null; then
      echo "stack lock held by dead PID $holder — reclaiming" >&2
      rm -rf "$STACK_LOCK_DIR"; continue
    fi
    if [[ "$timeout" -gt 0 && "$waited" -ge "$timeout" ]]; then
      echo "🛑 could not acquire the shared rsync-ai stack lock after ${timeout}s" >&2
      echo "   held by: '$(cat "$STACK_LOCK_DIR/owner" 2>/dev/null || echo unknown)' (PID ${holder:-?})" >&2
      echo "   the e2e gate and staging cannot drive project rsync-ai at the same time." >&2
      return 1
    fi
    [[ "$waited" -eq 0 ]] && \
      echo "waiting for shared rsync-ai stack lock (held by '$(cat "$STACK_LOCK_DIR/owner" 2>/dev/null || echo unknown)')…" >&2
    sleep 2; waited=$((waited + 2))
  done
  echo "$$"  > "$STACK_LOCK_DIR/pid"
  echo "$who" > "$STACK_LOCK_DIR/owner"
}

release_stack_lock() {
  # Only the owning PID may remove it (avoids a late trap nuking a new holder).
  [[ "$(cat "$STACK_LOCK_DIR/pid" 2>/dev/null || true)" == "$$" ]] && rm -rf "$STACK_LOCK_DIR"
  return 0
}

# --- persistent staging-ownership marker ------------------------------------
# The mkdir mutex above lasts only a PROCESS's lifetime (it is PID-reclaimed the
# moment the holder exits), so it CANNOT express "staging is up and owns the
# shared stack" AFTER scripts/staging-up.sh returns and the staging stack runs
# detached. That gap is the clobber bug: a later e2e gate finds the mutex free,
# takes it, and reconciles project `rsync-ai` back to LOCAL wiring
# (preflight-e2e-runtime.sh --fix) — silently overwriting the detached staging
# stack's Azure wiring. This durable marker closes the gap: staging-up.sh SETS it
# (it survives the process), run_gate.sh REFUSES to touch the stack while it is
# set, and scripts/staging-down.sh CLEARS it. Advisory, like the mutex — the
# blessed release path is scripts/staging-down.sh (or STAGING_HOLD_OVERRIDE=1).
STAGING_HOLD_FILE="${STAGING_HOLD_FILE:-/tmp/rsync-ai-stack.staging-hold}"

set_staging_hold() {
  local note="${1:-staging}"
  {
    echo "owner=staging"
    echo "note=${note}"
    echo "pid=$$"
    echo "since=$(date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo unknown)"
    echo "host=$(hostname 2>/dev/null || echo unknown)"
  } > "$STAGING_HOLD_FILE" 2>/dev/null || true
}

clear_staging_hold() { rm -f "$STAGING_HOLD_FILE" 2>/dev/null || true; }

staging_hold_active() { [[ -f "$STAGING_HOLD_FILE" ]]; }

# One-line summary of the marker for log/error messages (empty if absent).
staging_hold_info() { tr '\n' ' ' < "$STAGING_HOLD_FILE" 2>/dev/null || true; }
