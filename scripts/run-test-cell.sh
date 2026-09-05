#!/usr/bin/env bash
# scripts/run-test-cell.sh
#
# Multi-agent test cell launcher. Encapsulates the Driver + Reviewer
# (optionally + Fixer) pattern that proved high-signal in PR #61 / #62
# into one reproducible command.
#
# A "test cell" is one independent investigation into a feature path
# (e.g. "Shopify→PG via API", "Tool generator wizard for Linear",
# "PG→MySQL CDC"). Each cell produces:
#
#   .test-evidence/<cell>-driver-journal.md     — Driver's evidence trail
#   .test-evidence/<cell>-reviewer-issues.md    — Reviewer's cold-read findings
#   .test-evidence/<cell>-fixer-summary.md      — (optional) Fixer's diff summary
#
# Multiple cells can be launched in parallel from separate terminals.
# Cells targeting disjoint code paths can also have their Fixer agents
# run concurrently; cells touching the same files MUST run Fixers
# sequentially (last-writer-wins on the worktree).
#
# Usage:
#   scripts/run-test-cell.sh <cell-name> --target "<one-line description>" \
#       [--surface ui|api|both] [--with-fixer]
#
# Example:
#   scripts/run-test-cell.sh shopify-pg-api \
#       --target "Shopify → PostgreSQL via the api-gateway REST surface (no browser)" \
#       --surface api
#
# What it does:
#   1. Spawns a Driver agent with a self-contained brief targeting the
#      named cell. Driver writes its journal during execution.
#   2. After Driver completes, spawns a Reviewer agent that has NO
#      access to the conversation — only the journal file + live repo
#      state. Reviewer writes an independent issues doc.
#   3. (Optional, --with-fixer) Spawns a Fixer agent that reads the
#      Reviewer's issues doc and applies code patches, one commit per
#      cell. Defaults to OFF — fixes are usually orchestrated by a
#      human after triage.
#
# Design notes:
#   - Driver does NOT see the Reviewer's prompt or issues doc — keeps
#     the Reviewer cold.
#   - Reviewer is told to skeptically verify every Driver claim with
#     independent commands, and is allowed to invalidate Driver claims.
#   - All artefacts land under `.test-evidence/` which is `merge=union`
#     in .gitattributes — concurrent cells writing different files
#     never conflict.

set -euo pipefail

CELL=""
TARGET=""
SURFACE="both"
WITH_FIXER=0

while [ $# -gt 0 ]; do
  case "$1" in
    --target)      TARGET="$2"; shift 2 ;;
    --surface)     SURFACE="$2"; shift 2 ;;
    --with-fixer)  WITH_FIXER=1; shift ;;
    -h|--help)
      sed -n '2,50p' "$0" >&2
      exit 0
      ;;
    -*)
      echo "unknown flag: $1" >&2; exit 1 ;;
    *)
      if [ -z "$CELL" ]; then CELL="$1"; shift
      else echo "unexpected positional: $1" >&2; exit 1
      fi ;;
  esac
done

[ -n "$CELL" ]   || { echo "error: missing <cell-name>" >&2; exit 1; }
[ -n "$TARGET" ] || { echo "error: --target required" >&2; exit 1; }
case "$SURFACE" in ui|api|both) ;; *) echo "error: --surface must be ui|api|both" >&2; exit 1;; esac

mkdir -p .test-evidence
JOURNAL=".test-evidence/${CELL}-driver-journal.md"
ISSUES=".test-evidence/${CELL}-reviewer-issues.md"
FIXER=".test-evidence/${CELL}-fixer-summary.md"

cat >&2 <<EOF
[test-cell] cell=${CELL}
[test-cell]   target:    ${TARGET}
[test-cell]   surface:   ${SURFACE}
[test-cell]   journal:   ${JOURNAL}
[test-cell]   issues:    ${ISSUES}
[test-cell]   with-fixer: $( [ $WITH_FIXER -eq 1 ] && echo yes || echo no )
EOF

if ! command -v claude >/dev/null 2>&1; then
  cat >&2 <<EOF

[test-cell] this script is intended to be invoked from inside an active
[test-cell] Claude Code session — it prints the prompts to launch each
[test-cell] agent and the human (or the parent agent) pastes them into
[test-cell] the Agent tool. Direct CLI execution is not yet supported.

EOF
fi

cat <<EOF

────────────────────────────────────────────────────────────────────────
 DRIVER agent prompt (paste into Agent({subagent_type:'general-purpose'}))
────────────────────────────────────────────────────────────────────────
You are the Driver agent for test cell "${CELL}".

Working dir: $(pwd)
Branch:      $(git rev-parse --abbrev-ref HEAD)

**Target:** ${TARGET}

**Surface:** ${SURFACE}

**Your job:**
1. Drive the target end-to-end. Use real data, no stubs or mocks.
2. Capture every step in ${JOURNAL} — command, output snippet, ✅/⚠️/❌.
3. For each issue you hit, add a "Finding F-NN" section: symptom +
   suspected file:line + proposed minimal fix.
4. Do NOT fix anything — Reviewer will independently verify your
   findings before any code changes land.
5. Redact secrets (access tokens, passwords) in captured output.

**Constraints:**
- For UI surface: use mcp__Claude_Preview__preview_* tools on the
  active preview server.
- For API surface: use curl against http://localhost:5001 and
  scripts/e2e-shopify-pg.sh as reference.
- Time budget: 20 minutes. If blocked, document the blocker and move
  on — Reviewer can re-confirm.
- Report length: ≤300 lines in the journal.

End by writing ${JOURNAL} and printing the final 'Headline' sentence.

────────────────────────────────────────────────────────────────────────
 REVIEWER agent prompt (paste AFTER Driver completes)
────────────────────────────────────────────────────────────────────────
You are a **cold Reviewer** for test cell "${CELL}". You have NOT seen
the Driver's conversation. Your only inputs are:

  ${JOURNAL}  (the Driver's evidence trail)
  the live state of this repo and docker stack

**Your job:**
1. Read ${JOURNAL} once, end to end.
2. For each "Finding F-NN" in the journal: independently reproduce
   the symptom with your own commands. Mark VALIDATED / INVALIDATED /
   PARTIAL with the exact command output you saw.
3. For each fix proposal in the journal: read the cited file:line.
   If the proposal is wrong, off-by-one, or misses an edge case,
   flag it.
4. Look for issues the Driver missed. Skim the touched files for
   related smells.
5. Write ${ISSUES} with: validated findings table, invalidated
   findings list, additional issues, recommended fix order.

**Constraints:**
- READ-ONLY on source code. No edits.
- Skepticism mandate: every Driver claim is a hypothesis until you
  reproduce it.
- Report length: ≤400 lines.

End by writing ${ISSUES} and printing the final 'Verdict' sentence.

EOF

if [ $WITH_FIXER -eq 1 ]; then
  cat <<EOF
────────────────────────────────────────────────────────────────────────
 FIXER agent prompt (paste AFTER Reviewer completes)
────────────────────────────────────────────────────────────────────────
You are the Fixer agent for test cell "${CELL}".

Working dir: $(pwd)
Branch:      $(git rev-parse --abbrev-ref HEAD)

**Inputs:**
  ${ISSUES}  (cold Reviewer's validated findings)

**Your job:**
1. For each VALIDATED finding in ${ISSUES} with a high-confidence
   proposed fix: apply the patch. Single commit per logical fix.
2. Skip findings marked INVALIDATED or PARTIAL — they need more
   investigation.
3. Skip findings outside this cell's scope — the Reviewer's
   "Recommended fix order" lists what's in vs out.
4. After all patches: \`go build ./...\` (or equivalent for non-Go
   files) must pass. \`docker compose build <service>\` for the
   affected service, then a focused live re-test of the originally
   failing symptom.
5. Write ${FIXER} listing: each finding fixed, file:line of patch,
   verification command output proving the symptom is gone.

**Constraints:**
- Do NOT touch files outside the Reviewer's "in scope" list.
- Each commit message starts with the finding ID: \`fix: F-NN ...\`
- Do NOT push — caller pushes after reviewing the worktree state.

End by writing ${FIXER} and printing 'N findings fixed, K skipped'.

EOF
fi
