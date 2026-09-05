#!/usr/bin/env bash
# Say, inside the run, which SARIF destinations actually took the findings.
#
# usage: report-sarif-sinks.sh <label> <sarif-file>
#   env: CODE_SCANNING_OUTCOME, ARCHIVE_OUTCOME  (steps.<id>.outcome, see below)
#
# A reporting job has two MACHINE-READABLE destinations for its SARIF: code
# scanning (`codeql-action/upload-sarif`) and the `Archive SARIF` artifact. Both
# are `continue-on-error: true`, and on 2026-08-24/25 both were dead at once --
# GHAS off on this private repo, artifact quota exhausted -- while the workflow
# concluded success. Nothing in the run said so. You had to open the raw log of a
# step whose API `conclusion` had been rewritten to `success`, which is what
# security.yml's header used to tell you to do by hand.
#
# This script is that missing sentence, automated.
#
# It is NOT a third sink. `scripts/summarize-sarif.sh` already writes a
# rule-level tally (level / ruleId / count, capped at 40 rules) into the job
# summary -- readable, but no file, no line, no message, so nobody can triage
# from it. This script reports on DELIVERY, not findings.
#
# ONCE IT HAS ITS TWO ARGUMENTS, NO DELIVERY VERDICT AND NO RUNNER CONDITION CAN
# MAKE IT EXIT NON-ZERO. The reporting tier must not block (security.yml's gate
# policy), and a red `gosec (api-gateway)` reads as "gosec found something" --
# which it cannot, gosec runs `-no-fail`. Letting a storage quota redden these jobs
# is exactly the misread #868 removed; re-adding it here under a new step name
# would undo that. Loud-but-green is the deliberate middle: visible without
# impersonating a security finding. (Nothing survives the runner killing the step
# or closing its output; that is outside any script's reach, and is not claimed.)
#
# That is narrower than the claim this header used to make -- a blanket promise to
# exit 0 in all cases, deleted rather than quoted here because a false sentence
# reproduced for context greps exactly like a live one. It was false twice:
#   * under `set -euo pipefail` an UNGUARDED `>> "$GITHUB_STEP_SUMMARY"` aborts the
#     script non-zero when that file is unwritable or the runner disk is full
#     (measured: rc=1, "Permission denied"). An infrastructure condition wearing a
#     security finding's clothes -- the precise misread this script exists to
#     prevent, committed by the script itself. The append is now guarded and falls
#     back to a log line, so the verdict is not silently lost either. Guarded by
#     `test_sink_reporter_survives_an_unwritable_step_summary`.
#   * a USAGE error -- called without the SARIF path -- still exits non-zero, on
#     purpose. That is a workflow wiring bug, not a runner condition, and
#     `test_sink_reporter_requires_the_sarif_path` asserts it stays that way.
#
# THREE THINGS IT REFUSES TO GUESS, each of which is a way this kind of reporter
# lies:
#
#   1. A CANCELLED run is not a delivery failure. security.yml sets
#      `cancel-in-progress: true`, so every second push to a PR branch cancels the
#      run in flight. A step that annotates `::error` on cancellation would stamp
#      six jobs with a red security annotation blaming billing, on a routine
#      event. The workflow gates the calling step with `!cancelled()` so this
#      never runs then; this script ALSO treats a literal `cancelled` outcome as
#      quiet, so restoring `always()` upstream cannot resurrect the false alarm.
#
#   2. An outcome that is EMPTY or `skipped` is "not attempted", never "failed".
#      A step skipped by its `if:` and a step whose `id:` was never wired both
#      arrive here as one of those two spellings, and neither is a quota problem.
#      Reporting "the artifact copy failed (unset) -- check the account artifact
#      quota" in the steady state would point every reader at the wrong cause.
#
#   3. A SUCCESSFUL upload of NOTHING is not a copy. `actions/upload-artifact`
#      runs with `if-no-files-found: ignore`, which succeeds trivially when the
#      scanner crashed and no SARIF exists -- so `outcome == success` alone would
#      certify a copy that contains zero bytes. That is this repo's documented "an
#      empty set reads as a pass" class, and it is why this takes the SARIF path
#      as an argument instead of trusting the step outcome on its own.
set -euo pipefail

LABEL=${1:?usage: report-sarif-sinks.sh <label> <sarif-file>}
SARIF=${2:?usage: report-sarif-sinks.sh <label> <sarif-file>}

# `outcome`, never `conclusion`. `continue-on-error: true` rewrites `conclusion`
# to `success` and leaves `outcome` at the truth. security.yml's header warns
# that the REST API's conclusion for these steps will lie to you; this is the
# machine-readable half of that warning, so it must read the field that does not.
# `${VAR-}` (no colon) so an unset variable and an empty one land in the same
# bucket -- both mean "no outcome reached this script", neither means success.
CODE_SCANNING=${CODE_SCANNING_OUTCOME-}
ARCHIVE=${ARCHIVE_OUTCOME-}

# delivered | absent | cancelled | refused
classify() {
  case "$1" in
    success)     printf 'delivered' ;;
    ""|skipped)  printf 'absent' ;;
    cancelled)   printf 'cancelled' ;;
    *)           printf 'refused' ;;
  esac
}

# `set -e` + an `a && b` one-liner is a trap: when the test is false the list
# returns 1. Spell both of these as `if` so a false branch can never abort the
# script -- a reporter that exits early reports nothing, silently.
show() {
  if [ -n "$1" ]; then printf '%s' "$1"; else printf '(empty)'; fi
}

# One sink's state in words. Used only by the cancellation branch, which must not
# describe a sink that succeeded, refused, or was never attempted as "cancelled".
# $1 = classified state, $2 = the raw outcome as shown.
sink_phrase() {
  case "$1" in
    delivered) printf 'took it (`%s`)' "$2" ;;
    absent)    printf 'was not attempted (`%s`)' "$2" ;;
    cancelled) printf 'was cancelled' ;;
    *)         printf 'did not take it (`%s`)' "$2" ;;
  esac
}

cs_state=$(classify "$CODE_SCANNING")
ar_state=$(classify "$ARCHIVE")
cs_shown=$(show "$CODE_SCANNING")
ar_shown=$(show "$ARCHIVE")

# Did the scanner leave anything to deliver at all? `-s`: present AND non-empty.
# A zero-byte file still "uploads" and still carries no findings.
sarif_present=yes
[ -s "$SARIF" ] || sarif_present=no

annotation=""
verdict=""

if [ "$cs_state" = cancelled ] || [ "$ar_state" = cancelled ]; then
  # See refusal 1. Quiet on purpose: no verdict can be drawn about a step that did
  # not finish, and a cancellation is not a security condition.
  #
  # But say WHICH step was cancelled. A cancellation can hit one upload step
  # without taking the whole job with it, and the other outcome is then real. The
  # blanket line this branch used to print asserted that the run had been cancelled
  # before the SARIF reached anywhere -- flatly false for (code scanning: success,
  # artifact: cancelled), where code scanning demonstrably did take it. Describing
  # each sink separately is accurate on all NINE pairings that reach this branch
  # (5 with cs=cancelled + 5 with ar=cancelled - 1 counted twice), and stays quiet
  # on every one of them.
  verdict="a cancellation interrupted delivery — code scanning $(sink_phrase "$cs_state" "$cs_shown"), artifact $(sink_phrase "$ar_state" "$ar_shown"). No verdict is drawn for a cancelled step."
  echo "${LABEL}: cancelled mid-delivery — code scanning: ${cs_shown}, artifact: ${ar_shown}. No sink verdict for the cancelled step."
elif [ "$sarif_present" = no ]; then
  # See refusal 3. `scripts/summarize-sarif.sh` has already exited 2 and reddened
  # this job for the same reason, so this does not need to; it exists so the sink
  # report cannot certify a delivery of a document that was never written.
  verdict="**no SARIF existed to deliver** — \`$(basename "$SARIF")\` is missing or empty, so any \`success\` above stored zero findings (\`if-no-files-found: ignore\`). This is a SCANNER failure, not a storage one."
  annotation="::error title=No SARIF was produced::${LABEL}: $(basename "$SARIF") is missing or empty, so nothing was delivered to any sink regardless of the upload outcomes (code scanning: ${cs_shown}, artifact: ${ar_shown}). summarize-sarif.sh has already failed for the same reason — fix the scanner, not the quota."
elif [ "$cs_state" = delivered ] && [ "$ar_state" = delivered ]; then
  verdict="both machine-readable sinks took it."
  echo "${LABEL}: SARIF accepted by code scanning and archived."
elif [ "$cs_state" = delivered ] && [ "$ar_state" = absent ]; then
  # See refusal 2. Code scanning has it; the artifact is a convenience copy that
  # was not attempted. Silence is the correct output.
  verdict="code scanning took it; the artifact copy was not attempted (\`${ar_shown}\`)."
  echo "${LABEL}: SARIF accepted by code scanning."
elif [ "$cs_state" = delivered ]; then
  verdict="code scanning took it; the artifact copy did not store (\`${ar_shown}\`)."
  annotation="::warning title=SARIF artifact not stored::${LABEL}: code scanning accepted the SARIF but the artifact copy failed (${ar_shown}). The findings have a durable machine-readable home; check the account artifact quota when convenient."
elif [ "$ar_state" = delivered ]; then
  verdict="code scanning did **not** take it (\`${cs_shown}\`); the artifact is the only machine-readable copy."
  annotation="::warning title=SARIF delivered to one sink only::${LABEL}: code scanning did not accept the SARIF (${cs_shown}), so the artifact is the only machine-readable copy. Expected while GHAS is off on this private repository."
elif [ -z "$CODE_SCANNING" ] && [ -z "$ARCHIVE" ]; then
  # Tested on the LITERALS, not on the classified state: two `skipped` outcomes
  # mean both upload steps really were skipped (nothing delivered -- the branch
  # below is right for that), whereas two EMPTY outcomes mean nobody wired the
  # step ids and this report is describing nothing. Saying "neither sink took it"
  # for the second would blame billing for a workflow typo.
  verdict="**this report could not read either step outcome** (code scanning: \`${cs_shown}\`, artifact: \`${ar_shown}\`)."
  annotation="::error title=Sink report is miswired::${LABEL}: neither upload step's outcome reached this step, so the delivery is unknown, not green. Check that both upload steps carry an \`id:\` and that this step's \`env:\` reads \`steps.<id>.outcome\`."
else
  verdict="**neither machine-readable sink took it** (code scanning: \`${cs_shown}\`, artifact: \`${ar_shown}\`)."
  annotation="::error title=SARIF has nowhere to land::${LABEL}: SARIF reached NEITHER code scanning (${cs_shown}) NOR an artifact (${ar_shown}). Only the rule-level tally in this job's summary survives, and it expires with the run's logs. The job stays green on purpose — the scanner passed; its delivery did not."
fi

summary="#### ${LABEL} — where these findings landed

| sink | needs | result |
|---|---|---|
| code scanning | GHAS on this repository | \`${cs_shown}\` |
| SARIF artifact | account artifact quota | \`${ar_shown}\` |
| job summary (tally only) | nothing | written above |

${verdict}

The job summary above is a rule-level TALLY — level, rule id, count, capped at 40
rules. It is not a third machine-readable sink: it carries no file, no line and no
message, so a finding cannot be triaged from it. When both rows above are red the
detail is gone.
"

if [ -n "$annotation" ]; then printf '%s\n' "$annotation"; fi
printf '%s\n' "$summary"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
  # Guarded: an unwritable summary file or a full runner disk must not redden a
  # security job. Under `set -euo pipefail` the bare append did exactly that
  # (verified: rc=1, "Permission denied", on a read-only $GITHUB_STEP_SUMMARY).
  # The fallback keeps the verdict visible in the step log instead of losing it.
  if ! printf '%s\n' "$summary" >> "$GITHUB_STEP_SUMMARY" 2>/dev/null; then
    echo "${LABEL}: could not append to \$GITHUB_STEP_SUMMARY; the delivery table above is in this step's log instead."
  fi
fi
exit 0
