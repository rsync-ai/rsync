#!/usr/bin/env bash
# Render a SARIF file's findings into the GitHub job summary.
#
# Why this exists: the reporting scanners (gosec, Semgrep, Trivy) had exactly two
# places to put their findings, and on this repo both were dead at once.
#
#   1. code-scanning (`codeql-action/upload-sarif`) needs GitHub code scanning,
#      which a PRIVATE repo without GHAS does not have. That step is
#      `continue-on-error: true`, so it fails in silence.
#   2. the `Archive SARIF` artifact upload -- which on 2026-08-24/25 failed with
#      "Artifact storage quota has been hit" on every run, main included.
#
# So the findings went nowhere, while `security-audit-results/REPORT.md` said the
# archive step "preserves findings". Worse, the artifact failure turned six
# REPORTING jobs red, and a red `gosec (api-gateway)` reads as "gosec found
# something" -- it cannot, gosec runs with `-no-fail`. A storage-billing
# condition was wearing a security finding's clothes.
#
# The job summary is a third destination that consumes no artifact storage, needs
# no GHAS, and renders next to the red X where a reader is already looking.
#
# This script is deliberately strict about the one thing that IS a real failure:
# a SARIF that is missing, unparseable, or not SARIF means the scanner did not
# report, and that must go red. A quota problem must not.
set -euo pipefail

SARIF=${1:?usage: summarize-sarif.sh <file.sarif> <label>}
LABEL=${2:?usage: summarize-sarif.sh <file.sarif> <label>}

# CI has no setup-python (see security.yml semgrep job); prefer the brew 3.11 the
# runners already use, fall back to whatever python3 is on PATH.
for c in /opt/homebrew/opt/python@3.11/bin/python3.11 python3; do
  if command -v "$c" >/dev/null 2>&1; then PY=$c; break; fi
done
[ -n "${PY:-}" ] || { echo "FATAL: no python3 interpreter found." >&2; exit 2; }

if [ ! -f "$SARIF" ]; then
  echo "FATAL: $SARIF does not exist — the scanner produced no SARIF, so there is" >&2
  echo "       nothing to report. That is a scanner failure, not a storage problem." >&2
  exit 2
fi

"$PY" - "$SARIF" "$LABEL" <<'PY'
import collections, json, os, sys

path, label = sys.argv[1], sys.argv[2]

try:
    with open(path, encoding="utf-8") as fh:
        doc = json.load(fh)
except (OSError, ValueError) as exc:
    sys.stderr.write(f"FATAL: {path} is not readable JSON: {exc}\n")
    sys.exit(2)

if not isinstance(doc, dict) or "runs" not in doc:
    sys.stderr.write(
        f"FATAL: {path} parsed as JSON but has no top-level 'runs' key, so it is not\n"
        f"       SARIF. The scanner wrote something unexpected.\n")
    sys.exit(2)

# A rule's severity may live on the result (`level`) or only on the rule's
# defaultConfiguration. Read the result first, fall back to the rule, then to
# SARIF's own default of "warning" -- never guess "none", which would hide things.
SARIF_DEFAULT_LEVEL = "warning"
rows = collections.Counter()
levels = collections.Counter()
total = 0

for run in doc.get("runs") or []:
    if not isinstance(run, dict):
        continue
    rule_levels, rule_names = {}, {}
    driver = ((run.get("tool") or {}).get("driver") or {})
    for rule in (driver.get("rules") or []):
        if not isinstance(rule, dict):
            continue
        rid = rule.get("id")
        if rid is None:
            continue
        cfg = rule.get("defaultConfiguration") or {}
        if cfg.get("level"):
            rule_levels[rid] = cfg["level"]
        name = rule.get("name") or (rule.get("shortDescription") or {}).get("text")
        if name:
            rule_names[rid] = name

    for res in (run.get("results") or []):
        if not isinstance(res, dict):
            continue
        total += 1
        rid = res.get("ruleId") or "(no ruleId)"
        level = res.get("level") or rule_levels.get(rid) or SARIF_DEFAULT_LEVEL
        levels[level] += 1
        rows[(level, rid, rule_names.get(rid, ""))] += 1

ORDER = {"error": 0, "warning": 1, "note": 2, "none": 3}
ranked = sorted(rows.items(), key=lambda kv: (ORDER.get(kv[0][0], 9), -kv[1], kv[0][1]))

out = [f"### {label}", ""]
if total == 0:
    out.append(f"**0 findings.** Parsed `{os.path.basename(path)}` successfully.")
else:
    tally = ", ".join(f"{n} {lv}" for lv, n in
                      sorted(levels.items(), key=lambda kv: ORDER.get(kv[0], 9)))
    out += [f"**{total} findings** ({tally}) in `{os.path.basename(path)}`.", "",
            "| level | rule | n |", "|---|---|---:|"]
    CAP = 40
    for (level, rid, name) in [k for k, _ in ranked][:CAP]:
        n = rows[(level, rid, name)]
        shown = f"`{rid}`" + (f" {name}" if name and name != rid else "")
        out.append(f"| {level} | {shown} | {n} |")
    if len(ranked) > CAP:
        # No silent caps: say what was dropped, and how many findings it covered.
        dropped = sum(n for _, n in ranked[CAP:])
        out.append(f"| … | _{len(ranked) - CAP} more rules not shown_ | {dropped} |")

out += ["", "_Rendered here because code scanning needs GHAS on a private repo and the "
        "SARIF artifact upload is subject to the account's storage quota; this summary "
        "depends on neither._"]

text = "\n".join(out) + "\n"
sys.stdout.write(text)
summary = os.environ.get("GITHUB_STEP_SUMMARY")
if summary:
    with open(summary, "a", encoding="utf-8") as fh:
        fh.write(text + "\n")
PY
