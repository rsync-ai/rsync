#!/usr/bin/env bash
# Every .gitleaks.toml allowlist must be as narrow as it claims to be.
#
# Why this exists: gitleaks silently ignores unknown allowlist keys. Scoping a block
# with `rules = ["generic-api-key"]` LOOKS correct and is accepted without a word of
# warning — but `rules` is not a field (the real one is `targetRules`), so the block
# degrades to exempting EVERY rule for those paths. The blind spot the scoping exists
# to remove is restored invisibly, and no scan ever complains. That shipped here: a
# single '''.*_test\.go$''' entry exempted all 377 Go test files from every rule, so a
# real AWS key or private key committed into any of them would never have been
# reported.
#
# Checking the FIELD NAMES would be a guess about gitleaks' internals that rots the
# next time upstream adds a key. So this checks the PROPERTY instead, by mutation:
# plant a private key in each rule-scoped exempt file and require gitleaks to still
# report it. If someone writes `rules` again, the plant goes unreported and this fails.
#
# The control matters as much as the probe. Both of the first mutation probes written
# for this config came back "no findings" because the probe VALUE was dead (gitleaks
# allowlists AKIA…EXAMPLE by default; the stopword "fake" kills another), which reads
# exactly like a broken config. Every run below therefore also plants the same value
# in a NON-exempt file and requires a finding — no control, no verdict.
set -euo pipefail
cd "$(dirname "$0")/.."

CONFIG=.gitleaks.toml

# An exemption wider than this is not "a verified fixture" any more, it is a policy
# hole. A ceiling on the CURRENT contents, not an aspiration: today the widest
# pattern matches exactly 1 file.
MAX_REACH=25

# The mutation plants a private key: a rule no allowlist block may target (asserted
# below), so "still reported" is a real signal in every exempt file.
PLANT_RULE=private-key

fail=0
note() { printf '  %s\n' "$*"; }
bad()  { fail=1; printf '  FAIL  %s\n' "$*"; }

PY=python3
for cand in /opt/homebrew/opt/python@3.11/bin/python3.11 python3.11; do
  command -v "$cand" >/dev/null 2>&1 && { PY=$cand; break; }
done
"$PY" -c 'import tomllib' 2>/dev/null || {
  echo "FATAL: $PY has no tomllib (needs Python >= 3.11). Refusing to skip." >&2
  exit 2
}

# How to run gitleaks: a local binary if present, else the same pinned image CI uses.
# Never silently skip — a scanner that isn't there must not read as a pass.
if command -v gitleaks >/dev/null 2>&1; then
  GL_MODE=binary
elif docker info >/dev/null 2>&1; then
  GL_MODE=docker
else
  echo "FATAL: neither a gitleaks binary nor a working Docker daemon. Refusing to skip." >&2
  exit 2
fi

scan() { # $1 = tree to scan, $2 = json report path
  case $GL_MODE in
    binary) gitleaks dir "$1" --config "$1/$CONFIG" --no-banner --redact \
              --exit-code 0 --report-format json --report-path "$2" >/dev/null 2>&1 ;;
    docker) docker run --rm -v "$1:/repo" -v "$(dirname "$2"):/out" \
              ghcr.io/gitleaks/gitleaks:latest dir /repo --config "/repo/$CONFIG" \
              --no-banner --redact --exit-code 0 --report-format json \
              --report-path "/out/$(basename "$2")" >/dev/null 2>&1 ;;
  esac
}

echo "gitleaks allowlist scope check ($GL_MODE)"

# ---------------------------------------------------------------- reach
# `git ls-files` (never `find`): the scanner sees the tracked tree, and this repo has
# untracked generated connector dirs that must not count.
git ls-files > /tmp/gl-tracked.$$

REACH=$("$PY" - "$CONFIG" /tmp/gl-tracked.$$ "$MAX_REACH" "$PLANT_RULE" <<'PY'
import re, sys, tomllib
cfg, tracked, cap, plant_rule = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4]
files = open(tracked).read().split()
d = tomllib.load(open(cfg, "rb"))
blocks = list(d.get("allowlists", []))
if isinstance(d.get("allowlist"), dict):
    blocks.append(d["allowlist"])
out, probes, bad = [], [], False
if not blocks:
    print("ERR|no allowlist block found at all — the config parsed to zero blocks")
    print("VERDICT|bad"); sys.exit(0)
for i, b in enumerate(blocks):
    paths = b.get("paths", [])
    if not paths:
        continue
    desc = b.get("description", f"block {i}")
    tr = b.get("targetRules") or []
    # INVARIANT 1. An unscoped block exempts its paths from every rule. This is also
    # how a MISSPELLED scope key surfaces: gitleaks ignores `rules` in silence, and so
    # does tomllib, so the block reads as blanket here and fails loudly instead.
    if not tr:
        out.append(f"ERR|{desc}: no targetRules — these paths are exempt from EVERY rule.")
        out.append("ERR|      If you meant to scope it, the key is `targetRules` (not `rules`).")
        bad = True
        continue
    # The mutation below plants a {plant_rule}. A block that silences that rule would
    # make its own probe unfalsifiable, so it must not exist.
    if plant_rule in tr:
        out.append(f"ERR|{desc}: targets {plant_rule}, which is the mutation probe's rule — pick another probe.")
        bad = True
        continue
    for p in paths:
        r = re.compile(p)
        hits = [f for f in files if r.search(f)]
        n = len(hits)
        if n == 0:
            # A pattern matching nothing is an exemption that outlived its file. Left
            # alone it silently re-widens the day a matching path is added back.
            out.append(f"ERR|dead exemption, matches no tracked file: {p}")
            bad = True
        elif n > cap:
            out.append(f"ERR|{p} exempts {n} files (ceiling {cap}) — too wide to be a verified fixture")
            bad = True
        else:
            out.append(f"OK|{n:>3}  {p}  [{','.join(tr)}]")
            probes.extend(hits)
print("\n".join(out))
print("PROBES|" + " ".join(sorted(set(probes))))
print("VERDICT|" + ("bad" if bad else "ok"))
PY
)
while IFS='|' read -r tag rest; do
  case $tag in
    OK)      note "$rest" ;;
    ERR)     bad "$rest" ;;
    PROBES)  PROBE_FILES=$rest ;;
    VERDICT) : ;;
  esac
done <<< "$REACH"
rm -f /tmp/gl-tracked.$$

if [ "$fail" -ne 0 ]; then
  echo
  echo "gitleaks allowlist check FAILED (scope). Not running the mutation stage:"
  echo "its verdict on a config this shape would be noise."
  exit 1
fi

# ------------------------------------------------------------- mutation
# Each block claims its files lose only the named rules and stay scanned by the rest.
# Prove it per file: a private key planted in each must still be reported.
# Under /tmp, not the default: macOS mktemp returns /var/folders/..., which Docker
# Desktop does not share, so the docker fallback below would mount an empty dir and
# report a triumphant zero findings.
TREE=$(mktemp -d /tmp/gl-tree.XXXXXX)
OUT=$(mktemp -d /tmp/gl-out.XXXXXX)
trap 'rm -rf "$TREE" "$OUT"' EXIT
git archive HEAD | tar -x -C "$TREE"          # exactly the tracked tree, real paths
cp "$CONFIG" "$TREE/$CONFIG"                  # ...but the config as it stands now

scan "$TREE" "$OUT/base.json"
base=$("$PY" -c "import json,sys; print(len(json.load(open(sys.argv[1]))))" "$OUT/base.json")
if [ "$base" -ne 0 ]; then
  bad "the tracked tree is not clean under this config ($base findings) — fix those first"
else
  note "  0  findings on the clean tracked tree"
fi

# The plant is assembled at runtime instead of written out literally. Spelled in full,
# it makes THIS file a `private-key` finding in the tracked tree -- which is exactly how
# it failed its own clean-tree assertion the moment it was first committed (the assertion
# runs against `git archive HEAD`, so an uncommitted script is invisible to it). The body
# below is fake padding, not a key.
_K='PRIVATE KEY'
PLANT="-----BEGIN RSA ${_K}-----
MIIEowIBAAKCAQEAx7Vn0Qk2bYpLmNqRsTuVwXyZaBcDeFgHiJkLmNoPqRsTuVwX
-----END RSA ${_K}-----"

# The control carries the whole run. Both of the first probes written for this config
# came back "no findings" because the probe VALUE was dead (gitleaks allowlists
# AKIA…EXAMPLE by default; a stopword kills another), which is indistinguishable from
# a broken config. So the same plant goes into a file no allowlist names, and if THAT
# is not reported the run is void rather than green.
CONTROL=$(git ls-files 'api-gateway/internal/handlers/*.go' | grep -v '_test\.go$' | head -1)
[ -n "$CONTROL" ] || { echo "FATAL: found no non-exempt control file" >&2; exit 2; }

for f in $CONTROL $PROBE_FILES; do
  [ -f "$TREE/$f" ] || { bad "probe target missing from the tracked tree: $f"; continue; }
  printf '\n%s\n' "$PLANT" >> "$TREE/$f"
done
scan "$TREE" "$OUT/mut.json"

# gitleaks reports paths relative to whatever root it was handed, and the Docker path
# differs from the local one — normalise by suffix rather than assuming either.
VERDICTS=$("$PY" - "$OUT/mut.json" "$PLANT_RULE" "$CONTROL" $PROBE_FILES <<'PY'
import json, sys
report, rule, wanted = sys.argv[1], sys.argv[2], sys.argv[3:]
hit = [x["File"] for x in json.load(open(report)) if x["RuleID"] == rule]
for w in wanted:
    print(("ok|" if any(h == w or h.endswith("/" + w) for h in hit) else "miss|") + w)
PY
)
CONTROL_OK=no
while IFS='|' read -r st f; do
  [ -n "$f" ] || continue
  if [ "$f" = "$CONTROL" ]; then
    [ "$st" = ok ] && CONTROL_OK=yes
    continue
  fi
  if [ "$st" = ok ]; then
    note "scoped: $PLANT_RULE in $f still caught"
  else
    bad "$f is exempt from EVERY rule — a planted $PLANT_RULE went unreported."
    bad "      Most likely its block uses \`rules\` instead of \`targetRules\`;"
    bad "      gitleaks ignores the unknown key and the paths silence everything."
  fi
done <<< "$VERDICTS"

if [ "$CONTROL_OK" != yes ]; then
  echo "FATAL: control failed — a $PLANT_RULE planted in $CONTROL was NOT reported." >&2
  echo "       The probe value is dead; this check cannot judge the config." >&2
  exit 2
fi
note "control: $PLANT_RULE in $CONTROL reported — probe is live"

if [ "$fail" -ne 0 ]; then
  echo
  echo "gitleaks allowlist check FAILED"
  exit 1
fi
echo
echo "gitleaks allowlist check passed"
