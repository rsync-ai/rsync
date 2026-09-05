#!/usr/bin/env bash
# The Go table in THIRD_PARTY_NOTICES.md must name exactly the modules that ship.
#
# Why this exists: on 2026-08-25 that table was wrong in 90 places — 58 shipped
# modules missing (the entirety of connector-deployer's closure, jackc/pgx/v5, the
# aws-sdk-go-v2 family), 29 stale versions, and github.com/lib/pq still listed months
# after it was removed from every go.mod and go.sum. None of it was visible: the file
# asserted it was "scanned from the current source tree; not manually edited", and no
# check compared it to anything. For a redistribution notice on a public repo, silent
# rot is the whole failure mode.
#
# Deliberately narrow. It does NOT run scripts/gen-third-party-notices.sh, which
# rewrites the whole file and would delete the hand-curated "Container image / OS
# packages" section — including the msodbcsql18 Microsoft-EULA flag that is the one
# item in the file actually awaiting legal review. Licenses are not re-resolved here
# either; this answers one question only: does the table list what ships?
set -euo pipefail
cd "$(dirname "$0")/.."

NOTICES=THIRD_PARTY_NOTICES.md

# Fail loudly on a missing tool rather than skipping: a check that quietly does
# nothing is indistinguishable from a check that passed.
command -v go >/dev/null 2>&1 || { echo "FATAL: no 'go' on PATH." >&2; exit 2; }
# Repo convention on the self-hosted Macs (see ci.yml / security.yml): setup-python
# cannot write the hosted tool cache, so workflows use the brew interpreter directly.
# Only the stdlib `re` is used here, so any python3 works -- prefer the pinned one.
for c in /opt/homebrew/opt/python@3.11/bin/python3.11 python3; do
  if command -v "$c" >/dev/null 2>&1; then PY=$c; break; fi
done
[ -n "${PY:-}" ] || { echo "FATAL: no python3 interpreter found." >&2; exit 2; }

# Keep in step with GO_MODULES in scripts/gen-third-party-notices.sh.
GO_MODULES=(
  api-gateway
  backend-orchestrator
  backend-temporal-adapter
  connector-deployer
  shared/mcp-connectors/internal/kafka-mcp-sink/worker-src
)

# Guard the list itself: a new Go service that nobody adds here ships unlisted, which
# is exactly how connector-deployer's 14 modules went unrecorded. Every go.mod that
# builds a main package must be named above.
missing_mod=0
gl_err=$(mktemp /tmp/tpn-golist.XXXXXX)
trap 'rm -f "$gl_err"' EXIT
while IFS= read -r gomod; do
  d=$(dirname "$gomod")
  case " ${GO_MODULES[*]} " in *" $d "*) continue ;; esac
  # shared/go/* are libraries pulled in via `replace`; they have no main package.
  # Never `2>/dev/null` this: a go list that FAILS would then read exactly like
  # "this module has no main package", and a broken new service would be skipped
  # silently -- the same shape of blindness this whole script exists to remove.
  # stderr goes to its own file rather than into `names`: `go list` writes progress
  # lines ("go: downloading ...") to stderr even on success, and merging them would
  # put them in the same stream the classification greps.
  if ! names=$(cd "$d" && go list -f '{{.Name}}' ./... 2>"$gl_err"); then
    echo "FATAL: go list failed in $d, so it cannot be classified:" >&2
    head -5 "$gl_err" >&2
    exit 2
  fi
  if grep -qx main <<<"$names"; then
    echo "FAIL: $d builds a main package but is not in GO_MODULES — its dependencies"
    echo "      would ship unlisted. Add it here and in scripts/gen-third-party-notices.sh."
    missing_mod=1
  fi
done < <(git ls-files '*go.mod')

closure=$(mktemp /tmp/tpn-closure.XXXXXX)
trap 'rm -f "$closure" "$gl_err"' EXIT
# GOOS=linux is pinned, not inherited. `go list -deps` resolves build constraints
# for the HOST by default, and every artifact this file describes is a Linux
# container image -- so an unpinned run on a macOS runner silently omits the
# Linux-only closure. That is not hypothetical: it hid
# github.com/prometheus/procfs (Apache-2.0, reached from client_golang by
# api-gateway, orchestrator and temporal-adapter) from this notice for as long
# as CI ran on the self-hosted Macs. GOARCH is deliberately NOT pinned: linux
# arm64 and amd64 produce identical closures, so pinning it would imply a
# distinction that does not exist.
for m in "${GO_MODULES[@]}"; do
  ( cd "$m" && GOOS=linux go list -deps -f '{{with .Module}}{{.Path}} {{.Version}}{{end}}' ./... ) \
    >> "$closure"
done
# `sort -u` on (path, version): a module legitimately appears twice when two services
# resolve different versions, and both binaries ship.
grep -v '^github.com/rsync-ai/' "$closure" | awk 'NF==2' | sort -u > "$closure.u"
mv "$closure.u" "$closure"

n=$(wc -l < "$closure" | tr -d ' ')
# A count of zero is not an answer. An empty closure — a go list that failed, a bad
# cwd — would otherwise make every table row look "extra" and could just as easily
# have been read as a pass by a laxer comparison.
if [ "$n" -lt 50 ]; then
  echo "FATAL: go list produced only $n modules; expected the full shipped closure." >&2
  echo "       Refusing to judge the notices against a result this small." >&2
  exit 2
fi

"$PY" - "$NOTICES" "$closure" "$n" <<'PY' || exit 1
import re, sys
notices, closure, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
truth = set()
for line in open(closure):
    p, _, v = line.strip().partition(" ")
    if p and v:
        truth.add((p, v))
rows, body, spdx = set(), False, {}
text = open(notices).read()
for line in text.splitlines(True):
    if line.startswith("## Go modules"):
        body = True
        continue
    if line.startswith("## "):
        body = False
    if body:
        m = re.match(r'^\|\s*\[([^\]]+)\]\([^)]*\)\s*\|\s*(v[^\s|]+)\s*\|\s*([^|]+?)\s*\|', line)
        if m:
            rows.add((m.group(1), m.group(2)))
            spdx[m.group(3)] = spdx.get(m.group(3), 0) + 1
if not rows:
    print("FATAL: parsed 0 rows out of the Go table — the section shape changed.")
    sys.exit(1)
missing = sorted(truth - rows)
extra   = sorted(rows - truth)
print(f"  shipped closure: {len(truth)} module@version   table: {len(rows)}")

# The two summary lines restate the table. They are the part a human edits by hand and
# forgets, so derive both from the table and require an exact match -- otherwise a row
# added correctly still leaves the file asserting a count that is now wrong.
summary = []
inv = re.search(r'^_Inventory: (\d+) Go module@version rows', text, re.M)
if not inv:
    summary.append("the `_Inventory:` line no longer states a Go module@version row count")
elif int(inv.group(1)) != len(rows):
    summary.append(f"`_Inventory:` says {inv.group(1)} Go rows; the table has {len(rows)}")
dist = re.search(r'^- \*\*Go\*\* \((\d+) module@version rows\): (.+)$', text, re.M)
if not dist:
    summary.append("the License-distribution line for Go is missing or reshaped")
else:
    if int(dist.group(1)) != len(rows):
        summary.append(f"License distribution says {dist.group(1)} Go rows; the table has {len(rows)}")
    want = "; ".join(f"{k} x {v}" for k, v in sorted(spdx.items(), key=lambda kv: (-kv[1], kv[0])))
    if dist.group(2).strip() != want:
        summary.append("License distribution does not match the table. Replace it with:\n"
                       f"    - **Go** ({len(rows)} module@version rows): {want}")

if not missing and not extra and not summary:
    print("OK: the Go table names exactly what ships, and its summary lines agree")
    sys.exit(0)
for problem in summary:
    print(f"\nFAIL: {problem}")
if not missing and not extra:
    sys.exit(1)
if missing:
    print(f"\nFAIL: {len(missing)} module(s) ship but are NOT listed. Add a row for each:")
    for p, v in missing:
        url = "https://" + "/".join(p.split("/")[:3]) if p.startswith("github.com/") else "https://" + p
        print(f"  | [{p}]({url}) | {v} | <SPDX from that version's LICENSE> |")
if extra:
    print(f"\nFAIL: {len(extra)} listed row(s) no longer ship. Delete them:")
    for p, v in extra:
        print(f"  {p} {v}")
print("\nAlso update the counts in the _Inventory:_ line and the License distribution section.")
sys.exit(1)
PY

[ "$missing_mod" -eq 0 ] || exit 1
