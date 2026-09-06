#!/usr/bin/env bash
# Proves the PUBLISHED OSS images carry NONE of the connector-generation moat.
#
# TWO images are published from llm-service/, and both need this proof:
#
#   llm-service-oss     <- Dockerfile.community   the /chat gateway. This is the
#                                                 image every OSS/self-host user
#                                                 actually pulls, and it has the
#                                                 LARGER surface (15+ COPY lines).
#   connector-lifecycle <- Dockerfile.oss         the connector deploy runtime. A
#                                                 strict subset: may ship less
#                                                 than the community image, never
#                                                 more.
#
# This script used to build only Dockerfile.oss, which left the image with the
# bigger attack surface -- and the one on the front page of the install docs --
# with no runtime proof at all. tests/test_oss_image_boundary.py reasons about
# both, but it reasons about COPY *lines*; it cannot see what a pip install, a
# base-layer file or a transitive copy actually put on disk. That is what this
# script is for, so it must cover both images.
#
# Checks, per image:
#   A  no moat YAML / jsonl anywhere in the image (by filename, whole filesystem)
#   B  every path in oss-strip-list.txt is absent under /app
#   C  layer audit (docker cp) finds no *.yaml / *.yml / *.jsonl under /app/src
#   D  the image's own entrypoint module imports with the moat physically absent
#   E  buildx present/absent as that image intends
#   F  POSITIVE CONTROL: the packages the image is supposed to ship are there
#
# F is not a formality. A, B and C are all ABSENCE checks, and an image that
# failed to copy anything at all would pass every one of them. Without F, "the
# build silently shipped an empty /app" and "the moat is correctly stripped" are
# the same green result.
#
# SCOPE: these checks prove the images are moat-free, carry the intended build
# tooling, and are non-empty. They do NOT POST /v1/deploy, so they do NOT prove
# the lifecycle image can actually deploy a connector at runtime (e.g. they would
# stay green even if the `docker` Python SDK were missing -- the F1 "Docker not
# available" dead-on-arrival). For that runtime proof, run the companion
# scripts/oss-deploy-smoke.sh, which stands up the lifecycle container and drives
# a real /v1/deploy to a running connector.
#
# Usage:  bash scripts/oss-leak-proof-test.sh
set -uo pipefail

CTX="llm-service"
STRIP_LIST="$CTX/oss-strip-list.txt"
FAIL=0
say()  { printf '\n=== %s ===\n' "$1"; }
head2() { printf '\n########## %s ##########\n' "$1"; }
ok()   { printf '  ✓ %s\n' "$1"; }
bad()  { printf '  ✗ %s\n' "$1"; FAIL=1; }

cd "$(git rev-parse --show-toplevel)" || exit 1

# ── the moat path list, read from the single source of truth ──────────────────
# Hand-copying it here is how the runtime check and the static guard drift: a new
# strip entry would be enforced by tests/test_oss_image_boundary.py and silently
# unchecked at runtime. Reading the file means adding a strip entry extends this
# script for free.
STRIP=()
while IFS= read -r line; do
  case "$line" in ''|'#'*) continue ;; esac
  STRIP+=("$line")
done < "$STRIP_LIST"

# A shrunken denominator would let every B check below pass while testing almost
# nothing -- the failure mode where the list is empty and "no leaks found" is
# vacuously true.
#
# This was a bare `-lt 20` against a list that had exactly 20 entries, so it
# carried no headroom: the first legitimate deletion tripped it, and the only
# way to get green again was to edit the literal -- which is also the edit that
# would hide a real truncation. A floor pinned to the current value does not
# measure the property it names.
#
# Two checks instead, because "under-read" and "gutted" are different faults.
# The first needs no maintained number at all: count the list's entries by an
# independent method and require the parse to agree. That catches the fault the
# comment above actually describes (a loop that reads nothing, or half, because
# of a bad path, a CRLF, or an early `break`) at ANY list size, forever.
DECLARED=$(grep -cvE '^[[:space:]]*(#|$)' "$STRIP_LIST" || true)
if [ "${#STRIP[@]}" -ne "$DECLARED" ]; then
  echo "FATAL: parsed ${#STRIP[@]} paths from $STRIP_LIST but it declares $DECLARED."
  echo "       Refusing to run: an under-read list makes check B pass by testing nothing."
  exit 1
fi

# The second is the absolute floor, and it is 15 to match the guard on the same
# file in llm-service/tests/test_oss_image_boundary.py -- one list, one floor.
# Deliberately below the current count: this number exists to catch the list
# being gutted, not to notice that one entry left.
if [ "${#STRIP[@]}" -lt 15 ]; then
  echo "FATAL: $STRIP_LIST yielded only ${#STRIP[@]} paths; expected the moat list (>=15)."
  echo "       Refusing to run: a gutted list makes check B pass by testing almost nothing."
  exit 1
fi

# ── moat filenames, pinned to files that actually exist ───────────────────────
MOAT_FILES='vendor_apis.yaml auth_rules.yaml capability_rules.yaml docs_fetch_rules.yaml authoritativeness_config.yaml resource_fallbacks.yaml learned_apis.jsonl'
# A renamed moat file would turn its check-A entry into a no-op that greps for a
# filename nothing uses any more. learned_apis.jsonl is exempt: it is generated at
# runtime by the private image and is legitimately absent from a clean checkout.
MISSING_MOAT=""
for f in $MOAT_FILES; do
  [ "$f" = "learned_apis.jsonl" ] && continue
  git ls-files --error-unmatch "$CTX/src/agents/tool_generator/config/$f" >/dev/null 2>&1 \
    || MISSING_MOAT="$MISSING_MOAT $f"
done
if [ -n "$MISSING_MOAT" ]; then
  # Two very different things produce a missing moat file, and only one is a bug.
  #
  #   (a) somebody renamed or deleted it in the private repo -- check A silently
  #       decays into a grep for a name nothing uses. FATAL, as before.
  #   (b) this IS the public repo. The flip's 2a loop deleted every path in
  #       oss-strip-list.txt, config/ among them, so there is no moat source left
  #       to rename and the guard has no subject. FATALing here would turn every
  #       llm-touching public PR red for doing exactly what the flip prescribes
  #       -- and this job's `if:` at .github/workflows/ci.yml admits both slugs
  #       precisely so it keeps running after the flip.
  #
  # The discriminator is the whole cut, not this one directory: (b) means EVERY
  # one of the >=20 strip paths is untracked. Deleting just config/ by accident
  # leaves the other 19 tracked and still lands in (a). That is what keeps this
  # branch from becoming a way to make the script pass by deleting things.
  #
  # Note it does not skip the run. Checks A and B interrogate the built IMAGES,
  # not the source tree, so both stay meaningful -- and load-bearing -- once the
  # moat is out of the repo but the images are still published from it.
  STILL_TRACKED=""
  for p in "${STRIP[@]}"; do
    git ls-files --error-unmatch "$CTX/$p" >/dev/null 2>&1 && STILL_TRACKED="$STILL_TRACKED $p"
  done
  if [ -n "$STILL_TRACKED" ]; then
    echo "FATAL: moat filename(s) not tracked at the expected path:$MISSING_MOAT"
    echo "       Check A would grep for a name nothing uses. Update MOAT_FILES."
    echo "       This is not the post-flip public tree: these strip paths are still"
    echo "       tracked, so the moat was renamed or partially deleted, not cut:"
    echo "      $STILL_TRACKED"
    exit 1
  fi
  echo "NOTE: no moat file is tracked, and neither is any of the ${#STRIP[@]} oss-strip-list.txt"
  echo "      paths -- this is the post-flip public tree. Skipping the rename guard,"
  echo "      which has no subject here. Checks A and B still run against the built"
  echo "      images, which is the half that matters once the source is gone."
fi

# ── per-image expectations ────────────────────────────────────────────────────
# Fields: dockerfile | tag | entrypoint module | buildx | extra-absent | must-ship
#
# buildx: the lifecycle image builds connectors just-in-time and holds the docker
# socket, so it needs the CLI. The community image never builds one and says so
# (Dockerfile.community:34). Asserting its ABSENCE keeps that comment honest --
# picking up docker-ce-cli there would be both bloat and a privilege surface, and
# nothing else in the repo would notice.
run_image() {
  local dockerfile="$1" tag="$2" entry="$3" want_buildx="$4" extra_absent="$5" must_ship="$6"
  local img="rsync-oss-leaktest:$tag"

  head2 "$tag  ($dockerfile)"

  say "BUILD"
  docker build -f "$CTX/$dockerfile" -t "$img" "$CTX" >/dev/null || { bad "build failed"; return; }
  ok "built $img"

  # Every scan below runs --user root ON PURPOSE.
  #
  # Both Dockerfiles end with `USER appuser`, so a scan at the image's default
  # user cannot read /root -- and `2>/dev/null` then swallows the "Permission
  # denied" that would have said so. Measured on these images: appuser sees 24
  # fewer paths than root in the community image (all under /root, including a
  # surviving /root/.cache/pip) and 5 fewer in the lifecycle image. A moat file
  # left in /root would therefore be invisible to this test and the test would
  # print a clean pass. Scanning as root is what makes "no moat files in image"
  # a statement about the image rather than about appuser's permissions.
  say "A: moat files absent anywhere in the image (scanned as root)"
  local found
  found=$(docker run --rm --user root --entrypoint sh "$img" -c '
    find / -xdev \( -name vendor_apis.yaml -o -name auth_rules.yaml -o -name capability_rules.yaml \
      -o -name docs_fetch_rules.yaml -o -name authoritativeness_config.yaml \
      -o -name resource_fallbacks.yaml -o -name learned_apis.jsonl \) 2>/dev/null')
  if [ -z "$found" ]; then ok "no moat files in image"; else bad "moat files present:"; echo "$found"; fi

  say "B: every oss-strip-list.txt path absent under /app (${#STRIP[@]} paths)"
  local leaks
  leaks=$(docker run --rm --user root --entrypoint sh "$img" -c '
    for p in "$@"; do [ -e "/app/$p" ] && echo "LEAK:$p"; done; exit 0' _ "${STRIP[@]}")
  if [ -n "$extra_absent" ]; then
    leaks="$leaks
$(docker run --rm --user root --entrypoint sh "$img" -c '
      for p in "$@"; do [ -e "/app/$p" ] && echo "LEAK(subset):$p"; done; exit 0' _ $extra_absent)"
  fi
  if echo "$leaks" | grep -q '^LEAK'; then
    bad "moat / out-of-subset paths present:"; echo "$leaks" | grep '^LEAK'
  else
    ok "no stripped path reached the image"
  fi

  say "C: layer audit (docker cp) — no yaml/yml/jsonl under /app/src"
  # Deliberately strict rather than a second pass of check A's filename list: the
  # point of C is to catch a moat file arriving under a name A does not know. As
  # of writing, EVERY tracked yaml/yml/jsonl under llm-service/src/ lives in
  # src/agents/tool_generator/{config,tests} -- both stripped -- so a clean image
  # has none at all and this check has no false positives.
  #
  # If you add a legitimate config yaml under a shipped package, this check will
  # fail. WIDEN IT (add the path to C_ALLOWED below); do not strip the file to
  # quiet it, and do not weaken C to a filename match -- that would make it a
  # duplicate of A and retire the only detector for a renamed moat file.
  local cid tmp audit
  cid=$(docker create "$img")
  tmp=$(mktemp -d)
  docker cp "$cid":/app/src "$tmp/src" >/dev/null 2>&1
  docker rm "$cid" >/dev/null
  audit=$(find "$tmp/src" \( -name '*.yaml' -o -name '*.yml' -o -name '*.jsonl' \) 2>/dev/null \
          | sed "s|^$tmp/||")
  local C_ALLOWED=''   # none today; see the note above before adding to this
  if [ -n "$C_ALLOWED" ]; then
    audit=$(echo "$audit" | grep -vxF "$C_ALLOWED")
  fi
  rm -rf "$tmp"
  if [ -z "$audit" ]; then ok "no yaml/yml/jsonl in copied src layer"; else bad "yaml/jsonl in image src:"; echo "$audit"; fi

  say "D: entrypoint '$entry' imports with the moat absent (runtime proof)"
  if docker run --rm --entrypoint sh "$img" -c "cd /app && python -c 'import $entry; print(1)'" 2>/dev/null | grep -q '^1$'; then
    ok "import $entry succeeds (loads clean without the generation moat)"
  else
    bad "import $entry FAILED — the image cannot start"
    docker run --rm --entrypoint sh "$img" -c "cd /app && python -c 'import $entry'" 2>&1 | tail -5
  fi

  say "E: buildx expected $want_buildx"
  if docker run --rm --entrypoint docker "$img" buildx version >/dev/null 2>&1; then
    if [ "$want_buildx" = present ]; then ok "buildx available"
    else bad "buildx PRESENT but this image never builds a connector (Dockerfile.community:34) — docker-ce-cli has crept in"; fi
  else
    if [ "$want_buildx" = absent ]; then ok "buildx absent, as intended"
    else bad "buildx missing — /v1/deploy JIT builds would fail"; fi
  fi

  say "F: positive control — the image actually shipped what it must"
  local missing
  missing=$(docker run --rm --user root --entrypoint sh "$img" -c '
    for p in "$@"; do [ -e "/app/$p" ] || echo "MISSING:$p"; done; exit 0' _ $must_ship)
  if [ -n "$missing" ]; then
    bad "the image is missing packages it is supposed to ship — every absence check above passed vacuously:"
    echo "$missing"
  else
    ok "all expected packages present (checks A–C were not vacuous)"
  fi
}

# community image: ships the gateway + planner; must NOT have buildx.
run_image Dockerfile.community community src.gateway.main absent \
  '' \
  'src/gateway src/agents/planner src/agents/tool_generator/deployment src/utils prompts/chat'

# lifecycle image: strict subset — the gateway, planner and prompts tree that the
# community image legitimately ships must NOT be here; buildx must be.
run_image Dockerfile.oss lifecycle src.lifecycle.main present \
  'src/gateway src/agents/planner prompts' \
  'src/lifecycle src/agents/tool_generator/deployment src/utils/connector_paths.py'

say "RESULT"
if [ "$FAIL" = 0 ]; then
  echo "  ✅ PASS — both published OSS images are moat-free, non-empty, and carry the"
  echo "     build tooling they each intend."
  echo "     (Presence only — this does NOT POST /v1/deploy. For the actual runtime deploy"
  echo "      proof, run scripts/oss-deploy-smoke.sh.)"
else
  echo "  ❌ FAIL — see above"
fi
exit "$FAIL"
