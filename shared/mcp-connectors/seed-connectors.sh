#!/bin/sh
# Keep the mcp_connectors volume in step with the connector catalog this image
# ships. Runs on every boot as a one-shot init container (Dockerfile.seed).
#
# The volume used to be seeded once, when empty, and never touched again. So an
# upgrade kept the previous release's connector code under the new services, and
# the tree stayed root-owned so tool-generator could not save into it (0.1.7-rc1).
#
# Now: a stamp of the shipped catalog is kept in the volume. Same stamp -> no-op.
# Different (first boot or an upgrade) ->
#   * every shipped versions/<v>/ directory is replaced whole, so a file the new
#     release dropped does not linger beside the new code;
#   * everything else shipped is copied over, so latest.json points at the
#     shipped version again;
#   * anything the release does not ship is kept -- generated connectors, and a
#     generated version of a shipped connector (though latest.json goes back to
#     the shipped version);
#   * the tree goes to CONNECTOR_OWNER (tool-generator's uid, the one writer) and
#     stays readable by every other uid that mounts it.
# The stamp is written last, so a run that dies part-way is redone next boot.
set -eu

SEED=${SEED_DIR:-/seed}
TARGET=${SEED_TARGET:-/target}
OWNER=${CONNECTOR_OWNER-1000:1000} # empty = leave ownership alone (tests)
STAMP_FILE="$TARGET/.rsync-connector-seed"

# busybox and GNU have sha256sum; macOS (where the tests also run) has shasum.
if command -v sha256sum >/dev/null 2>&1; then HASH=sha256sum; else HASH="shasum -a 256"; fi
# Paths AND contents: a renamed file changes the stamp too.
# shellcheck disable=SC2086 # HASH is a command with its arguments
want=$(cd "$SEED" && find . -type f -exec $HASH {} + | sort -k 2 | $HASH | cut -d ' ' -f 1)
have=$(cat "$STAMP_FILE" 2>/dev/null || true)

if [ "$want" = "$have" ]; then
	echo "connector catalog already current ($want)"
	exit 0
fi

(cd "$SEED" && find . -type d -name versions) | while IFS= read -r versions; do
	for shipped in "$SEED/$versions"/*/; do
		[ -d "$shipped" ] || continue
		rm -rf "$TARGET/$versions/$(basename "$shipped")"
	done
done
cp -a "$SEED/." "$TARGET/"

if [ -n "$OWNER" ]; then
	chown -R "$OWNER" "$TARGET"
fi
chmod -R a+rX "$TARGET"

printf '%s\n' "$want" >"$STAMP_FILE"
chmod 644 "$STAMP_FILE"
if [ -z "$have" ]; then
	echo "connector catalog seeded ($want)"
else
	echo "connector catalog refreshed $have -> $want"
fi
