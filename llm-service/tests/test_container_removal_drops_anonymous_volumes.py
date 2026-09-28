"""Every `docker rm` in this repo passes `-v`, so a removed container takes its
anonymous volumes with it.

An image's `VOLUME` lines give every container made from it one anonymous volume
each. `docker run --rm` drops them when the container exits; `docker rm` without
`-v` does not, and nothing else ever will: the volume has no name and no owner.
The kafka-connect image declares three (/kafka/config, /kafka/data, /kafka/logs),
and the Kafka security matrix starts one worker per connect row with `run -d` and
removes it with `rm -f`. That was 36 volumes a run on the self-hosted runners'
one Docker VM: 4,284 had piled up (about 13.5 GB) by 2026-09-27.

`-v` removes only anonymous volumes, never a named one, and skips any volume
another container still uses. So it is always safe on a removal, and this test
asks for it on every one: shell (`docker rm`, `docker container rm`) and Python
(`["docker", "rm", ...]`, or a `docker(...)`-style helper whose first argument is
"rm"), comments and echoed instructions included, since people copy those.
"""

import os
import re
import subprocess

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SUFFIXES = (".sh", ".bash", ".py", ".yml", ".yaml")

_REMOVAL = re.compile(
    r"\bdocker\s+(?:container\s+)?rm\b"  # shell; `\b` keeps `rmi` out
    r"|[\"']docker[\"']\s*,\s*(?:[\"']container[\"']\s*,\s*)?[\"']rm[\"']"  # ["docker", "rm", ...]
    r"|\b\w*docker\(\s*[\"']rm[\"']"  # docker("rm", ...) / _docker("rm", ...)
)
# The removal's own arguments end at the first shell separator, or at the close of
# a Python call or list, so a `-v` bind mount elsewhere on the line cannot count.
_END = re.compile(r"[;&|>)\]]")
_VOLUMES_FLAG = re.compile(r"(?<![\w-])-[a-zA-Z]*v[a-zA-Z]*\b|--volumes\b")


def removals_without_volumes(text):
    """(line number, line) for each container removal that keeps its volumes."""
    found = []
    for n, line in enumerate(text.splitlines(), 1):
        for m in _REMOVAL.finditer(line):
            tail = line[m.end():]
            end = _END.search(tail)
            if not _VOLUMES_FLAG.search(tail[: end.start()] if end else tail):
                found.append((n, line.strip()))
    return found


def _scanned_files():
    out = subprocess.run(["git", "ls-files"], cwd=REPO_ROOT, capture_output=True, text=True, check=True)
    this = os.path.relpath(os.path.abspath(__file__), REPO_ROOT)
    return [p for p in out.stdout.splitlines() if p.endswith(SUFFIXES) and p != this]


def _all_removals():
    hits, bad = 0, []
    for rel in _scanned_files():
        path = os.path.join(REPO_ROOT, rel)
        if not os.path.isfile(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as fh:
            text = fh.read()
        hits += sum(len(_REMOVAL.findall(line)) for line in text.splitlines())
        bad += [f"{rel}:{n}: {line}" for n, line in removals_without_volumes(text)]
    return hits, bad


def test_every_container_removal_drops_its_anonymous_volumes():
    hits, bad = _all_removals()
    # Armed: a matcher that finds nothing would pass on any tree.
    assert hits >= 20, f"only {hits} container removals found; the matcher has gone blind"
    assert not bad, (
        "these remove a container but keep its anonymous volumes, which then leak "
        "forever -- add -v (`docker rm -fv`, or \"-fv\" in a Python arg list):\n  "
        + "\n  ".join(bad)
    )


@pytest.mark.parametrize("line", [
    'docker rm -f "$NAME" >/dev/null 2>&1 || true',
    "[ -z \"$ids\" ] || docker rm -f $ids",
    'xargs -r docker rm >/dev/null',
    'docker container rm "$cid"',
    '    docker("rm", "-f", cname, cname + "-props")',
    '        _docker("rm", "-f", MOCK_CONTAINER)',
    '        quiet(["docker", "rm", "-f", DEST_PG])',
    '["docker", "rm", "-f", container_id],',
    # A `-v` that belongs to another command on the same line does not count.
    'docker run -v "$PWD:/w" img; docker rm -f "$cid"',
    'docker rm -f "$cid" && docker run -v x:/y img',
])
def test_a_removal_that_keeps_its_volumes_is_found(line):
    assert removals_without_volumes(line), line


@pytest.mark.parametrize("line", [
    'docker rm -fv "$NAME" >/dev/null 2>&1 || true',
    'docker rm -f -v $ids',
    'docker rm --volumes --force "$cid"',
    'xargs -r docker rm -v >/dev/null',
    '    docker("rm", "-fv", cname, cname + "-props")',
    '        quiet(["docker", "rm", "-f", "-v", DEST_PG])',
    # Not a container removal at all.
    'docker rmi -f "$IMAGE_REF"',
    'docker volume rm "$KAFKA_VOLUME"',
    'docker network rm "$NET"',
    'rm -f "$tmp"',
])
def test_a_removal_that_drops_its_volumes_passes(line):
    assert not removals_without_volumes(line), line
