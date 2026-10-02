"""No setting of ours shares a name with one the rsync file-copy tool reads.

Our settings are prefixed `RSYNC_`, and so are the environment variables of rsync,
the decades-old file-copy utility (rsync(1), section ENVIRONMENT VARIABLES). A name
in both is read by both. scripts/loadtest read its API password from rsync's own
password variable until it moved to `RSYNC_AI_PASSWORD`: exporting it for the load
test also authenticated every rsync daemon transfer run from that shell, and an
rsync user's own password was offered to our API. This keeps the overlap out.

The names are never written out whole in this file, which is scanned like any other.
"""

import pathlib
import re
import subprocess

REPO = pathlib.Path(__file__).resolve().parents[2]

# rsync(1), ENVIRONMENT VARIABLES. Written without the prefix so this file does not
# match its own search.
_TOOL_SUFFIXES = (
    "ICONV", "OLD_ARGS", "PROTECT_ARGS", "RSH", "PROXY", "PASSWORD",
    "CONNECT_PROG", "SHELL", "CHECKSUM_LIST", "COMPRESS_LIST", "MAX_ALLOC",
    "PORT", "PARTIAL_DIR",
)
_PREFIX = "RSYNC" + "_"
_TOOL_VAR = re.compile(r"\b" + _PREFIX + "(" + "|".join(_TOOL_SUFFIXES) + r")\b")

# Records of what changed: they name the old variable to say it is gone.
_HISTORY = {"CAPABILITIES-ARCHIVE.md", "CHANGELOG.md"}


def _offences():
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", "-z"],
        capture_output=True, text=True, timeout=120, check=True,
    ).stdout
    files = [p for p in out.split("\0") if p and p not in _HISTORY]
    assert len(files) > 500, f"only {len(files)} tracked files listed; the scan below would prove nothing"
    hits = []
    for rel in files:
        try:
            text = (REPO / rel).read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if _PREFIX not in text:
            continue
        for number, line in enumerate(text.splitlines(), start=1):
            if _TOOL_VAR.search(line):
                hits.append(f"{rel}:{number}: {line.strip()[:140]}")
    return hits


def test_no_setting_is_named_like_one_the_rsync_tool_reads():
    hits = _offences()
    assert not hits, (
        "These lines use an environment variable the rsync file-copy tool also reads, so "
        "setting it changes rsync's behaviour too. Prefix ours RSYNC_AI_ instead:\n  "
        + "\n  ".join(hits)
    )


def test_the_matcher_sees_the_names_it_bans():
    assert _TOOL_VAR.search(f"default=os.environ.get('{_PREFIX}PASSWORD')")
    assert not _TOOL_VAR.search(f"{_PREFIX}AI_PASSWORD {_PREFIX}PASSWORD_FILE {_PREFIX}VERSION")
