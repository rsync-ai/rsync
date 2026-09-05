"""Every ``ENV`` slot a connector Dockerfile bakes must be read by its own code.

A connector Dockerfile that bakes ``ENV FOO_HOST=""`` is advertising an operator
override slot: "set this on the container and it will be honoured." If no Python
in that image's build context ever reads ``FOO_HOST``, the slot is a lie — an
operator sets it, nothing happens, and the failure is silent. That is
``KI-CONNECTOR-DEAD-ENV-OVERRIDE-SLOTS``; this file turns the one-off sweep that
found it into a permanent detector.

Two denominators are load-bearing, and BOTH have been got wrong before:

* **The FILE denominator.** The slot may be read by a shared module that is not
  in the version directory at all — bigquery's ``RSYNC_BQ_LOAD_JOB`` resolves in
  ``public/warehouse_adapters.py``, pulled in by ``COPY --from=shared``. Scanning
  only ``connector.py`` reports a false POSITIVE. So the read-set is built from
  the whole build context: every non-test ``.py`` under the current version dir
  PLUS every path the Dockerfile itself names via ``COPY --from=shared``.

* **The READ denominator.** The slot may be read INDIRECTLY. clickhouse /
  databricks / snowflake / redshift all do
  ``env_map = {"host": "CLICKHOUSE_HOST", ...}`` then ``os.getenv(env)`` — the
  variable name never appears as an argument to ``getenv``. A sweep that
  extracts literal names from ``getenv`` CALL SITES reports a false NEGATIVE and
  declares 18 live slots dead (which is exactly what the original entry did). So
  the read-set is every ``ast.Constant`` string in the build context, compared
  for EXACT equality.

AST + exact equality also refuses to credit a mere mention: a variable named only
in a comment or a docstring is not a read (comments are not in the AST at all,
and a docstring is a whole-block string that will not compare equal to the bare
variable name).

Measured baseline when this guard was written (2026-09-01): **21 connectors,
71 env slots, 0 dead**. The denominator assertions below exist because an empty
dead-set reads identically whether the scan found nothing wrong or scanned
nothing at all.

Discovery goes through ``git ls-files`` deliberately: an ``rglob``/``find`` for
``latest.json`` would drag in untracked local scratch connectors that are not
part of the repo.
"""

from __future__ import annotations

import ast
import re
import subprocess
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

import pytest

from src.utils.connector_paths import resolve_current_dir

# Repo root is two levels above this file: <repo>/llm-service/tests/<this>.
_REPO_ROOT = Path(__file__).resolve().parents[2]
_PUBLIC_CONNECTORS_ROOT = _REPO_ROOT / "shared" / "mcp-connectors" / "public"

# Baked by every connector image as plumbing, not as an operator override slot.
# These are consumed by the image/runtime (uvicorn port, Python import path,
# the "am I in Docker" switch), not by connector config resolution.
_INFRA_ENV = frozenset({"PYTHONPATH", "MCP_HTTP_MODE", "DOCKER_CONTAINER", "MCP_PORT", "PORT"})

_ENV_LINE = re.compile(r"^\s*ENV\s+([A-Z][A-Z0-9_]*)\s*=")
_COPY_FROM_SHARED = re.compile(r"^\s*COPY\s+--from=shared\s+(\S+)")

# Never walk into a local virtualenv / dependency tree that a dev may have left
# inside a version dir; site-packages would add tens of thousands of files and
# could credit a slot by pure coincidence.
_PRUNED_DIRS = frozenset({"venv", ".venv", "node_modules", "site-packages", "__pycache__"})

# Denominator floors. Deliberately well below the measured baseline (21 / 71) so
# ordinary connector churn does not trip them, but high enough that a discovery
# bug which collapses the scan to a handful of files fails loudly instead of
# passing with an empty dead-set.
_MIN_CONNECTORS = 15
_MIN_ENV_SLOTS = 50

_string_constants_cache: Dict[Path, Set[str]] = {}


def _tracked_connector_dirs() -> List[Path]:
    """Connector ROOT dirs (the ones holding ``latest.json``), git-tracked only."""
    out = subprocess.run(
        ["git", "ls-files", "shared/mcp-connectors/public"],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    dirs: List[Path] = []
    seen: Set[Path] = set()
    for rel in out:
        p = Path(rel)
        if p.name != "latest.json" or "versions" in p.parts:
            continue
        d = _REPO_ROOT / p.parent
        if d not in seen:
            seen.add(d)
            dirs.append(d)
    return sorted(dirs)


def _baked_env_slots(dockerfile: Path) -> List[str]:
    """Operator-override ENV names baked by this Dockerfile (infra names dropped)."""
    slots: List[str] = []
    for line in dockerfile.read_text(errors="replace").splitlines():
        m = _ENV_LINE.match(line)
        if m and m.group(1) not in _INFRA_ENV and m.group(1) not in slots:
            slots.append(m.group(1))
    return slots


def _shared_paths(dockerfile: Path) -> List[Path]:
    """Paths pulled into the build context by ``COPY --from=shared <path>``.

    The ``shared`` named build context is ``shared/mcp-connectors/public`` (see
    ``docker-compose.mcp.yml`` ``build.additional_contexts``), so the argument is
    resolved relative to that directory.
    """
    paths: List[Path] = []
    for line in dockerfile.read_text(errors="replace").splitlines():
        m = _COPY_FROM_SHARED.match(line)
        if m:
            paths.append(_PUBLIC_CONNECTORS_ROOT / m.group(1))
    return paths


def _iter_python_files(roots: Iterable[Path]) -> Iterable[Path]:
    """Every non-test ``.py`` under ``roots`` (files are yielded directly)."""
    for root in roots:
        if root.is_file():
            if root.suffix == ".py" and not root.name.startswith("test_"):
                yield root
            continue
        if not root.is_dir():
            continue
        for path in root.rglob("*.py"):
            if _PRUNED_DIRS.intersection(path.parts):
                continue
            if path.name.startswith("test_"):
                continue
            yield path


def _string_constants(path: Path) -> Set[str]:
    """Every ``str`` value appearing as an ``ast.Constant`` in ``path``.

    An unparseable file contributes nothing rather than exploding the whole
    guard — a syntax error is a different test's job.
    """
    cached = _string_constants_cache.get(path)
    if cached is not None:
        return cached
    values: Set[str] = set()
    try:
        tree = ast.parse(path.read_text(errors="replace"))
    except (SyntaxError, ValueError, OSError):
        _string_constants_cache[path] = values
        return values
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            values.add(node.value)
    _string_constants_cache[path] = values
    return values


def _scan() -> Tuple[List[str], int, int]:
    """Return ``(dead_slots, connectors_scanned, env_slots_scanned)``.

    ``dead_slots`` entries are formatted ``<connector>:<VAR>``.
    """
    dead: List[str] = []
    connectors = 0
    slots_total = 0
    for connector_dir in _tracked_connector_dirs():
        current = resolve_current_dir(connector_dir)
        dockerfile = current / "Dockerfile"
        if not dockerfile.exists():
            continue
        connectors += 1
        slots = _baked_env_slots(dockerfile)
        if not slots:
            continue
        slots_total += len(slots)
        read_set: Set[str] = set()
        for py in _iter_python_files([current, *_shared_paths(dockerfile)]):
            read_set |= _string_constants(py)
        for slot in slots:
            if slot not in read_set:
                dead.append(f"{connector_dir.name}:{slot}")
    return dead, connectors, slots_total


def test_no_connector_bakes_an_env_slot_nothing_reads():
    """No Dockerfile advertises an override slot its build context never reads."""
    dead, connectors, slots = _scan()

    # Denominators FIRST: an empty dead-set is only meaningful if the scan
    # actually looked at something. A discovery regression (git ls-files failing,
    # resolve_current_dir returning an empty dir, the ENV regex going stale) would
    # otherwise present as a clean pass.
    assert connectors >= _MIN_CONNECTORS, (
        f"only {connectors} connectors scanned (expected >= {_MIN_CONNECTORS}); "
        "discovery is broken, so a passing dead-slot assertion would be meaningless"
    )
    assert slots >= _MIN_ENV_SLOTS, (
        f"only {slots} env slots scanned across {connectors} connectors "
        f"(expected >= {_MIN_ENV_SLOTS}); the Dockerfile ENV parse is broken"
    )

    assert not dead, (
        "Dockerfile bakes ENV override slots that nothing in the build context reads "
        f"({len(dead)} dead of {slots} slots across {connectors} connectors):\n  "
        + "\n  ".join(sorted(dead))
        + "\n\nEither wire the variable into the connector's config resolution "
        "(`if not config.get(k): v = os.getenv(ENV)` — never the two-arg getenv, the "
        "Dockerfile bakes these SET-but-empty) or delete the ENV line. If the slot is "
        "genuinely read by a shell entrypoint rather than Python, add an explicit "
        "allowlist entry here — do not loosen the exact-match rule."
    )


def test_scan_denominators_match_the_recorded_baseline():
    """Pin the measured baseline so a silent collapse in coverage is visible.

    Recorded 2026-09-01: 21 connectors / 71 env slots. Bounds are one-sided
    (floors) so adding a connector or a slot is not a failure.
    """
    _, connectors, slots = _scan()
    assert connectors >= _MIN_CONNECTORS, connectors
    assert slots >= _MIN_ENV_SLOTS, slots


@pytest.mark.parametrize(
    "connector,env_var",
    [
        # The indirect `env_map = {...}` + `os.getenv(env)` idiom the original
        # sweep could not see. If a future refactor makes the scanner
        # call-site-based again, these four regress to "dead" and this test says so.
        ("clickhouse", "CLICKHOUSE_SECURE"),
        ("snowflake", "SNOWFLAKE_WAREHOUSE"),
        ("databricks", "DATABRICKS_HTTP_PATH"),
        ("redshift", "REDSHIFT_SSLMODE"),
        # Read in a shared module pulled in by `COPY --from=shared`, not in the
        # version dir at all — the false-POSITIVE half of the trap.
        ("bigquery", "RSYNC_BQ_LOAD_JOB"),
        # The slot this guard was written for.
        ("mongodb", "MONGODB_HOST"),
    ],
)
def test_known_indirect_slots_are_credited(connector, env_var):
    """Sanity-check the scanner against slots whose reads are known-indirect."""
    dead, _, _ = _scan()
    assert f"{connector}:{env_var}" not in dead, (
        f"{connector}:{env_var} scored as DEAD; the scanner has lost the ability to "
        "see an indirect read (env_map + os.getenv(env)) or a shared-module read."
    )
