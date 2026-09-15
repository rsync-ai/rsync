"""A path the code opens and no compose file mounts is an absence, and an absence
is valid YAML -- the mount half of test_quickstart_delivers_what_the_code_reads.

The defect this guard is built from: docker-compose.quickstart.yml mounted the
``mcp_connectors`` volume into five services and not into temporal-adapter, which is
the one container that runs ConnectorAvailabilityActivityV2. That activity resolves
connectors off the local filesystem (connector_check_activity.go:93-97 stats
/app/shared/mcp-connectors/{public,internal,} and /app/tools), so with no mount every
root Stat failed, checkConnectorExists returned false for EVERY connector, the
workflow escalated to generation, and the user was told "Connector generation failed
for mongodb" about a connector sitting in the volume the whole time. Not one pipeline
of any kind could be created on a self-host install.

Three properties of the bug are why a guard is worth more than the one-line fix:

  * It was invisible in development. docker-compose.yml has carried the mount from
    the start -- ``./shared:/app/shared``, commented "Required for connector
    filesystem checks" -- and prod/staging/e2e/ci-isolate are all OVERLAYS on that
    base, so they inherit it. The quickstart is the single standalone compose, so it
    is the only file that had to repeat the mount, and the only deployment path that
    could lose it. Every local run, every CI run and every cloud deploy was green.
  * It reads as a connector bug. The user-facing text names one connector, so the
    natural next move is to re-check that connector, its credentials and its
    metadata -- none of which are involved.
  * Nothing failed. No container crashed, no health check went red, no log line said
    "mount missing". A directory that is not there returns ENOENT to a Stat, and this
    code treats ENOENT as "connector absent", which is exactly what it means when the
    mount IS present. The correct and the broken state are indistinguishable from
    inside the process.

So the assertion is derived, not listed. A list of which service needs which mount is
a claim that goes stale the first time someone moves a helper; instead each service's
source tree is read for the paths it actually opens, and the mapping from service name
to tree comes from docker-compose.yml's own build stanzas (the quickstart has no
``build:`` -- it pulls pinned images -- so the trees have to come from the dev file).
Move the connector resolver into a new service and this test starts requiring the
mount there, with no edit here.

Two directions are checked, because either alone passes on a broken file:

  1. Code -> compose. Every quickstart service whose tree opens a path under the
     connector root must have a mount covering it.
  2. Base -> quickstart. No service may hold the mount in docker-compose.yml and
     lose it in the quickstart. This catches the case where someone deletes the
     literal from the Go source into a constant the regex misses: the differential
     still fires.

Both carry a positive control. A census that silently finds nothing produces a green
run over zero assertions, which is the failure mode that let the original defect
through -- see the ``_found_anything`` tests.

Static and cheap: pure YAML/text parsing, no docker.
"""

import functools
import pathlib
import re

import pytest
import yaml

REPO = pathlib.Path(__file__).resolve().parents[2]
BASE = REPO / "docker-compose.yml"
QUICKSTART = REPO / "docker-compose.quickstart.yml"

# The container path the connector catalog is expected at. Every tree below opens
# something at or under it.
CONNECTOR_ROOT = "/app/shared/mcp-connectors"

# Services that must turn up in the code census. Not the list being tested -- these
# are the positive control. If the parser breaks, or a rename moves a tree out from
# under the census, these fail loudly instead of the real assertions passing on an
# empty set.
MUST_BE_CENSUSED = ("temporal-adapter", "orchestrator", "api-gateway")


def _ignore_unknown_tag(loader, suffix, node):
    """Construct a node carrying a tag SafeLoader has never heard of.

    docker-compose.prod.yml writes ``ports: !override [...]``, a Compose merge
    directive from 2.24, and safe_load raises on it. A guard that cannot parse its
    subject is a guard that passes on nothing.
    """
    if isinstance(node, yaml.ScalarNode):
        return loader.construct_scalar(node)
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    return loader.construct_mapping(node)


class _ComposeLoader(yaml.SafeLoader):
    pass


_ComposeLoader.add_multi_constructor("!", _ignore_unknown_tag)


@functools.lru_cache(maxsize=None)
def _services(path: pathlib.Path) -> dict:
    return (yaml.load(path.read_text(), Loader=_ComposeLoader) or {}).get("services", {}) or {}


def _mount_targets(service: dict) -> list[str]:
    """Container-side paths this service mounts, in either compose syntax.

    Short form is ``source:target[:mode]``; the mode is what makes a naive
    ``split(':')[1]`` right by accident and a ``[-1]`` wrong. Long form is a mapping
    with ``target:``. A bind whose source contains a colon is not something this
    repo writes, and would show up as a target that is not absolute -- dropped.
    """
    out = []
    for entry in service.get("volumes") or []:
        if isinstance(entry, dict):
            target = entry.get("target")
        else:
            parts = str(entry).split(":")
            target = parts[1] if len(parts) >= 2 else None
        if target and str(target).startswith("/"):
            out.append(str(target).rstrip("/") or "/")
    return out


def _covers(targets: list[str], path: str) -> bool:
    """True if any mount target is ``path`` itself or a directory above it.

    Mounting /app/shared covers /app/shared/mcp-connectors -- that is exactly what
    docker-compose.yml does -- so an equality check would report the dev compose as
    broken and teach the next reader to distrust this file.
    """
    path = path.rstrip("/")
    return any(path == t or path.startswith(t + "/") for t in targets)


# A path literal in code, not in a comment. The adapter's own file documents the
# layout in a comment block naming the same paths five times; counting those would
# make a tree that only TALKS about the catalog look like a tree that reads it.
_PATH_LITERAL = re.compile(r'"(/app/(?:shared/mcp-connectors|tools)[^"]*)"')
_PY_PATH_LITERAL = re.compile(r"""['"](/app/(?:shared/mcp-connectors|tools)[^'"]*)['"]""")


def _code_lines(text: str, comment_prefixes: tuple[str, ...]) -> list[str]:
    return [
        line
        for line in text.splitlines()
        if not line.lstrip().startswith(comment_prefixes)
    ]


@functools.lru_cache(maxsize=None)
def _service_trees() -> dict[str, str]:
    """service name -> repo-relative source tree, taken from the dev compose.

    Read from ``build.dockerfile`` rather than written down: a hand list was wrong on
    its first run in the sibling guard, naming ``temporal-adapter`` for a directory
    called ``backend-temporal-adapter``.
    """
    out = {}
    for name, svc in _services(BASE).items():
        build = (svc or {}).get("build")
        dockerfile = build.get("dockerfile") if isinstance(build, dict) else None
        if not dockerfile:
            continue
        tree = str(pathlib.PurePosixPath(dockerfile).parent)
        if tree in {"", "."} or not (REPO / tree).is_dir():
            continue
        out[name] = tree
    return out


@functools.lru_cache(maxsize=None)
def _paths_read_by(tree: str) -> frozenset[str]:
    """Absolute catalog paths this tree opens, from string literals in its code."""
    root = REPO / tree
    assert root.is_dir(), f"{tree} is not a directory"
    found = set()
    for path in root.rglob("*.go"):
        if path.name.endswith("_test.go"):
            continue
        for line in _code_lines(path.read_text(errors="ignore"), ("//",)):
            found.update(_PATH_LITERAL.findall(line))
    for path in root.rglob("*.py"):
        if "/tests/" in path.as_posix() or path.name.startswith("test_"):
            continue
        for line in _code_lines(path.read_text(errors="ignore"), ("#",)):
            found.update(_PY_PATH_LITERAL.findall(line))
    return frozenset(found)


@functools.lru_cache(maxsize=None)
def _readers() -> dict[str, frozenset[str]]:
    """Quickstart service -> catalog paths its code opens. Empty entries dropped."""
    out = {}
    for name, tree in _service_trees().items():
        if name not in _services(QUICKSTART):
            continue
        paths = _paths_read_by(tree)
        if paths:
            out[name] = paths
    return out


# ---------------------------------------------------------------------------
# Positive controls. An empty census and a clean repo look identical.
# ---------------------------------------------------------------------------


def test_the_census_found_the_trees_it_is_supposed_to_find():
    trees = _service_trees()
    missing = [s for s in MUST_BE_CENSUSED if s not in trees]
    assert not missing, (
        f"docker-compose.yml no longer names a build for {missing}, so the mount "
        f"assertions below would cover {len(missing)} service(s) fewer and still "
        f"report green. Census saw: {sorted(trees)}"
    )


def test_the_code_scan_found_services_that_read_the_catalog():
    readers = _readers()
    assert readers, (
        "no quickstart service was found to open a path under "
        f"{CONNECTOR_ROOT}, which cannot be true -- the connector resolvers are "
        "still there. The literal-scanning regex has stopped matching, and every "
        "assertion parameterised on this set is now vacuous."
    )
    assert "temporal-adapter" in readers, (
        "temporal-adapter is the service that runs ConnectorAvailabilityActivityV2 "
        "and it is the service the original defect hit. If the census stops seeing "
        "it, this whole file has stopped guarding the bug it was written for."
    )


def test_the_base_compose_still_mounts_the_catalog():
    """The differential below is only as good as the reference it differs against."""
    mounted = [
        name
        for name, svc in _services(BASE).items()
        if _covers(_mount_targets(svc or {}), CONNECTOR_ROOT)
    ]
    assert mounted, (
        "docker-compose.yml mounts the connector catalog into no service at all. "
        "Either the dev stack is broken too, or this parser no longer understands "
        "the volume syntax -- either way the base-vs-quickstart differential below "
        "is comparing against an empty set and passes on anything."
    )


# ---------------------------------------------------------------------------
# 1. Code -> compose.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("service", sorted(_readers()))
def test_a_service_that_opens_the_catalog_has_it_mounted(service):
    """At least one of the roots the code tries must exist in the container.

    ``at least one``, not ``all``, and the difference is not a softening -- the
    roots are ALTERNATIVES. checkConnectorExists loops over
    {mcp-connectors/public, mcp-connectors/internal, mcp-connectors, /app/tools}
    and returns on the first hit, so a resolver needs one of them, not the set.
    ``/app/tools`` in particular is a pre-#183 legacy root that NO compose file in
    this repo has ever mounted (0 occurrences as a mount target against 5 for the
    connector volume in the quickstart alone) -- demanding it would fail every
    service forever and the next person would delete this file rather than read it.
    A guard nobody believes is worse than no guard.
    """
    svc = _services(QUICKSTART)[service]
    targets = _mount_targets(svc)
    wanted = sorted(_readers()[service])
    assert any(_covers(targets, p) for p in wanted), (
        f"{service} opens {wanted} and docker-compose.quickstart.yml mounts "
        f"{targets or 'nothing'} into it -- not one of those roots exists in the "
        f"container.\n\n"
        f"This does not crash and nothing logs it: the directory is simply not "
        f"there, Stat returns ENOENT, and the code reads that as 'connector "
        f"absent' -- the same answer it gives when the mount IS present and the "
        f"connector genuinely is missing. On temporal-adapter this made every "
        f"connector unresolvable, so the workflow escalated to generation and the "
        f"UI blamed whichever connector the user had picked.\n\n"
        f"Fix: give {service} the volume its peers already have --\n"
        f"    volumes:\n"
        f"      - mcp_connectors:{CONNECTOR_ROOT}:ro"
    )


# ---------------------------------------------------------------------------
# 2. Base -> quickstart.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "service",
    sorted(
        name
        for name, svc in _services(BASE).items()
        if name in _services(QUICKSTART)
        and _covers(_mount_targets(svc or {}), CONNECTOR_ROOT)
    ),
)
def test_no_service_loses_the_catalog_between_base_and_quickstart(service):
    """docker-compose.yml is the reference: every other compose overlays it.

    The quickstart is the exception -- standalone, pulled by install.sh, and the
    only file that can drop a mount the base grants. That asymmetry is the whole
    bug class, and it is invisible to any check that reads one file at a time.
    """
    targets = _mount_targets(_services(QUICKSTART)[service])
    assert _covers(targets, CONNECTOR_ROOT), (
        f"docker-compose.yml mounts the connector catalog into {service} but "
        f"docker-compose.quickstart.yml does not (it mounts {targets or 'nothing'}).\n"
        f"The quickstart is the only standalone compose -- prod, staging, e2e and "
        f"ci-isolate all overlay the base and inherit its mounts -- so a mount "
        f"dropped here breaks self-host installs ONLY, while every local run, CI "
        f"run and cloud deploy stays green."
    )
