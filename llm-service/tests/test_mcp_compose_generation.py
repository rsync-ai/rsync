"""Guards on the generated docker-compose.mcp.yml.

This file is a build artifact that is also committed, which is the shape that has
bitten this repo before: the source of truth (the connector directory) moves and
the artifact does not, so a connector exists in the repo and can never start.
Nothing raises -- the service is simply absent from the compose file.

Two properties are asserted here, both cheap and both about a silent failure:

  * every tracked connector has a service (catches the stranded-connector drift)
  * no service ships the fluentd log driver (which pushes to a collector the
    self-host stack does not run, and does so asynchronously -- i.e. silently)
  * every service in every base compose file bounds its log file

Coverage is a SUPERSET check, not byte-equality against a fresh regeneration: a
developer box legitimately carries untracked, locally generated connectors, and
the generator picks those up. Byte-equality would fail for them while catching
nothing extra.
"""

import fnmatch
import json
import os
import re
import subprocess

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
COMPOSE = os.path.join(REPO_ROOT, "docker-compose.mcp.yml")
GENERATOR = os.path.join(REPO_ROOT, "scripts", "mcp_generate_compose.py")
PUBLIC = os.path.join(REPO_ROOT, "shared", "mcp-connectors", "public")


def _tracked_connector_ids():
    """Connector ids that git actually carries, via `git ls-files`.

    Deliberately asks git rather than walking the filesystem: the filesystem also
    holds locally generated connectors that are correctly absent from the
    committed compose file, and including those would make this test fail on a
    developer box for a non-problem.
    """
    out = subprocess.run(
        ["git", "-C", REPO_ROOT, "ls-files", "shared/mcp-connectors/public/**/latest.json"],
        capture_output=True, text=True, check=True,
    ).stdout
    ids = set()
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        # .../public/<category>/<id>/latest.json  or  .../public/<id>/latest.json
        ids.add(os.path.basename(os.path.dirname(line)))
    return ids


def _compose_service_ids():
    """Service ids in the compose file, with the -vX-Y-Z-mcp suffix stripped."""
    ids = set()
    with open(COMPOSE) as fh:
        for line in fh:
            m = re.match(r"^  ([a-z0-9][a-z0-9-]*)-v\d+-\d+-\d+-mcp:\s*$", line)
            if m:
                ids.add(m.group(1))
    return ids


def test_every_tracked_connector_has_a_compose_service():
    tracked = _tracked_connector_ids()
    assert tracked, "no tracked connectors found -- the ls-files glob stopped matching"
    missing = sorted(tracked - _compose_service_ids())
    assert not missing, (
        "tracked connectors with no service in docker-compose.mcp.yml: "
        f"{missing}. They are committed to the repo and cannot start. "
        "Re-run `python3 scripts/mcp_generate_compose.py` and commit the result."
    )


def test_no_connector_ships_the_fluentd_log_driver():
    """The fluentd driver points at a collector the self-host stack does not run.

    Note what this is NOT: it is not that `docker logs` breaks. Docker writes its
    local cache as well as the remote driver, so `docker logs` keeps working on a
    fluentd container (verified on 28.5.1). The defect is the other half --
    `fluentd-address: localhost:24224` with `fluentd-async: "true"` on a box
    whose compose ships no fluent-bit at all (docker-compose.quickstart.yml
    does not), which does not fail the container, it buffers into a void. The
    async flag is what makes it silent, and silent is what makes it a bug.
    """
    with open(COMPOSE) as fh:
        compose = fh.read()
    assert "driver: fluentd" not in compose, (
        "a connector is back on the fluentd log driver, pushing to a collector "
        "that a self-host box does not run -- and asynchronously, so it fails silently"
    )
    with open(GENERATOR) as fh:
        gen = fh.read()
    assert '"      driver: fluentd"' not in gen, (
        "scripts/mcp_generate_compose.py emits the fluentd driver again -- the next "
        "regeneration would undo this for all connectors at once"
    )


def test_every_connector_service_bounds_its_log_file():
    """An unbounded log file fills the disk of a box nobody is watching.

    Not hypothetical: rsync-ai-otel-collector was measured at 106 MB with
    driver=json-file and options={} -- the default driver with no cap, which is
    what a service with no logging block at all inherits.
    """
    with open(COMPOSE) as fh:
        compose = fh.read()
    services = len(_compose_service_ids())
    assert services > 0
    assert compose.count("driver: json-file") == services, (
        "not every connector service declares the json-file driver"
    )
    assert compose.count("max-size:") == services, "a connector service has an unbounded log file"
    assert compose.count("max-file:") == services, "a connector service has an unbounded log file count"


def test_every_connector_service_bounds_its_memory_and_pids():
    """A connector reaches a host two ways -- this compose file, or the deployer's
    JIT spawn (connector-deployer/internal/spec/spec.go). A ceiling on only one of
    them is worse than none: bounded where it is tested, unbounded where it runs.

    The short `mem_limit`/`pids_limit` spelling is asserted rather than
    `deploy.resources.limits`, so that this file stays readable by both the v2
    plugin and the legacy docker-compose v1 binary (which ignores `deploy:`
    entirely outside Swarm). Nothing here claims v2 ignores it -- only that the
    short form is unambiguous on every runner a self-host box might have.
    """
    with open(COMPOSE) as fh:
        compose = fh.read()
    n = len(_compose_service_ids())
    assert n > 0
    assert compose.count("    mem_limit:") == n, (
        f"{compose.count('    mem_limit:')} mem_limit entries for {n} services"
    )
    assert compose.count("    pids_limit:") == n, (
        f"{compose.count('    pids_limit:')} pids_limit entries for {n} services"
    )
    assert "deploy:" not in compose, (
        "use the short mem_limit/pids_limit form here, not deploy.resources"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Connector discovery must be able to reach the whole tree.
#
# public/ is not flat. Ten connectors sit directly under it (public/stripe/) and
# eleven sit one level deeper (public/database/mysql/, public/storage/aws-s3/).
# Every reader of that tree therefore has to cross a directory separator, and the
# ways of failing to are not obvious from reading the pattern:
#
#   pathlib  Path.glob("*/latest.json")          finds 10 of 21   -- `*` stops at /
#   pathlib  Path.rglob("latest.json")           finds 21
#   python   glob.glob(p, recursive=True) + **   finds 21
#   git      ls-files 'public/**/latest.json'    finds 21
#   git      ls-files ':(glob)public/*/...'      finds 10 of 21
#
# The git default is forgiving -- a pathspec `*` DOES cross `/` unless :(glob)
# magic is used -- so the workflow's glob is safe for a reason that has nothing
# to do with the `**` a reader assumes is doing the work. Adding :(glob) magic
# would silently halve the release.
#
# A shortfall raises nothing. It is a smaller number, and a smaller number reads
# as a pass, which is exactly how eleven connectors served decimals over HTTP
# unguarded while their guard test was green (fixed in the same PR as this test).
DISCOVERY_ROOT = "shared/mcp-connectors/public"
PUBLISH_WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "docker-publish.yml")

_CODE_SUFFIXES = (".py", ".sh", ".yml", ".yaml", ".go", ".j2", ".tpl")
_NON_RECURSIVE_GLOB = re.compile(r"""\.glob\(\s*f?["']([^"']*latest\.json)["']""")


def _tracked_latest_json_paths():
    """Truth: every tracked latest.json, found with no glob at all."""
    out = subprocess.run(
        ["git", "-C", REPO_ROOT, "ls-files", DISCOVERY_ROOT],
        capture_output=True, text=True, check=True,
    ).stdout
    return {l.strip() for l in out.splitlines() if l.strip().endswith("/latest.json")}


def test_the_release_pathspec_reaches_every_tracked_connector():
    """Run the workflow's OWN pathspec, rather than a copy of it.

    Read out of docker-publish.yml so that narrowing the glob there fails here.
    A test carrying its own duplicate of the pattern would keep passing while the
    thing it claims to guard published half a release.
    """
    with open(PUBLISH_WORKFLOW) as fh:
        wf = fh.read()
    specs = re.findall(r"""git ls-files ['"]([^'"]*latest\.json)['"]""", wf)
    assert specs, (
        "no `git ls-files ...latest.json` pathspec found in docker-publish.yml -- "
        "this test just stopped guarding anything. Find where connector discovery "
        "moved to and point it there."
    )

    truth = _tracked_latest_json_paths()
    assert len(truth) >= 15, f"only {len(truth)} tracked latest.json files -- the census itself is broken"

    for spec in specs:
        got = {
            l.strip()
            for l in subprocess.run(
                ["git", "-C", REPO_ROOT, "ls-files", spec],
                capture_output=True, text=True, check=True,
            ).stdout.splitlines()
            if l.strip()
        }
        missing = sorted(truth - got)
        assert not missing, (
            f"the release pathspec {spec!r} misses {len(missing)} tracked connectors: "
            f"{missing}. Those images would never be built, and the publish job would "
            "report success having shipped a partial set."
        )


def test_no_connector_discovery_uses_a_non_recursive_glob():
    """Sweep, rather than guarding the one site that was found broken.

    Comment lines are skipped on purpose: this file and the fidelity test both
    quote the bad pattern while explaining it, and a substring match that cannot
    tell a quotation from a call is the same class of bug as everything above.
    """
    files = subprocess.run(
        ["git", "-C", REPO_ROOT, "ls-files"], capture_output=True, text=True, check=True
    ).stdout.splitlines()

    offenders = []
    scanned = 0
    for rel in files:
        if not rel.endswith(_CODE_SUFFIXES):
            continue
        try:
            with open(os.path.join(REPO_ROOT, rel), encoding="utf-8") as fh:
                body = fh.read()
        except (OSError, UnicodeDecodeError):
            continue
        if "latest.json" not in body:
            continue
        scanned += 1
        for i, line in enumerate(body.splitlines(), 1):
            if line.lstrip().startswith("#"):
                continue
            m = _NON_RECURSIVE_GLOB.search(line)
            if m and "**" not in m.group(1):
                offenders.append(f"{rel}:{i}  {m.group(1)!r}")

    assert scanned >= 5, f"only {scanned} files mention latest.json -- the sweep is not sweeping"
    assert not offenders, (
        "connector discovery that cannot cross a directory separator -- it will find "
        "the flat connectors and silently skip everything under public/database/ and "
        "public/storage/:\n  " + "\n  ".join(offenders) +
        "\nUse rglob('latest.json'), or glob(..., recursive=True) with '**'."
    )


def test_ci_runs_these_guards_when_the_release_workflow_changes():
    """The guard above reads docker-publish.yml, so editing docker-publish.yml has
    to be what runs it. It was not: the `llm` filter listed ci.yml but not the
    publish workflow, so a PR narrowing the release pathspec -- the single edit
    this file exists to catch -- would not have run this file at all.

    Asserted rather than eyeballed, because a path filter is exactly the kind of
    thing that gets tidied up later by someone who cannot see what depends on it.
    """
    import yaml

    with open(os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")) as fh:
        ci = yaml.safe_load(fh)

    step = next(
        s
        for j in ci["jobs"].values()
        for s in (j.get("steps") or [])
        if isinstance(s, dict) and "filters" in (s.get("with") or {})
    )
    globs = yaml.safe_load(step["with"]["filters"])["llm"]
    assert globs, "the llm path filter parsed empty -- this check would pass vacuously"

    def covered(rel):
        # fnmatch, so a basename glob counts as coverage. The hand-rolled
        # matcher here understood only `dir/**` and exact literals, so when
        # ci.yml collapsed its root compose entries into `docker-compose*.yml`
        # this read as UNCOVERED -- a false negative that would have pushed the
        # filter back toward redundant literals. fnmatch is looser than the
        # picomatch dorny/paths-filter uses (its `*` crosses `/`), so it can
        # only over-report coverage, never fail a pattern CI would honour.
        return any(
            fnmatch.fnmatch(rel, g) or (g.endswith("/**") and rel.startswith(g[:-2]))
            for g in globs
        )

    for rel in (".github/workflows/docker-publish.yml", "docker-compose.mcp.yml"):
        assert covered(rel), (
            f"{rel} is read by the guards in this file but is not in the `llm` path "
            f"filter, so a PR touching only it would not run them. Filter is: {globs}"
        )
