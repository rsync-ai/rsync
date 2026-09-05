"""No image a self-hoster pulls may carry a floating tag.

WHY THIS EXISTS. A floating tag is the one kind of change that arrives with no
change on our side: nothing in the repo differs, no test runs, no reviewer sees
a diff, and CI cannot go red -- the new bits simply appear on the next
`docker compose pull` or `--force-recreate`. It had already happened here.
`tecnativa/docker-socket-proxy:latest` moved v0.4.2 -> v0.5.0 on 2026-07-27, and
that container IS the docker-socket security boundary: it is what stands between
Traefik/api-gateway and the raw socket. `scripts/deploy-service.sh` runs
`--force-recreate`, so the swap would have landed on the next deploy of that
service, on prod, unannounced.

BACKLOG.md tracked a narrower version of this as F-CHART-MINIO-FLOATING-TAGS. It
named only `values.yaml` and only MinIO -- it never named the one-command
install (docker-compose.quickstart.yml), the base stack, the prod overlay, or
the socket proxy, and its cited line numbers had drifted. This test is the
generalisation: it checks every shipped compose file and the chart, so the next
floating tag cannot be introduced anywhere in the delivery surface.

WHAT COUNTS AS FLOATING. `:latest` explicitly, any of the other conventional
moving aliases, and -- the sneakiest form -- NO TAG AT ALL, which Docker
resolves to `:latest` while reading in a diff as if it were a considered choice.

FIRST-PARTY IMAGES ARE OUT OF SCOPE, DELIBERATELY. `ghcr.io/rsync-ai/*` images
are written `:${RSYNC_VERSION:-latest}` by convention, and install.sh derives
RSYNC_VERSION from the ref it fetched the compose file from, so both halves of
an install name the same code (#892). That convention is already enforced --
test_shipped_images_are_publishable.py fails any first-party image NOT written
that way -- and pinning them here would fight it. What IS asserted below is the
complement: no first-party image may be HARD-pinned to a bare `:latest` with the
interpolation dropped. That is the exact shape of the bug #892 fixed -- half the
first-party images honoured the variable and half were hard-pinned `:latest`, so
`RSYNC_VERSION=v0.1.0 docker compose up` silently ran a mix of two refs.

A GUARD IS ONLY WORTH ITS REACH. ci.yml runs this file from the
`llm-service-unit` job, which is gated on the `llm` paths filter. When this
file was first written that filter named exactly two root compose files, so
the guard was dead on the other 18 -- including docker-compose.yml, the base
stack the prod overlay merges onto and where the socket-proxy pin lives. A PR
floating a tag there would have skipped the job, and a skipped check looks
exactly like a passing one. ci.yml's own comments record five earlier
instances of that shape; nothing enforced the rule. The last test below does.
"""

import fnmatch
import glob
import os

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))

FIRST_PARTY = "ghcr.io/rsync-ai/"

# A tag that names a moving target rather than a build. `latest` is the one that
# bit us; the rest are the conventional aliases that mean the same thing and
# would sail past a check that only looked for `latest`.
MOVING_TAGS = {"latest", "stable", "edge", "nightly", "main", "master", "dev"}


class _ComposeLoader(yaml.SafeLoader):
    """Compose's merge tags are not standard YAML.

    `docker-compose.oss.yml` uses `!override`. `yaml.safe_load` raises
    ConstructorError on it, so a census that did not handle it would either
    crash or -- far worse, if someone wrapped the parse in a try/except -- skip
    that file silently and report green while checking one fewer file than it
    claimed. `test_every_compose_file_parsed` below pins that it does not.
    """


def _passthrough(loader, node):
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node)
    return loader.construct_scalar(node)


for _tag in ("!override", "!reset"):
    _ComposeLoader.add_constructor(_tag, _passthrough)


# Test fixtures, not delivery. These compose files start throwaway databases for
# the e2e suite; they are never fetched by install.sh, never referenced by the
# chart, and their containers are torn down at the end of a run. A moving tag
# there costs a flaky test, not a silent change on someone's server. Keyed by
# (file, service) so the exemption cannot widen to a file's other services.
ALLOWLIST = {
    ("docker-compose.e2e.dbs.yml", "minio"):
        "e2e fixture: throwaway object store, torn down with the test run",
    ("docker-compose.e2e.dbs.yml", "minio-init"):
        "e2e fixture: one-shot bucket creation against the fixture above",
    ("docker-compose.e2e.dbs.extended.yml", "sqlserver-e2e-init"):
        "e2e fixture: mssql-tools publishes no stable version tag we pin elsewhere",
}


def _compose_files():
    return sorted(
        os.path.basename(p)
        for p in glob.glob(os.path.join(REPO_ROOT, "docker-compose*.yml"))
    )


def _load_compose(name):
    with open(os.path.join(REPO_ROOT, name)) as fh:
        return yaml.load(fh, Loader=_ComposeLoader) or {}


def _compose_image_refs():
    """-> [(file, service, image)] for every service that names an image."""
    out = []
    for name in _compose_files():
        doc = _load_compose(name)
        for svc, spec in (doc.get("services") or {}).items():
            if isinstance(spec, dict) and isinstance(spec.get("image"), str):
                out.append((name, svc, spec["image"].strip()))
    return out


def _chart_image_refs():
    """-> [(file, dotted.key, image)] for the chart's flat image strings.

    Only the flat `image:`/`mcImage:` strings -- the third-party ones. First-party
    images use a `repository:` + `tag:` block whose tag defaults to
    `.Chart.AppVersion`, and test_shipped_images_are_publishable.py owns that.
    """
    out = []
    for path in sorted(glob.glob(os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "values*.yaml"))):
        with open(path) as fh:
            doc = yaml.safe_load(fh) or {}
        rel = os.path.relpath(path, REPO_ROOT)

        def walk(node, prefix):
            if isinstance(node, dict):
                for key, val in node.items():
                    dotted = f"{prefix}.{key}".lstrip(".")
                    if isinstance(val, str) and "image" in key.lower() and val.strip():
                        out.append((rel, dotted, val.strip()))
                    walk(val, dotted)
            elif isinstance(node, list):
                for item in node:
                    walk(item, prefix)

        walk(doc, "")
    return out


def _tag_of(image):
    """-> the tag, or None when the ref carries none (Docker reads that as `latest`).

    Splits on the last `/` first so a registry host's port (`host:5000/img`) is
    never mistaken for a tag. A digest pin (`img@sha256:...`) is not a tag and is
    treated as pinned, which it is -- more tightly than any tag.
    """
    if "@" in image:
        return None if ":" in image.split("@")[0].rsplit("/", 1)[-1] else "@digest"
    last = image.rsplit("/", 1)[-1]
    return last.split(":", 1)[1] if ":" in last else None


def _is_floating(image):
    if "@sha256:" in image:
        return False
    tag = _tag_of(image)
    if tag is None:
        return True  # untagged == :latest, just harder to see
    if "${" in tag:
        return False  # resolved from the install ref by design; see the docstring
    return tag.lower() in MOVING_TAGS


def _shipped_third_party():
    """Third-party image refs on the delivery surface, minus the e2e fixtures."""
    return [
        (f, svc, img)
        for f, svc, img in _compose_image_refs()
        if FIRST_PARTY not in img and (f, svc) not in ALLOWLIST
    ]


# --------------------------------------------------------------------------
# Vacuity guards. Every assertion below is parametrised over a discovered set,
# so a rename, a moved directory or a parser that quietly returns nothing would
# reduce this file to zero assertions and still report green. Pin each
# denominator so that failure is loud instead.
# --------------------------------------------------------------------------

def test_every_compose_file_parsed():
    files = _compose_files()
    assert len(files) >= 15, f"expected >=15 compose files at the repo root, found {files}"
    for name in files:
        doc = _load_compose(name)
        assert isinstance(doc, dict), f"{name} did not parse to a mapping"
    # The `!override` file specifically -- the trap this loader exists for.
    assert "docker-compose.oss.yml" in files
    assert (_load_compose("docker-compose.oss.yml").get("services")), \
        "docker-compose.oss.yml parsed to no services; the !override constructor regressed"


def test_the_image_census_is_not_empty():
    refs = _compose_image_refs()
    assert len(refs) >= 60, f"only {len(refs)} compose image refs found; the census under-read its subject"
    shipped = _shipped_third_party()
    assert len(shipped) >= 25, f"only {len(shipped)} shipped third-party refs; assertions below are near-vacuous"
    chart = _chart_image_refs()
    assert len(chart) >= 6, f"only {len(chart)} chart image strings found: {chart}"


# --------------------------------------------------------------------------
# The guard itself.
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "compose_file,service,image",
    [pytest.param(f, s, i, id=f"{f}::{s}") for f, s, i in _shipped_third_party()],
)
def test_no_shipped_third_party_image_floats(compose_file, service, image):
    assert not _is_floating(image), (
        f"{compose_file} service '{service}' pulls `{image}`, whose tag names a moving "
        f"target. Upstream can change what this starts with no diff here, so no test "
        f"or review can catch it. Pin it to a released version, or -- if it is a test "
        f"fixture rather than something a self-hoster pulls -- add it to ALLOWLIST "
        f"with the reason."
    )


@pytest.mark.parametrize(
    "values_file,key,image",
    [pytest.param(f, k, i, id=f"{os.path.basename(f)}::{k}") for f, k, i in _chart_image_refs()],
)
def test_no_chart_image_floats(values_file, key, image):
    assert not _is_floating(image), (
        f"{values_file} key `{key}` is `{image}`. This file's own rule at the `tag:` "
        f"key says it: an unpinned tag silently changes what a re-pulled pod runs."
    )


@pytest.mark.parametrize(
    "compose_file,service,image",
    [
        pytest.param(f, s, i, id=f"{f}::{s}")
        for f, s, i in _compose_image_refs()
        if FIRST_PARTY in i
    ],
)
def test_no_first_party_image_is_pinned_to_latest(compose_file, service, image):
    """First-party tags come from `${RSYNC_VERSION:-latest}`, never a bare `latest`.

    A hard-pinned `:latest` ignores RSYNC_VERSION, so `RSYNC_VERSION=v0.1.0
    docker compose up` runs that service at the newest release while its
    neighbours run 0.1.0 -- one stack built from two refs, invisible in a diff.
    """
    assert _tag_of(image) != "latest", (
        f"{compose_file} service '{service}' hard-pins `{image}`. Write it as "
        f"`:${{RSYNC_VERSION:-latest}}` so one variable moves every first-party image "
        f"together; a bare `latest` ignores RSYNC_VERSION and splits the stack across refs."
    )


def test_the_allowlist_has_no_stale_entries():
    """A stale exemption is an exemption for nothing, and it hides the next one.

    If an allowlisted service is renamed or deleted, the entry silently stops
    matching -- and someone reading the list still believes that file is covered.
    """
    live = {(f, s) for f, s, _ in _compose_image_refs()}
    stale = sorted(k for k in ALLOWLIST if k not in live)
    assert not stale, f"ALLOWLIST names services that no longer exist: {stale}"


def test_every_allowlisted_entry_actually_needed_the_exemption():
    """An entry that is already pinned should be deleted, not left standing.

    Otherwise the allowlist grows into a list of things nobody has re-checked,
    and its length stops meaning anything.
    """
    by_key = {(f, s): i for f, s, i in _compose_image_refs()}
    unnecessary = sorted(k for k in ALLOWLIST if not _is_floating(by_key.get(k, "")))
    assert not unnecessary, (
        f"these are pinned already and no longer need an exemption: {unnecessary}"
    )


# --------------------------------------------------------------------------
# Reach. Everything above is worthless on a PR that does not run it.
# --------------------------------------------------------------------------

CI_WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")


def _llm_filter_patterns():
    """-> the `llm` paths-filter patterns that decide whether this file runs.

    ci.yml gates `llm-service-unit` on `needs.changes.outputs.llm == 'true'`,
    and that output is the `llm` key of the dorny/paths-filter step's inline
    `filters` block -- a YAML string nested inside the workflow YAML.
    """
    doc = yaml.safe_load(open(CI_WORKFLOW))
    job = doc["jobs"]["changes"]
    step = next(
        st for st in job["steps"]
        if "paths-filter" in str(st.get("uses", "")) and "filters" in (st.get("with") or {})
    )
    return yaml.safe_load(step["with"]["filters"])["llm"]


def test_the_llm_job_is_still_the_one_gated_on_that_filter():
    """The premise of the next test: change the gate and it stops meaning anything."""
    doc = yaml.safe_load(open(CI_WORKFLOW))
    job = doc["jobs"]["llm-service-unit"]
    assert "needs.changes.outputs.llm == 'true'" in str(job.get("if", "")), (
        "llm-service-unit is no longer gated on the `llm` paths filter, so the "
        "reach test below is asserting against a filter that decides nothing. "
        "Point it at whatever gates the job now."
    )
    pats = _llm_filter_patterns()
    assert len(pats) >= 8, f"only {len(pats)} llm filter patterns read from ci.yml: {pats}"


@pytest.mark.parametrize(
    "subject",
    [pytest.param(f, id=f) for f in sorted(
        set(_compose_files())
        | {f for f, _, _ in _chart_image_refs()}
    )],
)
def test_the_ci_filter_covers_every_file_this_guard_reads(subject):
    """Every file the census reads must be able to trigger the job that reads it.

    Matching is fnmatch, not the picomatch dorny/paths-filter actually uses.
    fnmatch is the looser of the two -- its `*` crosses `/` where picomatch's
    does not -- so this can only ever be more permissive than CI, never
    stricter. It cannot fail a pattern CI would honour.
    """
    pats = _llm_filter_patterns()
    assert any(fnmatch.fnmatch(subject, p) for p in pats), (
        f"`{subject}` is read by this guard, but no `llm` paths-filter pattern in "
        f"ci.yml matches it. A PR touching only that file would skip "
        f"llm-service-unit, so the guard would not run on exactly the change it "
        f"exists to catch -- and a skipped check reads as a passing one.\n"
        f"Add a pattern covering it to the `llm:` filter in {os.path.relpath(CI_WORKFLOW, REPO_ROOT)}.\n"
        f"Patterns today: {pats}"
    )
