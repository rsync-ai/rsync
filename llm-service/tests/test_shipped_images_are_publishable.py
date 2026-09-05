"""Every image we hand a customer a way to pull must actually be published.

There are two delivery vehicles and they fail differently, so both are
covered here against the SAME matrix parser: docker-compose.quickstart.yml
(what install.sh runs) and the Helm chart (what a Kubernetes self-host runs).
The chart half exists because `connector-seed` was referenced by the chart as
an initContainer on the six pods that read the connector catalog (api-gateway,
orchestrator, temporal-adapter, llm-service, planner, tool-generator) while
being absent from the publish matrix -- compose hid that behind a `build:`
fallback, and Kubernetes has no
such fallback, so it would have surfaced as Init:ImagePullBackOff on a real
cluster rather than as anything visible in CI.

`install.sh` downloads exactly one file -- docker-compose.quickstart.yml -- and
then runs `docker compose pull` + `up -d`. It ships no source, so every service
a default install starts must resolve to a PULLABLE image. Two things have to
hold for that, and they are checked separately because they fail for different
reasons and get fixed by different people:

  1. the image is built at all, i.e. it appears in docker-publish.yml's matrix
  2. the tag the compose file asks for actually got minted

(2) is not checkable from the repo -- it depends on registry state -- so it is
recorded in the docstring of the tag test rather than asserted: `latest` is
gated on `github.ref_type == 'tag'` and the workflow only triggers on
`push: tags: ["v*.*.*"]` or workflow_dispatch. `v0.1.0` was the repo's first tag
(2026-08-19) and the first run of this workflow ever to reach a build step -- the
three earlier runs, from the twelve days before the trigger went tags-only, all died
at runner allocation with zero steps. Before v0.1.0 no `:latest` had ever been
minted for ANY matrix image (eight when this was written; nine after connector-seed was
added, and thirty once the discovered connector matrix landed -- which is why the
count is no longer transcribed into an assertion here). Verified by simulating the customer -- a
directory holding only the compose file and a generated .env -- where
`docker compose up --dry-run` 401s on api-gateway, mcp-minio, temporal-adapter,
orchestrator and frontend alike.

A profile-gated service is exempt: `--profile cdc` / `--profile generate` are
opt-in, so an unpublished image there does not break the default install.
"""

import os
import re
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
QUICKSTART = os.path.join(REPO_ROOT, "docker-compose.quickstart.yml")
WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "docker-publish.yml")
CHART_VALUES = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "values.yaml")

# Empty on purpose, and kept rather than deleted: it is the two-directional pin
# below. `llm-service` used to live here -- its build context carries the
# connector-generation moat (vendor_apis.yaml, auth_rules.yaml,
# capability_rules.yaml, learned_apis.jsonl), so publishing it would bake that
# into a pullable artifact irreversibly, and the quickstart pointed three
# services at it anyway. That was KI-QUICKSTART-PULLS-AN-IMAGE-THAT-IS-NEVER-
# PUBLISHED. It is closed by the two allowlist images the workflow now builds --
# `llm-service-oss` (Dockerfile.community) and `connector-lifecycle`
# (Dockerfile.oss) -- whose stripped set is llm-service/oss-strip-list.txt and
# whose boundary is enforced by tests/test_oss_image_boundary.py.
#
# Adding a name back here is not a fix. An entry means "we ship a way to pull an
# image we never publish", which is the bug this file exists to catch; the fix is
# a matrix entry, a profile gate, or an allowlist image.
KNOWN_UNPUBLISHABLE = set()


def _discovered_connectors(workflow_text):
    """The connector images the workflow builds from a DISCOVERED matrix.

    These carry no `- service:` line to grep -- the matrix is generated at run
    time from each connector's latest.json -- so the literal parser below is
    blind to all 21 of them. Left that way, this file would report a connector
    image as unpublishable the day one entered the quickstart.

    Gated on the discovery job still existing: if that job is deleted, this
    returns empty and the images correctly stop counting as published. Keying it
    off the connector tree alone would keep vouching for images nothing builds.
    """
    if "build-and-push-connectors:" not in workflow_text:
        return set()
    # git ls-files, matching the workflow: locally generated connectors are
    # untracked by design and are not published, so a filesystem walk would
    # claim images that do not exist.
    out = subprocess.run(
        ["git", "ls-files", "shared/mcp-connectors/public/**/latest.json"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    ).stdout.split()
    return {"mcp-" + os.path.basename(os.path.dirname(f)) for f in out}


def _publish_matrix():
    with open(WORKFLOW) as fh:
        text = fh.read()
    literal = set(re.findall(r"-\s+service:\s+([a-z0-9-]+)", text))
    return literal | _discovered_connectors(text)


def _quickstart_images():
    """-> {image_name: [(service, profiles), ...]} for ghcr.io/rsync-ai images."""
    with open(QUICKSTART) as fh:
        doc = yaml.safe_load(fh)
    out = {}
    for name, spec in (doc.get("services") or {}).items():
        if not isinstance(spec, dict):
            continue
        image = spec.get("image") or ""
        if "ghcr.io/rsync-ai/" in image:
            short = image.split("/")[-1].split(":")[0]
            out.setdefault(short, []).append((name, spec.get("profiles") or []))
    return out


def _ungated(image_map):
    """Images with at least one service that starts WITHOUT an opt-in profile."""
    return {img for img, svcs in image_map.items() if any(not p for _, p in svcs)}


def test_the_matrix_and_the_quickstart_were_both_parsed():
    """Both greps are brittle by nature -- if either returns nothing the rest of
    this file passes vacuously, which is worse than failing."""
    assert len(_publish_matrix()) >= 5, "docker-publish.yml matrix parsed as near-empty"
    assert len(_quickstart_images()) >= 5, "quickstart parsed as having near-no ghcr images"


@pytest.mark.parametrize("image", sorted(_ungated(_quickstart_images())))
def test_every_ungated_quickstart_image_is_built_by_the_publish_workflow(image):
    assert image in _publish_matrix(), (
        f"docker-compose.quickstart.yml starts {image} with no profile, so a default "
        f"`install.sh` run must pull it -- but docker-publish.yml never builds it. "
        f"Either add it to the matrix or put the service behind an opt-in profile."
    )


def test_an_unbuildable_image_is_at_least_profile_gated():
    """The inverse framing, so the failure message names the right remedy: any
    ghcr image absent from the matrix must be opt-in."""
    matrix = _publish_matrix()
    images = _quickstart_images()
    offenders = {
        img: [s for s, p in svcs if not p]
        for img, svcs in images.items()
        if img not in matrix and any(not p for _, p in svcs)
    }
    assert offenders == {}, (
        f"the set of unbuildable-and-ungated quickstart images changed: {offenders}. "
        f"It has been empty since the allowlist images landed; anything here is a "
        f"service a default `install.sh` run starts and cannot pull."
    )


def test_no_quickstart_image_is_absent_from_the_publish_matrix():
    """The strict form of the test above, now that the exception is gone.

    `test_an_unbuildable_image_is_at_least_profile_gated` only asks that an
    unbuildable image be opt-in, and for a while `mcp-context7` was exactly that:
    profile-gated, so a default `install.sh` run never touched it. That reasoning
    has a hole. `--profile generate` is a documented path -- the quickstart's own
    header advertises it -- and on that path an image the matrix does not build is
    a hard `pull access denied`, not a degraded feature. Compose has no "skip the
    image you cannot find" mode.

    The compose comment beside the service says its absence is tolerated by
    design (one call site, 3s timeout, try/except). True, and irrelevant here:
    that tolerance covers a context7 that is DOWN, which is a different event from
    a container that could never be created.

    So: every ghcr image the quickstart names, gated or not, is built. If this
    fails, either add the matrix entry or stop shipping the service.
    """
    matrix = _publish_matrix()
    images = _quickstart_images()
    assert len(images) >= 5, "quickstart parsed as having near-no ghcr images"
    missing = {
        img: sorted({s for s, _ in svcs})
        for img, svcs in images.items()
        if img not in matrix
    }
    assert missing == {}, (
        f"quickstart images that no publish-matrix entry builds: {missing}. "
        f"A profile gate is not a defence -- the profile is documented, so someone "
        f"will run it and get `pull access denied` for an image that was never pushed."
    )


def test_a_service_that_needs_no_build_context_ships_an_image():
    """install.sh downloads the compose file and nothing else, so a `build:` with
    no `image:` is a context the customer's box does not have.

    This asserted `== ["connector-deployer"]` until 2026-08-24 -- the exception
    was written down, which made it look considered, but writing a defect down
    does not stop it happening. It is the worse half of the unpublished-image
    bug, not a milder one: an unpublished image fails at pull with
    manifest-unknown, which at least names the artifact, whereas a build stanza
    aborts the whole `up` with

        unable to prepare context: path ".../connector-deployer" not found

    -- a message about a path on the customer's machine that points at nothing
    they did. Reproduced from a directory holding only the compose file and a
    generated .env (exit 1), with the same build from the repo root as the
    control (exit 0, `Built`), so the failure is the missing source tree and not
    the Dockerfile. Fixed by publishing the image.

    The list is empty and must stay empty. Re-pinning a name here is not a fix.
    """
    with open(QUICKSTART) as fh:
        doc = yaml.safe_load(fh)
    build_only = sorted(
        n for n, s in (doc.get("services") or {}).items()
        if isinstance(s, dict) and s.get("build") and not s.get("image")
    )
    assert build_only == [], (
        f"services in the quickstart that can only be BUILT, not pulled: {build_only}. "
        f"install.sh ships no source tree, so `docker compose up` tries to build from a "
        f"context that does not exist on the customer's box. Publish the image and "
        f"reference it by tag -- do not re-add the name to this assertion."
    )


def test_every_first_party_image_reads_the_same_version_knob():
    """One stack, one version.

    Half the first-party images honoured ${RSYNC_VERSION:-latest} and half were
    hard-pinned `:latest`, so `RSYNC_VERSION=v0.1.0 docker compose up` produced a
    v0.1.0 llm-service next to a latest orchestrator and said nothing. A
    cross-version stack does not fail at startup -- it fails later, at whichever
    wire format the two halves disagree about, which is the expensive place to
    find it. Third-party images (postgres, redis, kafka, minio, temporal) are
    excluded on purpose: they carry their own upstream versions.
    """
    with open(QUICKSTART) as fh:
        text = fh.read()
    pinned = re.findall(r"image:\s*(ghcr\.io/rsync-ai/[a-z0-9-]+:(?!\$\{RSYNC_VERSION)\S+)", text)
    assert pinned == [], (
        f"first-party images not on the shared version knob: {pinned}. "
        f"Use ghcr.io/rsync-ai/<name>:${{RSYNC_VERSION:-latest}} so one variable moves "
        f"the whole stack together."
    )


# ── Helm chart ──────────────────────────────────────────────────────────────────
# Same invariant, different delivery vehicle. Compose can fall back to `build:`;
# Kubernetes cannot, so an unpublished image is a harder failure there.


def _chart_images():
    """-> {repository: dotted.path} for every image block in the chart's values."""
    with open(CHART_VALUES) as fh:
        doc = yaml.safe_load(fh)
    out = {}

    def walk(node, path):
        if isinstance(node, dict):
            repo = node.get("repository")
            if isinstance(repo, str) and repo:
                out[repo] = path
            for key, val in node.items():
                walk(val, f"{path}.{key}" if path else key)
        elif isinstance(node, list):
            for item in node:
                walk(item, path)

    walk(doc, "")
    return out


def _chart_default_enabled(dotted):
    """Is the component owning this image on by default? Walks the `enabled` flag
    of each ancestor, so a component nested under a disabled parent counts as off."""
    with open(CHART_VALUES) as fh:
        doc = yaml.safe_load(fh)
    node, parts = doc, dotted.split(".")[:-1]  # drop the trailing `image`
    for part in parts:
        node = (node or {}).get(part)
        if not isinstance(node, dict):
            return True
        if node.get("enabled") is False:
            return False
    return True


def test_the_chart_values_were_parsed():
    """Vacuity guard -- a rename under deploy/helm/ must fail loudly, not silently
    reduce this half of the file to zero assertions."""
    assert len(_chart_images()) >= 8, f"chart values parsed as near-imageless: {_chart_images()}"


@pytest.mark.parametrize("image", sorted(_chart_images()))
def test_every_default_enabled_chart_image_is_built_by_the_publish_workflow(image):
    dotted = _chart_images()[image]
    if not _chart_default_enabled(dotted):
        pytest.skip(f"{image} ({dotted}) is disabled by default -- opting in is a choice")
    assert image in _publish_matrix(), (
        f"the Helm chart points {dotted} at `{image}`, which is on by default, but "
        f"docker-publish.yml never builds it. On Kubernetes there is no `build:` "
        f"fallback, so this is Init:ImagePullBackOff or ErrImagePull on a real cluster."
    )


def test_no_chart_image_is_absent_from_the_publish_matrix():
    """Two-directional pin against KNOWN_UNPUBLISHABLE, which is now empty.

    Every image the chart names is built by the publish matrix. If this grows, a
    new chart component just became undeployable on Kubernetes -- and unlike
    compose there is no `build:` fallback to hide it, so it surfaces as
    ErrImagePull on a real cluster and nowhere earlier.
    """
    matrix = _publish_matrix()
    unpublished = {img: path for img, path in _chart_images().items() if img not in matrix}
    assert set(unpublished) == KNOWN_UNPUBLISHABLE, (
        f"chart images absent from the publish matrix changed: {unpublished}"
    )
    for image, dotted in unpublished.items():
        assert not _chart_default_enabled(dotted), (
            f"{image} is unpublished AND enabled by default at {dotted} -- a default "
            f"`helm install` cannot succeed."
        )


# --- the hand-listed matrix must point at paths that exist -------------------------

def _literal_matrix_entries():
    """The hand-maintained `include:` entries of the build-and-push job.

    Parsed as YAML rather than by regex: the point of this test is that a path is
    wrong, and a regex that skips a malformed entry would hide exactly that.
    """
    with open(WORKFLOW) as fh:
        doc = yaml.safe_load(fh)
    return doc["jobs"]["build-and-push"]["strategy"]["matrix"]["include"]


def test_the_literal_matrix_scan_finds_entries():
    """Vacuity floor -- an empty include list would pass every assertion below."""
    entries = _literal_matrix_entries()
    assert len(entries) >= 9, f"only parsed {len(entries)} matrix entries -- parser broken"


@pytest.mark.parametrize(
    "entry", _literal_matrix_entries(), ids=lambda e: e["service"]
)
def test_every_hand_listed_build_path_exists(entry):
    """A matrix entry naming a path that is not there fails only at release time.

    mcp-minio shipped this way: its entry still pointed at the pre-versioning
    `internal/minio/Dockerfile` after the root copies were removed repo-wide, while
    debezium's entry was updated. Nothing caught it, because this workflow runs only
    on a `v*.*.*` tag -- and the repo had no tag at all until v0.1.0, at which point
    the job failed on the very first release.

    The connector half of the matrix cannot regress this way: it is discovered from
    each connector's own latest.json and hard-fails on a missing Dockerfile. This
    test gives the hand-listed half the same floor.
    """
    ctx = os.path.join(REPO_ROOT, entry["context"])
    dockerfile = os.path.join(REPO_ROOT, entry["dockerfile"])
    assert os.path.isdir(ctx), (
        f"{entry['service']}: build context {entry['context']} does not exist"
    )
    assert os.path.isfile(dockerfile), (
        f"{entry['service']}: dockerfile {entry['dockerfile']} does not exist -- "
        "this job will fail on the next tag. If the connector was versioned, point "
        "at versions/<current_version>/."
    )


# --- the kind harness builds the same images, from its own copy of the paths ----

KIND_BUILD_SCRIPT = os.path.join(
    REPO_ROOT, "deploy", "helm", "rsync-ai", "test", "kind", "build-and-load.sh"
)

_KIND_ENTRY = re.compile(r'^\s*"([a-z0-9-]+)\|([^|"]+)\|([^|"]+)', re.M)


def _kind_images():
    """The ALL_IMAGES entries of the kind build script: (name, context, dockerfile)."""
    with open(KIND_BUILD_SCRIPT) as fh:
        return _KIND_ENTRY.findall(fh.read())


def test_the_kind_image_scan_finds_entries():
    """Vacuity floor -- a regex that matches nothing would pass the test below."""
    entries = _kind_images()
    assert len(entries) >= 8, f"only parsed {len(entries)} kind images -- parser broken"


@pytest.mark.parametrize("name,context,dockerfile", _kind_images(), ids=lambda v: v)
def test_every_kind_build_path_exists(name, context, dockerfile):
    """The kind harness keeps its own copy of every build path, so it drifts too.

    mcp-minio was wrong in BOTH this script and docker-publish.yml, from the same
    root-copy removal. It went unnoticed here for a different reason than it did
    there: the script's default build set is KAFKA_PATH, which excludes mcp-minio,
    so only `build-and-load.sh all` ever touches the broken entry.

    Two files holding the same path is the actual defect; this test is the cheaper
    half of admitting that. If they are ever unified, delete this and keep the
    single source.
    """
    assert os.path.isdir(os.path.join(REPO_ROOT, context)), (
        f"{name}: kind build context {context} does not exist"
    )
    assert os.path.isfile(os.path.join(REPO_ROOT, dockerfile)), (
        f"{name}: kind dockerfile {dockerfile} does not exist -- "
        "`build-and-load.sh all` will fail on it"
    )


# ── Chart PROSE ─────────────────────────────────────────────────────────────────
# The two tests above read image *fields* -- values.yaml `repository:` keys and
# compose `image:` lines. Neither can see an image name written in English, and
# NOTES.txt is printed verbatim to every operator at `helm install` time, so a
# wrong name there is not a stale comment: it is an instruction.
#
# It was one. NOTES.txt item 6 told operators the generation tier runs
# `ghcr.io/rsync-ai/llm-service`, "deliberately never published by this repo's
# release workflow -- you must build and push that image yourself." Every clause
# was wrong by the time it was read. The tier runs `llm-service-oss` and
# `connector-lifecycle` (values.yaml generation.image / generation.toolGenerator
# .image), both of which the publish matrix builds; the image it named is the
# cloud one, which the chart never references. The block is gated on
# `generation.enabled`, which defaults TRUE, so the bad advice printed on every
# default install. The chart README had already been corrected -- this was the
# half of that sweep that got missed, which is exactly the shape a test is for.

CHART_DIR = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai")
_GHCR_LITERAL = re.compile(r"ghcr\.io/rsync-ai/([a-z0-9][a-z0-9._-]*)")


def _chart_prose_images():
    """-> {image_name: [relpath, ...]} for ghcr.io/rsync-ai literals in chart text.

    Scope is what an OPERATOR reads: templates (NOTES.txt above all), the chart
    README, and the values files they copy to build an overlay. Deliberately
    reads the RAW template rather than a render, because a name written as a
    literal is the thing under test -- `{{ .Values... }}` substitutions, which is
    what the fix replaced the literal with, correctly do not appear here at all.

    test/kind/JOURNAL.md is excluded on purpose. It is a historical lab record of
    what was observed on a given day, not an instruction to anyone; an image name
    that was accurate when written should not start failing the build the day
    that image leaves the matrix, because the only way to "fix" that is to edit
    the past. Everything in scope here is a document that tells a live operator
    what to do.

    Sourced from git ls-files so an untracked scratch file cannot fail the build.
    """
    out = subprocess.run(
        ["git", "ls-files", "-z", "deploy/helm/rsync-ai"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    ).stdout
    found, scanned = {}, 0
    for rel in (p for p in out.split("\0") if p):
        if os.path.basename(rel) == "JOURNAL.md":
            continue
        path = os.path.join(REPO_ROOT, rel)
        try:
            with open(path, encoding="utf-8") as fh:
                text = fh.read()
        except (UnicodeDecodeError, IsADirectoryError, FileNotFoundError):
            continue
        scanned += 1
        for name in _GHCR_LITERAL.findall(text):
            # `ghcr.io/rsync-ai/charts` is the OCI chart repo, not an image.
            if name == "charts":
                continue
            found.setdefault(name, []).append(rel)
    return found, scanned


def test_the_chart_prose_scan_finds_image_literals():
    """Vacuity guard. This scan is a regex over prose, so it degrades to zero
    findings for many uninteresting reasons -- a moved chart, a renamed registry,
    a rewritten NOTES. Zero findings would then read as 'all clear', which is the
    failure mode the file it lives in exists to prevent."""
    found, scanned = _chart_prose_images()
    # Two levers, because the image count alone is a weak denominator: today the
    # chart names exactly two images in prose, so `>= 2` is satisfied at the
    # boundary and would not notice one of them vanishing. The file count is the
    # lever that actually detects a moved/renamed chart.
    assert scanned >= 20, (
        f"only {scanned} chart files were readable; the chart moved or the glob "
        "broke, and this guard is now scanning almost nothing."
    )
    assert len(found) >= 2, (
        f"chart prose parsed as near-imageless ({found}); this guard would pass "
        "while checking nothing. Confirm the chart still names images in text."
    )


@pytest.mark.parametrize("image", sorted(_chart_prose_images()[0]))
def test_every_image_named_in_chart_prose_is_actually_published(image):
    where = ", ".join(sorted(set(_chart_prose_images()[0][image])))
    assert image in _publish_matrix(), (
        f"chart text names `ghcr.io/rsync-ai/{image}` ({where}) but docker-publish.yml "
        f"never builds it. If that text is an instruction -- NOTES.txt is printed to "
        f"the operator on every `helm install` -- it is an instruction to pull an image "
        f"that does not exist. Name an image the matrix builds, or read the repository "
        f"from values (`{{{{ .Values...image.repository }}}}`) so it cannot drift again."
    )


# --- the appVersion must name a tag that actually built the chart's images ---------
#
# The guards above pin the chart against the publish matrix *at HEAD*. That is the
# right check for "did someone add a chart component without adding it to the
# matrix", and it is green. It is silent about the question a user actually asks,
# which is different: the chart resolves its image tag to `.Chart.AppVersion`, so a
# default `helm install` pulls `<image>:<appVersion>` -- and whether those images
# exist is decided by what the *tag* `v<appVersion>` built, not by what HEAD builds.
#
# Those two sets drift apart silently, and only at release time, which is the most
# expensive moment to find out. v0.1.1 built 9 services; HEAD's matrix has 13. The
# four that joined afterwards include both images of the generation tier, which is
# on by default -- so `helm install ./deploy/helm/rsync-ai` with no overrides pulls
# two tags that were never published, and lands them in ImagePullBackOff.
#
# This is the same class as mcp-minio at v0.1.0: a tag-gated workflow banks its
# bugs until someone cuts a tag. The pin below unbanks them -- it is checked on
# every CI run, with no tag required.

# Chart images that the tag named by appVersion did NOT build.
#
# EXPECTED TO BE EMPTY. It is not empty today because the generation tier entered
# the matrix after v0.1.1 was cut, and the next release tag is deliberately being
# held until the repo goes public. When that tag lands this test will fail by
# announcing the set is now empty -- delete the two entries, and the release is
# genuinely complete. Do not add to this set to make a failure go away: a new name
# here means a new component is undeployable at the version the chart advertises.
APPVERSION_TAG_GAP = {"llm-service-oss", "connector-lifecycle"}


def _appversion():
    with open(os.path.join(CHART_DIR, "Chart.yaml")) as fh:
        return str(yaml.safe_load(fh)["appVersion"]).strip()


def _build_set_at(ref):
    """The images a given git ref's release workflow would build, or None if the
    ref does not exist. Static matrix plus the connectors discovered at that ref --
    both, because the chart names images from each."""
    probe = subprocess.run(["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"],
                           cwd=REPO_ROOT, capture_output=True, text=True)
    if probe.returncode != 0:
        return None
    text = subprocess.run(["git", "show", f"{ref}:.github/workflows/docker-publish.yml"],
                          cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout
    names = set(re.findall(r"-\s+service:\s+([a-z0-9-]+)", text))
    tree = subprocess.run(["git", "ls-tree", "-r", "--name-only", ref, "--",
                           "shared/mcp-connectors/public"],
                          cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout
    for path in tree.splitlines():
        if path.endswith("/latest.json"):
            names.add("mcp-" + os.path.basename(os.path.dirname(path)))
    return names


def _checkout_has_tags():
    """Whether this working copy can see release tags at all.

    Distinguishing "no tag was ever cut" from "this checkout did not fetch tags"
    matters more than it looks. Both make `git rev-parse v0.1.1` fail, and the
    honest-sounding message -- "the tag does not exist in this repository" -- is
    a false statement about a tag that exists on the remote and was simply never
    downloaded. actions/checkout fetches no tags by default, so the CI default is
    the ambiguous case, and a guard that cannot tell absence from not-having-
    looked is precisely the failure this whole file exists to catch.
    """
    out = subprocess.run(["git", "tag", "--list", "v*.*.*"],
                         cwd=REPO_ROOT, capture_output=True, text=True).stdout
    return bool(out.split())


def _tagless_checkout_hint():
    """The message to print when no tag is visible. Names both causes."""
    shallow = subprocess.run(["git", "rev-parse", "--is-shallow-repository"],
                             cwd=REPO_ROOT, capture_output=True, text=True).stdout.strip()
    return (
        "\nNO v*.*.* TAG IS VISIBLE IN THIS CHECKOUT, and there are exactly two reasons "
        "for that -- do not assume the first:\n"
        f"  (a) this checkout never fetched tags (is-shallow-repository={shallow!r}). "
        "actions/checkout does not fetch them by default; the llm-service-unit job sets "
        "`fetch-depth: 0` for this reason. If that `with:` block was dropped, THIS is "
        "your cause and no release is missing.\n"
        "  (b) no release has genuinely been cut yet, in which case `latest` was never "
        "minted and the advertised install cannot pull anything at all.\n"
        "Tell them apart with `git ls-remote --tags origin 'v*.*.*'`: output means (a), "
        "silence means (b)."
    )


def test_the_appversion_tag_build_set_was_actually_read():
    """Vacuity guard. A missing tag must FAIL here, never skip.

    A skip is what turns this into the bug it exists to catch: the whole point is
    that nothing fails when a release artifact is absent, so "the tag isn't there,
    nothing to check" is precisely the wrong answer.
    """
    tag = f"v{_appversion()}"
    built = _build_set_at(tag)
    assert built is not None, (
        f"Chart.yaml appVersion is {_appversion()!r}, so a default `helm install` pulls "
        f"`<image>:{_appversion()}` -- but {tag} could not be read here.\n"
        + (
            _tagless_checkout_hint() if not _checkout_has_tags() else
            f"Other tags ARE visible, so this checkout can see tags and {tag} genuinely "
            "does not exist: nothing ever built those images. Cut the tag, or move "
            "appVersion to a version that was actually released."
        )
    )
    assert len(built) >= 25, (
        f"{tag} parsed as having built only {len(built)} images; the workflow moved or "
        "the parse broke, and this pin is now comparing against almost nothing."
    )


def test_the_chart_appversion_names_a_release_that_built_its_images():
    tag = f"v{_appversion()}"
    built = _build_set_at(tag)
    assert built is not None, f"{tag} does not exist -- see the vacuity guard above"

    gap = {
        image for image, dotted in _chart_images().items()
        if image not in built and _chart_default_enabled(dotted)
    }
    assert gap == APPVERSION_TAG_GAP, (
        f"the set of default-enabled chart images missing from {tag} changed.\n"
        f"  expected: {sorted(APPVERSION_TAG_GAP) or '(none)'}\n"
        f"  actual:   {sorted(gap) or '(none)'}\n"
        "If it SHRANK to empty, a release tag was cut that covers the chart -- delete "
        "APPVERSION_TAG_GAP's contents and this becomes a real guarantee.\n"
        "Then remove the interim caveats that exist ONLY because of this gap, or they\n"
        "become false the day the tag lands: the blockquote in docs/deployment/kubernetes.md,\n"
        "the k8s blockquote in README.md, and KI-NO-RELEASE-TAG-COVERS-THE-SHIPPED-IMAGE-SET\n"
        "in CAPABILITIES.md. Naming them here is the point -- a constant that deletes itself\n"
        "while three documents keep warning about it is how the docs rot.\n"
        "If it GREW, a component that is on by default now points at an image the "
        "advertised version never published: `helm install` with no overrides puts "
        "that pod in ImagePullBackOff, and Kubernetes has no `build:` fallback."
    )


# --- the same gap on the compose path, for anyone who installs a release --------
#
# docker-compose.quickstart.yml pins its images to `:${RSYNC_VERSION:-latest}`, and
# `latest` is minted only on a tag (docker-publish.yml gates it on
# `github.ref_type == 'tag'`), so `latest` is whatever the NEWEST release tag
# built, not what HEAD builds.
#
# This USED to be the default `install.sh` path, and it is why a default install
# could not work: the script left RSYNC_VERSION empty. It now derives the image tag
# from RSYNC_REF (see the pins at the end of this file), so the default pairs main's
# compose with main's images and never reaches `latest`.
#
# The check below therefore no longer describes the default install -- it describes
# `RSYNC_REF=v0.1.1`, which is what anyone pinning to a release gets, and what the
# Helm chart still does unconditionally via `.Chart.AppVersion`. Keep it: a release
# that cannot be installed is a real defect, and this is the only thing that says so.
#
# The quickstart guards higher up compare against HEAD's matrix and are green. This
# one asks the question a user pinning a release asks: does the image that install
# pulls actually exist? There is no `build:` directive anywhere in the quickstart
# file -- zero, deliberately -- so a missing image is a hard failure of the pull,
# not a slow local build.

# Ungated quickstart images the newest release tag did NOT build. EXPECTED EMPTY;
# see APPVERSION_TAG_GAP above for why it is not, and delete entries rather than
# adding them. `mcp-context7` is absent from this set on purpose: it is gated
# behind the `generate` profile, so it cannot break a default install.
RELEASE_TAG_GAP_COMPOSE = {"connector-deployer", "llm-service-oss", "connector-lifecycle"}


def _newest_release_tag():
    out = subprocess.run(["git", "tag", "--list", "v*.*.*", "--sort=v:refname"],
                         cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout
    tags = [t for t in out.split() if t]
    return tags[-1] if tags else None


def test_a_release_tag_exists_to_compare_against():
    """Vacuity guard, and a real finding in its own right: `latest` is minted only
    on a tag, so with no tag at all `install.sh` cannot resolve a single image."""
    tag = _newest_release_tag()
    assert tag is not None, (
        "no v*.*.* tag is visible, so this pin has nothing to compare `install.sh` "
        "against." + _tagless_checkout_hint()
    )
    built = _build_set_at(tag)
    assert built is not None and len(built) >= 25, (
        f"{tag} parsed as having built {built and len(built)} images -- the parse broke "
        "and this pin is comparing against almost nothing."
    )


def test_the_install_script_pulls_images_the_newest_release_actually_built():
    tag = _newest_release_tag()
    built = _build_set_at(tag)
    # Explicit, because the alternative is `argument of type 'NoneType' is not
    # iterable` from the comprehension below -- a message that names neither the
    # tag nor the reason, on the one code path where the reader most needs both.
    assert built is not None, (
        "no release tag could be read, so there is nothing to compare against."
        + _tagless_checkout_hint()
    )
    gap = {img for img in _ungated(_quickstart_images()) if img not in built}
    assert gap == RELEASE_TAG_GAP_COMPOSE, (
        f"the set of ungated quickstart images missing from {tag} changed.\n"
        f"  expected: {sorted(RELEASE_TAG_GAP_COMPOSE) or '(none)'}\n"
        f"  actual:   {sorted(gap) or '(none)'}\n"
        "If it SHRANK to empty, a release covering the compose path was cut -- empty "
        "RELEASE_TAG_GAP_COMPOSE and `install.sh` is genuinely one command again.\n"
        "Then delete the interim caveat under the headline install in README.md, which\n"
        "says this stops at the image pull. Leaving it is worse than never adding it:\n"
        "the front door would warn readers away from a command that now works.\n"
        "If it GREW, `docker compose pull` now fails for a service that starts by "
        "default, and the quickstart has no `build:` fallback to hide it."
    )


# ---------------------------------------------------------------------------
# A chart is a promise that its images exist.
#
# `publish-chart` pushes the Helm chart to the registry. The moment it lands,
# `helm install oci://...` is a live command that any reader can run, and every
# image the chart names must already be pushed -- because Kubernetes has no
# `build:` fallback. A missing image is not a slow install, it is
# ImagePullBackOff on a cluster the operator has already committed to.
#
# So the chart job must depend on EVERY job that builds an image. Today it
# does. Nothing was holding it there: a repo-wide sweep for `publish-chart`
# found it named only in the workflow itself and in three .md files -- no test,
# no schema, nothing that fails if a future edit drops a dependency or adds a
# fourteenth build job the chart job does not wait for.
#
# That gap matters more here than it would in a normal workflow, because
# docker-publish.yml only runs on a tag. A regression would not show up on any
# PR or any `main` push; it would sit dormant and surface as a broken release,
# which is exactly how `mcp-minio` stayed broken for the workflow's entire life
# and how the v0.1.1/HEAD matrix drift (KI-NO-RELEASE-TAG-COVERS-THE-SHIPPED-
# IMAGE-SET) went unseen. A tag-gated workflow banks its bugs until someone
# cuts a tag; the counter is to assert its structure on every ordinary run.
#
# This is deliberately structural rather than a hardcoded list of job names: it
# asks which jobs build images (any job with a `service` matrix, plus the
# connector fan-out) and requires the chart job to transitively wait on all of
# them. Adding a fourteenth service to the existing matrix keeps passing, as it
# should -- but adding a NEW build job and forgetting to wire it fails here.
def _publish_workflow():
    with open(WORKFLOW) as fh:
        return yaml.safe_load(fh)


def _needs_of(job):
    needs = job.get("needs") or []
    return [needs] if isinstance(needs, str) else list(needs)


def _transitive_needs(jobs, name, seen=None):
    seen = set() if seen is None else seen
    for dep in _needs_of(jobs.get(name, {})):
        if dep not in seen:
            seen.add(dep)
            _transitive_needs(jobs, dep, seen)
    return seen


def _image_building_jobs(jobs):
    building = set()
    for name, job in jobs.items():
        matrix = (job.get("strategy") or {}).get("matrix")
        if matrix is None:
            continue
        # `matrix:` has two shapes here and only one of them is a mapping. The
        # first-party job spells out `include:` with a `service:` per entry; the
        # connector fan-out sets the whole matrix from a job output, so YAML
        # parses it as a bare STRING -- `${{ fromJson(...) }}` -- and calling
        # .get() on it raises rather than returning nothing. Treating a string
        # matrix as "not a build job" would be worse than the crash: the
        # connector fan-out is 21 of the images, and the chart would be free to
        # publish ahead of all of them with this test still green.
        if isinstance(matrix, str):
            if "fromJson" in matrix:
                building.add(name)
            continue
        include = matrix.get("include")
        if isinstance(include, list) and any(
            isinstance(e, dict) and e.get("service") for e in include
        ):
            building.add(name)
        if any(isinstance(v, str) and "fromJson" in v for v in matrix.values()):
            building.add(name)
    return building


def test_the_publish_job_graph_was_parsed():
    """Vacuity guard: every assertion below is a statement about a set of jobs.

    An empty parse would make them all pass while checking nothing -- the same
    shape as a `>= N` floor satisfied by a broken scan.
    """
    jobs = _publish_workflow()["jobs"]
    assert len(jobs) >= 3, f"only parsed {len(jobs)} jobs from {WORKFLOW}"
    assert "publish-chart" in jobs, (
        "no `publish-chart` job in docker-publish.yml. If chart publishing was "
        "renamed, retarget this test; if it was REMOVED, the chart is once again "
        "unreachable by any user and that is the bug this file exists to catch."
    )
    building = _image_building_jobs(jobs)
    assert len(building) >= 2, (
        f"found only {sorted(building)} image-building jobs; expected at least the "
        "first-party matrix and the connector fan-out. The detector stopped working."
    )


def test_the_chart_publishes_only_after_every_image_it_promises_is_pushed():
    jobs = _publish_workflow()["jobs"]
    building = _image_building_jobs(jobs)
    waits_for = _transitive_needs(jobs, "publish-chart")
    missing = sorted(building - waits_for)
    assert not missing, (
        "`publish-chart` does not wait for image-building job(s): "
        f"{missing}.\n"
        "The chart would be pushed while those images are still building or have "
        "failed, so `helm install oci://...` resolves a chart whose pods cannot "
        "pull. Kubernetes has no `build:` fallback -- this surfaces as "
        "ImagePullBackOff on the operator's cluster, not as a failed download.\n"
        f"  builds images: {sorted(building)}\n"
        f"  chart waits on: {sorted(waits_for)}\n"
        "Add the job to `needs:` on publish-chart."
    )


# ---------------------------------------------------------------------------
# The header comment's own arithmetic.
#
# docker-publish.yml opens with a prose account of why it is tag-gated, and that
# account carries two transcribed counts: how many images the hand-written
# matrix holds, and what a release therefore builds in total. Both went stale
# without a single test noticing -- the comment said "9 here / 30 total" from
# 2026-08-19 until 2026-08-29 while the matrix quietly grew to 13 (llm-service-
# oss, connector-lifecycle, connector-deployer and mcp-context7 all landed after
# the number was written), making the real total 34.
#
# A wrong number in a comment ships nothing broken, which is exactly why it
# survives: no build fails, no image is missing, and the next reader budgets
# Actions minutes against a figure that is 4 images light. The counts elsewhere
# in the repo are already safe by construction -- INVENTORY.md derives its "13
# images" from the matrix, CAPABILITIES.md pins its 30 to "as of #853", and
# every assertion in this file uses a `>=` floor rather than an exact count on
# purpose. This comment was the one place a bare number was still trusted.
#
# Rather than delete the numbers (they are genuinely useful -- the whole point
# of the comment is that the build is expensive), pin them to the thing they
# describe.
# ---------------------------------------------------------------------------


def _header_counts():
    """The two counts asserted in the `on:` block's prose, or None if absent.

    Deliberately scoped to the header: the file says "21 public mcp-<connector>
    images" again down at the matrix, and that sentence is about a different
    claim. Matching file-wide would couple this test to prose it does not own.
    """
    with open(WORKFLOW) as fh:
        header = fh.read().split("push:", 1)[0]
    total = re.search(r"a release now builds (\d+) images", header)
    listed = re.search(r"the (\d+)\s*\n?\s*#?\s*hand-listed", header)
    found = re.search(r"the (\d+)\s*\n?\s*#?\s*discovered connectors", header)
    return (
        int(total.group(1)) if total else None,
        int(listed.group(1)) if listed else None,
        int(found.group(1)) if found else None,
    )


def test_the_header_prose_is_still_there_to_check():
    """Vacuity floor. If the comment is rewritten and the phrases vanish, the
    two tests below would pass by matching nothing at all."""
    total, listed, found = _header_counts()
    assert total is not None, (
        "docker-publish.yml's header no longer says 'a release now builds N "
        "images'. If the prose was intentionally reworded, update the regex in "
        "_header_counts(); if the counts were dropped on purpose, delete these "
        "three tests rather than leaving them matching nothing."
    )
    assert listed is not None, (
        "docker-publish.yml's header no longer says 'the N hand-listed'. "
        "Same remedy as above."
    )
    assert found is not None, (
        "docker-publish.yml's header no longer says 'the N discovered "
        "connectors'. Same remedy as above."
    )


def test_the_header_hand_listed_count_matches_the_matrix():
    _, listed, _found = _header_counts()
    actual = len(_literal_matrix_entries())
    assert listed == actual, (
        f"docker-publish.yml's header claims {listed} hand-listed images; the "
        f"`include:` list actually holds {actual}.\n"
        "The matrix is the authority -- fix the comment, not the matrix. This "
        "drifts every time a service image is added, which is how it came to "
        "say 9 while the real figure was 13."
    )


def test_the_header_total_matches_hand_listed_plus_discovered():
    """The total is the number an operator budgets Actions minutes against."""
    total, listed, found = _header_counts()
    with open(WORKFLOW) as fh:
        discovered = len(_discovered_connectors(fh.read()))
    assert discovered > 0, (
        "no connectors discovered -- the parser is broken, and a total of "
        "`listed + 0` would then 'confirm' the hand-listed count as the total"
    )
    # Both halves, not just the sum. Checking only the total lets a partial fix
    # through: bump 34 to 35 for a new connector, leave "21 discovered" alone,
    # and the comment reads 13 + 21 = 35 while every assertion passes.
    assert found == discovered, (
        f"docker-publish.yml's header claims {found} discovered connectors; "
        f"the connector tree yields {discovered}.\n"
        "Fix the comment -- the discover-connectors job is the authority."
    )
    assert total == listed + discovered, (
        f"docker-publish.yml's header claims a release builds {total} images, "
        f"but the workflow builds {listed} hand-listed + {discovered} "
        f"discovered connectors = {listed + discovered}.\n"
        "Fix the comment."
    )


# ---------------------------------------------------------------------------
# The two halves of an install must name the same code.
#
# install.sh fetches the compose file from RSYNC_REF (default `main`) and starts
# the images that compose file names. Those two were independent: the compose
# tracked a moving branch while RSYNC_VERSION defaulted to empty, which compose
# resolves to `latest` -- a tag docker-publish.yml mints only on a release. So a
# default install paired HEAD's compose with v0.1.1's images.
#
# That is not a staleness that a fresh tag repairs for good. One side tracks a
# branch and the other a tag, so the very next commit to the compose file
# reopens it. install.sh now derives one from the other; these pin that.
#
# Deliberately behavioural rather than a regex over the source: the failure this
# guards against is a derivation that computes the WRONG tag, and a text match
# cannot tell a correct mapping from a broken one. The block is extracted and
# executed for representative refs instead.
INSTALL_SH = os.path.join(REPO_ROOT, "install.sh")

# Mirrors docker/metadata-action's own rules, which are what actually name the
# published tags: `type=semver,pattern={{version}}` for a tag and
# `type=ref,event=branch` for a branch. Keep these two in lockstep with
# docker-publish.yml's `tags:` blocks -- if that workflow's tag rules change,
# this mapping is what has to change with them.
REF_TO_IMAGE_TAG = [
    ("main", "main"),                  # the default install
    ("v0.1.1", "0.1.1"),               # release: {{version}} strips the leading v
    ("v0.2.0-rc1", "0.2.0-rc1"),       # pre-release keeps its suffix
    ("fix/foo", "fix-foo"),            # branch: slashes are not legal in a tag
    ("develop", "develop"),            # a branch that is not `main`
    # A branch whose name merely STARTS with v. Present because a mutation that
    # loosened the release-tag test to `^v` survived every other row here: it
    # would strip the v off this one and pull `2-rewrite`, an image tag nothing
    # ever published. Only a non-semver `v` name can tell the two regexes apart.
    ("v2-rewrite", "v2-rewrite"),
]


def _derivation_block():
    """install.sh's RSYNC_VERSION derivation, extracted for execution."""
    with open(INSTALL_SH) as fh:
        text = fh.read()
    m = re.search(r'^if \[\[ "\$\{RSYNC_REF\}".*?^fi$', text, re.S | re.M)
    return m.group(0) if m else None


def _derive(ref, explicit=None):
    block = _derivation_block()
    pre = f'RSYNC_VERSION={explicit!r}\n' if explicit is not None else ""
    script = f'set -euo pipefail\nRSYNC_REF={ref!r}\n{pre}{block}\nprintf "%s" "$RSYNC_VERSION"\n'
    out = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
    assert out.returncode == 0, f"derivation failed for ref={ref!r}: {out.stderr}"
    return out.stdout


def test_the_install_script_still_derives_an_image_tag_from_the_ref():
    """Vacuity floor. Without this, deleting the block would make every
    assertion below vanish rather than fail, and the pairing would silently
    revert to the drift this whole section exists to prevent."""
    assert _derivation_block() is not None, (
        "install.sh no longer derives RSYNC_VERSION from RSYNC_REF. If that was "
        "deliberate, the compose file and the images it starts are free to come "
        "from different commits again -- which shipped a compose wiring settings "
        "the pulled images had never heard of, and three services the resolved "
        "tag had never built."
    )


@pytest.mark.parametrize("ref,want", REF_TO_IMAGE_TAG)
def test_the_derived_image_tag_matches_what_the_release_workflow_publishes(ref, want):
    got = _derive(ref)
    assert got == want, (
        f"install.sh maps RSYNC_REF={ref} to image tag {got!r}, but "
        f"docker-publish.yml publishes {want!r} for that ref.\n"
        "A wrong tag here is not a degraded install -- the quickstart has zero "
        "`build:` directives, so every image fails to pull at once."
    )


def test_an_explicit_version_still_wins_over_the_derivation():
    """Pinning one ref's compose against another ref's images stays possible.
    Someone debugging a regression needs exactly that, and the derivation must
    not take it away."""
    assert _derive("main", explicit="0.1.1") == "0.1.1"
    assert _derive("v0.1.1", explicit="9.9.9") == "9.9.9"


def test_the_generated_env_records_the_resolved_tag_rather_than_a_blank():
    """A blank RSYNC_VERSION in the .env resolves to `latest` on every later
    `docker compose up` the operator runs by hand, quietly undoing the pairing
    the installer just computed. The value has to be written down."""
    with open(INSTALL_SH) as fh:
        text = fh.read()
    assert "RSYNC_VERSION=${RSYNC_VERSION:-}" not in text, (
        "install.sh writes a blank RSYNC_VERSION into the generated .env. Blank "
        "means `latest`, which is minted only on a release tag -- so the operator's "
        "next `docker compose up` silently swaps the images out from under the "
        "compose file the installer paired them with."
    )
    assert "RSYNC_VERSION=${RSYNC_VERSION}" in text, (
        "the generated .env no longer records the resolved image tag."
    )


# ---------------------------------------------------------------------------
# What refreshes the tag the DEFAULT install pulls?
#
# The pins above prove the two halves of an install name the same code. They do
# not ask whether anything ever MOVES that name, and the answer is no.
#
# `install.sh:17` defaults `RSYNC_REF=main`; the derivation maps that to the
# image tag `main`. Measured on `origin`, in both directions:
#
#   run 32271218739  push of tag v0.1.1   ->  0.1.1, 0.1, latest   (no `main`)
#   run 33264548995  workflow_dispatch    ->  main                 (nothing else)
#
# `type=ref,event=branch` is real and does fire -- on a DISPATCH. It cannot fire
# on the only automatic trigger the workflow has, because that trigger is a tag
# push and the ref is then `refs/tags/...`. So `:main` moves when a human runs
# the workflow by hand and at no other time, and cutting a release does not
# refresh the tag the advertised command pulls.
#
# The consequence is the defect #892 fixed, arriving through a different door.
# There the two halves named different tags; here they name matching tags built
# from different commits -- the compose file fetched live from the branch, the
# images left at whatever the last manual dispatch produced. Same failure: a
# compose file wiring settings the images have never heard of.
#
# The fix is to pin the default at a release, and it cannot land before one
# exists that covers the compose set (see RELEASE_TAG_GAP_COMPOSE). Until then
# the runbook must carry the step, which is what the first test requires; the
# second takes over the moment the release makes the switch safe.
RUNBOOK = os.path.join(REPO_ROOT, "docs", "internal", "public-flip-runbook.md")

# Matched as a substring so the surrounding prose stays free to change. Short
# enough to survive a rewording, specific enough that it cannot appear by
# accident in a document about something else.
REPIN_STEP = "repin `install.sh` to that tag"


def _default_ref():
    """The RSYNC_REF a bare `curl … | bash` uses -- the advertised command."""
    with open(INSTALL_SH) as fh:
        m = re.search(r'^RSYNC_REF="\$\{RSYNC_REF:-([^}"]*)\}"', fh.read(), re.M)
    return m.group(1) if m else None


def _is_release_ref(ref):
    """install.sh's own test for a release ref, so the two cannot disagree."""
    return bool(re.match(r"^v[0-9]+\.[0-9]+\.[0-9]+", ref or ""))


def _push_branches():
    """Branches whose push builds images, or None if the trigger block is gone.

    PyYAML resolves a bare `on:` key to the BOOLEAN True (YAML 1.1 says so), and
    the workflow spells it bare. Reading only `wf["on"]` would find nothing and
    quietly report "no branch triggers" for a workflow that had them.
    """
    wf = _publish_workflow()
    triggers = wf.get("on", wf.get(True))
    if not isinstance(triggers, dict):
        return None
    push = triggers.get("push")
    if not isinstance(push, dict):
        return []
    return push.get("branches") or []


def test_the_default_install_ref_and_the_triggers_were_both_read():
    """Vacuity floor. Both assertions below are statements about values parsed
    out of two files; if either parse breaks they stop meaning anything, and a
    regex that silently matches nothing is the failure mode this repo has
    shipped before."""
    assert _default_ref() is not None, (
        "install.sh no longer sets a default RSYNC_REF in the form this reads. "
        "The pin below cannot see what a bare `curl … | bash` resolves to."
    )
    assert _push_branches() is not None, (
        "docker-publish.yml's trigger block did not parse as a mapping, so the "
        "pin below cannot tell whether a branch push builds images."
    )


def test_the_tag_the_default_install_pulls_is_refreshed_by_something():
    ref = _default_ref()
    tag = _derive(ref)
    branches = _push_branches()

    by_release = _is_release_ref(ref)
    by_push = ref in branches

    if not os.path.isfile(RUNBOOK):
        # docs/internal/ does not ship (scripts/flip/excludes.txt), so the third
        # disjunct cannot be evaluated here. The first two still can, and if either
        # holds the assertion below stands on its own -- only the case that rests
        # ENTIRELY on the runbook is unverifiable, and that one skips by name rather
        # than reading a missing file as "not documented" and failing a public
        # checkout over a private document.
        if not (by_release or by_push):
            pytest.skip(
                "docs/internal/public-flip-runbook.md does not ship, so the "
                "documented-repin escape hatch cannot be checked in this tree. The "
                "condition it excuses is unchanged and is asserted in the repo where "
                "the runbook lives."
            )
        documented = False
    else:
        with open(RUNBOOK) as fh:
            documented = REPIN_STEP in fh.read()

    assert by_release or by_push or documented, (
        f"install.sh defaults to RSYNC_REF={ref!r} -> image tag {tag!r}, and nothing "
        "refreshes it.\n"
        f"  a release does not:  docker-publish.yml's only automatic trigger is a tag "
        f"push, and a tag ref cannot produce a branch tag (v0.1.1 minted 0.1.1/0.1/"
        f"latest and no `main`)\n"
        f"  a branch push does not: push.branches = {branches or '(none)'}\n"
        "  and the flip runbook no longer carries the step that repins it.\n"
        "So the advertised default pulls whatever a human last dispatched by hand, "
        "against a compose file fetched live from the branch. That is #892's defect "
        "with the tags matching and the COMMITS diverged.\n"
        "Restore the runbook step, add the branch to push.branches (read the header "
        "of docker-publish.yml first -- a push trigger there exhausted the org's "
        "Actions minutes), or default RSYNC_REF to a release."
    )


def test_the_installer_repins_to_a_release_once_one_covers_the_compose_set():
    """The switch the test above defers, armed on the condition that makes it safe.

    Gated on the live gap rather than on RELEASE_TAG_GAP_COMPOSE: a constant can
    be edited to make a test stop looking, and the sibling pin already fails if
    the constant and the gap disagree. So this skips only while a real release
    genuinely cannot be installed, and turns into an assertion the moment one can.
    """
    tag = _newest_release_tag()
    built = _build_set_at(tag)
    assert built is not None, (
        "no release tag could be read, so there is nothing to compare against."
        + _tagless_checkout_hint()
    )
    gap = {img for img in _ungated(_quickstart_images()) if img not in built}
    if gap:
        pytest.skip(
            f"{tag} never built {sorted(gap)}, which start by default -- repinning "
            "the installer to it would fail `docker compose pull` on them. This "
            "becomes an assertion when a release covers the whole compose set."
        )
    ref = _default_ref()
    assert _is_release_ref(ref), (
        f"{tag} now builds every ungated quickstart image, so the installer can "
        f"finally pin to a release -- but it still defaults to RSYNC_REF={ref!r}.\n"
        "That resolves to an image tag no release refreshes, so the default install "
        "keeps pulling whatever the last manual workflow_dispatch produced while "
        "fetching its compose file live from the branch.\n"
        "Set the default to a release ref, and delete the repin step from §6 of the "
        "flip runbook -- it exists only to hold this open until now."
    )


# ---------------------------------------------------------------------------
# The README made the opposite claim, adjacent to the true one.
#
# `install.sh`'s own header gets this right (:31-32): "Cutting a tag also fixes
# it, but only until the next commit touches the compose file: one side tracks a
# branch and the other a tag, so the gap reopens on its own."  README.md's
# user-facing note said "so the stack and its images cannot drift apart" -- and
# then, two sentences later, described the drift correctly.  Deriving the image
# tag from RSYNC_REF makes the two halves name the same REF; it does not make
# them move at the same RATE, and on the default `main` they do not: the compose
# file is fetched from the branch tip and changes with every commit, while
# `:main` moves only when a human dispatches the publish workflow.
#
# This guard is keyed on the note's own heading, so renaming the section turns it
# red rather than quietly skipping it.
# ---------------------------------------------------------------------------

README = os.path.join(REPO_ROOT, "README.md")
CODE_NOTE_HEADING = "**Which code you get.**"


def _readme_code_note():
    """The install note that tells a reader which code a bare install pulls.

    Returns the blockquote paragraph as one string, or None if the heading is
    gone -- which is a failure, not a skip.
    """
    with open(README) as fh:
        lines = fh.read().splitlines()
    start = next(
        (i for i, ln in enumerate(lines) if CODE_NOTE_HEADING in ln),
        None,
    )
    if start is None:
        return None
    out = []
    for ln in lines[start:]:
        if not ln.startswith(">"):
            break
        out.append(ln.lstrip("> ").rstrip())
    return " ".join(out)


def test_the_readme_install_note_was_actually_found():
    """Vacuity floor: every assertion below is silent if the note is not located."""
    note = _readme_code_note()
    assert note is not None, (
        f"{CODE_NOTE_HEADING} is gone from README.md. It is the only place a "
        "reader is told which code `curl ... | bash` actually pulls; the "
        "assertions below have no subject without it."
    )
    assert len(note) > 200, (
        f"the install note collapsed to {len(note)} chars -- too short to be "
        "saying anything about ref/tag pairing."
    )


def test_the_readme_does_not_promise_the_two_install_halves_stay_in_step():
    """A no-drift absolute is false on the default ref, and the README made it.

    Naming one ref removed the OLD defect (a moving `main` compose against
    `latest` images frozen at v0.1.1). It did not remove drift: measured on
    origin, the v0.1.1 tag push minted 0.1.1/0.1/latest and no `main`, and the
    2026-08-29 workflow_dispatch minted `main` and nothing else. So `:main`
    advances only on a manual dispatch while the compose file advances on every
    commit to the branch.
    """
    note = _readme_code_note()
    assert note is not None, "see test_the_readme_install_note_was_actually_found"
    lowered = note.lower()
    for claim in ("cannot drift", "can not drift", "never drift", "do not drift"):
        assert claim not in lowered, (
            f"README.md's install note claims the two halves {claim!r}. On the "
            f"default ref they do drift: the compose file is fetched from the "
            f"branch tip and moves every commit, while the image tag moves only "
            f"on a manual workflow_dispatch. install.sh:31-32 states this "
            f"correctly; the user-facing note must not contradict it."
        )


def test_the_readme_discloses_that_the_default_images_lag_the_branch():
    """Not-lying is not enough -- the reader needs the actual behaviour stated.

    Asserted positively so that deleting the sentence, rather than fixing it,
    cannot satisfy the test above.
    """
    note = _readme_code_note()
    assert note is not None, "see test_the_readme_install_note_was_actually_found"
    lowered = note.lower()
    assert "last publish" in lowered, (
        "README.md's install note no longer tells the reader that the default "
        "`main` images track the last publish rather than the branch's newest "
        "commit. That sentence is the disclosure; without it the note is silent "
        "on the one thing a self-hoster needs to know about a bare install."
    )
    assert _default_ref() is not None, (
        "install.sh's default-ref line no longer parses, so this test cannot "
        "tell which ref the note is describing."
    )
