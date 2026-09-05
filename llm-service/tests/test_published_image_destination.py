"""An image reference is a DESTINATION, not a name — check the whole string.

`tests/test_shipped_images_are_publishable.py` already pins every image the Helm
chart and the quickstart name against `docker-publish.yml`'s build matrix, and it
is green. It compares *bare repository names* — `api-gateway`, `mcp-minio` — and
never reads the registry those names are prefixed with:

  * the chart half (`_chart_images`) collects `image.repository` strings out of
    values.yaml and stops there; the word "registry" does not appear anywhere in
    that half of the file. `global.image.registry` is what `rsync-ai.image`
    (deploy/helm/rsync-ai/templates/_helpers.tpl:66-76) prepends to every one of
    them, and nothing checks it.
  * the quickstart half (`_quickstart_images`, :110) selects its subjects with
    `if "ghcr.io/rsync-ai/" in image`. The prefix under test is also the filter,
    so an image moved to a different registry does not fail — it leaves the
    denominator. Fifteen first-party services are named there; move one and
    fourteen still clear the `>= 5` vacuity floor.

So both halves stay green for a change that puts every pod in ErrImagePull:
retarget `global.image.registry` at a registry the workflow never pushes to and
the repository names still match the matrix perfectly. That is the same defect
class the file it complements exists to catch — "the artifact no workflow
publishes" — reached by moving the shelf instead of dropping the artifact. On
Kubernetes there is no `build:` fallback, so it surfaces on the operator's
cluster and nowhere earlier.

This file therefore compares FULL references. The published set is derived from
the workflow rather than assumed: `env.REGISTRY` + `env.ORG` + `matrix.service`,
read back out of the `docker/metadata-action` `images:` line of each build job,
so if the workflow ever stops composing its destination that way the derivation
fails loudly instead of vouching for a prefix nobody publishes to.

The third assertion is the `publish-chart` job graph, and it is deliberately NOT
the same detector as the existing one. That test asks which jobs carry a `service`
matrix (:771, `if matrix is None: continue`), which is a statement about the two
jobs that exist today; a job that builds and pushes ONE image with no `strategy:`
at all is invisible to it, and the chart would be free to publish ahead of that
image with the existing test still green. This one asks which jobs run a step that
pushes an image. A matrix scan and a step scan do not fail the same way — the same
reasoning the workflow's own discovery job uses when it cross-checks a git pathspec
against a filesystem walk (docker-publish.yml, "a walk and a pathspec do not fail
the same way").

Every scan here carries an explicit denominator: a zero from any of them is
reported as a broken probe, never as a pass.
"""

import os
import re
import subprocess

import pytest
import yaml

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "docker-publish.yml")
CHART_VALUES = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "values.yaml")
QUICKSTART = os.path.join(REPO_ROOT, "docker-compose.quickstart.yml")

# Floors, not exact counts: a new service must not have to edit this file. Each is
# set below today's real number so it detects a scan that broke, not a scan that
# grew. Today: 13 hand-listed + 21 discovered services, 12 chart image blocks,
# 15 first-party quickstart images, 2 image-pushing jobs.
MIN_PUBLISHED = 20
MIN_CHART_IMAGES = 10
MIN_QUICKSTART_IMAGES = 10
MIN_PUSH_JOBS = 2


def _text(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


def _doc(path):
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# What the workflow actually pushes, as full references.
# ---------------------------------------------------------------------------

# The destination is composed in the metadata-action step of each build job:
#     images: ${{ env.REGISTRY }}/${{ env.ORG }}/${{ matrix.service }}
# Matched rather than assumed. If a job starts composing its name some other way
# (a hardcoded org, a per-job env), the assumption below is wrong and the prefix
# this file vouches for is fiction, so the parse must fail instead of defaulting.
_IMAGES_LINE = re.compile(
    r"images:\s*\$\{\{\s*env\.REGISTRY\s*\}\}/\$\{\{\s*env\.ORG\s*\}\}/\$\{\{\s*matrix\.service\s*\}\}"
)


def _published_prefix(workflow=None):
    """-> "ghcr.io/rsync-ai", from the workflow's own env block."""
    doc = _doc(workflow or WORKFLOW)
    env = doc.get("env") or {}
    registry, org = env.get("REGISTRY"), env.get("ORG")
    assert registry and org, (
        f"docker-publish.yml no longer defines env.REGISTRY/env.ORG ({env!r}). "
        "Every assertion in this file is a statement about where images land; "
        "without those two it has nothing to compare against."
    )
    return f"{registry}/{org}"


def _discovered_connectors(workflow_text):
    """The connector images built from the RUN-TIME matrix.

    They carry no `- service:` line, so a literal scan is blind to all 21. Gated
    on the discovery job still existing: delete that job and these correctly stop
    counting as published rather than this file vouching for them off the
    connector tree alone. `git ls-files`, matching the workflow's own pathspec —
    locally generated connectors are untracked by design and are never published.
    """
    if "build-and-push-connectors:" not in workflow_text:
        return set()
    out = subprocess.run(
        ["git", "ls-files", "shared/mcp-connectors/public/**/latest.json"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=True,
    ).stdout.split()
    return {"mcp-" + os.path.basename(os.path.dirname(f)) for f in out}


def _published_images(workflow=None):
    """-> {full_reference} for every image a release pushes, e.g.
    "ghcr.io/rsync-ai/api-gateway"."""
    workflow = workflow or WORKFLOW
    text = _text(workflow)
    services = set(re.findall(r"-\s+service:\s+([a-z0-9-]+)", text))
    services |= _discovered_connectors(text)
    prefix = _published_prefix(workflow)
    return {f"{prefix}/{s}" for s in services}


def test_the_publish_destination_was_derived_from_the_workflow():
    """Vacuity guard for the set every other test in this file compares against.

    Two levers: the count (a broken service scan) and the shape of the
    metadata-action `images:` line (a workflow that composes its destination some
    other way, which would make the prefix below a guess).
    """
    published = _published_images()
    assert len(published) >= MIN_PUBLISHED, (
        f"only {len(published)} published references parsed from {WORKFLOW}: "
        f"{sorted(published)}. The service scan broke; every comparison in this "
        f"file would now pass by checking almost nothing."
    )
    hits = _IMAGES_LINE.findall(_text(WORKFLOW))
    assert len(hits) >= 2, (
        "the build jobs no longer compose their image name as "
        "`${{ env.REGISTRY }}/${{ env.ORG }}/${{ matrix.service }}` "
        f"(matched {len(hits)} times, expected one per build job). This file "
        "derives the published prefix from env.REGISTRY/env.ORG; if the jobs push "
        "somewhere else, that derivation is wrong and the guard is vouching for a "
        "registry nothing publishes to."
    )
    assert _published_prefix() == "ghcr.io/rsync-ai", (
        f"the publish destination moved to {_published_prefix()!r}. That is not "
        "necessarily wrong, but every deploy artifact in the repo has to move with "
        "it — update this pin only together with values.yaml, the quickstart, "
        "install.sh and the docs."
    )


# ---------------------------------------------------------------------------
# The chart: resolve references the way the chart's own helper does.
# ---------------------------------------------------------------------------


def _chart_references(values=None):
    """-> {dotted.path: full_reference} for every `{repository: ...}` image block.

    Mirrors `rsync-ai.image` (templates/_helpers.tpl:66-76) on the registry half:
    a repository containing "/" is treated as fully qualified and NOTHING is
    prepended; otherwise a per-component `registry` wins over
    `global.image.registry`. The tag half is deliberately not modelled — which tag
    resolves is `test_shipped_images_are_publishable.py`'s appVersion pin. This is
    about which registry the pull goes to.

    Plain-string `image:` values (postgres:16-alpine, apache/kafka:3.7.0) are
    third-party upstreams carrying their own coordinates and are correctly not
    collected: the block shape IS the first-party marker, because only a
    `{repository: ...}` block gets the org prefix prepended.
    """
    doc = _doc(values or CHART_VALUES)
    default_registry = (((doc.get("global") or {}).get("image") or {}).get("registry")) or ""
    out = {}

    def walk(node, path):
        if isinstance(node, dict):
            repo = node.get("repository")
            if isinstance(repo, str) and repo:
                if "/" in repo:
                    out[path] = repo          # fully qualified: no prefix is added
                else:
                    reg = node.get("registry") or default_registry
                    out[path] = f"{reg}/{repo}" if reg else repo
            for key, val in node.items():
                walk(val, f"{path}.{key}" if path else key)
        elif isinstance(node, list):
            for i, item in enumerate(node):
                walk(item, f"{path}[{i}]")

    walk(doc, "")
    return out


def test_the_chart_image_blocks_were_parsed():
    refs = _chart_references()
    assert len(refs) >= MIN_CHART_IMAGES, (
        f"only {len(refs)} image blocks parsed from {CHART_VALUES}: {refs}. "
        "The chart moved or the values were restructured; this half of the file "
        "is now asserting almost nothing."
    )


def test_the_chart_prepends_the_registry_the_workflow_pushes_to():
    """The single knob that moves every chart image at once.

    Named on its own, ahead of the per-image test below, because when it is wrong
    the per-image failures are 12 identical messages about a mistake made in one
    place.
    """
    doc = _doc(CHART_VALUES)
    registry = ((doc.get("global") or {}).get("image") or {}).get("registry")
    assert registry == _published_prefix(), (
        f"deploy/helm/rsync-ai/values.yaml sets global.image.registry={registry!r}, "
        f"but docker-publish.yml pushes to {_published_prefix()!r}. Every component "
        f"whose `image.repository` carries no '/' is prefixed with that value "
        f"(templates/_helpers.tpl:66-76), so a default `helm install` pulls the whole "
        f"stack from a registry no job ever pushed to. The repository NAMES still "
        f"match the build matrix, which is why the name-only guard in "
        f"test_shipped_images_are_publishable.py stays green through this."
    )


@pytest.mark.parametrize("dotted", sorted(_chart_references()))
def test_every_chart_image_resolves_to_something_a_release_publishes(dotted):
    ref = _chart_references()[dotted]
    assert ref in _published_images(), (
        f"the chart resolves {dotted} to `{ref}`, which no docker-publish.yml job "
        f"pushes. Published references: {sorted(_published_images())}.\n"
        f"Kubernetes has no `build:` fallback — this is ImagePullBackOff on a real "
        f"cluster and nothing earlier. Fix the reference, or add the image to the "
        f"build matrix."
    )


# ---------------------------------------------------------------------------
# The quickstart: select subjects by NAME, so a moved registry cannot hide.
# ---------------------------------------------------------------------------


def _service_names(workflow=None):
    return {ref.rsplit("/", 1)[-1] for ref in _published_images(workflow)}


def _strip_tag(image):
    """`ghcr.io/rsync-ai/api-gateway:${RSYNC_VERSION:-latest}` -> the repo half.

    Split on the FIRST colon of the last path segment, not the last colon of the
    string: every first-party tag in the quickstart is `${RSYNC_VERSION:-latest}`,
    a shell default-expansion that contains a colon of its own. `rsplit(":", 1)`
    cuts inside the expansion and yields `.../api-gateway:${RSYNC_VERSION`, which
    matches no service and silently empties this scan — which is exactly what it
    did until the denominator assertion below failed and said so.
    """
    head, _, last = image.rpartition("/")
    name = last.split(":", 1)[0]
    return f"{head}/{name}" if head else name


def _quickstart_references(quickstart=None, workflow=None):
    """-> {service: reference} for the first-party images the quickstart names.

    First-party is decided by the image's NAME — its last path segment matches a
    service the workflow builds — and NOT by its registry prefix. That is the
    whole point: selecting on `"ghcr.io/rsync-ai/" in image` (which is what the
    existing guard does at :110) makes the prefix under test also the filter, so
    an image moved off it leaves the denominator instead of failing.

    A third-party image that happened to share a name with one of our services
    would be collected here and would fail. That is the correct outcome: it means
    the stack is pulling somebody else's image where ours is meant to run.
    """
    doc = _doc(quickstart or QUICKSTART)
    ours = _service_names(workflow)
    out = {}
    for name, spec in (doc.get("services") or {}).items():
        if not isinstance(spec, dict):
            continue
        image = spec.get("image")
        if not isinstance(image, str) or not image:
            continue
        ref = _strip_tag(image)
        if ref.rsplit("/", 1)[-1] in ours:
            out[name] = ref
    return out


def test_the_quickstart_first_party_images_were_found():
    refs = _quickstart_references()
    assert len(refs) >= MIN_QUICKSTART_IMAGES, (
        f"only {len(refs)} first-party images matched in {QUICKSTART}: {refs}. "
        "install.sh downloads that file and nothing else, so this is the whole "
        "Docker-on-a-VM delivery path; a near-empty match means the scan broke."
    )


@pytest.mark.parametrize("service", sorted(_quickstart_references()))
def test_every_first_party_quickstart_image_is_pulled_from_where_it_is_pushed(service):
    ref = _quickstart_references()[service]
    assert ref in _published_images(), (
        f"docker-compose.quickstart.yml points service `{service}` at `{ref}`, which "
        f"no docker-publish.yml job pushes. The image NAME is one we build, so this "
        f"is the registry half being wrong — and a name-only guard cannot see it. "
        f"install.sh ships no source tree, so `docker compose pull` fails with "
        f"manifest-unknown / pull access denied on the customer's first run.\n"
        f"Published references: {sorted(_published_images())}"
    )


# ---------------------------------------------------------------------------
# publish-chart must wait for every job that pushes an image — detected by STEP.
# ---------------------------------------------------------------------------


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


def _job_pushes_an_image(job):
    """True if any STEP of this job pushes a container image.

    Two shapes, because both are used in the wild and a release only has to slip
    through one of them:
      * `uses: docker/build-push-action@...` with `push: true` (or an expression,
        which is treated as pushing — an unevaluatable gate is not a defence).
      * a `run:` block invoking `docker push` / `buildx ... --push`.

    `helm push` is deliberately not matched: publish-chart itself pushes the
    chart, and counting that would make the job depend on itself.
    """
    for step in job.get("steps") or []:
        if not isinstance(step, dict):
            continue
        uses = step.get("uses") or ""
        if uses.split("@")[0].endswith("docker/build-push-action"):
            push = (step.get("with") or {}).get("push")
            if push is False or str(push).strip().lower() == "false":
                continue
            return True
        run = step.get("run") or ""
        if re.search(r"docker\s+push\b", run) or re.search(r"buildx\s+build[^\n]*--push", run):
            return True
    return False


def _image_pushing_jobs(workflow=None):
    jobs = _doc(workflow or WORKFLOW)["jobs"]
    return {name for name, job in jobs.items() if isinstance(job, dict) and _job_pushes_an_image(job)}


def test_the_image_pushing_jobs_were_detected_by_their_steps():
    """Vacuity guard. This detector is the independent one — it must not quietly
    degrade to the empty set, which would make the wiring assertion below pass
    while the chart waits for nothing."""
    jobs = _doc(WORKFLOW)["jobs"]
    assert "publish-chart" in jobs, (
        "no `publish-chart` job in docker-publish.yml. If it was renamed, retarget "
        "this test; if it was removed, the chart is unpublishable again."
    )
    pushing = _image_pushing_jobs()
    assert len(pushing) >= MIN_PUSH_JOBS, (
        f"the step-based detector found {sorted(pushing)} — fewer than "
        f"{MIN_PUSH_JOBS} image-pushing jobs. Expected at least the first-party "
        f"matrix and the connector fan-out. The detector stopped working, so the "
        f"assertion below is vacuous."
    )
    assert "publish-chart" not in pushing, (
        "publish-chart itself was detected as pushing an IMAGE. It pushes the chart "
        "(`helm push`), which must not count, or the wiring test below would demand "
        "the job wait on itself."
    )


def test_the_chart_job_waits_for_every_job_that_pushes_an_image():
    """A chart is a promise that the images it names exist.

    The existing guard asks the same question of jobs that carry a `service`
    matrix. This one asks it of jobs that run a push STEP, which also catches a
    single-image job written with no `strategy:` at all — invisible to a matrix
    scan, and a chart published ahead of it is ImagePullBackOff for whatever it
    builds.
    """
    jobs = _doc(WORKFLOW)["jobs"]
    pushing = _image_pushing_jobs()
    waits_for = _transitive_needs(jobs, "publish-chart")
    missing = sorted(pushing - waits_for)
    assert not missing, (
        f"`publish-chart` does not wait for image-pushing job(s): {missing}.\n"
        f"The chart would be pushed while those images are still building or have "
        f"failed, so `helm install oci://...` resolves a chart whose pods cannot "
        f"pull.\n"
        f"  pushes images: {sorted(pushing)}\n"
        f"  chart waits on: {sorted(waits_for)}\n"
        f"Add the job to `needs:` on publish-chart."
    )
