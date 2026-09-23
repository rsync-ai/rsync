"""The paid image gets its own name, and no public surface can publish or pull it.

Mechanism 4 of the four in §3.3 of `docs/internal/public-vs-cloud-feature-split.md`.
Mechanisms 1-3 keep the paid *source* out of the public repository. This one keeps the
paid *artifact* out of the public repository's release, which is a separate failure with
a worse blast radius: source that leaks can be deleted, but an image published under a
name the community pulls is on every self-hosted machine that ran `docker compose pull`.

THE TWO WAYS A NAME BECOMES A WRITE PATH
----------------------------------------
1. **The shared workflow builds it.** `.github/workflows/docker-publish.yml` is
   byte-identical in both repositories -- `scripts/flip/apply-ci-split.py` rewrites
   `runs-on` for named jobs and nothing else, and this file is not among them. So a
   `- service: <paid>` line added to its matrix is added to the PUBLIC release too. There
   it would build a directory the public tree does not contain, and the honest repair
   under deadline is to ship the directory.

2. **The name collides with a community image.** `ghcr.io/rsync-ai/api-gateway` is pulled
   by the chart, by the quickstart and by every self-host compose file. A paid image
   pushed to a name already on one of those lists does not sit beside the community
   image; it REPLACES it at that tag, and the next `pull` on a self-hosted machine
   fetches paid code. Nothing in the registry refuses this. The only thing that can
   refuse it is a guard that reads both lists.

So the assertion is: the paid name is absent from the shared publish matrix, and absent
from every surface a community install pulls from.

WHY THIS IMPORTS ITS PARSE INSTEAD OF WRITING ONE
-------------------------------------------------
`test_published_image_destination.py` already derives where a release pushes, straight
from the workflow's own `env.REGISTRY`/`env.ORG` and its matrix, including the 21
connector images built from a run-time matrix that carry no `- service:` line and that a
literal scan is blind to. A second parse of the same files is a second thing to drift,
and a guard that reads a file differently from the thing that consumes it can pass on
entries the real consumer sees. If that module is renamed, the import fails loudly here,
which is the correct failure -- not a silent skip.

WHAT THIS FILE DOES NOT ASSERT, YET
-----------------------------------
That some private-only workflow DOES publish the paid image. There is nothing to publish
at step 1: the tree is a boundary and a README, and the service skeleton is step 2. A
guard asserting a publish job exists would be asserting a fiction. This file reserves the
name; the positive half arrives with the thing it names.
"""

import os
import re
import subprocess

import pytest
import yaml

# Both parses this guard is measured against are imported, never re-written. See the
# module docstring: a second, drifting copy is the failure mode.
#
#   * where a release pushes, straight from the workflow's own env and matrix;
#   * how a compose file is read -- `yaml.safe_load` RAISES on Compose's `!override`
#     tag, which prod.yml, staging.yml and the OSS overlay all use, so a guard with its
#     own naive loader silently sweeps three fewer files than it reports.
from test_published_image_destination import (  # noqa: E402
    WORKFLOW,
    _chart_references,
    _published_images,
    _service_names,
    _strip_tag,
    _text,
)
from test_byo_parked_deps_in_every_compose_file import _ComposeLoader, _load  # noqa: E402

_TESTS = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(_TESTS))

# Declared with os.path.join(REPO_ROOT, <constants>) so
# test_ci_filter_covers_every_guard_subject.py can read this guard's subjects.
_PUBLISH_WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "docker-publish.yml")
_CHART_VALUES = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai", "values.yaml")
_QUICKSTART = os.path.join(REPO_ROOT, "docker-compose.quickstart.yml")

# The reserved name. Deliberately NOT one of the service names the workflow builds, and
# deliberately prefixed so it cannot be confused with a component of the community stack.
_PAID_SERVICE = "rsync-enterprise"

# Floors, all well under what was measured on 2026-09-22 (34 published images across 13
# static matrix entries and 21 discovered connectors; 20 tracked compose files) and well
# above zero, so a parse that stops matching cannot report a clean sweep over nothing.
_MIN_PUBLISHED = 10
_MIN_CHART_IMAGES = 5
_MIN_COMPOSE_FILES = 10
_MIN_COMPOSE_IMAGES = 20


def _compose_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "docker-compose*.yml"],
        cwd=REPO_ROOT, capture_output=True, text=True, check=False,
    )
    assert out.returncode == 0, f"git ls-files failed: {out.stderr.strip()}"
    return [p for p in out.stdout.split("\n") if p]


def _image_names(doc) -> set[str]:
    """Every image NAME (last path segment, tag stripped) a compose document pulls.

    Name, not full reference, because a collision is a collision whatever registry it is
    written against: `rsync-enterprise` and `ghcr.io/rsync-ai/rsync-enterprise:v1` are
    the same artifact from the puller's point of view once the prefix is defaulted.
    """
    names = set()
    for spec in (doc.get("services") or {}).values():
        if not isinstance(spec, dict):
            continue
        image = spec.get("image")
        if isinstance(image, str) and image:
            names.add(_strip_tag(image).rsplit("/", 1)[-1])
    return names


# --------------------------------------------------------------------------------------
# controls
# --------------------------------------------------------------------------------------


def test_the_name_extractor_finds_a_paid_image_when_one_is_there():
    """The positive control. Without it, a broken extractor reads as a clean stack."""
    forged = {
        "services": {
            "paid": {"image": f"ghcr.io/rsync-ai/{_PAID_SERVICE}:${{RSYNC_VERSION:-latest}}"},
            "gateway": {"image": "ghcr.io/rsync-ai/api-gateway:latest"},
        }
    }
    names = _image_names(forged)
    assert _PAID_SERVICE in names, (
        f"the extractor did not find {_PAID_SERVICE!r} in a document that names it: "
        f"{names}. Every sweep below would pass over a stack that pulls the paid image."
    )
    assert "api-gateway" in names, f"the extractor lost an ordinary image too: {names}"


def test_the_matrix_scan_finds_a_paid_service_when_one_is_there():
    """The same control for the workflow half."""
    forged = "      matrix:\n        include:\n          - service: %s\n" % _PAID_SERVICE
    assert _PAID_SERVICE in set(re.findall(r"-\s+service:\s+([a-z0-9-]+)", forged)), (
        "the matrix scan cannot see a service line, so the assertion that the shared "
        "publish workflow does not build the paid image is vacuous."
    )


# --------------------------------------------------------------------------------------
# the assertions
# --------------------------------------------------------------------------------------


def test_the_release_destination_was_parsed():
    """Denominator for everything that compares against the published set."""
    published = _published_images()
    assert len(published) >= _MIN_PUBLISHED, (
        f"only {len(published)} published images parsed from {WORKFLOW}: "
        f"{sorted(published)[:5]}. 34 were measured on 2026-09-22. The collision check "
        "below is comparing against an empty or gutted list."
    )


def test_the_shared_publish_workflow_never_builds_the_paid_image():
    """`docker-publish.yml` is byte-identical in both repos, so its matrix is public.

    Checked two ways on purpose: the parsed matrix, which is what actually builds, and
    the raw text, which catches the name arriving in a job name, an `if:`, a step or a
    comment. The second is the early warning -- by the time it is in the matrix the
    public release is already trying to build a directory it does not have.
    """
    assert _PAID_SERVICE not in _service_names(), (
        f"{_PAID_SERVICE!r} is in docker-publish.yml's build matrix. That workflow ships "
        "to the public repository unchanged, so the public release would build it too. "
        "The paid image is published by a private-only workflow; see §3.3 of "
        "docs/internal/public-vs-cloud-feature-split.md."
    )
    text = _text(_PUBLISH_WORKFLOW)
    assert _PAID_SERVICE not in text, (
        f"{_PAID_SERVICE!r} appears in docker-publish.yml. Even outside the matrix, that "
        "file is copied to the public repository byte for byte, so the name would be "
        "published there as an intention the public tree cannot satisfy."
    )


def test_the_paid_name_is_not_one_the_community_release_already_publishes():
    """A name already published is a name that can be overwritten at its tag."""
    community = _service_names()
    assert _PAID_SERVICE not in community, (
        f"{_PAID_SERVICE!r} is also a community image name. Pushing the paid image to it "
        "replaces the community image at that tag, and the next `docker compose pull` on "
        "a self-hosted machine fetches paid code."
    )


def test_the_chart_pulls_no_paid_image():
    refs = _chart_references(_CHART_VALUES)
    assert len(refs) >= _MIN_CHART_IMAGES, (
        f"only {len(refs)} image blocks parsed from {_CHART_VALUES}: {refs}. The sweep "
        "below is scanning nothing."
    )
    offenders = {k: v for k, v in refs.items() if v.rsplit("/", 1)[-1] == _PAID_SERVICE}
    assert not offenders, (
        f"the chart pulls the paid image: {offenders}. The chart is published from the "
        "public repository and installed by self-hosted users, who have no entitlement "
        "to it and no way to authenticate for it."
    )


@pytest.mark.parametrize("rel", sorted(_compose_files()), ids=lambda p: p)
def test_no_compose_file_pulls_the_paid_image(rel):
    """Every compose file, not just the quickstart.

    The quickstart is the one a self-hosted user runs, but `docker-compose.yml` and the
    BYO overlays are what a developer and CI run, and a paid image named in any of them
    is a paid image someone without entitlement is asked to pull. They are all public.
    """
    names = _image_names(_load(os.path.join(REPO_ROOT, rel)))
    assert _PAID_SERVICE not in names, (
        f"{rel} pulls {_PAID_SERVICE!r}. Every compose file in this repository ships to "
        "the public repository; a self-host or CI run would fail on an image it cannot "
        "pull, or succeed and run paid code. The paid service is composed privately."
    )


def test_the_compose_sweep_covered_the_files_it_claims_to():
    """Denominator for the parametrised sweep above, which is per-file and so cannot
    notice that it was handed no files or that every file parsed to nothing."""
    files = _compose_files()
    assert len(files) >= _MIN_COMPOSE_FILES, (
        f"found {len(files)} compose files, expected at least {_MIN_COMPOSE_FILES} "
        "(20 measured 2026-09-22). The per-file sweep above would have been "
        "parametrised over an empty list, which pytest reports as a pass."
    )
    total = sum(len(_image_names(_load(os.path.join(REPO_ROOT, f)))) for f in files)
    assert total >= _MIN_COMPOSE_IMAGES, (
        f"the compose files parsed to {total} image names in total, expected at least "
        f"{_MIN_COMPOSE_IMAGES}. The YAML parse has stopped finding `services.*.image`, "
        "so every file in the sweep above passed over an empty set."
    )


def test_the_compose_loader_reads_through_the_tag_that_would_hide_a_service():
    """`!override` is why the loader is imported and not written here.

    `yaml.safe_load` raises `ConstructorError` on it, and three files use it. The
    obvious repair -- catch the error and return `{}` -- reports a clean sweep of
    exactly the files most likely to name a paid image, so the loader is checked
    directly instead of being taken on trust.

    Checked as a parse of a document carrying the tag, not as "the tagged files
    contain images": `docker-compose.staging.yml` is a pure overlay whose 15 services
    declare no `image:` at all, inheriting them from the base file. Zero images there
    is the correct answer, and an assertion that could not tell that apart from a
    broken loader would have to be relaxed the first time it ran.
    """
    tagged = [
        f for f in _compose_files()
        if "!override" in open(os.path.join(REPO_ROOT, f), encoding="utf-8").read()
    ]
    assert tagged, (
        "no compose file uses `!override` any more. This guard imports a special loader "
        "for it; re-check whether that is still needed rather than leaving an untested "
        "dependency in place."
    )
    for rel in tagged:
        doc = _load(os.path.join(REPO_ROOT, rel))
        assert doc.get("services"), (
            f"{rel} uses `!override` and parsed to no services at all. The loader is "
            "not reading through the tag, and this file is being swept vacuously."
        )

    forged = yaml.load(
        "services:\n"
        "  paid:\n"
        "    depends_on: !override []\n"
        f"    image: ghcr.io/rsync-ai/{_PAID_SERVICE}:latest\n",
        Loader=_ComposeLoader,
    )
    assert _PAID_SERVICE in _image_names(forged), (
        "the loader lost a service that carries an `!override` key. A real compose file "
        "shaped like this one would be swept as if it named no images."
    )


def test_the_quickstart_is_among_the_files_that_were_swept():
    """The one a self-hosted user actually runs, named explicitly so a change to the
    glob cannot quietly drop it."""
    rel = os.path.relpath(_QUICKSTART, REPO_ROOT)
    assert rel in _compose_files(), (
        f"{rel} is no longer matched by the compose glob, so the highest-consequence "
        "file in the sweep is not being swept."
    )
