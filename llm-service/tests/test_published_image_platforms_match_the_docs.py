"""Three files describe which platforms rsync.ai publishes. Only one of them builds.

`docker-publish.yml` sets no `platforms:` key on either `docker/build-push-action`
step, and every job runs on `ubuntu-latest`, so buildx tags each image for the
runner's own platform and nothing else: all 14 published images carry a single
`linux/amd64` entry. Meanwhile `docs/deployment/cloud-options.md` recommended an
ARM64 Oracle A1.Flex VM as the flagship free tier, and said multi-arch was fine.

That claim was true when it was written -- the product published no images then,
and the section was about third-party dependencies. It went false silently, the
way a status claim does: nothing fires when a doc's premise expires.

So the docs no longer assert a platform set; they *declare* the one they were
written against, in a `<!-- published-platforms: ... -->` sentinel, and this file
computes the real set from the workflow and compares. It is deliberately
bidirectional. Adding arm64 to the workflow is a good change, and it makes the
warnings in both docs and the preflight in `install.sh` wrong in the obstructive
direction -- turning the guard red is how the person doing it finds out.
"""

import os
import re

import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "docker-publish.yml")
INSTALL_SH = os.path.join(REPO_ROOT, "install.sh")
DOCS = [
    os.path.join(REPO_ROOT, "docs", "deployment", "cloud-options.md"),
    os.path.join(REPO_ROOT, "docs", "deployment", "kubernetes.md"),
]

# What a GitHub runner label builds when a step names no platform. Kept explicit:
# an unrecognised label must stop this file rather than be guessed into amd64,
# because guessing produces a green run that describes a chart nobody published.
RUNNER_PLATFORM = {
    "ubuntu-latest": "linux/amd64",
    "ubuntu-24.04": "linux/amd64",
    "ubuntu-22.04": "linux/amd64",
    "ubuntu-24.04-arm": "linux/arm64",
    "ubuntu-22.04-arm": "linux/arm64",
}


def _published_platforms():
    """The platform set `docker-publish.yml` actually produces, per build step."""
    doc = yaml.safe_load(open(WORKFLOW, encoding="utf-8"))
    platforms, steps_seen = set(), 0

    for job_name, job in doc["jobs"].items():
        for step in job.get("steps") or []:
            if "build-push-action" not in str(step.get("uses", "")):
                continue
            steps_seen += 1
            with_ = step.get("with") or {}
            if with_.get("platforms"):
                platforms |= {p.strip() for p in str(with_["platforms"]).split(",") if p.strip()}
                continue
            # No platforms: key -- buildx builds for the runner, and nothing else.
            runs_on = job.get("runs-on")
            label = runs_on if isinstance(runs_on, str) else None
            assert label in RUNNER_PLATFORM, (
                f"job {job_name!r} builds images on runs-on={runs_on!r}, which this guard "
                "cannot map to a platform. Add it to RUNNER_PLATFORM."
            )
            platforms.add(RUNNER_PLATFORM[label])

    # Anti-vacuity: no build steps means the set is empty and every comparison
    # below passes for the wrong reason.
    assert steps_seen >= 2, f"found {steps_seen} build-push-action steps in docker-publish.yml -- expected the image and connector builds"
    return platforms


def _sentinel(path):
    m = re.search(r"<!--\s*published-platforms:\s*(.+?)\s*-->", open(path, encoding="utf-8").read())
    assert m, (
        f"{os.path.relpath(path, REPO_ROOT)} has no `<!-- published-platforms: ... -->` "
        "sentinel. Its architecture prose is then a free-floating claim with nothing "
        "checking it, which is the exact defect this guard exists to prevent."
    )
    return {p.strip() for p in m.group(1).split(",") if p.strip()}


def test_each_doc_declares_the_platform_set_the_workflow_really_builds():
    actual = _published_platforms()
    for path in DOCS:
        rel = os.path.relpath(path, REPO_ROOT)
        assert _sentinel(path) == actual, (
            f"{rel} was written against platforms {sorted(_sentinel(path))}, but "
            f"docker-publish.yml now publishes {sorted(actual)}.\n"
            "Rewrite the architecture section for the new set, then update the sentinel. "
            "If arm64 was just added, the warning block and the ARM64 truth table are now "
            "wrong in the direction that turns readers away from a platform that works."
        )


def test_the_installer_preflight_tracks_the_same_set():
    """`install.sh` compares the daemon's arch to a literal. That literal is a claim too.

    The coupling is exact rather than textual: the arch the preflight accepts is
    read out of the comparison itself, so a workflow that moved to arm64-only
    would leave the installer rejecting the only host that can run the images.
    """
    src = open(INSTALL_SH, encoding="utf-8").read()
    actual = _published_platforms()

    if len(actual) > 1:
        assert "check_arch" not in src, (
            f"docker-publish.yml now publishes {sorted(actual)}, so the images are multi-arch "
            "and install.sh's check_arch() preflight turns away hosts that would work. Remove it."
        )
        return

    only_arch = next(iter(actual)).split("/", 1)[1]
    assert re.search(r'^\s*check_arch\s*$', src, re.M), (
        "install.sh has no check_arch call in main(). The images are single-platform "
        f"({sorted(actual)}), so a mismatched host fails at `docker compose pull` with a "
        "manifest error, or worse runs under qemu and dies with `exec format error`."
    )
    accepted = set(re.findall(r'\[\[\s*"\$darch"\s*==\s*"([a-z0-9_]+)"\s*\]\]', src))
    assert accepted == {only_arch}, (
        f"install.sh's preflight accepts {sorted(accepted) or 'nothing'}, but docker-publish.yml "
        f"publishes {sorted(actual)}. The installer would reject the hosts the images run on."
    )


def test_the_preflight_warns_rather_than_exiting_when_the_daemon_is_unreadable():
    """A third outcome, not two -- and the one a mutation would quietly drop.

    `docker version --format` returns empty against a daemon that is up but
    answering oddly. Treating that as arm64 would stop an amd64 install for no
    reason; treating it as amd64 would hide the real warning. It has to warn and
    continue, which is also what check_ram does with an unreadable total.
    """
    src = open(INSTALL_SH, encoding="utf-8").read()
    body = src[src.index("check_arch() {") : src.index("check_ram() {")]
    empty_branch = re.search(r'if\s+\[\[\s+-z\s+"\$darch"\s+\]\];\s*then(.*?)\n\s*fi', body, re.S)
    assert empty_branch, "check_arch no longer has a distinct branch for an unreadable daemon arch"
    assert "return 0" in empty_branch.group(1), (
        "check_arch now exits when it cannot read the daemon's architecture. An unreadable "
        "arch is not evidence of a wrong one -- warn and continue, as check_ram does."
    )
