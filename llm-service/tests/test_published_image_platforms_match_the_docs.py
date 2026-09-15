"""Three files describe which platforms rsync.ai publishes. Only one of them builds.

`docker-publish.yml` is the only source of truth: the `platforms:` key on each
`docker/build-push-action` step -- or, with no key, the runner's own platform --
is the set every `ghcr.io/rsync-ai/*` image carries. The deployment docs once
recommended an ARM64 Oracle A1.Flex VM while every image was `linux/amd64` only.
That claim had been true when it was written, and went false silently, the way a
status claim does: nothing fires when a doc's premise expires.

So the docs do not assert a platform set; they *declare* the one they were
written against, in a `<!-- published-platforms: ... -->` sentinel, and this file
computes the real set from the workflow and compares. It is deliberately
bidirectional. Adding a platform makes single-arch warnings, the installer's arch
preflight and the quickstart's `platform:` pins wrong in the obstructive
direction; dropping one makes the docs promise hosts the images cannot run on.
Turning the guard red is how the person making either change finds out.
"""

import os
import re

import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "docker-publish.yml")
INSTALL_SH = os.path.join(REPO_ROOT, "install.sh")
QUICKSTART = os.path.join(REPO_ROOT, "docker-compose.quickstart.yml")
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
    if len(_published_platforms()) > 1:
        # Multi-arch: there is no preflight to hold to anything, and the test above
        # asserts it stays gone. This branch pins the single-arch shape only.
        return
    src = open(INSTALL_SH, encoding="utf-8").read()
    body = src[src.index("check_arch() {") : src.index("check_ram() {")]
    empty_branch = re.search(r'if\s+\[\[\s+-z\s+"\$darch"\s+\]\];\s*then(.*?)\n\s*fi', body, re.S)
    assert empty_branch, "check_arch no longer has a distinct branch for an unreadable daemon arch"
    assert "return 0" in empty_branch.group(1), (
        "check_arch now exits when it cannot read the daemon's architecture. An unreadable "
        "arch is not evidence of a wrong one -- warn and continue, as check_ram does."
    )


def test_quickstart_pins_the_platform_it_publishes_on_every_ghcr_service():
    """`check_arch()` warning-then-continuing is not the same as continuing successfully.

    The two tests above hold the WARNING accurate; neither one holds the "yes,
    continue anyway" path WORKING. Before this guard, an operator who answered
    yes still hit `docker compose pull` asking the registry for a manifest that
    does not exist -- `no matching manifest for linux/arm64/v8` -- because
    nothing in docker-compose.quickstart.yml told compose which platform to
    request for the 14 `ghcr.io/rsync-ai/*` images it pulls. That is the same
    failure the warning describes, arriving one step later than the guard could
    see it: this file only ever parsed the warning text and install.sh, never
    the compose file operators actually run.

    Third-party images (postgres, kafka, redis, temporal, minio) are deliberately
    exempt -- they already publish native arm64 manifests, and pinning those too
    would only add emulation overhead with no upside.
    """
    doc = yaml.safe_load(open(QUICKSTART, encoding="utf-8"))
    actual = _published_platforms()

    ghcr_services = {
        name: svc
        for name, svc in doc["services"].items()
        if str(svc.get("image", "")).startswith("ghcr.io/rsync-ai/")
    }
    # Anti-vacuity: a moved or renamed image reference would otherwise let this
    # guard pass over an empty set.
    assert len(ghcr_services) >= 10, (
        f"found only {len(ghcr_services)} ghcr.io/rsync-ai/* services in "
        f"{os.path.relpath(QUICKSTART, REPO_ROOT)} -- expected at least 10. Did the image "
        "prefix or the compose file move?"
    )

    if len(actual) > 1:
        # Multi-arch publish: a hardcoded pin would now be the obstruction --
        # it would stop compose negotiating the arm64 manifest on hosts that
        # could run it natively.
        pinned = {n for n, svc in ghcr_services.items() if svc.get("platform")}
        assert not pinned, (
            f"docker-publish.yml now publishes {sorted(actual)}, so the images are multi-arch. "
            f"Remove the now-obstructive `platform:` pin from: {sorted(pinned)}."
        )
        return

    only_platform = next(iter(actual))
    unpinned = {n for n, svc in ghcr_services.items() if svc.get("platform") != only_platform}
    assert not unpinned, (
        f"docker-publish.yml publishes {sorted(actual)} only, but these "
        f"docker-compose.quickstart.yml services don't pin `platform: {only_platform}`: "
        f"{sorted(unpinned)}. Without the pin, `docker compose pull` on a host whose daemon "
        "architecture differs asks the registry for a manifest that was never published and "
        "fails with `no matching manifest for linux/arm64/v8` -- even after check_arch() has "
        "already warned the operator and they chose to continue."
    )


def test_a_build_for_a_platform_the_runner_cannot_execute_sets_up_qemu_first():
    """Cross-compiling the builder stage is not the whole build.

    The Go and Maven builder stages run on `$BUILDPLATFORM` and only emit for
    `$TARGETARCH`, but every runtime stage still RUNs apk/apt/pip/npm as the
    target architecture. On an amd64 runner that needs a binfmt handler, which is
    what `docker/setup-qemu-action` registers. Without it the arm64 half of every
    image fails with `exec format error` -- in the publish run, where a failure
    costs a release rather than a PR.
    """
    doc = yaml.safe_load(open(WORKFLOW, encoding="utf-8"))
    foreign_builds = 0

    for job_name, job in doc["jobs"].items():
        steps = job.get("steps") or []
        runs_on = job.get("runs-on")
        native = RUNNER_PLATFORM.get(runs_on if isinstance(runs_on, str) else None)
        for i, step in enumerate(steps):
            if "build-push-action" not in str(step.get("uses", "")):
                continue
            declared = str((step.get("with") or {}).get("platforms") or "")
            foreign = {p.strip() for p in declared.split(",") if p.strip()} - {native}
            if not foreign:
                continue
            foreign_builds += 1
            assert any("setup-qemu-action" in str(s.get("uses", "")) for s in steps[:i]), (
                f"job {job_name!r} builds {sorted(foreign)} on a {native} runner "
                f"(runs-on={runs_on!r}) with no docker/setup-qemu-action step before its "
                "build-push step. Every runtime-stage RUN for those platforms will fail "
                "with `exec format error`."
            )

    # Anti-vacuity: a multi-arch workflow whose build steps this loop failed to see
    # would otherwise pass with nothing checked.
    if len(_published_platforms()) > 1:
        assert foreign_builds >= 2, (
            f"docker-publish.yml publishes {sorted(_published_platforms())} but only "
            f"{foreign_builds} build-push step(s) were checked for a QEMU setup -- expected "
            "the image and connector builds"
        )
