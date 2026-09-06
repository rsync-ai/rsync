"""The published Helm chart must not carry the kind harness to its users.

`helm package` ships every file under the chart directory that `.helmignore`
does not exclude. `.helmignore` listed only editor and VCS noise, so the chart
published at 0.1.2 carried twelve files of developer scaffolding: two Java
sources, a Dockerfile, three shell scripts and five `values-kind-byo*.yaml`
fixtures, none of which mean anything without a checkout beside them.

That is the visible half. The sharp half is that `helm package` reads the DISK
and `.gitignore` has no authority over it. `test/kind/.gitignore` hides the
harness's generated credentials -- `.tls/`, `.broker-sasl-password`,
`.oidc-client-secret` -- and its own comment says of the CA key "one thing here
that must never be [committed]". True of git; irrelevant to Helm. Packaging a
checkout where `broker-up.sh` has been run picks all nine up, `ca.key` and
`broker.key` included. Measured here: 57 files, 23 under `test/kind`, against 34
with the rule in place. The published 0.1.2 chart comes off a fresh checkout, so
it carries no key material -- 12 of its 46 files are the tracked half.

This guard is structural rather than a grep for `test/` in `.helmignore`: it
classifies every tracked file under the chart as one Helm ships or one Helm
ignores, and fails on any file in neither bucket. Deleting the `test/` rule is
caught, and so is a new scaffolding directory that no wordlist would name.
"""

import os
import subprocess

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CHART_DIR = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai")
HELMIGNORE = os.path.join(CHART_DIR, ".helmignore")

# The shapes an operator installs. Everything Helm reads by name, plus the three
# directories the chart format defines. A file outside these is either scaffolding
# or a new shipped artifact nobody has thought about -- both want a human.
SHIPPED_PREFIXES = ("templates/", "charts/", "crds/")
SHIPPED_NAMES = {"Chart.yaml", "Chart.lock", "values.yaml", "values.schema.json", "README.md", "LICENSE", ".helmignore"}

# The chart-internal .gitignore this guard is written for, named as a real
# repo-relative path. Two wrong spellings were tried first and both are instructive:
# a bare ".gitignore" reads to test_ci_filter_covers_every_guard_subject as a
# repo-root file that no `llm` paths-filter pattern matches, so it failed the census
# loudly -- and a `**` glob made the census SILENT, because it resolves subjects with
# non-recursive glob.glob, where `**` matches one level and this file is two deep.
# A literal that resolves to nothing is dropped, which looks exactly like a subject
# that is covered.
CHART_GITIGNORE = "deploy/helm/rsync-ai/test/kind/.gitignore"
GITIGNORE_NAME = os.path.basename(CHART_GITIGNORE)


def _rules():
    """The `.helmignore` lines Helm would act on, in Helm's own order."""
    out = []
    for raw in open(HELMIGNORE, encoding="utf-8").read().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        out.append(line)
    return out


def _ignored(rel):
    """Does `.helmignore` exclude this chart-relative path?

    Models helm.sh/helm/v3/internal/ignore: a pattern with no interior slash is
    matched against a base NAME at any depth, one with an interior slash is
    anchored to the chart root, and a trailing slash restricts the rule to
    directories. Helm's loader returns SkipDir on a matching directory, so an
    ancestor match takes the whole subtree -- which is why each ancestor is
    tested, not just the file.
    """
    import fnmatch

    parts = rel.split("/")
    # (path-so-far, name, is_dir) for every ancestor directory, then the file itself.
    candidates = [("/".join(parts[: i + 1]), parts[i], True) for i in range(len(parts) - 1)]
    candidates.append((rel, parts[-1], False))

    for rule in _rules():
        dir_only = rule.endswith("/")
        pat = rule.rstrip("/")
        anchored = "/" in pat
        for path, name, is_dir in candidates:
            if dir_only and not is_dir:
                continue
            if fnmatch.fnmatch(path if anchored else name, pat):
                return True
    return False


def _tracked_chart_files():
    out = subprocess.run(
        ["git", "-C", REPO_ROOT, "ls-files", "-z", "--", "deploy/helm/rsync-ai"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return sorted(p[len("deploy/helm/rsync-ai/") :] for p in out.split("\0") if p)


def test_helmignore_uses_no_negation():
    """A `!` line would make the model above wrong in the permissive direction.

    Helm's ignore parser does not implement negation, so a `!` rule here would
    read to a human as "ship this back" while Helm silently kept excluding it --
    and this guard would agree with the human. Assert the gap shut rather than
    guess which side of it Helm lands on.
    """
    offending = [r for r in _rules() if r.startswith("!")]
    assert not offending, f".helmignore uses negation, which Helm does not support: {offending}"


def test_every_tracked_chart_file_is_either_shipped_or_ignored():
    files = _tracked_chart_files()
    # Anti-vacuity: `git ls-files` returning nothing (wrong cwd, renamed chart) would
    # make every assertion below trivially true.
    assert len(files) > 20, f"only {len(files)} tracked files under the chart -- the census lost its subject"

    strays = [
        f
        for f in files
        if not (f in SHIPPED_NAMES or f.startswith(SHIPPED_PREFIXES) or f.startswith("values-") or _ignored(f))
    ]
    assert not strays, (
        "these tracked chart files are neither a shipped chart artifact nor excluded by "
        f".helmignore, so `helm package` puts them in front of every operator: {strays}\n"
        "Add a rule to deploy/helm/rsync-ai/.helmignore, or -- if they are genuinely meant "
        "to ship -- widen SHIPPED_NAMES/SHIPPED_PREFIXES here and say why."
    )


def test_the_kind_harness_is_excluded_and_the_helm_test_hook_is_not():
    """The two directions that matter, named, so a regression says which one broke.

    `templates/tests/connection.yaml` is the trap: it is a `helm test` hook, its
    path contains `tests`, and a `.helmignore` rule aimed at the harness by name
    rather than by location would take it out. Nothing else in the suite would
    notice -- the chart installs fine without it and `helm lint` never runs it.
    """
    assert _ignored("test/kind/build-and-load.sh"), "the kind harness is shipping to users again"
    assert _ignored("test/kind/values-kind-byo-s3.yaml"), (
        "a kind fixture is shipping beside values-gke.yaml, where only the filename tells them apart"
    )
    assert not _ignored("templates/tests/connection.yaml"), (
        "the `helm test` hook is being excluded -- a .helmignore rule matched it by name "
        "instead of by location. It must ship."
    )
    assert not _ignored("values-gke.yaml"), "the GKE overlay is being excluded"


@pytest.mark.skipif(not __import__("shutil").which("helm"), reason="helm not installed on this runner")
def test_the_model_above_agrees_with_helm_itself(tmp_path):
    """Fidelity control: `helm package` is the authority, this file only models it.

    Without this, `_ignored` could drift from Helm's real semantics and the whole
    suite would go on passing while describing a chart nobody publishes.
    """
    import tarfile

    rc = subprocess.run(
        ["helm", "package", CHART_DIR, "--version", "9.9.9", "--app-version", "9.9.9", "-d", str(tmp_path)],
        capture_output=True,
        text=True,
    )
    assert rc.returncode == 0, f"helm package failed: {rc.stderr}"

    tgz = next(p for p in os.listdir(tmp_path) if p.endswith(".tgz"))
    with tarfile.open(os.path.join(tmp_path, tgz)) as tf:
        packaged = {m.name.split("/", 1)[1] for m in tf.getmembers() if m.isfile() and "/" in m.name}

    tracked = set(_tracked_chart_files())
    predicted = {f for f in tracked if not _ignored(f)}
    # Helm synthesises files that are in no checkout, and honours .gitignore'd paths
    # this census never sees; compare only over the tracked set both agree on.
    assert predicted == (packaged & tracked), (
        "the .helmignore model in this file disagrees with helm package.\n"
        f"  model says shipped, helm dropped: {sorted(predicted - packaged)}\n"
        f"  helm shipped, model says ignored: {sorted((packaged & tracked) - predicted)}"
    )


def _chart_gitignore_patterns():
    """Every pattern declared by a `.gitignore` INSIDE the chart, chart-relative.

    Returns (source, pattern, representative_path). The representative is a path
    that the pattern certainly covers, so `_ignored` can be asked about it
    without this file reimplementing gitignore matching too.
    """
    out = []
    for dirpath, dirnames, filenames in os.walk(CHART_DIR):
        if GITIGNORE_NAME not in filenames:
            continue
        rel_dir = os.path.relpath(dirpath, CHART_DIR)
        rel_dir = "" if rel_dir == "." else rel_dir + "/"
        src = rel_dir + GITIGNORE_NAME
        for raw in open(os.path.join(dirpath, GITIGNORE_NAME), encoding="utf-8").read().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith("!"):
                continue
            body = line.lstrip("/")
            rep = rel_dir + body.rstrip("/")
            if line.endswith("/"):
                rep += "/a-generated-file"
            elif "*" in body:
                rep = rep.replace("*", "x")
            out.append((src, line, rep))
    return out


def test_nothing_git_hides_inside_the_chart_is_something_helm_ships():
    """A `.gitignore` inside a packaging root protects nothing on its own.

    This is the invariant the census above cannot see, because it walks TRACKED
    files and these paths are by definition untracked. `helm package` walks the
    filesystem: a credential that `.gitignore` keeps out of the repo is still
    picked up by a package built from a working checkout. The chart's own
    `test/kind/.gitignore` says the CA private key "must never be" committed --
    accurate about git, and no constraint at all on Helm.

    Stated as a rule over PATTERNS rather than over files that happen to exist,
    so it holds on a clean CI checkout where none of these paths are present.
    """
    patterns = _chart_gitignore_patterns()
    # Anti-vacuity: no .gitignore found (chart moved, walk broken) would pass silently.
    assert patterns, (
        "no .gitignore found inside the chart -- either the walk is broken or this "
        "guard has lost its subject; do not let it pass on an empty set"
    )
    # And pin the known one by name, so a rename shows up here rather than as a
    # quietly smaller census -- CHART_GITIGNORE is also what the CI-filter guard reads.
    known = os.path.relpath(os.path.join(REPO_ROOT, CHART_GITIGNORE), CHART_DIR)
    assert any(src == known for src, _, _ in patterns), (
        f"{CHART_GITIGNORE} is no longer where this guard expects it; update the "
        f"constant. Found instead: {sorted({src for src, _, _ in patterns})}"
    )

    leaks = [(src, pat, rep) for src, pat, rep in patterns if not _ignored(rep)]
    assert not leaks, (
        "these paths are hidden from git but NOT from `helm package`, so a chart "
        "packaged from a working checkout ships them:\n"
        + "\n".join(f"  {src} declares {pat!r} -> {rep} is not covered by .helmignore" for src, pat, rep in leaks)
        + "\nAdd a .helmignore rule covering the directory that holds them."
    )


def test_no_git_ignored_file_on_this_disk_would_be_packaged():
    """The same invariant against the actual filesystem, where it is non-vacuous.

    Reports its own denominator: on a clean checkout there is nothing to find and
    that is not a pass, it is an absence -- the pattern-level test above is what
    holds there.
    """
    on_disk = []
    for dirpath, dirnames, filenames in os.walk(CHART_DIR):
        if ".git" in dirnames:
            dirnames.remove(".git")
        for name in filenames:
            rel = os.path.relpath(os.path.join(dirpath, name), CHART_DIR)
            if not _ignored(rel):
                on_disk.append(rel)

    ignored_by_git = subprocess.run(
        ["git", "-C", CHART_DIR, "check-ignore", "--stdin"],
        input="\n".join(on_disk),
        capture_output=True,
        text=True,
    ).stdout.split()

    assert not ignored_by_git, (
        f"{len(ignored_by_git)} of the {len(on_disk)} files `helm package` would ship from this "
        f"checkout are git-ignored, i.e. generated local state rather than chart content: "
        f"{sorted(ignored_by_git)}"
    )
