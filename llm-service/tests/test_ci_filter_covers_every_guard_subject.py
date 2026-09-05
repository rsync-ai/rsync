"""The five guards added alongside the self-host compose fixes must be able to run.

`llm-service-unit` is gated on the `llm` paths filter in ci.yml. A guard whose
subject is not matched by that filter is dead on exactly the change it exists to
catch -- a PR touching only that file skips the job, and a skipped check reads as
a passing one. This repo has hit that shape at least six times; the `llm:` filter
is a stack of comments each recording one of them.

This file closes it for the guards added with the BYO-Postgres / preflight fixes.
It derives each guard's subjects from the guard's own source rather than from a
hand-written list, so adding a subject to a guard cannot silently escape the
check. It is deliberately scoped to these five files: a repo-wide census finds
sixteen guard files with uncovered subjects, most of them narrative markdown, and
widening the filter to cover those is a CI-cost decision, not a bug fix. That
wider finding is recorded as KI-CI-FILTER-EXCLUDES-GUARDS-OWN-SUBJECTS.

Mirrors the mechanism of test_the_ci_filter_covers_every_file_this_guard_reads in
test_shipped_images_are_pinned.py -- same filter reader, same fnmatch caveat.
"""

import ast
import fnmatch
import glob
import os

import pytest
import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TESTS_DIR = os.path.join(REPO_ROOT, "llm-service", "tests")
CI_WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")

# The guards this file is responsible for. Scoped, not repo-wide -- see module docstring.
GUARDS = [
    # The five guards this PR adds.
    "test_byo_parked_deps_in_every_compose_file.py",
    "test_prod_compose_postgres_tls_is_coherent.py",
    "test_oss_overlay_documents_its_invocation.py",
    "test_published_image_destination.py",
    "test_flip_runbook_does_not_invoke_its_own_deleted_tooling.py",
    # Five pre-existing guards whose only uncovered subject was `install.sh` or
    # `scripts/mcp_generate_compose.py`, both now in the filter. They are enrolled here so
    # the coverage cannot silently regress -- see KI-CI-FILTER-EXCLUDES-GUARDS-OWN-SUBJECTS
    # for the subjects that remain uncovered and are deliberately out of scope.
    "test_install_reachability.py",
    "test_byo_overlays_are_complete.py",
    "test_repo_slug_is_publishable.py",
    "test_shipped_images_are_publishable.py",
    "test_mcp_compose_generation.py",
    # Enrolled 2026-09-05. Its subject is scripts/flip/excludes.txt, already covered by
    # `scripts/flip/**`. It is listed anyway because GUARDS is a hand-maintained literal:
    # line ~122 asserts a listed guard still EXISTS, so a deletion is caught, but nothing
    # notices a guard that was never added -- and the omission looks exactly like a pass.
    # Measured on the day it was written: absent from GUARDS, the census reported
    # `104 passed` with zero cases naming it.
    "test_flip_excludes_name_paths_that_exist.py",
]


def _llm_filter_patterns():
    """The `llm` pattern list as dorny/paths-filter actually receives it."""
    doc = yaml.safe_load(open(CI_WORKFLOW))
    step = next(
        st
        for st in doc["jobs"]["changes"]["steps"]
        if "paths-filter" in str(st.get("uses", "")) and "filters" in (st.get("with") or {})
    )
    return yaml.safe_load(step["with"]["filters"])["llm"]


def _subjects_of(guard):
    """Repo-relative files a guard reads, derived from its source.

    Two shapes are recognised, both of which every guard here uses:
    `os.path.join(REPO_ROOT, "a", "b")` and a bare literal that resolves to a
    file under the repo root. Glob literals are expanded. Only paths that exist
    on disk survive, which drops runtime scratch files a guard writes itself
    (the preflight mutant) and any literal that merely looks like a path.
    """
    tree = ast.parse(open(os.path.join(TESTS_DIR, guard)).read())
    raw = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "join"
            and node.args
            and isinstance(node.args[0], ast.Name)
            and node.args[0].id in ("REPO_ROOT", "ROOT")
        ):
            rest = [a.value for a in node.args[1:] if isinstance(a, ast.Constant) and isinstance(a.value, str)]
            if rest and len(rest) == len(node.args) - 1:
                raw.add("/".join(rest))
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            v = node.value
            if "/" in v or v.startswith(".") or v.endswith((".yml", ".yaml", ".sh", ".md", ".txt")):
                raw.add(v)

    out = set()
    for r in raw:
        for hit in glob.glob(os.path.join(REPO_ROOT, r)):
            if os.path.isfile(hit):
                out.add(os.path.relpath(hit, REPO_ROOT))
    # A guard reading its own directory is not a subject; llm-service/** is covered anyway.
    # `.git` is git plumbing -- in a worktree it is a *file*, so isfile() keeps it -- and is
    # read to shell out to git, never as a subject a PR can edit.
    return {s for s in out if not s.startswith("llm-service/tests") and s != ".git"}


def test_the_llm_job_is_still_the_one_gated_on_that_filter():
    """The premise of every test below: change the gate and they stop meaning anything."""
    doc = yaml.safe_load(open(CI_WORKFLOW))
    job = doc["jobs"]["llm-service-unit"]
    assert "needs.changes.outputs.llm == 'true'" in str(job.get("if", "")), (
        "llm-service-unit is no longer gated on the `llm` paths filter, so the "
        "coverage tests below assert against a filter that decides nothing. "
        "Point them at whatever gates the job now."
    )


@pytest.mark.parametrize("guard", [pytest.param(g, id=g) for g in GUARDS])
def test_every_guard_declares_at_least_one_subject(guard):
    """Anti-vacuity floor: coverage over an empty subject set passes for free.

    Without this, a refactor that stops the deriver recognising a guard's path
    shapes turns its coverage test into zero parameterisations -- which reads in
    the log exactly like a guard whose subjects are all covered.
    """
    assert os.path.isfile(os.path.join(TESTS_DIR, guard)), f"{guard} is gone; drop it from GUARDS"
    subs = _subjects_of(guard)
    if not subs:
        # Two very different causes look identical here, and the difference is
        # measurable: if the DERIVER broke, it breaks for everything; if this tree
        # simply does not contain this guard's subject, every other guard still
        # derives fine. The public cut is the second case -- it removes
        # docs/internal/ and scripts/flip/, which is the entire subject of
        # test_flip_runbook_does_not_invoke_its_own_deleted_tooling.py (that guard
        # skips there too, by its own skipif). Skipping on a measured denominator
        # rather than on a name keeps a real deriver regression failing.
        others = [g for g in GUARDS if g != guard and _subjects_of(g)]
        if len(others) == len(GUARDS) - 1:
            pytest.skip(
                f"{guard} names no path that exists in this tree, while all "
                f"{len(others)} other guards still derive subjects -- so the deriver "
                f"works and this guard's subject was removed from the tree (the "
                f"public cut removes docs/internal/ and scripts/flip/)."
            )
    assert subs, (
        f"no subject files derived from {guard}. Either it genuinely reads nothing "
        f"outside llm-service/tests (then remove it from GUARDS), or _subjects_of "
        f"no longer recognises the path shape it uses -- in which case its coverage "
        f"test below is silently checking nothing."
    )


@pytest.mark.parametrize(
    "guard,subject",
    [
        pytest.param(g, s, id=f"{g}::{s}")
        for g in GUARDS
        for s in sorted(_subjects_of(g))
    ],
)
def test_the_ci_filter_covers_every_guard_subject(guard, subject):
    """Every file a guard reads must be able to trigger the job that runs the guard.

    Matching is fnmatch, not the picomatch dorny/paths-filter actually uses.
    fnmatch is the looser of the two -- its `*` crosses `/` where picomatch's does
    not -- so this can only ever be more permissive than CI, never stricter. It
    cannot fail a pattern CI would honour.
    """
    pats = _llm_filter_patterns()
    assert any(fnmatch.fnmatch(subject, p) for p in pats), (
        f"`{subject}` is read by {guard}, but no `llm` paths-filter pattern in "
        f"ci.yml matches it. A PR touching only that file would skip "
        f"llm-service-unit, so the guard would not run on exactly the change it "
        f"exists to catch -- and a skipped check reads as a passing one.\n"
        f"Add a pattern covering it to the `llm:` filter in "
        f"{os.path.relpath(CI_WORKFLOW, REPO_ROOT)}.\n"
        f"Patterns today: {pats}"
    )
