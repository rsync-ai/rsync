"""The markdown link gate must actually run on a markdown-only pull request.

The bug this closes was invisible at the job level. `doc-links` lived in ci.yml
carrying an explicit "No path filter on purpose" comment, and that comment was
right about the case it named: a link dies when its TARGET is deleted, in a
commit touching no *.md at all. But ci.yml's own `pull_request` trigger carries
`paths-ignore: ['**.md', 'docs/**']`, so a PR touching ONLY markdown never
triggered the workflow, and the one gate whose entire subject is markdown never
ran on it. The job said "always"; the workflow said "never" for exactly the
markdown case, and the workflow won.

Both failure directions are asserted here, because fixing one by reintroducing
the other is the obvious wrong move:

  * a markdown-only PR must reach the gate  (the bug that was found)
  * a code-only PR must reach it too        (the case the original comment named)

Generalised 2026-08-29, because fixing this for ONE gate fixed nothing for its
siblings. `check-doc-links.sh` was moved to its own workflow; the other guards
whose subject is markdown were left in ci.yml's `llm-service-unit` job, which is
gated on a paths-filter with no `.md` entry. So `test_doc_merge_claims_are_true.py`
-- whose only input is CAPABILITIES.md -- could not run on a change to
CAPABILITIES.md, and #884 (docs-only, and written specifically to make that gate
pass) ran one check that was not it, then merged into a green `main` that had
skipped it. The list below is now the subject of the rule, not one script.
"""
import re
from pathlib import Path

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[2] / ".github" / "workflows"
TESTS = Path(__file__).resolve().parent
SCRIPT = "check-doc-links.sh"

# Every guard whose SUBJECT is markdown. Each must be reachable from a workflow
# that filters on nothing; a new one is added here and to doc-links.yml together.
MERGE_GUARD = "test_doc_merge_claims_are_true.py"
GUARDS = (SCRIPT, MERGE_GUARD, "test_doc_links_resolve.py",
          "test_documented_install_commands_render.py",
          # Both added by #887. The first reads only markdown and templates. The
          # second is a mixed subject -- it asserts a code fact (no reader of
          # `pii_policies` has appeared) AND a markdown fact (INVENTORY.md and
          # docs/api/README.md still say the table is inert) -- and it is the
          # markdown half that decides where it lives: a docs-only PR deleting the
          # caveat must not be able to reach main unchecked.
          "test_no_infrastructure_ips_in_docs.py",
          "test_pii_policies_are_not_advertised_as_enforced.py",
          # Added by #888. Its subject is docs/connectors/reference.md, so a PR
          # that adds a connector and hand-edits the catalogue is exactly the
          # docs-only shape ci.yml's paths-ignore would wave through.
          "test_connector_reference_matches_disk.py",
          # Added 2026-09-02. Its subject is the rendered shape of every tracked
          # markdown table, so a docs-only PR is precisely the change that can
          # introduce the defect -- a row gaining one unescaped `|` inside a code
          # span silently loses everything after it.
          "test_markdown_tables_do_not_drop_cells.py",
          # Added 2026-09-06. Its subject is an existence claim about a
          # connector, written in prose and falsified by a commit in a
          # different tree -- so the PR that makes it true is a code PR and
          # the PR that repairs the sentence is a docs-only one. Both must
          # reach it.
          "test_docs_do_not_call_built_connectors_unbuilt.py")

# The half of the ref promise that lives in the test module. Named in both files;
# asserted equal below, because a rename on one side disarms the other silently.
ENV_VAR = "DOC_GUARDS_REQUIRE_MAIN_REF"

MARKDOWN_ONLY = ["docs/internal/public-flip-runbook.md", "README.md"]
CODE_ONLY = ["backend-orchestrator/internal/cdc/postgresql.go"]


def _load(p):
    d = yaml.safe_load(p.read_text())
    # PyYAML resolves the bare key `on:` to the boolean True (YAML 1.1).
    return d, (d.get(True) or d.get("on") or {})


def _to_regex(pattern):
    """GitHub path-filter glob -> regex. `**` crosses `/`, `*` does not."""
    out, i = [], 0
    while i < len(pattern):
        c = pattern[i]
        if pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif c == "*":
            out.append("[^/]*")
            i += 1
        elif c == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(c))
            i += 1
    return re.compile("^" + "".join(out) + "$")


def _matches_any(path, patterns):
    return any(_to_regex(p).match(path) for p in patterns)


def _pr_triggers_for(on, changed):
    """Would this workflow's pull_request trigger fire for `changed`?"""
    if not isinstance(on, dict) or "pull_request" not in on:
        return False
    cfg = on.get("pull_request") or {}
    if not isinstance(cfg, dict):
        return True
    if "paths" in cfg:
        return any(_matches_any(f, cfg["paths"]) for f in changed)
    if "paths-ignore" in cfg:
        # GitHub runs the workflow when at least one changed file is NOT ignored.
        return any(not _matches_any(f, cfg["paths-ignore"]) for f in changed)
    return True


def _commands(doc):
    """Every executable line of every `run:` step in a workflow document.

    Comment lines are dropped. That is the whole point: a `run: |` block is a
    YAML *scalar*, so a `#` inside it is a shell comment that survives parsing
    intact -- naming a guard in one used to be indistinguishable from invoking it.
    """
    for job in (doc.get("jobs") or {}).values():
        for step in (job.get("steps") or []):
            for line in str(step.get("run") or "").splitlines():
                if line.strip() and not line.lstrip().startswith("#"):
                    yield line


def _workflows_running(needle):
    """Workflows that actually INVOKE `needle`, not ones that merely mention it.

    This matched the raw file text until #888, which meant a comment naming a
    guard counted as running it -- the same defect these tests exist to catch,
    aimed at themselves. Deleting a pytest argument while leaving a comment above
    it kept every assertion below green, so the census could certify a guard that
    no longer ran. Proven by mutation: drop the argument and this file goes red.
    """
    out = []
    for p in sorted(WORKFLOWS.glob("*.yml")):
        doc = _load(p)[0]
        if isinstance(doc, dict) and any(needle in c for c in _commands(doc)):
            out.append(p)
    return out


def _gate_workflows():
    return _workflows_running(SCRIPT)


def _jobs_running(needle):
    """(path, job_id, job) for each job whose own definition mentions `needle`."""
    out = []
    for p in _workflows_running(needle):
        for jid, job in (_load(p)[0].get("jobs") or {}).items():
            if needle in yaml.safe_dump(job):
                out.append((p, jid, job))
    return out


def _checkout_fetch_depth(job):
    for step in job.get("steps") or []:
        if str(step.get("uses") or "").startswith("actions/checkout@"):
            return (step.get("with") or {}).get("fetch-depth")
    return None


@pytest.mark.parametrize("guard", GUARDS)
def test_the_gate_is_findable(guard):
    """Vacuity guard. If a guard is renamed or its job deleted, every assertion
    below would pass over an empty set and report green."""
    assert _workflows_running(guard), (
        f"no workflow under {WORKFLOWS} runs {guard}; that gate is gone and the "
        "tests below are vacuous for it"
    )
    assert len(list(WORKFLOWS.glob("*.yml"))) >= 4, "workflow dir under-read"


def test_the_matcher_itself_works():
    """A glob matcher that silently matches nothing would make every
    `_pr_triggers_for` call return True and hide the bug."""
    assert _matches_any("README.md", ["**.md"])
    assert _matches_any("docs/a/b.md", ["**.md"])
    assert _matches_any("docs/x.txt", ["docs/**"])
    assert not _matches_any("main.go", ["**.md"])
    assert not _matches_any("docs/a/b.go", ["**.md"])


@pytest.mark.parametrize("guard", GUARDS)
@pytest.mark.parametrize(
    "changed,label",
    [(MARKDOWN_ONLY, "markdown-only"), (CODE_ONLY, "code-only")],
)
def test_a_pull_request_reaches_the_doc_link_gate(changed, label, guard):
    hosts = _workflows_running(guard)
    reachable = [p.name for p in hosts if _pr_triggers_for(_load(p)[1], changed)]
    assert reachable, (
        f"a {label} pull request touching {changed} triggers NO workflow that runs "
        f"{guard}. The gate exists but cannot see this class of change.\n"
        f"Workflows containing it: {[p.name for p in hosts]}"
    )


@pytest.mark.parametrize("guard", GUARDS)
def test_the_gate_workflow_declares_no_path_filter(guard):
    """Direct statement of the rule, so the failure names the cause and not just
    the symptom. Any paths/paths-ignore on the gate's own trigger re-opens this."""
    offenders = []
    for p in _workflows_running(guard):
        cfg = (_load(p)[1] or {}).get("pull_request") or {}
        if isinstance(cfg, dict) and ("paths" in cfg or "paths-ignore" in cfg):
            offenders.append((p.name, sorted(k for k in cfg if k.startswith("paths"))))
    assert not offenders, (
        f"the doc-link gate carries a path filter on its pull_request trigger: "
        f"{offenders}. Filtering it in either direction reintroduces the bug -- "
        "markdown-only PRs skip it, or target-deletion commits do."
    )


# --- The ref promise -------------------------------------------------------
#
# test_doc_merge_claims_are_true.py answers "is this row contradicted by main?"
# by probing `origin/main`. A shallow checkout has no such ref and that test
# SKIPS -- so the failure mode here is not a red job, it is a green one that
# checked nothing. The promise is made in the workflow (`fetch-depth: 0`) and
# enforced in the test (ENV_VAR turns the skip into a failure). Neither half is
# any use alone, so both are asserted, and so is the fact that they name the
# same string.


def test_the_merge_claim_guard_is_given_the_ref_it_probes():
    jobs = _jobs_running(MERGE_GUARD)
    assert jobs, f"no job names {MERGE_GUARD}; this assertion is vacuous"
    for path, jid, job in jobs:
        assert _checkout_fetch_depth(job) == 0, (
            f"{path.name} job `{jid}` runs {MERGE_GUARD} but checks out with "
            f"fetch-depth={_checkout_fetch_depth(job)!r}. Without `fetch-depth: 0` "
            "there is no origin/main to probe, and the guard reports green by "
            "skipping every claim in CAPABILITIES.md."
        )


def test_a_missing_ref_is_made_loud_where_the_ref_was_promised():
    for path, jid, job in _jobs_running(MERGE_GUARD):
        env = {}
        for step in job.get("steps") or []:
            if MERGE_GUARD in yaml.safe_dump(step):
                env = step.get("env") or {}
        assert str(env.get(ENV_VAR)) == "1", (
            f"{path.name} job `{jid}` promises the ref via fetch-depth but does not "
            f"set {ENV_VAR}=1 on the step that runs {MERGE_GUARD}. Losing the "
            "fetch-depth would then silently downgrade the gate to a skip."
        )


def test_both_halves_of_the_promise_spell_the_variable_the_same_way():
    """A rename on one side disarms the other and nothing goes red."""
    src = (TESTS / MERGE_GUARD).read_text()
    assert f'"{ENV_VAR}"' in src, (
        f"{MERGE_GUARD} does not read {ENV_VAR}; the workflow sets a variable "
        "nothing consumes, so a missing ref would skip rather than fail."
    )
