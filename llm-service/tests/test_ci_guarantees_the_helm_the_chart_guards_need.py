"""Every CI job that collects a helm-gated chart case must guarantee helm.

Eight cases across six suites are `skipif(shutil.which("helm") is None)`. They
render the chart: the documented `helm install` blocks, the four cloud overlays,
the external-Postgres TLS wiring, the three docker-host feature gates, and what
`helm package` actually ships. Every one of them is the *executable* half of a
guard whose other half is a text parse -- so when one skips, the pair silently
degrades to "the YAML says the right words", which is the failure mode those
render layers exist to close.

Nothing in this repo installs helm by default, and the jobs that collect these
cases run on `ubuntu-latest`, whose image makes no promise to carry one. So
presence has to be arranged by the workflow rather than inherited from the
runner -- and when it is not arranged, nothing goes red: pytest prints one
summary line whether a case ran or was skipped, so eight render assertions
would stop running and every check would stay green.

The repo already reasons this way about a different binary, one job over.
`doc-links.yml`'s pytest step says of `bash`: it is "guaranteed on these macOS
runners and on ubuntu-latest, and the suite fails rather than skips if it is
ever absent, because a syntax probe that returns 0 for everything is exactly the
shape that goes green having checked nothing." A silent skip is that same shape.
This file applies the argument to helm.

Three things are checked, and the first two are the load-bearing ones because
they are about the *mechanism* rather than about what anybody wrote down:

  1. the census finds helm-gated cases at all (an empty census would make
     everything below vacuous, and a count of zero is not an error);
  2. every workflow job that collects one of those files runs a step, before
     the pytest step, that guarantees helm -- `helm version`, or
     `azure/setup-helm`, which `docker-publish.yml` already uses for
     `publish-chart`;
  3. no skip reason asserts anything about CI. "helm not installed" describes
     the machine the test is on and is true anywhere. "CI has no helm" is a
     claim about a runner the test cannot see, and it was false.

The fourth check is about prose, and it exists because the false version of (3)
did real damage: "CI has NO helm binary" was written down repeatedly, and in
every one of those places it was the stated *reason* a chart guard stayed
text-only. The decisions were right; the reason was not. A text parse needs no binary and holds
on any checkout, which is a better argument and does not expire.

That check has to survive being written about. Retracting a false claim means
quoting it, so a wordlist sweep would fail on the very sentences that fix the
problem -- the trap `test_no_doc_claims_a_live_prod_environment.py` fell into
when it went red on the row describing its own fix. So the discriminator is
structural, not lexical: a hit is a violation only when no retraction marker
PRECEDES it. Asserting the claim is caught; quoting it in order to withdraw it
is not.
"""

from __future__ import annotations

import ast
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
TESTS = Path(__file__).resolve().parent
WORKFLOWS = REPO / ".github" / "workflows"

yaml = pytest.importorskip("yaml")


# --------------------------------------------------------------------------
# 1. Census: which test files gate on a helm binary, and on which cases.
# --------------------------------------------------------------------------

_HELM_WHICH = re.compile(r"""which\(\s*['"]helm['"]\s*\)""")


def _helm_gated_cases() -> dict[Path, list[str]]:
    """Map each test file to the case names gated on a helm binary.

    Parsed from the AST rather than grepped, so a decorator spanning lines or
    written with `not __import__("shutil").which("helm")` is found the same way
    as the plain form -- both spellings are in the tree today.
    """
    found: dict[Path, list[str]] = {}
    me = Path(__file__).resolve()
    for path in sorted(TESTS.glob("test_*.py")):
        if path.resolve() == me:
            # This file quotes the decorator in its docstring in order to
            # explain it, so it matches itself and would enrol with zero cases,
            # tripping the parser floor below on a file that has none to find.
            continue
        src = path.read_text(encoding="utf-8")
        if not _HELM_WHICH.search(src):
            continue
        tree = ast.parse(src, filename=str(path))
        names = [
            node.name
            for node in ast.walk(tree)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            for dec in node.decorator_list
            if "skipif" in (ast.get_source_segment(src, dec) or "")
            and _HELM_WHICH.search(ast.get_source_segment(src, dec) or "")
        ]
        # A file that mentions which("helm") but yields no parsed case means the
        # walk above is broken, not that the file is clean. Record it as an
        # empty list so the floor below can tell the two apart.
        found[path] = names
    return found


def test_the_census_finds_helm_gated_cases_and_parses_every_file_it_finds():
    """Positive denominator, and a parser floor on top of it.

    Two ways this whole file could go green having checked nothing: the glob
    finds no files, or the AST walk finds files but no decorators. The first is
    a count of zero, which reads exactly like a pass. The second is the bug that
    `test_chart_kafka_security_env.py` hit twice -- a character class that
    silently dropped a template -- and it is why the roster there is pinned
    rather than discovered.
    """
    census = _helm_gated_cases()
    assert census, (
        "no test file under llm-service/tests/ gates on a helm binary. Either "
        "the render layers were deleted -- in which case delete this guard too "
        "-- or _HELM_WHICH no longer matches how they are written."
    )
    unparsed = sorted(p.name for p, names in census.items() if not names)
    assert not unparsed, (
        "these files mention which(\"helm\") but the AST walk found no gated "
        f"case in them, so the walk is broken, not the files: {unparsed}"
    )
    total = sum(len(names) for names in census.values())
    assert total >= 4, (
        f"only {total} helm-gated cases across {len(census)} files; there were "
        "8 across 6 when this guard was written. A floor this far below the "
        "real count is here to catch a collapsed census, not to pin the number."
    )


# --------------------------------------------------------------------------
# 2. Which workflow jobs collect those files, and do they guarantee helm.
# --------------------------------------------------------------------------

# Flags that swallow the token after them, so it is not a path argument.
_VALUE_FLAGS = {"-p", "-k", "-m", "-o", "-n", "--rootdir", "--deselect", "--ignore"}

# pytest steps that resolve to no path argument at all. Listed by (workflow,
# job, step name) rather than skipped silently: a bare `pytest` cannot be
# resolved statically, and a NEW one appearing is a question for a human, not
# something this guard should quietly drop. `run_tree` cds into each connector
# directory in a subshell, so it never collects llm-service/tests.
_PYTEST_STEPS_WITH_NO_PATH_ARGS = {
    ("ci.yml", "llm-service-unit", "Per-connector regression tests (offline suites)"),
}


def _pytest_path_args(run_body: str) -> tuple[int, list[str]]:
    """(invocation count, positional path arguments) for a run body.

    The count is returned separately because `pip install pytest` puts the word
    in a body that never runs it. Treating that as an unresolvable invocation
    made three dependency-install steps look like collection steps.
    """
    # Join backslash continuations, then drop comment-only lines: a `#` line can
    # legitimately contain the word pytest (several in ci.yml do).
    body = run_body.replace("\\\n", " ")
    args: list[str] = []
    invocations = 0
    for line in body.split("\n"):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        line = re.sub(r"\s#.*$", "", line)
        for match in re.finditer(r"(?:^|[;&|(]|\bthen\b|\bdo\b)\s*(pytest\b[^;&|)]*)", line):
            invocations += 1
            tokens = match.group(1).split()[1:]
            skip_next = False
            for token in tokens:
                if skip_next:
                    skip_next = False
                    continue
                if token in _VALUE_FLAGS:
                    skip_next = True
                    continue
                if token.startswith("-"):
                    continue
                if token.startswith("$") or '"' in token or "'" in token:
                    continue
                args.append(token)
    return invocations, args


def _jobs_collecting(target: Path) -> list[tuple[str, str, int]]:
    """Every (workflow, job, step index) whose pytest invocation collects `target`."""
    hits: list[tuple[str, str, int]] = []
    unresolved: list[tuple[str, str, str]] = []
    for wf in sorted(WORKFLOWS.glob("*.yml")):
        doc = yaml.safe_load(wf.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            continue
        for job_id, job in (doc.get("jobs") or {}).items():
            job_wd = ((job.get("defaults") or {}).get("run") or {}).get("working-directory")
            for index, step in enumerate(job.get("steps") or []):
                run_body = step.get("run") or ""
                if "pytest" not in run_body:
                    continue
                base = REPO / (step.get("working-directory") or job_wd or ".")
                invocations, paths = _pytest_path_args(run_body)
                if not invocations:
                    continue  # names pytest without running it, e.g. `pip install pytest`
                if not paths:
                    unresolved.append((wf.name, job_id, step.get("name") or f"step {index}"))
                    continue
                for arg in paths:
                    resolved = (base / arg).resolve()
                    if resolved == target or resolved in target.parents:
                        hits.append((wf.name, job_id, index))
                        break
    unexpected = sorted(set(unresolved) - _PYTEST_STEPS_WITH_NO_PATH_ARGS)
    assert not unexpected, (
        "these pytest steps have no resolvable path argument, so this guard "
        "cannot tell whether they collect a helm-gated case. Work out what each "
        "one collects and either add the helm step or add it to "
        f"_PYTEST_STEPS_WITH_NO_PATH_ARGS with a reason: {unexpected}"
    )
    return hits


def _step_guarantees_helm(step: dict) -> bool:
    uses = step.get("uses") or ""
    if uses.split("@")[0].strip() == "azure/setup-helm":
        return True
    body = "\n".join(
        line for line in (step.get("run") or "").split("\n")
        if not line.strip().startswith("#")
    )
    return bool(re.search(r"\bhelm\s+(?:version|--help)\b", body))


@pytest.mark.parametrize(
    "test_file",
    sorted(_helm_gated_cases()),
    ids=lambda p: p.name,
)
def test_every_job_collecting_a_helm_gated_case_guarantees_helm(test_file: Path):
    """The load-bearing check: presence must be asserted, not inherited.

    A job that collects one of these files and does not guarantee helm is green
    only for as long as the runner image happens to carry one. Take that away
    and the render cases stop running, with nothing red to say so. The remedy is
    one line either way: a `helm version --short` step, or
    `uses: azure/setup-helm@<pinned sha>` as docker-publish.yml already does.
    """
    collecting = _jobs_collecting(test_file)
    assert collecting, (
        f"{test_file.name} holds a helm-gated case but no workflow job "
        "collects it. That is the unreachable-guard shape, not a pass -- "
        "either wire it into a job or delete it."
    )
    workflows = {wf: yaml.safe_load((WORKFLOWS / wf).read_text(encoding="utf-8"))
                 for wf, _, _ in collecting}
    for wf, job_id, pytest_index in collecting:
        steps = workflows[wf]["jobs"][job_id]["steps"]
        guarantors = [i for i, s in enumerate(steps) if _step_guarantees_helm(s)]
        assert guarantors, (
            f"{wf} job '{job_id}' runs {test_file.name}, which holds "
            f"{len(_helm_gated_cases()[test_file])} helm-gated case(s), but no "
            "step in that job guarantees helm is installed. Without one the "
            "cases skip silently on any runner that lacks it. Add either "
            "`run: helm version --short` or `uses: azure/setup-helm@<sha>`."
        )
        assert min(guarantors) < pytest_index, (
            f"{wf} job '{job_id}' guarantees helm at step {min(guarantors)}, "
            f"after the pytest step at {pytest_index}. The cases have already "
            "skipped by then."
        )


# --------------------------------------------------------------------------
# 3. What the skip reasons are allowed to say.
# --------------------------------------------------------------------------

_CI_WORD = re.compile(r"\bCI\b")


def test_no_helm_skip_reason_makes_a_claim_about_ci():
    """A skip reason may describe this machine. It may not describe a runner.

    "helm not installed" is checkable where it is printed and true wherever it
    prints. "helm not installed (CI has none)" is an assertion about a host the
    process cannot see, and the first thing a reader does with it is stop
    wondering whether the case ran.
    """
    offenders = []
    for path in sorted(TESTS.glob("test_*.py")):
        src = path.read_text(encoding="utf-8")
        if not _HELM_WHICH.search(src):
            continue
        tree = ast.parse(src, filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            segment = ast.get_source_segment(src, node) or ""
            if "skipif" not in segment or not _HELM_WHICH.search(segment):
                continue
            for kw in node.keywords:
                if kw.arg == "reason" and isinstance(kw.value, ast.Constant):
                    if _CI_WORD.search(str(kw.value.value)):
                        offenders.append(f"{path.name}:{node.lineno} -> {kw.value.value!r}")
    assert not offenders, (
        "a helm skip reason asserts something about CI. Say what is true of the "
        f"machine running the test instead: {offenders}"
    )


# --------------------------------------------------------------------------
# 4. The prose rule, with a structural exemption for retractions.
# --------------------------------------------------------------------------

# Two orders the claim gets written in. Tolerates markdown emphasis, backticks
# and quotes around the words, because it appeared in all of those forms.
_QUOTES = r"[\s\"'`*“”]*"
_CLAIM_PATTERNS = (
    # "no helm binary in CI" / "no helm binaries on the CI runners"
    re.compile(
        rf"\bno{_QUOTES}helm{_QUOTES}binar(?:y|ies)\s+(?:in|on)\s+(?:the\s+)?"
        r"(?:CI|runners?|GitHub|workflows?)\b",
        re.IGNORECASE,
    ),
    # "CI has no helm binary" / "the runners have NO helm binary"
    re.compile(
        rf"\b(?:CI|runners?|workflows?){_QUOTES}(?:has|have|had)\s+no{_QUOTES}"
        r"helm" + _QUOTES + r"binar(?:y|ies)",
        re.IGNORECASE,
    ),
)

# A retraction says, before quoting the claim, that the claim is being quoted.
_RETRACTION_MARKERS = (
    "used to",
    "at the time",
    "asserted the opposite",
    "not because",
    "no longer",
    "formerly",
    "this said",
    "recorded here was",
    "which was false",
)

# How far back a marker may sit and still govern the claim: about a paragraph.
_MARKER_WINDOW = 320

_TEXT_SUFFIXES = {".md", ".py", ".yml", ".yaml", ".sh", ".txt", ".j2"}


def _tracked_text_files() -> list[Path]:
    out = subprocess.run(
        ["git", "-C", str(REPO), "ls-files", "-z"],
        capture_output=True, text=True, check=True,
    ).stdout
    names = [n for n in out.split("\0") if n]
    assert len(names) > 500, (
        f"git ls-files returned only {len(names)} paths; this sweep would be "
        "near-vacuous. Refusing to pass on an empty denominator."
    )
    return [REPO / n for n in names if Path(n).suffix in _TEXT_SUFFIXES]


def test_no_shipped_text_asserts_that_ci_has_no_helm():
    """The claim is catchable when it is asserted, not when it is mentioned.

    Every correction of this belief has to quote it, so the exemption keys on
    ORDER: a retraction marker must appear before the claim it withdraws.
    Writing "CI has no helm binary" earns a failure; writing "this used to say
    CI has no helm binary, and it was false" does not.
    """
    me = Path(__file__).resolve()
    violations = []
    scanned = 0
    for path in _tracked_text_files():
        if path.resolve() == me:
            continue  # the patterns above are literal claims by construction
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, FileNotFoundError):
            continue
        scanned += 1
        for pattern in _CLAIM_PATTERNS:
            for match in pattern.finditer(text):
                window = text[max(0, match.start() - _MARKER_WINDOW):match.start()]
                window = " ".join(window.split()).lower()
                if any(marker in window for marker in _RETRACTION_MARKERS):
                    continue
                line = text.count("\n", 0, match.start()) + 1
                violations.append(
                    f"{path.relative_to(REPO)}:{line} -> {match.group(0)!r}"
                )
    assert scanned > 200, f"only {scanned} text files scanned; denominator too small"
    assert not violations, (
        "this says CI has no helm binary. Nothing here installs one by default, "
        "but every job that runs the chart guards now sets helm up explicitly "
        "and asserts `helm version` before pytest. If you "
        "are quoting the old claim in order to retract it, put the retraction "
        f"marker before the quote: {violations}"
    )
