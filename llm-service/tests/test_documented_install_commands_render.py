"""The install command a reader copies out of a doc has to actually work.

This is the Kubernetes half of the same defect class install.sh had on the
Docker side: the advertised one-command install had never been run end to end,
so it exited before creating anything. Here it was
KI-CHART-README-INSTALL-DOES-NOT-RENDER -- the first code block in
deploy/helm/rsync-ai/README.md was two `--set` flags short of the chart's own
fail-closed guard, so `helm install` exited 1 at validate.yaml:83 with
`secrets.minioAccessKey is required`. Every reader hit it, on the very first
thing the chart asks them to run.

The guard is deliberately NOT "keep the READMEs in sync with a hand-written
list". The required-key set is DERIVED from the chart's own fail messages
(`secrets.X is required`), so adding a new required secret to validate.yaml
tightens this test automatically instead of leaving it stale. That is the whole
point: a second hand-maintained copy of the list would rot the same way the
README did.

Two layers, because CI and a laptop can check different things:

  1. the static layer runs everywhere, including CI, which has NO helm binary.
     It parses the `--set` flags out of each documented block and compares them
     against the derived required set.
  2. the render layer actually runs `helm template` on the extracted command and
     asserts exit 0. It skips without helm -- and a skip is not a pass, which is
     exactly why layer 1 does not depend on it.

Scope, stated so the next reader does not think it is broader than it is: only
blocks that set secrets INLINE are checked. Blocks of the shape
`helm install … -f my-values.yaml` delegate to a values file, and whether that
file needs the minio keys depends on which overlay is layered under it --
values-eks.yaml sets objectStorage.mode=s3, so the minio guard does not fire
there at all. Enforcing a flat "all five keys" on those would be a false
positive on the correct EKS documentation. The render layer covers them instead,
on any machine that has helm.
"""

import os
import re
import shutil
import subprocess

import pytest

REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
CHART_DIR = os.path.join(REPO_ROOT, "deploy", "helm", "rsync-ai")
VALIDATE = os.path.join(CHART_DIR, "templates", "validate.yaml")

# Docs that hand a reader a runnable chart install. Tracked paths, repo-relative.
DOC_FILES = [
    "README.md",
    os.path.join("deploy", "helm", "rsync-ai", "README.md"),
    os.path.join("docs", "deployment", "kubernetes.md"),
]

_FENCE = re.compile(r"```(?:bash|sh|shell)\n(.*?)```", re.DOTALL)
_REQUIRED = re.compile(r"([a-zA-Z][a-zA-Z0-9]*\.[a-zA-Z][a-zA-Z0-9]*) is required")
_SET_FLAG = re.compile(r"--set\s+([a-zA-Z][a-zA-Z0-9.]*)=")
# The runnable form, as opposed to the words "helm install" in prose or a
# table cell. Used to prove the fences capture every invocation in a file.
_INVOCATION = "helm install rsync ./deploy/helm/rsync-ai"


def _required_value_paths():
    """The keys the chart itself says are required, read out of its fail messages.

    Every one of these fires under DEFAULT values: postgresql.enabled and
    objectStorage.mode=minio are both defaults, and secrets.existingSecret is
    empty by default, which is the branch the whole secrets block sits under.
    """
    with open(VALIDATE) as fh:
        return sorted(set(_REQUIRED.findall(fh.read())))


def _install_blocks():
    """(doc, block_text) for every fenced shell block that installs this chart."""
    out = []
    for rel in DOC_FILES:
        with open(os.path.join(REPO_ROOT, rel)) as fh:
            text = fh.read()
        for block in _FENCE.findall(text):
            if "helm install" in block and "deploy/helm/rsync-ai" in block:
                out.append((rel, block))
    return out


def test_the_docs_still_contain_a_chart_install_command():
    """A zero-length work list reads exactly like a pass. Assert the denominator.

    If someone reformats a doc so the fence stops saying ```bash, every test
    below starts iterating over nothing and goes green while the command it was
    written to protect is unchecked.
    """
    blocks = _install_blocks()
    # Per-doc, not a global count. A global `>= 3` survives retagging one fence
    # from ```bash to ```console -- the total stays at the threshold because
    # another doc carries several blocks, and the retagged command silently
    # stops being checked. Verified by mutation: the global form passed, this
    # form fails.
    found = {doc for doc, _ in blocks}
    assert found == set(DOC_FILES), (
        "every doc that hands a reader a chart install must contribute at least "
        f"one ```bash block; missing: {sorted(set(DOC_FILES) - found)}"
    )

    # Per-doc presence is still not enough on its own: a doc carrying two
    # install blocks keeps its membership when ONE of them is retagged, and the
    # retagged command silently stops being checked. So count the runnable
    # invocations in the raw file and require the fences to capture all of them.
    # No pinned magic number -- the file is its own denominator. Verified by
    # mutation: retagging the chart README's first fence to ```console passed
    # under the membership check alone and fails here.
    for rel in DOC_FILES:
        with open(os.path.join(REPO_ROOT, rel)) as fh:
            raw = fh.read()
        in_file = raw.count(_INVOCATION)
        in_blocks = sum(b.count(_INVOCATION) for d, b in blocks if d == rel)
        assert in_file == in_blocks, (
            f"{rel}: {in_file} `{_INVOCATION}` command(s) in the file but only "
            f"{in_blocks} inside a ```bash fence -- the uncaptured one(s) are "
            "not being checked by anything below"
        )
    assert len(_required_value_paths()) >= 5, (
        "validate.yaml stopped yielding required-key fail messages -- the "
        "derivation broke, so the checks below are vacuous"
    )


def test_every_inline_secret_install_block_sets_every_required_key():
    """The KI itself: a block that supplies secrets inline must supply them all.

    Reproduced before the fix by rendering the chart README's block verbatim:
    exit 1, `secrets.minioAccessKey is required ... at validate.yaml:83:4`,
    nothing created. Adding minioAccessKey then surfaced minioSecretKey
    immediately, so it was short two flags rather than one -- which is the
    reason this asserts the whole derived set instead of the one key that
    happened to be reported first.
    """
    required = _required_value_paths()
    missing = {}
    for doc, block in _install_blocks():
        flags = set(_SET_FLAG.findall(block))
        if not any(f.startswith("secrets.") for f in flags):
            continue  # delegates to a values file -- see the module docstring
        gap = [k for k in required if k not in flags]
        if gap:
            missing[doc] = gap
    assert missing == {}, (
        "documented install command(s) are short a required --set flag, so "
        "`helm install` exits at the chart's own guard before creating "
        f"anything: {missing}\n\nRequired (derived from validate.yaml fail "
        f"messages): {required}"
    )


@pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")
def test_every_documented_install_block_renders(tmp_path):
    """Run what the reader runs. A skip here is not a pass -- see layer 1.

    `helm install` becomes `helm template` so this needs no cluster and creates
    nothing. Two substitutions make the blocks executable, and both are narrow
    on purpose:

      * a `git clone` of this repo is dropped rather than executed;
      * a `-f <name>.yaml` naming a file that does not exist in the repo is the
        reader's own values file, which the docs tell them to write. It is
        replaced by a synthesized stand-in carrying every required secret (built
        from the SAME derived list layer 1 uses) plus placeholder external
        endpoints.

    Supplying that stand-in is not the test cheating past its own assertion.
    Layer 1 is what checks the secrets; this layer checks everything else in the
    command -- that the overlay paths exist, that no flag names a value the
    chart dropped, and that no non-secret guard (bootstrap servers, ingress
    hosts, a securityProtocol/mechanism mismatch) fires on the documented
    combination. Those are exactly the failures a reader cannot fix by filling
    in their own file.
    """
    required = _required_value_paths()
    secrets = "\n".join(
        f"  {k.split('.', 1)[1]}: \"PLACEHOLDER\"" for k in required
        if k.startswith("secrets.")
    )
    stand_in = tmp_path / "reader-values.yaml"
    stand_in.write_text(
        "secrets:\n" + secrets + "\n"
        + """frontend:
  apiUrl: https://api.example.com
  publicUrl: https://app.example.com
postgresql:
  external:
    host: pg.example.com
redis:
  external:
    host: redis.example.com
kafka:
  external:
    bootstrapServers: "b-1.example.com:9096"
    saslUsername: fakeuser
    saslPassword: "PLACEHOLDER"
objectStorage:
  external:
    endpointUrl: https://s3.eu-west-1.amazonaws.com
    region: eu-west-1
    accessKeyId: "AKIAEXAMPLE"
    secretAccessKey: "PLACEHOLDER"
"""
    )

    failures = {}
    for doc, block in _install_blocks():
        lines = []
        for line in block.strip().splitlines():
            if line.strip().startswith("git clone"):
                continue
            lines.append(line)
        script = "\n".join(lines)
        assert script.count("helm install") == 1, (
            f"{doc}: expected exactly one helm install in the block, got "
            f"{script.count('helm install')} -- refusing to execute it"
        )
        stray = [
            ln for ln in script.splitlines()
            if ln.strip() and not ln.lstrip().startswith(("helm", "--", "-f", "#"))
        ]
        assert stray == [], f"{doc}: unexpected command line(s) in install block: {stray}"

        script = script.replace("helm install", "helm template", 1)
        for ref in re.findall(r"-f\s+(\S+\.ya?ml)", script):
            # A BARE filename is the reader's own file -- `my-values.yaml`,
            # `my-secrets.yaml` -- written in whatever directory they are
            # standing in, and it is the only thing the stand-in may replace.
            # Anything carrying a path is a file that ships in this repo, and it
            # must exist. Substituting for those too is how an earlier version of
            # this test survived a mutation: a typo'd
            # `deploy/helm/rsync-ai/values-eks-typo.yaml` did not exist either,
            # so it was quietly swapped for the stand-in and rendered green.
            if os.sep in ref or "/" in ref:
                assert os.path.exists(os.path.join(REPO_ROOT, ref)), (
                    f"{doc}: install block references {ref}, which is not in the repo"
                )
                continue
            script = script.replace(f"-f {ref}", f"-f {stand_in}")

        proc = subprocess.run(
            ["bash", "-c", script], cwd=REPO_ROOT, capture_output=True, text=True
        )
        if proc.returncode != 0:
            failures[doc] = (proc.stderr.strip().splitlines() or ["<no stderr>"])[0]
    assert failures == {}, f"documented install command(s) failed to render: {failures}"


# ---------------------------------------------------------------------------
# A documented `helm test` must be backed by a real test hook.
# ---------------------------------------------------------------------------
#
# docs/deployment/kubernetes.md has offered `helm -n rsync test rsync` as its
# verification step since the chart was written. For most of that time the chart
# defined ZERO `helm.sh/hook: test` templates, so the command printed
# `TEST SUITE: None` and exited 0 -- a reader ran the documented verify step, got
# a pass, and had verified nothing. That is the empty-set-reads-as-a-pass shape
# this repo has now hit in several forms: a zero-length work list looks exactly
# like a completed one unless something asserts the denominator.
#
# This check is deliberately TEXT-ONLY. There is no `helm` binary on the CI
# runners, so the render test above skips there; a helm-dependent assertion would
# silently protect nothing in the one place that matters. Reading the templates
# as text works everywhere.
_HELM_TEST_CMD = re.compile(r"\bhelm\b[^\n|&;]*\btest\b")
_TEST_HOOK = re.compile(r"helm\.sh/hook:\s*(?![^\n]*\bpost-)[^\n]*\btest\b")


def _docs_invoking_helm_test():
    hits = []
    for doc in DOC_FILES:
        path = os.path.join(REPO_ROOT, doc)
        if not os.path.exists(path):
            continue
        for block in _FENCE.findall(open(path, encoding="utf-8").read()):
            for line in block.splitlines():
                if _HELM_TEST_CMD.search(line):
                    hits.append((doc, line.strip()))
    return hits


def _templates_defining_a_test_hook():
    found = []
    tpl_root = os.path.join(CHART_DIR, "templates")
    for dirpath, _dirs, files in os.walk(tpl_root):
        for name in files:
            if not name.endswith((".yaml", ".yml", ".tpl")):
                continue
            full = os.path.join(dirpath, name)
            if _TEST_HOOK.search(open(full, encoding="utf-8").read()):
                found.append(os.path.relpath(full, REPO_ROOT))
    return sorted(found)


def test_a_documented_helm_test_is_backed_by_a_real_test_hook():
    invocations = _docs_invoking_helm_test()
    hooks = _templates_defining_a_test_hook()

    # Assert the denominator before asserting anything about it. If the docs
    # stopped mentioning `helm test` this test would otherwise pass vacuously --
    # the exact failure mode it exists to catch.
    assert invocations, (
        "no documented `helm test` invocation found in "
        f"{DOC_FILES}; if the verify step was removed on purpose, remove this "
        "guard deliberately rather than letting it pass on an empty set"
    )
    assert hooks, (
        "the docs tell readers to run "
        f"{invocations[0][1]!r} ({invocations[0][0]}), but no template under "
        f"{os.path.relpath(CHART_DIR, REPO_ROOT)}/templates carries a "
        "`helm.sh/hook: test` annotation -- `helm test` would print "
        "'TEST SUITE: None' and exit 0, which reads as a pass"
    )
