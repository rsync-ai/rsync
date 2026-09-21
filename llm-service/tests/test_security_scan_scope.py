"""The Security workflow scans only what a pull request touched -- and never less.

Until 2026-09-18 security.yml fanned out into 14 jobs per push: six govulncheck
and four gosec matrix legs, each scanning for 2-5 s behind ~30 s of setup and a
wait for a runner. It is now one job per scanner, and on a PR those jobs scan
the modules (and, for Semgrep, the files) the PR touched.
``scripts/security/ci-scan.py`` makes that choice, which makes it a security
control in its own right: a bug that selects too little turns a blocking gate
green without anything visibly failing. So every test here pins a direction the
script must err in, and the failure modes are the ones that read as a pass:

  * a change under a shared module that a service compiles in through a local
    ``replace`` must re-scan the service, transitively;
  * anything but a PR, a PR whose file list cannot be read, and a PR editing
    the scoping itself must scan everything;
  * a Semgrep include pattern must match the one file it names -- Next.js
    route directories are called ``[token]``, which unescaped is a character
    class that matches nothing, and a scoped scan of nothing is green;
  * the merged gosec SARIF must be ONE run with repo-relative URIs, since code
    scanning rejects several runs of one tool per category;
  * a scoped scan must never reach code scanning, which would read every
    finding in what it skipped as fixed.
"""
import http.server
import importlib.util
import json
import os
import subprocess
import sys
import threading

import pytest
import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPT = os.path.join(REPO_ROOT, "scripts", "security", "ci-scan.py")
SECURITY_WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "security.yml")

_spec = importlib.util.spec_from_file_location("ci_scan", SCRIPT)
ci_scan = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(ci_scan)


def _write(root, rel, text=""):
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


@pytest.fixture
def tree(tmp_path):
    """Three services over two shared libraries, wired the way the real tree is.

    svc-a -> lib-x -> lib-y (transitive, through lib-x's own go.mod)
    svc-b -> lib-y           (block-form replace, with a version on the left)
    svc-c                    (no replace at all)
    """
    _write(tmp_path, "svc-a/go.mod", "module a\n\nreplace example.com/x => ../lib/x\n")
    _write(tmp_path, "svc-b/go.mod", (
        "module b\n\nreplace (\n"
        "\texample.com/y v0.0.0 => ../lib/y // the shared driver\n"
        "\texample.com/z => example.com/z-fork v1.2.3\n"
        ")\n"
    ))
    _write(tmp_path, "svc-c/go.mod", "module c\n")
    _write(tmp_path, "lib/x/go.mod", "module x\n\nreplace example.com/y => ../y\n")
    _write(tmp_path, "lib/y/go.mod", "module y\n")
    return tmp_path


def _run(cwd, *args, changed=None, event="pull_request", extra_env=None):
    out = cwd / "github_output"
    out.write_text("")
    env = {k: v for k, v in os.environ.items() if not k.startswith("GITHUB_") and k not in ("GH_TOKEN",)}
    env.update({"GITHUB_EVENT_NAME": event, "GITHUB_OUTPUT": str(out)})
    if changed is not None:
        listing = cwd / "changed.txt"
        listing.write_text("".join(f"{f}\n" for f in changed))
        env["SCAN_SCOPE_CHANGED_FILES"] = str(listing)
    env.update(extra_env or {})
    proc = subprocess.run(
        [sys.executable, SCRIPT, *args], cwd=cwd, env=env, capture_output=True, text=True, timeout=60
    )
    outputs = dict(line.split("=", 1) for line in out.read_text().splitlines() if "=" in line)
    return proc, outputs


MODULES = ("svc-a", "svc-b", "svc-c")


@pytest.mark.parametrize(
    "changed, expected_mode, expected",
    [
        (["svc-c/main.go"], "partial", "svc-c"),
        (["lib/x/x.go"], "partial", "svc-a"),
        # lib/y is reached by svc-a only THROUGH lib/x's go.mod -- the transitive case.
        (["lib/y/y.go"], "partial", "svc-a svc-b"),
        (["svc-a/a.go", "svc-b/b.go", "svc-c/c.go"], "full", "svc-a svc-b svc-c"),
        (["frontend/src/app/never-a-module.tsx", "docs/never-a-module.md"], "none", ""),
        # A sibling whose name merely starts with a module's name is not inside it.
        (["svc-ab/x.go"], "none", ""),
    ],
)
def test_a_pr_scans_the_modules_it_touches_through_replace(tree, changed, expected_mode, expected):
    proc, out = _run(tree, "go-modules", *MODULES, changed=changed)
    assert proc.returncode == 0, proc.stderr
    assert (out["mode"], out["modules"]) == (expected_mode, expected)


def test_a_replace_to_another_module_path_is_not_a_local_dependency(tree):
    # svc-b's `example.com/z => example.com/z-fork v1.2.3` names a module, not a
    # directory; treating it as one would invent a path nothing can ever change.
    assert ci_scan.module_closure(str(tree / "svc-b")) == [str(tree / "svc-b"), str(tree / "lib/y")]


def test_the_real_services_reach_the_shared_driver():
    """The fixture above is only worth something if the real go.mod files parse the same way."""
    cwd = os.getcwd()
    os.chdir(REPO_ROOT)
    try:
        closure = ci_scan.module_closure("api-gateway")
    finally:
        os.chdir(cwd)
    # api-gateway reaches pgdriver both directly and through backend-orchestrator.
    assert "shared/go/pgdriver" in closure
    assert "backend-orchestrator" in closure


@pytest.mark.parametrize("trigger", ci_scan.SCAN_EVERYTHING_WHEN_CHANGED)
def test_a_pr_that_edits_the_scoping_scans_everything(tree, trigger):
    proc, out = _run(tree, "go-modules", *MODULES, changed=[trigger])
    assert (out["mode"], out["modules"]) == ("full", "svc-a svc-b svc-c"), proc.stdout
    # Semgrep too: a partial scan cannot show what an edited invocation does to
    # the rest of the tree.
    _write(tree, "svc-c/main.go", "package main\n")
    proc, out = _run(tree, "semgrep-includes", str(tree / "inc.txt"), changed=[trigger, "svc-c/main.go"])
    assert out["mode"] == "full", proc.stdout
    assert (tree / "inc.txt").read_text() == ""


@pytest.mark.parametrize("event", ["push", "schedule", "workflow_dispatch"])
def test_anything_but_a_pr_scans_everything(tree, event):
    # The changed-file list is ignored outright: only a PR is ever scoped.
    proc, out = _run(tree, "go-modules", *MODULES, changed=["svc-c/main.go"], event=event)
    assert (out["mode"], out["modules"]) == ("full", "svc-a svc-b svc-c")
    proc, out = _run(tree, "semgrep-includes", str(tree / "inc.txt"), changed=["svc-c/main.go"], event=event)
    assert out["mode"] == "full"
    assert (tree / "inc.txt").read_text() == ""


def _event(tree, number=7):
    return str(_write(tree, "event.json", json.dumps({"pull_request": {"number": number}})))


def test_an_unreadable_pr_file_list_scans_everything(tree):
    # Port 9 (discard) on loopback: nothing listens, so the connect is refused at once.
    env = {
        "GITHUB_API_URL": "http://127.0.0.1:9",
        "GITHUB_REPOSITORY": "o/r",
        "GH_TOKEN": "t",
        "GITHUB_EVENT_PATH": _event(tree),
    }
    proc, out = _run(tree, "go-modules", *MODULES, extra_env=env)
    assert (out["mode"], out["modules"]) == ("full", "svc-a svc-b svc-c")
    assert "::warning" in proc.stdout


def test_no_token_scans_everything(tree):
    proc, out = _run(tree, "go-modules", *MODULES, extra_env={"GITHUB_REPOSITORY": "o/r", "GITHUB_EVENT_PATH": _event(tree)})
    assert out["mode"] == "full"
    assert "::warning" in proc.stdout


class _FakeFiles(http.server.BaseHTTPRequestHandler):
    pages = {}
    seen = []

    def do_GET(self):  # noqa: N802 -- http.server's spelling
        type(self).seen.append((self.path, self.headers.get("Authorization")))
        page = int(self.path.rsplit("page=", 1)[1])
        body = json.dumps(type(self).pages.get(page, [])).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_api():
    server = http.server.HTTPServer(("127.0.0.1", 0), _FakeFiles)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _FakeFiles.seen = []
    yield server, _FakeFiles
    server.shutdown()


def test_the_api_listing_is_paged_and_counts_a_renames_old_path(tree, fake_api):
    server, handler = fake_api
    # A full first page forces a second request; the rename OUT of svc-c on page 2
    # must still select svc-c, whose tree it changed.
    handler.pages = {
        1: [{"filename": f"docs/f{i}.md"} for i in range(100)],
        2: [{"filename": "lib/moved.go", "previous_filename": "svc-c/moved.go"}],
    }
    env = {
        "GITHUB_API_URL": f"http://127.0.0.1:{server.server_address[1]}",
        "GITHUB_REPOSITORY": "o/r",
        "GH_TOKEN": "secret-token",
        "GITHUB_EVENT_PATH": _event(tree, number=42),
    }
    proc, out = _run(tree, "go-modules", *MODULES, extra_env=env)
    assert (out["mode"], out["modules"]) == ("partial", "svc-c"), proc.stdout
    assert [p for p, _ in handler.seen] == [
        "/repos/o/r/pulls/42/files?per_page=100&page=1",
        "/repos/o/r/pulls/42/files?per_page=100&page=2",
    ]
    assert all(auth == "Bearer secret-token" for _, auth in handler.seen)


def test_a_pr_at_the_api_listing_cap_scans_everything(tree, fake_api):
    server, handler = fake_api
    pages = ci_scan.PR_FILES_API_CAP // 100
    handler.pages = {p: [{"filename": f"docs/p{p}-{i}.md"} for i in range(100)] for p in range(1, pages + 1)}
    env = {
        "GITHUB_API_URL": f"http://127.0.0.1:{server.server_address[1]}",
        "GITHUB_REPOSITORY": "o/r",
        "GH_TOKEN": "t",
        "GITHUB_EVENT_PATH": _event(tree),
    }
    proc, out = _run(tree, "go-modules", *MODULES, extra_env=env)
    # 3,000 docs-only files would be `none` -- but the list cannot show what it cut off.
    assert out["mode"] == "full"
    assert "::warning" in proc.stdout


def _gitignore_match(pattern, path):
    """The subset of gitignore matching an anchored, escaped pattern uses."""
    import fnmatch

    literal = pattern.lstrip("/")
    unescaped = ""
    i = 0
    while i < len(literal):
        if literal[i] == "\\" and i + 1 < len(literal):
            unescaped += "[" + literal[i + 1] + "]" if literal[i + 1] in "[]*?" else literal[i + 1]
            i += 2
        else:
            unescaped += literal[i]
            i += 1
    return fnmatch.fnmatchcase(path, unescaped)


def test_semgrep_includes_are_anchored_escaped_and_skip_deleted_files(tree):
    route = "web/app/(auth)/invite/[token]/page.tsx"
    _write(tree, route)
    _write(tree, "api/app.py")
    _write(tree, "odd*name?.py")
    changed = [route, "api/app.py", "odd*name?.py", "api/deleted.py"]
    proc, out = _run(tree, "semgrep-includes", str(tree / "inc.txt"), changed=changed)
    assert out["mode"] == "partial", proc.stdout
    patterns = (tree / "inc.txt").read_text().splitlines()
    assert patterns == [
        "/api/app.py",
        "/odd\\*name\\?.py",
        "/web/app/(auth)/invite/\\[token\\]/page.tsx",
    ]
    # Each pattern selects exactly its own file -- the unescaped `[token]` would
    # have matched a one-letter directory and never the real one.
    for pattern, path in zip(patterns, ["api/app.py", "odd*name?.py", route]):
        assert _gitignore_match(pattern, path)
    assert not _gitignore_match("/web/app/(auth)/invite/[token]/page.tsx", route)


def test_a_pr_with_nothing_left_to_scan_is_none_not_a_whole_tree_scan(tree):
    # An empty include list handed to Semgrep would scan `.` in full; `none`
    # is what lets the workflow skip the job instead.
    proc, out = _run(tree, "semgrep-includes", str(tree / "inc.txt"), changed=["api/deleted.py"])
    assert out["mode"] == "none"
    assert (tree / "inc.txt").read_text() == ""


def _sarif(results, rules, taxa):
    return {
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "version": "2.1.0",
        "runs": [{
            "tool": {"driver": {"name": "gosec", "rules": [{"id": r} for r in rules]}},
            "taxonomies": [{"name": "CWE", "guid": "cwe", "taxa": [{"id": t} for t in taxa]}],
            "results": [
                {"ruleId": rule, "locations": [{"physicalLocation": {"artifactLocation": {"uri": uri}}}]}
                for rule, uri in results
            ],
        }],
    }


def test_merged_gosec_sarif_is_one_run_with_repo_relative_uris(tree):
    a = _write(tree, "a.sarif", json.dumps(_sarif([("G204", "internal/x.go")], ["G204"], ["78"])))
    b = _write(tree, "b.sarif", json.dumps(_sarif(
        [("G204", "cmd/main.go"), ("G304", "file:///abs/y.go")], ["G204", "G304"], ["78", "22"]
    )))
    proc = subprocess.run(
        [sys.executable, SCRIPT, "merge-sarif", "out.sarif",
         f"svc-a={a}", f"lib/y={b}", f"svc-c={tree / 'never-written.sarif'}"],
        cwd=tree, capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    merged = json.loads((tree / "out.sarif").read_text())
    assert len(merged["runs"]) == 1
    run = merged["runs"][0]
    uris = [r["locations"][0]["physicalLocation"]["artifactLocation"]["uri"] for r in run["results"]]
    assert uris == ["svc-a/internal/x.go", "lib/y/cmd/main.go", "file:///abs/y.go"]
    assert [r["id"] for r in run["tool"]["driver"]["rules"]] == ["G204", "G304"]
    assert [t["id"] for t in run["taxonomies"][0]["taxa"]] == ["78", "22"]
    # The module whose scanner wrote nothing is named, not silently absent.
    assert "svc-c contributes nothing" in proc.stdout


def test_no_gosec_sarif_at_all_writes_no_merged_file(tree):
    # summarize-sarif.sh exits 2 on a missing SARIF, which is what turns the job
    # red when every scanner failed; an empty merged file would hide that.
    proc = subprocess.run(
        [sys.executable, SCRIPT, "merge-sarif", "out.sarif", f"svc-a={tree / 'missing.sarif'}"],
        cwd=tree, capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0
    assert not (tree / "out.sarif").exists()


def _steps(job):
    return yaml.safe_load(open(SECURITY_WORKFLOW))["jobs"][job]["steps"]


@pytest.mark.parametrize("job", ["gosec", "semgrep"])
def test_a_scoped_scan_never_reaches_code_scanning(job):
    upload = next(s for s in _steps(job) if "upload-sarif" in str(s.get("uses", "")))
    condition = upload["if"].replace(" ", "")
    assert "steps.scope.outputs.mode=='full'" in condition, upload["if"]


def test_the_workflow_scans_modules_that_exist():
    """Every module security.yml hands the scope step has a go.mod -- a typo
    here would be a module that is never scanned, and never says so."""
    for job in ("govulncheck", "gosec"):
        scope = next(s for s in _steps(job) if s.get("id") == "scope")
        words = scope["run"].split("go-modules", 1)[1].replace("\\", " ").split()
        assert words, job
        for module in words:
            assert os.path.isfile(os.path.join(REPO_ROOT, module, "go.mod")), f"{job}: {module}"
