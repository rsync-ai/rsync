"""A `fix` PR must change a test file, or say on a `No-test:` line why it cannot.

``scripts/check-fix-has-test.py`` runs as a step of ci.yml's env-templates job on
every PR. It is a nudge, not proof: it cannot tell whether the test it sees
covers the bug. What it must never do is the two things that read as a pass --
let a fix with no test through because it misread a path or a description, or
pass a PR it could not read. Each test here pins one of those.

The real cases are the 6 `fix` commits on main, out of 120 before 2026-09-27,
that changed no file the script counts as a test. Their file lists are copied
below as they were merged.
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
SCRIPT = os.path.join(REPO_ROOT, "scripts", "check-fix-has-test.py")
CI_WORKFLOW = os.path.join(REPO_ROOT, ".github", "workflows", "ci.yml")

_spec = importlib.util.spec_from_file_location("check_fix_has_test", SCRIPT)
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)


@pytest.mark.parametrize(
    "title, is_fix",
    [
        ("fix: the version pins say 0.1.4", True),
        ("fix(cdc): Stop keeps the position", True),
        ("fix(sink,helm,temporal): three things", True),
        ("fix!: a breaking fix", True),
        ("Fix(ui): capitalised", True),
        ("feat(ui): redesign the /chat home", False),
        ("docs: fix-once protocol for bug fixes", False),
        ("ci: stop re-running CI on push to main", False),
        ("chore(deps): bump fix-esm", False),
        ("fixture: seed data", False),
        ("prefix: not a fix", False),
    ],
)
def test_only_a_fix_title_is_checked(title, is_fix):
    assert check.is_fix_title(title) is is_fix


@pytest.mark.parametrize(
    "path",
    [
        "backend-orchestrator/internal/cdc/postgresql_test.go",
        "llm-service/tests/test_scrubber_parity.py",
        "llm-service/tests/unit/test_planner.py",
        "shared/mcp-connectors/tests/test_delete_prefix_contract.py",
        "shared/mcp-connectors/internal/foo/v1.0.0/test_connector.py",
        "frontend/src/__tests__/chat-home.test.tsx",
        "frontend/src/lib/format.test.ts",
        "frontend/src/components/Card.spec.tsx",
        "frontend/e2e/a11y/pages.spec.ts",
        "e2e/test_mysql_cdc_to_postgres.sh",
        "e2e/_support/reconcile.py",
        "api-gateway/internal/handlers/testdata/pipeline.json",
        "e2e/postgres/fixtures/seed.sql",
        "shared/postgres_family_golden.json",
        "deploy/helm/rsync-ai/test/kind/kafka-matrix/run.py",
        "tests/data/sample.csv",
        "scripts/check-something.sh",
        "scripts/flip/assert-ci-split.py",
    ],
)
def test_these_count_as_a_test(path):
    assert check.is_test_path(path), path


@pytest.mark.parametrize(
    "path",
    [
        "backend-orchestrator/internal/cdc/postgresql.go",
        "llm-service/src/agents/planner/strategies.py",
        "frontend/src/components/Card.tsx",
        "docker-compose.yml",
        "deploy/helm/rsync-ai/Chart.yaml",
        "scripts/some-deploy-smoke.sh",
        "scripts/flip/apply-ci-split.py",
        ".github/workflows/ci.yml",
        "CAPABILITIES.md",
        "docs/testing/latest.md",
        "contest/notes.txt",
    ],
)
def test_these_do_not(path):
    assert not check.is_test_path(path), path


# The 6 real no-test fixes, by PR number, with the files each changed. Each file
# list is ONE whitespace-separated string, split below. They are data the
# classifier reads, not files this test opens, and as separate path literals
# test_ci_filter_covers_every_guard_subject.py would count every one that still
# exists as a subject this test reads.
NO_TEST_FIXES = {
    1146: (
        "fix(infra): the otel-collector was OOM-looping unnoticed",
        """CAPABILITIES-ARCHIVE.md CAPABILITIES.md
        backend-orchestrator/internal/handlers/topology.go
        backend-orchestrator/internal/kafka/topology.go
        deploy/otel-collector-config.yaml docker-compose.prod.yml
        docker-compose.quickstart.yml docker-compose.yml
        docs/testing/load-and-capacity-test-plan.md
        llm-service/src/agents/planner/strategies.py""",
    ),
    1142: (
        "fix(cdc): the CDC producer had no restart policy",
        """CAPABILITIES-ARCHIVE.md CAPABILITIES.md docker-compose.prod.yml
        docker-compose.yml docs/testing/load-and-capacity-test-plan.md""",
    ),
    1138: (
        "fix(traefik): commit sniStrict=false",
        "deploy/traefik/dynamic.yml docs/deployment/oracle-cloud.md",
    ),
    1124: (
        "fix(ci): the OSS deploy smoke authenticates",
        "CAPABILITIES-ARCHIVE.md CAPABILITIES.md scripts/oss-deploy-smoke.sh",
    ),
    1107: (
        "fix(k8s): default installs pull multi-arch images",
        """CAPABILITIES-ARCHIVE.md CAPABILITIES.md INVENTORY.md README.md
        deploy/helm/rsync-ai/Chart.yaml deploy/helm/rsync-ai/README.md
        docs/deployment/kubernetes.md install-k8s.sh install.sh""",
    ),
    1104: (
        "fix(flip): unbreak the public cut",
        """.github/workflows/doc-links.yml CAPABILITIES-ARCHIVE.md INVENTORY.md
        scripts/flip/delink-docs.sh""",
    ),
}


@pytest.mark.parametrize("pr", sorted(NO_TEST_FIXES))
def test_a_real_fix_with_no_test_fails(pr):
    title, files = NO_TEST_FIXES[pr]
    ok, why = check.verdict(title, "## Summary\n\nFixes the thing.", files.split())
    assert not ok, why


@pytest.mark.parametrize("pr", sorted(NO_TEST_FIXES))
def test_the_same_fix_passes_with_a_reason(pr):
    title, files = NO_TEST_FIXES[pr]
    body = "## Summary\n\nFixes the thing.\n\nNo-test: a compose value no unit test can observe\n"
    ok, why = check.verdict(title, body, files.split())
    assert ok, why
    assert "a compose value no unit test can observe" in why


def test_a_fix_with_a_test_passes():
    ok, why = check.verdict(
        "fix(cdc): Reload copies every PG row",
        None,
        ["backend-orchestrator/internal/cdc/postgresql.go",
         "backend-orchestrator/internal/cdc/postgresql_test.go"],
    )
    assert ok and "postgresql_test.go" in why


def test_a_pr_that_is_not_a_fix_is_not_checked():
    ok, _ = check.verdict("feat(ui): a new page", None, ["frontend/src/app/page.tsx"])
    assert ok


@pytest.mark.parametrize(
    "body",
    [
        None,
        "",
        "No-test: n/a",                                    # a reason under 10 characters
        "No-test:",                                        # no reason at all
        "<!-- No-test: a template's example line -->",     # a comment, not a line of its own
        "Why is there No-test: because reasons are hard",  # not at the start of a line
        "Notest: the hyphen is part of the key",
    ],
)
def test_these_descriptions_do_not_exempt_a_fix(body):
    ok, why = check.verdict("fix: x", body, ["docker-compose.yml"])
    assert not ok, (body, why)


@pytest.mark.parametrize(
    "body",
    [
        "No-test: a Traefik option only the deployed host reads",
        "Summary\n\n  no-test: chart appVersion; the chart has no unit tests\nmore",
        "Summary\r\nNo-test: a compose memory cap, no test can observe it\r\n",
    ],
)
def test_these_do(body):
    ok, why = check.verdict("fix: x", body, ["docker-compose.yml"])
    assert ok, (body, why)


# ---- the script end to end: the event gate, the live API read, failing closed ----


def _run(env):
    base = {k: v for k, v in os.environ.items() if not k.startswith("GITHUB_") and k not in ("GH_TOKEN",)}
    base.update(env)
    return subprocess.run([sys.executable, SCRIPT], capture_output=True, text=True, env=base, timeout=60)


def _event(tmp_path, number=7, title="chore: payload title"):
    path = tmp_path / "event.json"
    path.write_text(json.dumps({"pull_request": {"number": number, "title": title, "body": ""}}))
    return str(path)


@pytest.mark.parametrize("event", ["push", "schedule", "workflow_dispatch"])
def test_anything_but_a_pr_passes_without_reading_anything(event):
    proc = _run({"GITHUB_EVENT_NAME": event, "GITHUB_API_URL": "http://127.0.0.1:9"})
    assert proc.returncode == 0, proc.stdout + proc.stderr


def test_an_unreadable_pr_fails_closed(tmp_path):
    # Port 9 (discard) on loopback: nothing listens, so every attempt is refused.
    proc = _run({
        "GITHUB_EVENT_NAME": "pull_request",
        "GITHUB_API_URL": "http://127.0.0.1:9",
        "GITHUB_REPOSITORY": "o/r",
        "GH_TOKEN": "t",
        "GITHUB_EVENT_PATH": _event(tmp_path),
    })
    assert proc.returncode == 1, proc.stdout
    assert "::error" in proc.stdout


def test_no_token_fails_closed(tmp_path):
    proc = _run({
        "GITHUB_EVENT_NAME": "pull_request",
        "GITHUB_REPOSITORY": "o/r",
        "GITHUB_EVENT_PATH": _event(tmp_path),
    })
    assert proc.returncode == 1, proc.stdout
    assert "::error" in proc.stdout


class _FakePR(http.server.BaseHTTPRequestHandler):
    pr = {}
    pages = {}
    seen = []

    def do_GET(self):  # noqa: N802 -- http.server's spelling
        type(self).seen.append((self.path, self.headers.get("Authorization")))
        if "/files?" in self.path:
            payload = type(self).pages.get(int(self.path.rsplit("page=", 1)[1]), [])
        else:
            payload = type(self).pr
        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def fake_api(tmp_path):
    server = http.server.HTTPServer(("127.0.0.1", 0), _FakePR)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    _FakePR.seen = []
    env = {
        "GITHUB_EVENT_NAME": "pull_request",
        "GITHUB_API_URL": f"http://127.0.0.1:{server.server_address[1]}",
        "GITHUB_REPOSITORY": "o/r",
        "GH_TOKEN": "secret-token",
        "GITHUB_EVENT_PATH": _event(tmp_path, number=42),
    }
    yield _FakePR, env
    server.shutdown()


def test_the_live_title_is_judged_not_the_payloads(fake_api):
    # The payload says `chore:`; the PR was retitled `fix:` since. A re-run replays
    # the payload, so trusting it would wave the fix through.
    handler, env = fake_api
    handler.pr = {"title": "fix(cdc): retitled after the run started", "body": None}
    handler.pages = {1: [{"filename": "backend-orchestrator/internal/cdc/postgresql.go", "status": "modified"}]}
    proc = _run(env)
    assert proc.returncode == 1, proc.stdout
    assert "No-test:" in proc.stdout  # the failure says how to fix it


def test_a_no_test_line_added_after_the_run_started_counts(fake_api):
    handler, env = fake_api
    handler.pr = {"title": "fix(traefik): commit sniStrict=false",
                  "body": "No-test: a Traefik option only the deployed host reads"}
    handler.pages = {1: [{"filename": "deploy/proxy/options.yml", "status": "modified"}]}
    proc = _run(env)
    assert proc.returncode == 0, proc.stdout


def test_files_are_paged_and_a_deleted_test_does_not_count(fake_api):
    handler, env = fake_api
    handler.pr = {"title": "fix: x", "body": ""}
    handler.pages = {
        1: [{"filename": f"src/f{i}.go", "status": "modified"} for i in range(100)],
        2: [{"filename": "src/f_test.go", "status": "removed"}],
    }
    proc = _run(env)
    assert proc.returncode == 1, proc.stdout
    assert [p for p, _ in handler.seen] == [
        "/repos/o/r/pulls/42",
        "/repos/o/r/pulls/42/files?per_page=100&page=1",
        "/repos/o/r/pulls/42/files?per_page=100&page=2",
    ]
    assert all(auth == "Bearer secret-token" for _, auth in handler.seen)
    # And the same test ADDED on page 2 passes.
    handler.seen = []
    handler.pages[2] = [{"filename": "src/f_test.go", "status": "added"}]
    assert _run(env).returncode == 0


# ---- the wiring: the check can only fail a PR if CI actually runs it ----


def test_ci_runs_the_check_on_every_pr_unfiltered():
    doc = yaml.safe_load(open(CI_WORKFLOW))
    # A token that cannot read pull requests makes every run an API error --
    # red on every PR, not silently green, but useless all the same.
    assert doc["permissions"].get("pull-requests") in ("read", "write")
    job = doc["jobs"]["env-templates"]
    steps = [s for s in job["steps"] if "check-fix-has-test.py" in s.get("run", "")]
    assert len(steps) == 1, "env-templates no longer runs scripts/check-fix-has-test.py"
    step = steps[0]
    # env-templates is ci.yml's one unfiltered job; a step-level `if` would let a
    # paths filter decide which fix PRs get checked.
    assert "if" not in step, step
    assert "github.token" in step.get("env", {}).get("GH_TOKEN", ""), step
