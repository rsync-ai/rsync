"""The Kafka security matrix's timeout retries, its compiled JVM probe, and its PR gate.

deploy/helm/rsync-ai/test/kind/kafka-matrix/run.py runs ~110 cells on a CI Mac
that shares one Docker VM with every other job. PR #1251's run failed 9 of 112
cells, none of them the code's fault:

  * cx-02/03/04 tasks + history -- "TimeoutException: Topic kmatrix not present
    in metadata after 30000 ms". The client matrix re-runs a cell that timed out
    before any verdict; the connect matrix had no such rule.
  * cx-05/06/07 worker -- "not started after 180s". A worker still running at
    the deadline is the clock, but the message did not say "timed out", so even a
    retry rule would not have recognised it.

And the run should not have happened: #1251 changed only another service's env
in docker-compose.yml. The matrix reads a compose file for exactly one thing --
the kafka-connect and debezium-mcp environment it renders per connect row -- so
the gate now asks whether THAT differs from the base branch before spending
15-25 minutes on it.

Every JVM cell also compiled RoundTrip.java on start (the source launcher): ~4 s
idle, 30-60 s on a loaded runner, 30+ starts a run. It is compiled once now.
"""
from __future__ import annotations

import importlib.util
import pathlib
import re

import yaml

import _flip_cut

REPO = pathlib.Path(__file__).resolve().parents[2]
RUN_PY = REPO / "deploy/helm/rsync-ai/test/kind/kafka-matrix/run.py"
FILTERS = REPO / ".github/paths-filters.yml"
CI = REPO / ".github/workflows/ci.yml"
SELF = {".github/paths-filters.yml", ".github/workflows/ci.yml"}

_spec = importlib.util.spec_from_file_location("kmatrix_run", RUN_PY)
kmx = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(kmx)

# Verbatim from PR #1251's matrix log (run 36315094737).
METADATA_TIMEOUT = ("ExecutionException: org.apache.kafka.common.errors.TimeoutException: Topic kmatrix "
                    "not present in metadata after 30000 ms.")
REJECTED = ("ExecutionException: org.apache.kafka.common.errors.SaslAuthenticationException: "
            "Authentication failed during authentication due to invalid credentials with SASL mechanism")


def _row(name):
    return next(r for r in kmx.CONNECT_ROWS if r.name == name)


def _cells(row, worker, tasks=("-", ""), history=("-", "")):
    """What connect_row returns for these results (same expectation rules)."""
    started = worker[0] == "PASS"
    return [
        (row.name, "worker", row.expect, row.reason, *worker),
        (row.name, "tasks", "PASS" if started else "-", None, *tasks),
        (row.name, "history", "PASS" if started else "-", None, *history),
        (row.name, "quickstart", "SAME", None, "SAME", "security env identical"),
    ]


def _deadline_message():
    src = RUN_PY.read_text()
    m = re.search(r'why = "exited" if not running else f"([^"]+)"', src)
    assert m, "run.py no longer builds the worker-deadline message this test reads"
    return m.group(1).replace("{WORKER_TIMEOUT}", str(kmx.WORKER_TIMEOUT)) + ": log4j sync: ..."


class _Reruns:
    """Stands in for connect_row: hands out scripted re-runs and counts them."""

    def __init__(self, *reruns):
        self.reruns, self.calls = list(reruns), []

    def __call__(self, row, attempt=1):
        self.calls.append(attempt)
        return self.reruns.pop(0)


def _retry(monkeypatch, cells, *reruns):
    fake = _Reruns(*reruns)
    monkeypatch.setattr(kmx, "connect_row", fake)
    monkeypatch.setattr(kmx, "say", lambda _msg: None)
    return {c[1]: c for c in kmx.connect_retries(_row(cells[0][0]), cells)}, fake.calls


def _ok(cell):
    _, _, expect, reason, got, msg = cell
    return expect == "-" or kmx.grade(expect, reason, got, msg)


def test_a_timed_out_tasks_cell_is_rerun_and_the_worker_verdict_stands(monkeypatch):
    row = _row("cx-02-tls-ca")
    first = _cells(row, ("PASS", "worker started"), ("FAIL", METADATA_TIMEOUT), ("PASS", "round-trip ok"))
    rerun = _cells(row, ("PASS", "worker started"), ("PASS", "round-trip ok"), ("PASS", "round-trip ok"))
    got, calls = _retry(monkeypatch, first, rerun)
    assert calls == [2]
    assert got["tasks"][4:] == ("PASS", "attempt 2: round-trip ok")
    assert got["worker"] == first[0] and got["history"] == first[2], "a cell with a verdict was replaced"
    assert all(_ok(c) for c in got.values())


def test_a_worker_still_running_at_its_deadline_is_a_timeout(monkeypatch):
    row = _row("cx-05-scram-jaas")
    first = _cells(row, ("FAIL", _deadline_message()))
    assert kmx.timed_out(row.expect, row.reason, "FAIL", _deadline_message()), (
        "the worker-deadline message does not match R_TIMEOUT, so a slow boot is never retried")
    rerun = _cells(row, ("PASS", "worker started"), ("PASS", "round-trip ok"), ("PASS", "round-trip ok"))
    got, calls = _retry(monkeypatch, first, rerun)
    assert calls == [2]
    assert [got[c][2] for c in ("worker", "tasks", "history")] == ["PASS"] * 3, (
        "tasks/history never ran behind a dead worker; the re-run must run and grade them")
    assert all(_ok(c) for c in got.values())


def test_a_retry_whose_worker_never_starts_cannot_read_as_a_pass(monkeypatch):
    """The trap: behind a dead worker, tasks/history expect "-" -- i.e. skipped, ok."""
    row = _row("cx-03-mtls-keystore")
    first = _cells(row, ("PASS", "worker started"), ("FAIL", METADATA_TIMEOUT), ("PASS", "round-trip ok"))
    dead = _cells(row, ("FAIL", _deadline_message()))
    got, calls = _retry(monkeypatch, first, dead, dead)
    assert calls == [2, 3], "each attempt's worker timed out again, so every attempt is used"
    assert got["tasks"][2] == "PASS" and got["tasks"][4] == "ERROR"
    assert not _ok(got["tasks"]), "a timed-out cell turned green because its retry could not run"


def test_a_verdict_is_never_rerun(monkeypatch):
    row = _row("cx-06-scram-userpass")
    first = _cells(row, ("PASS", "worker started"), ("FAIL", REJECTED + " (timed out)"), ("PASS", "ok"))
    got, calls = _retry(monkeypatch, first)
    assert calls == [], "a rejection -- even one that also says 'timed out' -- was retried"
    assert not _ok(got["tasks"])


def test_timeouts_outlast_the_retries_and_stay_red(monkeypatch):
    row = _row("cx-04-mtls-certkey")
    again = _cells(row, ("PASS", "worker started"), ("FAIL", METADATA_TIMEOUT), ("PASS", "ok"))
    got, calls = _retry(monkeypatch, list(again), again, again)
    assert calls == [2, 3] and len(calls) == kmx.RETRIES
    assert not _ok(got["tasks"])


def test_every_jvm_start_runs_the_compiled_class():
    """Source-launching RoundTrip.java compiles it on every one of 30+ JVM starts."""
    assert not any(a.endswith(".java") for a in kmx.JAVA), kmx.JAVA
    assert kmx.JAVA[-1] == "RoundTrip" and kmx.CLS in kmx.JAVA[kmx.JAVA.index("-cp") + 1]
    src = RUN_PY.read_text()
    mounts = src.count('f"{W}/cls:{CLS}:ro"')
    assert mounts >= 2, "the compiled class must be mounted in the client cells AND the connect workers"
    assert "compile_roundtrip()" in src.split("def build():", 1)[1].split("\ndef ", 1)[0]


def test_the_gate_renders_both_composes_at_both_roots(monkeypatch, tmp_path):
    base = str(tmp_path)
    # The base's quickstart requires a var HEAD's may not: it is filled from the
    # base's own file, or the base render aborts and the matrix runs for nothing.
    (tmp_path / "docker-compose.quickstart.yml").write_text("x: ${KMX_BASE_ONLY_REQUIRED:?set it}\n")
    seen, quick_env = set(), {}

    def fake_render(compose_file, env, extra=(), root=None):
        seen.add((compose_file, root))
        if compose_file == "docker-compose.quickstart.yml":
            quick_env[root] = env
        return ({"K": "same"}, {})
    monkeypatch.setattr(kmx, "render", fake_render)
    assert kmx.renders_match(base) is True
    assert seen == {(f, r) for f in ("docker-compose.yml", "docker-compose.quickstart.yml")
                    for r in (None, base)}, seen
    assert quick_env[base].get("KMX_BASE_ONLY_REQUIRED") == "kmatrix-render-only", quick_env[base]
    assert "KMX_BASE_ONLY_REQUIRED" not in quick_env[None]

    def drifting(compose_file, env, extra=(), root=None):
        return ({"K": root or "head"}, {})
    monkeypatch.setattr(kmx, "render", drifting)
    assert kmx.renders_match(base) is False


def test_the_compose_filter_holds_only_what_the_gate_can_compare():
    """A path in `kafkasec_compose` skips the matrix whenever the renders match.

    So it may hold ONLY files the matrix reads through renders() -- a Dockerfile
    or a harness file there would be skipped on every PR that changes it -- and
    the gate must fetch the base copy of every one of them.
    """
    filters = yaml.safe_load(FILTERS.read_text())
    compose = set(filters["kafkasec_compose"]) - SELF
    rendered = set(re.findall(r'render\("([^"]+)"', RUN_PY.read_text()))
    assert compose and compose == rendered, (compose, rendered)
    assert not compose & set(filters["kafkasec"]), "a compose file in `kafkasec` always runs the matrix"
    jobs = yaml.safe_load(CI.read_text()).get("jobs") or {}
    if "kafka-security-matrix" not in jobs and not _flip_cut.is_a_pre_cut_tree():
        # The public cut drops the matrix job whole (scripts/flip/apply-ci-split.py),
        # so there is no gate to read there. Private still has to have it.
        return
    gate = next(st for st in jobs["kafka-security-matrix"]["steps"]
                if "RUN=1" in str(st.get("run", "")))
    for path in compose:
        assert f"git show HEAD^1:{path}" in gate["run"], f"the gate never fetches the base {path}"
    assert "--renders-match" in gate["run"]


# ---------------------------------------------------------------- why rows timed out
#
# Retries only paper over a host that is too slow. A CI worker's own log (run
# 36325162072, cx-01 alone) showed where its 147 s went: "Scanning plugins with
# ReflectionScanner took 67257 ms", against WORKER_TIMEOUT=180. Three workers at
# once, or a second matrix on the same Docker VM, pushed that past the deadline.

# Verbatim: the first line every kafka-connect container logs.
ENTRYPOINT_LINE = ("rsync log4j sync: set 1 logger level(s) in /kafka/config/log4j.properties: "
                   "log4j.logger.io.debezium.connector.postgresql.connection.pgoutput.PgOutputMessageDecoder=ERROR")


class _Res:
    def __init__(self, stdout="", returncode=0):
        self.stdout, self.stderr, self.returncode = stdout, "", returncode


def test_a_worker_boots_without_the_reflective_plugin_scan(monkeypatch, tmp_path):
    (tmp_path / "rows").mkdir()
    runs = []

    def fake_docker(*args, **kw):
        if args[0] == "run":
            runs.append(args)
        return _Res("true")
    monkeypatch.setattr(kmx, "W", str(tmp_path))
    monkeypatch.setattr(kmx, "docker", fake_docker)
    monkeypatch.setattr(kmx, "renders", lambda row: (({"KAFKA_BROKERS": "b:9092"}, {}),) * 2)
    monkeypatch.setattr(kmx, "_logs", lambda name: kmx.WORKER_READY)
    monkeypatch.setattr(kmx, "_tasks_cell", lambda row, cname: ("PASS", "round-trip ok"))
    monkeypatch.setattr(kmx, "_history_cell", lambda row, cname, dbz: ("PASS", "round-trip ok"))
    row = _row("cx-02-tls-ca")
    cells = {c[1]: c for c in kmx.connect_row(row)}
    assert re.fullmatch(r"worker started in \d+s", cells["worker"][5]), (
        "the CI log must keep each worker's boot time -- the margin to WORKER_TIMEOUT")
    env = dict(ln.split("=", 1) for ln in (tmp_path / "rows" / f"{row.name}.worker.env").read_text().splitlines())
    assert env.get("CONNECT_PLUGIN_DISCOVERY") == "service_load", (
        "the worker runs the default hybrid discovery, whose reflective scan took 39-67 s of the 180 s deadline")
    assert all(flag in env.get("KAFKA_JVM_PERFORMANCE_OPTS", "").split() for flag in kmx.JVM_LEAN), env
    assert env["KAFKA_BROKERS"] == "b:9092", "the compose render no longer reaches the worker"
    assert "--no-healthcheck" in runs[0], runs[0]


def test_every_short_lived_jvm_is_lean(monkeypatch, tmp_path):
    (tmp_path / "rows").mkdir()
    runs = []

    def fake_docker(*args, **kw):
        runs.append(args)
        return _Res("RESULT PASS pid round-trip ok")
    monkeypatch.setattr(kmx, "W", str(tmp_path))
    monkeypatch.setattr(kmx, "docker", fake_docker)
    monkeypatch.setattr(kmx, "connector_dir", lambda: "/connector")
    row = next(r for r in kmx.CLIENT_ROWS if r.name == "02-tls")
    assert kmx._client_cell(row, "jvm") == ("PASS", "round-trip ok")
    jvm = runs[-1]
    assert jvm[jvm.index(kmx.IMG_CONNECT) + 1:jvm.index(kmx.IMG_CONNECT) + 1 + len(kmx.JAVA)] == tuple(kmx.JAVA)
    assert kmx.JAVA[1:1 + len(kmx.JVM_LEAN)] == kmx.JVM_LEAN, kmx.JAVA
    assert "--no-healthcheck" in jvm[:jvm.index(kmx.IMG_CONNECT)], jvm


def test_the_control_is_retried_before_it_is_judged(monkeypatch):
    """Run 36305855777: the lone cx-01 worker missed its deadline once and the run
    ended as "the harness is broken" -- the one row the retry rule never reached."""
    control, other = _row("cx-01-plaintext"), _row("cx-02-tls-ca")
    ok = ("PASS", "worker started"), ("PASS", "round-trip ok"), ("PASS", "round-trip ok")
    script = {("cx-01-plaintext", 1): _cells(control, ("FAIL", _deadline_message())),
              ("cx-01-plaintext", 2): _cells(control, *ok),
              ("cx-02-tls-ca", 1): _cells(other, *ok)}
    calls = []

    def fake_row(row, attempt=1):
        calls.append((row.name, attempt))
        return script[(row.name, attempt)]
    monkeypatch.setattr(kmx, "connect_row", fake_row)
    monkeypatch.setattr(kmx, "say", lambda _msg: None)
    got = kmx.connect_matrix([control, other])
    assert calls[:2] == [("cx-01-plaintext", 1), ("cx-01-plaintext", 2)], calls
    assert sorted(calls) == sorted(script), f"a row ran more (or fewer) times than it needed: {calls}"
    assert [c[:2] for c in got] == [(r.name, c) for r in (control, other)
                                    for c in ("worker", "tasks", "history", "quickstart")], "rows out of order"
    assert all(_ok(c) for c in got)


def test_a_worker_that_timed_out_is_not_blamed_on_its_log_level_line():
    """Every cx worker that missed its deadline in runs 36305855777 and 36315094737
    was reported as ENTRYPOINT_LINE, because "=ERROR" matched the error pick."""
    progress = "2026-09-27 14:31:02,113 INFO   ||  Loading plugin from: /kafka/connect/debezium-connector-vitess"
    assert kmx._cause(f"{ENTRYPOINT_LINE}\n{progress}", None) == progress
    error = "2026-09-27 14:31:02,113 ERROR  ||  Stopping due to error"
    assert kmx._cause(f"{ENTRYPOINT_LINE}\n{error}\n{progress}", None) == error, (
        "a real ERROR line must still win over the last line")


def test_a_second_matrix_on_the_host_waits_for_the_first(monkeypatch, tmp_path):
    import fcntl
    import threading
    lock = tmp_path / "kmx.lock"
    monkeypatch.setattr(kmx, "LOCK", str(lock))
    monkeypatch.setattr(kmx, "say", lambda _msg: None)
    holder = open(lock, "a+")
    fcntl.flock(holder, fcntl.LOCK_EX)
    got = []
    waiter = threading.Thread(target=lambda: got.append(kmx.host_lock()), daemon=True)
    waiter.start()
    waiter.join(0.5)
    assert not got, "a second run went ahead while another held the host lock"
    holder.close()  # the first run exits
    waiter.join(5)
    assert got and lock.read_text() == kmx.PFX
    got[0].close()


def test_main_holds_the_host_lock_before_building(monkeypatch, tmp_path):
    import fcntl
    monkeypatch.setattr(kmx, "LOCK", str(tmp_path / "kmx.lock"))
    monkeypatch.setattr(kmx, "W", kmx.W)
    monkeypatch.setattr(kmx, "PKI", kmx.PKI)
    monkeypatch.setattr(kmx, "say", lambda _msg: None)
    monkeypatch.setattr(kmx, "cleanup", lambda keep: None)
    monkeypatch.setattr(kmx.signal, "signal", lambda *a: None)
    monkeypatch.setattr(kmx.tempfile, "mkdtemp", lambda prefix="": str(tmp_path / "w"))
    monkeypatch.setattr(kmx.sys, "argv", ["run.py"])
    seen = []

    def fake_build():
        with open(kmx.LOCK, "a+") as probe:
            try:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
                seen.append("free")
            except BlockingIOError:
                seen.append("held")
        raise SystemExit("stop after build")
    monkeypatch.setattr(kmx, "build", fake_build)
    try:
        kmx.main()
    except SystemExit as e:
        assert e.code == "stop after build"
    assert seen == ["held"], "build() and everything after it ran without the host lock"


def test_every_print_reaches_a_piped_log_as_it_happens():
    """Under CI stdout is a pipe, which Python block-buffers.

    say() flushed, but report()'s verdict lines and the final count did not, so a
    run killed at its timeout lost them, and run 36308137826 sat 45 min on a
    0-byte step log. The script's entry point now line-buffers stdout. Proven the
    way CI sees it: the script runs as __main__ with stdout on a pipe, the first
    thing main() does prints one plain line and then hangs, and the line must
    arrive while the process is still alive.
    """
    import subprocess
    import sys
    import threading

    driver = (
        "import argparse, runpy, time\n"
        "def hang(self, *a, **k):\n"
        "    print('first verdict line')\n"
        "    time.sleep(60)\n"
        "argparse.ArgumentParser.parse_args = hang\n"
        f"runpy.run_path({str(RUN_PY)!r}, run_name='__main__')\n"
    )
    proc = subprocess.Popen([sys.executable, "-c", driver], stdout=subprocess.PIPE, text=True)
    got = []
    reader = threading.Thread(target=lambda: got.append(proc.stdout.readline()), daemon=True)
    reader.start()
    reader.join(20)
    alive = proc.poll() is None
    proc.kill()
    proc.wait()
    assert got == ["first verdict line\n"], (
        f"a plain print() in run.py did not reach a piped stdout while the run was "
        f"still going (read {got!r}): the entry point no longer line-buffers stdout"
    )
    assert alive, "the driver exited early, so the line may have come from the exit flush"
