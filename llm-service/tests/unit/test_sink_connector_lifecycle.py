"""
Regression tests for kafka-mcp-sink connector.py lifecycle fixes:

  - STDIO entry point calls run() (BaseMCPConnector's real method), not the
    non-existent run_stdio() that crashed every non-HTTP launch.
  - start_sink fails (success:false, status:"exited") when the worker process
    exits during startup, instead of reporting a green "started".
  - sink_status promotes a worker-reported fatal error (last_error gated on
    failed>0 / dlq_publish_failures>0) to the top-level `error` field, and does
    NOT raise a false alarm for a healthy worker.

Self-contained: bootstraps the kafka-mcp-sink versioned dir on sys.path and skips
cleanly if its optional deps (fastapi/uvicorn/httpx) aren't installed.
"""

import os
import subprocess
import sys
import time

import pytest

_V = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../shared/mcp-connectors/internal/kafka-mcp-sink/versions/v1.0.0",
    )
)
if _V not in sys.path:
    sys.path.insert(0, _V)

connector = pytest.importorskip("connector")
httpx = pytest.importorskip("httpx")
C = connector.KafkaMCPSinkConnector


def test_stdio_entrypoint_uses_run_not_run_stdio():
    c = C()
    assert hasattr(c, "run"), "BaseMCPConnector stdio entry point run() is missing"
    assert not hasattr(c, "run_stdio"), "run_stdio() must not exist — the old call crashed on launch"


def test_start_sink_fails_when_worker_exits_during_startup(monkeypatch):
    c = C()
    monkeypatch.setattr(
        c, "_spawn_worker_process",
        lambda wc: subprocess.Popen(["python3", "-c", "import sys; sys.exit(1)"]),
    )
    # Skip the real 5s health wait; sleep briefly so the fake worker has exited.
    monkeypatch.setattr(c, "_wait_for_worker_ready", lambda port, timeout=5: time.sleep(0.4))
    res = c.start_sink({"config": {
        "topics": "t",
        "consumer_group": "g",
        "destination_connector": "postgresql",
        "destination_config": {"table": "x"},
    }})
    assert res.get("success") is False, res
    assert res.get("status") == "exited", res
    assert res.get("error"), "an exited worker must carry an error reason"


def _register_worker(c, params, metrics_port=1):
    wid = c._get_worker_id(params)
    proc = subprocess.Popen(["sleep", "30"])
    with c._lock:
        c.workers[wid] = {
            "process": proc, "pid": proc.pid, "config": {}, "metrics_port": metrics_port,
            "restart_attempts": 0, "intentional_stop": False, "auto_restart": True,
        }
    return proc


def test_sink_status_promotes_last_error_on_failure(monkeypatch):
    c = C()
    params = {"config": {"pipeline_id": "p-test", "execution_id": "e-test"}}
    proc = _register_worker(c, params)
    try:
        class R:
            def json(self):
                return {"last_error": "destination unreachable", "failed": 3, "dlq_publish_failures_total": 0}
        monkeypatch.setattr(httpx, "get", lambda *a, **k: R())
        st = c.sink_status(params)
        assert st.get("status") == "running", st
        assert st.get("error") == "destination unreachable", f"last_error not promoted: {st}"
    finally:
        proc.terminate()


def test_restart_backoff_is_exponential_and_capped():
    # A fast-failing worker must not be respawned every supervisor poll. rapid_restarts=0
    # is the healthy case (respawn immediately); thereafter backoff grows 1,2,4,8,… and
    # caps at 60s so a permanently-broken worker can't tight-loop.
    f = C._restart_backoff_seconds
    assert f(0) == 0.0
    assert [f(n) for n in (1, 2, 3, 4, 5, 6)] == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0]
    assert f(10) == 60.0
    assert f(100) == 60.0


def test_sink_status_no_false_alarm_when_healthy(monkeypatch):
    c = C()
    params = {"config": {"pipeline_id": "p2", "execution_id": "e2"}}
    proc = _register_worker(c, params)
    try:
        class R:
            def json(self):
                return {"last_error": "", "failed": 0, "dlq_publish_failures_total": 0}
        monkeypatch.setattr(httpx, "get", lambda *a, **k: R())
        st = c.sink_status(params)
        assert "error" not in st, f"healthy worker raised a false alarm: {st}"
    finally:
        proc.terminate()


# --------------------------------------------------------------------------- #
# health() liveness + circuit breaker (fix #2: de-mask dead/crash-looping sinks)
# --------------------------------------------------------------------------- #

def test_health_reports_liveness_not_registration_count():
    """health() must count ALIVE workers and go `degraded` when one has died — the old
    body returned len(self.workers)/`healthy` so a dead sink read green forever."""
    c = C()
    c._supervisor_stop.set()  # deterministic: no background respawn during the test
    params = {"config": {"pipeline_id": "hp", "execution_id": "he", "consumer_group": "hg"}}
    proc = _register_worker(c, params)
    try:
        h = c.health()
        assert h["status"] == "healthy", h
        assert h["active_workers"] == 1 and h["total_workers"] == 1, h
        assert h["dead_workers"] == 0 and h["crashed_workers"] == 0, h
    finally:
        proc.terminate()
        proc.wait(timeout=5)

    # Process is now dead but still registered → must read degraded, not healthy.
    h2 = c.health()
    assert h2["status"] == "degraded", h2
    assert h2["active_workers"] == 0 and h2["dead_workers"] == 1, h2
    assert any(u["state"] == "down" for u in h2["unhealthy_workers"]), h2


def test_health_and_sink_status_report_crashed_worker():
    """A circuit-breaker-tripped worker surfaces as terminal `crashed` (not stopped/healthy)."""
    c = C()
    c._supervisor_stop.set()
    wid = "xw"
    proc = subprocess.Popen(["python3", "-c", "import sys; sys.exit(1)"])
    proc.wait(timeout=5)  # dead
    with c._lock:
        c.workers[wid] = {
            "process": proc, "pid": proc.pid, "config": {"consumer_group": wid},
            "metrics_port": 1, "restart_attempts": connector.MAX_RAPID_RESTARTS,
            "intentional_stop": False, "auto_restart": True,
            "crashed": True, "last_error": "crash-looped: 5 rapid restarts",
        }
    h = c.health()
    assert h["status"] == "degraded", h
    assert h["crashed_workers"] == 1 and h["active_workers"] == 0, h
    assert any(u["state"] == "crashed" for u in h["unhealthy_workers"]), h

    st = c.sink_status({"config": {"consumer_group": wid}})
    assert st["status"] == "crashed", st
    assert st.get("error") == "crash-looped: 5 rapid restarts", st


def test_circuit_breaker_trips_after_max_rapid_restarts():
    """After MAX_RAPID_RESTARTS consecutive rapid deaths the breaker trips: the helper
    stops authorising respawns and marks the worker terminal `crashed`."""
    c = C()
    c._supervisor_stop.set()
    max_r = connector.MAX_RAPID_RESTARTS
    worker = {"last_spawn_monotonic": 100.0, "rapid_restarts": 0, "restart_attempts": 0}
    now = 100.0
    results = []
    for _ in range(max_r + 2):
        now += 1.0  # each death is well within RAPID_RESTART_WINDOW_SECONDS
        should_respawn = c._record_rapid_restart_and_maybe_trip(worker, "w", now)
        results.append(should_respawn)
        if should_respawn:
            worker["last_spawn_monotonic"] = now  # a real respawn resets the clock

    assert results[:max_r - 1] == [True] * (max_r - 1), results
    assert results[max_r - 1] is False, f"breaker must trip on death #{max_r}: {results}"
    assert worker.get("crashed") is True
    assert "crash-looped" in (worker.get("last_error") or "")


def test_circuit_breaker_resets_after_surviving_the_window():
    """A worker that survives past RAPID_RESTART_WINDOW_SECONDS resets its counter, so an
    occasional crash after healthy uptime restarts immediately and never trips."""
    c = C()
    c._supervisor_stop.set()
    worker = {"last_spawn_monotonic": 100.0, "rapid_restarts": 3}
    # Died 40s after the last spawn → outside the 30s window → not a rapid restart.
    assert c._record_rapid_restart_and_maybe_trip(worker, "w", 140.0) is True
    assert worker["rapid_restarts"] == 0
    assert not worker.get("crashed")


def test_restart_worker_skips_crashed_worker():
    """The supervisor's respawn path must be a no-op for a crashed worker."""
    c = C()
    c._supervisor_stop.set()
    wid = "cw"
    proc = subprocess.Popen(["sleep", "30"])
    with c._lock:
        c.workers[wid] = {
            "process": proc, "pid": proc.pid, "config": {"consumer_group": wid},
            "metrics_port": 1, "restart_attempts": 2, "intentional_stop": False,
            "auto_restart": True, "crashed": True,
        }
    try:
        before_pid = c.workers[wid]["pid"]
        c._restart_worker(wid)  # must not spawn a replacement
        assert c.workers[wid]["pid"] == before_pid, "crashed worker must not be respawned"
    finally:
        proc.terminate()


# --------------------------------------------------------------------------- #
# last_exit: how a dead worker ended (exit code, signal, cgroup OOM count).
# A kernel OOM kill SIGKILLs the Go worker before it can log, so this is the only
# trace the supervisor can keep.
# --------------------------------------------------------------------------- #

def _sigkilled_process():
    proc = subprocess.Popen(["sleep", "30"])
    proc.kill()
    proc.wait(timeout=5)
    return proc


def test_describe_exit_names_signal_and_exit_code(monkeypatch):
    monkeypatch.setattr(connector, "_read_cgroup_oom_kill_count", lambda: 7)
    killed = connector._describe_exit(-9)
    assert killed["returncode"] == -9 and killed["signal"] == "SIGKILL", killed
    assert killed["cgroup_oom_kill_count"] == 7, killed
    plain = connector._describe_exit(2)
    assert plain["returncode"] == 2 and plain["signal"] is None, plain


def test_read_cgroup_oom_kill_count_v2_v1_and_unreadable(monkeypatch, tmp_path):
    v2 = tmp_path / "memory.events"
    v2.write_text("low 0\nhigh 0\nmax 12\noom 3\noom_kill 2\noom_group_kill 0\n")
    v1 = tmp_path / "memory.oom_control"
    v1.write_text("oom_kill_disable 0\nunder_oom 0\noom_kill 5\n")
    missing = str(tmp_path / "nope")

    monkeypatch.setattr(connector, "CGROUP_OOM_EVENT_FILES", (str(v2), str(v1)))
    assert connector._read_cgroup_oom_kill_count() == 2
    monkeypatch.setattr(connector, "CGROUP_OOM_EVENT_FILES", (missing, str(v1)))
    assert connector._read_cgroup_oom_kill_count() == 5
    garbage = tmp_path / "garbage"
    garbage.write_text("oom_kill notanumber\n")
    # missing file, bad number, and a directory: all unreadable -> None, never raises
    monkeypatch.setattr(connector, "CGROUP_OOM_EVENT_FILES", (missing, str(garbage), str(tmp_path)))
    assert connector._read_cgroup_oom_kill_count() is None


def test_supervisor_records_last_exit_for_sigkilled_worker(monkeypatch):
    c = C()
    c._supervisor_stop.set()
    monkeypatch.setattr(connector, "_read_cgroup_oom_kill_count", lambda: 1)
    wid = "sk"
    proc = _sigkilled_process()
    with c._lock:
        c.workers[wid] = {
            "process": proc, "pid": proc.pid, "config": {"consumer_group": wid},
            "metrics_port": 1, "restart_attempts": 0, "intentional_stop": False,
            "auto_restart": False,
        }
        first = c._record_worker_exit(wid, c.workers[wid])
        again = c._record_worker_exit(wid, c.workers[wid])
    assert first["signal"] == "SIGKILL" and first["returncode"] == -9, first
    assert first["pid"] == proc.pid and first["cgroup_oom_kill_count"] == 1, first
    assert again is first, "the same dead pid must be recorded once, not re-described every poll"

    st = c.sink_status({"config": {"consumer_group": wid}})
    assert st["status"] == "stopped", st
    assert st["last_exit"]["signal"] == "SIGKILL", st
    h = c.health()
    down = [u for u in h["unhealthy_workers"] if u["worker_id"] == wid]
    assert down and down[0]["last_exit"]["signal"] == "SIGKILL", h


def test_start_sink_dead_worker_records_last_exit(monkeypatch):
    c = C()
    c._supervisor_stop.set()
    monkeypatch.setattr(connector, "_read_cgroup_oom_kill_count", lambda: 4)
    dead = _sigkilled_process()
    with c._lock:
        c.workers["g"] = {
            "process": dead, "pid": dead.pid, "config": {"topics": ["t"]},
            "metrics_port": 1, "restart_attempts": 0, "intentional_stop": False,
            "auto_restart": True,
        }
    spawned = []

    def spawn(wc):
        p = subprocess.Popen(["sleep", "30"])
        spawned.append(p)
        return p

    monkeypatch.setattr(c, "_spawn_worker_process", spawn)
    monkeypatch.setattr(c, "_wait_for_worker_ready", lambda port, timeout=5: None)
    try:
        res = c.start_sink({"config": {
            "topics": "t", "consumer_group": "g",
            "destination_connector": "postgresql", "destination_config": {"table": "x"},
        }})
        assert res.get("status") == "started", res
        new = c.workers["g"]
        assert new["pid"] == spawned[0].pid
        assert new["last_exit"]["signal"] == "SIGKILL", new.get("last_exit")
        assert new["last_exit"]["pid"] == dead.pid, new.get("last_exit")
        assert new["last_exit"]["cgroup_oom_kill_count"] == 4
    finally:
        for p in spawned:
            p.terminate()


def test_start_sink_startup_exit_returns_last_exit(monkeypatch):
    c = C()
    c._supervisor_stop.set()
    monkeypatch.setattr(
        c, "_spawn_worker_process",
        lambda wc: subprocess.Popen(["python3", "-c", "import sys; sys.exit(3)"]),
    )
    # Wait for the child itself to exit (not a fixed sleep) so a slow runner can't flake.
    monkeypatch.setattr(
        c, "_wait_for_worker_ready",
        lambda port, timeout=5: c.workers["g3"]["process"].wait(timeout=10),
    )
    res = c.start_sink({"config": {
        "topics": "t", "consumer_group": "g3",
        "destination_connector": "postgresql", "destination_config": {"table": "x"},
    }})
    assert res.get("status") == "exited", res
    assert res["last_exit"]["returncode"] == 3 and res["last_exit"]["signal"] is None, res


def test_circuit_breaker_trip_keeps_last_exit(monkeypatch):
    c = C()
    c._supervisor_stop.set()
    monkeypatch.setattr(connector, "_read_cgroup_oom_kill_count", lambda: 9)
    wid = "cb"
    proc = _sigkilled_process()
    worker = {
        "process": proc, "pid": proc.pid, "config": {"consumer_group": wid},
        "metrics_port": 1, "restart_attempts": 4, "intentional_stop": False,
        "auto_restart": True, "last_spawn_monotonic": 100.0,
        "rapid_restarts": connector.MAX_RAPID_RESTARTS - 1,
    }
    with c._lock:
        c.workers[wid] = worker
        assert c._record_rapid_restart_and_maybe_trip(worker, wid, 101.0) is False
    assert worker["crashed"] is True
    assert worker["last_exit"]["signal"] == "SIGKILL", worker
    assert "signal=SIGKILL" in worker["last_error"], worker["last_error"]
    assert "cgroup_oom_kill_count=9" in worker["last_error"], worker["last_error"]

    st = c.sink_status({"config": {"consumer_group": wid}})
    assert st["status"] == "crashed" and st["last_exit"]["signal"] == "SIGKILL", st
    h = c.health()
    crashed = [u for u in h["unhealthy_workers"] if u["state"] == "crashed"]
    assert crashed and crashed[0]["last_exit"]["returncode"] == -9, h


class _OnePassStop:
    """Stand-in for _supervisor_stop: no wait, and exactly one loop iteration."""

    def __init__(self):
        self.calls = 0

    def wait(self, timeout=None):
        return False

    def is_set(self):
        self.calls += 1
        # call 1: while-check, call 2: post-wait check -> run the body once; call 3: stop.
        return self.calls > 2


def test_supervisor_loop_records_last_exit_before_respawn(monkeypatch):
    """The supervisor loop itself must record the death before _restart_worker swaps the
    process: once respawned, sink_status/health can no longer see the old exit."""
    c = C()
    c._supervisor_stop.set()
    monkeypatch.setattr(connector, "_read_cgroup_oom_kill_count", lambda: 6)
    wid = "loop"
    dead = _sigkilled_process()
    with c._lock:
        c.workers[wid] = {
            "process": dead, "pid": dead.pid, "config": {"consumer_group": wid},
            "metrics_port": 1, "restart_attempts": 0, "intentional_stop": False,
            "auto_restart": True,
        }
    spawned = []

    def spawn(wc):
        p = subprocess.Popen(["sleep", "30"])
        spawned.append(p)
        return p

    monkeypatch.setattr(c, "_spawn_worker_process", spawn)
    c._supervisor_stop = _OnePassStop()
    try:
        c._supervisor_loop()
        worker = c.workers[wid]
        assert len(spawned) == 1 and worker["pid"] == spawned[0].pid, worker
        assert worker["restart_attempts"] == 1, worker
        assert worker["last_exit"] is not None, "supervisor loop dropped the dead worker's exit"
        assert worker["last_exit"]["signal"] == "SIGKILL", worker["last_exit"]
        assert worker["last_exit"]["pid"] == dead.pid, worker["last_exit"]
        assert worker["last_exit"]["cgroup_oom_kill_count"] == 6, worker["last_exit"]
        # The respawned process's own death is recorded afresh, even if the kernel
        # handed it the old pid (simulated: pid reuse after the counter wraps).
        old_record = worker["last_exit"]
        spawned[0].kill()
        spawned[0].wait(timeout=5)
        with c._lock:
            worker["pid"] = dead.pid
            fresh = c._record_worker_exit(wid, worker)
        assert fresh is not old_record, "a reused pid must not hide the new death"
    finally:
        for p in spawned:
            if p.poll() is None:
                p.terminate()


def _spawned_worker_config(monkeypatch, config):
    """start_sink's worker_config as handed to the Go worker (the CONFIG env)."""
    c = C()
    seen = {}

    def spawn(wc):
        seen.update(wc)
        return subprocess.Popen(["python3", "-c", "import sys; sys.exit(1)"])

    monkeypatch.setattr(c, "_spawn_worker_process", spawn)
    monkeypatch.setattr(c, "_wait_for_worker_ready", lambda port, timeout=5: time.sleep(0.4))
    c.start_sink({"config": config})
    return seen


def test_start_sink_forwards_object_layout_fields(monkeypatch):
    # worker_config is an allowlist: a layout field missing there reaches the Go
    # worker as v1, and a v2 pipeline would write pipeline-id keys again.
    base = {
        "topics": "t",
        "consumer_group": "g",
        "destination_connector": "gcs",
        "destination_config": {"bucket": "b"},
        "destination_namespace": "sales",
    }
    wc = _spawned_worker_config(monkeypatch, dict(
        base, storage_layout_version=2, source_family="postgresql", source_database="datingapp",
    ))
    assert wc["storage_layout_version"] == 2
    assert wc["source_family"] == "postgresql"
    assert wc["source_database"] == "datingapp"
    assert wc["destination_namespace"] == "sales"

    wc = _spawned_worker_config(monkeypatch, base)
    assert (wc["storage_layout_version"], wc["source_family"], wc["source_database"]) == (0, "", "")


def test_start_sink_forwards_mirror_source_namespace(monkeypatch):
    # Same allowlist rule: dropped here, a server-level source's databases all
    # collapse into the destination connection's one database.
    base = {
        "topics": "t",
        "consumer_group": "g",
        "destination_connector": "mysql",
        "destination_config": {"host": "h"},
    }
    assert _spawned_worker_config(monkeypatch, dict(base, mirror_source_namespace=True))["mirror_source_namespace"] is True
    assert _spawned_worker_config(monkeypatch, base)["mirror_source_namespace"] is False
