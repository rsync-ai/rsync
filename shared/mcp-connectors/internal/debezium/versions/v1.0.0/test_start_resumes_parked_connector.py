#!/usr/bin/env python3
"""Start after Stop resumes the connector.

Stop parks a CDC connector in Kafka Connect's STOPPED state (PAUSED on a Connect
without /stop) and keeps its offsets, slot and publication. Start then POSTs the
connector again and gets 409. That path used to return success without touching
the target state, so the pipeline read running while the connector captured
nothing. It now resumes a parked connector, for every source database.

The parked state is read BEFORE the config PUT. A PUT restarts the connector, and
a status read a few ms later returns UNASSIGNED/RESTARTING, not STOPPED: on GKE
1 of 3 Starts read that, skipped the resume, and captured nothing. The fake below
reports UNASSIGNED after a config PUT, so a status read taken after it fails.

Run: python3 -m pytest test_start_resumes_parked_connector.py -q
"""
import connector


class _Resp:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body
        self.text = ""

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class _Connect:
    """A connector that already exists (POST 409) in `state`. A config PUT
    restarts it, so status reads UNASSIGNED afterwards, as on a real worker in
    the ms after the PUT; the target state (STOPPED/PAUSED) does not change."""

    def __init__(self, state, same_config=True, resume_status=202):
        self.state = state
        self.same_config = same_config
        self.resume_status = resume_status
        self.calls = []
        self.cfg = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def post(self, url, json=None, **k):
        self.calls.append("POST " + url.split("/", 3)[-1])
        self.cfg = dict(json["config"])
        return _Resp(409)

    def get(self, url, **k):
        path = url.split("/", 3)[-1]
        self.calls.append("GET " + path)
        if path.endswith("/config"):
            return _Resp(200, dict(self.cfg) if self.same_config else {"stale": "x"})
        return _Resp(200, {"connector": {"state": self.state}, "tasks": []})

    def put(self, url, json=None, **k):
        path = url.split("/", 3)[-1]
        self.calls.append("PUT " + path)
        if path.endswith("/resume"):
            return _Resp(self.resume_status)
        self.state = "UNASSIGNED"
        return _Resp(200)


def _start(fake, db_type="postgresql"):
    srv = connector.DebeziumConnector()
    srv._client = lambda: fake
    args = {
        "database_type": db_type,
        "connector_name": "cdc-abc12345",
        "db_host": "db.example.com",
        "db_user": "svc",
        "db_password": "pw",
        "db_name": "app",
        "tables": ["public.users"] if db_type == "postgresql" else ["app.users"],
        "cdc_mode": "initial",
        "snapshot_mode": "initial",
    }
    return srv.debezium_start_sync(args)


def _resumed(fake):
    return any(c.endswith("/resume") for c in fake.calls)


def test_stopped_connector_is_resumed_for_every_source():
    for db_type in ("postgresql", "mysql", "mongodb"):
        fake = _Connect("STOPPED")
        out = _start(fake, db_type)
        assert out["success"] is True, out
        assert _resumed(fake), (db_type, fake.calls)
        assert out["resumed_from"] == "stopped"
        assert "already_running" not in out


def test_paused_connector_is_resumed():
    fake = _Connect("PAUSED")
    out = _start(fake)
    assert out["success"] is True and _resumed(fake), fake.calls
    assert out["resumed_from"] == "paused"


def test_config_change_then_resume():
    # A config PUT on a STOPPED connector keeps it STOPPED; resume must follow it.
    fake = _Connect("STOPPED", same_config=False)
    out = _start(fake)
    assert out["success"] is True, out
    puts = [c for c in fake.calls if c.startswith("PUT")]
    assert puts == ["PUT connectors/cdc-abc12345/config", "PUT connectors/cdc-abc12345/resume"], fake.calls


def test_parked_state_is_read_before_the_config_put():
    # Reading it after the PUT sees the restart (UNASSIGNED) and skips the resume.
    for state in ("STOPPED", "PAUSED"):
        fake = _Connect(state, same_config=False)
        out = _start(fake)
        assert out["success"] is True and _resumed(fake), (state, fake.calls)
        assert out["resumed_from"] == state.lower()
        status = fake.calls.index("GET connectors/cdc-abc12345/status")
        put = fake.calls.index("PUT connectors/cdc-abc12345/config")
        assert status < put, fake.calls


def test_running_connector_with_a_config_change_is_not_resumed():
    fake = _Connect("RUNNING", same_config=False)
    out = _start(fake)
    assert out["success"] is True and not _resumed(fake), fake.calls
    assert "resumed_from" not in out


def test_running_connector_is_left_alone():
    fake = _Connect("RUNNING")
    out = _start(fake)
    assert out["success"] is True and not _resumed(fake), fake.calls
    assert out.get("already_running") is True
    assert "resumed_from" not in out


def test_refused_resume_fails_start():
    fake = _Connect("STOPPED", resume_status=500)
    out = _start(fake)
    assert out["success"] is False, out
    # The healer must not read this as transient and retry into the same wall.
    err = out["error"].lower()
    assert "refused to resume" in err
    assert "timeout" not in err and "connection refused" not in err
