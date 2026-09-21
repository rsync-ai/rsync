"""kafka-mcp-sink start_sink: the object-storage CDC flush interval reaches the worker.

The worker's flush interval for object-storage CDC is set per destination with
`destination_config.max_file_interval_seconds`. start_sink must pass that setting to
the Go worker untouched, and must refuse a bad value itself, before spawning: a worker
that rejects its config exits before it can report why, so the caller would only ever
see "worker exited during startup".

The connector module is loaded from the versioned directory the container actually
runs, by file path and under a unique module name (several connectors are named
connector.py), with that directory's own base_connector. sys.path and sys.modules are
restored afterwards so other tests in the session still see their own imports.
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_SINK_DIR = os.path.join(
    _HERE, "..", "internal", "kafka-mcp-sink", "versions", "v1.0.0"
)
_SINK_DIR = os.path.abspath(_SINK_DIR)
# The case list the worker's own test runs (cdc_flush_interval_test.go), so the connector
# never lets through a value the worker would refuse at startup, or refuses one it takes.
_SHARED_CASES = os.path.abspath(os.path.join(
    _HERE, "..", "internal", "kafka-mcp-sink", "worker-src", "cmd", "kafka-sink-worker",
    "testdata", "flush_interval_cases.json",
))


def _load_sink_connector():
    saved_path = list(sys.path)
    saved_base = sys.modules.pop("base_connector", None)
    sys.path.insert(0, _SINK_DIR)
    try:
        spec = importlib.util.spec_from_file_location(
            "kafka_mcp_sink_connector_flush_interval", os.path.join(_SINK_DIR, "connector.py")
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        sys.path[:] = saved_path
        sys.modules.pop("base_connector", None)
        if saved_base is not None:
            sys.modules["base_connector"] = saved_base


sink = _load_sink_connector()


class _FakeProcess:
    pid = 424242

    def poll(self):
        return None


@pytest.fixture
def connector(monkeypatch):
    c = sink.KafkaMCPSinkConnector()
    # The supervisor thread would try to respawn the fake worker; stop it.
    c._supervisor_stop.set()
    spawned = []

    def fake_spawn(worker_config):
        spawned.append(worker_config)
        return _FakeProcess()

    monkeypatch.setattr(c, "_spawn_worker_process", fake_spawn)
    monkeypatch.setattr(c, "_wait_for_worker_ready", lambda port, timeout=5: None)
    monkeypatch.setattr(c, "_is_worker_alive", lambda worker_id: bool(c.workers.get(worker_id)))
    c.spawned = spawned
    return c


def _start(c, destination_config, consumer_group="g-flush"):
    return c.start_sink({"config": {
        "topics": ["cdc.shop.orders"],
        "consumer_group": consumer_group,
        "destination_connector": "gcs",
        "destination_version": "v1.0.0",
        "destination_config": destination_config,
        "sink_mode": "cdc",
    }})


with open(_SHARED_CASES, encoding="utf-8") as _f:
    SHARED = json.load(_f)


def test_shared_cases_match_the_connector_bounds():
    assert (SHARED["min_seconds"], SHARED["max_seconds"]) == (
        sink.MIN_FLUSH_INTERVAL_SECONDS, sink.MAX_FLUSH_INTERVAL_SECONDS,
    ), "update the shared cases, the worker's main.go and connector.py together"
    kinds = {"accepted": 0, "unset": 0, "refused": 0}
    for case in SHARED["cases"]:
        expect = case["expect"]
        kinds["accepted" if isinstance(expect, int) and not isinstance(expect, bool) else expect] += 1
    assert kinds["accepted"] >= 5 and kinds["unset"] >= 5 and kinds["refused"] >= 20, kinds
    assert sum(kinds.values()) == len(SHARED["cases"])


@pytest.mark.parametrize("case", SHARED["cases"], ids=[c["name"] for c in SHARED["cases"]])
def test_start_sink_agrees_with_the_worker_on_every_shared_case(connector, case):
    dest = {"bucket": "b", "max_file_interval_seconds": case["value"]}
    res = _start(connector, dest)
    if case["expect"] == "refused":
        assert res.get("success") is False, res
        assert res.get("status") == "invalid_config", res
        assert connector.spawned == []
    else:
        assert res.get("success") is True, res
        assert len(connector.spawned) == 1
        # Forwarded as given; the worker reads it the same way the Go test proves.
        assert connector.spawned[0]["destination_config"] == dest


@pytest.mark.parametrize("value", [120, "120", " 45 ", 1, 240, "240", 240.0, 60.0, "+30", "030"])
def test_valid_interval_is_forwarded_to_the_worker_unchanged(connector, value):
    dest = {"bucket": "b", "file_format": "parquet", "max_file_interval_seconds": value}
    res = _start(connector, dest)
    assert res.get("success") is True, res
    assert len(connector.spawned) == 1
    forwarded = connector.spawned[0]["destination_config"]
    assert forwarded["max_file_interval_seconds"] == value
    # The rest of the destination config travels with it.
    assert forwarded["bucket"] == "b" and forwarded["file_format"] == "parquet"


@pytest.mark.parametrize("dest", [
    {"bucket": "b"},
    {"bucket": "b", "max_file_interval_seconds": None},
    {"bucket": "b", "max_file_interval_seconds": ""},
    {"bucket": "b", "max_file_interval_seconds": "<nil>"},
    {"bucket": "b", "max_file_interval_seconds": "null"},
    {"bucket": "b", "max_file_interval_seconds": "NULL"},
    {"bucket": "b", "max_file_interval_seconds": " null "},
])
def test_unset_interval_starts_the_worker_on_its_default(connector, dest):
    res = _start(connector, dest)
    assert res.get("success") is True, res
    assert len(connector.spawned) == 1
    assert connector.spawned[0]["destination_config"] == dest


def test_missing_destination_config_does_not_stop_the_start(connector):
    # A caller that sends destination_config: null has no interval to check; the start
    # goes on to spawn the worker as it did before the check existed.
    res = _start(connector, None)
    assert res.get("success") is True, res
    assert len(connector.spawned) == 1
    assert connector.spawned[0]["destination_config"] is None


@pytest.mark.parametrize("value", [
    0, "0", "-0", -5, "-5", 241, "241", 900, "900", 240.5, 0.9999, 10**12, 30.5, "30.5",
    "abc", "1_000", "1_0", "0x1E", "+", "-", "\u0663\u0660",
    True, False, [30], {"seconds": 30}, float("inf"), float("nan"),
])
def test_invalid_interval_is_refused_before_any_worker_starts(connector, value):
    res = _start(connector, {"bucket": "b", "max_file_interval_seconds": value})
    assert res.get("success") is False, res
    assert res.get("status") == "invalid_config", res
    error = res.get("error") or ""
    assert "max_file_interval_seconds" in error
    assert "whole number of seconds from 1 to 240" in error
    assert "30-second default" in error
    assert connector.spawned == [], "a worker was spawned with a refused interval"
    assert connector.workers == {}
    # The metrics port reserved for the refused start is handed back.
    assert connector._reserved_ports == set()


def test_refusal_shows_the_value_that_was_refused(connector):
    res = _start(connector, {"max_file_interval_seconds": "abc"})
    assert res.get("status") == "invalid_config", res
    assert "(got 'abc')" in res["error"], res["error"]


def test_refusal_echo_is_bounded(connector):
    res = _start(connector, {"max_file_interval_seconds": "x" * 500})
    assert res.get("success") is False
    assert len(res["error"]) < 200, res["error"]


def test_a_refused_start_does_not_block_a_corrected_one(connector):
    bad = _start(connector, {"bucket": "b", "max_file_interval_seconds": 0})
    assert bad.get("success") is False
    good = _start(connector, {"bucket": "b", "max_file_interval_seconds": 200})
    assert good.get("success") is True, good
    assert [w["destination_config"]["max_file_interval_seconds"] for w in connector.spawned] == [200]


# ---------------------------------------------------------------------------
# The connection form.
#
# The setting is only reachable from the UI if the storage connectors declare it: the
# connection form renders the fields in metadata.json and nothing else, and the
# orchestrator copies every saved connection key into the sink's destination_config.

_STORAGE_ROOT = os.path.abspath(os.path.join(_HERE, "..", "public", "storage"))
_OBJECT_STORAGE = ("gcs", "aws-s3", "azure-blob")


def _declared_flush_interval_fields():
    fields = []
    for name in _OBJECT_STORAGE:
        conn_dir = os.path.join(_STORAGE_ROOT, name)
        with open(os.path.join(conn_dir, "latest.json")) as f:
            current = json.load(f)["current_version"]
        with open(os.path.join(conn_dir, "versions", current, "metadata.json")) as f:
            meta = json.load(f)
        # Both blocks: the form reads configuration_schema, other callers read
        # config_schema, and a field in only one of them is a drift bug.
        for block in ("configuration_schema", "config_schema"):
            props = meta.get(block, {}).get("properties", {})
            fields.append((f"{name}.{block}", props.get(sink.FLUSH_INTERVAL_KEY)))
    return fields


def test_object_storage_connection_form_declares_the_flush_interval():
    fields = _declared_flush_interval_fields()
    assert len(fields) == 2 * len(_OBJECT_STORAGE)
    missing = [where for where, field in fields if field is None]
    assert not missing, f"{sink.FLUSH_INTERVAL_KEY} is not declared in: {missing}"
    for where, field in fields:
        assert field["type"] == "integer", where
        assert field.get("applies") == "destination", where


def test_the_form_field_has_no_default():
    # The form saves a field's schema default onto every connection it creates, so a
    # default would be stored as if the user had typed it. "default": 0 would stop every
    # CDC pipeline on that connection from starting; any other number would pin the
    # value, so a later change to the worker's own default would never reach it. Blank
    # must mean "not set".
    for where, field in _declared_flush_interval_fields():
        assert "default" not in field, f"{where} declares default={field.get('default')!r}"


def test_the_form_describes_the_bounds_the_connector_enforces():
    bounds = f"{sink.MIN_FLUSH_INTERVAL_SECONDS} to {sink.MAX_FLUSH_INTERVAL_SECONDS}"
    for where, field in _declared_flush_interval_fields():
        assert bounds in field["description"], (
            f"{where} description does not say {bounds!r}: {field['description']!r}"
        )
