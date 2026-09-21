"""gcs_get_cdc_offsets (Tier C durable CDC high-water marks) — #16.

The kafka-mcp-sink called gcs_get_cdc_offsets at startup and got "Unknown tool", so
the high-water seed was always skipped. These tests drive the real connector against
an in-memory fake GCS client (google-cloud-storage is not needed): import_data stamps
CDC provenance into blob metadata, and get_cdc_offsets reads it back through the MCP
dispatcher the sink actually uses.
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
_d = _HERE
while _d != os.path.dirname(_d):
    if os.path.isdir(os.path.join(_d, "rsync_protocol")):
        sys.path.insert(0, _d)
        break
    _d = os.path.dirname(_d)

import connector as C  # noqa: E402


class _FakeBlob:
    def __init__(self, store, bucket, name):
        self._store, self._bucket, self.name = store, bucket, name
        self.metadata = None

    def upload_from_string(self, body, content_type=None):
        # Metadata must be set BEFORE upload to be persisted with the object.
        self._store[(self._bucket, self.name)] = (bytes(body), dict(self.metadata or {}))


class _Listed:
    def __init__(self, name, metadata):
        self.name, self.metadata = name, metadata


class _FakeBucket:
    def __init__(self, store, name):
        self._store, self._name = store, name

    def blob(self, key):
        return _FakeBlob(self._store, self._name, key)


class _FakeClient:
    def __init__(self, fail_list=False):
        self.store = {}
        self.fail_list = fail_list
        self.listed_prefixes = []

    def bucket(self, name):
        return _FakeBucket(self.store, name)

    def list_blobs(self, bucket, prefix=None, max_results=None):
        self.listed_prefixes.append(prefix)
        if self.fail_list:
            raise RuntimeError("403 storage.objects.list denied")
        for (b, name), (_, meta) in sorted(self.store.items()):
            if b == bucket and name.startswith(prefix or ""):
                yield _Listed(name, meta or None)


def _server(monkeypatch, client):
    s = C.GcsMCPServer()
    monkeypatch.setattr(s, "_get_gcs_client", lambda config: client)
    return s


def _call(s, tool, args):
    return s.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                             "params": {"name": tool, "arguments": args}})


def _result(resp):
    return resp.get("result", resp) if isinstance(resp, dict) else resp


def _write(s, key, pipeline, topic, partition, first, last):
    return s.import_data({
        "config": {"bucket": "b"}, "bucket": "b", "key": key, "format": "jsonl",
        "data": [{"op": "I", "kafka_offset": first}],
        "object_metadata": {"rsync_pipeline_id": pipeline, "rsync_topic": topic,
                            "rsync_partition": partition, "rsync_first_offset": first,
                            "rsync_last_offset": last},
    })


def test_tool_is_dispatchable_not_unknown(monkeypatch):
    s = _server(monkeypatch, _FakeClient())
    res = _result(_call(s, "gcs_get_cdc_offsets",
                        {"config": {"bucket": "b"}, "pipeline_id": "p1", "prefix": "demo/p1/"}))
    assert "Unknown tool" not in str(res), res
    assert res.get("success") is True and res.get("offsets") == [], res
    names = [op["name"] for op in s.get_capabilities()["operations"]]
    assert "get_cdc_offsets" in names


def test_returns_max_last_offset_per_topic_partition(monkeypatch):
    client = _FakeClient()
    s = _server(monkeypatch, client)
    t = "rsync.cdc-p1.datingapp.users"
    assert _write(s, "demo/p1/cdc/users/dt=2026-09-16/a-100.jsonl", "p1", t, 0, 100, 149)["success"]
    assert _write(s, "demo/p1/cdc/users/dt=2026-09-16/b-150.jsonl", "p1", t, 0, 150, 180)["success"]
    assert _write(s, "demo/p1/cdc/users/dt=2026-09-16/c-p1-7.jsonl", "p1", t, 1, 7, 9)["success"]
    # Metadata really is persisted with the object (set before upload).
    assert client.store[("b", "demo/p1/cdc/users/dt=2026-09-16/b-150.jsonl")][1]["rsync_last_offset"] == "180"

    res = _result(_call(s, "gcs_get_cdc_offsets",
                        {"config": {"bucket": "b"}, "pipeline_id": "p1", "prefix": "demo/p1/"}))
    assert res["success"] is True
    # The LAST offset (180), not the key's first offset (150): seeding 150 would let
    # a redelivered 151..180 land in a new object as duplicates.
    assert res["offsets"] == [{"topic": t, "partition": 0, "offset": 180},
                              {"topic": t, "partition": 1, "offset": 9}], res
    assert client.listed_prefixes[-1] == "demo/p1/"


def test_ignores_other_pipelines_and_unstamped_objects(monkeypatch):
    client = _FakeClient()
    s = _server(monkeypatch, client)
    t = "rsync.cdc.shop.orders"
    _write(s, "demo/p1/shop/orders/dt=2026-09-16/a-1.jsonl", "p1", t, 0, 1, 5)
    # A second pipeline whose root merely starts with "p1" and a stale pre-fix object.
    _write(s, "demo/p1/shop/orders/dt=2026-09-16/z-900.jsonl", "p10", t, 0, 900, 999)
    s.import_data({"config": {"bucket": "b"}, "bucket": "b", "format": "jsonl",
                   "key": "demo/p1/shop/orders/dt=2026-09-16/legacy-5000.jsonl", "data": [{"x": 1}]})
    res = s.get_cdc_offsets({"config": {"bucket": "b"}, "pipeline_id": "p1", "prefix": "demo/p1/"})
    assert res["offsets"] == [{"topic": t, "partition": 0, "offset": 5}], res


def test_never_errors(monkeypatch):
    # Listing failure → empty success (§2.3), so the sink starts exactly as before.
    s = _server(monkeypatch, _FakeClient(fail_list=True))
    res = s.get_cdc_offsets({"config": {"bucket": "b"}, "pipeline_id": "p1", "prefix": "demo/p1/"})
    assert res["success"] is True and res["offsets"] == []
    # Missing prefix → no unbounded whole-bucket scan.
    client = _FakeClient()
    s = _server(monkeypatch, client)
    res = s.get_cdc_offsets({"config": {"bucket": "b"}, "pipeline_id": "p1"})
    assert res["success"] is True and res["offsets"] == [] and client.listed_prefixes == []
    # Malformed metadata values are skipped, not raised.
    client.store[("b", "demo/p1/x.jsonl")] = (b"", {"rsync_pipeline_id": "p1", "rsync_topic": "t",
                                                  "rsync_partition": "zero", "rsync_last_offset": "9"})
    res = s.get_cdc_offsets({"config": {"bucket": "b"}, "pipeline_id": "p1", "prefix": "demo/p1/"})
    assert res["success"] is True and res["offsets"] == []


def test_import_without_object_metadata_is_unchanged(monkeypatch):
    client = _FakeClient()
    s = _server(monkeypatch, client)
    res = s.import_data({"config": {"bucket": "b"}, "bucket": "b", "key": "k.jsonl",
                         "format": "jsonl", "data": [{"a": 1}]})
    assert res["success"] is True
    assert client.store[("b", "k.jsonl")][1] == {}
