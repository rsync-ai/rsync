"""get_cdc_offsets: one contract for gcs, aws-s3, azure-blob and the template.

Layout v2 keys carry no pipeline id, so a v2 CDC object's Kafka coordinates travel
only as object metadata that the kafka-mcp-sink stamps at import_data
(cdcObjectMetadata in kafka-sink-worker main.go). At startup the sink calls
``<dest>_get_cdc_offsets`` to recover the durable high-water mark per
(topic, partition) and skip redelivered events (docs/connectors/cdc-exactly-once-offsets.md
§5). Every object store that can hold a v2 pipeline must therefore:

  * persist ``object_metadata`` with the object it writes, atomically;
  * answer get_cdc_offsets with the max ``rsync_last_offset`` per (topic, partition)
    over THIS pipeline's objects under the pipeline root;
  * never return an error (§2.3): a failure is ``{"success": True, "offsets": []}``,
    which the sink reads as "nothing to skip" (duplicates, never loss);
  * refuse to scan without a pipeline id, bucket/container and prefix.

The scenarios run against an in-memory fake of each SDK through the MCP dispatcher
the sink uses (tools/call), so no network, emulator or cloud SDK is needed.
"""
from __future__ import annotations

import datetime as _dt
import importlib.util
import json
import re
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]  # shared/mcp-connectors
STORAGE = ROOT / "public" / "storage"
TEMPLATE = ROOT.parents[1] / "llm-service" / "src" / "agents" / "tool_generator" / "templates" / "connector.py.j2"

CONNECTORS = {
    "gcs": "GcsMCPServer",
    "aws-s3": "AwsS3MCPServer",
    "azure-blob": "AzureBlobMCPServer",
}
BUCKET = "lake-bucket"
ROOT_PREFIX = "lake/sales_prod/"
TOPIC = "rsync.cdc-p1.shop.orders"


def _current_dir(name: str) -> Path:
    latest = json.loads((STORAGE / name / "latest.json").read_text())
    return STORAGE / name / "versions" / latest["current_version"]


def _load_server(name: str):
    path = _current_dir(name) / "connector.py"
    spec = importlib.util.spec_from_file_location(f"_cdc_offsets_contract_{name.replace('-', '_')}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, CONNECTORS[name])()


# --- one in-memory object store, three SDK faces ------------------------------------


class _ClientError(Exception):
    """Shaped like botocore.exceptions.ClientError: the code lives in .response."""

    def __init__(self, code):
        super().__init__(f"An error occurred ({code}) when calling the HeadObject operation")
        self.response = {"Error": {"Code": code}}


class _Store:
    def __init__(self):
        self.objects = {}  # key -> (body, metadata, mtime)
        self.clock = 1_700_000_000
        self.list_fails = False
        self.head_errors = {}  # key -> S3 error code
        self.list_calls = 0
        self.heads = []

    def put(self, key, body, metadata):
        self.clock += 1
        self.objects[key] = (bytes(body), dict(metadata or {}), self.clock)

    def listing(self, prefix):
        self.list_calls += 1
        if self.list_fails:
            raise RuntimeError("403 list denied")
        return sorted(k for k in self.objects if k.startswith(prefix or ""))

    def meta(self, key):
        return dict(self.objects[key][1])


class _FakeS3:
    def __init__(self, store):
        self.s = store

    def list_objects_v2(self, Bucket, Prefix="", MaxKeys=1000, ContinuationToken=None):
        assert Bucket == BUCKET and MaxKeys <= 1000
        keys = [k for k in self.s.listing(Prefix) if ContinuationToken is None or k > ContinuationToken]
        page = keys[:MaxKeys]
        resp = {
            "Contents": [
                {"Key": k, "LastModified": _dt.datetime.fromtimestamp(self.s.objects[k][2], tz=_dt.timezone.utc)}
                for k in page
            ],
            "IsTruncated": len(keys) > MaxKeys,
        }
        if resp["IsTruncated"]:
            resp["NextContinuationToken"] = page[-1]
        return resp

    def head_object(self, Bucket, Key):
        self.s.heads.append(Key)
        if Key in self.s.head_errors:
            raise _ClientError(self.s.head_errors[Key])
        if Key not in self.s.objects:
            raise _ClientError("404")
        return {"Metadata": self.s.meta(Key)}

    def put_object(self, Bucket, Key, Body, ContentType=None, Metadata=None):
        self.s.put(Key, Body, Metadata)

    def head_bucket(self, Bucket):
        return {}


class _Listed:
    def __init__(self, name, metadata):
        self.name, self.metadata = name, metadata


class _FakeGCSBlob:
    def __init__(self, store, key):
        self.s, self.name, self.metadata = store, key, None

    def upload_from_string(self, body, content_type=None):
        self.s.put(self.name, body, self.metadata)


class _FakeGCS:
    def __init__(self, store):
        self.s = store

    def bucket(self, name):
        assert name == BUCKET
        return types.SimpleNamespace(blob=lambda key: _FakeGCSBlob(self.s, key))

    def list_blobs(self, bucket, prefix=None, max_results=None):
        assert bucket == BUCKET
        for k in self.s.listing(prefix):
            yield _Listed(k, self.s.meta(k) or None)


class _FakeAzureBlobClient:
    def __init__(self, store, key):
        self.s, self.key = store, key

    def upload_blob(self, body, overwrite=False, content_settings=None, metadata=None):
        self.s.put(self.key, body, metadata)


class _FakeAzureContainer:
    def __init__(self, store):
        self.s = store

    def get_blob_client(self, key):
        return _FakeAzureBlobClient(self.s, key)

    def list_blobs(self, name_starts_with=None, include=None):
        # Azure returns blob metadata in a listing only when asked for it.
        with_meta = "metadata" in (include or [])
        for k in self.s.listing(name_starts_with):
            yield _Listed(k, (self.s.meta(k) or None) if with_meta else None)


class _FakeAzure:
    def __init__(self, store):
        self.s = store

    def get_container_client(self, name):
        assert name == BUCKET
        return _FakeAzureContainer(self.s)


def _wire(name, server, store, monkeypatch):
    if name == "aws-s3":
        s3 = _FakeS3(store)
        server._get_s3_client = lambda config: s3
        # import_data's row path builds its own boto3 client.
        boto3 = types.ModuleType("boto3")
        boto3.client = lambda *a, **k: s3
        botocore = types.ModuleType("botocore")
        botocore_config = types.ModuleType("botocore.config")
        botocore_config.Config = lambda *a, **k: None
        monkeypatch.setitem(sys.modules, "boto3", boto3)
        monkeypatch.setitem(sys.modules, "botocore", botocore)
        monkeypatch.setitem(sys.modules, "botocore.config", botocore_config)
    elif name == "gcs":
        gcs = _FakeGCS(store)
        server._get_gcs_client = lambda config: gcs
    else:
        az = _FakeAzure(store)
        server._get_blob_service_client = lambda config: az
        blob_mod = types.ModuleType("azure.storage.blob")
        blob_mod.ContentSettings = lambda **k: k
        monkeypatch.setitem(sys.modules, "azure", types.ModuleType("azure"))
        monkeypatch.setitem(sys.modules, "azure.storage", types.ModuleType("azure.storage"))
        monkeypatch.setitem(sys.modules, "azure.storage.blob", blob_mod)


@pytest.fixture(params=sorted(CONNECTORS))
def conn(request, monkeypatch):
    name = request.param
    server, store = _load_server(name), _Store()
    _wire(name, server, store, monkeypatch)
    return name, server, store


def _args(name, **extra):
    """The arguments the sink's getCDCOffsetsArgs sends (azure gets `container`)."""
    args = {"config": {"bucket": BUCKET}, "pipeline_id": "p1", "prefix": ROOT_PREFIX}
    args["container" if name == "azure-blob" else "bucket"] = BUCKET
    args.update(extra)
    return args


def _call(name, server, args):
    resp = server.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                                  "params": {"name": f"{name}_get_cdc_offsets", "arguments": args}})
    return resp.get("result", resp) if isinstance(resp, dict) else resp


def _write(server, key, pipeline="p1", topic=TOPIC, partition=0, first=0, last=0, stamp=True):
    params = {"config": {"bucket": BUCKET}, "bucket": BUCKET, "key": key, "format": "jsonl",
              "data": [{"op": "I", "kafka_offset": first}]}
    if stamp:
        # Exactly what cdcObjectMetadata sends: every value a string.
        params["object_metadata"] = {"rsync_pipeline_id": pipeline, "rsync_topic": topic,
                                     "rsync_partition": str(partition),
                                     "rsync_first_offset": str(first), "rsync_last_offset": str(last)}
    res = server.import_data(params)
    assert res.get("success") is True, res
    return res


def _key(leaf, table="orders"):
    return f"{ROOT_PREFIX}shop/public/{table}/dt=2026-09-18/{leaf}"


# --- the contract --------------------------------------------------------------------


def test_the_tool_is_advertised_and_dispatchable(conn):
    name, server, _ = conn
    ops = {op["name"]: op for op in server.get_capabilities()["operations"]}
    assert ops.get("get_cdc_offsets", {}).get("method") == f"{name}_get_cdc_offsets", ops.keys()
    res = _call(name, server, _args(name))
    assert "Unknown tool" not in str(res), res
    assert res["success"] is True and res["offsets"] == [], res


def test_import_data_persists_the_metadata_with_the_object(conn):
    _, server, store = conn
    _write(server, _key("20260918-101500000-100.parquet"), first=100, last=149)
    assert store.meta(_key("20260918-101500000-100.parquet")) == {
        "rsync_pipeline_id": "p1", "rsync_topic": TOPIC, "rsync_partition": "0",
        "rsync_first_offset": "100", "rsync_last_offset": "149"}


def test_import_data_without_metadata_writes_none(conn):
    _, server, store = conn
    _write(server, _key("LOAD00000001.parquet"), stamp=False)
    assert store.meta(_key("LOAD00000001.parquet")) == {}


def test_max_last_offset_per_topic_partition(conn):
    name, server, _ = conn
    _write(server, _key("20260918-101500000-100.parquet"), first=100, last=149)
    _write(server, _key("20260918-101600000-150.parquet"), first=150, last=180)
    _write(server, _key("20260918-101700000-p1-7.parquet"), partition=1, first=7, last=9)
    _write(server, _key("20260918-101800000-3.parquet", table="users"),
           topic="rsync.cdc-p1.shop.users", first=3, last=4)
    res = _call(name, server, _args(name))
    assert res["success"] is True, res
    # The LAST offset (180), not the key's first offset (150).
    assert res["offsets"] == [
        {"topic": TOPIC, "partition": 0, "offset": 180},
        {"topic": TOPIC, "partition": 1, "offset": 9},
        {"topic": "rsync.cdc-p1.shop.users", "partition": 0, "offset": 4},
    ], res
    assert res["truncated"] is False, res


def test_other_pipelines_unstamped_and_malformed_objects_are_ignored(conn):
    name, server, store = conn
    _write(server, _key("20260918-101500000-1.parquet"), first=1, last=5)
    # Layout v2 has no pipeline id in the key: a recreated pipeline writing the same
    # folder must not hand its predecessor's offsets to the new one.
    _write(server, _key("20260918-101600000-900.parquet"), pipeline="p0-old", first=900, last=999)
    _write(server, _key("LOAD00000001.parquet"), stamp=False)
    store.put(_key("20260918-101700000-50.parquet"), b"", {
        "rsync_pipeline_id": "p1", "rsync_topic": TOPIC, "rsync_partition": "zero", "rsync_last_offset": "70"})
    store.put(_key("20260918-101800000-60.parquet"), b"", {
        "rsync_pipeline_id": "p1", "rsync_topic": "", "rsync_partition": "0", "rsync_last_offset": "80"})
    # Outside the pipeline root.
    store.put("lake/other_prod/shop/public/orders/dt=2026-09-18/x-1.parquet", b"", {
        "rsync_pipeline_id": "p1", "rsync_topic": TOPIC, "rsync_partition": "0", "rsync_last_offset": "5000"})
    res = _call(name, server, _args(name))
    assert res["success"] is True and res["offsets"] == [{"topic": TOPIC, "partition": 0, "offset": 5}], res


@pytest.mark.parametrize("missing", ["pipeline_id", "prefix", "bucket"])
def test_an_unbounded_scan_is_refused_without_listing(conn, missing):
    name, server, store = conn
    _write(server, _key("20260918-101500000-1.parquet"), first=1, last=5)
    args = _args(name)
    if missing == "bucket":
        args.pop("bucket", None)
        args.pop("container", None)
        args["config"] = {}
    else:
        args.pop(missing)
    res = _call(name, server, args)
    assert res["success"] is True and res["offsets"] == [], res
    assert store.list_calls == 0, "scanned without a bounded root"


def test_a_listing_failure_is_an_empty_success(conn):
    name, server, store = conn
    _write(server, _key("20260918-101500000-1.parquet"), first=1, last=5)
    store.list_fails = True
    res = _call(name, server, _args(name))
    assert res["success"] is True and res["offsets"] == [] and "error" not in res, res


def test_max_objects_bounds_the_scan_and_says_so(conn):
    name, server, _ = conn
    for i in range(5):
        _write(server, _key(f"20260918-10150000{i}-{i * 10}.parquet"), first=i * 10, last=i * 10 + 9)
    res = _call(name, server, _args(name, max_objects=2))
    assert res["success"] is True and res["truncated"] is True and res["objects_scanned"] == 2, res
    # A bounded answer may only be LOW (duplicates on replay), never above the truth.
    assert all(o["offset"] <= 49 for o in res["offsets"]), res


# --- aws-s3 only: a listing has no metadata, so each object costs a HEAD --------------


@pytest.fixture
def s3(monkeypatch):
    server, store = _load_server("aws-s3"), _Store()
    _wire("aws-s3", server, store, monkeypatch)
    return server, store


def test_s3_sidecars_are_not_headed(s3):
    server, store = s3
    _write(server, _key("20260918-101500000-1.parquet"), first=1, last=5)
    store.put(f"{ROOT_PREFIX}_rsync/shop/public/orders/dt=2026-09-18/_MANIFEST.json", b"{}", {})
    store.put(f"{ROOT_PREFIX}_rsync/shop/public/orders/dt=2026-09-18/_SUCCESS", b"", {})
    res = _call("aws-s3", server, _args("aws-s3"))
    assert res["offsets"] == [{"topic": TOPIC, "partition": 0, "offset": 5}], res
    assert store.heads == [_key("20260918-101500000-1.parquet")], store.heads


def test_s3_max_objects_keeps_the_newest_objects(s3):
    """The objects written since the last Kafka commit are the newest ones, so a
    bounded scan must spend its HEADs on them first."""
    server, store = s3
    for i in range(5):
        _write(server, _key(f"20260918-10150000{i}-{i * 10}.parquet"), first=i * 10, last=i * 10 + 9)
    res = _call("aws-s3", server, _args("aws-s3", max_objects=2))
    assert res["offsets"] == [{"topic": TOPIC, "partition": 0, "offset": 49}], res
    assert res["truncated"] is True and len(store.heads) == 2, (res, store.heads)


def test_s3_an_exhausted_time_budget_is_truncated_not_an_error(s3, monkeypatch):
    server, store = s3
    _write(server, _key("20260918-101500000-1.parquet"), first=1, last=5)
    import time as _time

    real = _time.monotonic
    calls = {"n": 0}

    def _clock():
        # The deadline is taken on the first read; every later read is past it.
        calls["n"] += 1
        return real() + (0 if calls["n"] == 1 else 10_000)

    monkeypatch.setattr(_time, "monotonic", _clock)
    res = server.get_cdc_offsets(_args("aws-s3"))  # direct: the dispatcher may read the clock too
    assert res["success"] is True and res["truncated"] is True and res["offsets"] == [], res


def test_s3_an_object_deleted_after_listing_is_skipped(s3):
    server, store = s3
    _write(server, _key("20260918-101500000-1.parquet"), first=1, last=5)
    _write(server, _key("20260918-101600000-6.parquet"), first=6, last=8)
    store.head_errors[_key("20260918-101600000-6.parquet")] = "404"
    res = _call("aws-s3", server, _args("aws-s3"))
    assert res["offsets"] == [{"topic": TOPIC, "partition": 0, "offset": 5}], res


def test_s3_a_refused_head_is_an_empty_success(s3):
    """A HEAD the store refuses (403) could hide the true high-water mark; a partial
    max is still safe, but a caller that cannot read metadata should see no answer."""
    server, store = s3
    _write(server, _key("20260918-101500000-1.parquet"), first=1, last=5)
    store.head_errors[_key("20260918-101500000-1.parquet")] = "AccessDenied"
    res = _call("aws-s3", server, _args("aws-s3"))
    assert res == {"success": True, "offsets": []}, res


def test_s3_paging_and_head_through_the_real_botocore_model():
    """The fakes accept any kwargs; botocore's Stubber validates each request and
    response against the real S3 service model, so a wrong parameter name fails here."""
    boto3 = pytest.importorskip("boto3")
    stub_mod = pytest.importorskip("botocore.stub")
    client = boto3.client("s3", region_name="us-east-1",
                          aws_access_key_id="unit-test-access-key", aws_secret_access_key="unit-test-secret-key")
    stubber = stub_mod.Stubber(client)
    t0 = _dt.datetime(2026, 9, 18, 10, 0, tzinfo=_dt.timezone.utc)
    k1, k2 = _key("20260918-100000000-1.parquet"), _key("20260918-100100000-6.parquet")
    stubber.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": k1, "LastModified": t0}], "IsTruncated": True, "NextContinuationToken": "tok-1"},
        {"Bucket": BUCKET, "Prefix": ROOT_PREFIX, "MaxKeys": 1000},
    )
    stubber.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": k2, "LastModified": t0 + _dt.timedelta(minutes=1)}], "IsTruncated": False},
        {"Bucket": BUCKET, "Prefix": ROOT_PREFIX, "MaxKeys": 1000, "ContinuationToken": "tok-1"},
    )
    meta = {"rsync_pipeline_id": "p1", "rsync_topic": TOPIC, "rsync_partition": "0"}
    # Newest first: k2 is HEADed before k1.
    stubber.add_response("head_object", {"Metadata": {**meta, "rsync_last_offset": "8"}}, {"Bucket": BUCKET, "Key": k2})
    stubber.add_response("head_object", {"Metadata": {**meta, "rsync_last_offset": "5"}}, {"Bucket": BUCKET, "Key": k1})
    server = _load_server("aws-s3")
    server._get_s3_client = lambda config: client
    server.CDC_OFFSETS_HEAD_WORKERS = 1  # the Stubber's queue is ordered
    with stubber:
        res = server.get_cdc_offsets(_args("aws-s3"))
        stubber.assert_no_pending_responses()
    assert res["offsets"] == [{"topic": TOPIC, "partition": 0, "offset": 8}], res
    assert res["objects_scanned"] == 2 and res["truncated"] is False, res


def test_s3_import_data_sends_metadata_through_the_real_botocore_model(monkeypatch):
    boto3 = pytest.importorskip("boto3")
    stub_mod = pytest.importorskip("botocore.stub")
    client = boto3.client("s3", region_name="us-east-1",
                          aws_access_key_id="unit-test-access-key", aws_secret_access_key="unit-test-secret-key")
    stubber = stub_mod.Stubber(client)
    meta = {"rsync_pipeline_id": "p1", "rsync_topic": TOPIC, "rsync_partition": "0",
            "rsync_first_offset": "1", "rsync_last_offset": "5"}
    stubber.add_response("put_object", {}, {"Bucket": BUCKET, "Key": _key("a-1.jsonl"), "Body": stub_mod.ANY,
                                            "ContentType": stub_mod.ANY, "Metadata": meta})
    monkeypatch.setattr(boto3, "client", lambda *a, **k: client)
    server = _load_server("aws-s3")
    with stubber:
        res = server.import_data({"config": {"bucket": BUCKET}, "bucket": BUCKET, "key": _key("a-1.jsonl"),
                                  "format": "jsonl", "data": [{"a": 1}], "object_metadata": meta})
        stubber.assert_no_pending_responses()
    assert res["success"] is True, res


# --- template lockstep --------------------------------------------------------------

_BLOCK_START = "    # Object user-metadata keys the kafka-mcp-sink writes on every CDC object"


def test_template_get_cdc_offsets_is_the_aws_s3_implementation():
    """connector.py.j2 generates the get_cdc_offsets of every new cloud_storage
    connector. aws-s3 carries the same block and is covered by the scenarios above,
    so byte-equality carries that coverage to the template."""
    if not TEMPLATE.parent.is_dir():
        src = ROOT.parents[1] / "llm-service" / "tests" / "_flip_cut.py"
        spec = importlib.util.spec_from_file_location("_flip_cut_for_cdc_offsets", src)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.require_a_pre_cut_tree()
    text = TEMPLATE.read_text()
    start = text.index(_BLOCK_START)
    tpl = text[start:text.index("{% endif %}", start)].rstrip()
    s3_text = (_current_dir("aws-s3") / "connector.py").read_text()
    start = s3_text.index(_BLOCK_START)
    s3_block = s3_text[start:s3_text.index("\n    def delete_prefix", start)].rstrip()
    assert tpl == s3_block, "connector.py.j2 get_cdc_offsets drifted from aws-s3's"
    assert not re.search(r"\{\{|\{%|\{#", tpl), "get_cdc_offsets body must stay free of Jinja syntax"
