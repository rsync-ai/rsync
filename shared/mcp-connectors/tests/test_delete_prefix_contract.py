"""delete_prefix must fail closed: one contract for gcs, aws-s3, azure-blob and the template.

Reload (run_mode=reload) empties a table folder with ``delete_prefix`` before it writes
the new snapshot, so its result is the only evidence that the folder is empty. Before
this contract the three storage connectors could report ``success: True`` while:

  * stopping at a default ``max_objects`` of 100000 with objects left (``truncated``);
  * ignoring S3 ``delete_objects`` per-key ``Errors`` (the call succeeds, keys stay);
  * deleting a *sibling* folder: prefix ``a/b`` matched ``a/bc/...`` (no folder
    boundary), and the ``path_prefix`` guard accepted ``database/...`` for base ``data``;
  * stopping silently when S3 said ``IsTruncated`` but sent no continuation token.

Every scenario below comes from ``shared/delete_prefix_contract_golden.json`` and runs
against an in-memory fake of each SDK (no network, no emulator). The fakes list with
the real services' semantics -- prefix match, bounded page size, a continuation token
that is the last key returned -- so deleting while listing behaves as it does live.
"""
from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]  # shared/mcp-connectors
GOLDEN = json.loads((ROOT.parent / "delete_prefix_contract_golden.json").read_text())
STORAGE = ROOT / "public" / "storage"
TEMPLATE = ROOT.parents[1] / "llm-service" / "src" / "agents" / "tool_generator" / "templates" / "connector.py.j2"

CONNECTORS = {
    "gcs": "GcsMCPServer",
    "aws-s3": "AwsS3MCPServer",
    "azure-blob": "AzureBlobMCPServer",
}


def _current_dir(name: str) -> Path:
    latest = json.loads((STORAGE / name / "latest.json").read_text())
    return STORAGE / name / "versions" / latest["current_version"]


def _load_server(name: str):
    path = _current_dir(name) / "connector.py"
    spec = importlib.util.spec_from_file_location(f"_delete_prefix_contract_{name.replace('-', '_')}", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, CONNECTORS[name])()


# --- one in-memory object store, three SDK faces ------------------------------------


class _Refused(Exception):
    code = 403
    status_code = 403


class _Store:
    def __init__(self, keys, refused=(), list_fails_on_page=None):
        self.keys = set(keys)
        self.refused = set(refused)
        self.list_fails_on_page = list_fails_on_page
        self.list_calls = []  # (prefix, page_size)
        self.max_delete_batch = 0

    def page(self, prefix, page_size, after):
        assert page_size <= GOLDEN["max_keys_per_call"], f"unbounded page size {page_size}"
        self.list_calls.append((prefix, page_size))
        if self.list_fails_on_page is not None and len(self.list_calls) == self.list_fails_on_page:
            raise RuntimeError("listing failed: 503 backend unavailable")
        matching = sorted(k for k in self.keys if k.startswith(prefix) and (after is None or k > after))
        return matching[:page_size], len(matching) > page_size

    def delete(self, key):
        if key in self.refused:
            raise _Refused(f"403 AccessDenied on {key}")
        self.keys.discard(key)


class _FakeS3:
    def __init__(self, store):
        self.store = store

    def list_objects_v2(self, Bucket, Prefix, MaxKeys, ContinuationToken=None):
        keys, more = self.store.page(Prefix, MaxKeys, ContinuationToken)
        resp = {"Contents": [{"Key": k} for k in keys], "IsTruncated": more}
        if more:
            resp["NextContinuationToken"] = keys[-1]
        return resp

    def delete_objects(self, Bucket, Delete):
        objs = Delete["Objects"]
        assert len(objs) <= GOLDEN["max_keys_per_call"]
        self.store.max_delete_batch = max(self.store.max_delete_batch, len(objs))
        errors = []
        for o in objs:
            try:
                self.store.delete(o["Key"])
            except _Refused:
                errors.append({"Key": o["Key"], "Code": "AccessDenied", "Message": "Access Denied"})
        return {"Errors": errors} if errors else {}


class _Blob:
    def __init__(self, store, name):
        self._store, self.name = store, name

    def delete(self):
        self._store.delete(self.name)


class _Pages:
    def __init__(self, store, prefix, page_size, blob=True):
        self.store, self.prefix, self.page_size, self.blob = store, prefix, page_size, blob

    def __iter__(self):
        after = None
        while True:
            keys, more = self.store.page(self.prefix, self.page_size, after)
            if not keys:
                return
            yield iter([_Blob(self.store, k) for k in keys])
            if not more:
                return
            after = keys[-1]


class _FakeGCS:
    def __init__(self, store):
        self.store = store

    def list_blobs(self, bucket, prefix, page_size):
        class _It:
            pages = _Pages(self.store, prefix, page_size)
        return _It()


class _FakeAzureContainer:
    def __init__(self, store):
        self.store = store

    def list_blobs(self, name_starts_with, results_per_page):
        store = self.store

        class _It:
            def by_page(self):
                return iter(_Pages(store, name_starts_with, results_per_page))
        return _It()

    def delete_blob(self, name):
        self.store.delete(name)


class _FakeAzure:
    def __init__(self, store):
        self.store = store

    def get_container_client(self, name):
        return _FakeAzureContainer(self.store)


def _wire(name, server, store):
    if name == "aws-s3":
        server._get_s3_client = lambda config: _FakeS3(store)
    elif name == "gcs":
        server._get_gcs_client = lambda config: _FakeGCS(store)
    else:
        server._get_blob_service_client = lambda config: _FakeAzure(store)


@pytest.fixture(params=sorted(CONNECTORS))
def connector(request):
    return request.param, _load_server(request.param)


def _keys(prefix, n):
    return [f"{prefix}/dt=2026-09-17/LOAD{i:06d}.parquet" for i in range(n)]


def _assert_contract(res):
    for k in GOLDEN["required_keys"]:
        assert k in res, f"missing contract key {k!r}: {res}"
    assert res["success"] == (res["complete"] and res["failed"] == 0), res


# --- scenarios from the golden file ----------------------------------------------


@pytest.mark.parametrize("scenario", sorted(GOLDEN["scenarios"]))
def test_delete_prefix_scenarios_match_the_golden_contract(connector, scenario):
    name, server = connector
    sc = GOLDEN["scenarios"][scenario]
    folder = sc["requested_prefix"].rstrip("/")
    in_scope = _keys(folder, sc["objects_under_prefix"])
    siblings = sc.get("sibling_objects", [])
    refused = in_scope[1499:1499 + sc.get("refused_keys", 0)]
    store = _Store(in_scope + siblings, refused=refused, list_fails_on_page=sc.get("listing_fails_on_page"))
    _wire(name, server, store)

    params = {"config": {"bucket": "b"}, "bucket": "b", "prefix": sc["requested_prefix"]}
    for bound in ("max_objects", "max_pages"):
        if bound in sc:
            params[bound] = sc[bound]
    res = server.delete_prefix(params)

    _assert_contract(res)
    exp = sc["expect"]
    for k in ("success", "complete", "deleted", "failed"):
        assert res[k] == exp[k], f"{name}/{scenario}: {k}={res[k]!r}, want {exp[k]!r} ({res})"
    if not exp["success"]:
        assert res.get("error"), f"{name}/{scenario}: a failed delete must say why: {res}"
    if exp.get("siblings_intact"):
        assert all(s in store.keys for s in siblings), f"{name}: sibling objects deleted: {store.keys}"
    if exp["failed"]:
        assert [e["key"] for e in res["errors"]] == refused, res["errors"]
    # Folder boundary on the wire, and every listing call bounded.
    assert store.list_calls, "no listing call made"
    assert all(p == folder + "/" for p, _ in store.list_calls), store.list_calls


def test_many_pages_are_walked_to_the_end(connector):
    """The positive control for the bounds: 5001 objects is 6 pages, all of them read."""
    name, server = connector
    store = _Store(_keys("a/b", 5001))
    _wire(name, server, store)
    res = server.delete_prefix({"config": {"bucket": "b"}, "prefix": "a/b"})
    assert res["success"] and res["complete"] and res["deleted"] == 5001, res
    assert len(store.list_calls) >= 6, store.list_calls
    assert not store.keys
    if name == "aws-s3":
        assert store.max_delete_batch == 1000


@pytest.mark.parametrize("case", GOLDEN["prefix_normalization"], ids=lambda c: repr(c["requested"]))
def test_prefix_is_normalised_to_a_folder(connector, case):
    name, server = connector
    store = _Store(["a/b/x.parquet", "a/bc/keep.parquet"])
    _wire(name, server, store)
    res = server.delete_prefix({"config": {"bucket": "b"}, "prefix": case["requested"]})
    _assert_contract(res)
    assert res["prefix"] == case["listed"], res
    assert store.list_calls[0][0] == case["listed"]
    assert store.keys == {"a/bc/keep.parquet"}


@pytest.mark.parametrize("prefix", GOLDEN["refused_prefixes"], ids=repr)
def test_bucket_root_is_refused_without_listing(connector, prefix):
    name, server = connector
    store = _Store(["a/b/x.parquet"])
    _wire(name, server, store)
    res = server.delete_prefix({"config": {"bucket": "b"}, "prefix": prefix})
    _assert_contract(res)
    assert res["success"] is False and res["complete"] is False and res.get("error"), res
    assert store.list_calls == [] and store.keys == {"a/b/x.parquet"}


@pytest.mark.parametrize("case", GOLDEN["base_prefix_guard"], ids=lambda c: f"{c['base']}->{c['requested']}")
def test_base_prefix_guard_respects_the_folder_boundary(connector, case):
    name, server = connector
    store = _Store([])
    _wire(name, server, store)
    res = server.delete_prefix({"config": {"bucket": "b", "path_prefix": case["base"]}, "prefix": case["requested"]})
    _assert_contract(res)
    if case["allowed"]:
        assert res["success"] is True, res
    else:
        assert res["success"] is False and "Refusing" in res.get("error", ""), res
        assert store.list_calls == []


def test_s3_truncated_listing_without_a_token_is_not_complete():
    server = _load_server("aws-s3")
    store = _Store(_keys("a/b", 10))

    class _NoToken(_FakeS3):
        def list_objects_v2(self, **kw):
            resp = super().list_objects_v2(**kw)
            resp["IsTruncated"] = True
            resp.pop("NextContinuationToken", None)
            return resp

    server._get_s3_client = lambda config: _NoToken(store)
    res = server.delete_prefix({"config": {"bucket": "b"}, "prefix": "a/b"})
    _assert_contract(res)
    assert res["success"] is False and res["complete"] is False, res


@pytest.mark.parametrize("name", ["gcs", "azure-blob"])
def test_a_blob_already_gone_counts_as_deleted(name):
    server = _load_server(name)
    store = _Store(_keys("a/b", 3))

    class _Gone(Exception):
        code = 404
        status_code = 404

    real_delete = store.delete

    def _delete(key):
        real_delete(key)
        if key.endswith("000001.parquet"):
            raise _Gone("404")

    store.delete = _delete
    _wire(name, server, store)
    res = server.delete_prefix({"config": {"bucket": "b"}, "prefix": "a/b"})
    assert res["success"] is True and res["deleted"] == 3 and res["failed"] == 0, res


def test_azure_accepts_the_container_param_the_sink_sends():
    server = _load_server("azure-blob")
    store = _Store(_keys("a/b", 2))
    _wire("azure-blob", server, store)
    res = server.delete_prefix({"config": {}, "container": "c", "prefix": "a/b"})
    assert res["success"] is True and res["deleted"] == 2, res


@pytest.mark.parametrize("bound", ["max_pages", "max_objects"])
def test_a_malformed_bound_still_returns_the_contract(connector, bound):
    name, server = connector
    store = _Store(_keys("a/b", 2))
    _wire(name, server, store)
    res = server.delete_prefix({"config": {"bucket": "b"}, "prefix": "a/b", bound: "lots"})
    _assert_contract(res)
    assert res["success"] is False and res["complete"] is False and res.get("error"), res
    assert store.list_calls == [] and len(store.keys) == 2


# --- the real boto3 S3 client (request/response shapes validated by botocore) ------


def _stubbed_s3():
    boto3 = pytest.importorskip("boto3")
    stub_mod = pytest.importorskip("botocore.stub")
    client = boto3.client(
        "s3", region_name="us-east-1",
        aws_access_key_id="unit-test-access-key", aws_secret_access_key="unit-test-secret-key",
    )
    return client, stub_mod.Stubber(client)


def test_s3_paging_and_per_key_errors_through_the_real_botocore_model():
    """The fakes above accept any kwargs; botocore's Stubber validates each request and
    response against the real S3 service model, so a wrong parameter name fails here."""
    server = _load_server("aws-s3")
    client, stubber = _stubbed_s3()
    page1 = ["a/b/x1.parquet", "a/b/x2.parquet"]
    page2 = ["a/b/x3.parquet"]
    stubber.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": k} for k in page1], "IsTruncated": True, "NextContinuationToken": "tok-1"},
        {"Bucket": "b", "Prefix": "a/b/", "MaxKeys": 1000},
    )
    stubber.add_response(
        "delete_objects", {"Deleted": [{"Key": k} for k in page1]},
        {"Bucket": "b", "Delete": {"Objects": [{"Key": k} for k in page1], "Quiet": True}},
    )
    stubber.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": k} for k in page2], "IsTruncated": False},
        {"Bucket": "b", "Prefix": "a/b/", "MaxKeys": 1000, "ContinuationToken": "tok-1"},
    )
    stubber.add_response(
        "delete_objects",
        {"Errors": [{"Key": "a/b/x3.parquet", "Code": "AccessDenied", "Message": "Access Denied"}]},
        {"Bucket": "b", "Delete": {"Objects": [{"Key": "a/b/x3.parquet"}], "Quiet": True}},
    )
    server._get_s3_client = lambda config: client
    with stubber:
        res = server.delete_prefix({"config": {"bucket": "b"}, "prefix": "a/b"})
        stubber.assert_no_pending_responses()
    _assert_contract(res)
    assert res["complete"] is True and res["deleted"] == 2 and res["failed"] == 1, res
    assert res["success"] is False and res["errors"][0]["key"] == "a/b/x3.parquet", res


def test_s3_listing_client_error_on_page_two_through_the_real_botocore_model():
    server = _load_server("aws-s3")
    client, stubber = _stubbed_s3()
    stubber.add_response(
        "list_objects_v2",
        {"Contents": [{"Key": "a/b/x1.parquet"}], "IsTruncated": True, "NextContinuationToken": "tok-1"},
        {"Bucket": "b", "Prefix": "a/b/", "MaxKeys": 1000},
    )
    stubber.add_response(
        "delete_objects", {},
        {"Bucket": "b", "Delete": {"Objects": [{"Key": "a/b/x1.parquet"}], "Quiet": True}},
    )
    stubber.add_client_error("list_objects_v2", service_error_code="SlowDown", http_status_code=503)
    server._get_s3_client = lambda config: client
    with stubber:
        res = server.delete_prefix({"config": {"bucket": "b"}, "prefix": "a/b"})
    _assert_contract(res)
    assert res["success"] is False and res["complete"] is False and res["deleted"] == 1, res
    assert "SlowDown" in res.get("error", ""), res


# --- template lockstep --------------------------------------------------------------


def _method_body(text: str, end: str) -> str:
    start = text.index("    def delete_prefix(self, params: Dict)")
    return text[start:text.index(end, start)].rstrip()


def test_template_delete_prefix_is_the_aws_s3_implementation():
    """connector.py.j2 generates the delete_prefix of every new cloud_storage connector.

    aws-s3 was generated from it and is covered by the scenarios above, so byte-equality
    carries that coverage to the template: a fix in only one of them fails here.
    """
    if not TEMPLATE.parent.is_dir():
        import importlib.util as _u

        src = ROOT.parents[1] / "llm-service" / "tests" / "_flip_cut.py"
        spec = _u.spec_from_file_location("_flip_cut_for_delete_prefix", src)
        mod = _u.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod.require_a_pre_cut_tree()
    tpl = _method_body(TEMPLATE.read_text(), "{% endif %}")
    s3 = _method_body((_current_dir("aws-s3") / "connector.py").read_text(), "\n    def _object_exists")
    assert tpl == s3, "connector.py.j2 delete_prefix drifted from aws-s3's"
    assert not re.search(r"\{\{|\{%|\{#", tpl), "delete_prefix body must stay free of Jinja syntax"
