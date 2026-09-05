"""
Provider-agnostic contract test for ObjectStorageSourceMixin.

Uses an in-memory fake provider (no boto3 / google-cloud-storage / azure SDK,
no pyarrow) so it runs anywhere with stdlib only and exercises the exact source
pipeline that aws-s3 / gcs / azure-blob inherit: glob + table mapping
(single/per_table/dynamic), cursor-paged export (no re-emit, terminal page),
incremental `since`, provenance columns, and the INV-1 endpoint guard.

Parquet parsing (pyarrow) is intentionally out of scope here and is covered by
the live source E2E. Run standalone: `python3 test_object_storage_source.py`.
"""
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_d = _HERE
while _d != os.path.dirname(_d):
    if os.path.isdir(os.path.join(_d, "rsync_protocol")):
        sys.path.insert(0, _d)
        break
    _d = os.path.dirname(_d)

from rsync_protocol.object_storage_source import ObjectStorageSourceMixin  # noqa: E402


class _FakeStorage(ObjectStorageSourceMixin):
    """A storage connector backed by an in-memory {key: (bytes, last_modified)}."""

    connector_type = "fake-storage"
    max_batch_size = 10000

    def __init__(self, store):
        self._store = dict(store)

    def _get_config(self, params):
        return (params.get("config", params) if params else {}) or {}

    def list_objects(self, *, config, bucket, prefix="", max_keys=1000):
        out = []
        for k in sorted(self._store):
            if prefix and not k.startswith(prefix):
                continue
            body, lm = self._store[k]
            out.append({"key": k, "size": len(body), "last_modified": lm})
        return out[:max_keys]

    def download_file(self, *, config, bucket, key):
        return self._store[key][0]


def _csv(rows_header, rows):
    lines = [",".join(rows_header)]
    lines += [",".join(str(c) for c in r) for r in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _jsonl(rows):
    return ("\n".join(json.dumps(r) for r in rows) + "\n").encode("utf-8")


# ---- seed: data/<table>/... across json, jsonl, csv ----------------------------
STORE = {
    "data/users/u1.json": (json.dumps([{"id": 1, "name": "ada"},
                                       {"id": 2, "name": "linus"}]).encode(), "2026-06-20T10:00:00"),
    "data/users/u2.jsonl": (_jsonl([{"id": 3, "name": "grace"}]), "2026-06-21T10:00:00"),
    "data/events/e1.csv": (_csv(["ev", "n"], [["click", 1], ["view", 2], ["scroll", 3]]),
                           "2026-06-20T09:00:00"),
    "data/_SUCCESS": (b"", "2026-06-20T09:00:00"),  # zero-byte marker → must be skipped
}

BASE_CFG = {"bucket": "b", "path_prefix": "data/", "file_format": "infer",
            "file_mapping": "dynamic", "dynamic_table_regex": r"data/(?<table>[^/]+)/"}


def test_discover_dynamic():
    conn = _FakeStorage(STORE)
    res = conn.discover_schema({"config": BASE_CFG})
    assert res["success"], res
    names = sorted(t["name"] for t in res["tables"])
    assert names == ["events", "users"], names
    users = next(t for t in res["tables"] if t["name"] == "users")
    colnames = [c["name"] for c in users["columns"]]
    assert "id" in colnames and "name" in colnames, colnames
    # provenance columns appended for the dest DDL
    for mc in ("_rsync_source_file", "_rsync_source_file_modified", "_rsync_source_row"):
        assert mc in colnames, (mc, colnames)
    assert users["file_count"] == 2, users


def test_export_cursor_pagination_no_reemit():
    conn = _FakeStorage(STORE)
    seen = []
    cursor = None
    pages = 0
    while True:
        p = {"config": BASE_CFG, "table": "users", "limit": 2}
        if cursor:
            p["cursor"] = cursor
        res = conn.export(p)
        assert res["success"], res
        recs = res["records"]
        seen.extend(recs)
        pages += 1
        cursor = res.get("next_cursor")
        if not recs or cursor is None:
            break
        assert pages < 10, "pagination did not terminate"
    ids = sorted(r["id"] for r in seen)
    assert ids == [1, 2, 3], ids  # 3 user rows total (u1.json=2, u2.jsonl=1)
    assert len(seen) == 3, "re-emit detected"  # no duplicates across pages
    assert pages >= 2, "limit=2 over 3 rows should take >1 page"
    # provenance present + correct row index within file
    assert all("_rsync_source_file" in r and "_rsync_source_row" in r for r in seen)


def test_export_single_and_per_table():
    conn = _FakeStorage(STORE)
    # single → everything under prefix into one default-named table
    res = conn.export({"config": {**BASE_CFG, "file_mapping": "single",
                                  "source_table_name": "all"}, "table": "all"})
    assert res["success"] and len(res["records"]) == 6, res["stats"]  # 3 users + 3 events
    # per_table → explicit pattern map
    cfg = {"bucket": "b", "path_prefix": "data/", "file_format": "infer",
           "file_mapping": "per_table",
           "table_patterns": {"ev": ["*events*"], "usr": ["*users*"]}}
    res2 = conn.export({"config": cfg, "table": "ev"})
    assert res2["success"] and len(res2["records"]) == 3, res2["stats"]


def test_incremental_since():
    conn = _FakeStorage(STORE)
    # Only u2.jsonl (2026-06-21) is newer than the watermark → 1 user row.
    res = conn.export({"config": BASE_CFG, "table": "users",
                       "since": "2026-06-20T23:59:59"})
    assert res["success"], res
    ids = sorted(r["id"] for r in res["records"])
    assert ids == [3], ids
    assert res["stats"]["watermark"]["last_modified"] == "2026-06-21T10:00:00", res["stats"]


def test_inv1_guard_blocks_internal_minio():
    conn = _FakeStorage(STORE)
    res = conn.export({"config": {**BASE_CFG, "endpoint_url": "http://rsync-ai-minio:9000"},
                       "table": "users"})
    assert res["success"] is False, res
    assert "internal MinIO" in (res.get("error") or ""), res
    # discover_schema must also refuse
    res2 = conn.discover_schema({"config": {**BASE_CFG, "endpoint": "minio:9000"}})
    assert res2["success"] is False and "internal MinIO" in (res2.get("error") or ""), res2


def test_external_endpoint_passes():
    conn = _FakeStorage(STORE)
    res = conn.export({"config": {**BASE_CFG, "endpoint_url": "https://storage.googleapis.com"},
                       "table": "users"})
    assert res["success"], res  # GCS S3-interop endpoint is external → allowed


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"PASS {fn.__name__}")
    print(f"\nAll {len(fns)} ObjectStorageSourceMixin contract checks PASS")
