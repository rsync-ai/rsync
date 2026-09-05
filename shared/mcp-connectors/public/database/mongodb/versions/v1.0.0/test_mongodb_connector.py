#!/usr/bin/env python3
"""Offline unit tests for the MongoDB MCP connector.

MongoDB is a document store driven by ``pymongo`` (NOT DB-API). The connector
touches the driver through one seam: ``_get_client(config)`` returns a
``MongoClient``; everything flows through ``client[db][collection]``. Tests swap
that seam for :class:`_mongo_fakes.FakeMongoClient`, so there is no replica set,
network, or server.

MongoDB-specific facts pinned here:

  * It is a SOURCE and a DESTINATION: ``supports_destination is True``. Writes go
    through ``import_data`` (insert), ``upsert_data`` (idempotent replace keyed on
    _id / key_fields — the CDC insert/update path), and ``delete_data`` (delete by
    key — the CDC delete path). The connector never reads an op field; the sink
    picks the tool.
  * It does NOT capture CDC itself. Change streams are provisioned out-of-process
    by Debezium; the connector implements no ``.watch()`` / change-stream reader.
    Its only CDC-related surface is REPLICA-SET readiness detection in
    ``test_connection`` (change streams require a replica set) — that gate is the
    "CDC path" exercised below.
  * It has NO DDL (``supports_ddl is False``) — collections auto-create on first
    write (``auto_create_destination_tables is True``).
  * Batch export pages by ascending ``_id`` keyset only (stable under concurrent
    writes) — no skip/offset, no arbitrary cursor column. A 24-hex cursor is
    resumed as an ``ObjectId``; any other value is compared raw.
  * BSON is coerced JSON-safe on the way out (ObjectId→str, datetime→ISO,
    Decimal128→str, bytes→base64); a 24-hex string ``_id`` is coerced back to an
    ObjectId on the way IN so a Mongo→Mongo copy keeps native _id fidelity.

Run standalone (no pytest needed):  python3 test_mongodb_connector.py
"""
import os
import sys
from datetime import datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)

import connector as mg  # noqa: E402  (the module under test)
import _mongo_fakes  # noqa: E402

from bson import ObjectId  # noqa: E402  (ships with pymongo, installed in the image)

CFG = {"config": {"host": "localhost", "port": 27017, "database": "appdb",
                  "user": "u", "password": "p"}}


# ============================== identity ====================================

def test_connector_identity_source_and_destination():
    s = mg.MongodbMCPServer()
    assert s.connector_type == "mongodb", s.connector_type
    assert s.connector_category == "document_db", s.connector_category
    assert s.supports_source is True, s.supports_source
    assert s.supports_destination is True, "MongoDB connector is a source AND destination"
    assert s.supports_cdc is True, "CDC advertised (provisioned via Debezium)"
    # No DDL, but collections auto-create on first write.
    assert s.supports_ddl is False, s.supports_ddl
    assert s.auto_create_destination_tables is True, s.auto_create_destination_tables


def test_destination_write_methods_present_no_cdc_capture():
    """The connector must expose the three destination write ops but must NOT grow
    an in-process change-stream reader (CDC capture belongs to Debezium / the
    sink, never this connector)."""
    d = mg.MongodbMCPServer.__dict__
    for required in ("import_data", "upsert_data", "delete_data"):
        assert required in d, f"MongoDB connector must implement {required}"
    for forbidden in ("watch", "stream_changes", "read_cdc", "capture_changes"):
        assert forbidden not in d, f"MongoDB connector must not implement {forbidden}"


def test_get_capabilities_accepts_params_and_advertises_pymongo():
    s = mg.MongodbMCPServer()
    caps = s.get_capabilities({})  # must tolerate the positional arg
    assert caps["success"] is True, caps
    assert caps["connector_type"] == "mongodb", caps
    assert caps["driver_pattern"] == "pymongo", caps
    assert caps["supports_source"] is True and caps["supports_destination"] is True, caps
    ops = {o["name"] for o in caps.get("operations", [])}
    assert {"test_connection", "discover_schema", "export", "get_primary_key"} <= ops, ops
    # destination write ops advertised so the sink can dispatch CDC events
    assert {"import_data", "upsert_data", "delete_data"} <= ops, ops
    # no relational load-strategy leaked into a document-DB connector
    assert "load_strategy" not in caps, caps
    assert caps["capabilities"]["max_batch_size"] == 10000, caps


def test_validate_config_requires_host_or_uri_and_database():
    s = mg.MongodbMCPServer()
    bad = s.validate_config({"config": {}})
    assert bad["valid"] is False, bad
    assert any("host" in e for e in bad["errors"]), bad
    assert any("database" in e for e in bad["errors"]), bad
    # a connection_string satisfies the host requirement
    ok_uri = s.validate_config({"config": {"connection_string": "mongodb://h/", "database": "d"}})
    assert ok_uri["valid"] is True, ok_uri
    assert s.validate_config(CFG)["valid"] is True, "host+database is valid"


def test_get_primary_key_is_always_id():
    s = mg.MongodbMCPServer()
    out = s.get_primary_key({"collection": "anything"})
    assert out == {"success": True, "primary_key": "_id", "primary_keys": ["_id"]}, out


# ==================== CONNECT + CDC (replica-set) readiness =================

def test_test_connection_replica_set_ok_no_warning():
    s, client = _mongo_fakes.make_connector(mg, set_name="rs0")
    out = s.test_connection(CFG)
    assert out["success"] is True, out
    assert out["is_replica_set"] is True and out["replica_set"] == "rs0", out
    assert "warning" not in out, out
    assert client.closed is True, "client must be closed"
    assert "ping" in client.admin.commands, client.admin.commands


def test_test_connection_standalone_warns_cdc_needs_replica_set():
    """A standalone mongod cannot be a CDC source. test_connection must still
    succeed (batch is fine) but surface the change-streams-need-a-replica-set
    warning — this is the connector's CDC-readiness gate."""
    s, _ = _mongo_fakes.make_connector(mg, set_name=None)
    out = s.test_connection(CFG)
    assert out["success"] is True, out
    assert out["is_replica_set"] is False, out
    assert "warning" in out and "replica set" in out["warning"].lower(), out
    assert "change stream" in out["warning"].lower(), out


def test_test_connection_ping_failure_reports_error():
    s, _ = _mongo_fakes.make_connector(mg, ping_error=Exception("no route to host"))
    out = s.test_connection(CFG)
    assert out["success"] is False and "no route to host" in out["error"], out


# ============================ DISCOVER ======================================

def test_discover_schema_samples_fields_and_counts():
    docs = [{"_id": ObjectId(), "name": "a", "age": 30, "active": True,
             "meta": {"k": 1}, "created": datetime(2026, 1, 1)}]
    dbs = {"appdb": {
        "users": list(docs),
        "orders": [{"_id": ObjectId(), "total": 9.5}],
        "system.profile": [{"_id": ObjectId(), "x": 1}],  # must be filtered out
    }}
    s, _ = _mongo_fakes.make_connector(mg, dbs=dbs, version="7.0.5")
    out = s.discover_schema(CFG)
    assert out["overall_status"] == "success", out
    assert out["database_version"] == "7.0.5", out
    names = [t["name"] for t in out["tables"]]
    assert names == ["orders", "users"], names            # system.* dropped, sorted
    assert out["total_tables_available"] == 2, out
    users = next(t for t in out["tables"] if t["name"] == "users")
    assert users["schema"] == "appdb", users
    assert users["primary_keys"] == ["_id"] and users["primary_key"] == ["_id"], users
    assert users["row_count"] == 1 and users["is_exact_count"] is True, users
    cols = {c["name"]: c for c in users["columns"]}
    assert list(cols)[0] == "_id", "._id must be first"
    assert cols["_id"]["nullable"] is False and cols["name"]["nullable"] is True, cols
    assert cols["age"]["type"] == "integer", cols
    assert cols["active"]["type"] == "boolean", cols       # bool checked before int
    assert cols["meta"]["type"] == "object", cols
    assert cols["created"]["type"] == "timestamp", cols


# ============================ EXPORT: _id keyset paging =====================

def test_export_first_page_keyset_by_id_serializes_bson():
    ids = [ObjectId() for _ in range(3)]
    docs = [{"_id": ids[i], "v": i} for i in range(3)]
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"users": docs}})
    out = s.export({**CFG, "table": "users", "limit": 2})
    assert out["success"] is True, out
    assert out["row_count"] == 2 and len(out["data"]) == 2, out
    assert out["has_more"] is True, out                    # len(2) >= limit(2)
    assert out["paging_mode"] == "keyset" and out["cursor_column"] == "_id", out
    assert out["next_cursor"] == str(ids[1]), out          # str of last _id
    # BSON coerced JSON-safe: _id came back as a string, not an ObjectId.
    assert out["data"][0]["_id"] == str(ids[0]), out["data"][0]
    assert all(isinstance(r["_id"], str) for r in out["data"]), out["data"]
    # first page issues an unfiltered find (empty query).
    q = client["appdb"]["users"].find_calls[-1]["query"]
    assert q == {}, q


def test_export_continuation_24hex_cursor_uses_objectid_gt():
    ids = [ObjectId() for _ in range(3)]
    docs = [{"_id": ids[i], "v": i} for i in range(3)]
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"users": docs}})
    out = s.export({**CFG, "table": "users", "cursor": str(ids[0]), "limit": 10})
    assert out["success"] is True, out
    q = client["appdb"]["users"].find_calls[-1]["query"]
    assert isinstance(q.get("_id", {}).get("$gt"), ObjectId), q   # resumed as ObjectId
    assert q["_id"]["$gt"] == ids[0], q
    # only _ids strictly greater than the cursor come back
    assert [r["v"] for r in out["data"]] == [1, 2], out["data"]
    assert out["has_more"] is False, out                   # 2 < limit(10) → final page
    assert "next_cursor" not in out, out


def test_export_non_hex_cursor_compared_raw():
    """A non-24-hex cursor (e.g. an integer _id) is compared raw, not coerced to
    ObjectId."""
    docs = [{"_id": i, "v": i} for i in range(1, 4)]
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"nums": docs}})
    out = s.export({**CFG, "table": "nums", "cursor": 1, "limit": 10})
    assert out["success"] is True, out
    q = client["appdb"]["nums"].find_calls[-1]["query"]
    assert q == {"_id": {"$gt": 1}}, q                      # raw value, no ObjectId
    assert [r["v"] for r in out["data"]] == [2, 3], out["data"]


def test_export_strips_db_qualifier_from_collection():
    docs = [{"_id": ObjectId(), "v": 1}]
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"users": docs}})
    out = s.export({**CFG, "table": "appdb.users", "limit": 5})
    assert out["success"] is True, out
    assert out["row_count"] == 1, out
    assert client["appdb"]["users"].find_calls, "resolved to the bare collection"


def test_export_missing_collection_errors():
    s, _ = _mongo_fakes.make_connector(mg, dbs={"appdb": {}})
    out = s.export({**CFG})
    assert out["success"] is False, out
    assert "collection" in out["error"].lower() or "table" in out["error"].lower(), out


# ==================== DESTINATION: import / upsert / delete =================

def test_import_data_inserts_documents():
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"users": []}})
    out = s.import_data({**CFG, "table": "users",
                         "data": [{"name": "a"}, {"name": "b"}]})
    assert out["success"] is True, out
    assert out["rows_inserted"] == 2, out
    landed = client["appdb"]["users"].docs()
    assert [d["name"] for d in landed] == ["a", "b"], landed
    # Mongo auto-assigned an _id for each inserted doc.
    assert all("_id" in d for d in landed), landed


def test_import_data_replace_mode_truncates_first():
    seed = [{"_id": 1, "v": "old"}]
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"t": list(seed)}})
    out = s.import_data({**CFG, "table": "t", "mode": "replace",
                         "data": [{"_id": 2, "v": "new"}]})
    assert out["success"] is True and out["rows_inserted"] == 1, out
    coll = client["appdb"]["t"]
    assert coll.delete_calls and coll.delete_calls[0]["filter"] == {}, coll.delete_calls
    assert [d["_id"] for d in coll.docs()] == [2], coll.docs()


def test_import_data_tolerates_duplicate_key_on_redelivery():
    """A replayed CDC insert (same _id already present) must not fail the batch —
    the surviving inserts are counted (nInserted)."""
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"t": [{"_id": 1}]}})
    out = s.import_data({**CFG, "table": "t",
                         "data": [{"_id": 1, "v": "dup"}, {"_id": 2, "v": "fresh"}]})
    assert out["success"] is True, out
    assert out["rows_inserted"] == 1, out          # only _id=2 landed
    assert {d["_id"] for d in client["appdb"]["t"].docs()} == {1, 2}, client["appdb"]["t"].docs()


def test_import_data_missing_collection_errors():
    s, _ = _mongo_fakes.make_connector(mg, dbs={"appdb": {}})
    out = s.import_data({**CFG, "data": [{"x": 1}]})
    assert out["success"] is False, out
    assert "collection" in out["error"].lower() or "table" in out["error"].lower(), out


def test_upsert_data_by_id_is_idempotent():
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"users": []}})
    row = {"_id": 7, "name": "a", "v": 1}
    first = s.upsert_data({**CFG, "table": "users", "data": [row]})
    assert first["success"] is True and first["rows_upserted"] == 1, first
    # re-apply an updated version of the SAME _id → matched, still one doc.
    second = s.upsert_data({**CFG, "table": "users", "data": [{"_id": 7, "name": "a", "v": 2}]})
    assert second["success"] is True and second["rows_upserted"] == 1, second
    docs = client["appdb"]["users"].docs()
    assert len(docs) == 1 and docs[0]["v"] == 2, docs


def test_upsert_data_relational_key_fields():
    """A relational source's own PK column (id) is honored via key_fields, so the
    filter keys on id (Mongo auto-assigns _id)."""
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"orders": []}})
    out = s.upsert_data({**CFG, "table": "orders", "key_fields": ["id"],
                         "data": [{"id": 100, "total": 9.5}]})
    assert out["success"] is True and out["rows_upserted"] == 1, out
    op = client["appdb"]["orders"].bulk_ops[-1]["ops"][0]
    assert op._filter == {"id": 100}, op._filter          # keyed on the source PK
    assert op._upsert is True, op._upsert


def test_upsert_coerces_24hex_id_to_objectid():
    """A 24-hex string _id (from a Mongo source's JSON-safe export) is stored as an
    ObjectId so a Mongo→Mongo copy keeps native _id fidelity."""
    oid = ObjectId()
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"c": []}})
    out = s.upsert_data({**CFG, "table": "c", "data": [{"_id": str(oid), "v": 1}]})
    assert out["success"] is True, out
    op = client["appdb"]["c"].bulk_ops[-1]["ops"][0]
    assert isinstance(op._filter["_id"], ObjectId) and op._filter["_id"] == oid, op._filter
    stored = client["appdb"]["c"].docs()[0]
    assert isinstance(stored["_id"], ObjectId) and stored["_id"] == oid, stored


def test_upsert_data_skips_records_missing_key():
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"c": []}})
    out = s.upsert_data({**CFG, "table": "c", "key_fields": ["id"],
                         "data": [{"id": 1, "v": "ok"}, {"no_key": True}]})
    assert out["success"] is True, out
    assert out["rows_upserted"] == 1 and out.get("skipped") == 1, out


def test_delete_data_by_id_uses_in_filter():
    docs = [{"_id": 1}, {"_id": 2}, {"_id": 3}]
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"t": list(docs)}})
    out = s.delete_data({**CFG, "table": "t", "data": [{"_id": 1}, {"_id": 3}]})
    assert out["success"] is True and out["rows_deleted"] == 2, out
    flt = client["appdb"]["t"].delete_calls[-1]["filter"]
    assert flt == {"_id": {"$in": [1, 3]}}, flt
    assert [d["_id"] for d in client["appdb"]["t"].docs()] == [2], client["appdb"]["t"].docs()


def test_delete_data_unwraps_before_key():
    """A Debezium-style delete event carries the key under `before`; the connector
    must unwrap it and delete by the configured key field."""
    docs = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"orders": list(docs)}})
    out = s.delete_data({**CFG, "table": "orders", "key_fields": ["id"],
                         "data": [{"before": {"id": 2}}]})
    assert out["success"] is True and out["rows_deleted"] == 1, out
    assert [d["id"] for d in client["appdb"]["orders"].docs()] == [1], client["appdb"]["orders"].docs()


def test_delete_data_no_keys_is_noop():
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"t": [{"_id": 1}]}})
    out = s.delete_data({**CFG, "table": "t", "data": []})
    assert out["success"] is True and out["rows_deleted"] == 0, out
    assert len(client["appdb"]["t"].docs()) == 1, "no data → nothing deleted"


def test_drop_table_drops_collection_for_reload():
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"t": [{"_id": 1}], "keep": [{"_id": 9}]}})
    out = s.drop_table({**CFG, "table": "t"})
    assert out["success"] is True and out["dropped"] is True, out
    assert "t" not in client["appdb"].list_collection_names(), client["appdb"].list_collection_names()
    assert "keep" in client["appdb"].list_collection_names(), "other collections untouched"


def test_drop_table_missing_collection_is_noop_success():
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {}})
    out = s.drop_table({**CFG, "table": "ghost"})
    assert out["success"] is True and out["dropped"] is False, out


def test_import_data_claim_check_reads_from_staging(monkeypatch=None):
    """import_data must honor the Claim-Check pattern (data_ref → staging) inherited
    from base_connector: when data arrives by reference it is fetched before write."""
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"t": []}})
    s.read_from_staging = lambda data_ref, cfg: {"success": True,
                                                 "data": [{"name": "staged"}]}
    out = s.import_data({**CFG, "table": "t", "data_ref": "s3://b/k.json"})
    assert out["success"] is True and out["rows_inserted"] == 1, out
    assert client["appdb"]["t"].docs()[0]["name"] == "staged", client["appdb"]["t"].docs()


# ============ write-key indexing (perf: avoid O(n^2) collection scans) ======
# A keyed upsert/delete on a non-_id field would otherwise scan the whole
# collection per op (measured ~80x slower at 50k docs); the connector ensures an
# index on the write key once per (db, collection, key) in this long-lived process.

def test_upsert_ensures_index_on_relational_key():
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"orders": []}})
    s.upsert_data({**CFG, "table": "orders", "key_fields": ["id"],
                   "data": [{"id": 1, "v": 1}]})
    idx = client["appdb"]["orders"].index_calls
    assert idx == [[("id", 1)]], idx                     # index built on the write key


def test_upsert_default_id_key_creates_no_index():
    """_id is already uniquely indexed by Mongo — never create a redundant index."""
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"c": []}})
    s.upsert_data({**CFG, "table": "c", "data": [{"_id": 1, "v": 1}]})
    assert client["appdb"]["c"].index_calls == [], client["appdb"]["c"].index_calls


def test_ensure_index_is_cached_across_flushes():
    """Many sink flushes to the same collection cost at most one create_index."""
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"orders": []}})
    for i in range(5):
        s.upsert_data({**CFG, "table": "orders", "key_fields": ["id"],
                       "data": [{"id": i, "v": i}]})
    assert client["appdb"]["orders"].index_calls == [[("id", 1)]], \
        client["appdb"]["orders"].index_calls          # exactly one, not five


def test_delete_ensures_index_on_relational_key():
    docs = [{"id": 1}, {"id": 2}]
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"orders": list(docs)}})
    s.delete_data({**CFG, "table": "orders", "key_fields": ["id"],
                   "data": [{"before": {"id": 1}}]})
    assert client["appdb"]["orders"].index_calls == [[("id", 1)]], \
        client["appdb"]["orders"].index_calls


def test_ensure_index_composite_key_is_compound():
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"ck": []}})
    s.upsert_data({**CFG, "table": "ck", "key_fields": ["region", "sku"],
                   "data": [{"region": "us", "sku": 5, "q": 1}]})
    assert client["appdb"]["ck"].index_calls == [[("region", 1), ("sku", 1)]], \
        client["appdb"]["ck"].index_calls


def test_index_creation_failure_never_fails_the_batch():
    """A missing index only slows writes; if create_index raises, the write proceeds."""
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"orders": []}})
    coll = client["appdb"]["orders"]
    def boom(*a, **k):
        raise Exception("index build not permitted")
    coll.create_index = boom
    out = s.upsert_data({**CFG, "table": "orders", "key_fields": ["id"],
                         "data": [{"id": 1, "v": 1}]})
    assert out["success"] is True and out["rows_upserted"] == 1, out


# ===================== pure serializer / type helpers ======================

def test_json_safe_coerces_bson_and_nested():
    import base64
    from bson import Decimal128
    oid = ObjectId()
    val = {
        "_id": oid,
        "when": datetime(2026, 7, 5, 12, 0, 0),
        "amount": Decimal128("10.50"),
        "blob": b"\x00\x01",
        "nested": [ObjectId(), {"inner": oid}],
        "n": 5, "flag": True, "txt": "x", "none": None,
    }
    out = mg._json_safe(val)
    assert out["_id"] == str(oid), out
    assert out["when"] == "2026-07-05T12:00:00", out
    assert out["amount"] == "10.50", out
    assert out["blob"] == base64.b64encode(b"\x00\x01").decode("ascii"), out
    assert out["nested"][0] == str(val["nested"][0]), out
    assert out["nested"][1]["inner"] == str(oid), out
    assert out["n"] == 5 and out["flag"] is True and out["txt"] == "x" and out["none"] is None, out


def test_infer_type_bool_before_int():
    assert mg._infer_type(True) == "boolean"       # bool must win over int
    assert mg._infer_type(3) == "integer"
    assert mg._infer_type(1.5) == "double"
    assert mg._infer_type({"a": 1}) == "object"
    assert mg._infer_type([1, 2]) == "array"
    assert mg._infer_type(datetime(2026, 1, 1)) == "timestamp"
    assert mg._infer_type(ObjectId()) == "string"
    assert mg._infer_type("s") == "string"


# ============================ URI builder (pure) ============================

def test_build_uri_explicit_connection_string_wins():
    s = mg.MongodbMCPServer()
    uri = s._build_uri({"connection_string": "mongodb+srv://user:pw@cluster/db",
                        "host": "ignored"})
    assert uri == "mongodb+srv://user:pw@cluster/db", uri


def test_build_uri_assembles_with_creds_auth_and_replicaset():
    s = mg.MongodbMCPServer()
    uri = s._build_uri({"host": "db1", "port": 27018, "user": "a@b", "password": "p@ss",
                        "replica_set": "rs0"})
    assert uri.startswith("mongodb://a%40b:p%40ss@db1:27018/"), uri  # creds url-encoded
    assert "authSource=admin" in uri, uri
    assert "replicaSet=rs0" in uri, uri


def test_build_uri_enables_tls_for_atlas_host():
    s = mg.MongodbMCPServer()
    uri = s._build_uri({"host": "cluster0.ab12.mongodb.net", "user": "u", "password": "p"})
    assert "tls=true" in uri, uri


# ============================== env backfill ================================
#
# The image bakes five operator-override slots (Dockerfile: ENV MONGODB_HOST=""
# … MONGODB_PASSWORD=""). Until _get_config existed nothing read them, so setting
# one did nothing and said nothing (KI-CONNECTOR-DEAD-ENV-OVERRIDE-SLOTS). These
# four tests pin the semantics the fix must keep: BACKFILL only, never override;
# a baked-empty variable is a no-op; and the value survives all the way to the
# driver, not just out of _get_config in isolation.
#
# No pytest `monkeypatch` fixture here — the standalone _run() below calls every
# test with zero arguments, so env is saved/restored by hand in try/finally.

_ENV_SLOTS = ("MONGODB_HOST", "MONGODB_PORT", "MONGODB_DATABASE",
              "MONGODB_USER", "MONGODB_PASSWORD")


def _set_env(**values):
    """Set MONGODB_* slots, returning the prior state for _restore_env."""
    prior = {k: os.environ.get(k) for k in _ENV_SLOTS}
    for k in _ENV_SLOTS:
        os.environ.pop(k, None)
    for k, v in values.items():
        os.environ[k] = v
    return prior


def _restore_env(prior):
    for k, v in prior.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


def test_env_backfills_missing_connection_fields():
    prior = _set_env(MONGODB_HOST="envhost", MONGODB_PORT="27018",
                     MONGODB_DATABASE="envdb", MONGODB_USER="envuser",
                     MONGODB_PASSWORD="envpass")
    try:
        cfg = mg.MongodbMCPServer()._get_config({})
        assert cfg["host"] == "envhost", cfg
        assert cfg["database"] == "envdb", cfg
        assert cfg["user"] == "envuser", cfg
        assert cfg["password"] == "envpass", cfg
        # Port is coerced: the env value is a string, pymongo wants an int.
        assert cfg["port"] == 27018 and isinstance(cfg["port"], int), repr(cfg["port"])
    finally:
        _restore_env(prior)


def test_env_never_overrides_an_explicit_connection_config():
    """The anti-credential-bleed invariant (#760 family).

    A container-level MONGODB_* must never shadow a config the pipeline supplied
    — otherwise one connection's credentials silently redirect another's traffic.
    """
    prior = _set_env(MONGODB_HOST="envhost", MONGODB_PORT="27018",
                     MONGODB_DATABASE="envdb", MONGODB_USER="envuser",
                     MONGODB_PASSWORD="envpass")
    try:
        cfg = mg.MongodbMCPServer()._get_config(
            {"config": {"host": "real", "user": "ru", "password": "rp", "database": "rdb"}}
        )
        assert cfg["host"] == "real", cfg
        assert cfg["user"] == "ru", cfg
        assert cfg["password"] == "rp", cfg
        assert cfg["database"] == "rdb", cfg
        # Only the hole (port) is filled.
        assert cfg["port"] == 27018, cfg
    finally:
        _restore_env(prior)


def test_baked_empty_env_is_a_noop():
    """Exactly what the Dockerfile bakes: SET-but-empty. Behaviour must not move.

    This is why the backfill uses `v = os.getenv(k)` + `if v:` and NEVER the
    two-arg `os.getenv(k, default)`: the two-arg form returns "" for a
    SET-but-empty variable, which would inject host=""/port="" into configs that
    previously had no such keys.
    """
    prior = _set_env(**{k: "" for k in _ENV_SLOTS})
    try:
        s = mg.MongodbMCPServer()
        assert s._get_config({"config": {}}) == {}, s._get_config({"config": {}})
        assert s._build_uri({}) == "mongodb://localhost:27017/", s._build_uri({})
    finally:
        _restore_env(prior)


def test_env_database_reaches_the_driver():
    """The backfill must survive the real call path, not just _get_config alone."""
    prior = _set_env(MONGODB_DATABASE="envdb")
    try:
        s, client = _mongo_fakes.make_connector(mg, dbs={"envdb": {"t": [{"_id": 1}]}})
        out = s.drop_table({"config": {"host": "localhost"}, "table": "t"})
        # dropped is True only if the connector looked in "envdb" — with no
        # database resolved it would have addressed the empty-named db, found no
        # "t", and reported dropped=False.
        assert out["success"] is True and out["dropped"] is True, out
        assert "t" not in client["envdb"].list_collection_names(), \
            client["envdb"].list_collection_names()
    finally:
        _restore_env(prior)


# ================================= runner ===================================

def _run():
    tests = [v for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    failed = 0
    for t in tests:
        try:
            t()
            print(f"PASS {t.__name__}")
        except Exception as e:  # noqa: BLE001
            failed += 1
            print(f"FAIL {t.__name__}: {type(e).__name__}: {e}")
    print(f"\n{len(tests) - failed}/{len(tests)} passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_run())
