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


# ============ DESTINATION NAMESPACE (the sink's addNamespaceParam) ==========
#
# The sink forwards a pipeline's ``destination_namespace`` to every non-object-
# storage destination as BOTH ``namespace`` and ``db_or_schema`` (kafka-sink-worker
# ``addNamespaceParam``), having first stripped the ``<ns>.`` qualifier off the
# table. For MongoDB the namespace analog is the DATABASE — exactly as it is for
# ClickHouse. While this connector discarded it, CDC rows for a pipeline with a
# locked ``destination_namespace`` landed in the CONNECTION's database instead:
# "applied", right row counts, zero lag, wrong database.

def test_namespace_param_routes_every_write_to_that_database():
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"t": []}})
    ns = {"namespace": "postgres_test"}

    assert s.import_data({**CFG, **ns, "table": "t",
                          "data": [{"_id": 1, "v": "a"}]})["success"] is True
    assert s.upsert_data({**CFG, **ns, "table": "t",
                          "data": [{"_id": 2, "v": "b"}]})["success"] is True
    assert s.delete_data({**CFG, **ns, "table": "t",
                          "data": [{"_id": 1}]})["success"] is True

    assert [d["_id"] for d in client["postgres_test"]["t"].docs()] == [2], \
        client["postgres_test"]["t"].docs()
    assert client["appdb"]["t"].docs() == [], \
        "connection database must be untouched when a namespace is forwarded"


def test_db_or_schema_alone_is_honoured_like_namespace():
    """``addNamespaceParam`` sets BOTH keys to the same value, so either alone is
    the whole contract — a connector must not depend on the other being present."""
    s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"t": []}})
    out = s.import_data({**CFG, "db_or_schema": "ns2", "table": "t",
                         "data": [{"_id": 1}]})
    assert out["success"] is True, out
    assert len(client["ns2"]["t"].docs()) == 1, client["ns2"]["t"].docs()
    assert client["appdb"]["t"].docs() == [], client["appdb"]["t"].docs()


def test_empty_and_default_namespace_fall_back_to_the_connection_database():
    """Back-compat: the sink's ``isRealNamespace`` treats "" and the literal
    "default" as "this pipeline has no namespace". Both must resolve identically,
    or every historical single-namespace pipeline would re-route on upgrade."""
    for ns in ("", "   ", "default", "DEFAULT"):
        s, client = _mongo_fakes.make_connector(mg, dbs={"appdb": {"t": []}})
        out = s.import_data({**CFG, "namespace": ns, "table": "t",
                             "data": [{"_id": 1}]})
        assert out["success"] is True, (ns, out)
        assert len(client["appdb"]["t"].docs()) == 1, (ns, client["appdb"]["t"].docs())
        assert ns.strip() not in client._dbs or not client[ns.strip()]["t"].docs(), ns


def test_drop_table_resolves_the_same_database_as_the_writes():
    """The sink forwards the namespace on the reload-cleanup drop UNCONDITIONALLY
    (``addNamespaceParam(dropArgs, sm.DBOrSchema)``). A drop that resolved the
    connection database while the writes went to the namespace would "clean" the
    wrong place and let reload mode accumulate duplicates forever."""
    s, client = _mongo_fakes.make_connector(
        mg, dbs={"appdb": {"t": [{"_id": 99}]}, "ns": {"t": [{"_id": 1}]}})
    out = s.drop_table({**CFG, "namespace": "ns", "table": "t"})
    assert out["success"] is True and out["dropped"] is True, out
    assert "t" not in client["ns"].list_collection_names(), client["ns"].list_collection_names()
    assert "t" in client["appdb"].list_collection_names(), \
        "the connection database must not be dropped by a namespaced reload"


def test_source_reads_ignore_a_destination_namespace():
    """A DESTINATION namespace must never retarget a SOURCE read — the same
    connector instance serves both roles, and ``export`` resolving it would make a
    Mongo source silently read a different database."""
    s, _ = _mongo_fakes.make_connector(
        mg, dbs={"appdb": {"t": [{"_id": 1}, {"_id": 2}]}, "ns": {"t": []}})
    out = s.export({**CFG, "namespace": "ns", "table": "t", "limit": 10})
    assert out["success"] is True, out
    assert out["row_count"] == 2 and len(out["data"]) == 2, out


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


# ===================== FIND: Data Explorer document browse ==================

def _orders(n):
    return [{"_id": ObjectId(f"{i:024x}"), "n": i, "status": "paid" if i % 2 else "open",
             "customer": {"tier": "gold" if i % 3 == 0 else "std"},
             "created": datetime(2026, 1, 1)} for i in range(1, n + 1)]


def _find_server(docs):
    return _mongo_fakes.make_connector(mg, dbs={"appdb": {"orders": docs}})


def _find(s, **params):
    return s.find({**CFG, "collection": "orders", **params})


def _page_through(s, key, **params):
    """Follow next_cursor / next_skip to the end; return (all docs, page count)."""
    docs, pages, token = [], 0, {}
    while True:
        out = _find(s, **params, **token)
        assert out["success"] is True, out
        docs.extend(out["documents"])
        pages += 1
        assert pages < 50, "paging did not terminate"
        if not out["has_more"]:
            return docs, pages
        token = {key: out["next_cursor" if key == "cursor" else "next_skip"]}
        assert token[key] is not None, out


def test_find_default_page_is_keyset_on_id_with_relaxed_extjson():
    s, client = _find_server(_orders(3))
    out = _find(s)
    assert out["success"] is True, out
    assert out["returned"] == 3 and out["has_more"] is False, out
    assert out["paging_mode"] == "keyset" and out["next_cursor"] is None, out
    doc = out["documents"][0]
    # BSON types survive as Relaxed Extended JSON (paste-back-able into a filter)
    assert doc["_id"] == {"$oid": f"{1:024x}"}, doc
    assert doc["created"] == {"$date": "2026-01-01T00:00:00Z"}, doc
    assert doc["n"] == 1 and doc["customer"] == {"tier": "std"}, doc
    assert out["columns"][0] == "_id" and set(out["columns"]) == {"_id", "n", "status", "customer", "created"}, out
    cur = client["appdb"]["orders"].last_cursor
    assert cur.sorted_by == [("_id", 1)], cur.sorted_by
    assert cur.limited == mg.FIND_DEFAULT_LIMIT + 1, "fetches limit+1 to detect has_more"
    assert cur.max_time == mg.FIND_MAX_TIME_MS, cur.max_time
    assert client.closed is True


def test_find_keyset_pages_cover_every_document_once():
    s, client = _find_server(_orders(7))
    docs, pages = _page_through(s, "cursor", limit=3)
    assert [d["n"] for d in docs] == list(range(1, 8)), docs
    assert pages == 3, pages
    q = client["appdb"]["orders"].find_calls[1]["query"]
    assert set(q) == {"_id"} and isinstance(q["_id"]["$gt"], ObjectId), q


def test_find_keyset_descending_combined_with_filter():
    s, client = _find_server(_orders(9))
    docs, _ = _page_through(s, "cursor", limit=2, sort={"_id": -1}, filter={"status": "paid"})
    assert [d["n"] for d in docs] == [9, 7, 5, 3, 1], docs
    q = client["appdb"]["orders"].find_calls[1]["query"]
    assert q["$and"][0] == {"status": "paid"} and "$lt" in q["$and"][1]["_id"], q


def test_find_cursor_keeps_a_string_id_a_string():
    """export's str() cursor turns a 24-hex STRING _id into an ObjectId and skips
    nothing but matches nothing; find's cursor carries the BSON type."""
    hexes = [f"{i:024x}" for i in range(1, 4)]
    s, client = _find_server([{"_id": h, "v": i} for i, h in enumerate(hexes)])
    docs, _ = _page_through(s, "cursor", limit=1)
    assert [d["_id"] for d in docs] == hexes, docs
    q = client["appdb"]["orders"].find_calls[1]["query"]
    assert type(q["_id"]["$gt"]) is str, q


def test_find_rejects_cursor_tampering():
    s, client = _find_server(_orders(2))
    for bad in ("not json", '{"_id": {"$code": "function(){}"}}', '{"x": 1}', "[1]", "a" * 5000):
        out = _find(s, cursor=bad)
        assert out["success"] is False and out["error_code"] == "invalid_cursor", (bad, out)
    assert client["appdb"]["orders"].find_calls == []


def test_find_rejects_disallowed_operators_with_path_and_without_values():
    cases = [
        ({"$where": "sleep(1)"}, "filter.$where"),
        ({"$or": [{"a": 1}, {"$where": "sleep(1)"}]}, "filter.$or[1].$where"),
        ({"$expr": {"$gt": ["$a", "sleep(1)"]}}, "filter.$expr"),
        ({"$not": {"a": "sleep(1)"}}, "filter.$not"),  # $not is field-level only
        ({"$text": {"$search": "sleep(1)"}}, "filter.$text"),
        ({"a": {"$function": {"body": "sleep(1)", "args": [], "lang": "js"}}}, "filter.a.$function"),
        ({"a": {"$accumulator": {"init": "sleep(1)"}}}, "filter.a.$accumulator"),
        ({"a": {"$elemMatch": {"b": {"$where": "sleep(1)"}}}}, "filter.a.$elemMatch.b.$where"),
        ({"loc": {"$near": ["sleep(1)", 0]}}, "filter.loc.$near"),
        ({"a": {"$code": "sleep(1)"}}, "filter.a.$code"),
        ({"a": {"$binary": {"base64": "sleep(1)", "subType": "00"}}}, "filter.a.$binary"),
    ]
    s, client = _find_server(_orders(2))
    for flt, path in cases:
        out = _find(s, filter=flt)
        assert out["success"] is False, (flt, out)
        assert out["error_code"] == "operator_not_allowed", (flt, out)
        assert out["path"] == path, (flt, out)
        assert "sleep(1)" not in str(out), f"error echoed a filter value: {out}"
    # rejected before any driver call
    assert client["appdb"]["orders"].find_calls == []


def test_find_accepts_allowlisted_operators():
    s, _ = _find_server(_orders(6))
    out = _find(s, filter={"$and": [{"n": {"$gte": 2}}, {"n": {"$lte": 5}}],
                           "status": {"$in": ["paid"]}, "customer.tier": {"$exists": True},
                           "n2": {"$not": {"$eq": 1}}})
    assert out["success"] is True, out
    assert [d["n"] for d in out["documents"]] == [3, 5], out
    out = _find(s, filter={"status": {"$regex": "^OP", "$options": "i"}, "$nor": [{"n": 2}]})
    assert [d["n"] for d in out["documents"]] == [4, 6], out


def test_find_filter_depth_and_size_limits():
    def nested(k):
        f = {"x": 1}
        for _ in range(k):
            f = {"$and": [f]}
        return f
    s, _ = _find_server(_orders(1))
    assert _find(s, filter=nested(9))["success"] is True       # depth 19
    out = _find(s, filter=nested(10))                           # depth 21
    assert out["error_code"] == "filter_too_deep", out
    # far past json's recursion limit: must be refused, not crash
    out = _find(s, filter=nested(3000))
    assert out["error_code"] == "filter_too_deep", out
    out = _find(s, filter={"s": "x" * (mg.FIND_MAX_FILTER_BYTES + 1)})
    assert out["error_code"] == "filter_too_large", out


def test_find_decodes_extended_json_wrappers():
    from bson import Decimal128, Int64
    from datetime import timezone
    s, client = _find_server(_orders(1))
    out = _find(s, filter={"_id": {"$oid": f"{1:024x}"},
                           "created": {"$gte": {"$date": "2026-01-01T00:00:00Z"}},
                           "at": {"$lt": {"$date": 1767225600000}},
                           "big": {"$numberLong": "9007199254740993"},
                           "price": {"$numberDecimal": "1.10"},
                           "ids": {"$in": [{"$oid": f"{2:024x}"}]}})
    assert out["success"] is True, out
    q = client["appdb"]["orders"].find_calls[-1]["query"]
    assert q["_id"] == ObjectId(f"{1:024x}"), q
    assert q["created"]["$gte"] == datetime(2026, 1, 1, tzinfo=timezone.utc), q
    assert q["at"]["$lt"] == datetime(2026, 1, 1, tzinfo=timezone.utc), q
    assert isinstance(q["big"], Int64) and q["big"] == 9007199254740993, q
    assert isinstance(q["price"], Decimal128) and str(q["price"]) == "1.10", q
    assert q["ids"]["$in"] == [ObjectId(f"{2:024x}")], q
    for flt, path in [({"_id": {"$oid": "nothex"}}, "filter._id.$oid"),
                      ({"d": {"$date": "yesterday"}}, "filter.d.$date"),
                      ({"n": {"$numberLong": 5}}, "filter.n.$numberLong")]:
        out = _find(s, filter=flt)
        assert out["error_code"] == "invalid_filter" and out["path"] == path, (flt, out)
    out = _find(s, filter={"_id": {"$oid": f"{1:024x}", "n": 1}})
    assert out["error_code"] == "invalid_filter", out


def test_find_projection():
    s, client = _find_server(_orders(3))
    out = _find(s, projection={"status": 1})
    assert out["columns"] == ["_id", "status"], out
    # _id hidden: still keyset-pages (fetched for the cursor), stripped from output
    out = _find(s, projection={"_id": 0, "status": 1}, limit=1)
    assert out["documents"] == [{"status": "paid"}], out
    assert out["next_cursor"] is not None, out
    assert client["appdb"]["orders"].find_calls[-1]["projection"] == {"status": 1}
    docs, _ = _page_through(s, "cursor", projection={"_id": 0, "n": 1}, limit=2)
    assert docs == [{"n": 1}, {"n": 2}, {"n": 3}], docs
    for bad in ({"a": 1, "b": 0}, {"a.$": 1}, {"a": {"$slice": 2}}, {"a": 2}, ["a"]):
        out = _find(s, projection=bad)
        assert out["error_code"] == "invalid_projection", (bad, out)


def test_find_custom_sort_pages_by_skip_with_id_tiebreaker():
    s, client = _find_server(_orders(5))
    out = _find(s, sort={"n": -1}, limit=2)
    assert out["paging_mode"] == "skip" and out["next_skip"] == 2 and out["next_cursor"] is None, out
    assert client["appdb"]["orders"].last_cursor.sorted_by == [("n", -1), ("_id", 1)]
    docs, pages = _page_through(s, "skip", sort={"n": -1}, limit=2)
    assert [d["n"] for d in docs] == [5, 4, 3, 2, 1] and pages == 3, docs
    # list form keeps key order (a Go map would not)
    _find(s, sort=[["status", 1], ["n", -1]])
    assert client["appdb"]["orders"].last_cursor.sorted_by == [("status", 1), ("n", -1), ("_id", 1)]


def test_find_sort_and_paging_validation():
    s, client = _find_server(_orders(2))
    cases = [
        ({"sort": {f"k{i}": 1 for i in range(6)}}, "invalid_sort"),
        ({"sort": {"$natural": 1}}, "invalid_sort"),
        ({"sort": {"n": 2}}, "invalid_sort"),
        ({"sort": {"n": True}}, "invalid_sort"),
        ({"sort": [["n", 1], ["n", -1]]}, "invalid_sort"),
        ({"sort": "n"}, "invalid_sort"),
        ({"sort": {"n": 1}, "cursor": '{"_id": 1}'}, "invalid_cursor"),
        ({"skip": 5}, "invalid_skip"),
        ({"sort": {"n": 1}, "skip": mg.FIND_MAX_SKIP + 1}, "invalid_skip"),
        ({"sort": {"n": 1}, "skip": -1}, "invalid_skip"),
        ({"limit": "abc"}, "invalid_limit"),
        ({"limit": True}, "invalid_limit"),
        ({"limit": 2.5}, "invalid_limit"),
    ]
    for params, code in cases:
        out = _find(s, **params)
        assert out["success"] is False and out["error_code"] == code, (params, out)
    assert client["appdb"]["orders"].find_calls == []


def test_find_limits_are_clamped():
    s, client = _find_server(_orders(2))
    _find(s, limit=100000, max_time_ms=10 ** 9)
    cur = client["appdb"]["orders"].last_cursor
    assert cur.limited == mg.FIND_MAX_LIMIT + 1 and cur.max_time == mg.FIND_MAX_TIME_MS, vars(cur)
    _find(s, limit=0, max_time_ms=0)
    cur = client["appdb"]["orders"].last_cursor
    assert cur.limited == 2 and cur.max_time == 1, vars(cur)


def test_find_skip_paging_stops_at_the_skip_cap():
    prior = mg.FIND_MAX_SKIP
    mg.FIND_MAX_SKIP = 5
    try:
        s, _ = _find_server(_orders(20))
        out = _find(s, sort={"n": 1}, skip=4, limit=3)
        assert out["has_more"] is True and out["next_skip"] is None, out
        assert any("stops after 5" in w for w in out["warnings"]), out
    finally:
        mg.FIND_MAX_SKIP = prior


def test_find_response_byte_budget_truncates_and_stays_pageable():
    prior = mg.FIND_MAX_RESPONSE_BYTES
    mg.FIND_MAX_RESPONSE_BYTES = 400
    try:
        docs = [{"_id": i, "pad": "x" * 100} for i in range(1, 8)]
        s, _ = _find_server(docs)
        out = _find(s, limit=10)
        assert 0 < out["returned"] < 7 and out["truncated_bytes"] is True and out["has_more"] is True, out
        got, _ = _page_through(s, "cursor", limit=10)
        assert [d["_id"] for d in got] == list(range(1, 8)), got
        # a single document bigger than the whole budget: _id stub + warning
        s, _ = _find_server([{"_id": 1, "pad": "x" * 1000}, {"_id": 2, "pad": "y"}])
        out = _find(s)
        assert out["documents"] == [{"_id": 1}, {"_id": 2, "pad": "y"}], out
        assert out["truncated_bytes"] is True and "projection" in out["warnings"][0], out
    finally:
        mg.FIND_MAX_RESPONSE_BYTES = prior


def test_find_maps_server_errors_without_echoing_values():
    from pymongo.errors import ConnectionFailure, ExecutionTimeout, OperationFailure
    secret = "ssn-123-45-6789"
    cases = [
        (ExecutionTimeout(f"time limit {secret}", code=50, details={"codeName": "MaxTimeMSExpired"}),
         "query_timeout", "indexed field"),
        (OperationFailure(f"bad value {secret}", code=2, details={"codeName": "BadValue", "errmsg": secret}),
         "query_failed", "BadValue"),
        (RuntimeError(secret), "query_failed", "RuntimeError"),
        (ConnectionFailure("no servers"), "connection_failed", "no servers"),
    ]
    for err, code, hint in cases:
        s, client = _find_server(_orders(1))
        client["appdb"]["orders"].find_error = err
        out = _find(s, filter={"ssn": secret})
        assert out["success"] is False and out["error_code"] == code, (err, out)
        assert hint in out["error"], out
        assert secret not in str(out), f"error echoed a value: {out}"
        assert client.closed is True


def test_find_honours_read_preference():
    from pymongo import ReadPreference
    s, client = _find_server(_orders(1))
    _find(s)
    assert client["appdb"]["orders"].options_calls == []
    cfg = {"config": {**CFG["config"], "read_preference": "secondaryPreferred"}}
    out = s.find({**cfg, "collection": "orders"})
    assert out["success"] is True, out
    assert client["appdb"]["orders"].options_calls == [{"read_preference": ReadPreference.SECONDARY_PREFERRED}]
    cfg = {"config": {**CFG["config"], "read_preference": "fastest"}}
    out = s.find({**cfg, "collection": "orders"})
    assert out["error_code"] == "invalid_config", out


def test_find_collection_validation_and_string_config():
    import json
    s, client = _find_server(_orders(2))
    for bad in (None, "", "system.users", "a$b", "x" * 121, 5):
        out = s.find({**CFG, "collection": bad})
        assert out["error_code"] == "invalid_collection", (bad, out)
    assert s.find({**CFG, "table": "orders"})["returned"] == 2
    # the orchestrator's delegated path may send config as a JSON string
    out = s.find({"config": json.dumps(CFG["config"]), "collection": "orders"})
    assert out["success"] is True and out["returned"] == 2, out
    assert s.find({"config": "{not json", "collection": "orders"})["error_code"] == "invalid_config"


def test_find_never_writes():
    s, client = _find_server(_orders(4))
    _find(s)
    _find(s, filter={"status": "paid"}, sort={"n": -1}, projection={"n": 1}, limit=1)
    _page_through(s, "cursor", limit=1)
    coll = client["appdb"]["orders"]
    assert coll.insert_calls == [] and coll.bulk_ops == [] and coll.delete_calls == [] and coll.index_calls == []
    assert len(coll.docs()) == 4


def test_find_dispatches_as_mcp_tool_and_is_declared_in_lockstep():
    import json
    s, _ = _find_server(_orders(3))
    out = s._handle_tool_call({"name": "mongodb_find", "arguments": {
        **CFG, "collection": "orders", "filter": {"status": "paid"}, "sort": {"n": -1}, "limit": 1}})
    assert out["success"] is True and out["documents"][0]["n"] == 3, out
    assert s._resolves_to_handler("mongodb_find") is True
    cap_ops = {o["name"]: o for o in s.get_capabilities({})["operations"]}
    with open(os.path.join(_HERE, "metadata.json")) as fh:
        meta_ops = {o["name"]: o for o in json.load(fh)["operations"]}
    assert cap_ops["find"]["method"] == meta_ops["find"]["method"] == "mongodb_find"
    # the operator list published in metadata is the one the code enforces
    import re
    desc = next(p for p in meta_ops["find"]["parameters"] if p["name"] == "filter")["description"]
    assert set(re.findall(r"\$\w+", desc)) == set(mg.FIND_QUERY_OPERATORS | mg.FIND_EXTJSON_WRAPPERS), desc


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
