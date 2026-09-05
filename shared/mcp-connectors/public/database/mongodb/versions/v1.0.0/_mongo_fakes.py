"""Offline test doubles for ``pymongo`` — the MongoDB connector's driver seam.

``pymongo`` may be installed in the image, but these tests must run with NO real
MongoDB (no replica set, no network). ``MongodbMCPServer`` touches the driver
through exactly one seam: ``_get_client(config)`` returns a ``MongoClient``. The
connector then walks ``client[db_name][collection]`` and calls a small, fixed set
of pymongo methods:

  * ``client.admin.command("ping" | "hello" | "isMaster")``   (test_connection)
  * ``client.server_info()``                                   (discover_schema)
  * ``db.list_collection_names()``                             (discover_schema)
  * ``coll.find(limit=N)`` / ``coll.find(query).sort("_id", 1).limit(n)``
  * ``coll.count_documents({})``                               (discover_schema)
  * ``client.close()``

Tests swap ``_get_client`` for :class:`FakeMongoClient`. The fakes honour the one
filter shape the source emits — ``{"_id": {"$gt": <val>}}`` — and record every
``find`` call so a test can assert the connector built an ObjectId (vs raw) cursor.

MongoDB is ALSO a destination (``supports_destination=True``): it writes via
``coll.insert_many`` (import), ``coll.bulk_write([ReplaceOne(...)])`` (upsert), and
``coll.delete_many`` (delete). :class:`FakeCollection` models those three so the
write path can be exercised offline — ``bulk_write`` introspects real
``pymongo.ReplaceOne`` ops (``_filter``/``_doc``/``_upsert``) and ``insert_many``
raises a real ``BulkWriteError`` on a duplicate ``_id`` (the CDC-redelivery case).
The connector still does NOT capture CDC itself — Debezium change streams do that
out-of-process — so there is no ``watch`` to fake. The connector's only
CDC-readiness surface is replica-set detection in ``test_connection``;
:class:`FakeAdmin` models the ``hello`` reply.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _apply_id_gt(docs: List[Dict[str, Any]], query: Optional[Dict[str, Any]]):
    """Honour the connector's only filter shape: ``{"_id": {"$gt": val}}``.

    An empty/absent query returns everything. This is enough fidelity to exercise
    keyset continuation without a BSON query engine.
    """
    if not query:
        return list(docs)
    cond = query.get("_id")
    if isinstance(cond, dict) and "$gt" in cond:
        gt = cond["$gt"]
        return [d for d in docs if d.get("_id") is not None and d["_id"] > gt]
    if cond is not None:
        return [d for d in docs if d.get("_id") == cond]
    return list(docs)


class FakeCursor:
    """Supports the chained ``.find(q).sort("_id", 1).limit(n)`` and iteration."""

    def __init__(self, docs: List[Dict[str, Any]]):
        self._docs = list(docs)
        self.sorted_by = None

    def sort(self, key, direction=1):
        self.sorted_by = (key, direction)
        self._docs.sort(key=lambda d: d.get(key), reverse=direction < 0)
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __iter__(self):
        return iter(self._docs)


class _FakeInsertManyResult:
    def __init__(self, inserted_ids):
        self.inserted_ids = list(inserted_ids)


class _FakeBulkWriteResult:
    def __init__(self, matched, upserted, modified):
        self.matched_count = matched
        self.upserted_count = upserted
        self.modified_count = modified


class _FakeDeleteResult:
    def __init__(self, deleted):
        self.deleted_count = deleted


def _matches(doc: Dict[str, Any], flt: Optional[Dict[str, Any]]) -> bool:
    """Honour the write-path filter shapes the connector emits: equality,
    ``{"$in": [...]}`` (single-key delete), ``{"$gt": v}`` (find), and a top-level
    ``{"$or": [...]}`` (composite-key delete)."""
    if not flt:
        return True
    if "$or" in flt:
        return any(_matches(doc, sub) for sub in flt["$or"])
    for k, cond in flt.items():
        if isinstance(cond, dict) and "$in" in cond:
            if doc.get(k) not in cond["$in"]:
                return False
        elif isinstance(cond, dict) and "$gt" in cond:
            v = doc.get(k)
            if v is None or not (v > cond["$gt"]):
                return False
        else:
            if doc.get(k) != cond:
                return False
    return True


class FakeCollection:
    def __init__(self, docs: Optional[List[Dict[str, Any]]] = None,
                 name: Optional[str] = None, database: Optional[Any] = None):
        self._docs = list(docs or [])
        self.name = name                 # coll.name — used by _ensure_key_index cache key
        self.database = database         # coll.database.name — parent FakeDatabase
        # call logs so tests can assert the emitted filters / ops.
        self.find_calls: List[Dict[str, Any]] = []
        self.insert_calls: List[Dict[str, Any]] = []
        self.bulk_ops: List[Dict[str, Any]] = []
        self.delete_calls: List[Dict[str, Any]] = []
        self.index_calls: List[Any] = []   # create_index keys, so tests assert indexing

    def create_index(self, keys, **kw):
        self.index_calls.append(keys)
        # pymongo returns the index name; the value is unused by the connector.
        return "_".join(f"{k}_{d}" for k, d in keys) if isinstance(keys, list) else str(keys)

    def find(self, query=None, limit=None, projection=None):
        self.find_calls.append({"query": query, "limit": limit, "projection": projection})
        docs = _apply_id_gt(self._docs, query)
        if limit is not None:          # discover_schema: coll.find(limit=sample_size)
            docs = docs[:limit]
        return FakeCursor(docs)

    def count_documents(self, flt, **kw):
        return len(_apply_id_gt(self._docs, flt) if flt else self._docs)

    # ----------------------------- writes -------------------------------- #
    def insert_many(self, docs, ordered=True, **kw):
        """Insert docs, auto-assigning ObjectId _id when absent. A doc whose _id
        already exists raises a real BulkWriteError (the duplicate-key / CDC
        redelivery case) after the surviving inserts land — matching pymongo with
        ordered=False."""
        from bson import ObjectId
        self.insert_calls.append({"docs": list(docs), "ordered": ordered})
        inserted: List[Any] = []
        write_errors: List[Dict[str, Any]] = []
        for i, d in enumerate(docs):
            doc = dict(d)
            if "_id" not in doc:
                doc["_id"] = ObjectId()
            if any(e.get("_id") == doc["_id"] for e in self._docs):
                write_errors.append({"index": i, "code": 11000, "errmsg": "E11000 duplicate key"})
                if ordered:
                    break
                continue
            self._docs.append(doc)
            inserted.append(doc["_id"])
        if write_errors:
            from pymongo.errors import BulkWriteError
            raise BulkWriteError({"nInserted": len(inserted), "writeErrors": write_errors})
        return _FakeInsertManyResult(inserted)

    def _find_index(self, flt):
        for i, d in enumerate(self._docs):
            if _matches(d, flt):
                return i
        return None

    def bulk_write(self, ops, ordered=True, **kw):
        """Apply a list of pymongo ReplaceOne ops (the only op the connector emits),
        introspecting each op's _filter/_doc/_upsert."""
        self.bulk_ops.append({"ops": list(ops), "ordered": ordered})
        matched = upserted = modified = 0
        for op in ops:
            flt = getattr(op, "_filter", None)
            doc = getattr(op, "_doc", None)
            upsert = bool(getattr(op, "_upsert", False))
            idx = self._find_index(flt)
            if idx is not None:
                self._docs[idx] = dict(doc)
                matched += 1
                modified += 1
            elif upsert:
                self._docs.append(dict(doc))
                upserted += 1
        return _FakeBulkWriteResult(matched, upserted, modified)

    def delete_many(self, flt, **kw):
        self.delete_calls.append({"filter": flt})
        before = len(self._docs)
        self._docs = [d for d in self._docs if not _matches(d, flt)]
        return _FakeDeleteResult(before - len(self._docs))

    # test helper: current stored documents
    def docs(self):
        return list(self._docs)


class FakeDatabase:
    def __init__(self, colls: Dict[str, Any], name: str = "testdb"):
        self.name = name                 # db.name — used by _ensure_key_index cache key
        self._colls: Dict[str, FakeCollection] = {}
        for cname, c in colls.items():
            coll = c if isinstance(c, FakeCollection) else FakeCollection(c)
            coll.name, coll.database = cname, self
            self._colls[cname] = coll

    def __getitem__(self, name):
        if name not in self._colls:
            self._colls[name] = FakeCollection([], name=name, database=self)
        return self._colls[name]

    def list_collection_names(self):
        return list(self._colls)

    def drop_collection(self, name):
        self._colls.pop(name, None)
        return {"ok": 1}


class FakeAdmin:
    """Models ``client.admin.command(...)`` for the connectivity probe.

    ``ping`` returns ``{}`` (liveness). ``hello``/``isMaster`` return ``setName``
    so the connector can decide replica-set-ness (the CDC-readiness gate).
    """

    def __init__(self, set_name: Optional[str] = "rs0", ping_error: Optional[Exception] = None):
        self.set_name = set_name
        self.ping_error = ping_error
        self.commands: List[str] = []

    def command(self, name, *args, **kwargs):
        self.commands.append(name)
        if name == "ping":
            if self.ping_error is not None:
                raise self.ping_error
            return {"ok": 1}
        if name in ("hello", "isMaster"):
            return {"setName": self.set_name} if self.set_name else {}
        return {}


class FakeMongoClient:
    def __init__(self, dbs=None, set_name="rs0", version="7.0.0", ping_error=None):
        self._dbs: Dict[str, FakeDatabase] = {
            name: (d if isinstance(d, FakeDatabase) else FakeDatabase(d, name=name))
            for name, d in (dbs or {}).items()
        }
        self.admin = FakeAdmin(set_name=set_name, ping_error=ping_error)
        self._version = version
        self.closed = False

    def __getitem__(self, name):
        if name not in self._dbs:
            self._dbs[name] = FakeDatabase({}, name=name)
        return self._dbs[name]

    def server_info(self):
        return {"version": self._version}

    def close(self):
        self.closed = True


def make_connector(connector_module, *, dbs=None, set_name="rs0", version="7.0.0",
                   ping_error=None):
    """Build a MongodbMCPServer wired to a FakeMongoClient (the _get_client seam)."""
    server = connector_module.MongodbMCPServer()
    client = FakeMongoClient(dbs=dbs, set_name=set_name, version=version,
                             ping_error=ping_error)
    server._get_client = lambda config: client  # type: ignore[assignment]
    return server, client
