#!/usr/bin/env python3
"""MongoDB MCP Connector — document-database SOURCE and DESTINATION operations.

Batch source (pymongo) + CDC source (via Debezium change streams). This connector
does NOT capture CDC itself: for a CDC pipeline the orchestrator provisions a
Debezium MongoDB connector (change streams) and the kafka-mcp-sink decodes each
event into the packed { _id, document } destination shape. This connector's job is
the product-path plumbing every source needs: a pre-save connectivity test, schema
(collection) discovery for HITL table selection, capability advertisement, and a
batch export (used for cdc_initial_load=batch / plain batch pipelines).

As a DESTINATION it writes documents via pymongo: `import_data` (insert),
`upsert_data` (idempotent replace keyed on _id / key_fields — the CDC
insert/update path), and `delete_data` (remove by key — the CDC delete path). The
kafka-mcp-sink selects which of the three to call per change event; the connector
never inspects an op field itself. There is NO DDL: collections are schemaless and
auto-created by MongoDB on first write, so `import_data`/`upsert_data` need no
prior `ensure_table` (the sink skips DDL for document-DB destinations).

MongoDB has no fixed schema; discovery samples documents to surface field names and
_id is always the primary key of the packed destination table, so no collection can
ever be "missing a PK".
"""

import sys
import os
import base64
import ipaddress
import logging
from datetime import datetime, date
from typing import Dict, Any, List, Optional

# Resolve the shared base_connector (see Dockerfile: it is copied to
# /app/shared/mcp-connectors and that dir is on PYTHONPATH). Fall back to the
# sibling directory for local (non-Docker) runs.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
try:
    from base_connector import BaseMCPConnector
except ImportError:  # pragma: no cover - local dev fallback
    sys.path.insert(0, os.path.dirname(__file__))
    from base_connector import BaseMCPConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _json_safe(value: Any) -> Any:
    """Recursively convert a BSON/Mongo value into a JSON-serialisable form.

    ObjectId -> str, datetime/date -> ISO 8601, Decimal128 -> str, bytes ->
    base64, and nested dict/list are converted element-wise. Unknown types fall
    back to str() so a single exotic field never fails a whole export.
    """
    # Fast path for JSON primitives.
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    # bson types (ObjectId, Decimal128, Timestamp, Binary, ...) — import lazily so
    # the module still imports if bson is somehow absent.
    try:
        from bson import ObjectId, Decimal128, Timestamp
        from bson.binary import Binary
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, Decimal128):
            return str(value.to_decimal())
        if isinstance(value, Timestamp):
            return {"t": value.time, "i": value.inc}
        if isinstance(value, Binary):
            return base64.b64encode(bytes(value)).decode("ascii")
    except Exception:
        pass
    return str(value)


def _infer_type(value: Any) -> str:
    """Map a sampled Python/BSON value to a coarse column type string."""
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "double"
    if isinstance(value, (dict,)):
        return "object"
    if isinstance(value, (list, tuple)):
        return "array"
    if isinstance(value, (datetime, date)):
        return "timestamp"
    return "string"


def _is_local_db_host(host: str) -> bool:
    """Report whether *host* points at a local/dev or docker-internal database
    where TLS is typically not configured — the local/remote split behind the
    secure-by-default TLS decision in _build_uri. Mirrors the Go isLocalDBHost:
    empty and explicit loopback names are local; IP LITERALS are classified by
    address (loopback / RFC1918-private / link-local / CGNAT 100.64.0.0/10 →
    local) so an IPv6 literal or a public IPv4 literal is treated remote instead
    of by a textual '.' heuristic that would miss them; non-literal hostnames
    keep the dotless=local heuristic (docker service names), and any dotted
    hostname is remote so callers default to TLS.
    """
    h = str(host or "").strip().lower().strip("[]")  # tolerate bracketed IPv6
    if h in ("", "localhost", "127.0.0.1", "::1", "host.docker.internal"):
        return True
    try:
        ip = ipaddress.ip_address(h)
    except ValueError:
        ip = None
    if ip is not None:
        if ip.is_loopback or ip.is_private or ip.is_link_local:
            return True
        # RFC6598 carrier-grade NAT (100.64.0.0/10) — is_private misses it.
        if ip.version == 4 and ip in ipaddress.ip_network("100.64.0.0/10"):
            return True
        return False
    # Non-literal hostname: dotless single-label names are docker-internal/local
    # DNS; any dotted hostname is remote so callers default to verified TLS.
    return "." not in h


class MongodbMCPServer(BaseMCPConnector):
    """MCP server for MongoDB (document DB source)."""

    def __init__(self):
        super().__init__()

        # Connector identity — used by base_connector for tool-name dispatch
        # (tool "mongodb_<op>" -> self.<op>) and param normalization.
        self.connector_type = "mongodb"
        self.connector_category = "document_db"

        # Capability flags (advertised via get_capabilities). MongoDB is a source
        # for both batch and CDC (Debezium change streams) AND a destination: it
        # writes documents via pymongo (import/upsert/delete keyed on _id). It has
        # no DDL — collections are schemaless and auto-created on first write, so
        # supports_ddl stays False while auto_create_destination_tables is True.
        self.supports_source = True
        self.supports_destination = True
        self.supports_cdc = True
        self.supports_ddl = False
        self.auto_create_destination_tables = True

        # Performance metadata read by base_connector / callers.
        self.supported_formats = ["json"]
        self.max_batch_size = 10000

        # Collections whose write key has already been indexed this process, keyed
        # by (db, collection, key_fields). Upsert/delete filter on the write key,
        # so without an index every op is a full collection scan (O(n^2) at scale);
        # the index is ensured once per key here. See _ensure_key_index.
        self._indexed_keys: set = set()

        self.log("MongoDB MCP Server initialized")

    # ------------------------------------------------------------------ #
    # Connection                                                          #
    # ------------------------------------------------------------------ #
    def _build_uri(self, config: Dict[str, Any]) -> str:
        """Build a MongoDB connection URI from a connection config.

        An explicit connection string always wins (required for Atlas
        mongodb+srv://). Otherwise assemble mongodb://[creds@]host:port/ with
        replicaSet / authSource / tls query options — mirroring how the Debezium
        MongoDB branch builds its mongodb.connection.string so the batch source
        and the CDC source address the deployment identically.
        """
        from urllib.parse import quote_plus

        explicit = str(
            config.get("connection_string")
            or config.get("mongodb_connection_string")
            or config.get("mongodb_uri")
            or config.get("uri")
            or ""
        ).strip()
        if explicit:
            return explicit

        host = str(config.get("host") or "localhost").strip()
        port = config.get("port") or 27017
        user = str(config.get("user") or config.get("username") or "").strip()
        password = str(config.get("password") or "").strip()

        creds = ""
        if user:
            creds = quote_plus(user)
            if password:
                creds += ":" + quote_plus(password)
            creds += "@"

        opts: List[str] = []
        ssl_raw = str(
            config.get("sslmode") or config.get("ssl_mode") or config.get("ssl") or config.get("tls") or ""
        ).strip().lower()
        # TLS is secure-by-default with an explicit opt-out, mirroring the Go side
        # (resolveMySQLTLSMode / resolvePostgresSSLMode). Precedence:
        #   1. Atlas (mongodb.net) always uses TLS (unchanged).
        #   2. An explicit disable-family value (disable/false/off/0/none/…) opts out.
        #   3. An explicit enable/verify value turns TLS on (unchanged).
        #   4. No explicit ssl/tls config → default REMOTE hosts to TLS. This closes
        #      the plaintext hole: a self-hosted remote Mongo (e.g. mongo.acme.com)
        #      previously connected over mongodb:// with no TLS, so a passive
        #      eavesdropper could read every document/PII off the wire. Local /
        #      docker-internal hosts keep the no-TLS default (dev/e2e rarely runs TLS).
        _TLS_ENABLE = (
            "require", "required", "true", "on", "prefer", "preferred",
            "verify-ca", "verify_ca", "verify-full", "verify-identity", "verify_identity",
        )
        _TLS_DISABLE = ("disable", "disabled", "false", "off", "0", "none")
        if "mongodb.net" in host.lower():
            want_tls = True
        elif ssl_raw in _TLS_DISABLE:
            want_tls = False
        elif ssl_raw in _TLS_ENABLE:
            want_tls = True
        elif not ssl_raw:
            want_tls = not _is_local_db_host(host)
        else:
            want_tls = False  # unrecognized explicit value — preserve prior (off)
        if want_tls:
            opts.append("tls=true")
        if user:
            opts.append("authSource=" + quote_plus(str(config.get("auth_source") or config.get("authSource") or "admin")))
        rs = str(config.get("replica_set") or config.get("replicaSet") or "").strip()
        if rs:
            opts.append("replicaSet=" + quote_plus(rs))
        query = ("?" + "&".join(opts)) if opts else ""
        return f"mongodb://{creds}{host}:{port}/{query}"

    def _get_client(self, config: Dict[str, Any]):
        """Create a short-lived pymongo client for one operation."""
        import pymongo
        uri = self._build_uri(config)
        return pymongo.MongoClient(
            uri,
            serverSelectionTimeoutMS=int(config.get("server_selection_timeout_ms", 8000)),
            connectTimeoutMS=int(config.get("connect_timeout_ms", 8000)),
        )

    def _database_name(self, config: Dict[str, Any]) -> str:
        return str(config.get("database") or config.get("db_name") or config.get("db") or "").strip()

    def _get_config(self, params: Dict) -> Dict[str, Any]:
        """Connection config with MONGODB_* env BACKFILL (never an override).

        This image's Dockerfile bakes five operator-override slots
        (``ENV MONGODB_HOST="" … MONGODB_PASSWORD=""``). Until this method existed
        nothing in the connector read any of them, so an operator who set one saw
        nothing happen and no error — the dead-slot class fixed for oracle and
        sqlserver by #760 (KI-DBMCP-ENV-DEFAULTS-READ-MYSQL-NAMES).

        Three properties here are load-bearing; do not "simplify" them:

        1. ``if not config.get(key)`` comes FIRST. A supplied connection config
           always wins, so a stray container-level MONGODB_* can never shadow one
           tenant's credentials with another's. The backfill only fills holes.
        2. ``os.getenv(env)`` guarded by ``if v:`` — NEVER the two-arg
           ``os.getenv(env, default)``. The Dockerfile bakes these SET-but-empty,
           so the two-arg form returns "" rather than falling back, and would write
           "" into the dict — defeating ``_build_uri``'s ``or "localhost"`` /
           ``or 27017`` fallbacks for every existing deployment.
        3. ``dict(src)`` COPIES. Callers pass ``prepared``/``params`` dicts they
           own (``prepare_import_data`` owns ``prepared["config"]``); mutating them
           in place would leak the backfill back out to the caller.

        Note the deliberate divergence from the relational connectors'
        ``params.get('config', params)``: mongodb's call sites have always used a
        plain ``{}`` fallback with no flat-params shape, and that is preserved
        exactly — this method changes which VALUES land, never which SHAPES parse.
        """
        src = (params or {}).get("config") or {}
        config = dict(src) if isinstance(src, dict) else {}
        env_map = {
            "host": "MONGODB_HOST",
            "port": "MONGODB_PORT",
            "database": "MONGODB_DATABASE",
            "user": "MONGODB_USER",
            "password": "MONGODB_PASSWORD",
        }
        for key, env in env_map.items():
            if not config.get(key):
                v = os.getenv(env)
                if v:
                    config[key] = v
        if config.get("port"):
            try:
                config["port"] = int(config["port"])
            except (ValueError, TypeError):
                pass
        return config

    # ------------------------------------------------------------------ #
    # Core operations                                                     #
    # ------------------------------------------------------------------ #
    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        """Ping the deployment and report whether it is a replica set.

        Change streams (and therefore CDC) require a replica set or sharded
        cluster; a standalone mongod cannot be a CDC source. We surface that as a
        warning here (non-fatal for a batch connection test) so the failure is
        visible before a CDC pipeline is started.
        """
        config = self._get_config(params)
        client = None
        try:
            client = self._get_client(config)
            client.admin.command("ping")
            is_replica_set = False
            set_name = None
            try:
                hello = client.admin.command("hello")
                set_name = hello.get("setName")
                is_replica_set = bool(set_name)
            except Exception:
                # Older servers: fall back to isMaster.
                try:
                    im = client.admin.command("isMaster")
                    set_name = im.get("setName")
                    is_replica_set = bool(set_name)
                except Exception:
                    pass
            result = {
                "success": True,
                "message": "Connection successful",
                "is_replica_set": is_replica_set,
            }
            if set_name:
                result["replica_set"] = set_name
            if not is_replica_set:
                result["warning"] = (
                    "Connected, but this deployment is not a replica set. CDC "
                    "(change streams) requires a replica set or sharded cluster; "
                    "run rs.initiate() before starting a CDC pipeline."
                )
            return result
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        """Validate a config shape without connecting."""
        config = self._get_config(params)
        errors: List[str] = []
        has_uri = bool(
            config.get("connection_string")
            or config.get("mongodb_connection_string")
            or config.get("uri")
        )
        if not has_uri and not config.get("host"):
            errors.append("Missing required field: host (or connection_string)")
        if not self._database_name(config):
            errors.append("Missing required field: database")
        return {"valid": len(errors) == 0, "errors": errors, "warnings": []}

    def discover_schema(self, params: Dict = None) -> Dict[str, Any]:
        """List collections in the configured database as selectable 'tables'.

        Each collection is a table whose 'schema' is the MongoDB database name and
        whose primary key is always _id. Columns are inferred from a small sample
        of documents (union of top-level field names). Row counts are exact
        (countDocuments) when requested.
        """
        params = params or {}
        config = self._get_config(params)
        start = datetime.utcnow()
        db_name = self._database_name(config)

        result: Dict[str, Any] = {
            "schema_version": "2.0",
            "discovered_at": start.isoformat() + "Z",
            "connector_type": self.connector_type,
            "connector_version": os.getenv("MCP_CONNECTOR_VERSION", "1.0.0"),
            "database_version": None,
            "total_tables_available": 0,
            "total_tables_discovered": 0,
            "discovery_duration_ms": 0,
            "overall_status": "success",
            "warnings_objects": [],
            "warnings_messages": [],
            "tables": [],
        }

        include_columns = params.get("include_columns", True)
        include_row_counts = params.get("include_row_counts", True)
        max_tables = int(params.get("max_tables", 100))
        sample_size = int(params.get("sample_size", 20))

        client = None
        try:
            client = self._get_client(config)
            if not db_name:
                result["overall_status"] = "failed"
                result["warnings_messages"].append("Missing 'database' in config")
                return result
            try:
                result["database_version"] = client.server_info().get("version")
            except Exception:
                pass

            db = client[db_name]
            names = [n for n in db.list_collection_names() if not n.startswith("system.")]
            names.sort()
            result["total_tables_available"] = len(names)

            for coll_name in names[:max_tables]:
                coll = db[coll_name]
                columns: List[Dict[str, Any]] = []
                if include_columns:
                    seen: Dict[str, str] = {}
                    try:
                        for doc in coll.find(limit=sample_size):
                            for key, val in doc.items():
                                if key not in seen:
                                    seen[key] = _infer_type(val)
                    except Exception as e:
                        result["warnings_messages"].append(f"{coll_name}: sample failed: {e}")
                    # _id first, then the rest in first-seen order.
                    if "_id" not in seen:
                        seen = {"_id": "string", **seen}
                    for field, tname in seen.items():
                        columns.append({"name": field, "type": tname, "nullable": field != "_id"})

                table_obj: Dict[str, Any] = {
                    "name": coll_name,
                    "schema": db_name,
                    "discovery_status": "complete",
                    "columns": columns,
                    "primary_keys": ["_id"],
                    "primary_key": ["_id"],
                }
                if include_row_counts:
                    try:
                        table_obj["row_count"] = coll.count_documents({})
                        table_obj["is_exact_count"] = True
                    except Exception:
                        table_obj["row_count"] = None
                        table_obj["is_exact_count"] = False
                result["tables"].append(table_obj)

            result["total_tables_discovered"] = len(result["tables"])
            result["discovery_duration_ms"] = int((datetime.utcnow() - start).total_seconds() * 1000)
            return result
        except Exception as e:
            result["overall_status"] = "failed"
            result["warnings_messages"].append(str(e))
            return result
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    def get_primary_key(self, params: Dict = None) -> Dict[str, Any]:
        """MongoDB's primary key is always _id."""
        return {"success": True, "primary_key": "_id", "primary_keys": ["_id"]}

    def get_capabilities(self, params: Dict = None) -> Dict[str, Any]:
        """Advertise connector capabilities and the dispatchable operations."""
        runtime_version = (os.getenv("MCP_CONNECTOR_VERSION") or os.getenv("CONNECTOR_VERSION") or "").strip()
        if runtime_version and not runtime_version.startswith("v"):
            runtime_version = f"v{runtime_version}"
        return {
            "success": True,
            "connector_type": self.connector_type,
            "connector_category": self.connector_category,
            "connector_version": runtime_version or None,
            "supports_source": self.supports_source,
            "supports_destination": self.supports_destination,
            "supports_cdc": self.supports_cdc,
            "supports_ddl": self.supports_ddl,
            "auto_create_destination_tables": self.auto_create_destination_tables,
            "driver_pattern": "pymongo",
            "operations": [
                {"name": "test_connection", "method": "mongodb_test_connection", "type": "core",
                 "description": "Test connectivity to MongoDB (replica-set aware)"},
                {"name": "validate_config", "method": "mongodb_validate_config", "type": "core",
                 "description": "Validate configuration without connecting"},
                {"name": "discover_schema", "method": "mongodb_discover_schema", "type": "core",
                 "description": "Discover collections and sampled fields"},
                {"name": "get_capabilities", "method": "mongodb_get_capabilities", "type": "core",
                 "description": "Return connector capabilities"},
                {"name": "get_primary_key", "method": "mongodb_get_primary_key", "type": "core",
                 "description": "Return the primary key (_id) for a collection"},
                {"name": "export", "method": "mongodb_export", "type": "source",
                 "description": "Export documents from a collection (_id keyset paging)"},
                {"name": "import_data", "method": "mongodb_import_data", "type": "destination",
                 "description": "Insert documents into a collection (batch / CDC insert)"},
                {"name": "upsert_data", "method": "mongodb_upsert_data", "type": "destination",
                 "description": "Idempotent replace keyed on _id / key_fields (CDC insert+update)"},
                {"name": "delete_data", "method": "mongodb_delete_data", "type": "destination",
                 "description": "Delete documents by key field(s) (CDC delete)"},
                {"name": "drop_table", "method": "mongodb_drop_table", "type": "destination",
                 "description": "Drop a collection (reload-mode cleanup)"},
            ],
            "capabilities": {
                "max_batch_size": self.max_batch_size,
                "supported_formats": self.supported_formats,
                "supports_cdc": self.supports_cdc,
                "supports_ddl": self.supports_ddl,
                "auto_create_destination_tables": self.auto_create_destination_tables,
            },
        }

    # ------------------------------------------------------------------ #
    # Export (batch source)                                               #
    # ------------------------------------------------------------------ #
    def export(self, params: Dict = None) -> Dict[str, Any]:
        """Export documents from a collection using stable _id keyset paging.

        The collection may arrive as `collection`, `table`, or a db-qualified
        `db.collection`; the last segment is the collection. Documents are
        returned JSON-safe (ObjectId/date/Decimal128 coerced). Paging is by
        ascending _id (`cursor` = last _id seen) which is stable under concurrent
        writes, unlike skip/limit.
        """
        params = params or {}
        try:
            prepared = self.prepare_export_data(params)
        except Exception:
            prepared = dict(params)
            prepared.setdefault("config", params.get("config", {}) or {})

        config = self._get_config({"config": prepared.get("config") or params.get("config") or {}})
        raw_coll = (
            prepared.get("collection")
            or prepared.get("table")
            or params.get("collection")
            or params.get("table")
            or ""
        )
        collection = str(raw_coll).strip()
        if "." in collection:
            collection = collection.split(".")[-1]  # strip db-qualifier
        if not collection:
            return {"success": False, "error": "Missing 'collection'/'table' parameter"}

        try:
            limit = int(prepared.get("limit", params.get("limit", self.max_batch_size)) or self.max_batch_size)
        except Exception:
            limit = self.max_batch_size
        limit = max(1, min(limit, self.max_batch_size))
        cursor_val = prepared.get("cursor", params.get("cursor"))

        db_name = self._database_name(config)
        client = None
        try:
            client = self._get_client(config)
            db = client[db_name]
            coll = db[collection]

            query: Dict[str, Any] = {}
            if cursor_val not in (None, ""):
                # Resume after the last _id. Prefer ObjectId comparison when the
                # cursor is a 24-hex string; otherwise compare as the raw value.
                oid = None
                try:
                    from bson import ObjectId
                    if isinstance(cursor_val, str) and len(cursor_val) == 24:
                        oid = ObjectId(cursor_val)
                except Exception:
                    oid = None
                query = {"_id": {"$gt": oid if oid is not None else cursor_val}}

            docs = list(coll.find(query).sort("_id", 1).limit(limit))

            next_cursor = None
            if docs:
                last_id = docs[-1].get("_id")
                next_cursor = str(last_id) if last_id is not None else None

            rows = [_json_safe(d) for d in docs]

            # Column union across the batch (stable order, _id first).
            col_order: List[str] = ["_id"] if any("_id" in r for r in rows) else []
            for r in rows:
                for k in r.keys():
                    if k not in col_order:
                        col_order.append(k)

            try:
                result = self.finalize_export_result(rows, prepared, col_order)
            except Exception:
                result = {
                    "success": True,
                    "table": collection,
                    "data": rows,
                    "columns": col_order,
                    "row_count": len(rows),
                }
            result["has_more"] = len(docs) >= limit
            if next_cursor is not None and result.get("has_more"):
                result["next_cursor"] = next_cursor
                result["paging_mode"] = "keyset"
                result["cursor_column"] = "_id"
            return result
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------ #
    # Destination (write path)                                            #
    # ------------------------------------------------------------------ #
    #
    # The kafka-mcp-sink routes each change event to one of these by tool name
    # (mongodb_import_data / _upsert_data / _delete_data) — the connector never
    # reads an op field. Every method starts with self.prepare_import_data() so
    # the Claim-Check staging fetch + protected-config precedence (inherited from
    # base_connector) apply uniformly, mirroring the relational connectors.
    #
    # MongoDB has no DDL: collections auto-create on first write, so there is no
    # ensure_table/drop_table. Writes key on _id by default (Mongo's always-present
    # PK); a relational source's own key column(s) arrive via `key_fields` and are
    # honored so upsert/delete stay idempotent.

    @staticmethod
    def _close_client(client) -> None:
        if client is not None:
            try:
                client.close()
            except Exception:
                pass

    def _resolve_collection(self, prepared: Dict[str, Any], params: Dict[str, Any]) -> str:
        """Resolve the target collection, stripping any db-qualifier (db.coll)."""
        raw = (
            prepared.get("table")
            or prepared.get("collection")
            or params.get("collection")
            or params.get("table")
            or ""
        )
        coll = str(raw).strip()
        if "." in coll:
            coll = coll.split(".")[-1]
        return coll

    def _key_fields(self, params: Dict[str, Any], prepared: Dict[str, Any]) -> List[str]:
        """The write key. Defaults to Mongo's _id; a relational source passes its
        own primary key column(s) via key_fields/primary_key/pk_fields."""
        cfg = prepared.get("config", {}) or {}
        kf = (
            params.get("key_fields")
            or params.get("primary_key_fields")
            or params.get("primary_key")
            or params.get("pk_fields")
            or cfg.get("key_fields")
        )
        if isinstance(kf, str):
            kf = [kf]
        fields = [str(k).strip() for k in (kf or []) if str(k).strip()]
        return fields or ["_id"]

    @staticmethod
    def _coerce_id(value: Any) -> Any:
        """A 24-hex string _id is stored as an ObjectId so a Mongo→Mongo copy keeps
        native _id fidelity (mirrors the export cursor convention). Any other value
        (int _id, relational PK, non-hex string) is kept as-is."""
        if isinstance(value, str) and len(value) == 24:
            try:
                from bson import ObjectId
                return ObjectId(value)
            except Exception:
                return value
        return value

    def _prepare_doc(self, doc: Any) -> Any:
        """Coerce a document's _id back to ObjectId when it is a 24-hex string."""
        if isinstance(doc, dict) and "_id" in doc:
            out = dict(doc)
            out["_id"] = self._coerce_id(out["_id"])
            return out
        return doc

    def _key_filter(self, doc: Dict[str, Any], key_fields: List[str]) -> Optional[Dict[str, Any]]:
        """Build a Mongo filter from a document's key field(s); None if any key is
        absent (the record cannot be upserted/deleted by key)."""
        flt: Dict[str, Any] = {}
        for k in key_fields:
            if k not in doc:
                return None
            flt[k] = self._coerce_id(doc[k]) if k == "_id" else doc[k]
        return flt or None

    @staticmethod
    def _extract_key_doc(record: Any) -> Optional[Dict[str, Any]]:
        """Unwrap a CDC delete event to the dict holding key values. Delete events
        may carry the key under before/key/kafka_key/... rather than at top level."""
        if not isinstance(record, dict):
            return None
        for alias in ("before", "key", "kafka_key", "record_key", "primary_key", "keys"):
            inner = record.get(alias)
            if isinstance(inner, dict) and inner:
                return inner
        return record

    def _ensure_key_index(self, coll, key_fields: List[str]) -> None:
        """Index the write key so upsert/delete filter by an index instead of a
        full collection scan. Without it, each keyed op scans the whole collection,
        so a bulk upsert/delete is O(n^2) (measured ~80x slower at 50k docs). _id is
        already uniquely indexed by Mongo, so it is skipped. The result is cached per
        (db, collection, key) in this long-lived process, costing at most one
        create_index round-trip per key. A missing index only slows writes, never
        corrupts them, so index-creation failures are swallowed and never fail the
        batch. The index is non-unique on purpose — a transient CDC redelivery must
        not be rejected by a unique constraint."""
        if not key_fields or key_fields == ["_id"]:
            return
        try:
            cache_key = (coll.database.name, coll.name, tuple(key_fields))
        except Exception:
            cache_key = None
        if cache_key is not None and cache_key in self._indexed_keys:
            return
        try:
            coll.create_index([(k, 1) for k in key_fields])
        except Exception:
            pass
        if cache_key is not None:
            self._indexed_keys.add(cache_key)

    def import_data(self, params: Dict = None) -> Dict[str, Any]:
        """Insert documents into a collection (batch / CDC insert).

        mode=replace truncates the collection first; mode=upsert is delegated to
        the idempotent upsert path so a plan can route redelivery-safe writes here.
        Duplicate-key errors on redelivery are tolerated (the surviving inserts are
        counted) so a replayed CDC insert never fails the batch.
        """
        params = params or {}
        prepared = self.prepare_import_data(params)
        if not prepared.get("success"):
            return prepared

        collection = self._resolve_collection(prepared, params)
        if not collection:
            return {"success": False, "error": "Missing 'collection'/'table' parameter"}

        mode = str(prepared.get("mode") or "append").lower()
        if mode == "upsert":
            return self._upsert_nosql(collection, prepared.get("data") or [],
                                      self._key_fields(params, prepared), prepared)

        data = prepared.get("data") or []
        docs = [self._prepare_doc(d) for d in data if isinstance(d, dict)]
        if not docs:
            return {"success": True, "rows_inserted": 0, "message": "No data to import"}

        config = self._get_config(prepared)
        client = None
        try:
            client = self._get_client(config)
            coll = client[self._database_name(config)][collection]
            if mode == "replace":
                coll.delete_many({})
            try:
                result = coll.insert_many(docs, ordered=False)
                inserted = len(result.inserted_ids)
            except Exception as bulk_err:
                # Tolerate duplicate-key on CDC redelivery: count what landed.
                try:
                    from pymongo.errors import BulkWriteError
                except Exception:
                    BulkWriteError = ()  # type: ignore[assignment]
                if BulkWriteError and isinstance(bulk_err, BulkWriteError):
                    inserted = int(bulk_err.details.get("nInserted", 0))
                else:
                    raise
            return {"success": True, "rows_inserted": inserted}
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            self._close_client(client)

    def upsert_data(self, params: Dict = None) -> Dict[str, Any]:
        """Idempotent replace keyed on _id / key_fields (CDC insert + update)."""
        params = params or {}
        prepared = self.prepare_import_data(params)
        if not prepared.get("success"):
            return prepared
        collection = self._resolve_collection(prepared, params)
        if not collection:
            return {"success": False, "error": "Missing 'collection'/'table' parameter"}
        return self._upsert_nosql(collection, prepared.get("data") or [],
                                  self._key_fields(params, prepared), prepared)

    def _upsert_nosql(self, collection: str, data: List[Any], key_fields: List[str],
                      prepared: Dict[str, Any]) -> Dict[str, Any]:
        docs = [d for d in data if isinstance(d, dict)]
        if not docs:
            return {"success": True, "rows_upserted": 0, "message": "No data to upsert"}

        config = self._get_config(prepared)
        client = None
        try:
            from pymongo import ReplaceOne
            client = self._get_client(config)
            coll = client[self._database_name(config)][collection]
            self._ensure_key_index(coll, key_fields)

            ops = []
            skipped = 0
            for d in docs:
                doc = self._prepare_doc(d)
                flt = self._key_filter(doc, key_fields)
                if flt is None:
                    skipped += 1
                    continue
                ops.append(ReplaceOne(flt, doc, upsert=True))

            if not ops:
                return {"success": False,
                        "error": f"No records carried key field(s) {key_fields} for upsert"}

            result = coll.bulk_write(ops, ordered=False)
            rows = int((result.upserted_count or 0) + (result.matched_count or 0))
            out = {"success": True, "rows_upserted": rows}
            if skipped:
                out["skipped"] = skipped
            return out
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            self._close_client(client)

    def delete_data(self, params: Dict = None) -> Dict[str, Any]:
        """Delete documents by key field(s) (CDC delete)."""
        params = params or {}
        prepared = self.prepare_import_data(params)
        if not prepared.get("success"):
            return prepared
        collection = self._resolve_collection(prepared, params)
        if not collection:
            return {"success": False, "error": "Missing 'collection'/'table' parameter"}
        return self._delete_nosql(collection, prepared.get("data") or [],
                                  self._key_fields(params, prepared), prepared)

    def _delete_nosql(self, collection: str, data: List[Any], key_fields: List[str],
                      prepared: Dict[str, Any]) -> Dict[str, Any]:
        filters: List[Dict[str, Any]] = []
        for rec in data:
            keydoc = self._extract_key_doc(rec)
            if not isinstance(keydoc, dict):
                continue
            flt = self._key_filter(keydoc, key_fields)
            if flt is not None:
                filters.append(flt)

        if not filters:
            return {"success": True, "rows_deleted": 0, "message": "No deletable keys"}

        config = self._get_config(prepared)
        client = None
        try:
            client = self._get_client(config)
            coll = client[self._database_name(config)][collection]
            self._ensure_key_index(coll, key_fields)
            # Single key → one $in delete; composite key → $or of equality filters.
            if len(key_fields) == 1:
                k = key_fields[0]
                result = coll.delete_many({k: {"$in": [f[k] for f in filters]}})
            else:
                result = coll.delete_many({"$or": filters})
            return {"success": True, "rows_deleted": int(result.deleted_count)}
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            self._close_client(client)

    def drop_table(self, params: Dict = None) -> Dict[str, Any]:
        """Drop a collection — the reload-mode cleanup step. On run_mode=reload the
        orchestrator drops the destination so the next import rebuilds from scratch;
        for MongoDB that means dropping the collection. Dropping a non-existent
        collection is a no-op. The namespace param is ignored (the database comes
        from the connection config)."""
        params = params or {}
        config = self._get_config(params)
        raw = params.get("collection") or params.get("table") or ""
        collection = str(raw).strip()
        if "." in collection:
            collection = collection.split(".")[-1]
        if not collection:
            return {"success": False, "error": "Missing 'collection'/'table' parameter"}
        client = None
        try:
            client = self._get_client(config)
            db = client[self._database_name(config)]
            try:
                existed = collection in db.list_collection_names()
            except Exception:
                existed = False
            db.drop_collection(collection)
            return {"success": True, "dropped": existed}
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            self._close_client(client)


def create_http_app():
    """Create the FastAPI app exposing the MCP server over HTTP (Docker mode)."""
    from fastapi import FastAPI, HTTPException
    # Decimal crosses this boundary as a STRING, never a float. FastAPI's
    # jsonable_encoder maps Decimal -> float (ENCODERS_BY_TYPE), which silently
    # destroys precision on the way to the sink: a numeric 123456789012345678.5
    # arrives as 1.2345678901234568e+17, and the orchestrator writes that back
    # out as 123456789012345680 -- a changed value, with no error raised anywhere.
    # The stdio path already serialises with json.dumps(..., default=str), so
    # HTTP mode was the only lossy leg -- and it is the leg every containerised
    # deployment uses, which is why no stdio-based test could ever see it.
    from decimal import Decimal as _Decimal
    from fastapi.encoders import ENCODERS_BY_TYPE as _ENCODERS_BY_TYPE
    _ENCODERS_BY_TYPE[_Decimal] = str
    from pydantic import BaseModel
    from typing import Optional as _Optional

    runtime_version = os.getenv("MCP_CONNECTOR_VERSION") or os.getenv("CONNECTOR_VERSION") or "1.0.0"
    runtime_version = str(runtime_version).strip()
    if runtime_version and not runtime_version.startswith("v"):
        runtime_version = f"v{runtime_version}"

    app = FastAPI(
        title="MongoDB MCP Connector",
        description="Connector for MongoDB / Atlas — batch source + CDC via Debezium.",
        version=runtime_version or "1.0.0",
    )

    server = MongodbMCPServer()

    class MCPRequest(BaseModel):
        method: str
        params: _Optional[dict] = None

    @app.get("/health")
    async def health():
        return {"status": "healthy", "connector": server.connector_type, "version": runtime_version or "1.0.0"}

    @app.post("/mcp")
    async def mcp_handler(request: MCPRequest):
        import asyncio
        try:
            req_data = {"jsonrpc": "2.0", "id": 1, "method": request.method, "params": request.params or {}}

            def _handle():
                return MongodbMCPServer().handle_request(req_data)

            return await asyncio.to_thread(_handle)
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/capabilities")
    async def api_capabilities():
        return server.get_capabilities()

    @app.post("/test_connection")
    async def api_test_connection(params: _Optional[dict] = None):
        return server.test_connection(params or {})

    @app.post("/discover_schema")
    async def api_discover_schema(params: _Optional[dict] = None):
        return server.discover_schema(params or {})

    return app


if __name__ == "__main__":
    http_mode = os.getenv("MCP_HTTP_MODE", "false").lower() == "true"
    port = int(os.getenv("MCP_PORT", os.getenv("PORT", "8000")))
    if http_mode or os.getenv("DOCKER_CONTAINER"):
        import uvicorn
        app = create_http_app()
        logger.info(f"🚀 Starting MongoDB MCP Server in HTTP mode on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        MongodbMCPServer().run()
