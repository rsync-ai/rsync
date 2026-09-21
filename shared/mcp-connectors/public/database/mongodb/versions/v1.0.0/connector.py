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
import json
import logging
import time
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Any, List, Optional, Tuple

# Resolve the shared base_connector (see Dockerfile: it is copied to
# /app/shared/mcp-connectors and that dir is on PYTHONPATH). Fall back to the
# sibling directory for local (non-Docker) runs.
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
try:
    from base_connector import BaseMCPConnector
except ImportError:  # pragma: no cover - local dev fallback
    sys.path.insert(0, os.path.dirname(__file__))
    from base_connector import BaseMCPConnector

# Scope filter for server-level connections, shipped into the image via
# `COPY --from=shared namespace_filter.py` (see Dockerfile). Dev/test resolves
# it from the public/ root.
try:
    import namespace_filter  # noqa: E402
except ImportError:  # pragma: no cover - dev/test path
    sys.path.insert(0, os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..")))
    import namespace_filter  # noqa: E402

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


# --------------------------------------------------------------------------- #
# Document browse (`find`): request validation                                #
# --------------------------------------------------------------------------- #
#
# The Data Explorer's document mode sends a user-authored filter / projection /
# sort to `find` through the gateway. The gateway enforces this same allowlist
# before any network hop; it is enforced again here so the connector is safe when
# called directly. The two lists must stay in lockstep.
#
# An allowlist, never a denylist: an operator MongoDB adds later stays rejected
# until someone decides it is safe. Server-side JavaScript and arbitrary
# expressions ($where, $function, $accumulator, $expr) are why this exists.
FIND_QUERY_OPERATORS = frozenset({
    "$eq", "$ne", "$gt", "$gte", "$lt", "$lte", "$in", "$nin",  # comparison
    "$and", "$or", "$nor", "$not",                              # logical
    "$exists", "$type", "$elemMatch", "$size", "$all",          # element / array
    "$regex", "$options", "$mod",
})
# Top-level filter keys that are operators rather than field names.
FIND_TOP_LEVEL_OPERATORS = frozenset({"$and", "$or", "$nor"})
# Extended JSON wrappers a filter may use for BSON types plain JSON cannot express.
# Each must be the only key of its object. Decoded by _decode_find_extjson, NOT
# bson.json_util.loads, which would also decode $code (JavaScript), $binary,
# $regularExpression and legacy {$regex, $options} pairs.
FIND_EXTJSON_WRAPPERS = frozenset({"$oid", "$date", "$numberLong", "$numberDecimal"})

FIND_DEFAULT_LIMIT = 50
FIND_MAX_LIMIT = 500
FIND_MAX_TIME_MS = 15000
FIND_MAX_SKIP = 10000
FIND_MAX_FILTER_DEPTH = 20
FIND_MAX_FILTER_BYTES = 64 * 1024
FIND_MAX_SORT_KEYS = 5
FIND_MAX_PROJECTION_KEYS = 100
FIND_MAX_COLLECTION_BYTES = 120
FIND_MAX_DATABASE_BYTES = 64  # MongoDB database names are shorter than 64 bytes
FIND_MAX_CURSOR_CHARS = 4096
FIND_MAX_RESPONSE_BYTES = 5 * 1024 * 1024

_READ_PREFERENCES = {
    "primary": "PRIMARY",
    "primarypreferred": "PRIMARY_PREFERRED",
    "secondary": "SECONDARY",
    "secondarypreferred": "SECONDARY_PREFERRED",
    "nearest": "NEAREST",
}

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class FindRequestError(ValueError):
    """A rejected `find` request.

    `path` locates the problem (``filter.$or[1].$where``). Messages name paths and
    operators, never a filter VALUE: values are user data and must not be echoed
    into an error string that may be logged or shown elsewhere.
    """

    def __init__(self, code: str, message: str, path: str = ""):
        super().__init__(message)
        self.code = code
        self.path = path


def _find_path(parent: str, key: str) -> str:
    return f"{parent}.{key[:64]}"


def _validate_find_collection(raw: Any) -> str:
    name = raw.strip() if isinstance(raw, str) else ""
    if not name:
        raise FindRequestError("invalid_collection", "Missing 'collection' parameter", "collection")
    if len(name.encode("utf-8")) > FIND_MAX_COLLECTION_BYTES:
        raise FindRequestError(
            "invalid_collection", f"collection name exceeds {FIND_MAX_COLLECTION_BYTES} bytes", "collection")
    if "$" in name or "\x00" in name:
        raise FindRequestError("invalid_collection", "collection name must not contain '$' or NUL", "collection")
    if name.startswith("system."):
        raise FindRequestError("invalid_collection", "system collections cannot be browsed", "collection")
    return name


def _find_database(connector: Any, config: Dict[str, Any], requested: Any) -> str:
    """The database a find reads.

    A connection that names a database reads only it; a request naming another
    one is refused rather than silently redirected. A server-level connection
    (no database) needs the request's ``database``, which must pass the
    connection's scope filter.
    """
    configured = connector._database_name(config)
    wanted = requested.strip() if isinstance(requested, str) else ""
    if configured:
        if wanted and wanted != configured:
            raise FindRequestError("invalid_database", "database is outside this connection's scope", "database")
        return configured
    if not wanted:
        raise FindRequestError(
            "invalid_database", "This connection names no database: pick a database", "database")
    if (len(wanted.encode("utf-8")) > FIND_MAX_DATABASE_BYTES
            or any(ch in wanted for ch in '/\\. "$\x00')):
        raise FindRequestError("invalid_database", "database name is not valid", "database")
    try:
        connector._check_in_scope(config, wanted)
    except ValueError as e:
        raise FindRequestError("invalid_database", str(e), "database")
    return wanted


def _walk_find_filter(node: Any, path: str, depth: int) -> None:
    if not isinstance(node, (dict, list)):
        return
    if depth > FIND_MAX_FILTER_DEPTH:
        raise FindRequestError(
            "filter_too_deep", f"filter nesting exceeds {FIND_MAX_FILTER_DEPTH} levels", path)
    if isinstance(node, list):
        for i, item in enumerate(node):
            _walk_find_filter(item, f"{path}[{i}]", depth + 1)
        return
    wrapper = next((k for k in node if k in FIND_EXTJSON_WRAPPERS), None)
    if wrapper is not None:
        if len(node) != 1:
            raise FindRequestError(
                "invalid_filter", f"{wrapper} must be the only key in its object", _find_path(path, wrapper))
        return  # the wrapped value's shape is checked when it is decoded
    for key, value in node.items():
        if not isinstance(key, str) or "\x00" in key:
            raise FindRequestError("invalid_filter", "field names must be strings without NUL", path)
        child = _find_path(path, key)
        if key.startswith("$") and key not in FIND_QUERY_OPERATORS:
            raise FindRequestError("operator_not_allowed", f"operator {key[:64]} is not allowed", child)
        _walk_find_filter(value, child, depth + 1)


def _decode_find_date(value: Any) -> datetime:
    if isinstance(value, dict) and set(value) == {"$numberLong"} and isinstance(value["$numberLong"], str):
        value = int(value["$numberLong"])
    if isinstance(value, int) and not isinstance(value, bool):
        return _EPOCH + timedelta(milliseconds=value)
    if isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    raise ValueError("unsupported $date shape")


def _decode_find_wrapper(key: str, value: Any, path: str) -> Any:
    from bson import Decimal128, Int64, ObjectId
    try:
        if key == "$oid" and isinstance(value, str):
            return ObjectId(value)
        if key == "$numberLong" and isinstance(value, str):
            number = int(value)
            if -(2 ** 63) <= number < 2 ** 63:
                return Int64(number)
        if key == "$numberDecimal" and isinstance(value, str):
            return Decimal128(value)
        if key == "$date":
            return _decode_find_date(value)
    except Exception:  # InvalidId, ValueError, decimal.InvalidOperation, OverflowError
        pass
    raise FindRequestError("invalid_filter", f"malformed {key} value", path)


def _decode_find_extjson(node: Any, path: str) -> Any:
    if isinstance(node, list):
        return [_decode_find_extjson(v, f"{path}[{i}]") for i, v in enumerate(node)]
    if not isinstance(node, dict):
        return node
    if len(node) == 1:
        key = next(iter(node))
        if key in FIND_EXTJSON_WRAPPERS:
            return _decode_find_wrapper(key, node[key], _find_path(path, key))
    return {k: _decode_find_extjson(v, _find_path(path, k)) for k, v in node.items()}


def _validate_find_filter(flt: Any) -> Dict[str, Any]:
    """Check a filter against the allowlist and limits, then decode its Extended
    JSON wrappers. Returns the filter ready for pymongo."""
    if flt is None:
        return {}
    if not isinstance(flt, dict):
        raise FindRequestError("invalid_filter", "filter must be a JSON object", "filter")
    for key in flt:
        if isinstance(key, str) and key.startswith("$") and key not in FIND_TOP_LEVEL_OPERATORS:
            raise FindRequestError(
                "operator_not_allowed", f"{key[:64]} is not allowed at the top level of a filter",
                _find_path("filter", key))
    # Depth first: the walk stops at the limit, so an absurdly nested filter never
    # reaches json.dumps' own recursion limit.
    _walk_find_filter(flt, "filter", 1)
    try:
        size = len(json.dumps(flt, separators=(",", ":")).encode("utf-8"))
    except (TypeError, ValueError):
        raise FindRequestError("invalid_filter", "filter must be plain JSON", "filter")
    if size > FIND_MAX_FILTER_BYTES:
        raise FindRequestError("filter_too_large", f"filter exceeds {FIND_MAX_FILTER_BYTES // 1024} KB", "filter")
    return _decode_find_extjson(flt, "filter")


def _validate_find_projection(projection: Any) -> Optional[Dict[str, int]]:
    if projection is None or projection == {}:
        return None
    if not isinstance(projection, dict):
        raise FindRequestError("invalid_projection", "projection must be a JSON object", "projection")
    if len(projection) > FIND_MAX_PROJECTION_KEYS:
        raise FindRequestError(
            "invalid_projection", f"projection accepts at most {FIND_MAX_PROJECTION_KEYS} fields", "projection")
    out: Dict[str, int] = {}
    for key, value in projection.items():
        path = _find_path("projection", str(key))
        if not isinstance(key, str) or not key or "$" in key or "\x00" in key:
            raise FindRequestError(
                "invalid_projection", "projection field names must be non-empty and contain no '$'", path)
        if isinstance(value, bool):
            value = int(value)
        if not isinstance(value, int) or value not in (0, 1):
            raise FindRequestError("invalid_projection", "projection values must be 0 or 1", path)
        out[key] = value
    if len({v for k, v in out.items() if k != "_id"}) > 1:
        raise FindRequestError(
            "invalid_projection", "projection cannot mix inclusion and exclusion (except _id)", "projection")
    return out


def _validate_find_sort(sort: Any) -> List[tuple]:
    """Return the sort as ordered (field, 1|-1) pairs.

    Accepts an object or a list of [field, direction] pairs. The list form exists
    for callers whose maps do not preserve key order (a Go map[string]any does not),
    since sort order is the order of the keys.
    """
    if sort is None or sort == {} or sort == []:
        return []
    if isinstance(sort, dict):
        items = list(sort.items())
    elif isinstance(sort, list):
        items = []
        for i, pair in enumerate(sort):
            if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                raise FindRequestError("invalid_sort", "sort list entries must be [field, 1|-1] pairs", f"sort[{i}]")
            items.append((pair[0], pair[1]))
    else:
        raise FindRequestError("invalid_sort", "sort must be an object or a list of [field, direction] pairs", "sort")
    if len(items) > FIND_MAX_SORT_KEYS:
        raise FindRequestError("invalid_sort", f"sort accepts at most {FIND_MAX_SORT_KEYS} keys", "sort")
    out: List[tuple] = []
    seen = set()
    for key, direction in items:
        path = _find_path("sort", str(key))
        if not isinstance(key, str) or not key or "$" in key or "\x00" in key or key in seen:
            raise FindRequestError(
                "invalid_sort", "sort field names must be unique, non-empty and contain no '$'", path)
        if isinstance(direction, bool) or not isinstance(direction, int) or direction not in (1, -1):
            raise FindRequestError("invalid_sort", "sort direction must be 1 or -1", path)
        seen.add(key)
        out.append((key, direction))
    return out


def _find_int(value: Any, default: int, path: str) -> int:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        raise FindRequestError(f"invalid_{path}", f"{path} must be an integer", path)
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise FindRequestError(f"invalid_{path}", f"{path} must be an integer", path)
    if isinstance(value, float) and value != number:
        raise FindRequestError(f"invalid_{path}", f"{path} must be an integer", path)
    return number


def _export_cursor(last_id: Any) -> Any:
    """Keyset cursor export() hands back for the last _id of a page.

    A numeric _id (int / Int64 / float) stays a JSON number. str() would turn it
    into "10000", and {"_id": {"$gt": "10000"}} matches no numeric _id (BSON
    compares a string above every number), so paging silently stopped after the
    first page. Anything else (ObjectId, string, ...) keeps the str() form the
    executor already checkpoints; a 24-hex string resumes as an ObjectId.
    """
    if last_id is None:
        return None
    if isinstance(last_id, (int, float)) and not isinstance(last_id, bool):
        return last_id
    return str(last_id)


def _encode_find_cursor(last_id: Any) -> str:
    """Opaque keyset cursor for the last _id returned. Canonical Extended JSON keeps
    the _id's exact BSON type, so a 24-hex STRING _id resumes as a string and an
    ObjectId as an ObjectId (export's str() cursor cannot tell them apart)."""
    from bson import json_util
    return json_util.dumps({"_id": last_id}, json_options=json_util.CANONICAL_JSON_OPTIONS)


def _decode_find_cursor(cursor: Any) -> Any:
    from bson import json_util
    from bson.code import Code
    if not isinstance(cursor, str) or len(cursor) > FIND_MAX_CURSOR_CHARS:
        raise FindRequestError("invalid_cursor", "cursor is malformed", "cursor")
    try:
        doc = json_util.loads(cursor, json_options=json_util.CANONICAL_JSON_OPTIONS)
    except Exception:
        raise FindRequestError("invalid_cursor", "cursor is malformed", "cursor")
    if not isinstance(doc, dict) or set(doc) != {"_id"} or isinstance(doc["_id"], Code):
        raise FindRequestError("invalid_cursor", "cursor is malformed", "cursor")
    return doc["_id"]


def _find_read_preference(config: Dict[str, Any]):
    name = config.get("read_preference") or config.get("readPreference")
    if not name:
        return None
    attr = _READ_PREFERENCES.get(str(name).replace("_", "").replace("-", "").lower())
    if attr is None:
        raise FindRequestError(
            "invalid_config",
            "read_preference must be primary, primaryPreferred, secondary, secondaryPreferred or nearest",
            "config.read_preference")
    from pymongo import ReadPreference
    return getattr(ReadPreference, attr)


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

    @staticmethod
    def _is_real_namespace(ns: str) -> bool:
        """Mirror of the sink's ``isRealNamespace`` (kafka-sink-worker main.go).

        Empty and the literal ``"default"`` both mean "this pipeline has no real
        destination namespace" and MUST resolve identically, or a historical
        single-namespace pipeline would start routing somewhere new on upgrade.
        The sink already filters both in ``addNamespaceParam``; re-checking here
        keeps a hand-built call (tests, direct MCP use) on the same contract.
        """
        ns = (ns or "").strip()
        return bool(ns) and ns.lower() != "default"

    def _target_database(self, config: Dict[str, Any], params: Dict = None) -> str:
        """Resolve the database a DESTINATION write lands in.

        MongoDB is a single-namespace destination exactly like ClickHouse: the
        database IS the per-pipeline namespace analog. The sink bares the
        collection name and forwards the pipeline's ``destination_namespace``
        separately as ``namespace``/``db_or_schema`` (kafka-sink-worker
        ``addNamespaceParam``); honour that override, else fall back to the
        connection config.

        Until this existed every write path resolved ``config["database"]``
        unconditionally, so a pipeline with a locked ``destination_namespace``
        had it silently discarded and rows landed in the connection's database —
        which, when that database is also another pipeline's SOURCE, is
        cross-pipeline contamination that no row count or LAG metric can see.

        SOURCE reads (``discover_schema``, ``export``) deliberately keep using
        ``_database_name``: a destination namespace must never retarget a read.
        """
        ns = ""
        if params:
            ns = str(params.get("namespace") or params.get("db_or_schema") or "").strip()
        if self._is_real_namespace(ns):
            return ns
        db = self._database_name(config)
        if not db:
            # A server-level connection (no database) has nowhere to write unless
            # the pipeline names one: pymongo's own error for client[""] does not
            # say what to change.
            raise ValueError(
                "This connection names no database and the pipeline sent no destination "
                "namespace: set a destination database on the pipeline")
        return db

    def _check_in_scope(self, config: Dict[str, Any], db: str) -> None:
        """Raise ValueError unless ``db`` passes the connection's scope filter.

        Only server-level connections (no database named) carry a scope: the
        filter picks which databases discovery lists, and a read must not reach
        a database discovery would have hidden. MongoDB's own databases (admin,
        config, local) are always out of scope.
        """
        try:
            scope = namespace_filter.parse(config)
        except namespace_filter.NamespaceFilterError as e:
            raise ValueError(str(e))
        if not namespace_filter.allowed(db, scope, self._SYSTEM_DATABASES):
            raise ValueError("That database is outside this connection's scope")

    def _source_target(self, config: Dict[str, Any], name: str) -> Tuple[str, str]:
        """(database, collection) a SOURCE read of ``name`` targets.

        A connection that names a database reads only that database, and a
        db-qualified name keeps its last segment (the historical behaviour). A
        server-level connection reads ``<database>.<collection>``, split at the
        FIRST dot: a database name cannot contain a dot, a collection name can.
        Raises ValueError with a message safe to return to the caller.
        """
        name = (name or "").strip()
        configured = self._database_name(config)
        if configured:
            return configured, (name.split(".")[-1] if "." in name else name)
        db, sep, coll = name.partition(".")
        if not sep or not db.strip() or not coll.strip():
            raise ValueError(
                "This connection names no database: pass the collection as <database>.<collection>")
        db = db.strip()
        self._check_in_scope(config, db)
        return db, coll.strip()

    def _prepared_with_namespace(self, prepared: Dict, params: Dict) -> Dict:
        """Carry the destination namespace across ``prepare_import_data``.

        The shared ``prepare_import_data`` (base_connector) returns a FIXED key
        whitelist — success/config/table/data/mode/schema/database/row_count — so
        the sink's ``namespace``/``db_or_schema`` do NOT survive it. Every write
        path below resolves its database from ``prepared``, so without this the
        namespace would still be dropped even with ``_target_database`` wired in:
        a fix that looks complete and changes nothing.
        """
        if not prepared.get("success"):
            return prepared
        for key in ("namespace", "db_or_schema"):
            val = (params or {}).get(key)
            if val is not None and key not in prepared:
                prepared[key] = val
        return prepared

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
        """Ping the deployment and report its topology.

        Change streams (and therefore CDC) require a replica set or sharded
        cluster; a standalone mongod cannot be a CDC source. We surface that as a
        warning here (non-fatal for a batch connection test) so the failure is
        visible before a CDC pipeline is started.

        Topology comes from the ``hello`` reply (it needs no privileges), using the
        driver SDAM rules: ``setName`` means a replica-set member, ``msg ==
        "isdbgrid"`` means a mongos router. A mongos has no ``setName`` but streams
        fine, so ``is_replica_set`` alone must not be read as "standalone" — the
        orchestrator blocks a CDC start only when BOTH fields are explicitly false.
        When neither ``hello`` nor ``isMaster`` answers, both fields are omitted:
        the topology is unknown, and unknown must not block.

        With ``params["cdc_readiness"]`` set (the orchestrator's pre-migration
        assessment of a CDC pipeline) the reply also carries
        ``change_stream_access`` and, when the oplog is readable,
        ``oplog_window_hours`` — see :meth:`_cdc_readiness`. A plain connection
        test never opens a change stream.
        """
        config = self._get_config(params)
        client = None
        try:
            client = self._get_client(config)
            client.admin.command("ping")
            reply = None
            try:
                reply = client.admin.command("hello")
            except Exception:
                # Older servers: fall back to isMaster.
                try:
                    reply = client.admin.command("isMaster")
                except Exception:
                    pass
            result = {
                "success": True,
                "message": "Connection successful",
            }
            if isinstance(reply, dict):
                set_name = reply.get("setName")
                is_replica_set = bool(set_name)
                is_sharded_cluster = reply.get("msg") == "isdbgrid"
                result["is_replica_set"] = is_replica_set
                result["is_sharded_cluster"] = is_sharded_cluster
                if set_name:
                    result["replica_set"] = set_name
                if not is_replica_set and not is_sharded_cluster:
                    result["warning"] = (
                        "Connected, but this deployment is not a replica set. CDC "
                        "(change streams) requires a replica set or sharded cluster; "
                        "run rs.initiate() before starting a CDC pipeline."
                    )
            if (params or {}).get("cdc_readiness"):
                result.update(self._cdc_readiness(client, (params or {}).get("collections")))
            return result
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:
                    pass

    # Characters that cannot appear in a MongoDB database name.
    _FORBIDDEN_DB_CHARS = frozenset('/\\. "$*<>:|?')

    @classmethod
    def _change_stream_database(cls, collections: Any) -> Optional[str]:
        """The one database a CDC pipeline's change stream is scoped to, or None
        for a deployment-wide stream.

        Mirrors the llm-service ``_mongo_capture_scope`` rule that sets
        Debezium's ``capture.scope``: the stream is scoped to a database only
        when every selected collection is qualified ``db.collection`` with the
        same database. Any bare name, or two databases, means deployment scope —
        and a deployment-wide stream needs more privileges than a database one,
        so probing the wrong scope would report the wrong answer.
        """
        if isinstance(collections, str):
            entries = collections.split(",")
        elif isinstance(collections, (list, tuple)):
            entries = [str(e) for e in collections]
        else:
            return None
        dbs: List[str] = []
        for entry in (e.strip() for e in entries):
            if not entry:
                continue
            db, sep, coll = entry.partition(".")
            if not sep or not db or not coll or set(db) & cls._FORBIDDEN_DB_CHARS:
                return None
            if db not in dbs:
                dbs.append(db)
        return dbs[0] if len(dbs) == 1 else None

    def _cdc_readiness(self, client, collections: Any) -> Dict[str, Any]:
        """Read-only CDC checks for the pre-migration assessment.

        ``change_stream_access`` opens (and at once closes) a change stream at
        the scope Debezium will use, so a missing privilege shows up before the
        pipeline starts instead of as a failing connector. ``status`` is one of
        ok · unauthorized (code 13) · unsupported (40573, not a replica set) ·
        error (anything else, e.g. a network drop — never read as a verdict).

        ``oplog_window_hours`` is the time span of the oplog: how long CDC can
        be paused before its resume point is overwritten and it must re-snapshot.
        It is omitted when ``local.oplog.rs`` is not readable (a mongos, or a
        managed service that hides it) — unknown is not reported as short.
        """
        out: Dict[str, Any] = {}
        database = self._change_stream_database(collections)
        scope = "database" if database else "deployment"
        access: Dict[str, Any] = {"scope": scope}
        if database:
            access["database"] = database
        try:
            target = client[database] if database else client
            stream = target.watch(max_await_time_ms=1000)
            try:
                access["status"] = "ok"
            finally:
                stream.close()
        except Exception as e:  # pymongo OperationFailure carries .code
            code = getattr(e, "code", None)
            text = str(e)
            lowered = text.lower()
            if code == 13 or "not authorized" in lowered or "not allowed to do action" in lowered:
                access["status"] = "unauthorized"
            elif code == 40573 or "only supported on replica sets" in lowered:
                access["status"] = "unsupported"
            else:
                access["status"] = "error"
            if code is not None:
                access["error_code"] = code
            access["message"] = text[:300]
        out["change_stream_access"] = access
        hours = self._oplog_window_hours(client)
        if hours is not None:
            out["oplog_window_hours"] = hours
        return out

    @staticmethod
    def _oplog_window_hours(client) -> Optional[float]:
        """Hours between the oldest and newest oplog entries, or None."""
        try:
            oplog = client["local"]["oplog.rs"]
            first = next(iter(oplog.find({}, {"ts": 1}).sort("$natural", 1).limit(1)), None)
            last = next(iter(oplog.find({}, {"ts": 1}).sort("$natural", -1).limit(1)), None)
            if not first or not last:
                return None
            start = getattr(first.get("ts"), "time", None)
            end = getattr(last.get("ts"), "time", None)
            if start is None or end is None:
                return None
            return round(max(0, end - start) / 3600.0, 1)
        except Exception:
            return None

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
        # No database is a server-level connection: every database the login can
        # see, narrowed by the scope filter. A named database ignores the filter.
        if not self._database_name(config):
            try:
                namespace_filter.parse(config)
            except namespace_filter.NamespaceFilterError as e:
                errors.append(str(e))
        return {"valid": len(errors) == 0, "errors": errors, "warnings": []}

    def discover_schema(self, params: Dict = None) -> Dict[str, Any]:
        """List collections as selectable 'tables'.

        A connection that names a database lists that database. A server-level
        connection (no database) lists every database the login can see, minus
        MongoDB's own (admin, config, local), narrowed by the scope filter
        (namespace_filter_mode / namespace_filter_patterns). An invalid filter
        fails discovery; a filter that matches nothing is a warning.

        Each collection is a table whose 'schema' is its MongoDB database name and
        whose primary key is always _id. Columns are inferred from a small sample
        of documents (union of top-level field names). Row counts are exact
        (countDocuments) when requested.

        Sampling and counting cost a round trip or two per collection, so they
        share a time budget (params.enrich_budget_seconds, default 15s) that keeps
        discovery inside the orchestrator's 30s call timeout. Collections past
        the budget are still listed, with only _id and discovery_status "partial".
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
        try:
            budget_s = float(params.get("enrich_budget_seconds", 15))
        except (TypeError, ValueError):
            budget_s = 15.0

        scope = None
        if not db_name:
            try:
                scope = namespace_filter.parse(config)
            except namespace_filter.NamespaceFilterError as e:
                result["overall_status"] = "failed"
                result["warnings_messages"].append(str(e))
                return result

        client = None
        try:
            client = self._get_client(config)
            try:
                result["database_version"] = client.server_info().get("version")
            except Exception:
                pass

            # (database, collection) pairs, sorted by database then collection.
            pairs: List[Tuple[str, str]] = []
            if db_name:
                names = [n for n in client[db_name].list_collection_names() if not n.startswith("system.")]
                pairs = [(db_name, n) for n in sorted(names)]
            else:
                applied = namespace_filter.apply(
                    sorted(str(n) for n in client.list_database_names() if n),
                    scope, self._SYSTEM_DATABASES)
                if applied.warning:
                    result["warnings_messages"].append(applied.warning)
                for dbn in applied.kept:
                    try:
                        names = client[dbn].list_collection_names()
                    except Exception as e:
                        result["warnings_messages"].append(f"{dbn}: listing collections failed: {e}")
                        continue
                    pairs.extend((dbn, n) for n in sorted(names) if not n.startswith("system."))
            result["total_tables_available"] = len(pairs)

            deadline = time.monotonic() + budget_s
            unenriched = 0
            for table_db, coll_name in pairs[:max_tables]:
                # Warnings name the collection alone when the connection names its
                # database (the historical text), qualified when it spans several.
                label = coll_name if db_name else f"{table_db}.{coll_name}"
                left_ms = int((deadline - time.monotonic()) * 1000)
                if left_ms <= 0:
                    unenriched += 1
                    result["tables"].append({
                        "name": coll_name,
                        "schema": table_db,
                        "discovery_status": "partial",
                        "columns": ([{"name": "_id", "type": "string", "nullable": False}]
                                    if include_columns else []),
                        "primary_keys": ["_id"],
                        "primary_key": ["_id"],
                    })
                    continue
                coll = client[table_db][coll_name]
                columns: List[Dict[str, Any]] = []
                if include_columns:
                    seen: Dict[str, str] = {}
                    try:
                        for doc in coll.find(limit=sample_size, max_time_ms=left_ms):
                            for key, val in doc.items():
                                if key not in seen:
                                    seen[key] = _infer_type(val)
                    except Exception as e:
                        result["warnings_messages"].append(f"{label}: sample failed: {e}")
                    # _id first, then the rest in first-seen order.
                    if "_id" not in seen:
                        seen = {"_id": "string", **seen}
                    for field, tname in seen.items():
                        columns.append({"name": field, "type": tname, "nullable": field != "_id"})

                table_obj: Dict[str, Any] = {
                    "name": coll_name,
                    "schema": table_db,
                    "discovery_status": "complete",
                    "columns": columns,
                    "primary_keys": ["_id"],
                    "primary_key": ["_id"],
                }
                if include_row_counts:
                    # An exact count scans the collection; bound it by the budget
                    # and fall back to the metadata estimate.
                    left_ms = max(1, int((deadline - time.monotonic()) * 1000))
                    try:
                        table_obj["row_count"] = coll.count_documents({}, maxTimeMS=left_ms)
                        table_obj["is_exact_count"] = True
                    except Exception:
                        try:
                            table_obj["row_count"] = coll.estimated_document_count()
                        except Exception:
                            table_obj["row_count"] = None
                        table_obj["is_exact_count"] = False
                result["tables"].append(table_obj)

            if unenriched:
                result["warnings_messages"].append(
                    f"{unenriched} of {len(result['tables'])} collections listed without "
                    f"sampled fields or counts (discovery budget {budget_s:g}s)")
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

    # Databases MongoDB keeps for itself; never user data.
    _SYSTEM_DATABASES = frozenset({"admin", "config", "local"})

    def list_namespaces(self, params: Dict = None) -> Dict[str, Any]:
        """List the databases this login can see, without MongoDB's own (admin,
        config, local): the level metadata.json's namespace_model.table_namespace
        names. A login without the listDatabases privilege gets the databases it
        has privileges on (MongoDB 4.0.5+). "current" is the database the
        connection names, "" when it names none.
        """
        config = self._get_config(params or {})
        client = None
        try:
            client = self._get_client(config)
            names = [n for n in client.list_database_names() if n not in self._SYSTEM_DATABASES]
            return {
                "success": True,
                "namespaces": sorted({str(n) for n in names if n}),
                "current": self._database_name(config),
            }
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": f"Listing namespaces failed: {e}"}
        finally:
            self._close_client(client)

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
                {"name": "find", "method": "mongodb_find", "type": "source",
                 "description": "Read-only document browse for the Data Explorer (allowlisted filter, "
                                "projection, sort, keyset/skip paging, Relaxed Extended JSON)"},
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
        `db.collection`. With a database named on the connection the last segment
        is the collection; on a server-level connection the name must be
        `<database>.<collection>` (see _source_target). Documents are
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
        if not str(raw_coll).strip():
            return {"success": False, "error": "Missing 'collection'/'table' parameter"}
        try:
            db_name, collection = self._source_target(config, str(raw_coll))
        except ValueError as e:
            return {"success": False, "error": str(e)}
        if not collection:
            return {"success": False, "error": "Missing 'collection'/'table' parameter"}

        try:
            limit = int(prepared.get("limit", params.get("limit", self.max_batch_size)) or self.max_batch_size)
        except Exception:
            limit = self.max_batch_size
        limit = max(1, min(limit, self.max_batch_size))
        cursor_val = prepared.get("cursor", params.get("cursor"))

        client = None
        try:
            client = self._get_client(config)
            coll = client[db_name][collection]

            query: Dict[str, Any] = {}
            if cursor_val not in (None, ""):
                # Resume after the last _id. Prefer ObjectId comparison when the
                # cursor is a 24-hex string; otherwise compare as the raw value.
                # A numeric _id arrives as a JSON number (see _export_cursor), so
                # it compares against int/long/double _ids, not as a string.
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
                next_cursor = _export_cursor(last_id)

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
    # Document browse (Data Explorer)                                     #
    # ------------------------------------------------------------------ #
    def find(self, params: Dict = None) -> Dict[str, Any]:
        """Read-only document browse for the Data Explorer's document mode.

        Runs one bounded find against a collection: allowlisted filter operators
        (FIND_QUERY_OPERATORS), 0/1 projection, a sort of up to 5 keys, at most 500
        documents, maxTimeMS on the server, and a 5 MB response budget. Documents
        come back as Relaxed Extended JSON so BSON types survive ({"$oid": ...},
        {"$date": ...}) and a copied value pastes straight back into a filter.

        Paging: with no sort, or a sort on _id alone, paging is keyset on _id. Pass
        back `next_cursor`; it is stable under concurrent writes. Any other sort
        pages by skip: pass back `next_skip`, capped at FIND_MAX_SKIP.

        Deliberately separate from `export`: pipelines depend on export's contract
        (JSON-flattened rows, 10k batches, no filter), and nothing here changes it.
        """
        params = params or {}
        started = time.monotonic()
        try:
            raw_config = params.get("config")
            if isinstance(raw_config, str):
                try:
                    raw_config = json.loads(raw_config)
                except ValueError:
                    raise FindRequestError("invalid_config", "config is not valid JSON", "config")
            config = self._get_config({"config": raw_config})
            db_name = _find_database(self, config, params.get("database"))
            read_preference = _find_read_preference(config)

            collection = _validate_find_collection(params.get("collection") or params.get("table"))
            user_filter = _validate_find_filter(params.get("filter"))
            projection = _validate_find_projection(params.get("projection"))
            sort = _validate_find_sort(params.get("sort"))
            limit = max(1, min(_find_int(params.get("limit"), FIND_DEFAULT_LIMIT, "limit"), FIND_MAX_LIMIT))
            max_time_ms = max(1, min(_find_int(params.get("max_time_ms"), FIND_MAX_TIME_MS, "max_time_ms"),
                                     FIND_MAX_TIME_MS))
            skip = _find_int(params.get("skip"), 0, "skip")
            if not 0 <= skip <= FIND_MAX_SKIP:
                raise FindRequestError("invalid_skip", f"skip must be between 0 and {FIND_MAX_SKIP}", "skip")
            cursor_raw = params.get("cursor")
            has_cursor = cursor_raw not in (None, "")

            keyset = not sort or (len(sort) == 1 and sort[0][0] == "_id")
            if keyset:
                if skip:
                    raise FindRequestError(
                        "invalid_skip", "skip applies only to a custom sort; page with cursor instead", "skip")
                direction = sort[0][1] if sort else 1
                query = user_filter
                if has_cursor:
                    bound = {"_id": {"$gt" if direction == 1 else "$lt": _decode_find_cursor(cursor_raw)}}
                    query = {"$and": [user_filter, bound]} if user_filter else bound
                sort_keys = [("_id", direction)]
            else:
                if has_cursor:
                    raise FindRequestError(
                        "invalid_cursor", "cursor applies only to the default _id sort; page with skip instead",
                        "cursor")
                query = user_filter
                # _id tiebreaker: without a total order, skip pages can repeat or drop documents.
                sort_keys = sort if any(k == "_id" for k, _ in sort) else sort + [("_id", 1)]

            # Keyset paging needs every returned _id, so a projection hiding _id is
            # widened for the query and _id is dropped from the output instead.
            hide_id = bool(projection) and projection.get("_id") == 0
            query_projection = projection
            if keyset and hide_id:
                query_projection = {k: v for k, v in projection.items() if k != "_id"} or None
        except FindRequestError as e:
            return {"success": False, "error": str(e), "error_code": e.code, "path": e.path}

        from pymongo.errors import ConnectionFailure, ExecutionTimeout, OperationFailure
        client = None
        try:
            client = self._get_client(config)
            coll = client[db_name][collection]
            if read_preference is not None:
                coll = coll.with_options(read_preference=read_preference)
            cursor = (coll.find(query, query_projection)
                      .sort(sort_keys).skip(skip).limit(limit + 1).max_time_ms(max_time_ms))
            raw_docs = list(cursor)
        except ExecutionTimeout:
            return {"success": False, "error_code": "query_timeout",
                    "error": f"Query exceeded {max_time_ms} ms. Add a filter on an indexed field."}
        except OperationFailure as e:
            # The server's errmsg can quote filter values, so only the code name is surfaced.
            code_name = (getattr(e, "details", None) or {}).get("codeName") or f"code {getattr(e, 'code', '?')}"
            return {"success": False, "error_code": "query_failed", "mongo_code": getattr(e, "code", None),
                    "error": f"MongoDB rejected the query ({code_name})"}
        except ConnectionFailure as e:
            return {"success": False, "error_code": "connection_failed", "error": f"{type(e).__name__}: {str(e)[:300]}"}
        except Exception as e:
            return {"success": False, "error_code": "query_failed", "error": f"find failed ({type(e).__name__})"}
        finally:
            self._close_client(client)

        from bson import json_util
        has_more = len(raw_docs) > limit
        documents: List[Dict[str, Any]] = []
        columns: List[str] = []
        warnings: List[str] = []
        used_bytes = 0
        truncated_bytes = False
        last_id = None
        for position, doc in enumerate(raw_docs[:limit]):
            doc_id = doc.get("_id")
            if hide_id:
                doc = {k: v for k, v in doc.items() if k != "_id"}
            text = json_util.dumps(doc, json_options=json_util.RELAXED_JSON_OPTIONS)
            size = len(text.encode("utf-8"))
            if used_bytes + size > FIND_MAX_RESPONSE_BYTES:
                if documents:
                    truncated_bytes = has_more = True
                    break
                # One document larger than the whole budget: return only its _id so
                # paging still advances, and say how to see the rest.
                text = json_util.dumps({"_id": doc_id}, json_options=json_util.RELAXED_JSON_OPTIONS)
                size = len(text.encode("utf-8"))
                truncated_bytes = True
                warnings.append(
                    f"Document {position + 1} exceeds the {FIND_MAX_RESPONSE_BYTES // (1024 * 1024)} MB response "
                    "budget; only its _id is shown. Use a projection to view selected fields.")
            parsed = json.loads(text)
            documents.append(parsed)
            used_bytes += size
            last_id = doc_id
            for key in parsed:
                if key not in columns:
                    columns.append(key)
        if "_id" in columns:
            columns.remove("_id")
            columns.insert(0, "_id")

        next_cursor = None
        next_skip = None
        if has_more:
            if keyset:
                next_cursor = _encode_find_cursor(last_id) if last_id is not None else None
            elif skip + len(documents) <= FIND_MAX_SKIP:
                next_skip = skip + len(documents)
            else:
                warnings.append(
                    f"Paging with a custom sort stops after {FIND_MAX_SKIP} documents; narrow the filter to see more.")

        return {
            "success": True,
            "collection": collection,
            "documents": documents,
            "columns": columns,
            "returned": len(documents),
            "has_more": has_more,
            "paging_mode": "keyset" if keyset else "skip",
            "next_cursor": next_cursor,
            "next_skip": next_skip,
            "execution_time_ms": int((time.monotonic() - started) * 1000),
            "truncated_bytes": truncated_bytes,
            "warnings": warnings,
        }

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
        prepared = self._prepared_with_namespace(self.prepare_import_data(params), params)
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
            coll = client[self._target_database(config, prepared)][collection]
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
        prepared = self._prepared_with_namespace(self.prepare_import_data(params), params)
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
            coll = client[self._target_database(config, prepared)][collection]
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
        prepared = self._prepared_with_namespace(self.prepare_import_data(params), params)
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
            coll = client[self._target_database(config, prepared)][collection]
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
        collection is a no-op.

        The drop MUST resolve the same database the write paths resolve — the sink
        forwards the namespace here unconditionally (``addNamespaceParam(dropArgs,
        sm.DBOrSchema)``), so a drop that ignored it would clear the connection's
        database while the data lived under the pipeline's namespace: reload mode
        would then accumulate duplicates forever, having "cleaned" the wrong place.
        """
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
            db = client[self._target_database(config, params)]
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
