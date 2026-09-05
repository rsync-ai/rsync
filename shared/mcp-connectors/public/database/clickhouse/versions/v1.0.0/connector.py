#!/usr/bin/env python3
"""
ClickHouse MCP Connector — first-class batch SOURCE + DESTINATION (OLAP warehouse).

HAND-BUILT (not tool-generator output). ClickHouse speaks its own HTTP/native
protocol, so this connector rides the official pure-Python ``clickhouse-connect``
driver (HTTP interface, port 8123) — NOT the DB-API path (there is no psycopg2 /
information_schema-style cursor) and NOT a warehouse adapter.

It is modeled on the hand-curated ``postgresql`` connector (the live-proven
relational destination), because ClickHouse participates in the SAME generic
batch write path: the kafka-mcp-sink probes ``get_capabilities`` for
``supports_ddl`` + ``auto_create_destination_tables``, then calls
``ensure_table`` → ``upsert_data`` (NOT the warehouse ``load``/``merge`` path
that redshift/snowflake use). So this connector implements the full relational
tool set: test_connection / validate_config / get_capabilities / discover_schema
/ export / ensure_table / upsert_data / import_data / drop_table.

ClickHouse-specific choices, because "just copy postgresql" would be wrong:

  * DDL — every ClickHouse table needs an explicit engine + sort key. ``ensure_table``
    creates ``ReplacingMergeTree ORDER BY (<keys>)`` when key fields are known
    (so reruns/upserts converge to one row per key) and ``MergeTree ORDER BY
    tuple()`` otherwise. Non-key columns are ``Nullable(T)`` so NULLs from the
    source never fail the INSERT; key columns are non-Nullable (ORDER BY keys
    must be non-Nullable). Identifiers are backtick-quoted (not "double").

  * WRITES — rows arrive as JSON-decoded scalars (timestamps/dates/decimals are
    strings after the Go executor marshals them). The robust path is a raw
    ``INSERT ... FORMAT JSONEachRow`` with ``date_time_input_format=best_effort``
    so the ClickHouse server coerces string timestamps/numbers into the column
    types. Nested dict/list values are JSON-stringified (json columns are stored
    as String). ``import_data`` is a plain append; ``upsert_data`` inserts then
    ``OPTIMIZE TABLE ... FINAL`` to collapse duplicates on the sort key for keyed
    tables (best-effort — never fails the write if OPTIMIZE is unavailable).

  * READS — ``export`` is offset-paginated by default (keyset when a cursor
    column is supplied, verbatim single-page for a raw query) so large tables
    resume to EOF via ``next_cursor`` / ``has_more`` instead of capping at one
    ``LIMIT``. Result values are coerced to JSON-safe scalars (datetime/date →
    ISO, Decimal/UUID/IP → str) for the cross-connector JSON hop.

Identifier safety: table/column/cursor identifiers are validated against a plain
identifier allowlist before interpolation (there is no bind form for identifiers);
the ``db`` value and keyset cursor values are bound/escaped.
"""

import hashlib
import ipaddress
import json
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

# base_connector lives next to this file in the Docker image; in the repo we walk
# up so tests can import offline (clickhouse_connect is imported lazily in _connect).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_d = os.path.dirname(os.path.abspath(__file__))
for _ in range(8):
    if os.path.exists(os.path.join(_d, "base_connector.py")):
        sys.path.insert(0, _d)
    _d = os.path.dirname(_d)

from base_connector import (  # noqa: E402
    BaseMCPConnector,
    DestinationLoadMixin,
    DestinationLoadSpec,
)

# Canonical->DDL authority (single source of truth), shipped into the image via
# `COPY --from=shared canonical_types.py` (see Dockerfile). Dev/test resolves it
# from the public/ root.
try:
    from canonical_types import canonical_to_ddl, canonicalize_type  # noqa: E402
except ImportError:  # pragma: no cover - dev/test path
    sys.path.insert(0, os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..")))
    from canonical_types import canonical_to_ddl, canonicalize_type  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Synthetic-PK columns for PK-less sources (mirrors postgresql's fallback so a
# reload of the same source row is idempotent). Kept in lockstep with the sink,
# which computes the row hash the same way.
SYNTHETIC_PK_COL = "_rsync_row_hash"
SYNTHETIC_TS_COL = "_rsync_synced_at"


def _compute_row_hash(row: Dict[str, Any], source_cols: List[str]) -> str:
    """sha256 over canonical JSON of the source columns only (kept byte-for-byte
    in lockstep with mysql/oracle/sqlserver so the same PK-less row hashes to the
    same key regardless of which connector wrote it)."""
    payload = {c: row.get(c) for c in sorted(source_cols) if not c.startswith("_rsync_")}
    try:
        serialised = json.dumps(payload, sort_keys=True, default=str)
    except (TypeError, ValueError):
        serialised = str(sorted(payload.items()))
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()


def _is_local_db_host(host: str) -> bool:
    """True iff ``host`` is local/docker-internal (plaintext is acceptable by
    default); False for a remote host (TLS/HTTPS by default). Mirrors the mysql
    host-aware TLS classifier so the two stay in lockstep.

      * empty / localhost / 127.0.0.1 / ::1 / host.docker.internal → local
      * IP literal (``[]`` stripped): local iff loopback / private / link-local
        or CGNAT 100.64.0.0/10 (RFC 6598); any other IP → remote
      * hostname: bare/dotless (docker service, k8s short name) → local;
        dotted (fully-qualified) → remote
    """
    h = str(host or "").strip().lower()
    if h in ("", "localhost", "127.0.0.1", "::1", "host.docker.internal"):
        return True
    h = h.strip("[]")  # unwrap an IPv6 literal, e.g. [::1] / [2606:4700::1111]
    try:
        ip = ipaddress.ip_address(h)
    except ValueError:
        # Not an IP literal → treat as a hostname. Dotless names resolve on the
        # local/container network; a dotted FQDN is remote.
        return "." not in h
    if ip.is_loopback or ip.is_private or ip.is_link_local:
        return True
    # CGNAT / RFC 6598 shared address space is not flagged is_private on older
    # Python, so classify it explicitly as local.
    return ip.version == 4 and ip in ipaddress.ip_network("100.64.0.0/10")


class ClickHouseMCPServer(DestinationLoadMixin, BaseMCPConnector):
    """MCP Server for ClickHouse (HTTP interface via clickhouse-connect)."""

    _IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    # Canonical source-type vocabulary → ClickHouse DDL type. The canonical
    # names are the ones discover_schema emits across all source connectors
    # (see postgresql _SQL_TO_CANONICAL): string / integer / number / boolean /
    # timestamp / date / time / json / binary / decimal. Unknown → String so a
    # new source scalar never blocks CREATE TABLE.
    _CANONICAL_TO_CH = {
        "string":    "String",
        "integer":   "Int64",
        "number":    "Float64",
        "boolean":   "Bool",
        "timestamp": "DateTime64(3)",
        "date":      "Date32",
        "time":      "String",              # ClickHouse has no standalone time type
        "json":      "String",              # store JSON payloads as String
        "binary":    "String",              # ClickHouse String is byte-safe
        "decimal":   "Decimal(38, 9)",      # mirrors PG NUMERIC(38,9) / MySQL DECIMAL(38,9)
        "uuid":      "String",              # canonical collapses uuid→string; defensive
    }

    def __init__(self):
        super().__init__()

        self.connector_type = "clickhouse"
        self.connector_category = "relational_db"  # rides the relational sink path like redshift (NOT the warehouse load/merge adapter used by snowflake/bigquery)
        self.supports_source = True
        self.supports_destination = True
        self.supports_cdc = False

        # Gate the sink's auto-create on THIS (destination) connector's DDL
        # capability, exactly like mysql/postgresql. Without both flags the
        # kafka-mcp-sink skips ensure_table and every INSERT hits a missing table.
        self.supports_ddl = True
        self.auto_create_destination_tables = True

        self.supported_formats = ["json", "jsonl", "csv"]
        self.supported_modalities = ["structured"]
        self.max_batch_size = 100000  # ClickHouse favours large INSERT batches

        # Default bulk = INSERT (JSONEachRow); "merge" = ReplacingMergeTree
        # convergence via OPTIMIZE FINAL (ClickHouse has no atomic UPSERT).
        self.load_spec = DestinationLoadSpec(
            load_method="insert",
            merge_method="replacing_merge_tree",
            supports_staging=False,
            max_batch_rows=100000,
        )

        self.log("ClickHouse MCP Server initialized")

    # =========================================================================
    # CONFIG + AUTH (basic host/port/db/user/password)
    # =========================================================================
    def _get_config(self, params: Dict) -> Dict:
        """Extract connection config, filling missing values from CLICKHOUSE_* env."""
        config = dict(params.get("config", params) if params else {})
        env_map = {
            "host": "CLICKHOUSE_HOST",
            "port": "CLICKHOUSE_PORT",
            "database": "CLICKHOUSE_DATABASE",
            "user": "CLICKHOUSE_USER",
            "password": "CLICKHOUSE_PASSWORD",
            "secure": "CLICKHOUSE_SECURE",
        }
        for key, env in env_map.items():
            if config.get(key) in (None, ""):
                v = os.getenv(env)
                if v not in (None, ""):
                    config[key] = v
        # `username` is an accepted alias for `user`.
        if not config.get("user") and config.get("username"):
            config["user"] = config["username"]

        # Secure-by-default TLS. The ClickHouse HTTP interface on :8123 is
        # PLAINTEXT: HTTP basic-auth credentials + every exported row travel in
        # cleartext to any on-path observer. So a REMOTE host with no explicit
        # secure/ssl/tls key defaults to HTTPS (secure=True, port 8443), mirroring
        # the mysql/postgresql host-aware TLS upgrade. LOCAL/docker-internal hosts
        # keep plaintext. Explicit opt-out always wins: secure=false or a
        # disable-family value (disable/false/off/0/no/none) forces plaintext.
        # The port only flips 8123→8443 when secure is being DEFAULTED on and the
        # user set no explicit port — an explicit port is never clobbered.
        _explicit_secure = None
        for _k in ("secure", "ssl", "tls"):
            if config.get(_k) not in (None, ""):
                _explicit_secure = config.get(_k)
                break
        if _explicit_secure is not None:
            secure = str(_explicit_secure).strip().lower() not in (
                "disable", "disabled", "false", "off", "0", "no", "none")
        else:
            secure = not _is_local_db_host(config.get("host"))
        config["secure"] = secure

        # Sensible ClickHouse defaults (secure-aware port; never clobbers an
        # explicit port set via params or CLICKHOUSE_PORT).
        config.setdefault("port", 8443 if secure else 8123)
        config.setdefault("database", "default")
        config.setdefault("user", "default")
        return config

    @staticmethod
    def _as_bool(v: Any) -> bool:
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("1", "true", "yes", "on")

    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        if not params:
            return {"valid": False, "errors": ["No configuration provided"]}
        config = self._get_config(params)
        errors: List[str] = []
        warnings: List[str] = []
        if not config.get("host"):
            errors.append("Missing required field: host")
        if not config.get("user"):
            errors.append("Missing required field: user")
        if not config.get("database"):
            warnings.append("No database specified; defaulting to 'default'")
        if config.get("password") in (None, ""):
            warnings.append("No password provided; assuming a password-less ClickHouse user")
        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    # =========================================================================
    # DRIVER SEAM (the only place clickhouse_connect is touched)
    # =========================================================================
    def _connect(self, config: Dict[str, Any]):
        """Open a clickhouse-connect Client (HTTP interface). Swapped by tests."""
        try:
            import clickhouse_connect  # type: ignore
        except Exception as e:  # pragma: no cover - import guard
            raise Exception(
                f"clickhouse-connect is not installed: {e}. Install clickhouse-connect."
            )
        return clickhouse_connect.get_client(
            host=config.get("host"),
            port=int(config.get("port") or 8123),
            username=config.get("user") or "default",
            password=config.get("password") or "",
            database=config.get("database") or "default",
            secure=self._as_bool(config.get("secure")),
            connect_timeout=int(config.get("connect_timeout") or 10),
            send_receive_timeout=int(config.get("query_timeout") or 300),
            query_limit=0,  # no implicit row cap; export/discover page explicitly
        )

    # =========================================================================
    # IDENTIFIER SAFETY (no bind form for identifiers in SQL)
    # =========================================================================
    @classmethod
    def _safe_identifier(cls, name: str) -> Optional[str]:
        """Return ``name`` iff it is a plain identifier, else None. Stripping
        quotes is NOT sanitization, so unsafe values are rejected outright."""
        n = (name or "").strip().strip("`").strip()
        return n if cls._IDENTIFIER_RE.match(n) else None

    def _resolve_db_table(self, config: Dict[str, Any], table: str,
                          params: Dict = None) -> Tuple[str, str]:
        """Resolve (database, table) for a write/read target.

        ClickHouse is a single-namespace destination: the sink strips a leading
        ``schema.``/``db.`` qualifier and forwards the real namespace separately
        as ``namespace``/``db_or_schema`` (see kafka-sink ``addNamespaceParam``).
        Honour that override, else fall back to the connection database.
        """
        raw = (table or "").strip().strip("`")
        if not raw:
            raise ValueError("Missing table")
        parts = raw.split(".")
        if len(parts) == 2:
            db, tbl = parts[0], parts[1]
        elif len(parts) == 1:
            tbl = parts[0]
            db = ""
        else:
            raise ValueError(f"Invalid table reference: {table}")

        ns = ""
        if params:
            ns = str(params.get("namespace") or params.get("db_or_schema") or "").strip()
        if not db:
            db = ns or (config.get("database") or "default")

        s_db = self._safe_identifier(db)
        s_tbl = self._safe_identifier(tbl)
        if not s_db or not s_tbl:
            raise ValueError(f"Unsafe table identifier: {table}")
        return s_db, s_tbl

    def _qualify(self, db: str, table: str) -> str:
        return f"`{db}`.`{table}`"

    # =========================================================================
    # TYPE MAPPING
    # =========================================================================
    def _ch_ddl_type(self, canonical: str) -> str:
        """Canonical source type → ClickHouse DDL type. Preserves an explicit
        decimal(p,s) so source scale survives; unknown → String.

        Delegated to the shared canonical->DDL authority (single source of truth
        across every sink). Behavior-preserving: returns a BARE ClickHouse type
        so the caller's ``Nullable(...)`` wrapping for non-key columns still
        applies (ORDER BY key columns stay non-Nullable). decimal(p,s) still
        maps to ``Decimal(p, s)``; ``number`` stays ``Float64`` (not Decimal).
        """
        return canonical_to_ddl("clickhouse", canonical)

    @staticmethod
    def _canonical_from_ch(ch_type: str) -> str:
        """ClickHouse column type → canonical vocabulary for discover_schema.

        Delegates to the shared dialect-scoped ``canonicalize_type`` authority
        (canonical_types.py) so ingress uses the SAME map as the discovery
        contract test + every other connector — one authority, no per-connector
        drift. This fixes the wide-integer bug the old local prefix-matcher had:
        it collapsed EVERY int/uint via ``startswith("int"|"uint")`` to canonical
        ``integer``, so ``UInt64`` (up to ~1.8e19) and 128/256-bit ints overflow
        the destination BIGINT. The shared ``_CLICKHOUSE_CANONICAL`` maps
        ``UInt64`` → decimal (fits NUMERIC(38,9)) and 128/256-bit → string
        (exceed NUMERIC(38)) to stay lossless. Nullable()/LowCardinality()
        wrappers are stripped inside the authority. Unknown → string.
        """
        return canonicalize_type(ch_type, dialect="clickhouse")

    @staticmethod
    def _jsonify(v: Any) -> Any:
        """Coerce a ClickHouse-returned value to a JSON-serializable scalar for
        the cross-connector hop (datetime/date → ISO, Decimal/UUID/IP → str)."""
        import datetime as _dt
        import decimal as _dec
        import uuid as _uuid
        if v is None or isinstance(v, (str, int, float, bool)):
            return v
        if isinstance(v, (_dt.datetime, _dt.date)):
            return v.isoformat()
        if isinstance(v, _dt.time):
            return v.isoformat()
        if isinstance(v, _dec.Decimal):
            return str(v)
        if isinstance(v, _uuid.UUID):
            return str(v)
        if isinstance(v, (bytes, bytearray)):
            try:
                return v.decode("utf-8")
            except Exception:
                import base64
                return base64.b64encode(bytes(v)).decode("ascii")
        if isinstance(v, dict):
            return {str(k): ClickHouseMCPServer._jsonify(val) for k, val in v.items()}
        if isinstance(v, (list, tuple)):
            return [ClickHouseMCPServer._jsonify(x) for x in v]
        return str(v)

    @staticmethod
    def _json_for_insert(v: Any) -> Any:
        """Coerce a value for JSONEachRow insert. Nested dict/list values become
        JSON strings (json columns are stored as String); scalars pass through."""
        if isinstance(v, (dict, list)):
            return json.dumps(v, default=str)
        return v

    # =========================================================================
    # CORE OPS
    # =========================================================================
    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        config = self._get_config(params or {})
        client = None
        try:
            client = self._connect(config)
            version = client.query("SELECT version()").result_rows[0][0]
            return {"success": True, "message": "Connection successful",
                    "version": str(version)}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(client)

    def discover_schema(self, params: Dict = None) -> Dict[str, Any]:
        import time
        from datetime import datetime

        params = params or {}
        config = self._get_config(params)
        db = self._safe_identifier(config.get("database") or "default") or "default"
        include_columns = bool(params.get("include_columns", True))
        include_row_counts = bool(params.get("include_row_counts", True))
        try:
            max_tables = int(params.get("max_tables", 100))
        except Exception:
            max_tables = 100
        if max_tables <= 0:
            max_tables = 100

        start_ms = int(time.time() * 1000)
        result: Dict[str, Any] = {
            "schema_version": "2.0",
            "discovered_at": datetime.utcnow().isoformat() + "Z",
            "connector_type": "clickhouse",
            "connector_version": getattr(self, "version", "1.0.0"),
            "database_version": None,
            "total_tables_available": 0,
            "total_tables_discovered": 0,
            "discovery_duration_ms": 0,
            "overall_status": "success",
            "warnings_objects": [],
            "warnings_messages": [],
            "tables": [],
        }

        def add_warning(category, severity, message):
            result["warnings_objects"].append(
                {"category": category, "severity": severity, "message": message})
            result["warnings_messages"].append(message)
            if severity == "error":
                result["overall_status"] = "partial_success"

        client = None
        try:
            client = self._connect(config)
            try:
                result["database_version"] = "ClickHouse " + str(
                    client.query("SELECT version()").result_rows[0][0])
            except Exception as e:  # noqa: BLE001
                add_warning("catalog_missing", "warning",
                            f"Could not retrieve database version: {e}")

            # Table list with estimated row counts (system.tables is cheap; total_rows
            # is exact for MergeTree). Exclude views/dictionaries.
            tbl_rows = client.query(
                "SELECT name, coalesce(total_rows, 0) AS row_estimate "
                "FROM system.tables "
                "WHERE database = {db:String} AND engine NOT LIKE '%View' "
                "AND engine NOT LIKE '%Dictionary%' "
                "ORDER BY name",
                parameters={"db": db},
            ).result_rows
            all_names = [str(r[0]) for r in tbl_rows if r and r[0]]
            row_est = {str(r[0]): int(r[1] or 0) for r in tbl_rows}
            result["total_tables_available"] = len(all_names)
            names = all_names[:max_tables]

            # One query for ALL columns of ALL tables (batch, not per-table).
            cols_by_table: Dict[str, List[Dict[str, Any]]] = {n: [] for n in names}
            pks_by_table: Dict[str, List[str]] = {n: [] for n in names}
            if include_columns and names:
                col_rows = client.query(
                    "SELECT table, name, type, is_in_primary_key "
                    "FROM system.columns "
                    "WHERE database = {db:String} "
                    "ORDER BY table, position",
                    parameters={"db": db},
                ).result_rows
                for cr in col_rows:
                    tname = str(cr[0])
                    if tname not in cols_by_table:
                        continue
                    cname = str(cr[1])
                    ch_type = str(cr[2])
                    is_pk = bool(cr[3])
                    col_obj = {
                        "name": cname,
                        "type": self._canonical_from_ch(ch_type),
                        "data_type": ch_type,
                        "nullable": ch_type.strip().startswith("Nullable("),
                        "is_primary_key": is_pk,
                    }
                    cols_by_table[tname].append(col_obj)
                    if is_pk:
                        pks_by_table[tname].append(cname)

            tables = []
            for n in names:
                obj = {"name": n, "schema": db, "discovery_status": "complete"}
                if include_columns:
                    obj["columns"] = cols_by_table.get(n, [])
                    obj["primary_keys"] = pks_by_table.get(n, [])
                if include_row_counts:
                    obj["row_count"] = row_est.get(n, 0)
                tables.append(obj)
            result["tables"] = tables
            result["total_tables_discovered"] = len(tables)
        except Exception as e:  # noqa: BLE001
            result["overall_status"] = "failed"
            add_warning("connection_error", "error", f"Schema discovery failed: {e}")
        finally:
            self._safe_close(client)
        result["discovery_duration_ms"] = int(time.time() * 1000) - start_ms
        return result

    def export(self, params: Dict = None) -> Dict[str, Any]:
        """Optimized paginated read (offset default, keyset when cursor_column is
        given, verbatim single-page for a raw query). Returns
        paging_mode/has_more/next_cursor/truncated."""
        params = params or {}
        config = self._get_config(params)
        table = (params.get("table") or params.get("object") or "").strip()
        raw_query = (params.get("query") or params.get("sql") or "").strip()
        limit = int(params.get("limit", 10000) or 10000)

        raw_cursor = (params.get("cursor_column") or params.get("order_by")
                      or params.get("primary_key") or "")
        if isinstance(raw_cursor, (list, tuple)):
            raw_cursor = raw_cursor[0] if raw_cursor else ""
        raw_cursor = str(raw_cursor).strip()
        cursor_column = ""
        if raw_cursor:
            cursor_column = self._safe_identifier(raw_cursor)
            if cursor_column is None:
                return {"success": False,
                        "error": f"Unsafe cursor_column identifier: {raw_cursor!r}"}
        cursor_value = params.get("cursor_value")

        if not table and not raw_query:
            return {"success": False, "error": "Missing 'table' (or 'query') parameter"}

        offset = 0
        if raw_query:
            paging_mode, sql = "query", raw_query
        elif cursor_column:
            paging_mode = "keyset"
            db, tbl = self._resolve_db_table(config, table, params)
            fq = self._qualify(db, tbl)
            where = ""
            if cursor_value is not None:
                where = f"WHERE `{cursor_column}` > {self._sql_literal(cursor_value)} "
            sql = (f"SELECT * FROM {fq} {where}"
                   f"ORDER BY `{cursor_column}` ASC LIMIT {limit}")
        else:
            paging_mode = "offset"
            offset = int(params.get("offset", 0) or 0)
            db, tbl = self._resolve_db_table(config, table, params)
            fq = self._qualify(db, tbl)
            sql = f"SELECT * FROM {fq} LIMIT {limit} OFFSET {offset}"

        client = None
        try:
            client = self._connect(config)
            qr = client.query(sql)
            columns = list(qr.column_names)
            data = [
                {columns[i]: self._jsonify(row[i]) for i in range(len(columns))}
                for row in qr.result_rows
            ]
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(client)

        full_page = len(data) == limit
        truncated = (paging_mode == "query") and full_page
        has_more = (paging_mode != "query") and full_page
        next_cursor = None
        if has_more:
            if paging_mode == "keyset":
                next_cursor = data[-1].get(cursor_column) if data else None
                if next_cursor is None:
                    return {"success": False,
                            "error": (f"keyset cursor column '{cursor_column}' missing "
                                      "from result rows; cannot continue")}
            else:
                next_cursor = str(offset + limit)

        return {
            "success": True,
            "data": data,
            "columns": columns,
            "total_records": len(data),
            "paging_mode": paging_mode,
            "has_more": has_more,
            "next_cursor": next_cursor,
            "truncated": truncated,
        }

    @staticmethod
    def _sql_literal(v: Any) -> str:
        """Format a keyset cursor value as a SQL literal. The value is our OWN
        previously-emitted next_cursor (not user input); strings are escaped."""
        if isinstance(v, bool):
            return "1" if v else "0"
        if isinstance(v, (int, float)):
            return str(v)
        s = str(v).replace("\\", "\\\\").replace("'", "\\'")
        return f"'{s}'"

    # =========================================================================
    # DESTINATION: DDL
    # =========================================================================
    def ensure_table(self, params: Dict = None) -> Dict[str, Any]:
        """Create / migrate the destination table for a pipeline write.

        Called by the kafka-mcp-sink before the first INSERT batch with the
        column list + canonical types from discover_schema. Idempotent.

        ClickHouse requires an explicit engine + sort key:
          * key_fields present → ReplacingMergeTree ORDER BY (keys) so reruns and
            upserts converge to one row per key.
          * no key_fields      → MergeTree ORDER BY tuple() (append-only).
        Non-key columns are Nullable(T); key columns are non-Nullable (ORDER BY
        keys cannot be Nullable).
        """
        params = params or {}
        config = self._get_config(params)
        table = str(params.get("table") or "").strip()
        if not table:
            return {"success": False, "error": "Missing table"}

        columns: List[str] = [str(c).strip() for c in (params.get("columns") or [])
                              if str(c or "").strip()]
        col_types_raw = params.get("column_types") or {}
        column_types: Dict[str, str] = {}
        if isinstance(col_types_raw, dict):
            for k, v in col_types_raw.items():
                if k and v:
                    column_types[str(k)] = str(v)

        keys_raw = params.get("key_fields") or params.get("keys") or []
        if isinstance(keys_raw, str):
            keys_raw = [keys_raw]
        key_fields: List[str] = [str(c).strip() for c in keys_raw if str(c or "").strip()]

        # Synthetic-PK fallback for PK-less sources (mirrors postgresql). An
        # explicit synthetic_pk=False or append_mode=True opts out (INSERT-only).
        append_mode = bool(params.get("append_mode"))
        synthetic_pk_param = params.get("synthetic_pk")
        synthetic_pk_disabled = (synthetic_pk_param is False) or append_mode
        synthetic_pk_requested = (bool(synthetic_pk_param) or (not key_fields)) \
            and not synthetic_pk_disabled
        if synthetic_pk_requested and not key_fields:
            if SYNTHETIC_PK_COL not in columns:
                columns.append(SYNTHETIC_PK_COL)
            if SYNTHETIC_TS_COL not in columns:
                columns.append(SYNTHETIC_TS_COL)
            column_types.setdefault(SYNTHETIC_PK_COL, "string")
            column_types.setdefault(SYNTHETIC_TS_COL, "timestamp")
            key_fields = [SYNTHETIC_PK_COL]

        # Key fields must exist as columns so the sort key binds.
        for k in key_fields:
            if k not in columns:
                columns.append(k)

        safe_cols = [c for c in columns if self._safe_identifier(c)]
        if not safe_cols:
            return {"success": False,
                    "error": "No safe columns to create destination table"}
        safe_keys = [k for k in key_fields if self._safe_identifier(k)]

        try:
            db, name = self._resolve_db_table(config, table, params)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}

        key_set = set(safe_keys)

        def col_def(col: str) -> str:
            ch = self._ch_ddl_type(column_types.get(col, "string"))
            if col in key_set:
                return f"`{col}` {ch}"          # sort-key columns must be non-Nullable
            return f"`{col}` Nullable({ch})"

        client = None
        try:
            client = self._connect(config)
            client.command(f"CREATE DATABASE IF NOT EXISTS `{db}`")

            col_defs = ", ".join(col_def(c) for c in safe_cols)
            if safe_keys:
                engine = "ReplacingMergeTree"
                order_by = "(" + ", ".join(f"`{k}`" for k in safe_keys) + ")"
            else:
                engine = "MergeTree"
                order_by = "tuple()"
            client.command(
                f"CREATE TABLE IF NOT EXISTS {self._qualify(db, name)} ({col_defs}) "
                f"ENGINE = {engine} ORDER BY {order_by}"
            )

            # Idempotent schema-migration: add any columns missing on an existing
            # table (ORDER BY can't change post-hoc, so added columns are Nullable).
            for c in safe_cols:
                if c in key_set:
                    continue
                ch = self._ch_ddl_type(column_types.get(c, "string"))
                client.command(
                    f"ALTER TABLE {self._qualify(db, name)} "
                    f"ADD COLUMN IF NOT EXISTS `{c}` Nullable({ch})"
                )
            return {"success": True, "table": f"{db}.{name}",
                    "engine": engine, "key_fields": safe_keys}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(client)

    def drop_table(self, params: Dict = None) -> Dict[str, Any]:
        """Drop the destination table (reload mode). Missing table = success no-op."""
        params = params or {}
        config = self._get_config(params)
        table = str(params.get("table") or "").strip()
        if not table:
            return {"success": False, "error": "Missing table"}
        try:
            db, name = self._resolve_db_table(config, table, params)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        client = None
        try:
            client = self._connect(config)
            client.command(f"DROP TABLE IF EXISTS {self._qualify(db, name)}")
            return {"success": True, "table": f"{db}.{name}", "dropped": True}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(client)

    # =========================================================================
    # DESTINATION: WRITE
    # =========================================================================
    def import_data(self, params: Dict = None) -> Dict[str, Any]:
        """Plain append (INSERT ... FORMAT JSONEachRow)."""
        return self._write(params, upsert=False)

    def upsert_data(self, params: Dict = None) -> Dict[str, Any]:
        """Batch upsert. ClickHouse has no atomic UPSERT: INSERT the rows, then
        (for keyed ReplacingMergeTree tables) OPTIMIZE ... FINAL to collapse
        duplicates on the sort key. In reload mode the table is dropped first so
        the INSERT alone is exact; OPTIMIZE makes reruns/incremental idempotent."""
        return self._write(params, upsert=True)

    def load(self, params: Dict = None) -> Dict[str, Any]:
        """Alias for import_data (append) — parity with the warehouse tool name."""
        return self._write(params, upsert=False)

    def merge(self, params: Dict = None) -> Dict[str, Any]:
        """Alias for upsert_data."""
        return self._write(params, upsert=True)

    def _write(self, params: Dict, upsert: bool) -> Dict[str, Any]:
        params = params or {}
        config = self._get_config(params)
        table = (params.get("table") or "").strip()
        raw = params.get("data") or []
        if not table:
            return {"success": False, "error": "Target table not specified"}
        rows = [r for r in raw if isinstance(r, dict)]
        if not rows:
            return {"success": True, "rows_loaded": 0, "rows_written": 0,
                    "method": "insert", "message": "No data to load"}

        # Synthetic-PK write path (mirrors mysql/oracle/sqlserver). When the
        # orchestrator flags this PK-less run with synthetic_pk=true, compute a
        # per-row _rsync_row_hash over the source columns and stamp _rsync_synced_at.
        # ensure_table created a ReplacingMergeTree ORDER BY _rsync_row_hash for
        # these runs; without a real per-row hash EVERY row carries an identical
        # empty key and OPTIMIZE FINAL (or optimize_on_insert) collapses the whole
        # batch to a single row — silent full-table data loss for keyless sources.
        # Synthetic-PK detection MIRRORS ensure_table: any keyless, non-append
        # write dedups on the row hash (explicit synthetic_pk=False or
        # append_mode opts out). ensure_table already created a ReplacingMergeTree
        # ORDER BY _rsync_row_hash for keyless tables, so a write that did NOT
        # stamp the per-row hash would leave every row sharing an empty key and
        # OPTIMIZE FINAL / optimize_on_insert would collapse the whole batch to
        # ONE row — silent full-table loss. Gating on the same condition as
        # ensure_table closes that asymmetry (previously we only stamped when the
        # caller passed synthetic_pk=true explicitly).
        _append_mode = bool(params.get("append_mode"))
        _synthetic_param = params.get("synthetic_pk")
        _raw_keys = (params.get("key_fields") or params.get("primary_key_fields")
                     or params.get("primary_keys") or [])
        if isinstance(_raw_keys, str):
            _raw_keys = [_raw_keys]
        _real_keys = [str(k) for k in _raw_keys if k]
        synthetic_pk = (bool(_synthetic_param) or not _real_keys) \
            and not ((_synthetic_param is False) or _append_mode)
        if synthetic_pk:
            from datetime import datetime, timezone
            now_iso = datetime.now(timezone.utc).isoformat()
            # Hash over the union of all row keys (not just rows[0]) so a
            # heterogeneous batch hashes deterministically.
            _seen_src: set = set()
            source_cols = []
            for _r in rows:
                for _c in _r.keys():
                    if _c not in _seen_src and not str(_c).startswith("_rsync_"):
                        _seen_src.add(_c)
                        source_cols.append(_c)
            for row in rows:
                if SYNTHETIC_PK_COL not in row:
                    row[SYNTHETIC_PK_COL] = _compute_row_hash(row, source_cols)
                if SYNTHETIC_TS_COL not in row:
                    row[SYNTHETIC_TS_COL] = now_iso

        try:
            db, name = self._resolve_db_table(config, table, params)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}

        # Column universe: explicit columns param, else ordered union of row keys.
        columns = [str(c).strip() for c in (params.get("columns") or [])
                   if str(c or "").strip()]
        if not columns:
            columns = self._collect_columns(rows)
        if synthetic_pk:
            for _sc in (SYNTHETIC_PK_COL, SYNTHETIC_TS_COL):
                if _sc not in columns:
                    columns.append(_sc)
        bad = [c for c in columns if self._safe_identifier(c) is None]
        if bad:
            return {"success": False, "error": f"Unsafe column identifier(s): {bad}"}

        key_fields = (params.get("key_fields") or params.get("primary_key_fields")
                      or params.get("primary_keys") or [])
        if isinstance(key_fields, str):
            key_fields = [key_fields]
        key_fields = [str(k) for k in key_fields if k]
        # A synthetic-PK run dedups on the row hash regardless of any key hint.
        if synthetic_pk:
            key_fields = [SYNTHETIC_PK_COL]

        # Build a JSONEachRow block. Every row includes every column (missing →
        # None) so the column set is stable; nested values are JSON-stringified.
        lines = []
        for row in rows:
            obj = {c: self._json_for_insert(row.get(c)) for c in columns}
            lines.append(json.dumps(obj, default=str))
        payload = "\n".join(lines).encode("utf-8")

        settings = {
            "date_time_input_format": "best_effort",
            "input_format_skip_unknown_fields": 1,
            "input_format_null_as_default": 1,
        }

        client = None
        try:
            client = self._connect(config)
            client.raw_insert(
                self._qualify(db, name),
                column_names=columns,
                insert_block=payload,
                fmt="JSONEachRow",
                settings=settings,
            )
            n = len(rows)
            optimized = False
            if upsert and key_fields:
                # Collapse duplicate versions on the sort key (best-effort).
                try:
                    client.command(
                        f"OPTIMIZE TABLE {self._qualify(db, name)} FINAL",
                        settings={"optimize_throw_if_noop": 0},
                    )
                    optimized = True
                except Exception as oe:  # noqa: BLE001
                    self.log(f"OPTIMIZE FINAL skipped for {db}.{name}: {oe}")
            return {"success": True, "rows_loaded": n, "rows_written": n,
                    "rows_inserted": n, "method": "insert",
                    "optimized": optimized, "table": f"{db}.{name}"}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(client)

    # =========================================================================
    # small utils
    # =========================================================================
    @staticmethod
    def _collect_columns(rows: List[Dict[str, Any]]) -> List[str]:
        """Ordered union of keys: first row's order, then any new keys after."""
        cols: List[str] = []
        seen = set()
        for row in rows:
            for k in row.keys():
                if k not in seen:
                    seen.add(k)
                    cols.append(k)
        return cols

    @staticmethod
    def _safe_close(client) -> None:
        if client is None:
            return
        try:
            client.close()
        except Exception:
            pass

    # =========================================================================
    # DATA EXPLORER: single authorized statement (delegated write / DDL path)
    # =========================================================================
    _READ_STMT_RE = re.compile(
        r"^\s*(?:WITH\b|SELECT\b|SHOW\b|DESC(?:RIBE)?\b|EXPLAIN\b|EXISTS\b)",
        re.IGNORECASE,
    )

    def execute(self, params: Dict = None) -> Dict[str, Any]:
        """Run ONE authorized SQL statement for the Data Explorer write path.

        The api-gateway has already classified the statement and enforced the
        workspace-role gate (viewer=read, admin=DML/DDL, owner=destructive) and
        validated single-statement-ness before delegating here (executor.go
        ExplorerExecute → Operation "execute"); this method does not re-authorize.
        Read-shaped statements return rows/columns; everything else runs as a
        command and reports rows_affected when ClickHouse exposes one."""
        params = params or {}
        config = self._get_config(params)
        sql = (params.get("statement") or params.get("query")
               or params.get("sql") or "").strip()
        if not sql:
            return {"success": False, "error": "Missing 'statement' (or 'query'/'sql')"}

        client = None
        try:
            client = self._connect(config)
            if self._READ_STMT_RE.match(self._strip_leading_sql_comments(sql)):
                qr = client.query(sql)
                columns = list(qr.column_names)
                data = [
                    {columns[i]: self._jsonify(row[i]) for i in range(len(columns))}
                    for row in qr.result_rows
                ]
                return {"success": True, "data": data, "columns": columns,
                        "row_count": len(data), "rows_affected": len(data)}
            summary = client.command(sql)
            return {"success": True, "executed": True,
                    "rows_affected": self._rows_affected_from_summary(summary, client)}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(client)

    @staticmethod
    def _strip_leading_sql_comments(sql: str) -> str:
        """Strip leading whitespace + SQL comments so a comment-prefixed read
        (``/* c */ SELECT ...`` or ``-- x\\nSELECT ...``) is still classified as a
        read. Used ONLY for read-vs-command routing; the statement executed is
        unchanged (comments preserved)."""
        s = sql or ""
        while True:
            s = s.lstrip()
            if s.startswith("--"):
                nl = s.find("\n")
                s = "" if nl == -1 else s[nl + 1:]
                continue
            if s.startswith("/*"):
                end = s.find("*/")
                s = "" if end == -1 else s[end + 2:]
                continue
            return s

    @staticmethod
    def _rows_affected_from_summary(summary: Any, client: Any) -> Optional[int]:
        """Best-effort affected-row count from a clickhouse-connect command summary.
        ClickHouse returns no count for most mutations (ALTER…DELETE is async), so
        this is frequently None — which the orchestrator's extractRowsAffected
        tolerates (returns nil)."""
        for src in (summary, getattr(client, "last_query_summary", None)):
            if isinstance(src, dict):
                for k in ("written_rows", "result_rows", "read_rows"):
                    v = src.get(k)
                    if v is not None:
                        try:
                            return int(v)
                        except (TypeError, ValueError):
                            pass
        if isinstance(summary, int):
            return summary
        return None

    # =========================================================================
    # CAPABILITIES
    # =========================================================================
    def get_capabilities(self, params: Dict = None) -> Dict[str, Any]:
        runtime_version = (os.getenv("MCP_CONNECTOR_VERSION")
                           or os.getenv("CONNECTOR_VERSION") or "").strip()
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
            "supports_ddl": bool(getattr(self, "supports_ddl", False)),
            "auto_create_destination_tables": bool(
                getattr(self, "auto_create_destination_tables", False)),
            "supported_formats": self.supported_formats,
            "supported_modalities": self.supported_modalities,
            "max_batch_size": self.max_batch_size,
            "load_strategy": self.load_strategy_capability(),
            "operations": [
                {"name": "test_connection", "type": "core",
                 "method": "clickhouse_test_connection"},
                {"name": "validate_config", "type": "core",
                 "method": "clickhouse_validate_config"},
                {"name": "discover_schema", "type": "core",
                 "method": "clickhouse_discover_schema"},
                {"name": "get_capabilities", "type": "core",
                 "method": "clickhouse_get_capabilities"},
                {"name": "export", "type": "source", "method": "clickhouse_export"},
                {"name": "execute", "type": "source",
                 "method": "clickhouse_execute"},
                {"name": "ensure_table", "type": "destination",
                 "method": "clickhouse_ensure_table"},
                {"name": "import_data", "type": "destination",
                 "method": "clickhouse_import_data"},
                {"name": "upsert_data", "type": "destination",
                 "method": "clickhouse_upsert_data"},
                {"name": "drop_table", "type": "destination",
                 "method": "clickhouse_drop_table"},
            ],
        }


# =============================================================================
# HTTP SERVER (Docker) — mirrors the postgresql/redshift entrypoint
# =============================================================================
def create_http_app():
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

    app = FastAPI(title="ClickHouse MCP Connector")
    server = ClickHouseMCPServer()

    @app.get("/health")
    async def health():
        return {"status": "healthy", "connector": server.connector_type}

    @app.post("/mcp")
    async def mcp(request: dict):
        return server.handle_request(request)

    @app.post("/invoke/{tool_name}")
    async def invoke(tool_name: str, params: dict = {}):
        try:
            method_name = tool_name.replace("-", "_")
            if hasattr(server, method_name) and not method_name.startswith("_"):
                return getattr(server, method_name)(params)
            prefixed = f"clickhouse_{method_name}"
            stripped = method_name
            if method_name.startswith("clickhouse_"):
                stripped = method_name[len("clickhouse_"):]
            if hasattr(server, stripped) and not stripped.startswith("_"):
                return getattr(server, stripped)(params)
            raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
        except HTTPException:
            raise
        except Exception as e:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(e))

    @app.get("/capabilities")
    async def capabilities():
        return server.get_capabilities()

    @app.post("/test_connection")
    async def test_connection(params: dict = {}):
        return server.test_connection(params)

    @app.post("/validate_config")
    async def validate_config(params: dict = {}):
        return server.validate_config(params)

    @app.post("/discover_schema")
    async def discover_schema(params: dict = {}):
        return server.discover_schema(params)

    @app.post("/export")
    async def export(params: dict = {}):
        return server.export(params)

    @app.post("/execute")
    async def execute(params: dict = {}):
        return server.execute(params)

    @app.post("/ensure_table")
    async def ensure_table(params: dict = {}):
        return server.ensure_table(params)

    @app.post("/import_data")
    async def import_data(params: dict = {}):
        return server.import_data(params)

    @app.post("/upsert_data")
    async def upsert_data(params: dict = {}):
        return server.upsert_data(params)

    @app.post("/drop_table")
    async def drop_table(params: dict = {}):
        return server.drop_table(params)

    return app


if __name__ == "__main__":
    http_mode = os.getenv("MCP_HTTP_MODE", "false").lower() == "true"
    port = int(os.getenv("MCP_PORT", os.getenv("PORT", "8000")))
    if http_mode or os.getenv("DOCKER_CONTAINER"):
        import uvicorn

        app = create_http_app()
        logger.info(f"🚀 Starting ClickHouse MCP Server in HTTP mode on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        server = ClickHouseMCPServer()
        server.run()
