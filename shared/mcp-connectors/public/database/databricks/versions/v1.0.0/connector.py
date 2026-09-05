#!/usr/bin/env python3
"""
Databricks MCP Connector — first-class SOURCE + destination (SQL warehouse / Lakehouse).

HAND-BUILT (not tool-generator output). Databricks ships a PEP-249 DB-API 2.0
driver (``databricks-sql-connector``), so this connector rides the DB-API path via
a single ``_connect`` seam — NOT a warehouse adapter (those exist only for
non-DBAPI warehouses like BigQuery). It mirrors the hand-curated ``redshift`` /
``snowflake`` connectors' patterns, with Databricks-correct overrides because
"just copy snowflake" would be wrong:

  * IDENTIFIERS — Databricks (Spark SQL) quotes with BACKTICKS (not double quotes)
    and folds UNQUOTED names to LOWERCASE. Tables are Unity Catalog 3-level
    ``catalog.schema.table``; the connection's default catalog/schema fill the
    unqualified parts.
  * AUTH — a Personal Access Token (PAT) over ``server_hostname`` + ``http_path``
    to a SQL warehouse.
  * ROWS — the driver returns ``Row`` objects (not dicts); they are converted via
    ``asDict()`` / ``cursor.description`` into plain dicts.
  * WRITES — default bulk path is a batched, parameterized multi-row ``INSERT``.
    A flag-gated ``COPY INTO`` from cloud storage (``RSYNC_DATABRICKS_COPY_INTO`` /
    ``copy_into`` param) is Databricks' canonical bulk load, but it is DORMANT in
    v1.0.0 (no cloud stage is wired), so it always falls back to the multi-row
    INSERT (``fell_back=True``) — never a silent drop. ``_build_copy_sql`` is the
    tested seam for when a stage is wired.
  * MERGE — Delta Lake supports ``MERGE INTO``; upsert is stage -> single
    ``MERGE`` (``_build_merge_sql``). Dormant/untested in v1.0.0.

  * READS — ``export`` is keyset/offset paginated so large tables resume to EOF
    via ``next_cursor`` / ``has_more`` instead of capping at one ``LIMIT``.

Identifier safety: table/column/cursor identifiers are validated against a plain
identifier allowlist before interpolation (there is no bind form for identifiers);
all row VALUES are bound parameters (``?`` — Databricks native paramstyle; the
databricks-sql driver rejects positional ``%s`` when use_inline_params=False).
"""

import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional

# base_connector lives next to this file in the Docker image; in the repo we walk
# up so tests can import offline (databricks.sql is imported lazily inside
# _connect).
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
    from canonical_types import canonical_to_ddl  # noqa: E402
except ImportError:  # pragma: no cover - dev/test path
    sys.path.insert(0, os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..")))
    from canonical_types import canonical_to_ddl  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabricksMCPServer(DestinationLoadMixin, BaseMCPConnector):
    """MCP Server for Databricks (DB-API via databricks-sql-connector)."""

    _IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    # CDC soft-delete sentinels (matches shared/warehouse_adapters.py + the sink's
    # `_rsync_deleted` convention at kafka-mcp-sink main.go). A CDC delete is applied
    # as a tombstone (set `_rsync_deleted=true`), NOT a physical DELETE, so downstream
    # reads filter `WHERE NOT _rsync_deleted`. `_rsync_synced_at` is stamped on apply.
    _CDC_DELETED_COL = "_rsync_deleted"
    _CDC_SYNCED_COL = "_rsync_synced_at"

    # Databricks' Thrift backend rejects a parameterized query with more than
    # this many bind markers ("too many parameters: N given but the limit is
    # 10000"). A multi-row INSERT binds rows*columns params, so a wide table blows
    # past a naive row-based page_size (1000 rows * 19 cols = 19000 > 10000 ->
    # BAD_REQUEST -> the whole batch DLQ'd). _bulk_insert caps rows/INSERT to keep
    # rows*columns within this limit.
    _MAX_BIND_PARAMS = 10000

    # canonical source type -> Databricks/Delta (Spark SQL) DDL type. Mirrors the
    # postgresql _pg_type_for map but for Delta. Used by ensure_table ONLY when
    # types_are_ddl is false (the batch/snapshot sink sends CANONICAL names like
    # "integer"/"timestamp"/"json"); when types_are_ddl is true the sink already
    # resolved a final Databricks type and it is passed through verbatim. Extra
    # aliases (int/long/double/bool/datetime/...) keep an unknown-but-common
    # source type from silently collapsing a numeric column to STRING.
    _CANONICAL_TO_DATABRICKS = {
        "string": "STRING", "text": "STRING", "varchar": "STRING", "char": "STRING",
        "integer": "BIGINT", "int": "BIGINT", "long": "BIGINT", "bigint": "BIGINT",
        "smallint": "BIGINT", "tinyint": "BIGINT",
        "number": "DOUBLE", "float": "DOUBLE", "double": "DOUBLE", "real": "DOUBLE",
        "boolean": "BOOLEAN", "bool": "BOOLEAN",
        "timestamp": "TIMESTAMP", "datetime": "TIMESTAMP",
        "date": "DATE",
        "time": "STRING",       # Spark SQL has no native TIME type
        "json": "STRING", "jsonb": "STRING", "object": "STRING",
        "binary": "BINARY", "bytes": "BINARY", "blob": "BINARY",
        "decimal": "DECIMAL(38,9)", "numeric": "DECIMAL(38,9)",
        "uuid": "STRING",
    }

    def __init__(self):
        super().__init__()

        self.connector_type = "databricks"
        self.connector_category = "data_warehouse"
        self.supports_source = True
        self.supports_destination = True
        self.supports_cdc = True  # CDC-as-destination: soft-delete Delta MERGE (merge())

        self.supported_formats = ["json", "jsonl", "csv"]
        self.supported_modalities = ["structured"]
        self.max_batch_size = 10000

        # Default bulk = batched multi-row INSERT; merge = native Delta MERGE.
        # COPY-INTO-from-cloud-storage is the flag-gated upgrade, dormant in v1.0.0.
        self.load_spec = DestinationLoadSpec(
            load_method="multi_insert",
            merge_method="merge",
            supports_staging=True,
            max_batch_rows=10000,
        )

        self.log("Databricks MCP Server initialized")

    # =========================================================================
    # CONFIG + AUTH (server_hostname/http_path/access_token + catalog/schema)
    # =========================================================================
    def _get_config(self, params: Dict) -> Dict:
        """Extract connection config, filling missing values from DATABRICKS_* env."""
        config = dict(params.get("config", params) if params else {})
        env_map = {
            "server_hostname": "DATABRICKS_SERVER_HOSTNAME",
            "http_path": "DATABRICKS_HTTP_PATH",
            "access_token": "DATABRICKS_ACCESS_TOKEN",
            "catalog": "DATABRICKS_CATALOG",
            "schema": "DATABRICKS_SCHEMA",
        }
        for key, env in env_map.items():
            if not config.get(key):
                v = os.getenv(env)
                if v:
                    config[key] = v
        # Sensible defaults (Databricks/Spark folds unquoted names to LOWERCASE, so
        # the default schema is the canonical lowercase ``default``).
        config.setdefault("schema", "default")
        return config

    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        if not params:
            return {"valid": False, "errors": ["No configuration provided"]}
        config = self._get_config(params)
        errors: List[str] = []
        warnings: List[str] = []
        if not config.get("server_hostname"):
            errors.append("Missing required field: server_hostname")
        if not config.get("http_path"):
            errors.append("Missing required field: http_path")
        if not config.get("access_token"):
            errors.append("Missing required field: access_token")
        if not config.get("catalog"):
            warnings.append(
                "No catalog set; relies on the SQL warehouse's default catalog")
        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    # =========================================================================
    # DRIVER SEAM (the only place databricks.sql is touched)
    # =========================================================================
    def _connect(self, config: Dict[str, Any]):
        """Open a databricks.sql connection. Swapped by tests."""
        try:
            from databricks import sql as dbsql  # type: ignore
        except Exception as e:  # pragma: no cover - import guard
            raise Exception(
                f"databricks-sql-connector is not installed: {e}. "
                "Install databricks-sql-connector.")
        # Bound connect + retry time. Without this the driver's default
        # DatabricksRetryPolicy retries an unreachable host for ~15 min
        # (_retry_stop_after_attempts_duration default 900s), so a bad
        # hostname would hang test_connection and any load. These knobs are
        # driver-internal (leading underscore) but stable across 3.x/4.x and
        # are consumed by the thrift backend.
        try:
            connect_timeout = int(config.get("connect_timeout") or 30)
        except (TypeError, ValueError):
            connect_timeout = 30
        if connect_timeout <= 0:
            connect_timeout = 30
        kwargs: Dict[str, Any] = {
            "server_hostname": config.get("server_hostname"),
            "http_path": config.get("http_path"),
            "access_token": config.get("access_token"),
            "_socket_timeout": connect_timeout,
            "_retry_stop_after_attempts_count": 5,
            "_retry_stop_after_attempts_duration": float(connect_timeout),
        }
        if config.get("catalog"):
            kwargs["catalog"] = config["catalog"]
        if config.get("schema"):
            kwargs["schema"] = config["schema"]
        return dbsql.connect(**kwargs)

    # =========================================================================
    # IDENTIFIER SAFETY + ROW HANDLING
    # =========================================================================
    @classmethod
    def _safe_identifier(cls, name: str) -> Optional[str]:
        """Return ``name`` iff it is a plain identifier, else None. Stripping
        backticks is NOT sanitization, so unsafe values are rejected outright."""
        n = (name or "").strip().strip("`").strip()
        return n if cls._IDENTIFIER_RE.match(n) else None

    def _qualify_table(self, config: Dict[str, Any], table: str) -> str:
        """Qualify to a backtick-quoted ``\\`catalog\\`.\\`schema\\`.\\`table\\```
        (1/2/3 parts). The connection's default catalog/schema fill missing parts."""
        raw = (table or "").strip().strip("`")
        if not raw:
            raise ValueError("Missing table")
        parts = raw.split(".")
        if len(parts) == 3:
            cat, schema, tbl = parts
        elif len(parts) == 2:
            cat, schema, tbl = config.get("catalog"), parts[0], parts[1]
        elif len(parts) == 1:
            cat, schema, tbl = (config.get("catalog"),
                                config.get("schema") or "default", parts[0])
        else:
            raise ValueError(f"Invalid table reference: {table}")
        s, t = self._safe_identifier(schema), self._safe_identifier(tbl)
        if not s or not t:
            raise ValueError(f"Unsafe table identifier: {table}")
        fq = f"`{s}`.`{t}`"
        if cat:
            c = self._safe_identifier(cat)
            if not c:
                raise ValueError(f"Unsafe table identifier: {table}")
            fq = f"`{c}`." + fq
        return fq

    def _config_with_namespace(self, params: Dict[str, Any], config: Dict[str, Any],
                               table: str) -> Dict[str, Any]:
        """Return a config copy whose ``schema`` is overridden by an explicit
        ``namespace`` param when the table is bare — the destination-namespace
        contract the sink forwards (bare table + separate ``namespace``, so a
        source-qualified table can't leak). Applying it in BOTH ``ensure_table``
        and ``load`` keeps the INSERT target identical to the table ensure_table
        creates; without it ensure_table honors the namespace but ``load``'s
        ``_qualify_table(config, table)`` falls back to ``config["schema"]`` and
        the rows land in the connection's default schema — a silent mis-route
        (rows "loaded" but into the wrong schema). Mirrors ``drop_table``."""
        ns = (params.get("namespace") or params.get("schema")
              or params.get("destination_namespace") or "").strip()
        qcfg = dict(config)
        if ns and "." not in str(table).strip("`"):
            qcfg["schema"] = ns
        return qcfg

    @staticmethod
    def _row_to_dict(row: Any, description: Any = None) -> Dict[str, Any]:
        """Normalize a driver row to a plain dict. Databricks returns ``Row``
        objects (namedtuple-like with ``asDict()``); fall back to zipping the
        cursor description over a positional row."""
        if isinstance(row, dict):
            return dict(row)
        as_dict = getattr(row, "asDict", None)
        if callable(as_dict):
            try:
                return dict(as_dict())
            except Exception:
                pass
        if description:
            try:
                cols = [d[0] for d in description]
                return dict(zip(cols, row))
            except Exception:
                pass
        try:
            return dict(row)
        except Exception:
            return {}

    @staticmethod
    def _coerce(v: Any) -> Any:
        """Coerce non-scalar row values for the DB driver (dict/list -> JSON)."""
        if isinstance(v, (dict, list)):
            import json

            return json.dumps(v, default=str)
        return v

    # =========================================================================
    # CORE OPS
    # =========================================================================
    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        config = self._get_config(params or {})
        try:
            conn = self._connect(config)
            try:
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
            finally:
                self._safe_close(conn)
            return {"success": True, "message": "Connection successful"}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}

    def discover_schema(self, params: Dict = None) -> Dict[str, Any]:
        import time
        from datetime import datetime

        params = params or {}
        config = self._get_config(params)
        schema = self._safe_identifier(config.get("schema") or "default") or "default"
        catalog = config.get("catalog")
        catalog_safe = self._safe_identifier(catalog) if catalog else None
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
            "connector_type": "databricks",
            "database_version": "Databricks",
            "total_tables_available": 0,
            "total_tables_discovered": 0,
            "discovery_duration_ms": 0,
            "overall_status": "success",
            "warnings_objects": [],
            "warnings_messages": [],
            "tables": [],
        }
        try:
            conn = self._connect(config)
            try:
                cur = conn.cursor()
                # Unity Catalog INFORMATION_SCHEMA is per-catalog; qualify it when a
                # catalog is configured, else rely on the session default catalog.
                info = (f"`{catalog_safe}`.information_schema.tables"
                        if catalog_safe else "information_schema.tables")
                cur.execute(
                    f"SELECT table_name FROM {info} "
                    "WHERE table_schema = ? AND table_type IN ('MANAGED', 'EXTERNAL') "
                    "ORDER BY table_name",
                    [schema],
                )
                rows = cur.fetchall()
                desc = getattr(cur, "description", None)
            finally:
                self._safe_close(conn)
            dicts = [self._row_to_dict(r, desc) for r in rows]
            names = [d.get("table_name") or d.get("TABLE_NAME") for d in dicts]
            names = [n for n in names if n]
            result["total_tables_available"] = len(names)
            out = [
                {"name": n, "endpoint": f"{schema}.{n}", "discovery_status": "complete"}
                for n in names[:max_tables]
            ]
            result["tables"] = out
            result["total_tables_discovered"] = len(out)
        except Exception as e:  # noqa: BLE001
            result["overall_status"] = "partial_success"
            msg = f"Databricks discovery failed: {e}"
            result["warnings_objects"].append(
                {"category": "catalog_error", "severity": "error", "message": msg}
            )
            result["warnings_messages"].append(msg)
        result["discovery_duration_ms"] = int(time.time() * 1000) - start_ms
        return result

    def export(self, params: Dict = None) -> Dict[str, Any]:
        """Optimized paginated read (keyset preferred, offset fallback, verbatim
        query single-page). Returns paging_mode/has_more/next_cursor/truncated."""
        params = params or {}
        config = self._get_config(params)
        table = (params.get("table") or params.get("object") or "").strip()
        raw_query = (params.get("query") or params.get("sql") or "").strip()
        limit = int(params.get("limit", 10000) or 10000)

        raw_cursor = (
            params.get("cursor_column")
            or params.get("order_by")
            or params.get("primary_key")
            or ""
        )
        if isinstance(raw_cursor, (list, tuple)):
            raw_cursor = raw_cursor[0] if raw_cursor else ""
        raw_cursor = str(raw_cursor).strip()
        cursor_column = ""
        if raw_cursor:
            # keyset correctness also requires cursor_column be UNIQUE; a
            # non-unique column can skip rows sharing a boundary value across a
            # page. Callers pass a unique/primary key here (hence the alias).
            cursor_column = self._safe_identifier(raw_cursor)
            if cursor_column is None:
                return {"success": False,
                        "error": f"Unsafe cursor_column identifier: {raw_cursor!r}"}
        cursor_value = params.get("cursor_value")

        if not table and not raw_query:
            return {"success": False, "error": "Missing 'table' (or 'query') parameter"}

        sql_params: Optional[List[Any]] = None
        offset = 0
        if raw_query:
            paging_mode, sql = "query", raw_query
        elif cursor_column:
            paging_mode = "keyset"
            fq = self._qualify_table(config, table)
            where = ""
            if cursor_value is not None:
                where = f"WHERE `{cursor_column}` > ? "
                sql_params = [cursor_value]
            sql = (f"SELECT * FROM {fq} {where}"
                   f"ORDER BY `{cursor_column}` ASC LIMIT {limit}")
        else:
            paging_mode = "offset"
            offset = int(params.get("offset", 0) or 0)
            fq = self._qualify_table(config, table)
            sql = f"SELECT * FROM {fq} LIMIT {limit} OFFSET {offset}"

        try:
            conn = self._connect(config)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        try:
            cur = conn.cursor()
            if sql_params is not None:
                cur.execute(sql, sql_params)
            else:
                cur.execute(sql)
            rows = cur.fetchall()
            desc = getattr(cur, "description", None)
            data = [self._row_to_dict(r, desc) for r in rows]
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(conn)

        full_page = len(data) == limit
        truncated = (paging_mode == "query") and full_page
        has_more = (paging_mode != "query") and full_page
        next_cursor = None
        # Offset mode MUST leave next_cursor=None. The executor advances `offset`
        # itself only when the connector returns no cursor (executor.go:
        # `if cursor == nil { offset += ... }`). Emitting str(offset+limit) here
        # makes the executor treat offset paging as keyset — it never advances
        # offset, re-reads page 1, trips the non-advancing-cursor guard, and
        # silently truncates keyless tables at `limit`. Matches the postgresql
        # connector + connector_database.py.j2 (keyset-only next_cursor).
        if has_more and paging_mode == "keyset":
            next_cursor = data[-1].get(cursor_column) if data else None
            if next_cursor is None:
                # Full page but the cursor column is absent from the rows —
                # can't emit a resumable cursor; fail loud rather than
                # silently truncate or restart from the top next call.
                return {"success": False,
                        "error": (f"keyset cursor column '{cursor_column}' "
                                  "missing from result rows; cannot continue")}

        return {
            "success": True,
            "data": data,
            "total_records": len(data),
            "paging_mode": paging_mode,
            "has_more": has_more,
            "next_cursor": next_cursor,
            "truncated": truncated,
        }

    # =========================================================================
    # DESTINATION WRITE
    # =========================================================================
    def import_data(self, params: Dict = None) -> Dict[str, Any]:
        """Destination write — routes through the bulk-first ``load``."""
        return self.load(params)

    def load(self, params: Dict = None) -> Dict[str, Any]:
        """Bulk append. Default = batched multi-row INSERT. When ``copy_into`` is
        enabled the COPY-INTO-from-cloud-storage path is attempted first, falling
        back to the multi-row INSERT on any failure (``fell_back=True``) — never a
        silent drop. COPY-INTO is dormant/untested in v1.0.0."""
        params = params or {}
        config = self._get_config(params)
        table = (params.get("table") or "").strip()
        raw = params.get("data") or []
        if not table:
            return {"success": False, "error": "Target table not specified"}
        rows = [r for r in raw if isinstance(r, dict)]
        if not rows:
            return {"success": True, "rows_loaded": 0, "method": "multi_insert",
                    "fell_back": False, "message": "No data to load"}

        try:
            # Honor the sink-forwarded destination namespace on the INSERT target
            # too — not just in the ensure_table sub-call below — or rows land in
            # config["schema"] while the table is created in <namespace>.
            fq = self._qualify_table(
                self._config_with_namespace(params, config, table), table)
            columns = self._collect_columns(rows)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}

        # Validate column identifiers up front (they are interpolated).
        bad = [c for c in columns if self._safe_identifier(c) is None]
        if bad:
            return {"success": False, "error": f"Unsafe column identifier(s): {bad}"}

        # Auto-create the destination table. The kafka-mcp-sink SKIPS its own
        # ensure_table step for WAREHOUSE destinations (main.go `!isWarehouse`
        # gate — "warehouses own their DDL"), so this plain-INSERT path would hit
        # TABLE_OR_VIEW_NOT_FOUND on a fresh table and DLQ every row. ensure_table
        # is idempotent (CREATE ... IF NOT EXISTS) and reuses the column_types +
        # key_fields the sink forwards on the write call.
        ens = self.ensure_table({**params, "columns": columns})
        if not ens.get("success"):
            return {"success": False,
                    "error": f"ensure_table failed: {ens.get('error')}"}

        try:
            conn = self._connect(config)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        try:
            cur = conn.cursor()
            if self._copy_into_enabled(params):
                try:
                    n = self._copy_from_storage(cur, fq, columns, rows, config, params)
                    self._safe_commit(conn)
                    return {"success": True, "rows_loaded": n,
                            "method": "copy_into", "fell_back": False}
                except Exception as e:  # noqa: BLE001
                    # Dormant/failed COPY path — degrade to multi-row INSERT.
                    self.log(f"COPY-INTO unavailable ({e}); "
                             f"falling back to multi-row INSERT")
                    n = self._bulk_insert(cur, fq, columns, rows, params)
                    self._safe_commit(conn)
                    return {"success": True, "rows_loaded": n,
                            "method": "multi_insert", "fell_back": True}
            n = self._bulk_insert(cur, fq, columns, rows, params)
            self._safe_commit(conn)
            return {"success": True, "rows_loaded": n,
                    "method": "multi_insert", "fell_back": False}
        except Exception as e:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception:
                pass
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(conn)

    def merge(self, params: Dict = None) -> Dict[str, Any]:
        """CDC-as-destination apply: stage -> single soft-delete Delta ``MERGE
        INTO``.

        The kafka-mcp-sink routes every warehouse CDC event (insert/update/delete)
        to this one tool (``databricks_merge``); a delete arrives as the Before
        image with ``_rsync_deleted=true`` added by the sink. Semantics match
        ``shared/warehouse_adapters.py``: a delete is a **soft delete** (tombstone
        the row, ``_rsync_deleted=true``), an insert/update is an upsert on
        ``key_fields`` that also clears the tombstone; every apply stamps
        ``_rsync_synced_at``. Downstream reads filter ``WHERE NOT _rsync_deleted``.

        Namespace: honors the sink-forwarded bare-table + ``namespace`` contract via
        ``_config_with_namespace`` (same as ``load``/``ensure_table``/``drop_table``)
        so CDC rows land in the per-pipeline schema, not the connection default.

        Sentinels: the sink does NOT call ``ensure_table`` on the warehouse CDC path,
        so ``merge`` self-provisions the ``_rsync_deleted``/``_rsync_synced_at``
        columns on the target (idempotent, information-schema-guarded).
        """
        params = params or {}
        config = self._get_config(params)
        table = (params.get("table") or "").strip()
        rows = [r for r in (params.get("data") or []) if isinstance(r, dict)]
        key_fields = (params.get("key_fields") or params.get("primary_keys")
                      or params.get("primary_key_fields") or [])
        if isinstance(key_fields, str):
            key_fields = [key_fields]
        key_fields = [str(k) for k in key_fields if k]

        if not table:
            return {"success": False, "error": "Target table not specified"}
        if not rows:
            return {"success": True, "rows_merged": 0, "message": "No data to merge"}
        if not key_fields:
            return {"success": False,
                    "error": "Missing key fields for merge (key_fields/primary_keys)"}

        try:
            # Honor the destination namespace (bare table + separate `namespace`),
            # exactly like load()/ensure_table()/drop_table(); a per-table staging
            # name keeps concurrent merges into the same schema from colliding.
            qcfg = self._config_with_namespace(params, config, table)
            target_fq = self._qualify_table(qcfg, table)
            stg = self._safe_identifier(str(table).strip("`").split(".")[-1]) or "tbl"
            staging_fq = self._qualify_table(qcfg, f"_rsync_stg_{stg}")
            columns = self._collect_columns(rows)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        bad = [c for c in list(columns) + list(key_fields)
               if self._safe_identifier(c) is None]
        if bad:
            return {"success": False, "error": f"Unsafe identifier(s): {bad}"}

        try:
            conn = self._connect(config)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        try:
            cur = conn.cursor()
            # CDC-only bootstrap: create the target if it doesn't exist yet (the
            # sink skips ensure_table for warehouses, so a streaming-first pipeline
            # with no prior batch/initial load would otherwise fail on CREATE TABLE
            # ... LIKE below). No-op in the standard cdc_initial_load=batch flow.
            self._ensure_target_exists(cur, target_fq, columns, key_fields)
            # Guarantee the soft-delete sentinels exist before staging LIKE target
            # so the tombstone/un-delete branches bind.
            self._ensure_cdc_columns(cur, target_fq)
            # Databricks/Delta supports CREATE TABLE ... LIKE; used as a staging
            # table (no session-temp Delta tables), dropped at the end.
            cur.execute(f"DROP TABLE IF EXISTS {staging_fq}")
            cur.execute(f"CREATE TABLE {staging_fq} LIKE {target_fq}")
            self._bulk_insert(cur, staging_fq, columns, rows, params)
            merge_sql = self._build_merge_sql(
                target_fq, staging_fq, columns, key_fields)
            cur.execute(merge_sql)
            cur.execute(f"DROP TABLE IF EXISTS {staging_fq}")
            self._safe_commit(conn)
            return {"success": True, "rows_merged": len(rows)}
        except Exception as e:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception:
                pass
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(conn)

    def _ensure_target_exists(self, cursor, target_fq: str,
                              columns: List[str], key_fields: List[str]) -> None:
        """CDC-only bootstrap: ``CREATE TABLE IF NOT EXISTS`` the target (schema +
        table) from the incoming row so the first CDC event doesn't fail on
        ``CREATE TABLE ... LIKE`` when no batch/initial load ran first. Business
        columns degrade to ``STRING`` (the batch/initial-load path sets precise
        types via ``ensure_table``); the soft-delete sentinels are typed. No-op
        when the table already exists (the standard cdc_initial_load=batch flow)."""
        business = [c for c in columns
                    if c not in (self._CDC_DELETED_COL, self._CDC_SYNCED_COL)]
        for k in key_fields:
            if k not in business:
                business.append(k)
        safe = [c for c in business if self._safe_identifier(c)]
        if not safe:
            return
        col_defs = ", ".join(f"`{c}` STRING" for c in safe)
        col_defs += (f", `{self._CDC_DELETED_COL}` BOOLEAN, "
                     f"`{self._CDC_SYNCED_COL}` TIMESTAMP")
        schema_fq = target_fq.rsplit(".", 1)[0] if "." in target_fq else ""
        if schema_fq:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_fq}")
        cursor.execute(
            f"CREATE TABLE IF NOT EXISTS {target_fq} ({col_defs}) USING DELTA")

    def _ensure_cdc_columns(self, cursor, target_fq: str) -> None:
        """Add the ``_rsync_deleted``/``_rsync_synced_at`` soft-delete sentinels to
        the CDC target if missing. Delta's ``ALTER TABLE ADD COLUMNS`` has no
        IF-NOT-EXISTS, so probe the live columns first (``SHOW COLUMNS``) and only
        add what's absent; a redundant add is still swallowed as a belt-and-braces
        guard against a detection miss/race. Mirrors ``warehouse_adapters``'
        ``_ensure_soft_delete_columns``."""
        existing = set()
        try:
            cursor.execute(f"SHOW COLUMNS IN {target_fq}")
            for r in (cursor.fetchall() or []):
                name = self._row_value(r, "col_name", 0)
                if name:
                    existing.add(str(name).strip().strip("`").lower())
        except Exception:
            existing = set()
        for col, typ in ((self._CDC_DELETED_COL, "BOOLEAN"),
                         (self._CDC_SYNCED_COL, "TIMESTAMP")):
            if col.lower() in existing:
                continue
            try:
                cursor.execute(
                    f"ALTER TABLE {target_fq} ADD COLUMNS (`{col}` {typ})")
            except Exception as e:  # noqa: BLE001
                msg = str(e).lower()
                if "already" not in msg and "exist" not in msg:
                    raise

    # =========================================================================
    # WRITE HELPERS
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

    def _bulk_insert(self, cursor, fq: str, columns: List[str],
                     rows: List[Dict[str, Any]], params: Dict = None) -> int:
        """Batched, parameterized multi-row INSERT (hand-built so it stays
        driver-import-free and offline-testable). Row VALUES are bound params;
        column/table identifiers are pre-validated. Backtick-quoted (Spark SQL)."""
        col_sql = ", ".join(f"`{c}`" for c in columns)
        row_ph = "(" + ", ".join(["?"] * len(columns)) + ")"
        page_size = int((params or {}).get("page_size")
                        or (params or {}).get("batch_size") or 1000)
        if page_size <= 0:
            page_size = 1000
        # Cap rows/INSERT so rows*columns stays within Databricks' bind-marker
        # limit (see _MAX_BIND_PARAMS). Without this a wide table (e.g. 19 cols)
        # at the default 1000-row page emits 19000 params and the driver rejects
        # the whole batch. floor(limit/ncols) rows keeps every INSERT <= limit.
        ncols = max(1, len(columns))
        param_cap = max(1, self._MAX_BIND_PARAMS // ncols)
        page_size = max(1, min(page_size, param_cap))
        total = 0
        for start in range(0, len(rows), page_size):
            chunk = rows[start:start + page_size]
            if not chunk:
                continue
            values_sql = ", ".join([row_ph] * len(chunk))
            sql = f"INSERT INTO {fq} ({col_sql}) VALUES {values_sql}"
            flat: List[Any] = []
            for row in chunk:
                for c in columns:
                    flat.append(self._coerce(row.get(c)))
            cursor.execute(sql, flat)
            total += len(chunk)
        return total

    def _copy_into_enabled(self, params: Dict) -> bool:
        """COPY-INTO gate. Explicit ``copy_into`` param wins, else the
        ``RSYNC_DATABRICKS_COPY_INTO`` env flag (default-off)."""
        ci = (params or {}).get("copy_into")
        if ci is not None:
            return bool(ci)
        return (os.getenv("RSYNC_DATABRICKS_COPY_INTO") or "").strip().lower() in (
            "1", "true", "yes", "on")

    def _copy_from_storage(self, cursor, fq, columns, rows, config, params) -> int:
        """Databricks' canonical bulk load: stage files to cloud storage, then
        ``COPY INTO``.

        DORMANT in v1.0.0 — no cloud stage is wired in this platform, so this
        raises to trigger the multi-row INSERT fallback. ``_build_copy_sql`` is the
        tested seam for when a stage is wired.
        """
        raise NotImplementedError(
            "COPY-INTO is dormant in databricks v1.0.0 (no cloud stage wired)")

    @staticmethod
    def _build_copy_sql(fq: str, source_uri: str, *, file_format: str = "JSON") -> str:
        """Build a Databricks ``COPY INTO <table> FROM '<uri>'`` statement.
        Databricks loads from cloud storage (``s3://`` / ``abfss://`` / a volume),
        NOT ``FROM STDIN``."""
        fmt = (file_format or "JSON").upper()
        return (f"COPY INTO {fq} FROM '{source_uri}' "
                f"FILEFORMAT = {fmt}")

    @classmethod
    def _build_merge_sql(cls, target_fq: str, staging_fq: str,
                         columns: List[str], key_fields: List[str]) -> str:
        """Soft-delete CDC ``MERGE INTO`` (Spark SQL, backtick-quoted). Three
        conditional clauses keyed on the staging ``_rsync_deleted`` flag, matching
        ``warehouse_adapters.py``:

          1. matched & ``_rsync_deleted`` -> tombstone (set ``_rsync_deleted=true``)
          2. matched & live -> upsert business cols, clear tombstone
          3. not-matched & live -> insert business cols (+ sentinels)

        ``_rsync_deleted``/``_rsync_synced_at`` are managed here, never copied as
        ordinary business columns; ``_rsync_synced_at`` is stamped on every apply.
        A delete for a row that isn't present is a no-op (no NOT-MATCHED insert of a
        tombstone), and a re-insert after a delete un-tombstones the row.

        The target and source are ALIASED ``T``/``S`` (like ``warehouse_adapters``):
        a 3-level ``catalog.schema.table`` cannot be used as a column qualifier, so
        column refs MUST go through a short alias or Spark's analyzer can't bind
        them (``UNRESOLVED_COLUMN``)."""
        deleted, synced = cls._CDC_DELETED_COL, cls._CDC_SYNCED_COL
        sentinels = {deleted, synced}
        on = " AND ".join(f"T.`{k}` = S.`{k}`" for k in key_fields)
        key_set = set(key_fields)
        business = [c for c in columns if c not in sentinels]
        non_keys = [c for c in business if c not in key_set]
        s_del = f"COALESCE(S.`{deleted}`, false)"

        # 1. matched + tombstone -> soft delete
        del_clause = (
            f"WHEN MATCHED AND {s_del} = true THEN UPDATE SET "
            f"T.`{deleted}` = true, T.`{synced}` = current_timestamp() ")
        # 2. matched + live -> upsert business cols, clear tombstone, stamp synced
        upd_sets = [f"T.`{c}` = S.`{c}`" for c in non_keys]
        upd_sets += [f"T.`{deleted}` = false",
                     f"T.`{synced}` = current_timestamp()"]
        upd_clause = (f"WHEN MATCHED AND {s_del} = false THEN UPDATE SET "
                      + ", ".join(upd_sets) + " ")
        # 3. not matched + live -> insert business cols + sentinels
        ins_cols = ", ".join(f"`{c}`" for c in business + [deleted, synced])
        ins_vals = ", ".join(
            [f"S.`{c}`" for c in business] + [s_del, "current_timestamp()"])
        ins_clause = (f"WHEN NOT MATCHED AND {s_del} = false THEN INSERT "
                      f"({ins_cols}) VALUES ({ins_vals})")
        return (f"MERGE INTO {target_fq} AS T USING {staging_fq} AS S ON {on} "
                + del_clause + upd_clause + ins_clause)

    # =========================================================================
    # small utils
    # =========================================================================
    @staticmethod
    def _safe_close(conn) -> None:
        try:
            conn.close()
        except Exception:
            pass

    @staticmethod
    def _safe_commit(conn) -> None:
        # databricks-sql-connector autocommits; commit() is a best-effort no-op.
        try:
            commit = getattr(conn, "commit", None)
            if callable(commit):
                commit()
        except Exception:
            pass

    @staticmethod
    def _row_value(row, key, idx):
        if isinstance(row, dict):
            return row.get(key)
        try:
            return row[idx]
        except Exception:
            return None

    # =========================================================================
    # CAPABILITIES
    # =========================================================================
    def get_capabilities(self, params: Dict = None) -> Dict[str, Any]:
        # NOTE: accept `params` — the base_connector /mcp dispatch calls
        # get_capabilities(tool_args); a no-arg override made the sink's
        # destination DDL capability probe fail with a TypeError.
        return {
            # The kafka-mcp-sink's callDestinationTool treats any /mcp tool result
            # WITHOUT success==true as a failure (main.go). get_capabilities is one
            # such call (the DDL probe): without this flag the probe "fails", the
            # sink never learns supports_ddl, ensure_table is skipped, and every
            # write row is DLQ'd with TABLE_OR_VIEW_NOT_FOUND. postgresql sets it too.
            "success": True,
            "connector_type": self.connector_type,
            "category": self.connector_category,
            "supports_source": self.supports_source,
            "supports_destination": self.supports_destination,
            "supports_cdc": self.supports_cdc,
            "supported_formats": self.supported_formats,
            "supported_modalities": self.supported_modalities,
            "max_batch_size": self.max_batch_size,
            "load_strategy": self.load_strategy_capability(),
            # DDL auto-create: the kafka-mcp-sink gates ensure_table on BOTH flags
            # being true (probeDDLSupport -> supports_ddl && auto_create). Without
            # them the sink SKIPS table creation and every INSERT into the
            # not-yet-existent Delta table is DLQ'd (silent "completed, 0 rows").
            "supports_ddl": True,
            "auto_create_destination_tables": True,
            "operations": [
                {"name": "test_connection", "type": "core", "method": "test_connection"},
                {"name": "discover_schema", "type": "core", "method": "discover_schema"},
                {"name": "export", "type": "source", "method": "export"},
                {"name": "import_data", "type": "destination", "method": "import_data"},
                {"name": "load", "type": "destination", "method": "load"},
                {"name": "merge", "type": "destination", "method": "merge"},
                {"name": "ensure_table", "type": "destination", "method": "ensure_table"},
                {"name": "drop_table", "type": "destination", "method": "drop_table"},
            ],
        }

    # =========================================================================
    # DESTINATION DDL (auto-create + reload support)
    # =========================================================================
    def _type_for_databricks(self, canonical: Any, types_are_ddl: bool) -> str:
        """Resolve a column's Delta DDL type. When ``types_are_ddl`` the sink
        already sent a final Databricks type -> use verbatim; otherwise map the
        canonical source name (integer/number/timestamp/json/...) to a Delta type,
        defaulting to STRING (STRING holds any value, so an unknown source type
        degrades safely instead of erroring the whole write).

        The canonical branch is delegated to the shared canonical->DDL authority
        (single source of truth across every sink). Behavior-preserving: number
        -> DOUBLE, integer -> BIGINT, decimal -> DECIMAL(38,9), json/time ->
        STRING, binary -> BINARY. The ``types_are_ddl`` verbatim passthrough is
        preserved and stays OUTSIDE the shared authority."""
        raw = str(canonical or "string").strip()
        if types_are_ddl and raw:
            return raw
        return canonical_to_ddl("databricks", raw)

    def ensure_table(self, params: Dict = None) -> Dict[str, Any]:
        """Auto-create the destination table before a pipeline write (Delta Lake).

        The kafka-mcp-sink calls this before the first INSERT batch — gated on the
        supports_ddl + auto_create_destination_tables capability flags — with the
        column list + canonical types derived from the source's discover_schema.
        Without it, ``load`` INSERTs into a non-existent table and every row is
        DLQ'd (the silent "completed but 0 rows" write). Idempotent via
        ``CREATE TABLE IF NOT EXISTS``; the schema is created first because Unity
        Catalog does NOT auto-create a schema on table create.

        Honors a bare table name + a separate ``namespace`` (destination schema)
        the sink forwards for single-namespace dests, exactly like ``drop_table``.
        ``key_fields`` are honored by ``import_data``'s Delta MERGE, not by DDL —
        Delta has no enforced unique index/constraint, so none is created here.

        Scope: CREATE only. Additive column migration on drift is deferred — Delta's
        ``ALTER TABLE ADD COLUMNS`` lacks an IF-NOT-EXISTS guard, so idempotent
        evolution needs an information_schema diff (BACKLOG DBX-DestPipelineWrite).
        For a fixed-schema batch load this is sufficient.
        """
        params = params or {}
        config = self._get_config(params)
        table = str(params.get("table") or "").strip()
        if not table:
            return {"success": False, "error": "Missing table"}

        # Bare table + separate destination schema (same contract as drop_table);
        # shared with load() so the INSERT target matches the table created here.
        qcfg = self._config_with_namespace(params, config, table)

        cols_raw = params.get("columns") or []
        columns: List[str] = []
        if isinstance(cols_raw, list):
            for c in cols_raw:
                cc = str(c or "").strip()
                if cc and cc not in columns:
                    columns.append(cc)

        col_types_raw = params.get("column_types") or {}
        column_types: Dict[str, str] = {}
        if isinstance(col_types_raw, dict):
            for k, v in col_types_raw.items():
                if k and v:
                    column_types[str(k)] = str(v)

        keys_raw = params.get("key_fields") or params.get("keys") or []
        key_fields: List[str] = []
        if isinstance(keys_raw, list):
            for c in keys_raw:
                cc = str(c or "").strip()
                if cc:
                    key_fields.append(cc)
        # Key columns must exist so import_data's MERGE ON clause can bind them.
        for k in key_fields:
            if k not in columns:
                columns.append(k)

        # Only safe identifiers reach the interpolated DDL (stripping backticks is
        # NOT sanitization — reject outright, same guard as _qualify_table).
        safe_cols = [c for c in columns if self._safe_identifier(c)]
        if not safe_cols:
            return {"success": False,
                    "error": "No safe columns to create destination table"}

        types_are_ddl = bool(params.get("types_are_ddl"))
        try:
            fq = self._qualify_table(qcfg, table)  # `cat`.`schema`.`tbl`
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        # `cat`.`schema` (or `schema`) prefix for CREATE SCHEMA — everything left
        # of the final backtick-quoted identifier in the qualified name.
        schema_fq = fq.rsplit(".", 1)[0] if "." in fq else ""

        try:
            conn = self._connect(config)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        try:
            cur = conn.cursor()
            if schema_fq:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_fq}")
            col_defs = ", ".join(
                f"`{c}` {self._type_for_databricks(column_types.get(c, 'string'), types_are_ddl)}"
                for c in safe_cols
            )
            cur.execute(f"CREATE TABLE IF NOT EXISTS {fq} ({col_defs}) USING DELTA")
            self._safe_commit(conn)
            return {"success": True, "table": table, "columns": safe_cols,
                    "key_fields": key_fields, "created": True}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(conn)

    def drop_table(self, params: Dict = None) -> Dict[str, Any]:
        """DROP the destination table for run_mode=reload (Delta full rebuild).

        Reload = "rebuild from scratch": ``DROP TABLE IF EXISTS`` lets the next
        run recreate the table fresh from the latest source schema (matches
        DMS/Fivetran/Airbyte full-load semantics — TRUNCATE would keep a stale
        column list). Missing table is a success no-op so reload is idempotent on
        the first run. The sink invokes ``databricks_drop_table`` via /mcp; without
        this method the reload path failed with "Unknown tool". Honors a bare table
        name + a separate ``namespace`` (destination schema) the executor forwards
        for single-namespace destinations.
        """
        params = params or {}
        config = self._get_config(params)
        table = (params.get("table") or "").strip()
        if not table:
            return {"success": False, "error": "Missing table"}
        qcfg = self._config_with_namespace(params, config, table)
        try:
            fq = self._qualify_table(qcfg, table)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        try:
            conn = self._connect(config)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        try:
            cur = conn.cursor()
            cur.execute(f"DROP TABLE IF EXISTS {fq}")
            self._safe_commit(conn)
            return {"success": True, "table": table, "dropped": True}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(conn)


# =============================================================================
# HTTP SERVER (Docker) — mirrors the redshift/snowflake entrypoint
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

    app = FastAPI(title="Databricks MCP Connector")
    server = DatabricksMCPServer()

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
            # Security: this route bypasses _handle_tool_call, so it needs its own
            # copy of that dispatcher's guard. Without it a crafted name like
            # "_cleanup_worker" resolves via getattr and exposes internals to any
            # caller that can reach the container. Tool handlers are always public.
            if method_name.startswith("_"):
                raise HTTPException(status_code=404, detail=f"Tool not found: {tool_name}")
            if hasattr(server, method_name):
                return getattr(server, method_name)(params)
            prefixed = f"databricks_{method_name}"
            if hasattr(server, prefixed):
                return getattr(server, prefixed)(params)
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

    @app.post("/import_data")
    async def import_data(params: dict = {}):
        return server.import_data(params)

    return app


if __name__ == "__main__":
    http_mode = os.getenv("MCP_HTTP_MODE", "false").lower() == "true"
    port = int(os.getenv("MCP_PORT", os.getenv("PORT", "8000")))
    if http_mode or os.getenv("DOCKER_CONTAINER"):
        import uvicorn

        app = create_http_app()
        logger.info(f"🚀 Starting Databricks MCP Server in HTTP mode on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        server = DatabricksMCPServer()
        server.run()
