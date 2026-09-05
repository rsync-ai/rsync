#!/usr/bin/env python3
"""
Snowflake MCP Connector — first-class SOURCE + destination (data warehouse).

HAND-BUILT (not tool-generator output). Snowflake ships a PEP-249 DB-API 2.0
driver (``snowflake-connector-python``), so this connector rides the DB-API path
via a single ``_connect`` seam — NOT a warehouse adapter (those exist only for
non-DBAPI warehouses like BigQuery). It mirrors the hand-curated ``redshift``
connector's patterns, with Snowflake-correct overrides because "just copy
redshift" would be wrong:

  * IDENTIFIERS — Snowflake quotes with double quotes and folds UNQUOTED names to
    UPPERCASE. Names surfaced by ``discover_schema`` (from INFORMATION_SCHEMA) are
    already in their stored case, so quoting them verbatim round-trips correctly.
    Tables may be 3-level ``database.schema.table``.
  * AUTH — password (basic) OR key-pair. When ``private_key_file`` is supplied the
    driver loads the PEM itself (``private_key_file``/``private_key_file_pwd``);
    otherwise password auth is used. Snowflake is phasing out password auth for
    programmatic access, so key-pair is a first-class option.
  * WRITES — default bulk path is a batched, parameterized multi-row ``INSERT``.
    A flag-gated ``COPY INTO`` from a stage (``RSYNC_SNOWFLAKE_COPY_STAGE`` /
    ``copy_stage`` param) is Snowflake's canonical bulk load, but it is DORMANT in
    v1.0.0 (no stage is wired), so it always falls back to the multi-row INSERT
    (``fell_back=True``) — never a silent drop. ``_build_copy_sql`` is the tested
    seam for when a stage is wired.
  * MERGE — unlike Redshift, Snowflake HAS ``MERGE INTO``; upsert is stage ->
    single ``MERGE`` (``_build_merge_sql``). Dormant/untested in v1.0.0.

  * READS — ``export`` is keyset/offset paginated so large tables resume to EOF
    via ``next_cursor`` / ``has_more`` instead of capping at one ``LIMIT``.

Identifier safety: table/column/cursor identifiers are validated against a plain
identifier allowlist before interpolation (there is no bind form for identifiers);
all row VALUES are bound parameters. Snowflake's default paramstyle is ``pyformat``
(``%s``), matching the Redshift/psycopg2 idiom.
"""

import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional

# base_connector lives next to this file in the Docker image; in the repo we walk
# up so tests can import offline (snowflake.connector is imported lazily inside
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


class SnowflakeMCPServer(DestinationLoadMixin, BaseMCPConnector):
    """MCP Server for Snowflake (DB-API via snowflake-connector-python)."""

    _IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

    # Snowflake caps a single statement's bind variables (~16384). A multi-row
    # INSERT binds rows*columns params, so a wide table at a naive 1000-row page
    # can exceed it. _bulk_insert caps rows/INSERT to keep rows*columns within
    # this limit (mirrors the databricks 10000 cap; Snowflake's is higher).
    _MAX_BIND_PARAMS = 16000

    # canonical source type -> Snowflake DDL type. Used by ensure_table ONLY when
    # types_are_ddl is false (the batch/snapshot sink sends CANONICAL names);
    # types_are_ddl means the sink already resolved a final Snowflake type ->
    # passed through verbatim. Extra aliases keep an unknown-but-common source
    # type from silently collapsing a numeric column to VARCHAR. NOTE: json ->
    # VARCHAR (not VARIANT) so a plain INSERT of a JSON string succeeds without
    # PARSE_JSON; revisit if the write path adopts VARIANT staging.
    _CANONICAL_TO_SNOWFLAKE = {
        "string": "VARCHAR", "text": "VARCHAR", "varchar": "VARCHAR", "char": "VARCHAR",
        "integer": "NUMBER(38,0)", "int": "NUMBER(38,0)", "long": "NUMBER(38,0)",
        "bigint": "NUMBER(38,0)", "smallint": "NUMBER(38,0)", "tinyint": "NUMBER(38,0)",
        "number": "FLOAT", "float": "FLOAT", "double": "FLOAT", "real": "FLOAT",
        "boolean": "BOOLEAN", "bool": "BOOLEAN",
        "timestamp": "TIMESTAMP_NTZ", "datetime": "TIMESTAMP_NTZ",
        "date": "DATE",
        "time": "TIME",
        "json": "VARCHAR", "jsonb": "VARCHAR", "object": "VARCHAR",
        "binary": "BINARY", "bytes": "BINARY", "blob": "BINARY",
        "decimal": "NUMBER(38,9)", "numeric": "NUMBER(38,9)",
        "uuid": "VARCHAR",
    }

    def __init__(self):
        super().__init__()

        self.connector_type = "snowflake"
        self.connector_category = "data_warehouse"
        self.supports_source = True
        self.supports_destination = True
        self.supports_cdc = False  # merge()/CDC exist but stay dormant in v1.0.0

        self.supported_formats = ["json", "jsonl", "csv"]
        self.supported_modalities = ["structured"]
        self.max_batch_size = 10000

        # Default bulk = batched multi-row INSERT; merge = native MERGE (Snowflake
        # has MERGE INTO, unlike Redshift). COPY-INTO-from-stage is the flag-gated
        # upgrade, dormant in v1.0.0.
        self.load_spec = DestinationLoadSpec(
            load_method="multi_insert",
            merge_method="merge",
            supports_staging=True,
            max_batch_rows=10000,
        )

        self.log("Snowflake MCP Server initialized")

    # =========================================================================
    # CONFIG + AUTH (account/user/password|key-pair/warehouse/database/schema/role)
    # =========================================================================
    def _get_config(self, params: Dict) -> Dict:
        """Extract connection config, filling missing values from SNOWFLAKE_* env."""
        config = dict(params.get("config", params) if params else {})
        env_map = {
            "account": "SNOWFLAKE_ACCOUNT",
            "user": "SNOWFLAKE_USER",
            "password": "SNOWFLAKE_PASSWORD",
            "warehouse": "SNOWFLAKE_WAREHOUSE",
            "database": "SNOWFLAKE_DATABASE",
            "schema": "SNOWFLAKE_SCHEMA",
            "role": "SNOWFLAKE_ROLE",
            "private_key_file": "SNOWFLAKE_PRIVATE_KEY_FILE",
            "private_key_file_pwd": "SNOWFLAKE_PRIVATE_KEY_FILE_PWD",
        }
        for key, env in env_map.items():
            if not config.get(key):
                v = os.getenv(env)
                if v:
                    config[key] = v
        # Sensible defaults (Snowflake folds unquoted names to UPPERCASE, so the
        # default schema is the canonical uppercase PUBLIC).
        config.setdefault("schema", "PUBLIC")
        return config

    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        if not params:
            return {"valid": False, "errors": ["No configuration provided"]}
        config = self._get_config(params)
        errors: List[str] = []
        warnings: List[str] = []
        if not config.get("account"):
            errors.append("Missing required field: account")
        if not config.get("user"):
            errors.append("Missing required field: user")
        if not config.get("database"):
            errors.append("Missing required field: database")
        if not config.get("password") and not config.get("private_key_file"):
            errors.append(
                "Missing credentials: provide 'password' or 'private_key_file'")
        if not config.get("warehouse"):
            warnings.append(
                "No warehouse set; queries use the user's default warehouse if any")
        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    # =========================================================================
    # DRIVER SEAM (the only place snowflake.connector is touched)
    # =========================================================================
    def _connect(self, config: Dict[str, Any]):
        """Open a snowflake.connector connection. Swapped by tests."""
        try:
            import snowflake.connector  # type: ignore
        except Exception as e:  # pragma: no cover - import guard
            raise Exception(
                f"snowflake-connector-python is not installed: {e}. "
                "Install snowflake-connector-python.")
        kwargs: Dict[str, Any] = {
            "account": config.get("account"),
            "user": config.get("user"),
            "database": config.get("database"),
            "schema": config.get("schema") or "PUBLIC",
            "login_timeout": int(config.get("connect_timeout") or 15),
        }
        if config.get("warehouse"):
            kwargs["warehouse"] = config["warehouse"]
        if config.get("role"):
            kwargs["role"] = config["role"]
        # Key-pair auth wins when a private key file is provided; the driver loads
        # and decrypts the PEM itself (no extra crypto dependency here).
        if config.get("private_key_file"):
            kwargs["private_key_file"] = config["private_key_file"]
            if config.get("private_key_file_pwd"):
                kwargs["private_key_file_pwd"] = config["private_key_file_pwd"]
        else:
            kwargs["password"] = config.get("password") or ""
        return snowflake.connector.connect(**kwargs)

    def _dict_cursor(self, conn):
        """Cursor yielding dict rows (DictCursor in prod; plain in tests)."""
        try:
            from snowflake.connector import DictCursor  # type: ignore

            return conn.cursor(DictCursor)
        except Exception:
            return conn.cursor()

    # =========================================================================
    # IDENTIFIER SAFETY (no bind form for identifiers in SQL)
    # =========================================================================
    @classmethod
    def _safe_identifier(cls, name: str) -> Optional[str]:
        """Return ``name`` iff it is a plain identifier, else None. Stripping
        quotes is NOT sanitization, so unsafe values are rejected outright."""
        n = (name or "").strip().strip('"').strip()
        return n if cls._IDENTIFIER_RE.match(n) else None

    def _qualify_table(self, config: Dict[str, Any], table: str) -> str:
        """Qualify to a double-quoted ``"db"."schema"."table"`` (1/2/3 parts)."""
        raw = (table or "").strip().strip('"')
        if not raw:
            raise ValueError("Missing table")
        parts = raw.split(".")
        if len(parts) == 3:
            db, schema, tbl = parts
        elif len(parts) == 2:
            db, schema, tbl = None, parts[0], parts[1]
        elif len(parts) == 1:
            db, schema, tbl = None, (config.get("schema") or "PUBLIC"), parts[0]
        else:
            raise ValueError(f"Invalid table reference: {table}")
        s, t = self._safe_identifier(schema), self._safe_identifier(tbl)
        if not s or not t:
            raise ValueError(f"Unsafe table identifier: {table}")
        fq = f'"{s}"."{t}"'
        if db is not None:
            d = self._safe_identifier(db)
            if not d:
                raise ValueError(f"Unsafe table identifier: {table}")
            fq = f'"{d}".' + fq
        return fq

    def _config_with_namespace(self, params: Dict[str, Any], config: Dict[str, Any],
                               table: str) -> Dict[str, Any]:
        """Return a config copy whose ``schema`` is overridden by an explicit
        ``namespace`` param when the table is bare — the destination-namespace
        contract the sink forwards (bare table + separate ``namespace``). Applying
        it in ``ensure_table``, ``load`` and ``merge`` keeps the write target
        identical to the table ensure_table creates; without it the write falls
        back to ``config["schema"]`` and rows land in the connection's default
        schema — a silent mis-route. Mirrors ``drop_table``."""
        ns = (params.get("namespace") or params.get("schema")
              or params.get("destination_namespace") or "").strip()
        qcfg = dict(config)
        if ns and "." not in str(table).strip('"'):
            qcfg["schema"] = ns
        return qcfg

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
        schema = self._safe_identifier(config.get("schema") or "PUBLIC") or "PUBLIC"
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
            "connector_type": "snowflake",
            "database_version": "Snowflake",
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
                cur = self._dict_cursor(conn)
                # Snowflake's INFORMATION_SCHEMA is per-database; the connection's
                # database scopes it. TABLE_SCHEMA is stored folded (UPPERCASE for
                # unquoted schemas), so callers pass the stored form.
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                    "ORDER BY table_name",
                    [schema],
                )
                rows = cur.fetchall()
            finally:
                self._safe_close(conn)
            names = [self._row_value(r, "table_name", 0)
                     or self._row_value(r, "TABLE_NAME", 0) for r in rows]
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
            msg = f"Snowflake discovery failed: {e}"
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
                where = f'WHERE "{cursor_column}" > %s '
                sql_params = [cursor_value]
            sql = (f'SELECT * FROM {fq} {where}'
                   f'ORDER BY "{cursor_column}" ASC LIMIT {limit}')
        else:
            paging_mode = "offset"
            offset = int(params.get("offset", 0) or 0)
            fq = self._qualify_table(config, table)
            sql = f'SELECT * FROM {fq} LIMIT {limit} OFFSET {offset}'

        try:
            conn = self._connect(config)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        try:
            cur = self._dict_cursor(conn)
            if sql_params is not None:
                cur.execute(sql, sql_params)
            else:
                cur.execute(sql)
            rows = cur.fetchall()
            data = [dict(r) for r in rows]
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
            next_cursor = self._row_value(data[-1], cursor_column, None) \
                if data else None
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
        """Bulk append. Default = batched multi-row INSERT. When ``copy_stage`` is
        enabled the COPY-INTO-from-stage path is attempted first, falling back to
        the multi-row INSERT on any failure (``fell_back=True``) — never a silent
        drop. COPY-INTO-from-stage is dormant/untested in v1.0.0."""
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
            # too — not just the ensure_table sub-call — or rows land in
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
        # a "table does not exist" error on a fresh table and DLQ every row.
        # ensure_table is idempotent (CREATE ... IF NOT EXISTS).
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
            if self._copy_stage_enabled(params):
                try:
                    n = self._copy_from_stage(cur, fq, columns, rows, config, params)
                    conn.commit()
                    return {"success": True, "rows_loaded": n,
                            "method": "copy_stage", "fell_back": False}
                except Exception as e:  # noqa: BLE001
                    # Dormant/failed COPY path — degrade to multi-row INSERT.
                    self.log(f"COPY-INTO-from-stage unavailable ({e}); "
                             f"falling back to multi-row INSERT")
                    n = self._bulk_insert(cur, fq, columns, rows, params)
                    conn.commit()
                    return {"success": True, "rows_loaded": n,
                            "method": "multi_insert", "fell_back": True}
            n = self._bulk_insert(cur, fq, columns, rows, params)
            conn.commit()
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
        """CDC-style upsert via stage -> single ``MERGE INTO`` (Snowflake has
        native MERGE, unlike Redshift). Dormant/untested in v1.0.0."""
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
            target_fq = self._qualify_table(
                self._config_with_namespace(params, config, table), table)
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
            cur.execute(
                f'CREATE TEMPORARY TABLE "_RSYNC_STG_SNOWFLAKE" LIKE {target_fq}')
            self._bulk_insert(cur, '"_RSYNC_STG_SNOWFLAKE"', columns, rows, params)
            merge_sql = self._build_merge_sql(
                target_fq, '"_RSYNC_STG_SNOWFLAKE"', columns, key_fields)
            cur.execute(merge_sql)
            cur.execute('DROP TABLE IF EXISTS "_RSYNC_STG_SNOWFLAKE"')
            conn.commit()
            return {"success": True, "rows_merged": len(rows)}
        except Exception as e:  # noqa: BLE001
            try:
                conn.rollback()
            except Exception:
                pass
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(conn)

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
        column/table identifiers are pre-validated."""
        col_sql = ", ".join(f'"{c}"' for c in columns)
        row_ph = "(" + ", ".join(["%s"] * len(columns)) + ")"
        page_size = int((params or {}).get("page_size")
                        or (params or {}).get("batch_size") or 1000)
        if page_size <= 0:
            page_size = 1000
        # Cap rows/INSERT so rows*columns stays within Snowflake's bind limit
        # (see _MAX_BIND_PARAMS) — a wide table at the default page would exceed it.
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

    def _copy_stage_enabled(self, params: Dict) -> bool:
        """COPY-INTO-from-stage gate. Explicit ``copy_stage`` param wins, else the
        ``RSYNC_SNOWFLAKE_COPY_STAGE`` env flag (default-off)."""
        cs = (params or {}).get("copy_stage")
        if cs is not None:
            return bool(cs)
        return (os.getenv("RSYNC_SNOWFLAKE_COPY_STAGE") or "").strip().lower() in (
            "1", "true", "yes", "on")

    def _copy_from_stage(self, cursor, fq, columns, rows, config, params) -> int:
        """Snowflake's canonical bulk load: PUT rows to a stage, then
        ``COPY INTO``.

        DORMANT in v1.0.0 — no internal/named stage is wired in this platform, so
        this raises to trigger the multi-row INSERT fallback. ``_build_copy_sql``
        is the tested seam for when a stage is wired.
        """
        raise NotImplementedError(
            "COPY-INTO-from-stage is dormant in snowflake v1.0.0 (no stage wired)")

    @staticmethod
    def _build_copy_sql(fq: str, stage_uri: str, *, file_format: str = "JSON") -> str:
        """Build a Snowflake ``COPY INTO <table> FROM @stage`` statement. Snowflake
        loads from a stage (``@stage`` / ``s3://``), NOT ``FROM STDIN``."""
        fmt = (file_format or "JSON").upper()
        return (f"COPY INTO {fq} FROM '{stage_uri}' "
                f"FILE_FORMAT = (TYPE = {fmt})")

    @classmethod
    def _build_merge_sql(cls, target_fq: str, staging_fq: str,
                         columns: List[str], key_fields: List[str]) -> str:
        """Snowflake upsert = single ``MERGE INTO ... USING ... ON keys WHEN
        MATCHED UPDATE WHEN NOT MATCHED INSERT``."""
        on = " AND ".join(
            f'{target_fq}."{k}" = {staging_fq}."{k}"' for k in key_fields)
        key_set = set(key_fields)
        non_keys = [c for c in columns if c not in key_set]
        col_sql = ", ".join(f'"{c}"' for c in columns)
        val_sql = ", ".join(f'{staging_fq}."{c}"' for c in columns)
        matched = ""
        if non_keys:
            set_sql = ", ".join(f'"{c}" = {staging_fq}."{c}"' for c in non_keys)
            matched = f"WHEN MATCHED THEN UPDATE SET {set_sql} "
        return (f"MERGE INTO {target_fq} USING {staging_fq} ON {on} "
                f"{matched}"
                f"WHEN NOT MATCHED THEN INSERT ({col_sql}) VALUES ({val_sql})")

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
        # Snowflake auto-commits DDL; commit() is a best-effort no-op for DML.
        try:
            commit = getattr(conn, "commit", None)
            if callable(commit):
                commit()
        except Exception:
            pass

    @staticmethod
    def _row_value(row, key, idx):
        if isinstance(row, dict):
            if key in row:
                return row.get(key)
            # Snowflake may surface UPPERCASE keys; try the folded form.
            if isinstance(key, str) and key.upper() in row:
                return row.get(key.upper())
            return None
        try:
            return row[idx]
        except Exception:
            return None

    # =========================================================================
    # DESTINATION DDL (auto-create + reload support)
    # =========================================================================
    def _type_for_snowflake(self, canonical: Any, types_are_ddl: bool) -> str:
        """Resolve a column's Snowflake DDL type. When ``types_are_ddl`` the sink
        already sent a final Snowflake type -> use verbatim; otherwise map the
        canonical source name to a Snowflake type, defaulting to VARCHAR (holds any
        value, so an unknown source type degrades safely).

        The canonical branch is delegated to the shared canonical->DDL authority
        (single source of truth across every sink). Behavior-preserving: json ->
        VARCHAR (not VARIANT), integer -> NUMBER(38,0), number -> FLOAT, decimal
        -> NUMBER(38,9), binary -> BINARY. The ``types_are_ddl`` verbatim
        passthrough is preserved and stays OUTSIDE the shared authority."""
        raw = str(canonical or "string").strip()
        if types_are_ddl and raw:
            return raw
        return canonical_to_ddl("snowflake", raw)

    def ensure_table(self, params: Dict = None) -> Dict[str, Any]:
        """Auto-create the destination table before a pipeline write (Snowflake).

        Mirrors the databricks/postgresql contract: the kafka-mcp-sink calls this
        before the first INSERT batch (gated on supports_ddl +
        auto_create_destination_tables) with the column list + canonical types.
        Without it, ``load`` INSERTs into a non-existent table and every row is
        DLQ'd. Idempotent via ``CREATE TABLE IF NOT EXISTS``; the schema is created
        first. Honors a bare table + a separate ``namespace`` (destination schema).
        ``key_fields`` are honored by import_data's MERGE, not by DDL.

        NOTE: unit-tested via the DB-API fake; NOT yet live-verified against a real
        Snowflake account (BACKLOG TG-WarehouseDialects / DBX-DestPipelineWrite).
        """
        params = params or {}
        config = self._get_config(params)
        table = str(params.get("table") or "").strip()
        if not table:
            return {"success": False, "error": "Missing table"}

        # Bare table + separate destination schema; shared with load()/merge() so
        # the write target matches the table created here.
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
        for k in key_fields:
            if k not in columns:
                columns.append(k)

        safe_cols = [c for c in columns if self._safe_identifier(c)]
        if not safe_cols:
            return {"success": False,
                    "error": "No safe columns to create destination table"}

        types_are_ddl = bool(params.get("types_are_ddl"))
        try:
            fq = self._qualify_table(qcfg, table)  # "db"."schema"."tbl"
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
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
                f'"{c}" {self._type_for_snowflake(column_types.get(c, "string"), types_are_ddl)}'
                for c in safe_cols
            )
            cur.execute(f"CREATE TABLE IF NOT EXISTS {fq} ({col_defs})")
            self._safe_commit(conn)
            return {"success": True, "table": table, "columns": safe_cols,
                    "key_fields": key_fields, "created": True}
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        finally:
            self._safe_close(conn)

    def drop_table(self, params: Dict = None) -> Dict[str, Any]:
        """DROP the destination table for run_mode=reload (full rebuild). Missing
        table is a success no-op so reload is idempotent on the first run. Honors a
        bare table + a separate ``namespace``. Mirrors the databricks drop_table.

        NOTE: unit-tested; NOT yet live-verified against a real Snowflake account.
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

    # =========================================================================
    # CAPABILITIES
    # =========================================================================
    def get_capabilities(self, params: Dict = None) -> Dict[str, Any]:
        # NOTE: accept `params` — the base_connector /mcp dispatch calls
        # get_capabilities(tool_args); a no-arg override made the sink's
        # destination DDL capability probe fail with a TypeError (same bug fixed
        # for databricks in #381).
        return {
            # kafka-mcp-sink's callDestinationTool treats a /mcp result without
            # success==true as a failure; without this the DDL probe fails, the sink
            # skips ensure_table, and writes are DLQ'd (TABLE not found). Mirrors
            # postgresql. See databricks connector for the full note.
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
            # not-yet-existent table is DLQ'd (silent "completed, 0 rows").
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


# =============================================================================
# HTTP SERVER (Docker) — mirrors the redshift/bigquery entrypoint
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

    app = FastAPI(title="Snowflake MCP Connector")
    server = SnowflakeMCPServer()

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
            prefixed = f"snowflake_{method_name}"
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
        logger.info(f"🚀 Starting Snowflake MCP Server in HTTP mode on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        server = SnowflakeMCPServer()
        server.run()
