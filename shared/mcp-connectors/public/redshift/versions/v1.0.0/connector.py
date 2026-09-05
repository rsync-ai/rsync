#!/usr/bin/env python3
"""
Amazon Redshift MCP Connector — first-class SOURCE + destination (data warehouse).

HAND-BUILT (not tool-generator output). Redshift speaks the PostgreSQL wire
protocol, so this connector rides the DB-API path via ``psycopg2`` (port 5439) —
NOT a warehouse adapter (those exist only for non-DBAPI warehouses like BigQuery).

It mirrors the hand-curated ``postgresql`` connector's patterns, with two
Redshift-specific overrides, because "just copy postgresql" would be wrong:

  * WRITES — Redshift has NO ``COPY ... FROM STDIN``. The default bulk path is a
    batched, parameterized multi-row ``INSERT`` (the execute_values strategy).
    A flag-gated ``COPY FROM s3`` path (``RSYNC_REDSHIFT_COPY_S3`` / ``copy_s3``
    param) is Redshift's canonical bulk load, but it is DORMANT in v1.0.0: it
    needs an S3 staging bucket + IAM role that this platform doesn't wire yet,
    so it always falls back to the multi-row INSERT (``fell_back=True``) — never
    a silent drop. ``_build_copy_sql`` is the tested seam for when it is wired.
  * MERGE — Redshift has NO ``INSERT ... ON CONFLICT``. Upsert is stage -> DELETE
    USING -> INSERT SELECT (``_build_upsert_sql``). Dormant/untested in v1.0.0.

  * READS — ``export`` is keyset/offset paginated so large tables resume to EOF
    via ``next_cursor`` / ``has_more`` instead of capping at one ``LIMIT``.

Identifier safety: table/column/cursor identifiers are validated against a plain
identifier allowlist before interpolation (Redshift/psycopg2 has no bind form for
identifiers); all row VALUES are bound parameters.
"""

import ipaddress
import logging
import os
import re
import sys
from typing import Any, Dict, List, Optional

# base_connector lives next to this file in the Docker image; in the repo we walk
# up so tests can import offline (psycopg2 is imported lazily inside _connect).
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 100.64.0.0/10 — RFC 6598 carrier-grade NAT (treated as local/private).
_CGNAT_NET = ipaddress.ip_network("100.64.0.0/10")


def _is_local_db_host(host: Optional[str]) -> bool:
    """Classify a DB host as a local/dev target (TLS not meaningful) vs a remote
    one that must be verified. Mirrors the Go ``IsLocalDBHost`` in
    ``backend-orchestrator/internal/cdc/postgresql.go``.

    Local: empty, the well-known loopback names, or an IP LITERAL that is
    loopback / RFC1918-private / link-local / CGNAT (100.64.0.0/10). A bare
    hostname with no dot (docker/k8s service name) is local; a dotted name or a
    public IP literal is remote.
    """
    h = (host or "").strip().lower().strip("[]")  # tolerate bracketed IPv6
    if not h:
        return True
    if h in ("localhost", "127.0.0.1", "::1", "host.docker.internal"):
        return True
    try:
        ip = ipaddress.ip_address(h)
    except ValueError:
        return "." not in h  # hostname: dotless => local (service name)
    return (ip.is_loopback or ip.is_private or ip.is_link_local
            or ip in _CGNAT_NET)


class RedshiftMCPServer(DestinationLoadMixin, BaseMCPConnector):
    """MCP Server for Amazon Redshift (DB-API via psycopg2)."""

    _IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(self):
        super().__init__()

        self.connector_type = "redshift"
        self.connector_category = "relational_db"  # PG-wire family (like postgresql)
        self.supports_source = True
        self.supports_destination = True
        self.supports_cdc = False  # merge()/CDC exist but stay dormant in v1.0.0

        self.supported_formats = ["json", "jsonl", "csv"]
        self.supported_modalities = ["structured"]
        self.max_batch_size = 10000

        # Default bulk = batched multi-row INSERT; merge = Redshift-correct
        # delete/insert (NOT on_conflict). COPY-from-S3 is the flag-gated upgrade.
        self.load_spec = DestinationLoadSpec(
            load_method="multi_insert",
            merge_method="delete_insert",
            supports_staging=True,
            max_batch_rows=10000,
        )

        self.log("Redshift MCP Server initialized")

    # =========================================================================
    # CONFIG + AUTH (basic host/port/db/user/password — same model as postgresql)
    # =========================================================================
    def _get_config(self, params: Dict) -> Dict:
        """Extract connection config, filling missing values from REDSHIFT_* env."""
        config = dict(params.get("config", params) if params else {})
        env_map = {
            "host": "REDSHIFT_HOST",
            "port": "REDSHIFT_PORT",
            "database": "REDSHIFT_DATABASE",
            "user": "REDSHIFT_USER",
            "password": "REDSHIFT_PASSWORD",
            "schema": "REDSHIFT_SCHEMA",
            "sslmode": "REDSHIFT_SSLMODE",
            "iam_role": "REDSHIFT_IAM_ROLE",
            "s3_bucket": "REDSHIFT_S3_BUCKET",
            "region": "REDSHIFT_REGION",
        }
        for key, env in env_map.items():
            if not config.get(key):
                v = os.getenv(env)
                if v:
                    config[key] = v
        # Sensible defaults
        config.setdefault("port", 5439)
        config.setdefault("schema", "public")
        # --- TLS: verify-by-default with explicit opt-out (mirrors the Go
        # PostgreSQL fix, cdc/postgresql.go ResolvePostgresSSLMode). Redshift/AWS
        # presents a publicly-trusted CA cert, so verify-full (encrypt AND verify
        # cert+hostname) works out of the box and is the safe default for any
        # REMOTE host — bare `require` only encrypts, letting a MITM with a
        # self-signed cert harvest creds+rows. An explicit sslmode is always
        # honoured, so operators can still opt OUT: require/prefer =
        # encrypt-without-verify (server whose CA isn't trusted), disable =
        # plaintext; verify-ca/verify-full are honoured. Local/docker-internal
        # hosts default to disable (dev/e2e Redshift-alikes rarely run TLS).
        if not config.get("sslmode"):
            if _is_local_db_host(config.get("host")):
                config["sslmode"] = "disable"
            else:
                config["sslmode"] = "verify-full"
                # psycopg2/libpq verifies against sslrootcert, which defaults to
                # ~/.postgresql/root.crt (absent in this image), NOT the system
                # trust store. Point it at the OS CA bundle (shipped by the
                # python:3.13-slim base) so Redshift's public cert verifies.
                # Injected ONLY when WE defaulted to verify-full and the user gave
                # no sslrootcert; an explicit mode/cert is left untouched.
                if not config.get("sslrootcert"):
                    config["sslrootcert"] = "/etc/ssl/certs/ca-certificates.crt"
        return config

    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        if not params:
            return {"valid": False, "errors": ["No configuration provided"]}
        config = self._get_config(params)
        errors: List[str] = []
        warnings: List[str] = []
        if not config.get("host"):
            errors.append("Missing required field: host")
        if not config.get("database"):
            errors.append("Missing required field: database")
        if not config.get("user"):
            errors.append("Missing required field: user")
        if not config.get("password") and not config.get("iam_role"):
            warnings.append("No password provided; assuming IAM/identity-based auth")
        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    # =========================================================================
    # DRIVER SEAM (the only place psycopg2 is touched)
    # =========================================================================
    def _connect(self, config: Dict[str, Any]):
        """Open a psycopg2 connection to Redshift. Swapped by tests."""
        try:
            import psycopg2  # type: ignore
        except Exception as e:  # pragma: no cover - import guard
            raise Exception(
                f"psycopg2 is not installed: {e}. Install psycopg2-binary."
            )
        connect_kwargs = dict(
            host=config.get("host"),
            port=int(config.get("port") or 5439),
            dbname=config.get("database"),
            user=config.get("user"),
            password=config.get("password") or "",
            sslmode=config.get("sslmode") or "require",
            connect_timeout=int(config.get("connect_timeout") or 10),
        )
        # verify-ca/verify-full need a CA bundle. _get_config injects the system
        # bundle path when it defaults to verify-full; an operator may also pass an
        # explicit sslrootcert. Only forward it when set (libpq validates the path).
        sslrootcert = config.get("sslrootcert")
        if sslrootcert:
            connect_kwargs["sslrootcert"] = sslrootcert
        return psycopg2.connect(**connect_kwargs)

    def _dict_cursor(self, conn):
        """Cursor yielding dict rows (RealDictCursor in prod; plain in tests)."""
        try:
            from psycopg2.extras import RealDictCursor  # type: ignore

            return conn.cursor(cursor_factory=RealDictCursor)
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
        raw = (table or "").strip().strip('"')
        if not raw:
            raise ValueError("Missing table")
        parts = raw.split(".")
        if len(parts) == 2:
            schema, tbl = parts[0], parts[1]
        elif len(parts) == 1:
            schema, tbl = (config.get("schema") or "public"), parts[0]
        else:
            raise ValueError(f"Invalid table reference: {table}")
        s, t = self._safe_identifier(schema), self._safe_identifier(tbl)
        if not s or not t:
            raise ValueError(f"Unsafe table identifier: {table}")
        return f'"{s}"."{t}"'

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
        schema = self._safe_identifier(config.get("schema") or "public") or "public"
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
            "connector_type": "redshift",
            "database_version": "Redshift",
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
                cur.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                    "ORDER BY table_name",
                    [schema],
                )
                rows = cur.fetchall()
            finally:
                self._safe_close(conn)
            names = [self._row_value(r, "table_name", 0) for r in rows]
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
            msg = f"Redshift discovery failed: {e}"
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
            # page. Callers pass a primary key here (hence the alias).
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
        if has_more:
            if paging_mode == "keyset":
                next_cursor = data[-1].get(cursor_column) if data else None
                if next_cursor is None:
                    # Full page but the cursor column is absent from the rows —
                    # can't emit a resumable cursor; fail loud rather than
                    # silently truncate or restart from the top next call.
                    return {"success": False,
                            "error": (f"keyset cursor column '{cursor_column}' "
                                      "missing from result rows; cannot continue")}
            else:
                next_cursor = str(offset + limit)

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
        """Bulk append. Default = batched multi-row INSERT. When ``copy_s3`` is
        enabled the COPY-from-S3 path is attempted first, falling back to the
        multi-row INSERT on any failure (``fell_back=True``) — never a silent
        drop. COPY-from-S3 is dormant/untested in v1.0.0."""
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
            fq = self._qualify_table(config, table)
            columns = self._collect_columns(rows)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}

        # Validate column identifiers up front (they are interpolated).
        bad = [c for c in columns if self._safe_identifier(c) is None]
        if bad:
            return {"success": False, "error": f"Unsafe column identifier(s): {bad}"}

        try:
            conn = self._connect(config)
        except Exception as e:  # noqa: BLE001
            return {"success": False, "error": str(e)}
        try:
            cur = conn.cursor()
            if self._copy_s3_enabled(params):
                try:
                    n = self._copy_from_s3(cur, fq, columns, rows, config, params)
                    conn.commit()
                    return {"success": True, "rows_loaded": n,
                            "method": "copy_s3", "fell_back": False}
                except Exception as e:  # noqa: BLE001
                    # Dormant/failed COPY path — degrade to multi-row INSERT.
                    self.log(f"COPY-from-S3 unavailable ({e}); "
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
        """CDC-style upsert via stage -> DELETE USING -> INSERT SELECT (Redshift
        has no ON CONFLICT). Dormant/untested in v1.0.0."""
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
            target_fq = self._qualify_table(config, table)
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
            cur.execute(f'CREATE TEMP TABLE _rsync_stg_redshift (LIKE {target_fq})')
            self._bulk_insert(cur, "_rsync_stg_redshift", columns, rows, params)
            delete_sql, insert_sql = self._build_upsert_sql(
                target_fq, "_rsync_stg_redshift", columns, key_fields)
            cur.execute(delete_sql)
            cur.execute(insert_sql)
            cur.execute("DROP TABLE IF EXISTS _rsync_stg_redshift")
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
        """Batched, parameterized multi-row INSERT (the execute_values strategy,
        hand-built so it stays driver-import-free and offline-testable). Row
        VALUES are bound params; column/table identifiers are pre-validated."""
        col_sql = ", ".join(f'"{c}"' for c in columns)
        row_ph = "(" + ", ".join(["%s"] * len(columns)) + ")"
        page_size = int((params or {}).get("page_size")
                        or (params or {}).get("batch_size") or 1000)
        if page_size <= 0:
            page_size = 1000
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

    def _copy_s3_enabled(self, params: Dict) -> bool:
        """COPY-from-S3 gate. Explicit ``copy_s3`` param wins, else the
        ``RSYNC_REDSHIFT_COPY_S3`` env flag (default-off)."""
        cs = (params or {}).get("copy_s3")
        if cs is not None:
            return bool(cs)
        return (os.getenv("RSYNC_REDSHIFT_COPY_S3") or "").strip().lower() in (
            "1", "true", "yes", "on")

    def _copy_from_s3(self, cursor, fq, columns, rows, config, params) -> int:
        """Redshift's canonical bulk load: stage to S3, then ``COPY FROM s3``.

        DORMANT in v1.0.0 — the S3 staging (bucket + IAM role) is not wired in
        this platform, so this raises to trigger the multi-row INSERT fallback.
        ``_build_copy_sql`` is the tested seam for when staging is wired.
        """
        raise NotImplementedError(
            "COPY-from-S3 is dormant in redshift v1.0.0 (S3 staging not wired)")

    @staticmethod
    def _build_copy_sql(fq: str, s3_uri: str, *, iam_role: str = None,
                        region: str = None, fmt: str = "JSON") -> str:
        """Build a Redshift ``COPY ... FROM 's3://...'`` statement. Redshift loads
        from S3 (NOT ``FROM STDIN``); auth is via IAM_ROLE (creds never inlined)."""
        auth = f"IAM_ROLE '{iam_role}'" if iam_role else "IAM_ROLE default"
        reg = f" REGION '{region}'" if region else ""
        if (fmt or "JSON").upper() == "JSON":
            return f"COPY {fq} FROM '{s3_uri}' {auth} FORMAT AS JSON 'auto'{reg}"
        return f"COPY {fq} FROM '{s3_uri}' {auth} FORMAT AS CSV{reg}"

    @classmethod
    def _build_upsert_sql(cls, target_fq: str, staging_fq: str,
                          columns: List[str], key_fields: List[str]):
        """Redshift upsert = DELETE matching keys, then INSERT all staged rows.
        Redshift has no ``ON CONFLICT``; this is the portable stage/delete/insert
        idiom. Returns (delete_sql, insert_sql)."""
        col_sql = ", ".join(f'"{c}"' for c in columns)
        on = " AND ".join(
            f'{target_fq}."{k}" = {staging_fq}."{k}"' for k in key_fields)
        delete_sql = f"DELETE FROM {target_fq} USING {staging_fq} WHERE {on}"
        insert_sql = (f"INSERT INTO {target_fq} ({col_sql}) "
                      f"SELECT {col_sql} FROM {staging_fq}")
        return delete_sql, insert_sql

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
        # Accept `params`: the base dispatcher resolves this by name at step 2 and
        # calls handler(normalized_args) with one positional arg, so a no-arg
        # override raises "takes 1 positional argument but 2 were given" — the
        # step-5 zero-arg fallback is unreachable once hasattr() has matched.
        return {
            "connector_type": self.connector_type,
            "category": self.connector_category,
            "supports_source": self.supports_source,
            "supports_destination": self.supports_destination,
            "supports_cdc": self.supports_cdc,
            "supported_formats": self.supported_formats,
            "supported_modalities": self.supported_modalities,
            "max_batch_size": self.max_batch_size,
            "load_strategy": self.load_strategy_capability(),
            "operations": [
                {"name": "test_connection", "type": "core", "method": "test_connection"},
                {"name": "discover_schema", "type": "core", "method": "discover_schema"},
                {"name": "export", "type": "source", "method": "export"},
                {"name": "import_data", "type": "destination", "method": "import_data"},
                {"name": "load", "type": "destination", "method": "load"},
                {"name": "merge", "type": "destination", "method": "merge"},
            ],
        }


# =============================================================================
# HTTP SERVER (Docker) — mirrors the postgresql/bigquery entrypoint
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

    app = FastAPI(title="Redshift MCP Connector")
    server = RedshiftMCPServer()

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
            prefixed = f"redshift_{method_name}"
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
        logger.info(f"🚀 Starting Redshift MCP Server in HTTP mode on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        server = RedshiftMCPServer()
        server.run()
