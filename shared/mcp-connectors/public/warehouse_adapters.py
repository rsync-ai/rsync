#!/usr/bin/env python3
"""
Warehouse adapters (runtime plugins) for non-DBAPI warehouses.

Why this exists:
- `connector_database.py.j2` is designed around DB-API / cursor-style databases.
- Some "data warehouses" (notably BigQuery) are NOT DB-API compatible in the way
  our generic template assumes.

Instead of hardcoding BigQuery-only methods into the generic template, we provide a
small adapter registry here. The generated connector can ask for an adapter by
`connector_type` and delegate:
- test_connection
- discover_schema
- export
- import_data

To add support for a new non-DBAPI warehouse, implement an adapter and register it
in `ADAPTERS` without touching the generic template.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol


class DataWarehouseAdapter(Protocol):
    def test_connection(self, config: Dict[str, Any]) -> Dict[str, Any]: ...
    def discover_schema(self, config: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]: ...
    def export(self, config: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]: ...
    def import_data(self, config: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]: ...
    # Optional warehouse-native operations (prefer bulk load + merge semantics)
    def load(self, config: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]: ...
    def merge(self, config: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]: ...


@dataclass
class BigQueryWarehouseAdapter:
    """
    BigQuery adapter using google-cloud-bigquery client.

    Config inputs (preferred):
    - project_id (required for most operations)
    - dataset_id (required for list tables + unqualified table names)
    - service_account_json (optional, secret): full JSON string
    - credentials_path (optional): path to SA JSON file; also supports GOOGLE_APPLICATION_CREDENTIALS
    - location (optional)
    """

    def _client(self, config: Dict[str, Any]):
        try:
            from google.cloud import bigquery  # type: ignore
        except Exception as e:
            raise Exception(
                f"google-cloud-bigquery is not installed: {e}. "
                "Install with: pip install google-cloud-bigquery"
            )

        project_id = (config.get("project_id") or "").strip() or None
        location = (config.get("location") or "").strip() or None

        service_account_json = config.get("service_account_json") or config.get("credentials_json")
        credentials_path = (config.get("credentials_path") or "").strip()
        if not credentials_path:
            credentials_path = (os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "").strip()

        credentials = None
        if service_account_json:
            try:
                from google.oauth2 import service_account  # type: ignore

                info = json.loads(service_account_json) if isinstance(service_account_json, str) else service_account_json
                credentials = service_account.Credentials.from_service_account_info(info)
            except Exception as e:
                raise Exception(f"Invalid service_account_json: {e}")
        elif credentials_path:
            try:
                from google.oauth2 import service_account  # type: ignore

                credentials = service_account.Credentials.from_service_account_file(credentials_path)
            except Exception as e:
                raise Exception(f"Invalid credentials_path: {e}")

        return bigquery.Client(project=project_id, credentials=credentials, location=location)

    def _bq(self):
        try:
            from google.cloud import bigquery  # type: ignore
        except Exception as e:
            raise Exception(
                f"google-cloud-bigquery is not installed: {e}. "
                "Install with: pip install google-cloud-bigquery"
            )
        return bigquery

    def _ensure_table_has_columns(self, client, table_fq: str, sample_row: Dict[str, Any]) -> None:
        """
        Best-effort additive schema evolution:
        - Add missing columns as NULLABLE with inferred types
        - Never attempts narrowing type changes
        """
        bq = self._bq()
        table = client.get_table(table_fq)
        existing = {f.name: f for f in (table.schema or [])}
        new_fields = list(table.schema or [])

        def infer_type(v: Any) -> str:
            # Conservative, compatible defaults
            if v is None:
                return "STRING"
            if isinstance(v, bool):
                return "BOOL"
            if isinstance(v, int) and not isinstance(v, bool):
                return "INT64"
            if isinstance(v, float):
                return "FLOAT64"
            if isinstance(v, (dict, list)):
                # BigQuery has native JSON type; if not supported in some envs, STRING still works.
                return "JSON"
            return "STRING"

        for k, v in (sample_row or {}).items():
            if not k or k in existing:
                continue
            t = infer_type(v)
            new_fields.append(bq.SchemaField(name=str(k), field_type=t, mode="NULLABLE"))

        if len(new_fields) != len(table.schema or []):
            table.schema = new_fields
            client.update_table(table, ["schema"])

    def _ensure_soft_delete_columns(self, client, table_fq: str) -> None:
        bq = self._bq()
        table = client.get_table(table_fq)
        existing = {f.name for f in (table.schema or [])}
        new_fields = list(table.schema or [])
        changed = False

        if "_rsync_deleted" not in existing:
            new_fields.append(bq.SchemaField(name="_rsync_deleted", field_type="BOOL", mode="NULLABLE"))
            changed = True
        if "_rsync_synced_at" not in existing:
            new_fields.append(bq.SchemaField(name="_rsync_synced_at", field_type="TIMESTAMP", mode="NULLABLE"))
            changed = True

        if changed:
            table.schema = new_fields
            client.update_table(table, ["schema"])

    def _qualify_table(self, config: Dict[str, Any], table: str) -> str:
        raw = (table or "").strip().strip("`")
        if not raw:
            raise ValueError("Missing table")

        if raw.count(".") == 2:
            return raw

        project_id = (config.get("project_id") or "").strip()
        dataset_id = (config.get("dataset_id") or "").strip()

        if raw.count(".") == 1:
            if not project_id:
                raise ValueError("Missing project_id for dataset.table reference")
            return f"{project_id}.{raw}"

        if not project_id or not dataset_id:
            raise ValueError("Missing project_id/dataset_id for unqualified table reference")
        return f"{project_id}.{dataset_id}.{raw}"

    def test_connection(self, config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            client = self._client(config)
            job = client.query("SELECT 1 AS ok")
            _ = list(job.result(max_results=1))
            return {"success": True, "message": "Connection successful"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def discover_schema(self, config: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        import time
        from datetime import datetime

        params = params or {}
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
            "connector_type": "bigquery",
            "connector_version": os.getenv("MCP_CONNECTOR_VERSION") or os.getenv("CONNECTOR_VERSION") or None,
            "database_version": "BigQuery",
            "total_tables_available": 0,
            "total_tables_discovered": 0,
            "discovery_duration_ms": 0,
            "overall_status": "success",
            "warnings_objects": [],
            "warnings_messages": [],
            "tables": [],
        }

        dataset_id = (config.get("dataset_id") or "").strip()
        project_id = (config.get("project_id") or "").strip()
        if not dataset_id or not project_id:
            result["overall_status"] = "partial_success"
            msg = "Missing project_id/dataset_id; cannot list tables."
            result["warnings_objects"].append({"category": "config", "severity": "error", "message": msg})
            result["warnings_messages"].append(msg)
            result["discovery_duration_ms"] = int(time.time() * 1000) - start_ms
            return result

        try:
            client = self._client(config)
            tables = list(client.list_tables(f"{project_id}.{dataset_id}"))
            result["total_tables_available"] = len(tables)
            out: List[Dict[str, Any]] = []
            for t in tables[:max_tables]:
                out.append(
                    {
                        "name": t.table_id,
                        "endpoint": f"{project_id}.{dataset_id}.{t.table_id}",
                        "discovery_status": "complete",
                    }
                )
            result["tables"] = out
            result["total_tables_discovered"] = len(out)
        except Exception as e:
            result["overall_status"] = "partial_success"
            msg = f"BigQuery discovery failed: {e}"
            result["warnings_objects"].append(
                {"category": "catalog_error", "severity": "error", "message": msg, "subsystem": "tables"}
            )
            result["warnings_messages"].append(msg)

        result["discovery_duration_ms"] = int(time.time() * 1000) - start_ms
        return result

    # A BigQuery unquoted column identifier: letter/underscore then word chars.
    # cursor_column is caller/pipeline-config controlled and gets interpolated
    # into the query text (BQ has no bind-parameter form for identifiers), so it
    # MUST match this before it can touch the SQL — see _safe_identifier.
    _IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    @classmethod
    def _safe_identifier(cls, name: str) -> Optional[str]:
        """Return ``name`` iff it is a plain BigQuery column identifier, else None.

        Stripping backticks is NOT sanitization (it only affects the ends), so
        an unsafe value is rejected outright rather than escaped.
        """
        n = (name or "").strip().strip("`").strip()
        return n if cls._IDENTIFIER_RE.match(n) else None

    @staticmethod
    def _param_type(value: Any) -> str:
        """BigQuery scalar-parameter type for a Python cursor value."""
        import datetime as _dt

        if isinstance(value, bool):
            return "BOOL"
        if isinstance(value, int):
            return "INT64"
        if isinstance(value, float):
            return "FLOAT64"
        # datetime is a subclass of date — check the narrower type first so a
        # plain date binds as DATE (a DATE column rejects a TIMESTAMP parameter).
        if isinstance(value, _dt.datetime):
            return "TIMESTAMP"
        if isinstance(value, _dt.date):
            return "DATE"
        return "STRING"

    def export(self, config: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Paginated read (optimized for large tables via keyset continuation).

        Modes:
        - keyset (preferred): pass ``cursor_column`` (aka ``order_by`` /
          ``primary_key``) and, on continuation, the ``cursor_value`` echoed back
          as ``next_cursor`` from the prior page. Emits
          ``... WHERE col > @cursor ORDER BY col ASC LIMIT n`` and returns the
          last row's key as ``next_cursor`` so the caller resumes to EOF instead
          of capping at ``LIMIT``.
        - offset (fallback, no cursor_column): ``... LIMIT n OFFSET off``;
          ``next_cursor`` is the next offset. BigQuery does not guarantee a stable
          order across pages here — prefer keyset for correctness on large tables.
        - query (explicit ``query``/``sql``): run verbatim, single page.

        Always returns ``paging_mode``, ``has_more`` and ``next_cursor`` alongside
        ``data`` / ``total_records``.
        """
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
            # cursor_column is interpolated into the SQL identifier position, so
            # it must be a plain identifier — reject anything else outright.
            # NOTE: keyset correctness also requires cursor_column be UNIQUE; a
            # non-unique column can skip rows sharing a boundary value across a
            # page. Callers pass a primary key here (hence the alias).
            cursor_column = self._safe_identifier(raw_cursor)
            if cursor_column is None:
                return {"success": False,
                        "error": f"Unsafe cursor_column identifier: {raw_cursor!r}"}
        cursor_value = params.get("cursor_value")

        if not table and not raw_query:
            return {"success": False, "error": "Missing 'table' (or 'query') parameter"}

        client = self._client(config)
        job_config = None
        offset = 0

        if raw_query:
            paging_mode = "query"
            sql = raw_query
        elif cursor_column:
            paging_mode = "keyset"
            fq = self._qualify_table(config, table)
            where = ""
            if cursor_value is not None:
                bq = self._bq()
                job_config = bq.QueryJobConfig(query_parameters=[
                    bq.ScalarQueryParameter("cursor", self._param_type(cursor_value), cursor_value)
                ])
                where = "WHERE `%s` > @cursor " % cursor_column
            sql = f"SELECT * FROM `{fq}` {where}ORDER BY `{cursor_column}` ASC LIMIT {limit}"
        else:
            paging_mode = "offset"
            offset = int(params.get("offset", 0) or 0)
            fq = self._qualify_table(config, table)
            sql = f"SELECT * FROM `{fq}` LIMIT {limit} OFFSET {offset}"

        if job_config is not None:
            job = client.query(sql, job_config=job_config)
        else:
            job = client.query(sql)
        rows = list(job.result(max_results=limit))
        data = [dict(r) for r in rows]

        # A full page (== limit) means there may be more; a short page is EOF.
        full_page = len(data) == limit
        # Verbatim queries aren't paginated by us, but max_results still caps
        # the read — surface that as `truncated` instead of dropping silently.
        truncated = (paging_mode == "query") and full_page
        has_more = (paging_mode != "query") and full_page
        next_cursor = None
        if has_more:
            if paging_mode == "keyset":
                next_cursor = data[-1].get(cursor_column) if data else None
                if next_cursor is None:
                    # Full page but the cursor column is absent from the rows
                    # (e.g. a pseudo/computed column not returned by SELECT *).
                    # We can't emit a resumable cursor — fail loud rather than
                    # silently truncate or restart from the top next call.
                    return {"success": False,
                            "error": (f"keyset cursor column '{cursor_column}' "
                                      "missing from result rows; cannot continue")}
            elif paging_mode == "offset":
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

    def import_data(self, config: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        table = (params.get("table") or "").strip()
        data = params.get("data") or []
        if not table:
            return {"success": False, "error": "Target table not specified"}
        if not isinstance(data, list) or not data:
            return {"success": True, "rows_inserted": 0, "message": "No data to import"}

        client = self._client(config)
        fq = self._qualify_table(config, table)
        errors = client.insert_rows_json(fq, data)
        if errors:
            return {"success": False, "error": "BigQuery insert_rows_json failed", "details": errors}
        return {"success": True, "rows_inserted": len(data)}

    @staticmethod
    def _env_true(name: str) -> bool:
        return (os.getenv(name) or "").strip().lower() in ("1", "true", "yes", "on")

    def _bq_load_job_enabled(self, params: Dict[str, Any]) -> bool:
        """Gate for the bulk NDJSON load-job path.

        An explicit ``bulk_load`` param wins (True forces the load job, False
        disables it); otherwise the ``RSYNC_BQ_LOAD_JOB`` env flag decides.
        Default-off (safe) — mirrors postgresql's ``RSYNC_PG_BULK_COPY``.
        """
        bl = (params or {}).get("bulk_load")
        if bl is not None:
            return bool(bl)
        return self._env_true("RSYNC_BQ_LOAD_JOB")

    def _stream_insert(self, client, fq: str, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Row-wise streaming write (``insert_rows_json``). Fails loud on errors."""
        errors = client.insert_rows_json(fq, data)
        if errors:
            return {"success": False, "error": "BigQuery insert_rows_json failed",
                    "details": errors, "method": "streaming"}
        return {"success": True, "rows_loaded": len(data), "method": "streaming"}

    def _load_job_append(self, client, fq: str, data: List[Dict[str, Any]]) -> int:
        """Bulk append via a BigQuery load job from a temporary NDJSON file."""
        import tempfile
        import json as _json

        bq = self._bq()
        job_config = bq.LoadJobConfig(
            source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bq.WriteDisposition.WRITE_APPEND,
        )
        rows_written = 0
        # w+b (readable+writable): google-cloud-bigquery's load_table_from_file
        # rejects a write-only ("wb") handle ("Cannot upload files opened in text
        # mode"); it must be able to READ the staged NDJSON back after seek(0).
        with tempfile.NamedTemporaryFile(mode="w+b", suffix=".jsonl", delete=True) as f:
            for row in data:
                if not isinstance(row, dict):
                    continue
                # ensure_ascii=False: astral-plane chars (emoji, U+1F600+) must be
                # written as raw UTF-8, not \uXXXX surrogate-pair escapes — BigQuery's
                # NDJSON loader does not recombine surrogate pairs and would replace
                # each half with U+FFFD, silently corrupting the value.
                f.write((_json.dumps(row, default=str, ensure_ascii=False) + "\n").encode("utf-8"))
                rows_written += 1
            f.flush()
            f.seek(0)
            job = client.load_table_from_file(f, fq, job_config=job_config)
            try:
                job.result()
            except Exception:
                # result() can raise (e.g. a client-side poll timeout) AFTER the
                # load job has already committed. Re-check server-side job state
                # before propagating: if it actually succeeded, do NOT signal
                # failure — otherwise the caller's streaming fallback would
                # double-write these rows. Only re-raise on a genuine failure.
                if self._load_job_succeeded(job):
                    return rows_written
                raise
        return rows_written

    @staticmethod
    def _load_job_succeeded(job) -> bool:
        """True iff a BigQuery load job committed cleanly (server-side truth)."""
        try:
            reload_fn = getattr(job, "reload", None)
            if callable(reload_fn):
                reload_fn()
        except Exception:
            return False
        return (getattr(job, "error_result", None) is None
                and getattr(job, "state", None) == "DONE")

    def load(self, config: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Bulk append load with an optimized, flag-gated fast path.

        Routing (``method`` reports what actually ran):
        - load-job (bulk) when ``_bq_load_job_enabled`` AND (``bulk_load`` is True
          OR the batch exceeds ``staging_threshold_rows``);
        - streaming (``insert_rows_json``) otherwise.

        A load-job failure NEVER silently drops rows — it falls back to streaming
        and sets ``fell_back=True`` so the caller can see the degraded path.
        """
        table = (params.get("table") or "").strip()
        data = params.get("data") or []
        if not table:
            return {"success": False, "error": "Target table not specified"}
        if not isinstance(data, list) or not data:
            return {"success": True, "rows_loaded": 0, "method": "streaming",
                    "fell_back": False, "message": "No data to load"}

        client = self._client(config)
        fq = self._qualify_table(config, table)

        # Best-effort additive schema evolution; never fail the load for it.
        try:
            self._ensure_table_has_columns(client, fq, data[0] if isinstance(data[0], dict) else {})
        except Exception:
            pass

        threshold_rows = int(params.get("staging_threshold_rows") or params.get("threshold_rows") or 1000)
        force = params.get("bulk_load") is True
        use_load_job = self._bq_load_job_enabled(params) and (force or len(data) > threshold_rows)

        if not use_load_job:
            out = self._stream_insert(client, fq, data)
            out["fell_back"] = False
            return out

        try:
            rows_written = self._load_job_append(client, fq, data)
            return {"success": True, "rows_loaded": rows_written,
                    "method": "load_job", "fell_back": False}
        except Exception as load_job_error:
            # Never silent-drop: degrade to streaming inserts. But make the
            # degradation VISIBLE — a silently-swallowed load-job failure hides
            # config/library bugs (a write-only file handle once made EVERY bulk
            # load job fail here and silently fall back to streaming). Surface
            # the reason in the log and in the result so callers can see it.
            logging.getLogger(__name__).warning(
                "BigQuery bulk load job failed (%s: %s); falling back to "
                "streaming inserts", type(load_job_error).__name__, load_job_error,
            )
            out = self._stream_insert(client, fq, data)
            out["fell_back"] = True
            out["load_job_error"] = f"{type(load_job_error).__name__}: {load_job_error}"
            return out

    def merge(self, config: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply CDC-style upserts/deletes via BigQuery MERGE.

        Inputs:
        - table: target table (required)
        - data: list of rows to apply (required)
        - key_fields / primary_keys: list of PK columns (required)
        - Rows may include `_rsync_deleted` boolean for soft deletes.
        """
        import time

        table = (params.get("table") or "").strip()
        data = params.get("data") or []
        key_fields = params.get("key_fields") or params.get("primary_keys") or params.get("primary_key_fields") or []
        if isinstance(key_fields, str):
            key_fields = [key_fields]
        key_fields = [str(k) for k in key_fields if k]

        if not table:
            return {"success": False, "error": "Target table not specified"}
        if not isinstance(data, list) or not data:
            return {"success": True, "rows_merged": 0, "message": "No data to merge"}
        if not key_fields:
            return {"success": False, "error": "Missing primary key fields for merge (key_fields/primary_keys)"}

        client = self._client(config)
        target_fq = self._qualify_table(config, table)

        # Ensure soft delete columns exist on target.
        try:
            self._ensure_soft_delete_columns(client, target_fq)
        except Exception:
            pass

        # Create staging table (same dataset) and load into it.
        # Use dataset from target_fq (project.dataset.table)
        parts = target_fq.split(".")
        if len(parts) != 3:
            return {"success": False, "error": f"Invalid target table reference: {target_fq}"}
        project_id, dataset_id, _ = parts
        staging_name = f"_rsync_staging_{int(time.time() * 1000)}"
        staging_fq = f"{project_id}.{dataset_id}.{staging_name}"

        bq = self._bq()
        # Infer schema from the first row; ensure keys + soft-delete fields exist.
        sample = data[0] if isinstance(data[0], dict) else {}
        if isinstance(sample, dict):
            for k in key_fields:
                sample.setdefault(k, None)
            sample.setdefault("_rsync_deleted", False)
            sample.setdefault("_rsync_synced_at", None)

        # Create staging table with inferred schema by creating empty table then patching schema.
        try:
            client.create_table(bq.Table(staging_fq), exists_ok=True)
        except Exception:
            # Best-effort: table may already exist or permissions may prevent explicit create.
            pass
        try:
            self._ensure_table_has_columns(client, staging_fq, sample if isinstance(sample, dict) else {})
            self._ensure_soft_delete_columns(client, staging_fq)
        except Exception:
            pass

        # Load data into staging (truncate).
        job_config = bq.LoadJobConfig(
            source_format=bq.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bq.WriteDisposition.WRITE_TRUNCATE,
        )
        import tempfile
        import json as _json
        # w+b (readable+writable): google-cloud-bigquery's load_table_from_file
        # rejects a write-only ("wb") handle ("Cannot upload files opened in text
        # mode"); it must be able to READ the staged NDJSON back after seek(0).
        with tempfile.NamedTemporaryFile(mode="w+b", suffix=".jsonl", delete=True) as f:
            for row in data:
                if isinstance(row, dict):
                    # Ensure soft-delete columns are present for deterministic merge.
                    row.setdefault("_rsync_deleted", False)
                    row.setdefault("_rsync_synced_at", None)
                    # ensure_ascii=False: see _load_job_append — surrogate-pair escapes
                    # for astral chars get corrupted to U+FFFD by BigQuery's NDJSON loader.
                    f.write((_json.dumps(row, default=str, ensure_ascii=False) + "\n").encode("utf-8"))
            f.flush()
            f.seek(0)
            job = client.load_table_from_file(f, staging_fq, job_config=job_config)
            job.result()

        # Ensure target has additive columns from staging (best-effort).
        try:
            st = client.get_table(staging_fq)
            t = client.get_table(target_fq)
            target_cols = {f.name for f in (t.schema or [])}
            sample2 = {f.name: None for f in (st.schema or []) if f.name not in target_cols}
            if sample2:
                self._ensure_table_has_columns(client, target_fq, sample2)
        except Exception:
            pass

        # Build MERGE statement.
        def qcol(c: str) -> str:
            return f"`{c}`"

        on_cond = " AND ".join([f"T.{qcol(k)} = S.{qcol(k)}" for k in key_fields])

        # Columns to write: all staging columns except keys; always set _rsync_synced_at on apply.
        st_table = client.get_table(staging_fq)
        staging_cols = [f.name for f in (st_table.schema or [])]
        non_key_cols = [c for c in staging_cols if c not in key_fields]

        # For updates (non-deletes), set all non-key cols; also force _rsync_deleted false unless explicitly true.
        update_set = ", ".join([f"T.{qcol(c)} = S.{qcol(c)}" for c in non_key_cols if c != "_rsync_synced_at"])
        if update_set:
            update_set = update_set + ", "
        update_set += "T.`_rsync_synced_at` = CURRENT_TIMESTAMP()"

        delete_set = "T.`_rsync_deleted` = TRUE, T.`_rsync_synced_at` = CURRENT_TIMESTAMP()"

        insert_cols = ", ".join([qcol(c) for c in staging_cols if c != "_rsync_synced_at"] + ["`_rsync_synced_at`"])
        insert_vals = ", ".join([f"S.{qcol(c)}" for c in staging_cols if c != "_rsync_synced_at"] + ["CURRENT_TIMESTAMP()"])

        sql = f"""
MERGE `{target_fq}` T
USING `{staging_fq}` S
ON {on_cond}
WHEN MATCHED AND COALESCE(S.`_rsync_deleted`, FALSE) = TRUE THEN
  UPDATE SET {delete_set}
WHEN MATCHED AND COALESCE(S.`_rsync_deleted`, FALSE) = FALSE THEN
  UPDATE SET {update_set}
WHEN NOT MATCHED BY TARGET AND COALESCE(S.`_rsync_deleted`, FALSE) = FALSE THEN
  INSERT ({insert_cols}) VALUES ({insert_vals})
"""
        job = client.query(sql)
        job.result()

        # Best-effort cleanup.
        try:
            client.delete_table(staging_fq, not_found_ok=True)
        except Exception:
            pass

        # Tier B (idempotent): record the Kafka high-water offset AFTER the data
        # MERGE. Best-effort — duplicates on replay are absorbed by the idempotent
        # MERGE above, so a failed offset write only costs reprocessing, never data.
        try:
            self._write_cdc_offsets(client, project_id, dataset_id, params)
        except Exception:
            pass

        return {"success": True, "rows_merged": len(data)}

    _CDC_OFFSETS_TABLE = "_rsync_cdc_offsets"

    def _offsets_fq(self, config: Dict[str, Any], project_id: str = "", dataset_id: str = "") -> Optional[str]:
        project_id = (project_id or config.get("project_id") or "").strip()
        dataset_id = (dataset_id or config.get("dataset_id") or "").strip()
        if not project_id or not dataset_id:
            return None
        return f"{project_id}.{dataset_id}.{self._CDC_OFFSETS_TABLE}"

    def _write_cdc_offsets(self, client, project_id: str, dataset_id: str, params: Dict[str, Any]) -> None:
        """Persist per-partition Kafka high-water offsets (monotonic). No-op
        unless the caller (kafka-mcp-sink) supplied ``kafka_offset``."""
        ko = (params or {}).get("kafka_offset")
        if not ko:
            return
        offsets = ko if isinstance(ko, list) else [ko]
        offsets_fq = f"{project_id}.{dataset_id}.{self._CDC_OFFSETS_TABLE}"
        bq = self._bq()
        schema = [
            bq.SchemaField("pipeline_id", "STRING", mode="REQUIRED"),
            bq.SchemaField("topic", "STRING", mode="REQUIRED"),
            bq.SchemaField("kafka_partition", "INT64", mode="REQUIRED"),
            bq.SchemaField("last_offset", "INT64", mode="REQUIRED"),
            bq.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
        ]
        try:
            client.create_table(bq.Table(offsets_fq, schema=schema), exists_ok=True)
        except Exception:
            pass
        for o in offsets:
            if not isinstance(o, dict):
                continue
            pid = str(o.get("pipeline_id") or "")
            topic = o.get("topic") or ""
            part = o.get("partition")
            off = o.get("offset")
            if not topic or part is None or off is None:
                continue
            sql = f"""
MERGE `{offsets_fq}` T
USING (SELECT @pid AS pipeline_id, @topic AS topic, @part AS kafka_partition, @off AS last_offset) S
ON T.pipeline_id = S.pipeline_id AND T.topic = S.topic AND T.kafka_partition = S.kafka_partition
WHEN MATCHED THEN UPDATE SET
  T.last_offset = GREATEST(T.last_offset, S.last_offset), T.updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (pipeline_id, topic, kafka_partition, last_offset, updated_at)
  VALUES (S.pipeline_id, S.topic, S.kafka_partition, S.last_offset, CURRENT_TIMESTAMP())
"""
            job_config = bq.QueryJobConfig(query_parameters=[
                bq.ScalarQueryParameter("pid", "STRING", pid),
                bq.ScalarQueryParameter("topic", "STRING", str(topic)),
                bq.ScalarQueryParameter("part", "INT64", int(part)),
                bq.ScalarQueryParameter("off", "INT64", int(off)),
            ])
            client.query(sql, job_config=job_config).result()

    def get_cdc_offsets(self, config: Dict[str, Any], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Return durable per-partition Kafka high-water offsets so the sink can
        seed its skip-map on restart (CDC exactly-once recovery)."""
        params = params or {}
        try:
            offsets_fq = self._offsets_fq(config)
            if not offsets_fq:
                return {"success": True, "offsets": []}
            pipeline_id = str(params.get("pipeline_id") or "")
            client = self._client(config)
            bq = self._bq()
            sql = (
                f"SELECT topic, kafka_partition, last_offset FROM `{offsets_fq}` "
                "WHERE pipeline_id = @pid"
            )
            job_config = bq.QueryJobConfig(query_parameters=[
                bq.ScalarQueryParameter("pid", "STRING", pipeline_id),
            ])
            rows = list(client.query(sql, job_config=job_config).result())
            offsets = [
                {"topic": r["topic"], "partition": int(r["kafka_partition"]), "offset": int(r["last_offset"])}
                for r in rows
            ]
            return {"success": True, "offsets": offsets}
        except Exception:
            # Offset table not provisioned yet => no prior progress.
            return {"success": True, "offsets": []}


ADAPTERS: Dict[str, Any] = {
    # Non-DBAPI warehouse adapters
    "bigquery": BigQueryWarehouseAdapter,
}


def get_data_warehouse_adapter(connector_type: str) -> Optional[DataWarehouseAdapter]:
    ct = (connector_type or "").strip().lower()
    cls = ADAPTERS.get(ct)
    if not cls:
        return None
    return cls()

