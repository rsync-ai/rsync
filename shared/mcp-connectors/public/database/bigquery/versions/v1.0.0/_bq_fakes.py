"""Offline test doubles for ``google-cloud-bigquery``.

The BigQuery client library is NOT installed in the unit-test environment
(intentionally — these tests must run with stdlib only). ``BigQueryWarehouseAdapter``
touches the client only through two seams: ``_client(config)`` returns a
``bigquery.Client`` and ``_bq()`` returns the ``bigquery`` module. Tests swap
both seams for the fakes below, so no network / credentials / real library.

Fakes record every call (queries, load jobs, streaming inserts, param configs)
so tests can assert the SQL text, the load-job config, and the routing decision.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# --- module-level enums / config objects mirroring google.cloud.bigquery -----

class _SourceFormat:
    NEWLINE_DELIMITED_JSON = "NEWLINE_DELIMITED_JSON"
    CSV = "CSV"


class _WriteDisposition:
    WRITE_APPEND = "WRITE_APPEND"
    WRITE_TRUNCATE = "WRITE_TRUNCATE"
    WRITE_EMPTY = "WRITE_EMPTY"


class _LoadJobConfig:
    def __init__(self, source_format=None, write_disposition=None, **kw):
        self.source_format = source_format
        self.write_disposition = write_disposition
        self.extra = kw


class _QueryJobConfig:
    def __init__(self, query_parameters=None, **kw):
        self.query_parameters = list(query_parameters or [])
        self.extra = kw


class _ScalarQueryParameter:
    def __init__(self, name, type_, value):
        self.name = name
        self.type_ = type_
        self.value = value

    def __repr__(self):  # pragma: no cover - debug aid
        return f"ScalarQueryParameter({self.name!r},{self.type_!r},{self.value!r})"


class _SchemaField:
    def __init__(self, name, field_type=None, mode=None, **kw):
        self.name = name
        self.field_type = field_type
        self.mode = mode


class _Table:
    def __init__(self, ref, schema=None):
        self.ref = ref
        self.schema = list(schema or [])


class FakeBigQueryModule:
    """Stand-in for the ``google.cloud.bigquery`` module object."""

    SourceFormat = _SourceFormat
    WriteDisposition = _WriteDisposition
    LoadJobConfig = _LoadJobConfig
    QueryJobConfig = _QueryJobConfig
    ScalarQueryParameter = _ScalarQueryParameter
    SchemaField = _SchemaField
    Table = _Table


# --- client / job doubles ----------------------------------------------------

class FakeJob:
    def __init__(self, rows=None, raises: Optional[Exception] = None,
                 state: Optional[str] = None, error_result: Optional[Any] = None):
        self._rows = rows if rows is not None else []
        self._raises = raises
        # Server-side job status, inspected by the adapter after result() raises
        # to distinguish a truly-failed load job from one that committed but
        # whose result() call timed out client-side.
        self.state = state
        self.error_result = error_result

    def reload(self):
        # No-op: fake state is already "server-side" truth.
        return None

    def result(self, max_results=None):
        if self._raises is not None:
            raise self._raises
        if max_results is None:
            return list(self._rows)
        return list(self._rows)[:max_results]


class FakeClient:
    """Records every interaction so tests can assert routing + SQL + configs."""

    def __init__(self, query_rows: Optional[List[Dict[str, Any]]] = None,
                 load_job_raises: Optional[Exception] = None,
                 load_job_committed: bool = False,
                 insert_errors: Optional[List[Any]] = None,
                 tables: Optional[Dict[str, Any]] = None):
        self._query_rows = query_rows if query_rows is not None else []
        self._load_job_raises = load_job_raises
        # When True, a raising load job still reports server-side success
        # (state DONE, no error_result) — simulating result() timing out after
        # the load actually committed.
        self._load_job_committed = load_job_committed
        self._insert_errors = insert_errors or []
        self._tables = tables or {}
        # call logs
        self.queries: List[str] = []
        self.query_configs: List[Any] = []
        self.loaded: List[Dict[str, Any]] = []       # {fq, content, job_config}
        self.inserted: List[Dict[str, Any]] = []      # {fq, rows}
        self.created_tables: List[Any] = []
        self.deleted_tables: List[str] = []

    # -- query path --
    def query(self, sql, job_config=None):
        self.queries.append(sql)
        self.query_configs.append(job_config)
        return FakeJob(rows=self._query_rows)

    # -- streaming insert path --
    def insert_rows_json(self, fq, data):
        self.inserted.append({"fq": fq, "rows": list(data)})
        return list(self._insert_errors)

    # -- bulk load-job path --
    def load_table_from_file(self, fileobj, fq, job_config=None):
        # Faithfully mirror google-cloud-bigquery's real upload contract: the
        # stream MUST be opened in a READABLE binary mode (rb / r+b / w+b). The
        # real client rejects a write-only ("wb") handle with "Cannot upload
        # files opened in text mode". The previous fake accepted ANY handle,
        # which let a real "wb" regression pass the mocks and only fail against
        # live BigQuery — enforce it here so a unit test catches it.
        mode = getattr(fileobj, "mode", None)
        if mode is not None and ("b" not in mode or ("r" not in mode and "+" not in mode)):
            raise ValueError(
                "Cannot upload files opened in text mode:  "
                "use open(filename, mode='rb') or open(filename, mode='r+b')"
            )
        try:
            fileobj.seek(0)
            content = fileobj.read()
        except Exception:
            content = None
        self.loaded.append({"fq": fq, "content": content, "job_config": job_config})
        if self._load_job_raises is not None and self._load_job_committed:
            # result() will raise, but the server-side job actually committed.
            return FakeJob(raises=self._load_job_raises, state="DONE", error_result=None)
        return FakeJob(raises=self._load_job_raises)

    # -- schema evolution helpers (best-effort no-ops) --
    def get_table(self, fq):
        return self._tables.get(fq, _Table(fq, schema=[]))

    def update_table(self, table, fields):
        self._tables[getattr(table, "ref", None)] = table
        return table

    def create_table(self, table, exists_ok=False):
        self.created_tables.append(table)
        return table

    def delete_table(self, fq, not_found_ok=False):
        self.deleted_tables.append(fq)

    def list_tables(self, dataset):
        return []


def make_adapter(warehouse_adapters, *, query_rows=None, load_job_raises=None,
                 load_job_committed=False, insert_errors=None):
    """Build a BigQueryWarehouseAdapter wired to fakes (both seams swapped)."""
    adapter = warehouse_adapters.BigQueryWarehouseAdapter()
    client = FakeClient(query_rows=query_rows, load_job_raises=load_job_raises,
                        load_job_committed=load_job_committed,
                        insert_errors=insert_errors)
    fake_bq = FakeBigQueryModule()
    # Shadow the two seams with instance attributes (dataclass has no __slots__).
    adapter._client = lambda config: client          # type: ignore[assignment]
    adapter._bq = lambda: fake_bq                     # type: ignore[assignment]
    return adapter, client
