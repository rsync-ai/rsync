#!/usr/bin/env python3
"""
Google BigQuery MCP Connector — first-class SOURCE + destination (data warehouse).

HAND-BUILT (not tool-generator output). BigQuery is NOT DB-API compatible, so all
warehouse work is delegated to the shared ``BigQueryWarehouseAdapter`` in
``warehouse_adapters.py`` (the same adapter the generated ``connector_database.py.j2``
template delegates to for ``category == data_warehouse``). This file is a thin MCP
server: identity, capability declaration, config/auth extraction, and delegation.

Optimized paths (Level 1, batch):
  - READS  : ``export`` is keyset/offset paginated (see adapter) so large tables
             resume to EOF via ``next_cursor`` / ``has_more`` instead of capping at
             a single ``LIMIT``.
  - WRITES : the destination path (``import_data`` / ``load``) prefers a bulk NDJSON
             load job when ``RSYNC_BQ_LOAD_JOB`` (or a ``bulk_load`` param) is set,
             falling back to streaming inserts on failure — never a silent drop.

The ``DestinationLoadSpec`` advertises ``load_method='bq_load_job'`` so the
orchestrator discovers the bulk path via ``load_strategy_capability()``. The
DBAPI-cursor ``_stage_*`` hooks of ``DestinationLoadMixin`` are intentionally left
unimplemented — BigQuery's bulk write is the adapter load job, not a cursor COPY,
so ``staged_upsert`` is never invoked here.

CDC (merge / offset tracking) exists in the adapter but is dormant in v1.0.0
(``supports_cdc = False``); it is exposed as a tool but not verified live yet.
"""

import sys
import os
import logging
from typing import Dict, Any

# base_connector lives in shared/mcp-connectors; warehouse_adapters in .../public.
# In the Docker image both are copied next to this file / onto PYTHONPATH; in the
# repo we walk up to find them so tests can import offline.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
_d = os.path.dirname(os.path.abspath(__file__))
for _ in range(8):
    if os.path.exists(os.path.join(_d, "base_connector.py")):
        sys.path.insert(0, _d)
    if os.path.exists(os.path.join(_d, "warehouse_adapters.py")):
        sys.path.insert(0, _d)
    _d = os.path.dirname(_d)

from base_connector import (  # noqa: E402
    BaseMCPConnector,
    DestinationLoadMixin,
    DestinationLoadSpec,
)

try:  # shared adapter registry (google-cloud-bigquery imported lazily inside it)
    from warehouse_adapters import get_data_warehouse_adapter  # type: ignore
except Exception:  # pragma: no cover - defensive
    get_data_warehouse_adapter = None  # type: ignore

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BigqueryMCPServer(DestinationLoadMixin, BaseMCPConnector):
    """MCP Server for Google BigQuery (delegates to BigQueryWarehouseAdapter)."""

    def __init__(self):
        super().__init__()

        self.connector_type = "bigquery"
        self.connector_category = "data_warehouse"
        self.supports_source = True
        self.supports_destination = True
        self.supports_cdc = False  # merge()/offsets exist but stay dormant in v1.0.0

        self.supported_formats = ["json", "jsonl", "csv"]
        self.supported_modalities = ["structured"]  # no raw-blob passthrough
        self.max_batch_size = 10000

        # Advertise the optimized bulk write so the orchestrator's capability gate
        # can pick the load-job path (actual bulk work happens in adapter.load).
        self.load_spec = DestinationLoadSpec(
            load_method="bq_load_job",
            merge_method="merge",
            supports_staging=True,
            max_batch_rows=10000,
        )

        self._warehouse_adapter = (
            get_data_warehouse_adapter("bigquery") if get_data_warehouse_adapter else None
        )

        self.log("BigQuery MCP Server initialized")

    # =========================================================================
    # CONFIG + AUTH (GCP service account / ADC — same model as gcs/google-sheets)
    # =========================================================================
    def _get_config(self, params: Dict) -> Dict:
        """Extract connection config, filling missing values from BIGQUERY_* env.

        Auth precedence is handled inside the adapter: service_account_json >
        credentials_path > GOOGLE_APPLICATION_CREDENTIALS > ADC.
        """
        config = dict(params.get("config", params) if params else {})
        if not config.get("project_id"):
            config["project_id"] = os.getenv("BIGQUERY_PROJECT_ID", "")
        if not config.get("dataset_id"):
            config["dataset_id"] = os.getenv("BIGQUERY_DATASET_ID", "")
        if not config.get("service_account_json"):
            config["service_account_json"] = os.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON", "")
        if not config.get("location"):
            config["location"] = os.getenv("BIGQUERY_LOCATION", "")
        return config

    def _require_adapter(self) -> Dict[str, Any]:
        if self._warehouse_adapter is None:
            return {
                "success": False,
                "error": "BigQuery adapter unavailable (warehouse_adapters/"
                         "google-cloud-bigquery not importable)",
            }
        return {}

    def validate_config(self, params: Dict = None) -> Dict[str, Any]:
        if not params:
            return {"valid": False, "errors": ["No configuration provided"]}
        config = self._get_config(params)
        errors = []
        warnings = []
        if not config.get("project_id"):
            errors.append("Missing required field: project_id")
        if not config.get("dataset_id"):
            errors.append("Missing required field: dataset_id")
        if not (config.get("service_account_json") or config.get("credentials_path")
                or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")):
            warnings.append(
                "No service_account_json / credentials_path provided; falling back "
                "to Application Default Credentials (ADC)"
            )
        return {"valid": len(errors) == 0, "errors": errors, "warnings": warnings}

    # =========================================================================
    # OPERATIONS (all delegate to the shared adapter)
    # =========================================================================
    def test_connection(self, params: Dict = None) -> Dict[str, Any]:
        err = self._require_adapter()
        if err:
            return err
        return self._warehouse_adapter.test_connection(self._get_config(params or {}))

    def discover_schema(self, params: Dict = None) -> Dict[str, Any]:
        err = self._require_adapter()
        if err:
            return err
        params = params or {}
        return self._warehouse_adapter.discover_schema(self._get_config(params), params)

    def export(self, params: Dict = None) -> Dict[str, Any]:
        """Optimized paginated read (keyset/offset) — delegated to the adapter."""
        err = self._require_adapter()
        if err:
            return err
        params = params or {}
        return self._warehouse_adapter.export(self._get_config(params), params)

    def import_data(self, params: Dict = None) -> Dict[str, Any]:
        """Destination write. Routes through the adapter's bulk-first ``load`` so a
        set ``RSYNC_BQ_LOAD_JOB`` upgrades the standard import path to load jobs."""
        err = self._require_adapter()
        if err:
            return err
        params = params or {}
        return self._warehouse_adapter.load(self._get_config(params), params)

    def load(self, params: Dict = None) -> Dict[str, Any]:
        """Explicit bulk load (flag-gated NDJSON load job w/ streaming fallback)."""
        err = self._require_adapter()
        if err:
            return err
        params = params or {}
        return self._warehouse_adapter.load(self._get_config(params), params)

    def merge(self, params: Dict = None) -> Dict[str, Any]:
        """CDC-style upsert/delete via BigQuery MERGE (dormant in v1.0.0)."""
        err = self._require_adapter()
        if err:
            return err
        params = params or {}
        return self._warehouse_adapter.merge(self._get_config(params), params)

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
# HTTP SERVER (Docker) — mirrors the gcs/mysql entrypoint
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

    app = FastAPI(title="BigQuery MCP Connector")
    server = BigqueryMCPServer()

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
            prefixed = f"bigquery_{method_name}"
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
        logger.info(f"🚀 Starting BigQuery MCP Server in HTTP mode on port {port}")
        uvicorn.run(app, host="0.0.0.0", port=port)
    else:
        server = BigqueryMCPServer()
        server.run()
