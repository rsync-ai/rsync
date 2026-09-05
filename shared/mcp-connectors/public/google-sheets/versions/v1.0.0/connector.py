#!/usr/bin/env python3
"""
Google Sheets MCP Connector (source + destination)

Hand-curated MCP connector that targets the Google Sheets v4 REST API.
The workbook (identified by ``spreadsheet_id``) is the database; each
sheet tab is a "table", and the first row of every tab is the header
row that defines the column order.

Destination: append-only writes via ``values:append``; idempotent tab +
header creation via ``spreadsheets:batchUpdate`` and ``values:PUT``.
Source: offset-paged reads via ``values:get`` over a row range — see
``export`` (each tab is a table, row 1 is the header, data starts row 2).

Category:        api_saas
Auth:            OAuth2 — provider "google" (see shared/mcp-connectors/oauth/providers.json)
Scopes required: https://www.googleapis.com/auth/spreadsheets
Endpoint:        https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}

The connector advertises:
  - supports_source       = True   (export reads tab rows, offset-paged)
  - supports_destination  = True
  - supports_ddl          = True   (ensure_table creates missing tabs)
  - auto_create_destination_tables = True
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

# Add parent directory for base_connector imports — same pattern as
# every other connector in this repo (postgresql, mysql, shopify, ...).
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from base_connector import BaseMCPConnector  # type: ignore  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SHEETS_API_BASE = "https://sheets.googleapis.com/v4/spreadsheets"
DEFAULT_TIMEOUT_SECONDS = 30
# Sheets API has a soft cap of 100 sheets/workbook in the UI but the
# REST API enforces no hard cap; 100 is plenty for our discovery flow.
DEFAULT_MAX_TABS = 100
# Source/export default page size when the caller doesn't specify one.
# The orchestrator's executor passes its own `limit` (10000) per batch;
# this default only applies to direct / preview invocations.
DEFAULT_EXPORT_LIMIT = 1000
# Append window — A:Z is enough for ~26 columns. We rely on the
# Google API's "Auto-Expand" behaviour: when the inserted row is wider
# than the existing grid the API grows the tab automatically. The :Z
# upper-bound is just a hint for valueRange lookup, not a hard limit.
DEFAULT_APPEND_RANGE = "A:Z"
# Metadata tab name where _rsync_pipelines ownership rows live.
# Mirrors PostgreSQL's public._rsync_pipelines convention — see
# .design/destination-namespace.md.
METADATA_TAB = "_rsync_pipelines"
METADATA_HEADER = ["pipeline_id", "tab", "created_at"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _coerce_cell_value(v: Any) -> Any:
    """Convert a Python value to something Sheets will accept in a cell.

    Sheets cells store strings, numbers, booleans. ``None`` should be
    written as empty string (otherwise the row gets shifted left when
    using ``valueInputOption=USER_ENTERED`` because gaps are not
    skipped). Nested dicts/lists become JSON strings so the GraphQL
    payloads from Shopify (e.g. ``totalPriceSet``) land losslessly
    instead of as Python repr ("{'shopMoney': ...}").
    """
    if v is None:
        return ""
    if isinstance(v, (dict, list)):
        try:
            return json.dumps(v, ensure_ascii=False, default=str)
        except (TypeError, ValueError):
            return str(v)
    if isinstance(v, bool):
        # Sheets renders TRUE/FALSE for booleans; json.dumps would emit
        # lowercase "true"/"false" as strings which becomes literal text.
        # Letting the bool pass through preserves the cell type.
        return v
    return v


def _quote_tab(tab: str) -> str:
    """Quote a sheet/tab name for use in an A1 range.

    Google's A1 notation requires single-quoting any tab name with
    spaces or special characters. We always single-quote and escape
    embedded single quotes (per the Sheets API docs) — over-quoting is
    free and never wrong, under-quoting silently swaps the request
    onto a different tab.
    """
    safe = (tab or "").replace("'", "''")
    return f"'{safe}'"


def _build_a1_range(tab: str, cells: str) -> str:
    """Build an A1 range like ``'Sheet 1'!A:Z`` for the given tab."""
    return f"{_quote_tab(tab)}!{cells}"


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


class GoogleSheetsConnector(BaseMCPConnector):
    """Google Sheets v4 REST connector — source + destination.

    Operation surface (mirrors the PostgreSQL / MySQL connectors):
      - test_connection: ping the workbook, return spreadsheet title.
      - validate_config: confirm spreadsheet_id + access_token present.
      - discover_schema: list tabs in workbook; emit header row as columns.
      - get_capabilities: standard MCP capability listing.
      - export:          read rows from a tab, offset-paged (source).
      - ensure_table:    create missing tab, write header row if needed.
      - import_data:     append rows to a tab via values:append.
    """

    def __init__(self) -> None:
        super().__init__()

        # REQUIRED: metadata
        self.connector_type = "google-sheets"
        self.display_name = "Google Sheets"
        self.connector_category = "api_saas"
        self.supports_source = True
        self.supports_destination = True
        self.supports_cdc = False
        self.supports_ddl = True
        self.auto_create_destination_tables = True

        # OAuth wiring — declares the provider so the base class's
        # generic OAuth helpers (set_oauth_token, refresh callback) can
        # find this connector's entry in oauth/providers.json. We don't
        # rely on the base's HTTP plumbing (we own our own _request),
        # but advertising the provider is required for the api-gateway
        # OAuth callback handler to know which provider's tokens to
        # store under this connector's connections.
        self.auth_type = "oauth"
        self.oauth_provider = "google"

        # Capability stats
        self.supported_formats = ["json"]
        # Sheets values:append accepts up to 10MB per request — be
        # conservative for v1 to avoid 413s when rows are wide.
        self.max_batch_size = 1000

        self.log("Google Sheets MCP Server initialized")

    # ----- Config helpers --------------------------------------------------

    @staticmethod
    def _get_config(params: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Unwrap config from the tool-call wrapper.

        ``_handle_tool_call`` passes the full arguments dict; the real
        connection config sits under a ``"config"`` key. Accept both
        shapes so direct stdio invocations and tools/call invocations
        both work — mirrors the Shopify connector.
        """
        if not params:
            return {}
        if "config" in params and isinstance(params["config"], dict):
            return dict(params["config"])
        return dict(params)

    @staticmethod
    def _resolve_token(config: Dict[str, Any]) -> Optional[str]:
        """Pull the OAuth access token out of config (or env fallback).

        Accepts a handful of common spellings the orchestrator might
        use depending on auth path:
          - ``access_token``        — populated post-OAuth callback
          - ``token``               — legacy / manual-paste path
          - ``oauth_token_id``      — when the orchestrator delegates
                                      token lookup to TokenManager
        Falls back to ``GOOGLE_ACCESS_TOKEN`` env var for dev / local
        testing where the OAuth flow hasn't been wired yet.
        """
        for key in ("access_token", "token", "oauth_token_id"):
            v = config.get(key)
            if v:
                return str(v)
        env = os.environ.get("GOOGLE_ACCESS_TOKEN")
        if env:
            return env
        return None

    @staticmethod
    def _resolve_spreadsheet_id(config: Dict[str, Any]) -> Optional[str]:
        """Pull the spreadsheet id from config.

        Accepts ``spreadsheet_id`` (canonical) or ``spreadsheetId`` (the
        Google API field name) so docs that quote either spelling
        still work.
        """
        for key in ("spreadsheet_id", "spreadsheetId"):
            v = config.get(key)
            if v:
                return str(v).strip()
        env = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID")
        if env:
            return env.strip()
        return None

    # ----- HTTP plumbing ---------------------------------------------------

    def _request(
        self,
        method: str,
        url: str,
        config: Dict[str, Any],
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> Tuple[int, Any, str]:
        """Execute an HTTPS request and return (status, json|None, error_text).

        Centralises the access-token wiring, error envelope parsing
        (Google's ``{"error": {"code": ..., "message": ...}}`` shape),
        and connection/timeout exception handling so the operation
        methods read top-down.

        Returns:
            status:    HTTP status code (or 0 if transport failed)
            payload:   Parsed JSON dict (None when not JSON)
            error_text: Human-friendly error message, "" on success
        """
        token = self._resolve_token(config)
        if not token:
            return 0, None, (
                "Missing OAuth access token. Set config.access_token "
                "(populated by the Google OAuth callback) or the "
                "GOOGLE_ACCESS_TOKEN env var."
            )

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        if json_body is not None:
            headers["Content-Type"] = "application/json"

        try:
            with httpx.Client(timeout=timeout) as client:
                resp = client.request(
                    method=method.upper(),
                    url=url,
                    headers=headers,
                    params=params,
                    json=json_body,
                )
        except httpx.TimeoutException as e:
            return 0, None, f"Sheets API request timed out: {e}"
        except httpx.RequestError as e:
            return 0, None, f"Sheets API transport error: {e}"

        # Best-effort JSON parse — Sheets returns JSON for both success
        # and error envelopes.
        payload: Any = None
        try:
            payload = resp.json()
        except ValueError:
            payload = None

        if resp.status_code >= 400:
            err_msg = ""
            if isinstance(payload, dict) and isinstance(payload.get("error"), dict):
                inner = payload["error"]
                err_msg = str(inner.get("message") or "")
                code = inner.get("code")
                status_label = inner.get("status")
                if code or status_label:
                    err_msg = f"{err_msg} (code={code}, status={status_label})".strip()
            if not err_msg:
                err_msg = f"HTTP {resp.status_code}: {(resp.text or '')[:200]}"
            return resp.status_code, payload, err_msg

        return resp.status_code, payload, ""

    # ----- Core operations -------------------------------------------------

    def validate_config(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Static validation — required fields present without an API call."""
        cfg = self._get_config(params)
        errors: List[str] = []
        if not self._resolve_spreadsheet_id(cfg):
            errors.append(
                "Missing required field 'spreadsheet_id' (copy from the "
                "Google Sheets URL: docs.google.com/spreadsheets/d/{ID}/edit)."
            )
        if not self._resolve_token(cfg):
            errors.append(
                "Missing required field 'access_token' (obtained via the "
                "Google OAuth flow with scope "
                "https://www.googleapis.com/auth/spreadsheets)."
            )
        return {
            "valid": len(errors) == 0,
            "errors": errors,
            "warnings": [],
        }

    def test_connection(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Ping the workbook and return its title.

        Implementation:
            GET /v4/spreadsheets/{id}?fields=spreadsheetId,properties.title

        Cheap, scope-only check — confirms the access token has read
        access to the workbook and that the ID is valid. Doesn't
        list any tabs / values so it works on workbooks with hundreds
        of sheets without timing out.
        """
        cfg = self._get_config(params)
        spreadsheet_id = self._resolve_spreadsheet_id(cfg)
        if not spreadsheet_id:
            return {
                "success": False,
                "error": "Missing required field 'spreadsheet_id'.",
            }

        url = f"{SHEETS_API_BASE}/{spreadsheet_id}"
        status, payload, err = self._request(
            "GET",
            url,
            cfg,
            params={"fields": "spreadsheetId,properties.title"},
        )
        if err:
            # Surface a friendlier hint for the two most common failures.
            hint = ""
            if status == 401:
                hint = (
                    " — token is missing or expired; re-run the Google "
                    "OAuth flow"
                )
            elif status == 403:
                hint = (
                    " — the connected Google account doesn't have access "
                    "to this workbook (share it with the account or grant "
                    "the spreadsheets scope)"
                )
            elif status == 404:
                hint = (
                    " — workbook not found; double-check spreadsheet_id "
                    "matches the URL"
                )
            return {"success": False, "error": err + hint}

        title = ""
        if isinstance(payload, dict):
            props = payload.get("properties") or {}
            if isinstance(props, dict):
                title = str(props.get("title") or "")
        return {
            "success": True,
            "message": "Connection successful",
            "spreadsheet_title": title,
        }

    def discover_schema(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """List tabs in the workbook + the header row of each tab.

        Each tab becomes a "table" in the discovery contract. The
        column list is the first row of the tab — Sheets' canonical
        header convention. If a tab is empty (no header row written
        yet), ``columns`` is an empty list and the destination can
        synthesise it from the source's discover_schema response in
        ensure_table().

        We deliberately skip row_count exact-detection because counting
        rows in Sheets requires reading the values grid which is
        expensive on large workbooks — the orchestrator never relies on
        it for destination connectors.
        """
        start_ms = int(time.time() * 1000)
        cfg = self._get_config(params or {})

        result: Dict[str, Any] = {
            "schema_version": "2.0",
            "connector_type": self.connector_type,
            "connector_version": getattr(self, "version", "1.0.0"),
            "total_tables_available": 0,
            "total_tables_discovered": 0,
            "discovery_duration_ms": 0,
            "overall_status": "success",
            "warnings_objects": [],
            "warnings_messages": [],
            "tables": [],
        }

        spreadsheet_id = self._resolve_spreadsheet_id(cfg)
        if not spreadsheet_id:
            result["overall_status"] = "failed"
            msg = "Missing required field 'spreadsheet_id'"
            result["warnings_objects"].append({
                "category": "config_missing", "severity": "error", "message": msg,
            })
            result["warnings_messages"].append(msg)
            result["discovery_duration_ms"] = int(time.time() * 1000) - start_ms
            return result

        try:
            max_tabs = int((params or {}).get("max_tables", DEFAULT_MAX_TABS))
        except Exception:
            max_tabs = DEFAULT_MAX_TABS
        if max_tabs <= 0:
            max_tabs = DEFAULT_MAX_TABS

        # Step 1: list tab properties (cheap — no cell payload).
        url = f"{SHEETS_API_BASE}/{spreadsheet_id}"
        _, payload, err = self._request(
            "GET",
            url,
            cfg,
            params={"fields": "spreadsheetId,properties.title,sheets.properties"},
        )
        if err:
            result["overall_status"] = "failed"
            result["warnings_objects"].append({
                "category": "connection_error", "severity": "error", "message": err,
            })
            result["warnings_messages"].append(err)
            result["discovery_duration_ms"] = int(time.time() * 1000) - start_ms
            return result

        sheets = []
        if isinstance(payload, dict):
            sheets = payload.get("sheets") or []
        if not isinstance(sheets, list):
            sheets = []

        # Step 2: for each tab, fetch row 1 to derive header columns.
        # Tabs without any data return an empty values list — we surface
        # that as columns=[] so the orchestrator can write the header
        # row on the first import.
        tabs_meta: List[Dict[str, Any]] = []
        for sheet in sheets[:max_tabs]:
            props = (sheet or {}).get("properties") or {}
            title = str(props.get("title") or "")
            if not title:
                continue
            # Skip the metadata tab from discovery so the table picker
            # doesn't surface internal bookkeeping as a writable target.
            if title == METADATA_TAB:
                continue
            tabs_meta.append({
                "title": title,
                "sheet_id": props.get("sheetId"),
                "row_count": (props.get("gridProperties") or {}).get("rowCount"),
                "column_count": (props.get("gridProperties") or {}).get("columnCount"),
            })

        result["total_tables_available"] = len(tabs_meta)

        for tab in tabs_meta:
            tab_title = tab["title"]
            header_url = f"{SHEETS_API_BASE}/{spreadsheet_id}/values/{_build_a1_range(tab_title, '1:1')}"
            _, h_payload, h_err = self._request("GET", header_url, cfg)
            columns: List[Dict[str, Any]] = []
            if h_err:
                # Per-tab failure shouldn't take the whole response down.
                result["warnings_objects"].append({
                    "category": "catalog_error",
                    "severity": "warning",
                    "message": f"Could not read header row for tab '{tab_title}': {h_err}",
                    "table": tab_title,
                    "subsystem": "columns",
                })
                result["warnings_messages"].append(
                    f"Could not read header row for tab '{tab_title}': {h_err}"
                )
            else:
                values = []
                if isinstance(h_payload, dict):
                    values = h_payload.get("values") or []
                header_row = values[0] if values and isinstance(values[0], list) else []
                for col_name in header_row:
                    name = str(col_name).strip()
                    if not name:
                        continue
                    columns.append({
                        "name": name,
                        "type": "string",  # Sheets cells are stringly-typed at the API surface
                        "source_type": "Sheets/Cell",
                    })

            result["tables"].append({
                "name": tab_title,
                "schema": "sheets",
                "columns": columns,
                "row_count": -1,  # unknown — see docstring
                "is_exact_count": False,
                "discovery_status": "complete",
                "type": "table",
            })

        result["total_tables_discovered"] = len(result["tables"])
        result["discovery_duration_ms"] = int(time.time() * 1000) - start_ms
        return result

    def get_capabilities(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Standard MCP capability listing for auto-dispatch."""
        runtime_version = (
            os.getenv("MCP_CONNECTOR_VERSION")
            or os.getenv("CONNECTOR_VERSION")
            or ""
        ).strip()
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
            "operations": [
                {
                    "name": "test_connection",
                    "method": "google-sheets_test_connection",
                    "type": "core",
                    "description": "Verify access to the configured workbook",
                },
                {
                    "name": "validate_config",
                    "method": "google-sheets_validate_config",
                    "type": "core",
                    "description": "Validate config without calling the API",
                },
                {
                    "name": "discover_schema",
                    "method": "google-sheets_discover_schema",
                    "type": "core",
                    "description": "List tabs (treated as tables) and their header columns",
                },
                {
                    "name": "get_capabilities",
                    "method": "google-sheets_get_capabilities",
                    "type": "core",
                    "description": "Return connector capabilities",
                },
                {
                    "name": "export",
                    "method": "google-sheets_export",
                    "type": "source",
                    "description": "Read rows from a tab (offset-paged); row 1 is the header",
                },
                {
                    "name": "ensure_table",
                    "method": "google-sheets_ensure_table",
                    "type": "destination",
                    "description": "Create missing tab and write the header row",
                },
                {
                    "name": "import_data",
                    "method": "google-sheets_import_data",
                    "type": "destination",
                    "description": "Append rows to a tab",
                },
            ],
            "capabilities": {
                "max_batch_size": self.max_batch_size,
                "supported_formats": self.supported_formats,
                "supports_cdc": self.supports_cdc,
            },
        }

    # ----- Destination helpers ---------------------------------------------

    def _list_sheets(
        self,
        spreadsheet_id: str,
        config: Dict[str, Any],
    ) -> Tuple[Dict[str, Dict[str, Any]], str]:
        """Return {tab_title: properties} for every sheet in the workbook.

        Used by ensure_table to detect tab existence without re-walking
        ``discover_schema``. Returns ({}, error_msg) on failure.
        """
        url = f"{SHEETS_API_BASE}/{spreadsheet_id}"
        _, payload, err = self._request(
            "GET",
            url,
            config,
            params={"fields": "sheets.properties"},
        )
        if err:
            return {}, err
        sheets_map: Dict[str, Dict[str, Any]] = {}
        if isinstance(payload, dict):
            for s in payload.get("sheets") or []:
                props = (s or {}).get("properties") or {}
                title = str(props.get("title") or "")
                if title:
                    sheets_map[title] = props
        return sheets_map, ""

    def _read_header_row(
        self,
        spreadsheet_id: str,
        tab: str,
        config: Dict[str, Any],
    ) -> Tuple[List[str], str]:
        """Read the first row of a tab. Returns ([], err) on failure."""
        url = f"{SHEETS_API_BASE}/{spreadsheet_id}/values/{_build_a1_range(tab, '1:1')}"
        _, payload, err = self._request("GET", url, config)
        if err:
            return [], err
        values = []
        if isinstance(payload, dict):
            values = payload.get("values") or []
        if values and isinstance(values[0], list):
            return [str(c) for c in values[0]], ""
        return [], ""

    def _add_sheet(
        self,
        spreadsheet_id: str,
        tab: str,
        config: Dict[str, Any],
    ) -> str:
        """Create a new tab via batchUpdate. Returns "" on success, else err."""
        url = f"{SHEETS_API_BASE}/{spreadsheet_id}:batchUpdate"
        body = {
            "requests": [
                {
                    "addSheet": {
                        "properties": {
                            "title": tab,
                        }
                    }
                }
            ]
        }
        _, _, err = self._request("POST", url, config, json_body=body)
        return err

    def _write_header_row(
        self,
        spreadsheet_id: str,
        tab: str,
        columns: List[str],
        config: Dict[str, Any],
    ) -> str:
        """Write the header row to A1:.. via values.update.

        Uses RAW input option so column names are stored verbatim (no
        leading "=" interpretation as a formula). Returns "" on
        success, else err.
        """
        url = (
            f"{SHEETS_API_BASE}/{spreadsheet_id}/values/"
            f"{_build_a1_range(tab, '1:1')}"
        )
        body = {"values": [[str(c) for c in columns]]}
        _, _, err = self._request(
            "PUT",
            url,
            config,
            params={"valueInputOption": "RAW"},
            json_body=body,
        )
        return err

    def _ensure_metadata_tab(
        self,
        spreadsheet_id: str,
        sheets_map: Dict[str, Dict[str, Any]],
        config: Dict[str, Any],
        pipeline_id: str,
        tab: str,
    ) -> None:
        """Best-effort: write a _rsync_pipelines ownership row.

        Mirrors the PostgreSQL connector's public._rsync_pipelines
        table. Failure is non-fatal — we'd rather land data than abort
        a pipeline because the metadata tab couldn't be touched.
        """
        if not pipeline_id:
            return

        if METADATA_TAB not in sheets_map:
            err = self._add_sheet(spreadsheet_id, METADATA_TAB, config)
            if err:
                self.log(
                    f"Could not create metadata tab {METADATA_TAB}: {err}",
                    level="warning",
                )
                return
            # Write the header row.
            self._write_header_row(spreadsheet_id, METADATA_TAB, METADATA_HEADER, config)

        # Read existing metadata to skip duplicates — Sheets append doesn't
        # support ON CONFLICT semantics so we dedupe on read.
        url = (
            f"{SHEETS_API_BASE}/{spreadsheet_id}/values/"
            f"{_build_a1_range(METADATA_TAB, 'A:C')}"
        )
        _, payload, err = self._request("GET", url, config)
        if err:
            self.log(f"Could not read metadata tab: {err}", level="warning")
            return
        existing = []
        if isinstance(payload, dict):
            existing = payload.get("values") or []
        for row in existing[1:]:  # skip header
            if isinstance(row, list) and len(row) >= 1 and row[0] == pipeline_id:
                return  # already recorded

        append_url = (
            f"{SHEETS_API_BASE}/{spreadsheet_id}/values/"
            f"{_build_a1_range(METADATA_TAB, 'A:C')}:append"
        )
        body = {
            "values": [
                [
                    pipeline_id,
                    tab,
                    time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                ]
            ]
        }
        _, _, append_err = self._request(
            "POST",
            append_url,
            config,
            params={
                "valueInputOption": "USER_ENTERED",
                "insertDataOption": "INSERT_ROWS",
            },
            json_body=body,
        )
        if append_err:
            self.log(f"Could not append metadata row: {append_err}", level="warning")

    # ----- Destination operations -----------------------------------------

    def ensure_table(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create / migrate the destination tab + header row.

        The orchestrator calls this before the first import_data batch
        with the column list pulled from discover_schema(). Idempotent
        — safe to call on every run.

        Behaviour:
          * If the tab does not exist: create it via batchUpdate(addSheet),
            then write the header row.
          * If the tab exists with no header row (empty A1:?1): write
            the header row.
          * If the tab exists with a header row that matches: no-op.
          * If the tab exists with a header row that DIFFERS: do NOT
            overwrite (would corrupt existing data). Log a warning and
            return the existing header so the caller can decide.

        Params:
            config:        Connection config (spreadsheet_id + access_token).
            table:         Tab name to ensure.
            columns:       List of column names to write as the header.
            column_types:  (Ignored — Sheets cells are stringly-typed.)
            pipeline_id:   Optional — recorded in _rsync_pipelines.
        """
        params = params or {}
        config = self._get_config(params)
        tab = str(params.get("table") or params.get("endpoint") or "").strip()
        if not tab:
            return {"success": False, "error": "Missing required parameter 'table' (tab name)"}

        cols_raw = params.get("columns") or []
        columns: List[str] = []
        if isinstance(cols_raw, list):
            for c in cols_raw:
                cc = str(c or "").strip()
                if cc:
                    columns.append(cc)
        if not columns:
            return {
                "success": False,
                "error": "ensure_table requires non-empty 'columns' to write the header row",
            }

        spreadsheet_id = self._resolve_spreadsheet_id(config)
        if not spreadsheet_id:
            return {"success": False, "error": "Missing required config 'spreadsheet_id'"}

        # Step 1: enumerate existing tabs.
        sheets_map, err = self._list_sheets(spreadsheet_id, config)
        if err:
            return {"success": False, "error": f"Could not list workbook tabs: {err}"}

        created_tab = False
        if tab not in sheets_map:
            # Step 2a: create the tab.
            add_err = self._add_sheet(spreadsheet_id, tab, config)
            if add_err:
                return {"success": False, "error": f"Could not create tab '{tab}': {add_err}"}
            created_tab = True
            sheets_map[tab] = {"title": tab}

        # Step 2b: read the existing header row (empty if new).
        existing_header, h_err = self._read_header_row(spreadsheet_id, tab, config)
        if h_err:
            return {"success": False, "error": f"Could not read header row for '{tab}': {h_err}"}

        header_written = False
        header_mismatch = False
        if not existing_header:
            w_err = self._write_header_row(spreadsheet_id, tab, columns, config)
            if w_err:
                return {
                    "success": False,
                    "error": f"Could not write header row for '{tab}': {w_err}",
                }
            header_written = True
        elif [c.strip() for c in existing_header if c] == [c.strip() for c in columns if c]:
            # Headers already match — nothing to do.
            pass
        else:
            # Existing header differs. Do NOT overwrite (that would
            # corrupt every row already in the tab). Surface a warning
            # in the response so the caller can recover (rename the
            # tab, drop+recreate, or live with the mismatch).
            header_mismatch = True
            self.log(
                f"ensure_table: tab '{tab}' has existing header that differs from "
                f"requested. Existing={existing_header}, requested={columns}. "
                f"Refusing to overwrite to preserve existing rows.",
                level="warning",
            )

        # Step 3: record ownership in the metadata tab. Best-effort.
        pipeline_id = (
            params.get("pipeline_id")
            or (params.get("config") or {}).get("pipeline_id")
            or ""
        )
        try:
            self._ensure_metadata_tab(spreadsheet_id, sheets_map, config, str(pipeline_id).strip(), tab)
        except Exception as e:
            self.log(f"_ensure_metadata_tab raised: {e}", level="warning")

        warnings: List[str] = []
        if header_mismatch:
            warnings.append(
                f"Tab '{tab}' has an existing header that differs from the requested "
                f"columns. Refusing to overwrite. Existing={existing_header}; "
                f"requested={columns}."
            )

        return {
            "success": True,
            "table": tab,
            "schema": "sheets",
            "columns": existing_header if existing_header else columns,
            "created_tab": created_tab,
            "header_written": header_written,
            "header_mismatch": header_mismatch,
            "warnings": warnings,
        }

    def import_data(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Append rows to a tab via values:append.

        Implementation: POST .../values/{tab}!A:Z:append with
        ``valueInputOption=USER_ENTERED`` and
        ``insertDataOption=INSERT_ROWS``. INSERT_ROWS forces Google to
        push existing rows down and insert ours rather than overwriting
        whatever's below the last filled row — important when a user
        has notes or summary rows below the data block.

        Row order is determined by the tab's header row (read fresh on
        every call so concurrent header edits are tolerated). Missing
        values become empty strings; nested dict/list values are
        JSON-serialised by ``_coerce_cell_value``.

        Params:
            config:        Connection config.
            table:         Tab name.
            data:          List of row dicts.
            column_types:  Ignored — Sheets is stringly-typed.
            columns:       Optional explicit column order; falls back
                           to the tab's header row when omitted.
        """
        params = params or {}
        config = self._get_config(params)
        tab = str(params.get("table") or params.get("endpoint") or "").strip()
        if not tab:
            return {"success": False, "error": "Missing required parameter 'table' (tab name)"}

        data = params.get("data") or []
        if not isinstance(data, list):
            return {"success": False, "error": "'data' must be a list of row dicts"}
        if not data:
            return {"success": True, "rows_inserted": 0, "message": "No data to import"}

        spreadsheet_id = self._resolve_spreadsheet_id(config)
        if not spreadsheet_id:
            return {"success": False, "error": "Missing required config 'spreadsheet_id'"}

        # Resolve column order: explicit > tab header > first row's keys.
        explicit_cols = params.get("columns") or []
        columns: List[str] = []
        if isinstance(explicit_cols, list) and explicit_cols:
            columns = [str(c).strip() for c in explicit_cols if str(c).strip()]
        else:
            header, h_err = self._read_header_row(spreadsheet_id, tab, config)
            if h_err:
                return {
                    "success": False,
                    "error": f"Could not read header row for '{tab}': {h_err}",
                }
            columns = [c for c in header if c]

        if not columns:
            # No header row and caller didn't pass columns — fall back
            # to the first row's keys. Preserves insertion order on
            # Python 3.7+.
            first = next((r for r in data if isinstance(r, dict)), None)
            if first:
                columns = list(first.keys())
        if not columns:
            return {
                "success": False,
                "error": (
                    f"No header row in tab '{tab}' and no 'columns' param "
                    "provided. Call ensure_table first or pass 'columns' "
                    "explicitly so we know the cell order."
                ),
            }

        # Convert each row dict into a positional list, batching to
        # max_batch_size to stay under the 10MB request cap.
        rows_inserted = 0
        for chunk_start in range(0, len(data), self.max_batch_size):
            chunk = data[chunk_start:chunk_start + self.max_batch_size]
            values: List[List[Any]] = []
            for row in chunk:
                if not isinstance(row, dict):
                    # Defensive: orchestrator may rarely pass a list-of-lists
                    # instead of list-of-dicts (e.g. from CSV staging).
                    if isinstance(row, list):
                        values.append([_coerce_cell_value(v) for v in row])
                    continue
                values.append([_coerce_cell_value(row.get(col)) for col in columns])

            if not values:
                continue

            url = (
                f"{SHEETS_API_BASE}/{spreadsheet_id}/values/"
                f"{_build_a1_range(tab, DEFAULT_APPEND_RANGE)}:append"
            )
            body = {
                "majorDimension": "ROWS",
                "values": values,
            }
            _, payload, err = self._request(
                "POST",
                url,
                config,
                params={
                    "valueInputOption": "USER_ENTERED",
                    "insertDataOption": "INSERT_ROWS",
                },
                json_body=body,
            )
            if err:
                return {
                    "success": False,
                    "rows_inserted": rows_inserted,
                    "error": f"values:append failed for tab '{tab}': {err}",
                }

            # Sheets returns ``updates.updatedRows`` — fall back to the
            # request size when the response shape is unexpected.
            appended = len(values)
            if isinstance(payload, dict):
                updates = payload.get("updates") or {}
                ur = updates.get("updatedRows")
                if isinstance(ur, int) and ur > 0:
                    appended = ur
            rows_inserted += appended

        return {
            "success": True,
            "rows_inserted": rows_inserted,
            "table": tab,
            "columns": columns,
        }

    def export(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Read rows from a tab — source/extract operation.

        The orchestrator's executor drives this with offset-based paging:
        it calls export repeatedly with ``limit`` + ``offset`` and keeps
        going while a batch comes back full (``row_count == limit``).
        Each tab is a table; row 1 is the header that names the columns;
        data rows are everything below it.

        Implementation:
            GET /v4/spreadsheets/{id}/values/'{tab}'!{start}:{end}
            with valueRenderOption=UNFORMATTED_VALUE so numbers/booleans
            keep their type, and dateTimeRenderOption=FORMATTED_STRING so
            dates come back human-readable instead of as serial numbers.

        We request a ROW range (``'{tab}'!2:1001``) rather than a
        column-bounded one (``A:Z``) so tabs wider than 26 columns are
        not silently truncated — the API returns every populated column.

        Params:
            config:  Connection config (spreadsheet_id + access_token).
            table:   Tab name (aliases: tab, sheet, resource, endpoint,
                     object). A ``schema.tab`` qualifier is tolerated —
                     the schema prefix is stripped.
            limit:   Max rows to return this call (aliases: batch_size,
                     max_records, max_rows). Defaults to
                     DEFAULT_EXPORT_LIMIT.
            offset:  Number of data rows to skip (0 = first data row).

        Returns the BaseMCPConnector export contract:
            {success, data: List[dict], columns: List[str],
             row_count: int, has_more: bool, error?: str}
        """
        params = params or {}
        config = self._get_config(params)

        # Resolve the tab name from any of the keys the executor / param
        # normaliser might use (api_saas connectors often see 'table'
        # rewritten to 'endpoint'/'resource').
        tab = ""
        for key in ("table", "tab", "sheet", "resource", "endpoint", "object"):
            v = params.get(key)
            if v:
                tab = str(v).strip()
                break
        # Tolerate a qualified name like "sheets.Customers" — the tab is
        # the last segment (mirrors the DB / REST connectors).
        if "." in tab:
            tab = tab.rsplit(".", 1)[-1].strip()
        if not tab:
            return {"success": False, "error": "Missing required parameter 'table' (tab name)"}
        if tab == METADATA_TAB:
            return {
                "success": False,
                "error": (
                    f"'{METADATA_TAB}' is internal bookkeeping and cannot be "
                    "used as a source tab."
                ),
            }

        spreadsheet_id = self._resolve_spreadsheet_id(config)
        if not spreadsheet_id:
            return {"success": False, "error": "Missing required config 'spreadsheet_id'"}

        # Pagination params — honour whatever the executor sends, fall
        # back to a sane default.
        def _as_int(*keys: str, default: int) -> int:
            for k in keys:
                if params.get(k) is not None:
                    try:
                        return int(params[k])
                    except (TypeError, ValueError):
                        pass
            return default

        limit = _as_int("limit", "batch_size", "max_records", "max_rows", default=DEFAULT_EXPORT_LIMIT)
        if limit <= 0:
            limit = DEFAULT_EXPORT_LIMIT
        offset = _as_int("offset", "start", default=0)
        if offset < 0:
            offset = 0

        # Step 1: header row → column order. discover_schema uses the
        # same convention (row 1 = header).
        header, h_err = self._read_header_row(spreadsheet_id, tab, config)
        if h_err:
            return {"success": False, "error": f"Could not read header row for '{tab}': {h_err}"}
        columns = [str(c).strip() for c in (header or []) if str(c).strip()]

        # Step 2: read the data window. Rows are 1-based and row 1 is the
        # header, so the first data row is row 2.
        start_row = offset + 2
        end_row = offset + 1 + limit
        value_render = str(params.get("value_render_option") or "UNFORMATTED_VALUE")
        datetime_render = str(params.get("date_time_render_option") or "FORMATTED_STRING")
        url = (
            f"{SHEETS_API_BASE}/{spreadsheet_id}/values/"
            f"{_build_a1_range(tab, f'{start_row}:{end_row}')}"
        )
        _, payload, err = self._request(
            "GET",
            url,
            config,
            params={
                "valueRenderOption": value_render,
                "dateTimeRenderOption": datetime_render,
                "majorDimension": "ROWS",
            },
        )
        if err:
            return {"success": False, "error": f"values:get failed for tab '{tab}': {err}"}

        values: List[Any] = []
        if isinstance(payload, dict):
            values = payload.get("values") or []
        if not isinstance(values, list):
            values = []

        # If the tab has no header row, synthesise positional column names
        # from the widest data row so data still flows.
        if not columns:
            widest = 0
            for row in values:
                if isinstance(row, list):
                    widest = max(widest, len(row))
            columns = [f"column_{i + 1}" for i in range(widest)]

        # Map each row (a positional list) to a dict keyed by the header.
        # Missing trailing cells become empty strings; cells beyond the
        # header width are dropped (the header defines the schema).
        data: List[Dict[str, Any]] = []
        for row in values:
            if not isinstance(row, list):
                continue
            data.append({col: (row[i] if i < len(row) else "") for i, col in enumerate(columns)})

        # Offset-paging continuation signal: a full window almost
        # certainly means there's more (the executor keeps calling while
        # row_count == limit). The Sheets API trims trailing empty rows,
        # so a short window is a reliable end-of-data marker.
        has_more = len(values) >= limit

        return {
            "success": True,
            "data": data,
            "columns": columns,
            "row_count": len(data),
            "has_more": has_more,
            "table": tab,
        }


# ---------------------------------------------------------------------------
# HTTP server entrypoint — matches the postgresql / shopify pattern so the
# orchestrator's container runtime (or the api-gateway connector spawner)
# can invoke us over JSON-RPC at /mcp + REST shims at /test_connection etc.
# ---------------------------------------------------------------------------


def _build_app(connector: "GoogleSheetsConnector"):
    """Build the FastAPI app — factored so tests can drive it in-process."""
    from fastapi import FastAPI
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

    app = FastAPI()

    @app.post("/test_connection")
    def api_test_connection(params: dict = {}):  # noqa: B008
        return connector.test_connection(params)

    @app.post("/validate_config")
    def api_validate_config(params: dict = {}):  # noqa: B008
        return connector.validate_config(params)

    @app.post("/discover_schema")
    def api_discover_schema(params: dict = {}):  # noqa: B008
        return connector.discover_schema(params)

    @app.post("/export")
    def api_export(params: dict = {}):  # noqa: B008
        return connector.export(params)

    @app.post("/get_capabilities")
    def api_get_capabilities(params: dict = {}):  # noqa: B008
        return connector.get_capabilities(params)

    @app.post("/ensure_table")
    def api_ensure_table(params: dict = {}):  # noqa: B008
        return connector.ensure_table(params)

    @app.post("/import_data")
    def api_import_data(params: dict = {}):  # noqa: B008
        return connector.import_data(params)

    @app.post("/mcp")
    def api_mcp(request: dict):
        """JSON-RPC entrypoint used by the orchestrator."""
        try:
            request_dict = request if isinstance(request, dict) else {}
            if "jsonrpc" not in request_dict:
                request_dict = {
                    "jsonrpc": "2.0",
                    "id": request_dict.get("id", 1),
                    "method": request_dict.get("method"),
                    "params": request_dict.get("params") or {},
                }
            return connector.handle_request(request_dict)
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": (request or {}).get("id", 1) if isinstance(request, dict) else 1,
                "error": {"code": -32000, "message": str(e)},
            }

    @app.get("/health")
    def health():
        return {"status": "ok"}

    return app


if __name__ == "__main__":
    connector = GoogleSheetsConnector()

    http_mode = os.getenv("MCP_HTTP_MODE", "false").lower() == "true"
    port = int(os.getenv("MCP_PORT", os.getenv("PORT", "8000")))

    if http_mode or os.getenv("DOCKER_CONTAINER"):
        try:
            import uvicorn
            app = _build_app(connector)
            logger.info(
                "Starting Google Sheets connector (HTTP) on port %d", port
            )
            uvicorn.run(app, host="0.0.0.0", port=port)
        except ImportError:
            # Fall back to stdio JSON-RPC if FastAPI is not installed.
            connector.run()
    else:
        # Smoke tests — only run when GOOGLE_ACCESS_TOKEN and
        # GOOGLE_SHEETS_SPREADSHEET_ID are present. Otherwise the
        # module just exits cleanly so CI can import it.
        smoke_token = os.environ.get("GOOGLE_ACCESS_TOKEN")
        smoke_sheet = os.environ.get("GOOGLE_SHEETS_SPREADSHEET_ID")
        if smoke_token and smoke_sheet:
            cfg = {
                "spreadsheet_id": smoke_sheet,
                "access_token": smoke_token,
            }
            logger.info("Smoke: test_connection -> %s", connector.test_connection({"config": cfg}))
            logger.info("Smoke: discover_schema -> %s", connector.discover_schema({"config": cfg}))
        else:
            connector.run()
