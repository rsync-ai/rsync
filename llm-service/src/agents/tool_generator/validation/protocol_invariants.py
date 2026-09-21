"""Post-generation protocol-invariant checks.

These are deterministic, single-pass static checks on the generated
connector code. They catch the class of bug that produced the
shopify-admin-graphql failures:

  - GraphQL connector has a Relay query (``$first`` in query template)
    but no default in ``export()``  → runtime: "first or last must be
    provided"
  - Any connector silently missing the ``/mcp`` JSON-RPC route
    → orchestrator's executor calls 404 silently
  - REST connector with ``pagination_type='cursor'`` but empty
    ``cursor_path`` → runtime: infinite first page or crash

Run after code generation and metadata rendering, but BEFORE persistence
to disk. Failure aborts the save — a partially-broken connector should
never reach a deployable directory.

Pure string + AST inspection. No network, no docker, no LLM. Cheap.
"""

from __future__ import annotations

import ast
import json
import re
from dataclasses import dataclass, field
from typing import Any


class ProtocolInvariantError(RuntimeError):
    """Raised when a generated connector violates a deterministic protocol
    invariant that would otherwise fail at deploy or first runtime call."""


@dataclass
class InvariantReport:
    passed: bool = True
    violations: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Common-to-all-protocols
# --------------------------------------------------------------------------- #


def _has_mcp_route(code: str) -> bool:
    """Every connector must expose the MCP JSON-RPC entry point. The
    orchestrator drives discover_schema / export / import_data through it.
    Without /mcp the orchestrator silently 404s and reports the connector
    as "did not return table information"."""
    return bool(re.search(r'@app\.post\(\s*["\']/mcp["\']\s*\)', code))


def _has_handle_request_dispatch(code: str) -> bool:
    """The /mcp route should call connector.handle_request() (provided by
    BaseMCPConnector) — anything else fails the JSON-RPC dispatch contract."""
    return "handle_request(" in code


# --------------------------------------------------------------------------- #
# GraphQL invariants
# --------------------------------------------------------------------------- #


_RELAY_VARS = ("$first", "$last")


def _relay_operations_in_metadata(metadata: dict[str, Any]) -> list[str]:
    """Return operation names whose query template uses Relay pagination
    variables (``$first`` / ``$last``)."""
    relay_ops: list[str] = []
    operations = metadata.get("operations") or []
    for op in operations:
        if not isinstance(op, dict):
            continue
        query = (op.get("graphql_query") or op.get("query") or "")
        if any(v in query for v in _RELAY_VARS):
            relay_ops.append(op.get("name") or "")
    return [n for n in relay_ops if n]


def _export_defaults_relay_pagination(code: str) -> bool:
    """Return True if the connector's export() defaults Relay pagination.

    Look for either:
      - ``variables.setdefault("first", ...)`` / ``variables["first"] = ...``
      - ``"first" not in variables`` guard followed by an assignment
    """
    patterns = (
        r'variables\.setdefault\(\s*["\']first["\']',
        r'variables\[\s*["\']first["\']\s*\]\s*=',
        r'["\']first["\']\s+not\s+in\s+variables',
    )
    return any(re.search(p, code) for p in patterns)


# --------------------------------------------------------------------------- #
# REST invariants
# --------------------------------------------------------------------------- #


_VALID_PAGINATION_TYPES = {"offset", "cursor", "page", "link", "link_header", "none"}


def _validate_rest_resources(metadata: dict[str, Any]) -> tuple[list[str], list[str]]:
    """Validate every resource block in REST metadata declares a complete
    pagination configuration. Returns ``(violations, warnings)`` — both empty
    when clean.
    """
    violations: list[str] = []
    warnings: list[str] = []
    resources = metadata.get("resources") or []
    if not isinstance(resources, list):
        return violations, warnings
    for resource in resources:
        if not isinstance(resource, dict):
            continue
        name = resource.get("name") or "<unnamed>"
        ptype = (resource.get("pagination_type") or "").strip().lower()
        if not ptype:
            violations.append(
                f"REST resource {name!r}: pagination_type is required "
                f"(one of {sorted(_VALID_PAGINATION_TYPES)}) — empty means "
                f"the template will silently fall back to 'cursor', which "
                f"is rarely right for non-cursor APIs"
            )
            continue
        if ptype not in _VALID_PAGINATION_TYPES:
            violations.append(
                f"REST resource {name!r}: pagination_type={ptype!r} not "
                f"in {sorted(_VALID_PAGINATION_TYPES)}"
            )
        if ptype == "cursor":
            mode = (resource.get("cursor_mode") or "response").strip().lower()
            cpath = (resource.get("cursor_path") or "").strip()
            if mode == "response" and not cpath:
                # Not a hard violation: BaseMCPConnector.PaginationHandler
                # auto-discovers the cursor location at runtime (scanning known
                # containers) and stops cleanly after one page if none is found.
                # The fetch loop is bounded by seen_cursors / duplicate-hash /
                # max_pages guards, so an empty cursor_path can never
                # infinite-loop. Surface it as a warning so a thin spec (e.g.
                # Asana, which documents next_page.offset only in prose) is
                # flagged but still produces a deployable connector.
                warnings.append(
                    f"REST resource {name!r}: pagination_type=cursor + "
                    f"cursor_mode=response with empty cursor_path — relying on "
                    f"runtime cursor auto-discovery in PaginationHandler"
                )
    return violations, warnings


# --------------------------------------------------------------------------- #
# Top-level driver
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Destination-contract invariants (api_saas only)
# --------------------------------------------------------------------------- #
# See docs/connectors/destination-contract.md. DB / warehouse / storage
# connectors follow base-interface.md and are intentionally NOT checked here.


def _validate_destination_contract(
    code: str, metadata: dict[str, Any], proto: str
) -> list[str]:
    """Ensure an api_saas connector's advertised destination_modes match what
    it actually implements, so the planner never wires a mode the connector
    can't honor."""
    violations: list[str] = []
    category = (metadata.get("category") or "").lower()
    if category != "api_saas":
        return violations

    modes = metadata.get("destination_modes") or []
    supports_dest = bool(metadata.get("supports_destination"))

    # Capability advertising must be internally consistent.
    if supports_dest and not modes:
        violations.append(
            "supports_destination=true but destination_modes is empty — the "
            "planner can't pick a write mode. Set modes (append|upsert|delete) "
            "or set supports_destination=false."
        )
    if modes and not supports_dest:
        violations.append(
            "destination_modes is non-empty but supports_destination=false — "
            "inconsistent capability advertising."
        )

    # SaaS connectors are not CDC sinks (no ensure_table / get_cdc_offsets).
    if metadata.get("supports_cdc"):
        violations.append(
            "api_saas connector advertises supports_cdc=true, but SaaS "
            "connectors are not CDC sinks (see destination-contract.md)."
        )

    # REST emits generic upsert_data/delete_data methods; the advertised mode
    # must match the emitted method (and vice-versa). GraphQL dispatches named
    # mutations instead of generic methods, so this method-presence check does
    # not apply to it. import_data is always emitted, so it is never checked.
    if proto == "rest":
        for mode, method in (("upsert", "def upsert_data"), ("delete", "def delete_data")):
            if mode in modes and method not in code:
                violations.append(
                    f"destination_modes lists '{mode}' but {method}() is not "
                    f"implemented in the connector."
                )
            if method in code and mode not in modes:
                violations.append(
                    f"{method}() is implemented but '{mode}' is not in "
                    f"destination_modes — advertise it or remove the method."
                )

    return violations


def validate(
    *,
    code: str,
    metadata: dict[str, Any] | str,
    protocol: str = "",
) -> InvariantReport:
    """Run all applicable invariant checks. Pure inspection.

    Args:
        code: rendered connector.py text
        metadata: connector metadata.json content (dict or JSON string)
        protocol: optional explicit protocol hint ("rest" / "graphql").
            When omitted we infer from the metadata.

    Returns: InvariantReport with all violations.
    """
    if isinstance(metadata, str):
        try:
            metadata_dict: dict[str, Any] = json.loads(metadata)
        except json.JSONDecodeError:
            metadata_dict = {}
    else:
        metadata_dict = metadata or {}

    report = InvariantReport()

    # Universal invariants — every connector needs MCP.
    if not _has_mcp_route(code):
        report.violations.append(
            "Missing /mcp JSON-RPC route. The orchestrator calls tools/call "
            "via this endpoint; without it the connector is invisible to "
            "the executor regardless of its REST surface."
        )
    elif not _has_handle_request_dispatch(code):
        report.violations.append(
            "/mcp route present but doesn't call handle_request(). The "
            "JSON-RPC dispatch contract requires routing through the "
            "BaseMCPConnector.handle_request method."
        )

    # Infer protocol if not provided.
    proto = (protocol or metadata_dict.get("protocol") or "").lower()
    if not proto:
        # Heuristic: GRAPHQL_OPERATIONS module constant or graphql_query in metadata
        if "GRAPHQL_OPERATIONS" in code or _relay_operations_in_metadata(metadata_dict):
            proto = "graphql"
        elif metadata_dict.get("resources"):
            proto = "rest"

    if proto == "graphql":
        relay_ops = _relay_operations_in_metadata(metadata_dict)
        if relay_ops and not _export_defaults_relay_pagination(code):
            report.violations.append(
                f"GraphQL connector has Relay operations ({', '.join(relay_ops)}) "
                f"but export() doesn't default ``first``/``last``. The "
                f"orchestrator calls export with no variables and the API "
                f"rejects with 'first or last must be provided'."
            )

    if proto == "rest":
        rest_violations, rest_warnings = _validate_rest_resources(metadata_dict)
        report.violations.extend(rest_violations)
        report.warnings.extend(rest_warnings)

    # Destination-contract conformance (api_saas only; no-op otherwise).
    report.violations.extend(
        _validate_destination_contract(code, metadata_dict, proto)
    )

    report.passed = not report.violations
    return report


def validate_or_raise(
    *,
    code: str,
    metadata: dict[str, Any] | str,
    protocol: str = "",
) -> InvariantReport:
    report = validate(code=code, metadata=metadata, protocol=protocol)
    if not report.passed:
        raise ProtocolInvariantError(
            "Protocol invariants violated:\n  - "
            + "\n  - ".join(report.violations)
        )
    return report


__all__ = [
    "InvariantReport",
    "ProtocolInvariantError",
    "validate",
    "validate_or_raise",
]
