"""
Context7 Capability Validation (Fail-Closed on Contradictions)

This module validates a generated ConnectorSpec against Context7-derived structured
documentation outputs (DocResearcherAgent -> DocumentationResult).

Coverage rule:
- Fail ONLY on contradictions for resources/tools we generate.
- Do NOT require covering every documented endpoint (docs can be incomplete).

Uses category-specific thresholds from the registry for strictness tuning.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple
import re

# Import registry for threshold access
try:
    from ..contracts.registry import get_context7_thresholds, Context7Thresholds
except ImportError:
    # Fallback if registry not available
    get_context7_thresholds = None
    Context7Thresholds = None


def _norm_path(path: str) -> str:
    if not path:
        return ""
    p = path.strip()
    # strip base url if accidentally included
    if "://" in p:
        # naive split: keep path portion
        try:
            p = p.split("://", 1)[1]
            p = p.split("/", 1)[1] if "/" in p else ""
            p = "/" + p
        except Exception:
            pass
    if not p.startswith("/"):
        p = "/" + p
    if len(p) > 1 and p.endswith("/"):
        p = p[:-1]

    # Treat common "action suffix" endpoints as documenting the collection path.
    # Example: "/persons/getAll" and "/persons/search" both imply the collection "/persons".
    # This keeps validation consistent with ResourceSynthesizer + Architect normalization and
    # prevents false Drafts due to doc/SDK naming quirks.
    action_suffixes = {
        "getall",
        "list",
        "search",
        "query",
        "getsummary",
        "summary",
        "add",
        "create",
        "update",
        "delete",
        "remove",
        "upsert",
    }
    action_prefix_re = re.compile(r"^(get|list|search|query|create|add|update|delete|remove|upsert)[a-z0-9_]*$")

    def is_action_segment(seg: str) -> bool:
        s = (seg or "").strip().lower()
        if not s:
            return False
        if s in action_suffixes:
            return True
        return bool(action_prefix_re.match(s))

    # Only strip when we have a real entity segment before the action.
    # Avoid collapsing global endpoints like "/v1/search" -> "/v1".
    segs = [s for s in p.split("/") if s]
    if len(segs) >= 2:
        last = (segs[-1] or "").strip().lower()
        prev = (segs[-2] or "").strip().lower()
        if is_action_segment(last):
            # Don't strip if the "entity" is just a version segment
            if not re.match(r"^v\\d+$", prev) and prev not in {"api", "rest"}:
                p = "/" + "/".join(segs[:-1])

    return p


def _method_set(method: Optional[str]) -> Set[str]:
    if not method:
        return set()
    return {method.strip().upper()}


def _expected_methods_from_resource(resource: Any) -> Set[str]:
    expected: Set[str] = set()
    # spec.ResourceConfig fields (pydantic) are not guaranteed to exist across versions, so use getattr defensively.
    if getattr(resource, "supports_list", False) or getattr(resource, "supports_get", False):
        expected.add("GET")
    if getattr(resource, "supports_create", False):
        expected.add("POST")
    if getattr(resource, "supports_update", False):
        expected.update({"PUT", "PATCH"})
    if getattr(resource, "supports_delete", False):
        expected.add("DELETE")
    return expected


def _flatten_doc_endpoints(doc_api_endpoints: List[Dict[str, Any]]) -> Dict[str, Set[str]]:
    """
    Return mapping: normalized_path -> set(methods)
    """
    out: Dict[str, Set[str]] = {}
    for ep in doc_api_endpoints or []:
        path = _norm_path(str(ep.get("path") or ""))
        method = str(ep.get("method") or "").upper()
        if not path or not method:
            continue
        out.setdefault(path, set()).add(method)
    return out


def _flatten_doc_params(doc_api_endpoints: List[Dict[str, Any]]) -> Dict[Tuple[str, str], Set[str]]:
    """
    Return mapping: (normalized_path, METHOD) -> set(query_param_names)
    Only includes params when DocResearcher provided them.
    """
    out: Dict[Tuple[str, str], Set[str]] = {}
    for ep in doc_api_endpoints or []:
        path = _norm_path(str(ep.get("path") or ""))
        method = str(ep.get("method") or "").upper()
        if not path or not method:
            continue
        params = ep.get("parameters") or []
        names: Set[str] = set()
        if isinstance(params, list):
            for p in params:
                if isinstance(p, dict):
                    n = p.get("name")
                    if n:
                        names.add(str(n))
        if names:
            out[(path, method)] = names
    return out


@dataclass
class CapabilityValidationResult:
    passed: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    error_details: Dict[str, Any] = field(default_factory=dict)


def has_sufficient_ground_truth(
    doc_api_endpoints: List[Dict[str, Any]],
    doc_auth_info: Optional[Dict[str, Any]],
    doc_length: int = 0
) -> bool:
    """
    Determine if Context7 documentation is sufficiently complete to use as ground truth.
    
    Args:
        doc_api_endpoints: Structured endpoints from Context7
        doc_auth_info: Auth information from Context7
        doc_length: Length of documentation text
    
    Returns:
        True if docs are authoritative enough for validation
    """
    has_endpoints = bool(doc_api_endpoints)
    auth_type = (doc_auth_info or {}).get("type")
    # Treat "none" as non-authoritative unless we have explicit evidence; many docs parsers
    # will default to "none" when they fail to extract auth.
    has_auth = bool(auth_type) and auth_type not in {"unknown", "none"}
    
    # Consider docs too small/cached to be authoritative
    if doc_length > 0 and doc_length < 500:
        return False
    
    # Need at least endpoints OR auth
    return has_endpoints or has_auth


def validate_connector_spec_against_context7(
    *,
    connector_spec: Any,
    doc_api_endpoints: List[Dict[str, Any]],
    doc_auth_info: Optional[Dict[str, Any]],
    doc_rate_limits: Optional[Dict[str, Any]] = None,
    doc_length: int = 0,
    category: Optional[str] = None,
    authoritativeness_score: float = 0.5,  # NEW: 0-1 score from AuthoritativeScorer
    authoritativeness_tier: str = "partial",  # NEW: "authoritative", "partial", "weak"
) -> CapabilityValidationResult:
    """
    Fail-closed on contradictions for generated resources.
    
    Validation strictness controlled by authoritativeness:
    - authoritative (>=0.8): Strict subset validation (fail on contradictions)
    - partial (0.5-0.8): Hybrid (warnings + conservative filtering)
    - weak (<0.5): Fallback-safe (skip validation)
    
    Args:
        connector_spec: Generated connector spec
        doc_api_endpoints: Structured endpoints from Context7
        doc_auth_info: Auth info from Context7
        doc_rate_limits: Rate limit info from Context7
        doc_length: Documentation length
        category: Connector category
        authoritativeness_score: Score from AuthoritativeScorer (0-1)
        authoritativeness_tier: Tier from AuthoritativeScorer
    """

    # Context7 MCP is used to fetch library/API docs. For non-HTTP connector categories
    # (databases, cloud storage, etc.), Context7 auth/endpoints are not reliable "ground truth"
    # for our connector spec. Skip fail-closed contradictions for those categories to avoid
    # false Drafts like: "oracle DB" being validated against an unrelated "oracle" library.
    if category:
        cat = str(category).strip().lower()
        if cat and cat != "api_saas":
            return CapabilityValidationResult(
                passed=True,
                warnings=[f"Skipped capability validation for non-api_saas category '{cat}'"],
                error_details={"skipped": True, "reason": "non_api_category"},
            )
    
    # NEW: Skip validation if authoritativeness is weak (<0.5)
    if authoritativeness_tier == "weak":
        return CapabilityValidationResult(
            passed=True,
            warnings=[f"Skipped capability validation due to low authoritativeness (score={authoritativeness_score:.2f})"],
            error_details={"skipped": True, "reason": "low_authoritativeness"},
        )

    if not has_sufficient_ground_truth(doc_api_endpoints, doc_auth_info, doc_length):
        return CapabilityValidationResult(
            passed=True,
            warnings=["Skipped capability validation (no structured Context7 ground truth)"],
            error_details={"skipped": True},
        )

    errors: List[str] = []
    warnings: List[str] = []

    # Get category-specific thresholds from registry
    thresholds = None
    if get_context7_thresholds and category:
        try:
            thresholds = get_context7_thresholds(category)
        except Exception:
            pass
    
    # Use defaults if registry not available or category not found
    if thresholds is None:
        # Fallback to hardcoded defaults
        from dataclasses import dataclass as _dc, field as _field
        @_dc
        class _Thresholds:
            min_endpoints_for_endpoint_contradiction: int = 10
            min_endpoints_for_method_contradiction: int = 5
            min_endpoints_for_param_contradiction: int = 5
            auth_none_requires_explicit_keywords: List[str] = _field(default_factory=lambda: [
                "no authentication", "no auth", "without authentication", "public endpoint"
            ])
            oauth_bearer_compatible: bool = True
        thresholds = _Thresholds()

    # Context7 structuring may return only a small sample of endpoints for some libraries/APIs.
    # In that case, treat endpoint/param coverage as *non-authoritative* to avoid false negatives.
    # We still validate auth when present.
    #
    # NEW: Authoritativeness-aware validation:
    # - authoritative: strict (fail on contradictions)
    # - partial: hybrid (warnings instead of errors)
    strict_mode = authoritativeness_tier == "authoritative"
    endpoint_count = len(doc_api_endpoints or [])
    strict_endpoint_validation = endpoint_count >= thresholds.min_endpoints_for_endpoint_contradiction
    strict_method_validation = endpoint_count >= thresholds.min_endpoints_for_method_contradiction
    strict_param_validation = endpoint_count >= thresholds.min_endpoints_for_param_contradiction

    doc_methods_by_path = _flatten_doc_endpoints(doc_api_endpoints)
    doc_params_by_path_method = _flatten_doc_params(doc_api_endpoints)

    # -----------------------
    # Auth contradiction
    # -----------------------
    expected_auth = (doc_auth_info or {}).get("type")
    actual_auth = None
    try:
        # spec.auth.type is often an Enum; use .value when present
        spec_auth = getattr(connector_spec, "auth", None)
        spec_auth_type = getattr(spec_auth, "type", None)
        actual_auth = getattr(spec_auth_type, "value", spec_auth_type)
    except Exception:
        actual_auth = None

    if expected_auth and expected_auth != "unknown":
        # Normalize oauth naming
        expected_norm = "oauth2" if str(expected_auth).lower() in {"oauth", "oauth2"} else str(expected_auth).lower()
        actual_norm = str(actual_auth).lower() if actual_auth else ""
        if expected_norm == "none":
            # "none" is frequently produced by incomplete structuring; only treat as ground truth when
            # docs explicitly state no auth is required. Otherwise, skip fail-closed auth contradiction.
            details = (doc_auth_info or {}).get("details") or {}
            evidence_text = ""
            if isinstance(details, dict):
                evidence_text = str(details.get("description") or details.get("notes") or "")
            else:
                evidence_text = str(details)
            ev = evidence_text.lower()
            if any(k in ev for k in thresholds.auth_none_requires_explicit_keywords):
                if actual_norm and actual_norm != "none":
                    msg = f"AUTH CONTRADICTION: docs require 'none' but spec uses '{actual_norm}'"
                    if strict_mode:
                        errors.append(msg)
                    else:
                        warnings.append(f"[PARTIAL] {msg}")
            else:
                warnings.append("AUTH NOTE: Context7 structured auth type='none' treated as non-authoritative (insufficient evidence)")
            expected_norm = ""

        if actual_norm and expected_norm and actual_norm != expected_norm:
            # Optional OAuth support:
            # If docs indicate OAuth2 but the spec chooses api_key/bearer AND also exposes oauth_provider,
            # treat this as "OAuth supported but not primary" (UI can still show OAuth connect).
            oauth_provider = None
            try:
                spec_auth = getattr(connector_spec, "auth", None)
                oauth_provider = getattr(spec_auth, "oauth_provider", None)
            except Exception:
                oauth_provider = None

            if expected_norm == "oauth2" and actual_norm in {"api_key", "bearer"} and oauth_provider:
                warnings.append(
                    f"AUTH NOTE: docs indicate oauth2; spec uses '{actual_norm}' with oauth_provider='{oauth_provider}' (treated as OAuth optional)"
                )
            # OAuth2-protected APIs often accept bearer tokens (PATs) in the same Authorization header.
            # Treat oauth2 (docs) vs bearer (spec) as compatible when the spec uses standard Bearer auth.
            elif thresholds.oauth_bearer_compatible and expected_norm == "oauth2" and actual_norm == "bearer":
                try:
                    spec_auth = getattr(connector_spec, "auth", None)
                    header_name = str(getattr(spec_auth, "header_name", "") or "").lower()
                    header_prefix = str(getattr(spec_auth, "header_prefix", "") or "").lower()
                    if header_name == "authorization" and header_prefix.startswith("bearer"):
                        warnings.append("AUTH NOTE: docs indicate oauth2; spec uses bearer token in Authorization header (treated as compatible)")
                    else:
                        msg = f"AUTH CONTRADICTION: docs require '{expected_norm}' but spec uses '{actual_norm}'"
                        if strict_mode:
                            errors.append(msg)
                        else:
                            warnings.append(f"[PARTIAL] {msg}")
                except Exception:
                    msg = f"AUTH CONTRADICTION: docs require '{expected_norm}' but spec uses '{actual_norm}'"
                    if strict_mode:
                        errors.append(msg)
                    else:
                        warnings.append(f"[PARTIAL] {msg}")
            else:
                msg = f"AUTH CONTRADICTION: docs require '{expected_norm}' but spec uses '{actual_norm}'"
                if strict_mode:
                    errors.append(msg)
                else:
                    warnings.append(f"[PARTIAL] {msg}")

    # -----------------------
    # Resource endpoint + method contradictions
    # -----------------------
    endpoint_mismatches: List[Dict[str, Any]] = []
    method_mismatches: List[Dict[str, Any]] = []
    param_mismatches: List[Dict[str, Any]] = []

    resources = getattr(connector_spec, "resources", None) or []
    for r in resources:
        spec_path = _norm_path(str(getattr(r, "endpoint", "") or ""))
        if not spec_path:
            continue

        doc_methods = doc_methods_by_path.get(spec_path)
        if not doc_methods:
            endpoint_mismatches.append({"resource": getattr(r, "name", None), "endpoint": spec_path})
            msg = f"ENDPOINT CONTRADICTION: resource '{getattr(r,'name',None)}' endpoint '{spec_path}' not found in Context7 endpoints"
            if strict_endpoint_validation:
                errors.append(msg)
            else:
                warnings.append(msg + " (non-fatal: insufficient structured endpoint coverage from Context7)")
            continue

        expected_methods = _expected_methods_from_resource(r)
        if expected_methods:
            # If doc shows only one of PUT/PATCH, accept either for update.
            for m in expected_methods:
                if m in {"PUT", "PATCH"} and ("PUT" in doc_methods or "PATCH" in doc_methods):
                    continue
                if m not in doc_methods:
                    method_mismatches.append({"resource": getattr(r, "name", None), "endpoint": spec_path, "expected": sorted(list(expected_methods)), "doc_methods": sorted(list(doc_methods))})
                    msg = f"METHOD CONTRADICTION: resource '{getattr(r,'name',None)}' expects {sorted(list(expected_methods))} but docs show {sorted(list(doc_methods))} for {spec_path}"
                    if strict_method_validation and strict_mode:
                        errors.append(msg)
                    else:
                        warnings.append(msg + " (non-fatal: insufficient structured endpoint coverage from Context7)")
                    break

        # Pagination param contradiction (only if docs provide param names for that endpoint+method)
        pagination_param = getattr(r, "pagination_param", None)
        limit_param = getattr(r, "limit_param", None)
        pagination_type = getattr(r, "pagination_type", None)

        # pick a representative method for validation
        probe_method = "GET" if "GET" in doc_methods else next(iter(doc_methods))
        doc_params = doc_params_by_path_method.get((spec_path, probe_method), set())
        if doc_params:
            if pagination_param and str(pagination_param) not in doc_params and str(pagination_type or "").lower() in {"offset", "page", "cursor"}:
                param_mismatches.append({"resource": getattr(r, "name", None), "endpoint": spec_path, "param": str(pagination_param), "doc_params": sorted(list(doc_params))})
                msg = f"PAGINATION PARAM CONTRADICTION: resource '{getattr(r,'name',None)}' uses pagination_param='{pagination_param}' but docs params are {sorted(list(doc_params))} for {spec_path}"
                if strict_param_validation and strict_mode:
                    errors.append(msg)
                else:
                    warnings.append(msg + " (non-fatal: insufficient structured endpoint coverage from Context7)")
            if limit_param and str(limit_param) not in doc_params:
                # limit params vary across APIs; treat as error only when pagination is configured
                if str(pagination_type or "").lower() in {"offset", "page", "cursor", "link"}:
                    param_mismatches.append({"resource": getattr(r, "name", None), "endpoint": spec_path, "param": str(limit_param), "doc_params": sorted(list(doc_params))})
                    msg = f"LIMIT PARAM CONTRADICTION: resource '{getattr(r,'name',None)}' uses limit_param='{limit_param}' but docs params are {sorted(list(doc_params))} for {spec_path}"
                    if strict_param_validation and strict_mode:
                        errors.append(msg)
                    else:
                        warnings.append(msg + " (non-fatal: insufficient structured endpoint coverage from Context7)")

    # Warnings: docs may be incomplete; do not block for extra resources/tools not in docs.
    # (Currently we only validate contradictions; endpoint mismatches are considered contradictions by design.)

    details: Dict[str, Any] = {
        "auth_expected": expected_auth,
        "auth_actual": actual_auth,
        "endpoint_mismatches": endpoint_mismatches,
        "method_mismatches": method_mismatches,
        "param_mismatches": param_mismatches,
        "rate_limit_hints": doc_rate_limits or {},
    }

    return CapabilityValidationResult(
        passed=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        error_details=details,
    )


