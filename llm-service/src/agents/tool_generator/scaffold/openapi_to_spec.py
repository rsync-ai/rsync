"""
OpenAPI/Swagger -> ConnectorSpec (deterministic, no LLM).

Reads an OpenAPI 3.x or Swagger 2.0 document and produces the ConnectorSpec
dict that ``generator.builder.ConnectorBuilder`` renders into a connector.

Every field is derived from the document by rule. The same input always
produces the same output; there is no model call, no network access and no
vendor knowledge base. Where the document is silent we fall back to the
ConnectorSpec field defaults rather than guessing, and record the gap in
``ConversionReport.notes`` so the operator can fill it in.

VERSION: 1.0.0
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Core operations every category requires (ConnectorSpec.validate_operations).
_CORE_OPERATIONS = ("test_connection", "validate_config", "discover_schema", "get_capabilities")

# The read operation each category requires on top of the core four.
_CATEGORY_READ_OPERATION = {
    "api_saas": "export",
    "relational_db": "export",
    "document_db": "export",
    "cloud_storage": "read",
}

# Query-parameter names that signal a pagination style, most specific first.
# Matched case-insensitively against the collection's declared query params.
_PAGINATION_SIGNALS: Tuple[Tuple[str, str], ...] = (
    ("cursor", "cursor"),
    ("after", "cursor"),
    ("starting_after", "cursor"),
    ("page_token", "cursor"),
    ("next_token", "cursor"),
    ("offset", "offset"),
    ("skip", "offset"),
    ("start", "offset"),
    ("page", "page"),
    ("page_number", "page"),
)

_LIMIT_PARAM_CANDIDATES = ("limit", "per_page", "page_size", "pagesize", "count", "max_results", "top")

# Query params that filter by a modification timestamp, enabling incremental sync.
_INCREMENTAL_PARAM_CANDIDATES = (
    "updated_since", "updated_after", "modified_since", "since", "start_time",
    "updated_at_min", "created_since", "if_modified_since",
)

# Response envelope keys that commonly hold the record array.
_DATA_KEY_CANDIDATES = ("data", "results", "items", "records", "values", "list", "entries", "content")

_IDENTIFIER_RE = re.compile(r"[^a-z0-9_]+")
_PATH_PARAM_RE = re.compile(r"\{([^}/]+)\}")


class OpenAPIConversionError(ValueError):
    """The document is not usable as a connector source."""


@dataclass
class ConversionReport:
    """What the conversion derived, and what it could not."""

    spec: Dict[str, Any]
    resource_count: int = 0
    skipped_paths: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    def add_note(self, note: str) -> None:
        if note not in self.notes:
            self.notes.append(note)


# ---------------------------------------------------------------------------
# Document shape
# ---------------------------------------------------------------------------

def _detect_version(doc: Dict[str, Any]) -> str:
    """Return 'openapi3' or 'swagger2'; raise if the document is neither."""
    if not isinstance(doc, dict):
        raise OpenAPIConversionError("Document root must be a JSON/YAML object")
    if str(doc.get("openapi", "")).startswith("3"):
        return "openapi3"
    if str(doc.get("swagger", "")).startswith("2"):
        return "swagger2"
    raise OpenAPIConversionError(
        "Document declares neither 'openapi: 3.x' nor 'swagger: 2.0' at the root; "
        "it is not an OpenAPI or Swagger specification"
    )


def slugify(value: str) -> str:
    """Lowercase identifier safe for a connector name or resource key."""
    s = _IDENTIFIER_RE.sub("_", (value or "").strip().lower())
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _parse_base_url(doc: Dict[str, Any], version: str) -> Optional[str]:
    """First declared server URL. Server variables are substituted with defaults."""
    if version == "openapi3":
        servers = doc.get("servers") or []
        if not servers or not isinstance(servers[0], dict):
            return None
        url = (servers[0].get("url") or "").strip()
        # {variable} placeholders resolve from the server's own default values.
        for var_name, var_def in (servers[0].get("variables") or {}).items():
            if isinstance(var_def, dict) and var_def.get("default") is not None:
                url = url.replace("{%s}" % var_name, str(var_def["default"]))
        return url.rstrip("/") or None

    host = (doc.get("host") or "").strip()
    if not host:
        return None
    schemes = doc.get("schemes") or ["https"]
    scheme = "https" if "https" in schemes else schemes[0]
    base_path = (doc.get("basePath") or "").rstrip("/")
    return f"{scheme}://{host}{base_path}"


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def _security_schemes(doc: Dict[str, Any], version: str) -> Dict[str, Dict[str, Any]]:
    if version == "openapi3":
        return (doc.get("components") or {}).get("securitySchemes") or {}
    return doc.get("securityDefinitions") or {}


def _scheme_to_auth(scheme: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map one OpenAPI security scheme to ConnectorSpec AuthConfig fields."""
    if not isinstance(scheme, dict):
        return None
    stype = str(scheme.get("type", "")).lower()

    if stype == "apikey":
        location = str(scheme.get("in", "header")).lower()
        param = scheme.get("name") or "Authorization"
        if location == "query":
            # api_key_query carries the credential as a URL parameter.
            return {"type": "api_key_query", "query_param": param, "header_prefix": ""}
        if location == "cookie":
            return None  # Cookie auth has no ConnectorSpec representation.
        # header_prefix must be explicit: AuthConfig defaults it to "Bearer",
        # which is wrong for a raw API key in an X-Api-Key style header.
        return {"type": "api_key", "header_name": param, "header_prefix": ""}

    if stype == "http":
        sub = str(scheme.get("scheme", "")).lower()
        if sub == "bearer":
            return {"type": "bearer", "header_name": "Authorization", "header_prefix": "Bearer"}
        if sub == "basic":
            return _basic_auth()
        return None

    if stype in ("oauth2", "openidconnect"):
        # ConnectorSpec's oauth2 auth requires an `oauth_provider` that resolves
        # against a curated provider registry, which a document cannot supply.
        # Emit bearer instead: the connector accepts an access token the
        # operator obtains through the API's own OAuth flow.
        return {"type": "bearer", "header_name": "Authorization", "header_prefix": "Bearer", "_was_oauth2": True}

    # Swagger 2.0 spells basic auth as type: basic
    if stype == "basic":
        return _basic_auth()

    return None


def _basic_auth() -> Dict[str, Any]:
    """AuthConfig rejects basic auth without both credential env vars."""
    return {
        "type": "basic",
        "header_name": "Authorization",
        "header_prefix": "Basic",
        "username_env": "MCP_USERNAME",
        "password_env": "MCP_PASSWORD",
    }


def _build_auth(doc: Dict[str, Any], version: str, report: ConversionReport) -> Dict[str, Any]:
    """Pick the connector's default auth method from the declared schemes.

    Preference order favours what a data connector can actually use unattended:
    a static credential beats an interactive OAuth flow.
    """
    schemes = _security_schemes(doc, version)
    if not schemes:
        report.add_note(
            "Document declares no securitySchemes. The connector authenticates only "
            "if you configure a key, which it then sends as X-API-Key."
        )
        return {"type": "none", "_undetermined": True}

    candidates: List[Dict[str, Any]] = []
    for scheme_name, scheme in schemes.items():
        mapped = _scheme_to_auth(scheme)
        if mapped:
            candidates.append(mapped)
        else:
            report.add_note(f"Security scheme '{scheme_name}' has no connector equivalent and was skipped")

    if not candidates:
        report.add_note("No usable security scheme found; the connector sends a configured key as X-API-Key")
        return {"type": "none", "_undetermined": True}

    # A static credential beats an interactive flow for an unattended connector,
    # so a downgraded oauth2 (marked below) sorts last among the bearers.
    priority = {"api_key": 0, "api_key_query": 1, "bearer": 2, "basic": 3}
    candidates.sort(key=lambda c: (priority.get(c["type"], 99), bool(c.get("_was_oauth2"))))
    chosen = dict(candidates[0])
    if len(candidates) > 1:
        # Name the schemes as the document declares them, not as they were
        # mapped: "oauth2" is what the reader will look for in their own spec.
        others = ", ".join(sorted(
            "oauth2" if c.get("_was_oauth2") else c["type"] for c in candidates[1:]
        ))
        report.add_note(f"API declares multiple auth methods; defaulted to '{chosen['type']}' (also available: {others})")
    if chosen.pop("_was_oauth2", False):
        report.add_note(
            "API uses OAuth2; the connector takes a pre-issued access token in its "
            "access_token field. Obtain one through the API's own OAuth flow."
        )
    for other in candidates[1:]:
        other.pop("_was_oauth2", None)
    return {k: v for k, v in chosen.items() if v is not None}


# ---------------------------------------------------------------------------
# Resources
# ---------------------------------------------------------------------------

def _collect_parameters(path_item: Dict[str, Any], operation: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Path-level and operation-level parameters, operation last so it wins."""
    params: List[Dict[str, Any]] = []
    for source in (path_item.get("parameters") or [], operation.get("parameters") or []):
        for p in source:
            if isinstance(p, dict):
                params.append(p)
    return params


def _query_param_names(params: List[Dict[str, Any]]) -> List[str]:
    return [str(p.get("name", "")) for p in params if str(p.get("in", "")).lower() == "query" and p.get("name")]


def _detect_pagination(query_params: List[str]) -> Dict[str, Any]:
    """Infer the pagination contract from declared query parameter names."""
    lowered = {q.lower(): q for q in query_params}

    pagination_type: Optional[str] = None
    pagination_param = "offset"
    cursor_param: Optional[str] = None
    for signal, style in _PAGINATION_SIGNALS:
        if signal in lowered:
            pagination_type = style
            pagination_param = lowered[signal]
            if style == "cursor":
                cursor_param = lowered[signal]
            break

    limit_param = "limit"
    for candidate in _LIMIT_PARAM_CANDIDATES:
        if candidate in lowered:
            limit_param = lowered[candidate]
            break

    result: Dict[str, Any] = {"limit_param": limit_param}
    if pagination_type:
        result["pagination_type"] = pagination_type
        result["pagination_param"] = pagination_param
        if cursor_param:
            result["cursor_param"] = cursor_param
    else:
        # Nothing declared: leave the field defaults rather than inventing a scheme.
        result["pagination_type"] = None
    return result


def _detect_incremental(query_params: List[str]) -> Dict[str, Any]:
    lowered = {q.lower(): q for q in query_params}
    for candidate in _INCREMENTAL_PARAM_CANDIDATES:
        if candidate in lowered:
            return {"supports_incremental": True, "incremental_param": lowered[candidate]}
    return {}


def _resolve_ref(doc: Dict[str, Any], ref: str, _depth: int = 0) -> Optional[Dict[str, Any]]:
    """Resolve a local $ref. Remote refs are not followed (offline by design)."""
    if _depth > 10 or not ref.startswith("#/"):
        return None
    node: Any = doc
    for part in ref[2:].split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    if isinstance(node, dict) and "$ref" in node:
        return _resolve_ref(doc, node["$ref"], _depth + 1)
    return node if isinstance(node, dict) else None


def _success_schema(doc: Dict[str, Any], operation: Dict[str, Any], version: str) -> Optional[Dict[str, Any]]:
    """Schema of the first 2xx response body, refs resolved one level."""
    responses = operation.get("responses") or {}
    for code in ("200", "201", 200, 201, "default"):
        resp = responses.get(code)
        if not isinstance(resp, dict):
            continue
        if "$ref" in resp:
            resp = _resolve_ref(doc, resp["$ref"]) or {}
        if version == "openapi3":
            content = resp.get("content") or {}
            for media_type, media in content.items():
                if "json" in str(media_type).lower() and isinstance(media, dict):
                    schema = media.get("schema")
                    break
            else:
                continue
        else:
            schema = resp.get("schema")
        if isinstance(schema, dict):
            if "$ref" in schema:
                schema = _resolve_ref(doc, schema["$ref"])
            return schema if isinstance(schema, dict) else None
    return None


def _detect_response_shape(doc: Dict[str, Any], schema: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Find where the records live in the success response body."""
    if not isinstance(schema, dict):
        return {}
    if schema.get("type") == "array":
        return {}  # Bare array; the runtime auto-detects this.

    props = schema.get("properties")
    if not isinstance(props, dict):
        return {}

    def _is_array(prop: Any) -> bool:
        if not isinstance(prop, dict):
            return False
        if "$ref" in prop:
            prop = _resolve_ref(doc, prop["$ref"]) or {}
        return isinstance(prop, dict) and prop.get("type") == "array"

    for candidate in _DATA_KEY_CANDIDATES:
        if candidate in props and _is_array(props[candidate]):
            return {"response_data_key": candidate}
    for key, prop in props.items():
        if _is_array(prop):
            return {"response_data_key": key}
    return {}


def _item_path_item(paths: Dict[str, Any], collection_path: str) -> Optional[Dict[str, Any]]:
    """The single-record route for a collection, e.g. /widgets -> /widgets/{widgetId}."""
    prefix = collection_path.rstrip("/") + "/{"
    for candidate, item in paths.items():
        if not isinstance(item, dict) or not candidate.startswith(prefix):
            continue
        # Exactly one extra segment, and that segment is the whole placeholder.
        tail = candidate[len(collection_path.rstrip("/")) + 1:]
        if tail.startswith("{") and tail.endswith("}") and "/" not in tail:
            return item
    return None


def _has_item_route(paths: Dict[str, Any], collection_path: str) -> bool:
    item = _item_path_item(paths, collection_path)
    return isinstance(item, dict) and isinstance(item.get("get"), dict)


def _resource_name_from_path(path: str) -> Optional[str]:
    """Last literal segment of the path, which names the collection."""
    segments = [s for s in path.split("/") if s and not (s.startswith("{") and s.endswith("}"))]
    if not segments:
        return None
    # Drop a trailing file extension (Twilio-style '/Accounts.json').
    last = segments[-1].split(".")[0]
    name = slugify(last)
    # A purely numeric or version-looking tail is not a resource name.
    if not name or name.isdigit() or re.fullmatch(r"v\d+", name):
        return None
    return name


def _build_resources(
    doc: Dict[str, Any], version: str, report: ConversionReport
) -> List[Dict[str, Any]]:
    """One resource per GET-able collection path."""
    paths = doc.get("paths") or {}
    if not isinstance(paths, dict):
        raise OpenAPIConversionError("Document has no usable 'paths' object")

    by_name: Dict[str, Dict[str, Any]] = {}

    for path, path_item in sorted(paths.items()):
        if not isinstance(path_item, dict):
            continue
        get_op = path_item.get("get")
        if not isinstance(get_op, dict):
            report.skipped_paths.append(f"{path} (no GET operation)")
            continue
        if get_op.get("deprecated") is True:
            report.skipped_paths.append(f"{path} (deprecated)")
            continue

        name = _resource_name_from_path(path)
        if not name:
            report.skipped_paths.append(f"{path} (no resource name in path)")
            continue

        path_params = _PATH_PARAM_RE.findall(path)
        params = _collect_parameters(path_item, get_op)
        query_params = _query_param_names(params)

        resource: Dict[str, Any] = {
            "name": name,
            "endpoint": path,
            "path_params": path_params,
            "supports_list": True,
            "supports_get": _has_item_route(paths, path),
            "supports_create": isinstance(path_item.get("post"), dict),
        }

        # Update and delete address a single record, so they live on the item
        # route (/widgets/{id}), not the collection. Fall back to the collection
        # for APIs that accept them there.
        item_item = _item_path_item(paths, path) or {}
        has_put = isinstance(item_item.get("put"), dict) or isinstance(path_item.get("put"), dict)
        has_patch = isinstance(item_item.get("patch"), dict) or isinstance(path_item.get("patch"), dict)
        resource["supports_update"] = has_put or has_patch
        resource["supports_delete"] = (
            isinstance(item_item.get("delete"), dict) or isinstance(path_item.get("delete"), dict)
        )
        if has_patch and not has_put:
            resource["update_method"] = "PATCH"

        resource.update(_detect_pagination(query_params))
        resource.update(_detect_incremental(query_params))
        resource.update(_detect_response_shape(doc, _success_schema(doc, get_op, version)))

        # Prefer the least-parameterised path for a given resource name: a plain
        # /widgets collection beats /orgs/{org}/widgets, which needs config to call.
        existing = by_name.get(name)
        if existing is None or len(path_params) < len(existing.get("path_params") or []):
            if existing is not None:
                report.skipped_paths.append(f"{existing['endpoint']} (superseded by {path} for resource '{name}')")
            by_name[name] = resource
        else:
            report.skipped_paths.append(f"{path} (duplicate resource '{name}')")

    resources = [by_name[k] for k in sorted(by_name)]
    # pagination_type=None means "not declared"; drop it so the field default applies.
    for r in resources:
        if r.get("pagination_type") is None:
            r.pop("pagination_type", None)
    return resources


# ---------------------------------------------------------------------------
# Config fields
# ---------------------------------------------------------------------------

# Config keys the rendered connector actually reads for each auth type. These
# mirror the auth dispatcher in ``templates/connector.py.j2``; a field name that
# does not appear there would render a form the runtime ignores.
_AUTH_CONFIG_FIELDS: Dict[str, Tuple[Tuple[str, str], ...]] = {
    "api_key": (("api_key", "API key sent in the {header} header."),),
    "api_key_query": (("api_key", "API key sent as the ?{query} URL parameter."),),
    "bearer": (("access_token", "Bearer token sent in the Authorization header."),),
    "basic": (
        ("username", "Username for HTTP Basic authentication."),
        ("password", "Password for HTTP Basic authentication."),
    ),
    "none": (("api_key", "Optional API key. Sent as X-API-Key only if you set one."),),
}


def _build_config_fields(auth: Dict[str, Any], base_url: str) -> List[Dict[str, Any]]:
    """The connection form the operator fills in.

    Without these the connector renders but has no configurable credential,
    which PreGenerationValidator flags and the product UI shows as an empty form.
    """
    fields: List[Dict[str, Any]] = [
        {
            "name": "base_url",
            "type": "string",
            "required": False,
            "secret": False,
            "default": base_url or None,
            "description": (
                "Base URL of your API instance. Defaults to the server the spec declares; "
                "override it to point at a self-hosted deployment."
            ),
            "env_var": "MCP_BASE_URL",
        }
    ]

    auth_type = auth.get("type", "none")
    for index, (field_name, description) in enumerate(_AUTH_CONFIG_FIELDS.get(auth_type, ())):
        fields.append({
            "name": field_name,
            "type": "string",
            "required": auth_type != "none",
            "secret": field_name != "username",
            "description": description.format(
                header=auth.get("header_name") or "Authorization",
                query=auth.get("query_param") or "api_key",
            ),
            "env_var": None,
            "ui_order": index + 1,
        })
    return fields


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def openapi_to_connector_spec(
    doc: Dict[str, Any],
    *,
    name: Optional[str] = None,
    display_name: Optional[str] = None,
    category: str = "api_saas",
    base_url: Optional[str] = None,
    supports_destination: bool = False,
    max_resources: Optional[int] = None,
) -> ConversionReport:
    """Convert a parsed OpenAPI/Swagger document into a ConnectorSpec dict.

    Args:
        doc: The parsed document (already JSON/YAML-decoded).
        name: Connector identifier. Defaults to a slug of ``info.title``.
        display_name: Human-readable name. Defaults to ``info.title``.
        category: ConnectorCategory value; ``api_saas`` for REST APIs.
        base_url: Overrides the document's declared server URL.
        supports_destination: Advertise write support on the connector.
        max_resources: Keep at most this many resources (alphabetical).

    Returns:
        A ConversionReport whose ``.spec`` is ready for ``ConnectorBuilder``.

    Raises:
        OpenAPIConversionError: the document is not OpenAPI/Swagger, or
            declares no GET-able collection to read from.
    """
    version = _detect_version(doc)
    info = doc.get("info") or {}

    title = str(info.get("title") or "").strip()
    resolved_display = (display_name or title or name or "").strip()
    resolved_name = slugify(name or title or "")
    if not resolved_name:
        raise OpenAPIConversionError(
            "Cannot derive a connector name: document has no 'info.title' and no --name was given"
        )
    if not resolved_display:
        resolved_display = resolved_name

    report = ConversionReport(spec={})

    resolved_base = (base_url or _parse_base_url(doc, version) or "").rstrip("/")
    if not resolved_base:
        report.add_note(
            "Document declares no server URL; set base_url by hand or pass --base-url, "
            "otherwise the connector has no host to call"
        )

    resources = _build_resources(doc, version, report)
    if not resources:
        raise OpenAPIConversionError(
            "No GET-able collection found in 'paths'; there is nothing for a source connector to read"
        )
    if max_resources is not None and len(resources) > max_resources:
        report.add_note(f"Kept {max_resources} of {len(resources)} resources (--max-resources)")
        resources = resources[:max_resources]

    read_op = _CATEGORY_READ_OPERATION.get(category, "export")
    operations = [{"name": op, "type": "core"} for op in _CORE_OPERATIONS]
    operations.append({"name": read_op, "type": "source"})
    if supports_destination:
        operations.append({"name": "import_data", "type": "destination"})

    auth = _build_auth(doc, version, report)
    # Spec-silence about auth is not the same as a confirmed-public API: the
    # connector must still be able to send a credential if the operator has one.
    undetermined = bool(auth.pop("_undetermined", False))

    spec: Dict[str, Any] = {
        "name": resolved_name,
        "display_name": resolved_display,
        "description": str(info.get("description") or "").strip()[:500],
        "version": "1.0.0",
        "category": category,
        "protocol": "openapi",
        "base_url": resolved_base,
        "auth": auth,
        "auth_undetermined": undetermined,
        "config_fields": _build_config_fields(auth, resolved_base),
        "resources": resources,
        "operations": operations,
        "supports_source": True,
        "supports_destination": supports_destination,
    }

    report.spec = spec
    report.resource_count = len(resources)
    return report
