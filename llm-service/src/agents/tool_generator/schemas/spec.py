"""
Connector Specification Schema

Defines the comprehensive configuration schema for MCP connectors.
This is the "Constitution" - strict contracts that eliminate syntax errors
and structural bugs by design.

Usage:
    spec = ConnectorSpec(
        name="stripe",
        display_name="Stripe",
        category=ConnectorCategory.API_SAAS,
        auth=AuthConfig(type=AuthType.BEARER, ...),
        operations=[...],
    )

VERSION: 1.0.0
"""

from enum import Enum
from typing import Dict, List, Any, Optional, Union
from pydantic import BaseModel, Field, field_validator, model_validator
import re


class ConnectorCategory(str, Enum):
    """Valid connector categories - aligned with base_connector.py CATEGORY_OPERATIONS"""
    RELATIONAL_DB = "relational_db"
    DOCUMENT_DB = "document_db"
    CLOUD_STORAGE = "cloud_storage"
    API_SAAS = "api_saas"
    STREAMING = "streaming"
    DATA_WAREHOUSE = "data_warehouse"
    WIDE_COLUMN_DB = "wide_column_db"


class AuthType(str, Enum):
    """Supported authentication types"""
    NONE = "none"
    API_KEY = "api_key"
    API_KEY_QUERY = "api_key_query"  # apiKey sent as a URL query param (?api_token=…), not a header
    BEARER = "bearer"
    BASIC = "basic"
    OAUTH2 = "oauth2"
    CUSTOM_HEADER = "custom_header"


class ParameterType(str, Enum):
    """Parameter data types"""
    STRING = "string"
    INTEGER = "integer"
    BOOLEAN = "boolean"
    FLOAT = "float"
    OBJECT = "object"
    ARRAY = "array"
    ANY = "any"


class OperationType(str, Enum):
    """Operation classification"""
    CORE = "core"           # Required: test_connection, validate_config, discover_schema
    SOURCE = "source"       # Data extraction: export, read, query
    DESTINATION = "destination"  # Data loading: import_data, write


class Protocol(str, Enum):
    """Wire protocol the connector speaks to the upstream API.

    Default is REST so existing connectors regenerate byte-identical when
    the protocol field is added to their spec (spec_version 1.0 → 1.1).
    """
    REST = "rest"
    GRAPHQL = "graphql"
    OPENAPI = "openapi"   # OpenAPI/Swagger-described REST (deterministic spec)
    AUTO = "auto"         # Detector decides; never persists to a generated spec


class ConnectorSource(str, Enum):
    """Who built the connector. Immutable signal of trust origin.

    Distinct from `lifecycle` (which is computed from runtime evidence).
    Mirrors the two-axis model used by Fivetran (Native/Lite/Partner/SDK) and
    Airbyte (Airbyte/Enterprise/Marketplace/Custom).
    """
    BUILT_IN = "built_in"      # Hand-authored in this repo; we maintain it.
    GENERATED = "generated"    # Tool-generator output from vendor_apis.yaml.
    PARTNER = "partner"        # Third-party contributed (reserved).
    CUSTOM = "custom"          # User-supplied via wizard (reserved).


class LifecycleStage(str, Enum):
    """Connector maturity. **Computed at read time** from real execution
    evidence (the `executions` table + the connector_validation_log written
    by the executor); not set statically in metadata.json so it can't rot.

    Promotion ladder mirrors Airbyte's Alpha→Beta→GA + Fivetran's
    Preview→Beta→GA. Promotion criteria are objective (see CAPABILITIES.md
    "Connector lifecycle" section).
    """
    DRAFT = "draft"        # Exists; never run against real vendor API.
    PREVIEW = "preview"    # test_connection passed once.
    BETA = "beta"          # test_connection + discover_schema + export all observed.
    GA = "ga"              # ≥1 pipeline execution success; no failures last 7d.


class GraphQLOperationType(str, Enum):
    """GraphQL operation kinds."""
    QUERY = "query"
    MUTATION = "mutation"
    SUBSCRIPTION = "subscription"


class GraphQLOperation(BaseModel):
    """GraphQL-specific metadata attached to an OperationConfig.

    Carries the actual query template + variable signature so the generator
    can render `_execute_graphql(query, variables)` calls and the QA agent
    can validate the query against an introspected schema.
    """
    operation_type: GraphQLOperationType = GraphQLOperationType.QUERY
    operation_name: str = Field(..., description="Field name on Query/Mutation root")
    query_template: str = Field(
        ...,
        description=(
            "Full GraphQL document text with $variable placeholders, "
            "e.g. 'query order($id: ID!) { order(id: $id) { id name } }'"
        ),
    )
    return_type: Optional[str] = Field(
        default=None,
        description="GraphQL return type name (e.g., 'Order', '[Order!]!')",
    )
    selection_set: Optional[str] = Field(
        default=None,
        description="The default selection set used inside query_template",
    )
    selected_fields: Optional[List[str]] = Field(
        default=None,
        description=(
            "Record columns export() returns for this query — the top-level "
            "field names of the record node, parsed from query_template at "
            "generation. discover_schema advertises EXACTLY these (never the "
            "wider full-type introspection) so the destination isn't built with "
            "columns export() never fills. None/[] → fall back to full introspection."
        ),
    )
    exportable_default: bool = Field(
        default=True,
        description=(
            "True when this root field is a parameterless / list export "
            "resource (surfaced as a default table in discover_schema). False "
            "for arg-required singular lookups (e.g. country(code: ID!)) — they "
            "stay callable via export() with explicit variables but are NOT "
            "shown as default exportable tables (selecting one parameterless "
            "produces a missing-required-argument error)."
        ),
    )


class ParameterDefinition(BaseModel):
    """Definition of an operation parameter"""
    name: str = Field(..., description="Parameter name")
    type: ParameterType = Field(default=ParameterType.STRING, description="Data type")
    required: bool = Field(default=False, description="Whether parameter is required")
    default: Optional[Any] = Field(default=None, description="Default value if not provided")
    description: str = Field(default="", description="Human-readable description")
    enum_values: Optional[List[str]] = Field(default=None, description="Allowed values for enum types")
    secret: bool = Field(default=False, description="Whether this is a sensitive value")
    
    @field_validator('name')
    @classmethod
    def validate_name(cls, v: str) -> str:
        if not re.match(r'^[a-z][a-z0-9_]*$', v):
            raise ValueError(f"Parameter name must be snake_case: {v}")
        return v


class OperationConfig(BaseModel):
    """Configuration for a single connector operation"""
    name: str = Field(..., description="Operation name (e.g., 'export', 'discover_schema')")
    type: OperationType = Field(..., description="Operation classification")
    description: str = Field(default="", description="Human-readable description")
    parameters: List[ParameterDefinition] = Field(default_factory=list)
    returns_schema: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Expected return schema (for validation)"
    )
    requires_connection: bool = Field(
        default=True,
        description="Whether this operation requires an active connection"
    )
    idempotent: bool = Field(
        default=True,
        description="Whether the operation is idempotent (safe to retry)"
    )
    # Protocol-specific extension. Populated when Protocol.GRAPHQL.
    graphql: Optional[GraphQLOperation] = Field(
        default=None,
        description="GraphQL operation metadata (query template, return type, …)",
    )
    
    @field_validator('name')
    @classmethod
    def validate_operation_name(cls, v: str) -> str:
        if not re.match(r'^[a-z][a-z0-9_]*$', v):
            raise ValueError(f"Operation name must be snake_case: {v}")
        return v
    
    @property
    def method_name(self) -> str:
        """Get the Python method name for this operation"""
        return self.name
    
    def get_required_params(self) -> List[ParameterDefinition]:
        """Get list of required parameters"""
        return [p for p in self.parameters if p.required]


class SupportedAuthMethod(BaseModel):
    """One auth method a connector accepts at runtime.

    Generation declares the SET of methods supported (per vendor); the user
    picks ONE at connection time via `config["auth_method"]`. The connector's
    runtime auth dispatcher formats the request accordingly.
    """
    method: AuthType = Field(..., description="Wire-format kind")
    header_name: str = Field(
        default="Authorization",
        description="HTTP header carrying the credential",
    )
    header_prefix: str = Field(
        default="",
        description="Prefix prepended to the credential (e.g. 'Bearer ')",
    )
    query_param: Optional[str] = Field(
        default=None,
        description=(
            "Query-parameter name carrying the credential (e.g. 'api_token' for "
            "?api_token=<value>). Set ONLY for api_key_query methods; when set the "
            "credential is sent as a URL query parameter instead of a header."
        ),
    )
    config_keys: List[str] = Field(
        default_factory=list,
        description=(
            "Connection-time field names that supply the credential, in priority order. "
            "e.g. ['access_token','token'] for bearer; ['username','password'] for basic."
        ),
    )
    description: str = Field(
        default="",
        description="Human-readable explanation shown in the connection UI",
    )
    label: str = Field(
        default="",
        description="Short human label shown in the connection-modal method picker "
                    "(e.g. 'API key (query param)'). Empty → the UI derives one from method.",
    )
    oauth_provider: Optional[str] = Field(
        default=None,
        description=(
            "For method=oauth2 only: the registered OAuth provider key (exact key in "
            "providers.json) this method authenticates through. Lets the connection modal "
            "bind the OAuth Connect button to this method even when oauth2 is NOT the "
            "connector's default scheme (the multi-scheme pipedrive case). None for "
            "non-oauth2 methods or when no registered provider matches."
        ),
    )
    credentials_optional: bool = Field(
        default=False,
        description=(
            "Declares that this method has a working path with NONE of its config_keys "
            "supplied, so the connection form must not refuse a blank save: a "
            "credential-less deployment (mongodb unauthenticated, gcs/bigquery via "
            "Application Default Credentials, azure-blob anonymous) or a one-of rule the "
            "connector enforces in validate_config rather than in required_config. "
            "Neither is visible to the pre-start gate, which only checks required_config. "
            "This is a CLAIM, not a derived value -- deriving it from `required` would "
            "make llm-service/tests/test_connector_auth_contract.py, which rejects a "
            "method that names credential keys, requires none of them and says nothing, "
            "unable to fail. Leave it False and mark the credential required instead."
        ),
    )

    @field_validator("config_keys")
    @classmethod
    def _validate_keys(cls, v: List[str]) -> List[str]:
        return [k.strip().lower() for k in v if k and k.strip()]


# Canonical OAuth2 grant/flow types we recognise (RFC 6749 names, snake_cased to
# match openapi_discovery's flow_type and api-gateway's token-exchange handling).
_OAUTH_GRANT_TYPES = frozenset(
    {"authorization_code", "client_credentials", "implicit", "password"}
)


class CredentialHeader(BaseModel):
    """A credential-bearing HTTP header the API requires IN ADDITION to the
    primary auth header.

    Some APIs demand more than one secret header on EVERY call — e.g. Datadog
    needs BOTH ``DD-API-KEY`` and ``DD-APPLICATION-KEY`` (its OpenAPI declares
    them AND-co-required in a single security requirement block). The primary
    credential is handled by the normal auth path (`AuthConfig` + `_get_headers`);
    each extra one is declared here and stamped from connection config at request
    time by the rendered connector's `_make_request_v2`.
    """
    header_name: str = Field(..., description="HTTP header carrying the credential")
    config_keys: List[str] = Field(
        default_factory=list,
        description="Connection-time field names supplying this header's value, in priority order",
    )
    header_prefix: str = Field(
        default="",
        description="Prefix prepended to the credential (e.g. 'Bearer '); usually empty for apiKey headers",
    )

    @field_validator("config_keys")
    @classmethod
    def _validate_keys(cls, v: List[str]) -> List[str]:
        return [k.strip().lower() for k in v if k and k.strip()]


# Connection-time field names the RENDERED connector's LEGACY single-auth
# ``_get_headers`` actually reads, per auth type — the ``{% else %}`` branch of
# ``templates/connector.py.j2``. This answers a DIFFERENT question from
# ``architect_rest._CANONICAL_CONFIG_KEYS``, which pins the keys the connection
# FORM should offer for a method the LLM proposed; this map describes the keys an
# already-rendered single-auth connector will actually look up at request time,
# in the order it looks them up. Used only to derive ``AuthConfig.declared_methods``
# for a spec that carries no explicit ``supported_methods``.
_LEGACY_AUTH_CONFIG_KEYS: Dict[AuthType, List[str]] = {
    AuthType.BEARER: ["access_token", "token", "api_key"],
    AuthType.API_KEY: ["api_key", "token"],
    AuthType.API_KEY_QUERY: ["api_key", "token"],
    AuthType.BASIC: ["username", "password"],
    AuthType.CUSTOM_HEADER: ["access_token", "token", "api_key"],
    AuthType.OAUTH2: ["access_token", "token"],
}


class AuthConfig(BaseModel):
    """Authentication configuration"""
    type: AuthType = Field(..., description="Default authentication type (when user doesn't pick)")

    # Phase 12: list of all auth methods the generated connector accepts at
    # runtime. When non-empty, the runtime dispatcher uses these; the user
    # selects one via `config["auth_method"]` at connection time. Empty list
    # falls back to the legacy single-auth path using the fields below.
    supported_methods: List[SupportedAuthMethod] = Field(
        default_factory=list,
        description="Auth methods the generated connector supports at runtime",
    )

    # Datadog-style AND-required multi-header auth: credential headers the API
    # demands on EVERY call IN ADDITION to the primary auth header. Empty for the
    # common single-credential case. See CredentialHeader.
    extra_required_headers: List[CredentialHeader] = Field(
        default_factory=list,
        description="Extra credential headers required on every request alongside the primary auth header",
    )

    # For API_KEY / BEARER
    header_name: str = Field(
        default="Authorization",
        description="Header name for auth token"
    )
    header_prefix: Optional[str] = Field(
        default="Bearer",
        description="Prefix for auth value (e.g., 'Bearer', 'Token')"
    )
    env_var: str = Field(
        default="MCP_API_KEY",
        description="Environment variable name for the API key"
    )
    
    # For BASIC auth
    username_env: Optional[str] = Field(
        default=None,
        description="Env var for username (basic auth)"
    )
    password_env: Optional[str] = Field(
        default=None,
        description="Env var for password (basic auth)"
    )
    
    # For OAUTH2
    oauth_provider: Optional[str] = Field(
        default=None,
        description="OAuth provider name (e.g., 'hubspot', 'salesforce')"
    )
    oauth_scopes: Optional[List[str]] = Field(
        default=None,
        description="Required OAuth scopes"
    )
    token_url: Optional[str] = Field(
        default=None,
        description="OAuth token endpoint URL"
    )
    authorize_url: Optional[str] = Field(
        default=None,
        description="OAuth authorization endpoint URL"
    )
    refresh_url: Optional[str] = Field(
        default=None,
        description="OAuth token refresh endpoint URL (the spec's refreshUrl); "
                    "often equal to token_url"
    )
    grant_type: Optional[str] = Field(
        default=None,
        description="OAuth2 grant/flow type the connector uses: one of "
                    "authorization_code | client_credentials | implicit | password. "
                    "Derived from the spec's declared OAuth2 flow; None when unknown "
                    "(api-gateway then defaults to authorization_code)."
    )
    requires_subdomain: bool = Field(
        default=False,
        description="OAuth provider whose auth/token URLs contain a per-tenant "
                    "subdomain placeholder (e.g. Shopify's {shop}). Derived "
                    "automatically from a '{shop}'/'{subdomain}' placeholder when not set."
    )
    token_never_expires: bool = Field(
        default=False,
        description="OAuth provider that issues non-expiring access tokens with no "
                    "refresh_token (e.g. Shopify offline tokens). When true, the registry "
                    "marks the provider so api-gateway does not stamp a bogus 1h expiry."
    )

    # For CUSTOM_HEADER
    custom_headers: Optional[Dict[str, str]] = Field(
        default=None,
        description="Custom headers to add to requests"
    )

    # For API_KEY_QUERY — credential sent as a URL query parameter, not a header.
    query_param: Optional[str] = Field(
        default=None,
        description="Query-parameter name carrying the credential (api_key_query auth)."
    )

    @model_validator(mode='after')
    def validate_auth_config(self):
        if self.type == AuthType.BASIC:
            # NOTE:
            # Some APIs (e.g., Segment write key) use HTTP Basic with an empty password.
            # In that case we represent the password as an empty string via password_env: "".
            if not self.username_env:
                raise ValueError("Basic auth requires username_env")
            if self.password_env is None:
                raise ValueError(
                    "Basic auth requires password_env (use empty string for password-less Basic auth)"
                )
        
        if self.type == AuthType.OAUTH2:
            if not self.oauth_provider:
                raise ValueError("OAuth2 auth requires oauth_provider")

        if self.grant_type is not None and self.grant_type not in _OAUTH_GRANT_TYPES:
            raise ValueError(
                f"grant_type must be one of {sorted(_OAUTH_GRANT_TYPES)}, "
                f"got {self.grant_type!r}"
            )

        if self.type == AuthType.API_KEY_QUERY:
            if not self.query_param:
                raise ValueError(
                    "api_key_query auth requires query_param (the URL query parameter name)"
                )

        return self

    credentials_optional: bool = Field(
        default=False,
        description=(
            "Connector-level form of SupportedAuthMethod.credentials_optional, applied to "
            "every non-oauth method this connector declares. Set it where the generator "
            "DELIBERATELY leaves every credential field optional -- the api_saas "
            "'API token now, OAuth later' path in architect.py, which downgrades "
            "access_token/api_key to optional and enforces one-of in validate_config. "
            "Without it that connector ships metadata whose form gate can never engage "
            "and whose contract test cannot tell the choice from an accident."
        ),
    )

    @property
    def declared_methods(self) -> List["SupportedAuthMethod"]:
        """The auth methods this connector DECLARES, for the metadata contract.

        Every credentialed connector must publish a non-empty
        ``supported_auth_methods`` array so the connection modal never has to
        infer credentials per connector (user decision 2026-06-20, enforced by
        ``llm-service/tests/test_connector_auth_contract.py``). Only the agentic
        architects populate ``supported_methods``; a spec built by the
        deterministic scaffolder — or by any other single-auth producer — leaves
        it empty and would otherwise ship metadata with no array at all.

        So: return the explicit list when there is one, and otherwise derive a
        one-element list from the flat auth fields, which describe exactly the
        single method that connector accepts. ``AuthType.NONE`` declares nothing
        — a public API has no credential to pick, and the contract exempts it.

        This is deliberately a view for the METADATA renderer only. The rendered
        connector's runtime dispatch still keys off ``supported_methods``, so a
        derived single method changes no generated Python: the connector keeps
        the legacy per-type ``_get_headers``, whose empty-credential handling
        differs from the multi-auth dispatcher's.
        """
        if self.supported_methods:
            return self._apply_credentials_optional(list(self.supported_methods))
        if self.type == AuthType.NONE:
            return []
        return self._apply_credentials_optional([SupportedAuthMethod(
            method=self.type,
            header_name=self.header_name,
            header_prefix=self.header_prefix or "",
            query_param=self.query_param,
            config_keys=list(_LEGACY_AUTH_CONFIG_KEYS.get(self.type, [])),
            oauth_provider=self.oauth_provider if self.type == AuthType.OAUTH2 else None,
        )])

    def _apply_credentials_optional(
        self, methods: List["SupportedAuthMethod"]
    ) -> List["SupportedAuthMethod"]:
        """Push the connector-level ``credentials_optional`` claim onto each method.

        A method may also carry the flag itself; this only ever adds. oauth2/oauth are
        left alone -- the Connect flow owns the credential, the form renders no field
        for it, and the contract test exempts those methods already.
        """
        if not self.credentials_optional:
            return methods
        out: List["SupportedAuthMethod"] = []
        for m in methods:
            kind = getattr(m.method, "value", m.method)
            if kind in ("oauth2", "oauth") or m.credentials_optional:
                out.append(m)
            else:
                out.append(m.model_copy(update={"credentials_optional": True}))
        return out


class ResourceConfig(BaseModel):
    """Configuration for a resource/endpoint in API connectors"""
    name: str = Field(..., description="Resource name (e.g., 'contacts', 'orders')")
    endpoint: str = Field(..., description="API endpoint path (e.g., '/crm/v3/objects/contacts')")
    # Names of {placeholders} that appear in `endpoint` and MUST be substituted
    # at request time, in order (e.g. ['owner', 'repo'] for
    # '/repos/{owner}/{repo}/commits'). Empty for non-parameterized collections.
    # The full path template is preserved in `endpoint` (never stripped to
    # '/repos/commits'); the generated connector substitutes these from
    # connection config (then call params) before issuing the request.
    path_params: List[str] = Field(
        default_factory=list,
        description="Ordered {placeholder} names in `endpoint` to substitute at request time",
    )
    id_field: str = Field(default="id", description="Field name for resource ID")
    supports_list: bool = Field(default=True)
    supports_get: bool = Field(default=True)
    supports_create: bool = Field(default=False)
    supports_update: bool = Field(default=False)
    supports_delete: bool = Field(default=False)
    # HTTP verb the item route uses for updates. HubSpot/Intercom item routes
    # accept PATCH only and 405 on PUT; keep PUT as the safe default so
    # existing PUT-only connectors are unchanged.
    update_method: str = Field(default="PUT", description="HTTP verb for item-route updates ('PUT' or 'PATCH')")
    list_method: str = Field(default="GET", description="HTTP verb for the list/collection read. 'GET' for normal REST; 'POST' for JSON-RPC / POST-list APIs (Outline /documents.list) that send list filters + pagination in the request body.")
    pagination_type: Optional[str] = Field(
        default="offset",
        description="Pagination strategy: 'offset', 'cursor', 'page', 'link'"
    )
    pagination_param: str = Field(default="offset", description="Pagination parameter name")
    limit_param: str = Field(default="limit", description="Limit parameter name")
    max_page_size: int = Field(default=100, description="Maximum items per page")
    
    # Incremental sync support
    supports_incremental: bool = Field(default=False, description="Whether resource supports incremental filtering")
    incremental_param: str = Field(default="updated_since", description="Query param for timestamp filter")
    incremental_field: str = Field(default="updated_at", description="Response field with last update time")
    cursor_param: Optional[str] = Field(default=None, description="Optional override for cursor-based pagination param")

    # Response shape hints (used by PaginationHandler to extract records + cursor)
    response_data_key: str = Field(
        default="",
        description="Response key where records live ('' = auto-detect). E.g. 'data', 'results', 'channels'",
    )
    cursor_path: str = Field(
        default="",
        description="Dot-path to next-cursor in response. E.g. 'paging.next.after', 'response_metadata.next_cursor'",
    )
    cursor_mode: str = Field(
        default="response",
        description="How to compute next cursor. 'response' = from response body; 'last_item_id' = last record id (Stripe-style)",
    )
    record_is_object: bool = Field(
        default=False,
        description="True when the 2xx response body IS a single record (a lone entity like Twilio /Balance.json or Jira /myself, with no records array). Tells the runtime to wrap the object as a one-row list instead of yielding 0 rows.",
    )

    @field_validator('endpoint')
    @classmethod
    def _sanitize_endpoint(cls, v: str) -> str:
        """Strip stray whitespace + trailing punctuation and ensure a leading
        slash. A no-spec/LLM path can tokenize resources out of description prose
        and leak garbage paths like '/data.', '/table,', '/table;'; real API
        paths never end in punctuation, so this is a no-op for spec-derived
        endpoints but stops 404-guaranteed paths from shipping."""
        if not isinstance(v, str):
            return v
        s = v.strip().rstrip(" .,;:!?")
        if s and not s.startswith("/"):
            s = "/" + s
        return s


class ConfigField(BaseModel):
    """Configuration field required for connector setup"""
    name: str = Field(..., description="Field name (e.g., 'host', 'api_key')")
    type: ParameterType = Field(default=ParameterType.STRING)
    required: bool = Field(default=True)
    secret: bool = Field(default=False, description="Is this a sensitive value?")
    default: Optional[Any] = Field(default=None)
    description: str = Field(default="")
    env_var: Optional[str] = Field(
        default=None,
        description="Environment variable to read from (e.g., 'MCP_HOST')"
    )
    validation_regex: Optional[str] = Field(
        default=None,
        description="Regex pattern for validation"
    )
    
    # Enum values for dropdowns
    enum_values: Optional[List[str]] = Field(
        default=None,
        description="Allowed values for this field (creates dropdown in UI)"
    )
    
    # Validation constraints
    pattern: Optional[str] = Field(default=None, description="Regex pattern for validation")
    min_length: Optional[int] = Field(default=None, description="Minimum string length")
    max_length: Optional[int] = Field(default=None, description="Maximum string length")
    minimum: Optional[Union[int, float]] = Field(default=None, description="Minimum numeric value")
    maximum: Optional[Union[int, float]] = Field(default=None, description="Maximum numeric value")
    
    # UI metadata
    ui_widget: Optional[str] = Field(
        default=None,
        description="UI widget type: 'text', 'textarea', 'select', 'password', 'number'"
    )
    ui_order: Optional[int] = Field(
        default=None,
        description="Display order in UI (lower numbers first)"
    )
    ui_placeholder: Optional[str] = Field(
        default=None,
        description="Placeholder text for UI input"
    )
    ui_help_text: Optional[str] = Field(
        default=None,
        description="Additional help text for UI"
    )
    sensitive: Optional[bool] = Field(
        default=None,
        description="Alias for 'secret' for backward compatibility"
    )
    
    # Conditional display
    depends_on: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Conditional display rules (e.g., {'provider': 'aws'})"
    )


def build_config_aliases(
    supported_methods: List[Any],
    config_fields: List[Any],
) -> Dict[str, List[str]]:
    """Derive the ``config_aliases`` map so the orchestrator's pre-start required-config
    gate accepts every credential key the connector runtime accepts.

    A single-secret auth method (bearer / api_key / api_key_query / custom_header) carries
    interchangeable ``config_keys`` — alternative NAMES for the SAME secret (e.g. Notion
    ``[access_token, token, integration_token]``). The generator forces the generic
    canonical (access_token / api_key) to ``config_keys[0]`` but keeps the VENDOR field
    (integration_token) as the sole configuration_schema property + ``required_config``
    entry. The connector runtime (``connector.py.j2 _get_headers``) reads the token from the
    first-present config_key, so any interchangeable key authenticates — but the
    orchestrator's PRE-START gate (``missingRequiredConfig``, backend-orchestrator
    ``server_manager.go``) only accepts the required key OR a declared alias. Without this
    map a raw-API submission under ``access_token`` is rejected ("missing required config:
    integration_token"), diverging from runtime.

    We map the canonical (required, schema-backed) key -> its interchangeable alias keys,
    mirroring the frontend's ``splitMethodCredentialKeys``: the schema-backed config_key is
    canonical; the non-schema config_keys are aliases. Multi-secret methods (aws-s3 api_key
    ``[access_key_id, secret_access_key]`` — BOTH schema-backed) are NOT aliased — their keys
    are DISTINCT required inputs, so the gate must keep requiring each. ``basic`` (distinct
    username + password) and ``oauth2`` (credential acquired via the Connect flow) are
    skipped for the same reason.
    """
    field_names_lower = {f.name.lower() for f in (config_fields or [])}
    required_lower = {f.name.lower() for f in (config_fields or []) if f.required}
    canonical_by_lower = {f.name.lower(): f.name for f in (config_fields or [])}

    aliases: Dict[str, List[str]] = {}
    for method in (supported_methods or []):
        kind = getattr(method, "method", None)
        kind = getattr(kind, "value", kind)  # AuthType enum -> str; plain str passes through
        if kind in ("basic", "oauth2", "oauth"):
            continue
        keys = list(getattr(method, "config_keys", None) or [])
        if not keys:
            continue
        schema_backed = [k for k in keys if k.lower() in field_names_lower]
        # Exactly one schema-backed key == a single interchangeable secret. Zero -> no
        # required/schema canonical to alias; two or more -> multi-secret distinct fields.
        if len(schema_backed) != 1:
            continue
        # Emit under the exact required_config spelling; only meaningful when the canonical
        # is a REQUIRED field (the gate only ever checks required keys).
        canonical = canonical_by_lower.get(schema_backed[0].lower(), schema_backed[0])
        if canonical.lower() not in required_lower:
            continue
        for k in keys:
            if k.lower() == canonical.lower() or k.lower() in field_names_lower:
                continue  # skip the canonical itself + any DISTINCT schema field
            aliases.setdefault(canonical, [])
            if k not in aliases[canonical]:
                aliases[canonical].append(k)
    return aliases


class ConnectorSpec(BaseModel):
    """
    Complete specification for an MCP connector.

    This is the master schema that drives code generation.
    All fields here map directly to generated connector code.
    """
    
    # Identity
    name: str = Field(..., description="Connector identifier (lowercase, no spaces)")
    display_name: str = Field(..., description="Human-readable display name")
    aliases: Optional[List[str]] = Field(
        default=None,
        description="Optional legacy identifiers accepted for this connector (e.g., ['aws_s3', 'AWS S3'])",
    )
    version: str = Field(default="1.0.0", description="Connector version (semver)")
    description: str = Field(default="", description="Connector description")
    # Vendor-declared logo URL captured from the source spec (e.g. OpenAPI
    # `info.x-logo.url`, the ReDoc/Redocly convention). When set, the logo
    # downloader fetches this directly instead of guessing a brand slug.
    logo_url: Optional[str] = Field(
        default=None,
        description="Vendor-declared logo URL (e.g. OpenAPI info.x-logo.url); fetched as the connector logo when present",
    )
    
    # Classification
    category: ConnectorCategory = Field(..., description="Connector category")
    # Wire protocol — defaults to REST so existing connector specs migrate
    # transparently. Generation templates branch on this value.
    protocol: Protocol = Field(
        default=Protocol.REST,
        description="Wire protocol (rest|graphql|openapi). Default REST for backward compat.",
    )
    supports_source: bool = Field(default=True, description="Can be used as data source")
    supports_destination: bool = Field(default=False, description="Can be used as data destination")
    # Which destination write modes the connector actually implements. Subset of
    # ["append", "upsert", "delete"]. The planner trusts this list when wiring a
    # pipeline, so it MUST match the methods the generated connector emits:
    # append→import_data, upsert→upsert_data, delete→delete_data. Empty when
    # supports_destination is False. See docs/connectors/destination-contract.md.
    destination_modes: List[str] = Field(
        default_factory=list,
        description="Implemented destination write modes: subset of append|upsert|delete",
    )
    supports_cdc: bool = Field(default=False, description="Supports Change Data Capture")
    # DDL / destination table provisioning
    # - supports_ddl: connector can execute DDL safely for its dialect (create schema/table/index)
    # - auto_create_destination_tables: sink/planner may rely on connector to auto-create missing dest tables
    supports_ddl: Optional[bool] = Field(
        default=None,
        description="Whether connector supports DDL helpers (ensure_schema/ensure_table) for its dialect",
    )
    auto_create_destination_tables: Optional[bool] = Field(
        default=None,
        description="Whether connector can auto-create destination schema/tables to prevent write failures",
    )

    # Optional domain hint (used for tier-2 fallbacks + QA A-then-C-soft attempts)
    api_category_hint: Optional[str] = Field(
        default=None,
        description="API domain hint (e.g., 'crm', 'ecommerce', 'payments') to improve fallbacks when docs are weak",
    )
    api_category_hint_source: Optional[str] = Field(
        default=None,
        description="Hint source: user_manual|user_confirmed_auto|auto_only|oauth_registry|known_apis|keyword|default",
    )
    api_category_hint_confidence: Optional[float] = Field(
        default=None,
        description="Confidence for api_category_hint (0.0-1.0)",
    )
    
    # Connection
    base_url: Optional[str] = Field(
        default=None,
        description="Base URL for API connectors (can include {placeholders})"
    )
    default_port: Optional[int] = Field(
        default=None,
        description="Default port for database connectors"
    )
    connection_timeout: int = Field(default=30, description="Connection timeout in seconds")

    # Write-body wire encoding: "json" (default) sends POST/PUT/PATCH bodies as
    # application/json; "form" sends them as application/x-www-form-urlencoded for
    # APIs that require it and ignore JSON (Twilio classic, Stripe → 400 otherwise).
    write_content_type: str = Field(
        default="json",
        description="Request body encoding for writes: 'json' or 'form' (x-www-form-urlencoded)",
    )

    # Authentication
    auth: AuthConfig = Field(..., description="Authentication configuration")

    # F7 — the spec declared NO securityScheme AND no positive auth signal was
    # recovered from header params / prose, so auth_type was defaulted to `none`
    # from spec-SILENCE (not a confirmed-public API). Such a connector still
    # injects an OPTIONAL user/env-supplied credential into `auth_undetermined_
    # header` WHEN ONE IS CONFIGURED — a genuinely public API sends nothing
    # (byte-identical to a plain none connector), while an auth-guarded API whose
    # spec merely omitted its scheme (Metabase's Malli-generated spec) authenticates
    # once the user provides a key. False for confirmed-public / vendor-declared-none.
    auth_undetermined: bool = Field(
        default=False,
        description="True when auth_type=none was defaulted from an OpenAPI spec that declared no securityScheme and carried no auth signal — inject an optional credential when configured.",
    )
    auth_undetermined_header: str = Field(
        default="X-API-Key",
        description="Header carrying the optional credential when auth_undetermined is True (X-API-Key covers the common self-hosted case, incl. Metabase).",
    )

    # Constant non-auth headers sent on EVERY request, derived from required
    # `in: header` parameters in the OpenAPI spec that carry a fixed value
    # (e.g. {'Notion-Version': '2022-06-28'}). The auth header is excluded here
    # (the auth dispatcher owns it). Many APIs 400 without a mandatory version
    # header, so these must be emitted in the generated _get_headers().
    default_headers: Dict[str, str] = Field(
        default_factory=dict,
        description="Constant non-auth headers sent on every request (e.g. API version pin)",
    )
    # Names of ubiquitous required headers the spec declares WITHOUT a fixed
    # value (e.g. Notion-Version). Surfaced as config fields; the connector
    # sends config[name] as the header when the user supplies it.
    header_config_keys: List[str] = Field(
        default_factory=list,
        description="Config-sourced header names sent from connection config when set",
    )

    # Configuration
    config_fields: List[ConfigField] = Field(
        default_factory=list,
        description="Required configuration fields"
    )
    
    # Operations
    operations: List[OperationConfig] = Field(
        default_factory=list,
        description="Supported operations"
    )
    
    # Resources (for API connectors)
    resources: List[ResourceConfig] = Field(
        default_factory=list,
        description="API resources/objects"
    )
    
    # Capabilities
    max_batch_size: int = Field(default=10000, description="Maximum batch size for operations")
    supported_formats: List[str] = Field(
        default_factory=lambda: ["json"],
        description="Supported data formats"
    )
    
    # Dependencies
    python_dependencies: List[str] = Field(
        default_factory=list,
        description="Python packages required (for requirements.txt)"
    )
    
    # Metadata
    spec_version: str = Field(default="1.1", description="Spec schema version")
    author: Optional[str] = Field(default=None)
    license: str = Field(default="MIT")
    repository: Optional[str] = Field(default=None)
    documentation_url: Optional[str] = Field(default=None)
    # Free-form architect/builder metadata (e.g. lazy_ops flag, custom_scalars list).
    # Survives JSON round-trip; templates read via `spec.metadata.get(...)`.
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # Quality tier (added for market-ready connector generation)
    quality_tier: Optional[str] = Field(
        default=None,
        description="Connector quality tier: gold, silver, bronze, draft"
    )
    quality_score: Optional[float] = Field(
        default=None,
        description="Quality score (0-100)"
    )
    authoritativeness_score: Optional[float] = Field(
        default=None,
        description="Context7 authoritativeness score (0-100)"
    )

    # ─── Connector lifecycle model (Phase 1) ──────────────────────────────
    # `source` is the immutable origin signal — set once at creation.
    # `lifecycle` is the computed maturity — DERIVED at read time from real
    # execution evidence by the api-gateway, NOT stored statically.
    # `status` and `quality_tier` are deprecated synonyms kept for one release
    # so existing readers don't break; new code should prefer `source` +
    # the api-gateway's computed lifecycle.
    source: Optional[str] = Field(
        default=None,
        description="Connector origin: built_in|generated|partner|custom",
    )

    # QA metadata (populated after QA runs; used for transparency in UI/API)
    status: Optional[str] = Field(
        default=None,
        description="DEPRECATED. Use `source` + computed lifecycle instead.",
    )
    qa_warnings: Optional[List[str]] = Field(
        default=None,
        description="Non-blocking QA warnings (e.g., export not validated, naming mismatch)",
    )
    qa_metadata: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Structured QA metadata (export attempts, outcomes, reasons)",
    )
    
    # Curated actions (added for market-ready connector generation)
    curated_actions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="High-value action operations (typed params)"
    )
    
    @field_validator('name')
    @classmethod
    def validate_connector_name(cls, v: str) -> str:
        """Ensure name is a valid Python identifier (lowercase, underscores ok)"""
        if not re.match(r'^[a-z][a-z0-9_]*$', v.replace('-', '_')):
            raise ValueError(f"Connector name must be lowercase alphanumeric with underscores: {v}")
        return v.replace('-', '_')  # Normalize to underscore
    
    @field_validator('version')
    @classmethod
    def validate_version(cls, v: str) -> str:
        """Validate semver format"""
        if not re.match(r'^\d+\.\d+\.\d+(-[a-z0-9]+)?$', v):
            raise ValueError(f"Version must be semver format (x.y.z): {v}")
        return v
    
    @model_validator(mode='after')
    def _dedupe_resources(self):
        """Drop duplicate resources by name (keep first). A no-spec/LLM path can
        emit two resources with the same name (e.g. 'table' tokenized from both
        '/table,' and '/table;'); the generated connector renders resources into
        a dict literal keyed by name, so duplicates would silently collide and
        one would be lost. Dedup here so the template never sees a colliding key."""
        if self.resources:
            seen: set = set()
            deduped = []
            for r in self.resources:
                if r.name in seen:
                    continue
                seen.add(r.name)
                deduped.append(r)
            self.resources = deduped
        return self

    @model_validator(mode='after')
    def validate_operations(self):
        """Ensure required operations are present based on category"""
        if not self.category:
            return self
        
        # Core operations required for all categories
        required_ops = {'test_connection', 'validate_config', 'discover_schema', 'get_capabilities'}
        
        # Add category-specific required operations
        if self.category in [ConnectorCategory.RELATIONAL_DB, ConnectorCategory.DOCUMENT_DB]:
            required_ops.add('export')
        elif self.category == ConnectorCategory.CLOUD_STORAGE:
            required_ops.add('read')
        elif self.category == ConnectorCategory.API_SAAS:
            required_ops.add('export')
        
        # Check if all required ops are present
        present_ops = {op.name for op in self.operations}
        missing = required_ops - present_ops
        
        if missing and self.operations:  # Only validate if operations were provided
            raise ValueError(f"Missing required operations for {self.category.value}: {missing}")

        # ---------------------------------------------------------------------
        # DDL capability defaults (fail-closed; enable only for known-safe dialects)
        # ---------------------------------------------------------------------
        # We only auto-enable for relational DB connectors we know how to implement safely today.
        # New dialects can set supports_ddl/auto_create_destination_tables explicitly in the spec.
        if self.supports_ddl is None:
            if self.supports_destination and self.category == ConnectorCategory.RELATIONAL_DB:
                n = (self.name or "").lower()
                # Conservative allow-list (extend as we implement more dialects)
                self.supports_ddl = any(k in n for k in ("postgres", "postgresql", "mysql", "mariadb"))
            else:
                self.supports_ddl = False

        if self.auto_create_destination_tables is None:
            # "Will the destination have somewhere to write without the user
            # pre-creating anything?" — a WIDER question than "can it run DDL":
            #   * relational sinks answer yes by issuing CREATE TABLE
            #   * an object store answers yes because there is no table to create
            #     at all; the sink writes one object per batch under a prefix
            # Defaulting cloud storage to False made every pipeline into gcs /
            # aws-s3 / azure-blob fail the pre-migration assessment with a
            # BLOCKING SINK_NO_DDL error the user could not clear — there was no
            # table for them to go and pre-create. supports_ddl stays False for
            # object stores: the sink worker requires supports_ddl AND
            # auto_create before it calls ensure_table, so it still issues none.
            self.auto_create_destination_tables = bool(
                self.supports_destination
                and (
                    (self.category == ConnectorCategory.RELATIONAL_DB and self.supports_ddl)
                    or self.category == ConnectorCategory.CLOUD_STORAGE
                )
            )
        
        return self
    
    @property
    def class_name(self) -> str:
        """Get the Python class name for this connector"""
        # PascalCase + Connector suffix — matches GraphQL template + hand-curated
        # connectors (PostgresqlConnector, ShopifyAdminGraphqlConnector).
        parts = self.name.split('_')
        return ''.join(word.capitalize() for word in parts) + 'Connector'
    
    @property
    def connector_type(self) -> str:
        """Get the connector type identifier (used in MCP protocol)"""
        return self.name.replace('_', '-')
    
    def get_required_config_fields(self) -> List[ConfigField]:
        """Get list of required configuration fields"""
        return [f for f in self.config_fields if f.required]

    @property
    def config_aliases(self) -> Dict[str, List[str]]:
        """Interchangeable-credential-key alias map consumed by the orchestrator's
        pre-start required-config gate. See module-level ``build_config_aliases``."""
        methods = self.auth.supported_methods if self.auth else []
        return build_config_aliases(methods, self.config_fields)
    
    def get_operations_by_type(self, op_type: OperationType) -> List[OperationConfig]:
        """Get operations filtered by type"""
        return [op for op in self.operations if op.type == op_type]
    
    def to_metadata_dict(self) -> Dict[str, Any]:
        """Convert to metadata.json format"""
        # Canonical connector id is kebab-case (used by UI/API/planner).
        # Keep legacy fields for backward compatibility.
        canonical_id = self.connector_type
        derived_aliases = [
            canonical_id,
            self.name,
            self.display_name,
            self.display_name.lower().replace(" ", "-"),
        ]
        aliases = list(dict.fromkeys((self.aliases or []) + derived_aliases))
        return {
            "id": canonical_id,
            "name": self.display_name,
            "display_name": self.display_name,
            "aliases": aliases,
            "version": self.version,
            "description": self.description,
            "connector_type": self.connector_type,
            "category": self.category.value,
            # Post-generation quality + QA transparency
            "status": self.status,
            "quality_tier": self.quality_tier,
            "quality_score": self.quality_score,
            "authoritativeness_score": self.authoritativeness_score,
            "api_category_hint": self.api_category_hint,
            "api_category_hint_source": self.api_category_hint_source,
            "api_category_hint_confidence": self.api_category_hint_confidence,
            "qa_warnings": self.qa_warnings or [],
            "qa_metadata": self.qa_metadata or {},
            # Capability flags at top level (required by API Gateway and Frontend)
            "supports_source": self.supports_source,
            "supports_destination": self.supports_destination,
            "destination_modes": self.destination_modes or [],
            "supports_cdc": self.supports_cdc,  # Moved to top level
            "supports_ddl": bool(self.supports_ddl),
            "auto_create_destination_tables": bool(self.auto_create_destination_tables),
            "runtime": "python",
            "entrypoint": "connector.py",
            "required_config": [f.name for f in self.get_required_config_fields()],
            "config_aliases": self.config_aliases,
            "optional_config": [f.name for f in self.config_fields if not f.required],
            "capabilities": {
                "max_batch_size": self.max_batch_size,
                "supported_formats": self.supported_formats,
                "supports_ddl": bool(self.supports_ddl),
                "auto_create_destination_tables": bool(self.auto_create_destination_tables),
            },
            "operations": [
                {
                    "name": op.name,
                    "type": op.type.value,
                    "description": op.description,
                }
                for op in self.operations
            ],
        }


# =============================================================================
# FACTORY FUNCTIONS FOR COMMON PATTERNS
# =============================================================================

def create_core_operations() -> List[OperationConfig]:
    """Create the standard core operations required by all connectors"""
    return [
        OperationConfig(
            name="test_connection",
            type=OperationType.CORE,
            description="Test connectivity to the data source",
            parameters=[],
            requires_connection=False,
        ),
        OperationConfig(
            name="validate_config",
            type=OperationType.CORE,
            description="Validate configuration without connecting",
            parameters=[],
            requires_connection=False,
        ),
        OperationConfig(
            name="discover_schema",
            type=OperationType.CORE,
            description="Discover available tables/collections and their schemas",
            parameters=[
                ParameterDefinition(name="include_columns", type=ParameterType.BOOLEAN, default=True,
                                   description="Include column metadata (names, types, nullability)"),
                ParameterDefinition(name="include_row_counts", type=ParameterType.BOOLEAN, default=True,
                                   description="Include estimated row counts (fast, no full scans)"),
                ParameterDefinition(name="include_relationships", type=ParameterType.BOOLEAN, default=False,
                                   description="Include primary keys and foreign keys (for agents)"),
                ParameterDefinition(name="include_indexes", type=ParameterType.BOOLEAN, default=False,
                                   description="Include index metadata (for agents)"),
                ParameterDefinition(name="max_tables", type=ParameterType.INTEGER, default=100,
                                   description="Maximum number of tables to return"),
            ],
        ),
        OperationConfig(
            name="get_capabilities",
            type=OperationType.CORE,
            description="Return connector capabilities for agent interaction",
            parameters=[],
            requires_connection=False,
        ),
    ]


def create_database_operations() -> List[OperationConfig]:
    """Create standard operations for database connectors"""
    ops = create_core_operations()
    ops.extend([
        OperationConfig(
            name="export",
            type=OperationType.SOURCE,
            description="Export data from a table",
            parameters=[
                ParameterDefinition(name="table", type=ParameterType.STRING, required=True),
                ParameterDefinition(name="limit", type=ParameterType.INTEGER, default=10000),
                ParameterDefinition(name="offset", type=ParameterType.INTEGER, default=0),
                ParameterDefinition(name="where", type=ParameterType.STRING, default=""),
                ParameterDefinition(name="columns", type=ParameterType.ARRAY, default=[]),
            ],
        ),
        OperationConfig(
            name="import_data",
            type=OperationType.DESTINATION,
            description="Import data into a table",
            parameters=[
                ParameterDefinition(name="table", type=ParameterType.STRING, required=True),
                ParameterDefinition(name="data", type=ParameterType.ARRAY, required=True),
                ParameterDefinition(name="mode", type=ParameterType.STRING, default="append", 
                                   enum_values=["append", "replace", "upsert"]),
            ],
        ),
    ])
    return ops


def create_api_operations() -> List[OperationConfig]:
    """Create standard operations for API/SaaS connectors"""
    ops = create_core_operations()
    ops.extend([
        OperationConfig(
            name="export",
            type=OperationType.SOURCE,
            description="Export data from an API resource",
            parameters=[
                ParameterDefinition(name="object", type=ParameterType.STRING, required=True,
                                   description="API object/resource to export"),
                ParameterDefinition(name="limit", type=ParameterType.INTEGER, default=100),
                ParameterDefinition(name="filter", type=ParameterType.OBJECT, default={}),
            ],
        ),
    ])
    return ops


def create_storage_operations() -> List[OperationConfig]:
    """Create standard operations for cloud storage connectors"""
    ops = create_core_operations()
    ops.extend([
        OperationConfig(
            name="read",
            type=OperationType.SOURCE,
            description="Read file from storage",
            parameters=[
                ParameterDefinition(name="bucket", type=ParameterType.STRING, required=True),
                ParameterDefinition(name="key", type=ParameterType.STRING, required=True),
            ],
        ),
        OperationConfig(
            name="import_data",
            type=OperationType.DESTINATION,
            description="Write data to storage",
            parameters=[
                ParameterDefinition(name="bucket", type=ParameterType.STRING, required=True),
                ParameterDefinition(name="key", type=ParameterType.STRING),
                ParameterDefinition(name="data", type=ParameterType.ANY, required=True),
                ParameterDefinition(name="format", type=ParameterType.STRING, default="json",
                                   enum_values=["json", "csv", "parquet", "jsonl"]),
            ],
        ),
    ])
    return ops


def get_default_config_fields(category: ConnectorCategory, auth_type: AuthType) -> List[ConfigField]:
    """
    Get default configuration fields based on connector category and auth type.
    These are the essential fields that should always be present.
    """
    fields = []
    
    if category == ConnectorCategory.RELATIONAL_DB:
        fields = [
            ConfigField(name="host", type=ParameterType.STRING, required=True, description="Database host"),
            ConfigField(name="port", type=ParameterType.INTEGER, required=True, description="Database port"),
            ConfigField(name="database", type=ParameterType.STRING, required=True, description="Database name"),
            ConfigField(name="user", type=ParameterType.STRING, required=True, description="Database user"),
            ConfigField(name="password", type=ParameterType.STRING, required=True, secret=True, description="Database password"),
        ]
    
    elif category == ConnectorCategory.CLOUD_STORAGE:
        if "s3" in auth_type.name.lower() or auth_type == AuthType.API_KEY:
            fields = [
                ConfigField(name="access_key_id", type=ParameterType.STRING, required=True, secret=True, description="AWS Access Key ID"),
                ConfigField(name="secret_access_key", type=ParameterType.STRING, required=True, secret=True, description="AWS Secret Access Key"),
                ConfigField(name="region", type=ParameterType.STRING, required=True, description="AWS Region"),
                ConfigField(name="bucket", type=ParameterType.STRING, required=True, description="S3 bucket name"),
                ConfigField(name="path_prefix", type=ParameterType.STRING, required=False, default="", description="Path prefix for files"),
                ConfigField(name="file_format", type=ParameterType.STRING, required=False, default="json", description="File format (json, csv, parquet)"),
                ConfigField(name="compression", type=ParameterType.STRING, required=False, default="none", description="Compression type (none, gzip, snappy)"),
            ]
    
    elif category == ConnectorCategory.API_SAAS:
        if auth_type == AuthType.OAUTH2:
            fields = [
                ConfigField(name="client_id", type=ParameterType.STRING, required=True, secret=True, description="OAuth2 Client ID"),
                ConfigField(name="client_secret", type=ParameterType.STRING, required=True, secret=True, description="OAuth2 Client Secret"),
                ConfigField(name="redirect_uri", type=ParameterType.STRING, required=False, description="OAuth2 Redirect URI"),
            ]
        elif auth_type == AuthType.API_KEY:
            fields = [
                ConfigField(name="api_key", type=ParameterType.STRING, required=True, secret=True, description="API Key"),
            ]
        elif auth_type == AuthType.BEARER:
            fields = [
                ConfigField(name="access_token", type=ParameterType.STRING, required=True, secret=True, description="Bearer Token"),
            ]
        elif auth_type == AuthType.BASIC:
            # Basic auth (username/password). Many SaaS APIs use "API key as username".
            fields = [
                ConfigField(name="username", type=ParameterType.STRING, required=True, description="Username (or API key)"),
                ConfigField(name="password", type=ParameterType.STRING, required=True, secret=True, description="Password"),
            ]
    
    elif category == ConnectorCategory.DOCUMENT_DB:
        fields = [
            ConfigField(name="host", type=ParameterType.STRING, required=True, description="Database host"),
            ConfigField(name="port", type=ParameterType.INTEGER, required=True, description="Database port"),
            ConfigField(name="database", type=ParameterType.STRING, required=True, description="Database name"),
            ConfigField(name="collection", type=ParameterType.STRING, required=False, description="Collection name"),
        ]
        if auth_type != AuthType.NONE:
            fields.extend([
                ConfigField(name="username", type=ParameterType.STRING, required=True, description="Username"),
                ConfigField(name="password", type=ParameterType.STRING, required=True, secret=True, description="Password"),
            ])
    
    return fields


def merge_config_fields(defaults: List[ConfigField], custom: List[ConfigField]) -> List[ConfigField]:
    """
    Merge custom config fields with defaults, preferring custom definitions.
    """
    # Create a dict of defaults by name
    defaults_dict = {f.name: f for f in defaults}
    
    # Add custom fields (overriding defaults)
    for field in custom:
        defaults_dict[field.name] = field
    
    return list(defaults_dict.values())

