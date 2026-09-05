/**
 * MCP Connector Type Definitions
 * Single source of truth for connector types and UI helpers
 */

// Configuration property schema
export interface ConfigProperty {
  type: "string" | "integer" | "boolean" | "number"
  description: string
  default?: string | number | boolean
  placeholder?: string
  sensitive?: boolean
  secret?: boolean  // Alternative to sensitive, used in metadata.json
  enum?: string[]
  // Optional UI hints (metadata.json). When absent, the form falls back to its
  // heuristics so connectors without these keys behave exactly as before.
  applies?: "source" | "destination" | "both"  // hide fields irrelevant to the chosen direction
  ui_tier?: "basic" | "advanced"               // basic → shown up-front; advanced → collapsed
  ui_order?: number                            // stable field ordering within a tier
  ui_widget?: string
}

// Configuration schema (JSON Schema format)
export interface ConfigurationSchema {
  type: "object"
  required: string[]
  properties: Record<string, ConfigProperty>
}

// Valid connector categories
export type ConnectorCategory =
  | "database"
  | "storage"
  | "api"
  | "analytics"
  | "messaging"
  | "streaming"
  | "file"
  | "other"

// Docker deployment status.
// "auto_deploys_on_use" is what api-gateway reports for Dockerfile connectors
// when it cannot read the Docker socket (the socket is intentionally not mounted
// on api-gateway) — the connector starts on first use rather than being "down".
export type DockerStatus = "running" | "stopped" | "restarting" | "not_deployed" | "auto_deploys_on_use" | "unknown"

// Authentication types supported by connectors.
// `oauth` is a legacy alias for `oauth2` (older curated metadata) — readers
// normalize the two. `api_key_query` carries the key as a URL query parameter
// rather than a header.
export type AuthType =
  | "oauth"
  | "oauth2"
  | "api_key"
  | "api_key_query"
  | "bearer"
  | "basic"
  | "none"
  | "custom_header"

// Phase 13b — Multi-auth metadata declared by the connector (Phase 12 backend).
// Each entry tells the connection-time UI how to format credentials for one
// auth method. The user picks ONE at connection time via a dropdown; the
// runtime dispatcher in the generated connector formats requests accordingly.
export interface SupportedAuthMethod {
  method: AuthType                       // wire-format identifier
  header_name: string                    // HTTP header carrying the credential
  header_prefix: string                  // e.g. "Bearer "
  config_keys: string[]                  // connection-time field names that supply the credential
  description: string                    // shown in the UI under the dropdown choice
  // Optional human label for the dropdown choice (falls back to a method->label
  // map when absent). Added in the canonical-auth-contract work (Phase A1).
  label?: string
  // Per-method OAuth provider id (exact providers.json key). Present only for an
  // `oauth2` method whose provider was linked at generation time (the pipedrive
  // gap fix, Phase B). The picker prefers this over connector.oauth_provider so
  // a multi-auth connector routes oauth2 to Connect instead of a paste-token.
  oauth_provider?: string
}

/**
 * Split a method's `config_keys` into the credential FIELDS to render and the
 * ALIAS names that map onto the first field.
 *
 * `config_keys` is overloaded in the contract: for a single-credential method
 * (bearer / single-key api_key / custom_header) the extra keys are alternative
 * accepted NAMES for the SAME secret (e.g. shopify custom_header
 * `[access_token, token]`); for a multi-secret method they are DISTINCT required
 * inputs (e.g. aws-s3 api_key `[access_key_id, secret_access_key]`). Rendering
 * only the first key (the old behaviour) silently dropped aws-s3's second
 * secret, leaving no field for it — the connection could never be completed.
 *
 * We disambiguate from the connector's `configuration_schema`: a `config_key`
 * declared as its own schema property is a distinct field; keys absent from the
 * schema are aliases of the primary. This needs no new metadata flag and stays
 * correct as long as every distinct secret is declared in configuration_schema
 * (required secrets always are). `basic` always renders every key; `oauth2` /
 * `oauth` render none (the Connect flow owns the credential).
 */
export function splitMethodCredentialKeys(
  method: Pick<SupportedAuthMethod, "method" | "config_keys">,
  schemaKeys: Set<string>,
): { fields: string[]; aliases: string[] } {
  const keys = method.config_keys || []
  if (method.method === "oauth2" || method.method === "oauth") return { fields: [], aliases: [] }
  if (method.method === "basic") return { fields: keys, aliases: [] }
  const inSchema = (k: string) => schemaKeys.has(k.toLowerCase())
  const distinct = keys.filter(inSchema)
  // Any schema-backed config_key is an authoritative field: configuration_schema is
  // what required_config references and what the orchestrator's pre-start gate
  // (missingRequiredConfig) validates against, so a value MUST be submitted under a
  // schema-backed key. Render every schema-backed key; treat non-schema config_keys
  // as accepted aliases of the primary. This covers BOTH the multi-secret case
  // (aws-s3: access_key_id + secret_access_key, two distinct fields) AND the single
  // vendor-named secret whose key is NOT config_keys[0] — the post-#484 dedup shape
  // where the generator forces the generic canonical (access_token/api_key) to the
  // front of config_keys but keeps the vendor field (integration_token, bot_token,
  // api_token) as the sole schema property. Rendering config_keys[0] there submitted
  // a key the backend rejected ("missing required config: integration_token").
  if (distinct.length >= 1) {
    return { fields: distinct, aliases: keys.filter((k) => !inSchema(k)) }
  }
  // No schema-backed key → classic single-credential method defined only via
  // config_keys: the first key is the field, the rest are accepted aliases for it.
  return { fields: keys.slice(0, 1), aliases: keys.slice(1) }
}

// Full MCP Connector metadata (from API)
export interface MCPConnector {
  name: string
  display_name: string
  version: string
  description: string
  category: ConnectorCategory
  icon: string
  color: string
  capabilities: Record<string, unknown> | string[]
  configuration_schema: ConfigurationSchema
  supports_source: boolean
  supports_destination: boolean
  supports_cdc: boolean
  // Which DB engine versions rsync supports, keyed by sync mode.
  // Sourced from metadata.json `supported_versions`; shown read-only in the config modal.
  supported_versions?: { batch?: string; cdc?: string } & Record<string, string>
  // Confidence (server-derived; preferred)
  confidence_level?: "high" | "medium" | "low" | "unknown"
  // Runtime lifecycle stage, server-computed by api-gateway computeLifecycle from
  // execution + connection-test evidence: "draft" = never run/tested,
  // "preview"/"beta"/"ga" = has >=1 successful connection test or pipeline run.
  // This is the "has it actually worked?" signal — distinct from the static,
  // QA-derived confidence_level above. Used to suppress the misleading "New"
  // chip on connectors the user has already tested.
  lifecycle?: "draft" | "preview" | "beta" | "ga"
  // Quality + QA transparency (optional)
  status?: "active" | "draft"
  quality_tier?: "gold" | "silver" | "bronze" | "draft"
  quality_score?: number
  authoritativeness_score?: number
  api_category_hint?: string
  api_category_hint_source?: string
  api_category_hint_confidence?: number
  qa_warnings?: string[]
  qa_metadata?: Record<string, unknown>
  internal?: boolean
  logo_path?: string
  logo_local?: boolean
  logo_url?: string  // Logo URL from API: /api/v1/connectors/{name}/logo
  // Docker deployment status
  docker_deployed: boolean
  docker_status?: DockerStatus
  docker_port?: number
  docker_container?: string
  has_dockerfile: boolean
  // Authentication metadata
  auth_type?: AuthType  // Default authentication type (used when supported_auth_methods is empty)
  oauth_provider?: string  // OAuth provider name (e.g., "hubspot", "google")
  // Maps an OAuth authorize-URL query param -> the configuration_schema field that
  // supplies its value, for providers whose authorize/token URL is per-tenant
  // (e.g. Shopify's https://{shop}.myshopify.com/...). The modal forwards each
  // mapped field's value as a query param to the authorize endpoint. This replaces
  // the old hardcoded shop/subdomain special case — any connector that declares it
  // works with zero modal changes. Absent/empty for standard global-endpoint OAuth.
  oauth_authorize_params?: Record<string, string>  // { authorizeParam: configField }
  // Phase 13b: when present and length > 1, the connection form renders an
  // auth-method dropdown letting the user pick which credential flow to use.
  supported_auth_methods?: SupportedAuthMethod[]
  // Logo is served via: /api/v1/connectors/{name}/logo
}

// API response for listing connectors
export interface MCPConnectorsResponse {
  connectors: MCPConnector[]
  total: number
}

// ============================================================================
// UI HELPERS - Single source of truth for icons, colors, emojis
// ============================================================================

/**
 * Confidence levels shown in UI (derived from connector quality tier).
 * We keep the backend field name `quality_tier` for compatibility, but present it as "Confidence"
 * throughout the UI to avoid "bronze/silver/gold" terminology.
 */
export type ConnectorConfidenceLevel = "high" | "medium" | "low" | "draft" | "new" | "unknown"

// A connector with no earned confidence signal is "New" (freshly generated,
// not yet QA-validated) — distinct from "Draft" (failed preflight). Both read
// as neutral chips; "New" is non-alarming, "Draft" flags an actual failure.
const NEW_BADGE = {
  level: "new" as const,
  label: "New",
  // Subtle sky tone, distinct from the zinc "Low"/"Draft" chips.
  className: "bg-sky-50 text-sky-700 dark:bg-sky-900/20 dark:text-sky-300",
}
const DRAFT_BADGE = {
  level: "draft" as const,
  label: "Draft",
  className: "bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300",
}
// A connector with runtime evidence (>=1 successful connection test or pipeline
// run, i.e. lifecycle preview/beta/ga) is demonstrably working — it must NOT read
// "New". This positive chip replaces "New" in that case, even when the static QA
// metadata is empty (the common case for hand-curated connectors like Shopify).
const TESTED_BADGE = {
  level: "high" as const,
  label: "Tested",
  className: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300",
}

// ----------------------------------------------------------------------------
// Two ORTHOGONAL axes, rendered as two separate chips (do not conflate them):
//   1. STATUS    — has this connector actually been run? (runtime evidence)
//   2. CONFIDENCE — how much static QA signal backs it? (test results)
// A connector can be "Tested" (ran successfully) yet carry only "Medium" QA
// confidence, or carry "High" confidence while still "New" (never run). The old
// single-chip helper let CONFIDENCE mask STATUS, so a runtime-tested DB connector
// (mysql/postgresql) could never read "Tested" because its "Medium" confidence
// won. These two functions keep the axes independent.
// ----------------------------------------------------------------------------

/**
 * STATUS chip — the *lifecycle* axis. Computed by api-gateway `computeLifecycle`
 * from real runtime evidence (successful connection tests + pipeline runs):
 *   - Draft  → failed preflight (status / quality_tier "draft")
 *   - Tested → >=1 successful connection test or pipeline run (lifecycle preview/beta/ga)
 *   - New    → generated, never successfully run
 * Always returns a badge — every connector has exactly one lifecycle state.
 */
export function getConnectorStatusBadge(input?: {
  status?: string | null
  quality_tier?: string | null
  lifecycle?: string | null
}): {
  level: ConnectorConfidenceLevel
  label: string
  className: string
} {
  const status = (input?.status || "").toLowerCase().trim()
  const t = (input?.quality_tier || "").toLowerCase().trim()
  // A failed-preflight connector reads "Draft", never "New".
  if (status === "draft" || t === "draft") return DRAFT_BADGE
  const lc = (input?.lifecycle || "").toLowerCase().trim()
  const lifecycleTested = lc === "preview" || lc === "beta" || lc === "ga"
  return lifecycleTested ? TESTED_BADGE : NEW_BADGE
}

/**
 * CONFIDENCE chip — the *QA-quality* axis (server-computed `confidence_level`
 * from test counts / export validation), ORTHOGONAL to runtime testing. Returns
 * `null` when there is no earned confidence signal so the UI shows only the
 * STATUS chip in that (common) case, rather than a redundant neutral chip.
 */
export function getConnectorConfidenceBadge(input?: {
  confidence_level?: string | null
  quality_tier?: string | null
  status?: string | null
  lifecycle?: string | null
}): {
  level: ConnectorConfidenceLevel
  label: string
  className: string
} | null {
  // A draft connector has no meaningful QA confidence — the STATUS chip already
  // says "Draft"; don't add a second chip.
  const status = (input?.status || "").toLowerCase().trim()
  const t = (input?.quality_tier || "").toLowerCase().trim()
  if (status === "draft" || t === "draft") return null

  // Prefer server-derived confidence_level when present.
  const explicit = (input?.confidence_level || "").toLowerCase().trim()
  switch (explicit) {
    case "high":
      return {
        level: "high",
        label: "High",
        className: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300",
      }
    case "medium":
      return {
        level: "medium",
        label: "Medium",
        className: "bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300",
      }
    case "low":
      return {
        level: "low",
        label: "Low",
        // Keep low visually subtle (avoid “wall of red”).
        className: "bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300",
      }
    case "unknown":
      // No earned confidence — the STATUS chip carries the signal instead.
      return null
  }

  // Backward-compatible fallback: derive from quality_tier when confidence_level
  // is absent (older payloads). Returns null when there's no usable tier.
  switch (t) {
    case "gold":
      return {
        level: "high",
        label: "High",
        className: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300",
      }
    case "silver":
      return {
        level: "medium",
        label: "Medium",
        className: "bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300",
      }
    case "bronze":
      return {
        level: "low",
        label: "Low",
        className: "bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300",
      }
    default:
      return null
  }
}

/**
 * Human-readable reasons explaining a connector's confidence chip — surfaced as
 * a tooltip. Built entirely from fields the API already returns; no backend
 * change required. Empty array means "no extra detail to show".
 */
export function getConnectorConfidenceReasons(connector?: {
  confidence_level?: string | null
  quality_tier?: string | null
  quality_score?: number | null
  status?: string | null
  lifecycle?: string | null
  qa_warnings?: string[] | null
  qa_metadata?: Record<string, unknown> | null
}): string[] {
  if (!connector) return []
  const reasons: string[] = []
  const statusBadge = getConnectorStatusBadge(connector)
  const confidenceBadge = getConnectorConfidenceBadge(connector)
  const qa = connector.qa_metadata || {}
  const passed = qa["tests_passed"]
  const failed = qa["tests_failed"]
  const exportStatus = qa["export_validation_status"]

  // Status axis (runtime evidence): Tested / New / Draft.
  if (statusBadge.label === "Tested") {
    reasons.push("Status: Tested — verified at runtime by at least one successful connection test or pipeline run.")
  } else if (statusBadge.label === "Draft") {
    reasons.push("Status: Draft — this connector did not pass preflight checks.")
  } else {
    reasons.push("Status: New — generated but not yet run; no successful connection test or pipeline run recorded.")
  }

  // Confidence axis (static QA quality): independent of whether it has been run.
  if (confidenceBadge) {
    reasons.push(`QA confidence: ${confidenceBadge.label} — static quality signal from test results, independent of runtime testing.`)
  } else {
    reasons.push("QA confidence: unrated — integration tests / export validation have not earned a level yet.")
  }

  if (connector.quality_tier) reasons.push(`Quality tier: ${connector.quality_tier}.`)
  if (typeof connector.quality_score === "number") reasons.push(`Quality score: ${connector.quality_score}/100.`)
  if (typeof passed === "number" || typeof failed === "number") {
    reasons.push(`Tests passed: ${passed ?? 0} · failed: ${failed ?? 0}.`)
  }
  if (typeof exportStatus === "string" && exportStatus) {
    reasons.push(`Export validation: ${exportStatus}.`)
  }
  if (Array.isArray(connector.qa_warnings)) reasons.push(...connector.qa_warnings)

  return reasons
}

/**
 * Get Lucide icon component name for a connector icon string
 */
export function getConnectorIconName(icon: string): string {
  const iconMap: Record<string, string> = {
    database: "Database",
    cloud: "Cloud",
    api: "Zap",
    analytics: "BarChart3",
    messaging: "MessageSquare",
    storage: "HardDrive",
    streaming: "Activity",
    file: "FileText",
    plug: "Plug",
  }
  return iconMap[icon] || "Puzzle"
}

/**
 * Get Tailwind gradient classes for a category
 */
export function getCategoryColor(category: string): string {
  const colorMap: Record<string, string> = {
    database: "from-blue-500 to-cyan-500",
    storage: "from-orange-500 to-amber-500",
    api: "from-purple-500 to-pink-500",
    analytics: "from-green-500 to-emerald-500",
    messaging: "from-indigo-500 to-blue-500",
    streaming: "from-red-500 to-orange-500",
    file: "from-slate-500 to-gray-500",
    other: "from-gray-500 to-slate-500",
  }
  return colorMap[category] || colorMap.other
}

/**
 * Emoji fallbacks for connectors (used when logo fails to load)
 * Complete mapping for all supported connectors
 */
export const connectorEmoji: Record<string, string> = {
  // Databases
  mysql: "🐬",
  postgresql: "🐘",
  postgres: "🐘",
  mongodb: "🍃",
  sqlite: "📁",
  redis: "🔴",
  cassandra: "👁️",
  oracle: "🔶",
  sqlserver: "🪟",
  mssql: "🪟",
  db2: "💙",
  mariadb: "🦭",
  
  // Storage
  "aws-s3": "📦",
  s3: "📦",
  minio: "🗄️",
  gcs: "☁️",
  
  // Streaming
  kafka: "📨",
  "kafka-mcp-sink": "🔄",
  debezium: "⚡",
  rabbitmq: "🐰",
  
  // Analytics
  snowflake: "❄️",
  bigquery: "📊",
  redshift: "🔺",
  databricks: "🧱",
  
  // API / SaaS
  stripe: "💳",
  hubspot: "🧲",
  salesforce: "☁️",
  zoho: "📊",
  "zoho-crm": "📊",
  freshdesk: "🎫",
  zendesk: "💬",
  slack: "💬",
  github: "🐙",
  
  // File
  json: "📄",
  csv: "📋",
  parquet: "📐",
  
  // Search
  elasticsearch: "🔍",
  
  // Default
  api: "🔌",
  default: "🔧",
}

/**
 * Get emoji for a connector by name
 */
export function getConnectorEmoji(name: string): string {
  const normalized = name.toLowerCase().replace(/[_\s]/g, "-")
  if (connectorEmoji[normalized]) return connectorEmoji[normalized]

  // Generic suffix fallbacks: if the canonical id includes common suffixes but the
  // emoji map only includes the base product slug, try the base.
  if (normalized.endsWith("-api")) {
    const base = normalized.slice(0, -4)
    if (connectorEmoji[base]) return connectorEmoji[base]
  }
  if (normalized.endsWith("-connector")) {
    const base = normalized.slice(0, -10)
    if (connectorEmoji[base]) return connectorEmoji[base]
  }

  return connectorEmoji.default
}

/**
 * Get display name for a connector
 * Converts "aws-s3" to "AWS S3", "postgresql" to "PostgreSQL", etc.
 */
export function getConnectorDisplayName(name: string): string {
  const displayNames: Record<string, string> = {
    mysql: "MySQL",
    postgresql: "PostgreSQL",
    postgres: "PostgreSQL",
    mongodb: "MongoDB",
    sqlite: "SQLite",
    redis: "Redis",
    cassandra: "Apache Cassandra",
    oracle: "Oracle",
    sqlserver: "SQL Server",
    mssql: "SQL Server",
    db2: "IBM DB2",
    mariadb: "MariaDB",
    "aws-s3": "AWS S3",
    s3: "AWS S3",
    minio: "MinIO",
    gcs: "Google Cloud Storage",
    kafka: "Apache Kafka",
    "kafka-mcp-sink": "Kafka MCP Sink",
    debezium: "Debezium",
    rabbitmq: "RabbitMQ",
    snowflake: "Snowflake",
    bigquery: "BigQuery",
    redshift: "Amazon Redshift",
    databricks: "Databricks",
    stripe: "Stripe",
    hubspot: "HubSpot",
    salesforce: "Salesforce",
    freshdesk: "Freshdesk",
    zendesk: "Zendesk",
    slack: "Slack",
    github: "GitHub",
    json: "JSON",
    csv: "CSV",
    parquet: "Apache Parquet",
    elasticsearch: "Elasticsearch",
  }
  return displayNames[name.toLowerCase()] || name.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
}
