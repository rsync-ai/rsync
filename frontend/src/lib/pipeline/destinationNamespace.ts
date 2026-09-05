/**
 * Destination-namespace helpers (PR-C).
 *
 * Pure functions shared by the destination-mapping UI (now folded into the
 * first-run table-selection HITL). They mirror the api-gateway/shared Go logic
 * for instant client-side feedback; the server re-validates authoritatively in
 * the HITL_TABLES handler + pre-migration assessment.
 */

// kindMeta renders the human label + helper text for a namespace kind so a
// single field reads correctly per destination engine.
export function kindMeta(kind: string): { noun: string; help: string; createable: boolean } {
  switch (kind) {
    case "database":
      return { noun: "Database", help: "Rows land in this database on the destination.", createable: true }
    case "dataset":
      return { noun: "Dataset", help: "Rows land in this dataset on the destination.", createable: true }
    case "prefix":
      return { noun: "Table prefix", help: "Tables are named <prefix>__<table> on the destination.", createable: false }
    case "path":
      return { noun: "Path prefix", help: "Objects are written under this path on the destination.", createable: false }
    case "schema":
    default:
      return { noun: "Schema", help: "Rows land in this schema on the destination.", createable: true }
  }
}

// Mirrors shared/go/naming.ValidateNamespace for instant client-side feedback.
// The server re-validates authoritatively; this just prevents an obviously-bad
// submit and explains why inline.
export const SUSPICIOUS = new Set([
  "the", "a", "an", "to", "into", "from", "as", "table", "tables", "source",
  "destination", "dest", "sink", "target", "connection", "it", "this", "that",
  "these", "those", "my", "your", "our", "their",
])

export function validateNamespace(name: string, opts?: { required?: boolean }): string {
  const n = name.trim()
  if (n === "") {
    // A blank namespace is only an error when a name is genuinely required.
    // Path/prefix destinations (object storage, sqlite) and multi-schema
    // selections that mirror each source schema at the destination pass
    // { required: false } — there an empty value is valid and means "no single
    // target namespace" (the backend preserves the source schemas).
    return opts?.required === false ? "" : "Enter a name."
  }
  if (n.length > 63) return "Name is too long (max 63 characters)."
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(n)) {
    return /^[0-9]/.test(n)
      ? "Name must not start with a digit."
      : "Use only letters, digits, and underscores."
  }
  if (SUSPICIOUS.has(n.toLowerCase())) return `"${n}" is a reserved word and can't be a namespace name.`
  return ""
}

// genericSourceDefaults mirrors the Go set in api-gateway/handlers/pipelines.go.
// These are source-engine internal placeholder names that must be translated to
// the destination's own default before being shown to users.
const GENERIC_SOURCE_DEFAULTS = new Set(["default", "public", "main"])

// destDefaultSchemaName mirrors destDefaultSchemaName in pipelines.go.
// SINGLE EXTENSION POINT: add one case per new destination type.
// Returns "" for destinations with no universal default (BigQuery, object stores)
// — the HITL input should start empty so the user is prompted to supply a name.
export function destDefaultSchemaName(destConnType?: string): string {
  switch ((destConnType || "").toLowerCase().trim()) {
    case "postgresql":
    case "postgres":
    case "pg":
    case "redshift":
    case "aurora_postgresql":
    case "aurora-postgresql":
      return "public"
    case "snowflake":
      return "public" // Snowflake's built-in default schema is PUBLIC
    case "mysql":
    case "mariadb":
    case "aurora_mysql":
    case "aurora-mysql":
      return "default"
    case "clickhouse":
    case "clickhouse-cloud":
      return "default"
    case "databricks":
    case "delta":
    case "delta-lake":
      return "default"
    case "bigquery":
      return "" // no universal default dataset; force user to supply one
    case "s3":
    case "aws-s3":
    case "minio":
    case "gcs":
    case "azure-blob":
    case "object-storage":
      return "" // path-style; no schema concept
  }
  return "" // unknown destination: do not translate
}

// defaultNamespaceForTypes returns the recommended pre-fill value for the
// destination namespace field given source and destination connector types.
// Mirrors seedDestinationNamespace in api-gateway/handlers/pipelines.go.
//
// Source-specific labels ("shopify", "stripe", etc.) pass through unchanged.
// Generic engine-default names ("default", "public", "main") are translated
// to what the destination engine calls its own default schema.
export function defaultNamespaceForTypes(srcType?: string, destType?: string): string {
  const src = (srcType || "").toLowerCase().trim()
  // Derive the source-side candidate (same logic as sourceSchemaCanonicalName)
  let candidate = ""
  switch (src) {
    case "shopify-admin-graphql": case "shopify": case "shopify-admin":
      candidate = "shopify"; break
    case "postgresql": case "postgres": case "pg":
      candidate = "public"; break
    case "mysql": case "mariadb":
      candidate = "default"; break
    case "clickhouse":
      candidate = "default"; break
    case "sqlite":
      candidate = "main"; break
    default:
      // slug-ify: letters/digits/underscore only
      candidate = src.replace(/[^a-z0-9_]/g, "_").replace(/^_+|_+$/g, "") || "default"
  }
  // Translate generic source-default names to the destination's equivalent
  if (GENERIC_SOURCE_DEFAULTS.has(candidate)) {
    const d = destDefaultSchemaName(destType)
    // d === "" means dest has no universal default (BigQuery etc.); return ""
    // so the input starts empty and the user must supply a name.
    return d
  }
  return candidate
}

// namespaceKindForType mirrors the api-gateway namespaceKindForConnector mapping
// so legacy pipelines (persisted before destination_config carried a kind) still
// get a correctly-labelled field.
export function namespaceKindForType(connectorType?: string): string {
  switch ((connectorType || "").toLowerCase().trim()) {
    case "postgresql":
    case "postgres":
    case "pg":
    case "redshift":
    case "snowflake":
      return "schema"
    case "mysql":
    case "mariadb":
    case "clickhouse":
      return "database"
    case "bigquery":
      return "dataset"
    case "sqlite":
      return "prefix"
    case "s3":
    case "aws-s3":
    case "minio":
    case "gcs":
    case "azure-blob":
    case "object-storage":
      return "path"
    default:
      return "schema"
  }
}
