/**
 * Destination-namespace helpers (PR-C).
 *
 * Pure functions shared by the destination-mapping UI (now folded into the
 * first-run table-selection HITL). They mirror the api-gateway/shared Go logic
 * for instant client-side feedback; the server re-validates authoritatively in
 * the HITL_TABLES handler + pre-migration assessment.
 *
 * The per-connector answers (kind, default) come from each connector's
 * namespace_model (./namespaceModel). A function called without models reads
 * the page's shared copy; components pass the one useNamespaceModels() returns
 * so they re-render when it arrives.
 */

import { namespaceModelFor, namespaceModelsSnapshot, type NamespaceModels } from "./namespaceModel"

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

// LAYOUT_V2_DESTINATIONS are the object stores written as
// <prefix>/<database>/[<schema>/]<table>/, with no pipeline id in the path. minio
// stays on the old layout. Pinned by v2_destinations in shared/object_layout_golden.json,
// which the sink and the orchestrator also read.
const LAYOUT_V2_DESTINATIONS = new Set(["gcs", "aws-s3", "azure-blob"])

// isLayoutV2Destination mirrors the orchestrator's objectLayoutV2DestSupported
// (backend-orchestrator executor/object_layout_v2.go).
export function isLayoutV2Destination(connectorType?: string): boolean {
  return LAYOUT_V2_DESTINATIONS.has((connectorType || "").trim().toLowerCase().replace(/_/g, "-"))
}

// validatePipelinePrefix checks the path prefix of a layout v2 destination. It is
// required (without one the pipeline falls back to the old layout, which puts the
// pipeline id in every path) and must pass both the server's namespace check
// (shared/go/naming.ValidateNamespace: letters, digits, underscore, no leading
// digit) and the layout v2 prefix rule (^[a-z0-9][a-z0-9_-]{0,62}$). The overlap
// is lowercase letters, digits and underscores, starting with a letter.
export function validatePipelinePrefix(name: string): string {
  const n = name.trim()
  if (n === "") return "Enter a path prefix. Every file of this pipeline is written under it."
  if (n.length > 63) return "Path prefix is too long (max 63 characters)."
  if (!/^[a-z][a-z0-9_]*$/.test(n)) {
    return "Use lowercase letters, digits and underscores, starting with a letter."
  }
  // "default" is read as "no namespace" by the executor (isRealNamespace).
  if (SUSPICIOUS.has(n) || n === "default") return `"${n}" is a reserved word and can't be a path prefix.`
  return ""
}

// OBJECT_STORAGE_DESTINATIONS are every object store, layout v2 or not (minio).
const OBJECT_STORAGE_DESTINATIONS = new Set(["gcs", "aws-s3", "azure-blob", "minio"])

export function isObjectStorageDestination(connectorType?: string): boolean {
  return OBJECT_STORAGE_DESTINATIONS.has((connectorType || "").trim().toLowerCase().replace(/_/g, "-"))
}

// PG_LAYOUT_TYPES are the normalised PostgreSQL-family source types
// (shared/postgres_family_golden.json members + the "postgres" alias).
const PG_LAYOUT_TYPES = new Set([
  "postgresql", "postgres", "cockroachdb", "cockroach_db", "aurora_postgresql", "alloydb", "neon", "supabase",
])

// objectLayoutSourceFamily mirrors the orchestrator's objectLayoutV2SourceFamily
// (executor/object_layout_v2.go): the family that decides a table folder's depth.
// "" = not a database family, which keeps the pipeline on the old layout.
export function objectLayoutSourceFamily(sourceType?: string): string {
  const t = (sourceType || "").trim().toLowerCase().replace(/-/g, "_")
  if (PG_LAYOUT_TYPES.has(t)) return "postgresql"
  if (["mongodb", "mongo", "mongodb_atlas", "mongodbatlas", "atlas"].includes(t)) return "mongodb"
  if (["mysql", "mariadb", "aurora_mysql"].includes(t)) return "mysql"
  if (["sqlserver", "mssql", "sql_server"].includes(t)) return "sqlserver"
  if (t === "oracle") return "oracle"
  return ""
}

// encodeLayoutSegment mirrors storage.layoutV2EncodeName: %, /, \, =, control
// characters and a leading _ or . are percent-encoded; case and non-ASCII stay.
function encodeLayoutSegment(s: string): string {
  let out = ""
  Array.from(s).forEach((ch, i) => {
    const c = ch.codePointAt(0) ?? 0
    const encode =
      ch === "%" || ch === "/" || ch === "\\" || ch === "=" || c < 0x20 || c === 0x7f ||
      (i === 0 && (ch === "_" || ch === "."))
    out += encode ? `%${c.toString(16).toUpperCase().padStart(2, "0")}` : ch
  })
  return out
}

// objectLayoutTableParts mirrors the orchestrator's objectLayoutV2TableFor: it
// splits a selected table name into the database / schema / table of its folder.
// PostgreSQL / SQL Server take [<db>.]<schema>.<table> or a bare table in the
// default schema (public / dbo; Oracle's default is the login user, unknown here,
// so a bare Oracle table gets no schema and the folder is not shown). MongoDB
// strips only the source database's own prefix; MySQL reads <db>.<table>.
export function objectLayoutTableParts(
  family: string,
  database: string | undefined,
  tableName: string
): { database?: string; schema?: string; table: string } {
  const name = (tableName || "").trim()
  const db = database || undefined
  switch (family) {
    case "postgresql":
    case "sqlserver":
    case "oracle": {
      const parts = name.split(".")
      const table = parts[parts.length - 1]
      const schema = parts.length >= 2 ? parts[parts.length - 2] : ""
      const fallback = family === "postgresql" ? "public" : family === "sqlserver" ? "dbo" : ""
      return { database: db, schema: schema.trim() ? schema : fallback, table }
    }
    case "mongodb":
      return { database: db, table: db && name.startsWith(`${db}.`) ? name.slice(db.length + 1) : name }
    case "mysql": {
      const i = name.indexOf(".")
      return i > 0 && i + 1 < name.length ? { database: name.slice(0, i), table: name.slice(i + 1) } : { database: db, table: name }
    }
  }
  return { table: name }
}

const LAYOUT_SPACE = /^[ \t\n\r\v\f]+|[ \t\n\r\v\f]+$/g

// objectStorageTableFolder is a layout v2 table's folder,
// "<conn prefix>/<pipeline prefix>/<db>/[<schema>/]<table>/", as
// storage.LayoutV2TablePrefix builds it (pinned by v2.table_prefix in
// shared/object_layout_golden.json). sourceFamily is a layout family (see
// objectLayoutSourceFamily). Returns null where the Go side returns an error, so a
// caller falls back instead of showing a path nothing writes to. With a bucket it
// is prepended, for display only.
export function objectStorageTableFolder(args: {
  bucket?: string
  connPrefix?: string
  pipelinePrefix: string
  sourceFamily?: string
  database?: string
  schema?: string
  table: string
}): string | null {
  const pipelinePrefix = args.pipelinePrefix || ""
  if (!/^[a-z0-9][a-z0-9_-]{0,62}$/.test(pipelinePrefix)) return null
  const db = (args.database || "").replace(LAYOUT_SPACE, "")
  const schema = (args.schema || "").replace(LAYOUT_SPACE, "")
  const table = (args.table || "").replace(LAYOUT_SPACE, "")
  const parts: string[] = []
  switch ((args.sourceFamily || "").replace(LAYOUT_SPACE, "").toLowerCase()) {
    case "postgresql":
    case "sqlserver":
    case "oracle":
      if (!db || !schema) return null
      parts.push(db, schema)
      break
    case "mongodb":
    case "mysql":
      if (!db) return null
      parts.push(db)
      break
    case "":
      break
    default:
      return null
  }
  if (!table) return null
  parts.push(table)
  const conn = (args.connPrefix || "").replace(LAYOUT_SPACE, "").replace(/^\/+|\/+$/g, "")
  const root = conn ? `${conn}/${pipelinePrefix}/` : `${pipelinePrefix}/`
  const folder = root + parts.map((p) => `${encodeLayoutSegment(p)}/`).join("")
  const bucket = (args.bucket || "").trim().replace(/^\/+|\/+$/g, "")
  return bucket ? `${bucket}/${folder}` : folder
}

// genericSourceDefaults mirrors the Go set in api-gateway/handlers/pipelines.go.
// These are source-engine internal placeholder names that must be translated to
// the destination's own default before being shown to users.
const GENERIC_SOURCE_DEFAULTS = new Set(["default", "public", "main"])

// destDefaultSchemaName is the destination engine's own default namespace
// ("public" on PostgreSQL), from its namespace_model.destination_default — the
// same answer as destDefaultSchemaName in api-gateway handlers/pipelines.go.
// Returns "" for destinations with no universal default (BigQuery, object stores)
// — the HITL input should start empty so the user is prompted to supply a name.
export function destDefaultSchemaName(
  destConnType?: string,
  models: NamespaceModels | undefined = namespaceModelsSnapshot()
): string {
  return namespaceModelFor(models, destConnType).destination_default
}

// defaultNamespaceForTypes returns the recommended pre-fill value for the
// destination namespace field given source and destination connector types.
// Mirrors seedDestinationNamespace in api-gateway/handlers/pipelines.go.
//
// Source-specific labels ("shopify", "stripe", etc.) pass through unchanged.
// Generic engine-default names ("default", "public", "main") are translated
// to what the destination engine calls its own default schema.
export function defaultNamespaceForTypes(
  srcType?: string,
  destType?: string,
  models: NamespaceModels | undefined = namespaceModelsSnapshot()
): string {
  // Object storage (#13): the namespace is the folder of every object key, and
  // left empty the writers use the source database. The backend seeds "" here on
  // purpose; callers fall back to this function when the stored value is "", so a
  // source slug returned here ("mongodb") was saved and became the folder.
  if (namespaceKindForType(destType, models) === "path") return ""
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
    const d = destDefaultSchemaName(destType, models)
    // d === "" means dest has no universal default (BigQuery etc.); return ""
    // so the input starts empty and the user must supply a name.
    return d
  }
  return candidate
}

// namespaceKindForType is the kind of name a destination takes, from its
// namespace_model.destination_namespace — the same answer as the api-gateway
// namespaceKindForConnector — so legacy pipelines (persisted before
// destination_config carried a kind) still get a correctly-labelled field.
export function namespaceKindForType(
  connectorType?: string,
  models: NamespaceModels | undefined = namespaceModelsSnapshot()
): string {
  return namespaceModelFor(models, connectorType).destination_namespace
}
