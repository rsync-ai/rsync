/**
 * Document browse mode (MongoDB) for the Data Explorer — request building, paging and
 * cell formatting for POST /api/v1/explorer/documents/find.
 *
 * The gateway and the connector both validate the filter against an operator
 * allowlist; this module only checks that each field is well-formed JSON of the right
 * shape so a typo is caught before a round trip. See
 * docs/explorer/document-browse-mode-plan.md.
 */

export const DOCUMENT_FIND_DEFAULT_LIMIT = 50
export const DOCUMENT_FIND_MAX_LIMIT = 500

export type FindField = "collection" | "filter" | "projection" | "sort" | "limit"

export interface FindInputs {
  connectionId: string
  collection: string
  /** The collection's database; required by a server-level connection, which names none. */
  database?: string
  filter?: string
  projection?: string
  sort?: string
  limit?: number
  cursor?: string
  skip?: number
}

export type BuiltFind = { ok: true; body: string } | { ok: false; field: FindField; error: string }

export interface DocumentFindResult {
  collection: string
  documents: Record<string, unknown>[]
  columns: string[]
  returned: number
  has_more: boolean
  paging_mode: "keyset" | "skip"
  next_cursor: string | null
  next_skip: number | null
  execution_time_ms: number | null
  truncated_bytes: boolean
  warnings: string[]
}

type JsonField = { ok: true; raw: string | null } | { ok: false; field: FindField; error: string }

function jsonField(
  text: string | undefined,
  field: FindField,
  label: string,
  accept: "object" | "object-or-array",
): JsonField {
  const trimmed = (text ?? "").trim()
  if (!trimmed) return { ok: true, raw: null }
  let value: unknown
  try {
    value = JSON.parse(trimmed)
  } catch (e) {
    return { ok: false, field, error: `${label} is not valid JSON${e instanceof Error ? `: ${e.message}` : ""}` }
  }
  if (value === null) return { ok: true, raw: null }
  const isObject = typeof value === "object" && !Array.isArray(value)
  if (isObject || (accept === "object-or-array" && Array.isArray(value))) return { ok: true, raw: trimmed }
  return {
    ok: false,
    field,
    error:
      accept === "object"
        ? `${label} must be a JSON object`
        : `${label} must be a JSON object or a list of [field, direction] pairs`,
  }
}

export function clampLimit(n: number | undefined): number {
  if (typeof n !== "number" || !Number.isFinite(n) || n < 1) return DOCUMENT_FIND_DEFAULT_LIMIT
  return Math.min(DOCUMENT_FIND_MAX_LIMIT, Math.floor(n))
}

/**
 * Builds the request body. It is assembled by hand rather than with JSON.stringify so
 * the filter, projection and sort reach the gateway exactly as typed — a parse and
 * re-stringify would round a 64-bit integer such as 9007199254740993 through a JS
 * number.
 */
export function buildFindBody(input: FindInputs): BuiltFind {
  const collection = (input.collection ?? "").trim()
  if (!collection) return { ok: false, field: "collection", error: "Pick a collection" }
  const filter = jsonField(input.filter, "filter", "Filter", "object")
  if (!filter.ok) return filter
  const projection = jsonField(input.projection, "projection", "Projection", "object")
  if (!projection.ok) return projection
  const sort = jsonField(input.sort, "sort", "Sort", "object-or-array")
  if (!sort.ok) return sort

  const parts = [
    `"connection_id":${JSON.stringify(input.connectionId)}`,
    `"collection":${JSON.stringify(collection)}`,
  ]
  const database = (input.database ?? "").trim()
  if (database) parts.push(`"database":${JSON.stringify(database)}`)
  if (filter.raw) parts.push(`"filter":${filter.raw}`)
  if (projection.raw) parts.push(`"projection":${projection.raw}`)
  if (sort.raw) parts.push(`"sort":${sort.raw}`)
  parts.push(`"limit":${clampLimit(input.limit)}`)
  if (input.cursor) parts.push(`"cursor":${JSON.stringify(input.cursor)}`)
  if (typeof input.skip === "number" && input.skip > 0) parts.push(`"skip":${Math.floor(input.skip)}`)
  return { ok: true, body: `{${parts.join(",")}}` }
}

/** The paging parameters for the next page, or null when there is none to fetch. */
export function nextPageParams(r: DocumentFindResult): { cursor: string } | { skip: number } | null {
  if (!r.has_more) return null
  if (r.paging_mode === "keyset") return r.next_cursor ? { cursor: r.next_cursor } : null
  return typeof r.next_skip === "number" ? { skip: r.next_skip } : null
}

/** Coerces the gateway's JSON into a DocumentFindResult with every field present. */
export function normalizeResult(data: unknown): DocumentFindResult {
  const d = (data && typeof data === "object" ? data : {}) as Record<string, unknown>
  const documents = Array.isArray(d.documents)
    ? (d.documents.filter((x) => x && typeof x === "object" && !Array.isArray(x)) as Record<string, unknown>[])
    : []
  const columns = Array.isArray(d.columns) ? d.columns.filter((c): c is string => typeof c === "string") : []
  return {
    collection: typeof d.collection === "string" ? d.collection : "",
    documents,
    columns: mergeColumns(columns, documents.flatMap((doc) => Object.keys(doc))),
    returned: typeof d.returned === "number" ? d.returned : documents.length,
    has_more: d.has_more === true,
    paging_mode: d.paging_mode === "skip" ? "skip" : "keyset",
    next_cursor: typeof d.next_cursor === "string" && d.next_cursor ? d.next_cursor : null,
    next_skip: typeof d.next_skip === "number" ? d.next_skip : null,
    execution_time_ms: typeof d.execution_time_ms === "number" ? d.execution_time_ms : null,
    truncated_bytes: d.truncated_bytes === true,
    warnings: Array.isArray(d.warnings) ? d.warnings.filter((w): w is string => typeof w === "string") : [],
  }
}

/** Union of two column lists in first-seen order, with _id always first. */
export function mergeColumns(a: string[], b: string[]): string[] {
  const out: string[] = []
  for (const c of [...a, ...b]) if (!out.includes(c)) out.push(c)
  const id = out.indexOf("_id")
  if (id > 0) {
    out.splice(id, 1)
    out.unshift("_id")
  }
  return out
}

export type CellKind =
  | "missing"
  | "null"
  | "string"
  | "number"
  | "boolean"
  | "objectId"
  | "date"
  | "object"
  | "array"
  | "special"

const CELL_MAX_CHARS = 160

// Relaxed Extended JSON type wrappers the connector can emit (bson.json_util).
const EJSON_WRAPPERS = new Set([
  "$oid",
  "$date",
  "$numberDecimal",
  "$numberLong",
  "$numberInt",
  "$numberDouble",
  "$binary",
  "$uuid",
  "$timestamp",
  "$regularExpression",
  "$minKey",
  "$maxKey",
  "$symbol",
  "$code",
])

function wrapperOf(v: unknown): [string, unknown] | null {
  if (!v || typeof v !== "object" || Array.isArray(v)) return null
  const keys = Object.keys(v)
  if (keys.length !== 1 || !EJSON_WRAPPERS.has(keys[0])) return null
  return [keys[0], (v as Record<string, unknown>)[keys[0]]]
}

function truncate(s: string): string {
  return s.length > CELL_MAX_CHARS ? `${s.slice(0, CELL_MAX_CHARS - 1)}…` : s
}

export function cellKind(v: unknown): CellKind {
  if (v === undefined) return "missing"
  if (v === null) return "null"
  if (typeof v === "string") return "string"
  if (typeof v === "number") return "number"
  if (typeof v === "boolean") return "boolean"
  if (Array.isArray(v)) return "array"
  const w = wrapperOf(v)
  if (!w) return typeof v === "object" ? "object" : "special"
  if (w[0] === "$oid") return "objectId"
  if (w[0] === "$date") return "date"
  if (w[0].startsWith("$number")) return "number"
  return "special"
}

/** Text for one EJSON wrapper, or undefined when it has no special rendering. */
function formatWrapper(key: string, inner: unknown): string | undefined {
  const obj = (inner && typeof inner === "object" ? inner : {}) as Record<string, unknown>
  switch (key) {
    case "$oid":
      return `ObjectId("${String(inner)}")`
    case "$date":
      if (typeof inner === "string") return inner
      if (typeof obj.$numberLong === "string") return `Date(${obj.$numberLong})`
      return undefined
    case "$numberDecimal":
    case "$numberLong":
    case "$numberInt":
    case "$numberDouble":
      return String(inner)
    case "$uuid":
      return `UUID("${String(inner)}")`
    case "$binary":
      return `Binary(subtype ${String(obj.subType ?? "?")})`
    case "$timestamp":
      return `Timestamp(${String(obj.t)}, ${String(obj.i)})`
    case "$regularExpression":
      return `/${String(obj.pattern ?? "")}/${String(obj.options ?? "")}`
    case "$minKey":
      return "MinKey"
    case "$maxKey":
      return "MaxKey"
  }
  return undefined
}

// JSON-like text for objects/arrays in which nested wrappers are rendered like
// top-level cells (35.10, ObjectId("…")) instead of {"$numberDecimal":"35.10"}.
function inlineValue(v: unknown): string {
  if (v === null || v === undefined) return "null"
  if (typeof v === "string") return JSON.stringify(v)
  if (typeof v !== "object") return String(v)
  if (Array.isArray(v)) return `[${v.map(inlineValue).join(",")}]`
  const w = wrapperOf(v)
  const text = w ? formatWrapper(w[0], w[1]) : undefined
  if (w && text !== undefined) {
    // An ISO date is a string on the wire; keep it quoted like other strings.
    return w[0] === "$date" && typeof w[1] === "string" ? JSON.stringify(text) : text
  }
  const fields = Object.entries(v as Record<string, unknown>)
    .filter(([, x]) => x !== undefined)
    .map(([k, x]) => `${JSON.stringify(k)}:${inlineValue(x)}`)
  return `{${fields.join(",")}}`
}

/** A one-line rendering of a field value for a grid cell. */
export function formatCell(v: unknown): string {
  if (v === undefined) return ""
  if (v === null) return "null"
  if (typeof v === "string") return truncate(v)
  if (typeof v === "number" || typeof v === "boolean") return String(v)
  const w = wrapperOf(v)
  const text = w ? formatWrapper(w[0], w[1]) : undefined
  return truncate(text ?? inlineValue(v))
}
