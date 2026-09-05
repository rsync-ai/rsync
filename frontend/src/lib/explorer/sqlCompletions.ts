// Pure, framework-agnostic SQL autocomplete logic for the Data Explorer's
// CodeMirror editor. Kept out of the React component so the matching rules
// (alias resolution, schema lookup, FK join hints) can be unit-tested
// without spinning up an editor.

export interface CompletionColumn {
  name: string
  type?: string
  is_primary_key?: boolean
}

export interface CompletionTable {
  name: string
  schema?: string
  columns?: CompletionColumn[]
}

export interface ForeignKeyLike {
  from_schema?: string
  from_table: string
  from_column: string
  to_schema?: string
  to_table: string
  to_column: string
}

export interface CompletionOption {
  label: string
  type: string
  detail?: string
  boost?: number
}

/** A table resolved into the lookup: qualified label + bare name + schema. */
interface LookupTable {
  label: string
  bare: string
  schema?: string
  columns: CompletionColumn[]
}

export interface SchemaLookup {
  /** Tables in input order, with their qualified label, bare name, schema. */
  tables: LookupTable[]
  /** Lowercased qualified label (`schema.table`) → columns. */
  byLabel: Map<string, CompletionColumn[]>
  /**
   * Lowercased bare table name → columns. A bare name is only resolvable when
   * it is UNIQUE across schemas; ambiguous bare names are removed from this
   * map (see `bareAmbiguous`) so we never silently autocomplete the wrong
   * table's columns.
   */
  byBareName: Map<string, CompletionColumn[]>
  /** Bare names that map to >1 table — must be schema-qualified to resolve. */
  bareAmbiguous: Set<string>
  /** Lowercased schema name → its tables (for `schema.` completion). */
  bySchema: Map<string, LookupTable[]>
}

const qualify = (t: CompletionTable): string => (t.schema ? `${t.schema}.${t.name}` : t.name)

/** Strip surrounding double-quotes or backticks from a SQL identifier. */
function stripQuotes(ident: string): string {
  const s = ident.trim()
  if (
    s.length >= 2 &&
    ((s[0] === '"' && s[s.length - 1] === '"') || (s[0] === "`" && s[s.length - 1] === "`"))
  ) {
    return s.slice(1, -1)
  }
  return s
}

/**
 * Normalize a qualifier (alias / table / `schema.table`) for lookup: strip
 * quotes on each dotted segment and lowercase. `"Sales"."Orders"` → `sales.orders`.
 */
function normalizeQualifier(qualifier: string): string {
  return qualifier
    .split(".")
    .map((seg) => stripQuotes(seg).toLowerCase())
    .join(".")
}

/** Words that can never be a table alias (SQL keywords that may follow a table). */
const NOT_ALIASES = new Set([
  "on", "where", "join", "inner", "left", "right", "full", "outer", "cross",
  "group", "order", "limit", "having", "union", "using", "as", "and", "or",
  "select", "from", "natural", "set", "values", "returning", "offset",
])

/** Build a case-insensitive lookup of tables + columns for completion. */
export function buildSchemaLookup(tables: CompletionTable[]): SchemaLookup {
  const list: LookupTable[] = []
  const byLabel = new Map<string, CompletionColumn[]>()
  const byBareName = new Map<string, CompletionColumn[]>()
  const bareAmbiguous = new Set<string>()
  const bySchema = new Map<string, LookupTable[]>()

  for (const t of tables) {
    const label = qualify(t)
    const columns = t.columns ?? []
    const entry: LookupTable = { label, bare: t.name, schema: t.schema, columns }
    list.push(entry)
    byLabel.set(label.toLowerCase(), columns)

    // Bare-name index with collision detection: the FIRST table claims the
    // bare name; a SECOND table with the same bare name makes it ambiguous,
    // so we drop it from the map and force schema-qualification (I3).
    const bare = t.name.toLowerCase()
    if (bareAmbiguous.has(bare)) {
      // already known-ambiguous — leave it out of byBareName
    } else if (byBareName.has(bare)) {
      byBareName.delete(bare)
      bareAmbiguous.add(bare)
    } else {
      byBareName.set(bare, columns)
    }

    if (t.schema) {
      const key = t.schema.toLowerCase()
      const arr = bySchema.get(key)
      if (arr) arr.push(entry)
      else bySchema.set(key, [entry])
    }
  }
  return { tables: list, byLabel, byBareName, bareAmbiguous, bySchema }
}

/**
 * Parse `FROM`/`JOIN`/comma table references and their aliases out of a SQL
 * string, e.g. `FROM sales.orders o JOIN users AS u` → {o: "sales.orders",
 * u: "users"}. Quoted identifiers (`"Users"`, `` `Orders` ``) are supported and
 * normalized (quotes stripped, lowercased). Returns lowercased alias →
 * lowercased table-ref. Best-effort (regex, not a full parser) and safe on
 * partial/incomplete SQL.
 */
export function parseTableAliases(sqlText: string): Map<string, string> {
  const aliases = new Map<string, string>()
  // table ref: optionally-quoted segment + optional `.segment`; then an
  // optionally-quoted alias (optionally preceded by AS).
  const re =
    /(?:from|join|,)\s+("[^"]+"|`[^`]+`|[a-z_]\w*)((?:\.(?:"[^"]+"|`[^`]+`|[a-z_]\w*))?)\s+(?:as\s+)?("[^"]+"|`[^`]+`|[a-z_]\w*)/gi
  let m: RegExpExecArray | null
  while ((m = re.exec(sqlText)) !== null) {
    const table = normalizeQualifier(m[1] + m[2])
    const alias = stripQuotes(m[3]).toLowerCase()
    if (NOT_ALIASES.has(alias) || NOT_ALIASES.has(table)) continue
    aliases.set(alias, table)
  }
  return aliases
}

/**
 * Extract the bare table names referenced in a statement's FROM/JOIN/comma
 * clauses (schema qualifier dropped, quotes stripped, lowercased). Used to
 * scope FK join-hints to the tables actually in play.
 */
export function tableRefsInStatement(sqlText: string): Set<string> {
  const refs = new Set<string>()
  const re = /(?:from|join|,)\s+("[^"]+"|`[^`]+`|[a-z_]\w*(?:\.[a-z_]\w*)?)/gi
  let m: RegExpExecArray | null
  while ((m = re.exec(sqlText)) !== null) {
    const norm = normalizeQualifier(m[1])
    const bare = norm.includes(".") ? norm.slice(norm.lastIndexOf(".") + 1) : norm
    if (NOT_ALIASES.has(bare)) continue
    refs.add(bare)
  }
  return refs
}

/**
 * Column completions for a qualifier that may be a table alias (`o`), a bare
 * table name (`orders`) or a fully-qualified table (`sales.orders`). Quotes are
 * tolerated. Returns [] when unresolved — and, crucially, when a BARE name is
 * ambiguous across schemas, so we never autocomplete the wrong table (I3).
 */
export function columnCompletionsFor(
  qualifier: string,
  lookup: SchemaLookup,
  aliases: Map<string, string>,
): CompletionOption[] {
  const norm = normalizeQualifier(qualifier)
  // Resolve an alias to its underlying (possibly schema-qualified) table.
  const resolved = aliases.get(norm) ?? norm

  let cols: CompletionColumn[] | undefined
  if (resolved.includes(".")) {
    // Fully-qualified `schema.table` — resolve against the qualified label
    // only; do NOT fall back to a bare-name guess for a multi-segment qualifier.
    cols = lookup.byLabel.get(resolved)
  } else {
    if (lookup.bareAmbiguous.has(resolved)) return []
    cols = lookup.byBareName.get(resolved) ?? lookup.byLabel.get(resolved)
  }
  if (!cols || cols.length === 0) return []
  return cols.map((c) => ({
    label: c.name,
    type: c.is_primary_key ? "constant" : "property",
    detail: c.type ?? "",
  }))
}

/**
 * Tables in a given schema, bare-labelled — offered after `schema.` so a
 * schema-qualified prefix completes table names rather than SQL keywords.
 * Case-insensitive; returns [] for an unknown schema.
 */
export function tablesInSchema(schema: string, lookup: SchemaLookup): CompletionOption[] {
  const tables = lookup.bySchema.get(stripQuotes(schema).toLowerCase())
  if (!tables) return []
  return tables.map((t) => ({ label: t.bare, type: "class", detail: "table", boost: 5 }))
}

/**
 * Completions at a word boundary: qualified tables (boosted so they sort
 * first) plus de-duplicated column names. When the same column name appears
 * across tables with DIFFERENT types, the type detail is neutralized to
 * "column" rather than presenting one table's type as authoritative (I8).
 * SQL keywords are added separately by the dialect keyword source.
 */
export function wordCompletions(lookup: SchemaLookup): CompletionOption[] {
  const opts: CompletionOption[] = []
  for (const t of lookup.tables) {
    opts.push({ label: t.label, type: "class", detail: "table", boost: 5 })
  }
  const firstType = new Map<string, string>() // column name → first type seen ("" if untyped)
  const order: string[] = []
  const conflicting = new Set<string>()
  for (const t of lookup.tables) {
    for (const c of t.columns) {
      const ty = c.type ?? ""
      if (!firstType.has(c.name)) {
        firstType.set(c.name, ty)
        order.push(c.name)
      } else if (firstType.get(c.name) !== ty) {
        conflicting.add(c.name)
      }
    }
  }
  for (const name of order) {
    const detail = conflicting.has(name) ? "column" : firstType.get(name) || "column"
    opts.push({ label: name, type: "property", detail })
  }
  return opts
}

/**
 * Suggest join-key equalities from foreign keys, e.g.
 * `sales.orders.user_id = sales.users.id` — offered after `ON` in a JOIN.
 * When `scope` is given, only FKs whose BOTH endpoints (bare table names) are
 * in scope are returned, so the hint list reflects the current FROM/JOIN set.
 */
export function foreignKeyJoinHints(
  fks: ForeignKeyLike[],
  scope?: Set<string>,
): CompletionOption[] {
  const inScope = (fk: ForeignKeyLike): boolean =>
    !scope || (scope.has(fk.from_table.toLowerCase()) && scope.has(fk.to_table.toLowerCase()))
  return fks.filter(inScope).map((fk) => {
    const from = fk.from_schema ? `${fk.from_schema}.${fk.from_table}` : fk.from_table
    const to = fk.to_schema ? `${fk.to_schema}.${fk.to_table}` : fk.to_table
    return {
      label: `${from}.${fk.from_column} = ${to}.${fk.to_column}`,
      type: "keyword",
      detail: "join key",
      boost: 3,
    }
  })
}
