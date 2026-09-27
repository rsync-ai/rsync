// Pure helpers for the Data Explorer's database → tables tree.
//
// The schema-index API returns a *flat* list of tables, each carrying a
// `schema` field (the MySQL database or Postgres schema it lives in).
// Grouping by that field yields the left-panel tree with no extra backend
// call — see api-gateway GetSchemaIndex / cache.ExplorerTableIndex.

export interface SchemaColumnLike {
  name: string
  type?: string
  is_primary_key?: boolean
}

export interface SchemaTableLike {
  name: string
  schema?: string
  row_count?: number
  columns?: SchemaColumnLike[]
}

export interface DatabaseGroup {
  /** The database / schema name, or "(default)" when a table has none. */
  database: string
  /** Tables in this database, sorted alphabetically (case-insensitive). */
  tables: SchemaTableLike[]
  tableCount: number
}

/** Bucket label used when a table reports no schema. */
export const DEFAULT_DATABASE = "(default)"

const compareCi = (a: string, b: string): number =>
  a.localeCompare(b, undefined, { sensitivity: "base" })

/**
 * Group a flat table list into databases, ready to render as an
 * expandable tree. Databases and the tables within them are each sorted
 * alphabetically (case-insensitive). Tables with no schema (undefined or
 * empty string) fall into the "(default)" bucket. Pure + deterministic.
 */
export function groupTablesByDatabase(tables: SchemaTableLike[]): DatabaseGroup[] {
  const byDatabase = new Map<string, SchemaTableLike[]>()
  for (const table of tables) {
    const db = table.schema && table.schema.trim() !== "" ? table.schema : DEFAULT_DATABASE
    const existing = byDatabase.get(db)
    if (existing) existing.push(table)
    else byDatabase.set(db, [table])
  }

  return [...byDatabase.entries()]
    .sort(([a], [b]) => compareCi(a, b))
    .map(([database, tbls]) => ({
      database,
      tables: [...tbls].sort((x, y) => compareCi(x.name, y.name)),
      tableCount: tbls.length,
    }))
}

// ── Relationships ───────────────────────────────────────────────────────────
//
// The same schema-index response that carries `tables` also carries
// `foreign_keys` (api-gateway cache.ExplorerForeignKeyIndex). The page already
// feeds them to NL→SQL generation so the model can write correct JOINs, but
// nothing ever showed them to the user. `confidence` distinguishes the two
// sources: 1.0 is a real FK constraint read from the catalog, anything lower is
// a relationship the Phase 3.5 heuristics *inferred* from names and types — a
// difference worth making visible before someone joins on it.

export interface SchemaForeignKeyLike {
  from_schema?: string
  from_table: string
  from_column: string
  to_schema?: string
  to_table: string
  to_column: string
  confidence?: number
}

/** Relationships touching one table, split by direction. */
export interface TableRelationships {
  /** This table's columns pointing at another table. */
  outgoing: SchemaForeignKeyLike[]
  /** Other tables' columns pointing at this one. */
  incoming: SchemaForeignKeyLike[]
}

/** A foreign key is "inferred" when it did not come from a real constraint. */
export function isInferredForeignKey(fk: SchemaForeignKeyLike): boolean {
  return typeof fk.confidence === "number" && fk.confidence < 1
}

/** Lookup key for a table: lower-cased, schema-qualified when there is one. */
export function foreignKeyTableKey(schema: string | undefined, table: string): string {
  const t = table.toLowerCase()
  return schema && schema.trim() !== "" ? `${schema.trim().toLowerCase()}.${t}` : t
}

/**
 * groupForeignKeysByTable indexes a flat foreign-key list by both endpoints, so
 * a table can render what it points at and what points at it in one lookup.
 * Pure + deterministic; input order is preserved within each direction.
 */
export function groupForeignKeysByTable(
  foreignKeys: SchemaForeignKeyLike[] | undefined | null,
): Map<string, TableRelationships> {
  const index = new Map<string, TableRelationships>()
  if (!foreignKeys?.length) return index
  const bucket = (key: string): TableRelationships => {
    const existing = index.get(key)
    if (existing) return existing
    const created: TableRelationships = { outgoing: [], incoming: [] }
    index.set(key, created)
    return created
  }
  for (const fk of foreignKeys) {
    if (!fk?.from_table || !fk.to_table) continue
    bucket(foreignKeyTableKey(fk.from_schema, fk.from_table)).outgoing.push(fk)
    bucket(foreignKeyTableKey(fk.to_schema, fk.to_table)).incoming.push(fk)
  }
  return index
}

const NO_RELATIONSHIPS: TableRelationships = { outgoing: [], incoming: [] }

/**
 * relationshipsFor looks a table up in the index. Tables and foreign keys come
 * from the same payload so the qualified key normally hits; the bare-name
 * fallback covers a connector that qualifies one list and not the other.
 */
export function relationshipsFor(
  index: Map<string, TableRelationships>,
  table: SchemaTableLike,
): TableRelationships {
  return (
    index.get(foreignKeyTableKey(table.schema, table.name)) ??
    index.get(foreignKeyTableKey(undefined, table.name)) ??
    NO_RELATIONSHIPS
  )
}
