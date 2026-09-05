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
