// Shared predicate for rsync's internal bookkeeping (`_rsync_*`, `rsync_*`) and
// pipeline-staging (`flat_mysql_<ts>`, `flat_pg_*`, `flat_postgres_*`) tables.
// These land in user destinations to track CDC offsets / pipeline state or hold
// denormalized staging copies, but they are noise in the Data Explorer — they
// must not appear in NL example chips, SQL autocomplete, the schema tree, or the
// table-resolution HITL. Anchored to specific prefixes (not a bare leading
// underscore) so a user's own `_staging_*` table is left alone.

export const INTERNAL_TABLE_PREFIX = "_rsync"

// All case-insensitive prefixes that mark a table as rsync-internal.
const INTERNAL_TABLE_PREFIXES = [
  INTERNAL_TABLE_PREFIX,
  "rsync_",
  "flat_mysql_",
  "flat_pg_",
  "flat_postgres_",
]

/**
 * True when `name` is an rsync-internal bookkeeping or pipeline-staging table.
 * Accepts either a bare (`flat_mysql_123`) or schema-qualified
 * (`public._rsync_pipelines`) name; matching is case-insensitive.
 * Null / undefined / empty → false.
 */
export function isInternalExplorerTable(name?: string | null): boolean {
  if (!name) return false
  // Strip a leading schema qualifier so `public._rsync_x` matches too.
  const dot = name.lastIndexOf(".")
  const bare = (dot >= 0 ? name.slice(dot + 1) : name).toLowerCase()
  return INTERNAL_TABLE_PREFIXES.some((p) => bare.startsWith(p))
}

/** Drop rsync-internal tables, preserving the order of the rest. */
export function excludeInternalTables<T extends { name: string }>(tables: T[]): T[] {
  return tables.filter((t) => !isInternalExplorerTable(t.name))
}
