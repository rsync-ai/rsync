// Shared display metadata for data-pipeline transforms.
//
// One source of truth for how a transform's engine `type` (a.k.a. the persisted
// `transform_config.operation`) is presented to humans — used by both the
// creation modal (SuggestionsReviewDialog) and the post-creation Transforms tab
// (PipelineTransformsTab). Keep the maps keyed by the raw engine type so a new
// engine type still renders sensibly via the fallbacks below.

// Human-readable group headers, keyed by transform type.
export const TRANSFORM_TYPE_LABELS: Record<string, string> = {
  json_flatten: "Flatten JSON columns",
  array_expand: "Expand array columns",
  rename_columns: "Rename columns",
  mask_pii: "Mask PII",
  select_columns: "Select columns",
  exclude_columns: "Exclude columns",
  filter: "Filter rows",
  null_handle: "Handle nulls",
  truncate: "Truncate values",
  type_convert: "Convert types",
}

// Short description of what a transform group does, shown under the header.
export const TRANSFORM_TYPE_BLURBS: Record<string, string> = {
  json_flatten: "Expands nested JSON into queryable top-level columns.",
  array_expand: "Expands array elements into indexed columns.",
  rename_columns: "Renames source columns in the destination.",
  mask_pii: "Masks or hashes sensitive values before they land.",
  select_columns: "Keeps only the listed columns.",
  exclude_columns: "Drops the listed columns.",
  filter: "Keeps only rows matching a condition.",
  null_handle: "Fills or drops rows with null values.",
  truncate: "Caps string length.",
  type_convert: "Casts values to a target type.",
}

// Falls back to a title-cased version of the raw type when unknown, so a new
// engine type still reads sensibly instead of showing a snake_case identifier.
export function transformTypeLabel(type: string): string {
  return (
    TRANSFORM_TYPE_LABELS[type] ||
    type.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
  )
}

export function transformTypeBlurb(type: string): string {
  return TRANSFORM_TYPE_BLURBS[type] || ""
}

// Tailwind class bundles for a per-type colored badge + dot. Colors encode
// meaning, not sequence: routine schema-shaping (flatten/array/rename/cast) uses
// muted neutral-leaning ramps so genuinely sensitive ops (mask_pii) stand out.
export type TransformTone = { badge: string; dot: string }

const TRANSFORM_TONES: Record<string, TransformTone> = {
  json_flatten: {
    badge: "bg-teal-50 text-teal-700 dark:bg-teal-950/40 dark:text-teal-300",
    dot: "bg-teal-500",
  },
  array_expand: {
    badge: "bg-blue-50 text-blue-700 dark:bg-blue-950/40 dark:text-blue-300",
    dot: "bg-blue-500",
  },
  rename_columns: {
    badge: "bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300",
    dot: "bg-zinc-400",
  },
  mask_pii: {
    badge: "bg-pink-50 text-pink-700 dark:bg-pink-950/40 dark:text-pink-300",
    dot: "bg-pink-500",
  },
}

const NEUTRAL_TONE: TransformTone = {
  badge: "bg-zinc-100 text-zinc-600 dark:bg-zinc-800 dark:text-zinc-300",
  dot: "bg-zinc-400",
}

export function transformTypeTone(type: string): TransformTone {
  return TRANSFORM_TONES[type] || NEUTRAL_TONE
}

// A short verb form used on the per-row type badge (e.g. "Flatten", "Mask").
const TRANSFORM_TYPE_VERBS: Record<string, string> = {
  json_flatten: "Flatten",
  array_expand: "Expand",
  rename_columns: "Rename",
  mask_pii: "Mask",
  select_columns: "Select",
  exclude_columns: "Exclude",
  filter: "Filter",
  null_handle: "Nulls",
  truncate: "Truncate",
  type_convert: "Cast",
}

export function transformTypeVerb(type: string): string {
  return TRANSFORM_TYPE_VERBS[type] || transformTypeLabel(type)
}

function asString(v: unknown): string {
  return typeof v === "string" ? v : ""
}

// Strip a table-qualified prefix (`schema.table.col` -> `col`) for display.
function bareColumn(name: string): string {
  const s = name.trim()
  return s.includes(".") ? s.slice(s.lastIndexOf(".") + 1) : s
}

// Produce a self-describing, scannable label for a single transform from its
// config — the column it targets and what it does to it — instead of repeating
// the opaque engine type. `primary` is the column/target (bold in the UI);
// `secondary` is the resulting shape (muted).
export function describeTransform(
  operation: string,
  config: Record<string, unknown> | undefined | null,
): { primary: string; secondary?: string } {
  const cfg = config || {}
  const column = bareColumn(asString(cfg.column))

  switch (operation) {
    case "json_flatten": {
      const prefix = asString(cfg.prefix) || (column ? `${column}_` : "")
      return { primary: column || "JSON column", secondary: prefix ? `→ ${prefix}*` : "→ flattened" }
    }
    case "array_expand": {
      const prefix = asString(cfg.output_prefix) || (column ? `${column}_` : "")
      return { primary: column || "array column", secondary: prefix ? `→ ${prefix}0, ${prefix}1…` : "→ expanded" }
    }
    case "mask_pii": {
      const maskType = asString(cfg.mask_type)
      if (column) return { primary: column, secondary: maskType ? `→ ${maskType}` : undefined }
      const cols = Array.isArray(cfg.columns) ? (cfg.columns as unknown[]).length : 0
      return { primary: cols ? `${cols} columns` : "PII columns", secondary: maskType ? `→ ${maskType}` : undefined }
    }
    case "rename_columns": {
      const mappings = (cfg.mappings && typeof cfg.mappings === "object" ? cfg.mappings : {}) as Record<string, unknown>
      const entries = Object.entries(mappings)
      if (entries.length === 0) return { primary: "Column renames" }
      const [from, to] = entries[0]
      const first = `${bareColumn(from)} → ${asString(to) || ""}`
      return entries.length === 1
        ? { primary: first }
        : { primary: `${entries.length} columns renamed`, secondary: `e.g. ${first}` }
    }
    case "select_columns":
    case "exclude_columns": {
      const cols = Array.isArray(cfg.columns) ? (cfg.columns as unknown[]).length : 0
      return { primary: cols ? `${cols} columns` : "columns" }
    }
    case "filter": {
      const cond = asString(cfg.condition)
      return { primary: cond || "filter condition" }
    }
    default:
      return { primary: column || transformTypeLabel(operation) }
  }
}
