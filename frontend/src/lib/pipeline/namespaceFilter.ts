/**
 * Namespace filter: which databases or schemas a connection may see (#31) —
 * the connection modal's Scope step. The server applies it (Python
 * shared/mcp-connectors/public/namespace_filter.py and Go
 * backend-orchestrator/pkg/namespacefilter); this copy only previews what a
 * pattern keeps before the connection is saved. All three are pinned to
 * shared/mcp-connectors/public/namespace_filter_vectors.json
 * (__tests__/namespaceFilter.test.ts); change the algorithm in all of them, or
 * in none.
 *
 * namespace_filter_mode: "all" (default when missing or empty), "include" or
 * "exclude", trimmed and case-insensitive; any other value is an error.
 * namespace_filter_patterns: comma-separated, entries trimmed and empties
 * dropped, at most 100 of at most 128 UTF-8 bytes each. "*" matches any run of
 * characters; everything else is literal; a pattern matches the whole name,
 * case-insensitively. "include" needs a pattern. System names are rejected
 * first, whatever the mode.
 */

export const NAMESPACE_FILTER_MODE_KEY = "namespace_filter_mode"
export const NAMESPACE_FILTER_PATTERNS_KEY = "namespace_filter_patterns"

export type NamespaceFilterMode = "all" | "include" | "exclude"
const MODES: readonly NamespaceFilterMode[] = ["all", "include", "exclude"]

export const MAX_NAMESPACE_PATTERNS = 100
export const MAX_NAMESPACE_PATTERN_BYTES = 128

export const NAMESPACE_NO_MATCH_WARNING = "The namespace filter matched no databases or schemas."

export class NamespaceFilterError extends Error {}

export type NamespaceFilter = {
  mode: NamespaceFilterMode
  patterns: string[]
}

export type NamespaceFilterResult = {
  kept: string[]
  excluded: string[]
  warning: string // "" when there is nothing to warn about
}

// Whole-name match where "*" is the only wildcard. The same iterative
// two-pointer matcher as the Python copy (Go compiles to RE2, which does not
// backtrack): a backtracking regex is exponential in the star count for a name
// that does not match. Both arguments are lower-cased code-point arrays.
function globMatch(pattern: string[], name: string[]): boolean {
  let p = 0
  let n = 0
  let star = -1
  let mark = 0
  while (n < name.length) {
    if (p < pattern.length && pattern[p] !== "*" && pattern[p] === name[n]) {
      p++
      n++
    } else if (p < pattern.length && pattern[p] === "*") {
      star = p
      mark = n
      p++
    } else if (star !== -1) {
      p = star + 1
      mark++
      n = mark
    } else {
      return false
    }
  }
  while (p < pattern.length && pattern[p] === "*") p++
  return p === pattern.length
}

const utf8 = new TextEncoder()

/** Reads the filter from a connection config. Throws NamespaceFilterError. */
export function parseNamespaceFilter(config: Record<string, unknown> | undefined): NamespaceFilter {
  const rawMode = config?.[NAMESPACE_FILTER_MODE_KEY] ?? ""
  const rawPatterns = config?.[NAMESPACE_FILTER_PATTERNS_KEY] ?? ""
  if (typeof rawMode !== "string") throw new NamespaceFilterError("namespace_filter_mode must be a string")
  if (typeof rawPatterns !== "string") {
    throw new NamespaceFilterError("namespace_filter_patterns must be a comma-separated string")
  }
  const mode = (rawMode.trim().toLowerCase() || "all") as NamespaceFilterMode
  if (!MODES.includes(mode)) {
    throw new NamespaceFilterError("namespace_filter_mode must be one of: all, include, exclude")
  }
  const patterns = rawPatterns
    .split(",")
    .map((p) => p.trim())
    .filter((p) => p !== "")
  if (patterns.length > MAX_NAMESPACE_PATTERNS) {
    throw new NamespaceFilterError(`namespace_filter_patterns allows at most ${MAX_NAMESPACE_PATTERNS} patterns`)
  }
  if (patterns.some((p) => utf8.encode(p).length > MAX_NAMESPACE_PATTERN_BYTES)) {
    throw new NamespaceFilterError(
      `each namespace_filter_patterns entry must be at most ${MAX_NAMESPACE_PATTERN_BYTES} bytes`,
    )
  }
  if (mode === "include" && patterns.length === 0) {
    throw new NamespaceFilterError("namespace_filter_mode 'include' needs at least one pattern")
  }
  return { mode, patterns }
}

/** True when the filter can drop a non-system name. */
export function namespaceFilterActive(f: NamespaceFilter): boolean {
  return f.mode === "include" || (f.mode === "exclude" && f.patterns.length > 0)
}

function matchesAny(name: string, f: NamespaceFilter): boolean {
  const lowered = Array.from(name.toLowerCase())
  return f.patterns.some((p) => globMatch(Array.from(p.toLowerCase()), lowered))
}

/** True when name passes: not a system namespace, then the mode applies. */
export function namespaceAllowed(name: string, f: NamespaceFilter, systemNames: readonly string[] = []): boolean {
  const lowered = name.toLowerCase()
  if (systemNames.some((s) => s.toLowerCase() === lowered)) return false
  if (f.mode === "include") return matchesAny(name, f)
  if (f.mode === "exclude") return !matchesAny(name, f)
  return true
}

/**
 * Splits names into kept and excluded, in input order. warning is set when an
 * active filter leaves nothing: a database may not exist yet, so it is not an
 * error.
 */
export function applyNamespaceFilter(
  names: readonly string[],
  f: NamespaceFilter,
  systemNames: readonly string[] = [],
): NamespaceFilterResult {
  const kept: string[] = []
  const excluded: string[] = []
  for (const n of names) (namespaceAllowed(n, f, systemNames) ? kept : excluded).push(n)
  return { kept, excluded, warning: namespaceFilterActive(f) && kept.length === 0 ? NAMESPACE_NO_MATCH_WARNING : "" }
}
