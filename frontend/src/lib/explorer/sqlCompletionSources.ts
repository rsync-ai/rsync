// CodeMirror completion-source assembly for the Data Explorer SQL editor.
// Kept separate from the React component so the source list can be unit-tested
// headlessly (no EditorView), and separate from the framework-agnostic
// `sqlCompletions.ts` because this module is intentionally CodeMirror-coupled.

import type {
  CompletionContext,
  CompletionResult,
  CompletionSource,
} from "@codemirror/autocomplete"
import { keywordCompletionSource, type SQLDialect } from "@codemirror/lang-sql"
import {
  type SchemaLookup,
  type ForeignKeyLike,
  parseTableAliases,
  columnCompletionsFor,
  tablesInSchema,
  wordCompletions,
  foreignKeyJoinHints,
  tableRefsInStatement,
} from "./sqlCompletions"

// A dotted qualifier before the cursor: one or more (optionally quoted)
// identifier segments joined by dots, ending in `.<partial-word>`. Supports
// `alias.`, `table.`, `schema.table.`, and quoted ids (`"Users".` / `` `Orders`. ``).
const DOT_RE = /(?:"[^"]*"|`[^`]*`|[\w$]+)(?:\.(?:"[^"]*"|`[^`]*`|[\w$]+))*\.[\w$]*$/

// A SINGLE qualifier + dot immediately before the cursor — used to suppress the
// keyword source after any `ident.`/`schema.` (keywords are never valid right
// after a dot, and would otherwise bury the column/table list).
const AFTER_DOT_RE = /(?:"[^"]*"|`[^`]*`|[\w$]+)\.[\w$]*$/

/**
 * Build the schema-aware completion source: alias/table/schema-qualified
 * columns after a dot, the schema's tables after `schema.`, and tables +
 * columns (plus FK join-keys after `ON`) at a word boundary. Closes over the
 * given lookup + foreign keys; returns null when it has nothing to offer.
 */
export function createSchemaCompletionSource(
  lookup: SchemaLookup,
  foreignKeys: ForeignKeyLike[],
): CompletionSource {
  return (ctx: CompletionContext): CompletionResult | null => {
    const dotMatch = ctx.matchBefore(DOT_RE)
    if (dotMatch) {
      const lastDot = dotMatch.text.lastIndexOf(".")
      const qualifier = dotMatch.text.slice(0, lastDot)
      const from = dotMatch.from + lastDot + 1 // first char past the final dot
      const aliases = parseTableAliases(ctx.state.sliceDoc(0, ctx.pos))

      const columns = columnCompletionsFor(qualifier, lookup, aliases)
      if (columns.length > 0) {
        return { from, options: columns, validFor: /^[\w$]*$/ }
      }
      // Not a table/alias — maybe a schema. Offer that schema's tables so
      // `public.` / `public._rs` completes table names, not SQL keywords (I1).
      const schemaTables = tablesInSchema(qualifier, lookup)
      if (schemaTables.length > 0) {
        return { from, options: schemaTables, validFor: /^[\w$]*$/ }
      }
      // Recognized a dot but resolved nothing — decline. The keyword source is
      // independently suppressed after a dot (see buildSqlCompletionSources),
      // so this yields an empty list rather than keyword garbage.
      return null
    }

    const wordMatch = ctx.matchBefore(/[\w$.]*/)
    if (!wordMatch || (wordMatch.from === wordMatch.to && !ctx.explicit)) {
      return null
    }

    // Right after `ON` in a JOIN, surface FK join-key equalities first — scoped
    // to the tables actually referenced in this statement (I7).
    const before = ctx.state.sliceDoc(0, ctx.pos)
    const afterJoinOn = /\bjoin\b/i.test(before) && /\bon\s+[\w.]*$/i.test(before)
    const fkHints =
      afterJoinOn && foreignKeys.length > 0
        ? foreignKeyJoinHints(foreignKeys, tableRefsInStatement(before))
        : []
    const options =
      fkHints.length > 0 ? [...fkHints, ...wordCompletions(lookup)] : wordCompletions(lookup)
    return { from: wordMatch.from, options, validFor: /^[\w$]*$/ }
  }
}

/**
 * Assemble the completion sources passed to `autocompletion({ override })`.
 *
 * `override` REPLACES the "autocomplete" language-data sources, so the dialect
 * keyword completer that `sql()` registers is NOT used unless we add it back.
 * We pair the caller's schema-aware source with a GUARDED keyword source: the
 * guard suppresses keyword completions immediately after `ident.`/`schema.`
 * (where keywords are never valid and would otherwise bury the column/table
 * list — the `public._rs` → PRINT_STRICT_PARAMS… bug, I1b), while still
 * completing SELECT / WHERE / JOIN / … everywhere else.
 *
 * `upperCase=true` matches the editor's `sql({ upperCaseKeywords: true })`.
 */
export function buildSqlCompletionSources(
  schemaSource: CompletionSource,
  dialect: SQLDialect,
): CompletionSource[] {
  const keywords = keywordCompletionSource(dialect, true)
  const guardedKeywords: CompletionSource = (ctx) => {
    if (ctx.matchBefore(AFTER_DOT_RE)) return null
    return keywords(ctx)
  }
  return [schemaSource, guardedKeywords]
}
