/**
 * Splits a SQL buffer into its top-level statements and decides which single
 * statement a Run action should execute.
 *
 * Why this exists
 * ---------------
 * The Data Explorer editor is a scratchpad: people keep several queries in it
 * and run them one at a time. The api-gateway, however, accepts exactly ONE
 * statement per request and always will — `validators.ValidateExplorerStatement`
 * rejects a stacked body BEFORE the class/role gate specifically so an admin's
 * `UPDATE …` cannot smuggle an owner-only `DROP …` past the role check
 * (lib/pq's simple-query protocol would run both in one implicit transaction).
 * That rule is a security boundary and is not relaxed.
 *
 * So the fix belongs here: keep the buffer intact, and send the server the one
 * statement the user actually asked for — the selection, or the statement under
 * the caret.
 *
 * SAFETY CONTRACT
 * ---------------
 * `resolveRunTarget` returns the exact string that will be executed. Every
 * client-side gate (statement class, workspace role, destructive confirmation)
 * MUST be evaluated against `RunTarget.sql` and nothing else. Classifying the
 * whole buffer while executing one statement is precisely the defect shape
 * documented in `runShortcut.ts`: a buffer starting with a harmless SELECT
 * would wave through a DROP sitting under the caret.
 *
 * Lexer scope: single quotes (`''` doubling, plus backslash escapes inside
 * `E'…'`), double-quoted and backtick identifiers, `--` line comments,
 * `/* … *\/` block comments, and Postgres dollar quoting (`$$ … $$`,
 * `$tag$ … $tag$`). Anything it gets wrong degrades to a visible error — an
 * under-split body is rejected by the server, an over-split one is a syntax
 * error — never to a silently different statement, because the gates read the
 * same string the fetch sends.
 */

export interface SqlStatement {
  /** Statement text, trimmed, without its terminating `;`. */
  text: string
  /** Offset of `text[0]` in the source document. */
  from: number
  /** Offset one past the last character of `text`. */
  to: number
}

export type RunTargetSource =
  /** The user selected text; we run the selection. */
  | "selection"
  /** Several statements in the buffer; we run the one under the caret. */
  | "statement"
  /** One statement (or an unparseable buffer); we run the whole thing. */
  | "buffer"

export interface RunTarget {
  /** THE string to execute. Gate on this, send this. */
  sql: string
  /** Range of `sql` within the source document — lets callers splice a
   *  rewritten statement back without disturbing the rest of the buffer. */
  from: number
  to: number
  source: RunTargetSource
  /** 1-based position of the target among the buffer's statements; 0 if unknown. */
  index: number
  /** Total top-level statements in the buffer. */
  total: number
  /** True when the target ITSELF holds more than one statement (only reachable
   *  by selecting across a `;`). Callers must refuse to run it — the server
   *  rejects stacked statements by design. */
  multi: boolean
}

const IDENT_CHAR = /[A-Za-z0-9_$]/

/** Matches a dollar-quote opener at `i`: `$$` or `$tag$`. Returns the tag text
 *  (including both `$`), or null. `$1` and `$foo` (no closer) are not openers. */
function dollarQuoteAt(sql: string, i: number): string | null {
  if (sql[i] !== "$") return null
  let j = i + 1
  while (j < sql.length && /[A-Za-z0-9_]/.test(sql[j])) j++
  if (sql[j] !== "$") return null
  // A tag may not start with a digit (`$1$` is not a valid dollar-quote tag).
  const tag = sql.slice(i + 1, j)
  if (tag.length > 0 && /^[0-9]/.test(tag)) return null
  return sql.slice(i, j + 1)
}

/** True when the `'` at `i` opens a Postgres escape string (`E'…'`), where a
 *  backslash escapes the following character. */
function isEscapeStringAt(sql: string, i: number): boolean {
  const prev = sql[i - 1]
  if (prev !== "E" && prev !== "e") return false
  const before = sql[i - 2]
  return before === undefined || !IDENT_CHAR.test(before)
}

/**
 * Splits `sql` into top-level statements. Whitespace-only and comment-only
 * spans are dropped, so `SELECT 1;;` and a trailing `-- note` yield one
 * statement, not three.
 */
export function splitSqlStatements(sql: string): SqlStatement[] {
  const src = String(sql ?? "")
  const out: SqlStatement[] = []

  // First / last offsets of executable (non-comment, non-whitespace) content in
  // the current span. -1 means "nothing executable seen yet".
  let contentStart = -1
  let contentEnd = -1

  const noteContent = (from: number, to: number) => {
    if (contentStart === -1) contentStart = from
    contentEnd = to
  }

  const flush = () => {
    if (contentStart !== -1) {
      out.push({
        text: src.slice(contentStart, contentEnd),
        from: contentStart,
        to: contentEnd,
      })
    }
    contentStart = -1
    contentEnd = -1
  }

  let i = 0
  while (i < src.length) {
    const c = src[i]

    // ── comments: skipped entirely, never count as content ──
    if (c === "-" && src[i + 1] === "-") {
      const nl = src.indexOf("\n", i)
      i = nl === -1 ? src.length : nl + 1
      continue
    }
    if (c === "/" && src[i + 1] === "*") {
      const end = src.indexOf("*/", i + 2)
      i = end === -1 ? src.length : end + 2
      continue
    }

    if (/\s/.test(c)) {
      i++
      continue
    }

    // ── quoted regions: opaque, so a `;` inside never splits ──
    if (c === "'" || c === '"' || c === "`") {
      const escapes = c === "'" && isEscapeStringAt(src, i)
      const start = i
      i++
      while (i < src.length) {
        if (escapes && src[i] === "\\") {
          i += 2
          continue
        }
        if (src[i] === c) {
          // A doubled quote is a literal quote, not the terminator.
          if (src[i + 1] === c) {
            i += 2
            continue
          }
          i++
          break
        }
        i++
      }
      noteContent(start, Math.min(i, src.length))
      continue
    }

    const tag = dollarQuoteAt(src, i)
    if (tag) {
      const start = i
      const close = src.indexOf(tag, i + tag.length)
      i = close === -1 ? src.length : close + tag.length
      noteContent(start, i)
      continue
    }

    // ── the only thing that ends a statement ──
    if (c === ";") {
      flush()
      i++
      continue
    }

    noteContent(i, i + 1)
    i++
  }
  flush()

  return out
}

/** Index of the statement the caret belongs to: the last one that starts at or
 *  before the caret, else the first. Returns -1 for an empty list. */
function statementIndexAtOffset(statements: SqlStatement[], caret: number): number {
  if (statements.length === 0) return -1
  let idx = 0
  for (let k = 0; k < statements.length; k++) {
    if (statements[k].from <= caret) idx = k
    else break
  }
  return idx
}

/**
 * Decides what a Run action should execute.
 *
 * - A non-empty selection wins — that is the user being explicit.
 * - Otherwise, with several statements in the buffer, the one under the caret.
 * - Otherwise the whole buffer.
 *
 * `selectionFrom`/`selectionTo` are document offsets; pass equal values (or
 * omit them) when there is no selection.
 */
export function resolveRunTarget(
  doc: string,
  selectionFrom = 0,
  selectionTo = 0
): RunTarget {
  const src = String(doc ?? "")
  const statements = splitSqlStatements(src)
  const total = statements.length

  const lo = Math.max(0, Math.min(selectionFrom, selectionTo))
  const hi = Math.min(src.length, Math.max(selectionFrom, selectionTo))

  if (hi > lo) {
    const inner = splitSqlStatements(src.slice(lo, hi))
    if (inner.length > 0) {
      const first = inner[0]
      const last = inner[inner.length - 1]
      return {
        // Span the whole selection when it holds several statements so the
        // caller's error message quotes what the user actually highlighted.
        sql: src.slice(lo + first.from, lo + last.to),
        from: lo + first.from,
        to: lo + last.to,
        source: "selection",
        index: statementIndexAtOffset(statements, lo + first.from) + 1,
        total,
        multi: inner.length > 1,
      }
    }
    // Selection is pure whitespace/comment — fall through to the caret rules.
  }

  if (total > 1) {
    const k = statementIndexAtOffset(statements, lo)
    const stmt = statements[k]
    return {
      sql: stmt.text,
      from: stmt.from,
      to: stmt.to,
      source: "statement",
      index: k + 1,
      total,
      multi: false,
    }
  }

  if (total === 1) {
    const stmt = statements[0]
    return {
      sql: stmt.text,
      from: stmt.from,
      to: stmt.to,
      source: "buffer",
      index: 1,
      total: 1,
      multi: false,
    }
  }

  // No statements at all. `noteContent` fires for every character that is not
  // whitespace and not inside a comment, so total === 0 means the buffer holds
  // nothing executable — blank, comments only, or everything swallowed by an
  // unterminated `/*`. Report an empty target: the caller disables Run and says
  // "enter a SQL query", which beats a round-trip that can only come back an error.
  return { sql: "", from: 0, to: 0, source: "buffer", index: 0, total: 0, multi: false }
}

/** Replaces `[target.from, target.to)` in `doc` with `replacement`, leaving the
 *  rest of the buffer — the user's other queries — untouched.
 *
 *  No-ops unless that range still holds `target.sql`. A target is resolved against one
 *  document and may be spliced into a later one (the destructive-confirm dialog can sit
 *  open across an edit); if the buffer has moved on, the offsets describe someone else's
 *  text and writing through them would corrupt a query the user never ran. */
export function spliceRunTarget(doc: string, target: RunTarget, replacement: string): string {
  const src = String(doc ?? "")
  if (target.from < 0 || target.to > src.length || target.from > target.to) return src
  if (src.slice(target.from, target.to) !== target.sql) return src
  return src.slice(0, target.from) + replacement + src.slice(target.to)
}
