/**
 * Client-side mirror of the api-gateway's statement classifier
 * (`validators.ClassifyStatementSQL` / `classToMinRole`). Both sides are pinned to
 * `shared/explorer_statement_class_golden.json` — change a class there and in both
 * classifiers together, or one of the two golden tests goes red.
 *
 * Advisory only — `validators.ValidateExplorerStatement` on the backend is the
 * source of truth and independently enforces the role gate. The UI copy exists
 * so the Run button, the destructive-confirm dialog, and the role message can
 * react before a round-trip.
 *
 * Extracted from the Explorer page so it can be unit-tested against
 * `resolveRunTarget` — the pairing is a safety property, not a convenience:
 * with several queries in the buffer, the class MUST be computed from the
 * statement that will actually be sent, never from the whole buffer. See
 * `__tests__/sqlStatements.test.ts`.
 */

import { meetsRole, type WorkspaceRole } from "@/lib/workspace/roles"

// ExplorerStmtClass mirrors the api-gateway's validators.StatementClass. The UI uses it
// to gate the Run button and choose the destructive-confirm flow BEFORE the server
// re-checks. This is advisory only — validators.ValidateExplorerStatement on the backend
// is the source of truth and independently enforces the role gate.
export type ExplorerStmtClass = "read" | "write" | "ddl" | "destructive" | "blocked" | "unknown"

/** Mirrors validators.removeComments: drops `--` and `/* … *\/` comments that sit
 *  outside quoted text, so the classifier sees what the engine runs. A MySQL
 *  `/*! … *\/` executable comment (optionally version-gated, `/*!40101 …`) keeps its
 *  body — those engines execute it, so hiding it would classify code as nothing. */
export function removeSqlComments(sql: string): string {
  const src = String(sql || "")
  let out = ""
  let inSingle = false
  let inDouble = false
  let inLineComment = false
  let inBlockComment = false
  let inExecComment = false

  for (let i = 0; i < src.length; i++) {
    const ch = src[i]

    if (inLineComment) {
      if (ch === "\n") {
        inLineComment = false
        out += ch
      }
      continue
    }
    if (inBlockComment) {
      if (ch === "*" && src[i + 1] === "/") {
        inBlockComment = false
        i++
      }
      continue
    }
    if (inSingle) {
      out += ch
      if (ch === "'") {
        if (src[i + 1] === "'") {
          out += "'"
          i++
        } else {
          inSingle = false
        }
      }
      continue
    }
    if (inDouble) {
      out += ch
      if (ch === '"') {
        if (src[i + 1] === '"') {
          out += '"'
          i++
        } else {
          inDouble = false
        }
      }
      continue
    }

    if (ch === "-" && src[i + 1] === "-") {
      inLineComment = true
      i++
      continue
    }
    if (ch === "/" && src[i + 1] === "*") {
      if (src[i + 2] === "!") {
        i += 2
        while (i + 1 < src.length && src[i + 1] >= "0" && src[i + 1] <= "9") i++
        inExecComment = true
        continue
      }
      inBlockComment = true
      i++
      continue
    }
    if (inExecComment && ch === "*" && src[i + 1] === "/") {
      inExecComment = false
      i++
      continue
    }

    if (ch === "'") inSingle = true
    else if (ch === '"') inDouble = true
    out += ch
  }
  return out
}

/** Mirrors validators.stripStringLiterals: blanks the contents of '…', "…" and `…`
 *  (keeping the opening quote) so a verb inside a literal or quoted identifier is not
 *  read as a statement keyword. */
function stripStringLiterals(sql: string): string {
  let out = ""
  let quote = ""
  for (let i = 0; i < sql.length; i++) {
    const ch = sql[i]
    if (quote) {
      out += " "
      if (ch === quote) {
        if (quote !== "`" && sql[i + 1] === quote) {
          out += " "
          i++
        } else {
          quote = ""
        }
      }
      continue
    }
    if (ch === "'" || ch === '"' || ch === "`") quote = ch
    out += ch
  }
  return out
}

/** Uppercase leading SQL verb (e.g. "DROP"), or "" for empty/garbage input. Comments
 *  are removed first, so this names the same verb the classifier gated on — the
 *  destructive-confirm dialog asks the user to type it. */
export function firstSqlVerb(sql: string): string {
  return leadingVerb(removeSqlComments(sql))
}

/** Leading verb of text whose comments are ALREADY removed. Never strip twice:
 *  removing `/**\/` from `-/**\/-` leaves `--`, and a second pass would read that as
 *  a comment and surface a verb the engine never sees (mirrors the Go side). */
function leadingVerb(strippedSql: string): string {
  const m = strippedSql.trim().toUpperCase().match(/^[A-Z]+/)
  return m ? m[0] : ""
}

/** Matches validators.alterDropsObject — an ALTER carrying a DROP sub-clause
 *  (DROP COLUMN / CONSTRAINT / PARTITION / …). The leading verb is "ALTER", so a
 *  verb-only classifier reads it as ordinary DDL even though it destroys data
 *  irreversibly. Word-boundary match so identifiers like `drop_reason` don't trip it;
 *  over-matching is the safe direction (extra confirm prompt, never a silent drop).
 *  Tested against comment-stripped text: a DROP named only in a comment never runs. */
const ALTER_DROP_RE = /\bDROP\b/i

/** Mirrors validators.cteWriteVerbs, in the same descending-severity order so the
 *  first match wins. */
const CTE_WRITE_VERBS: ReadonlyArray<readonly [RegExp, ExplorerStmtClass]> = [
  [/\bDROP\b/i, "destructive"],
  [/\bTRUNCATE\b/i, "destructive"],
  [/\bCREATE\b/i, "ddl"],
  [/\bALTER\b/i, "ddl"],
  [/\bINSERT\b/i, "write"],
  [/\bUPDATE\b/i, "write"],
  [/\bDELETE\b/i, "write"],
  [/\bMERGE\b/i, "write"],
]

/** Mirrors validators.cteWriteClass. WITH claims to be a read, but the write can sit
 *  in a CTE body (`WITH gone AS (DELETE … RETURNING *) SELECT …`) or after the CTE
 *  list (`WITH s AS (…) MERGE INTO …`), so scan the whole statement. */
function cteWriteClass(strippedSql: string): ExplorerStmtClass {
  const scanned = stripStringLiterals(strippedSql)
  for (const [re, cls] of CTE_WRITE_VERBS) {
    if (re.test(scanned)) return cls
  }
  return "read"
}

/** Human label for the destructive warning + confirm dialog. The leading verb alone
 *  understates `ALTER … DROP COLUMN`, which reads as a routine "ALTER". */
export function destructiveLabel(sql: string): string {
  const verb = firstSqlVerb(sql)
  if (verb === "ALTER" && ALTER_DROP_RE.test(removeSqlComments(sql))) {
    return "ALTER … DROP"
  }
  return verb
}

/** Mirrors validators.ClassifyStatementSQL — classify the statement the engine will
 *  run (comments removed); reads stay SELECT/WITH unless a WITH carries a write;
 *  everything else maps to a write tier or is blocked. Pinned to the server by
 *  shared/explorer_statement_class_golden.json. */
export function classifyExplorerStatement(sql: string): ExplorerStmtClass {
  const stripped = removeSqlComments(sql).trim()
  switch (leadingVerb(stripped)) {
    case "SELECT":
      return "read"
    case "WITH":
      return cteWriteClass(stripped)
    case "INSERT":
    case "UPDATE":
    case "DELETE":
    case "MERGE":
      return "write"
    case "CREATE":
      return "ddl"
    case "ALTER":
      // ALTER … DROP COLUMN is as irreversible as DROP TABLE; classify by what the
      // statement does, not by the word it starts with.
      return ALTER_DROP_RE.test(stripped) ? "destructive" : "ddl"
    case "DROP":
    case "TRUNCATE":
      return "destructive"
    case "":
      return "unknown"
    case "GRANT":
    case "REVOKE":
    case "CALL":
    case "EXEC":
    case "EXECUTE":
    case "SET":
    case "COPY":
    case "VACUUM":
    case "ANALYZE":
    case "EXPLAIN":
    case "SHOW":
    case "DESCRIBE":
    case "DESC":
      return "blocked"
    default:
      return "unknown"
  }
}

/** Minimum workspace role for a statement class; null = blocked (no role may run it).
 *  Mirrors validators.classToMinRole. */
export function minRoleForStmtClass(cls: ExplorerStmtClass): WorkspaceRole | null {
  switch (cls) {
    case "read":
      return "viewer"
    case "write":
    case "ddl":
      return "admin"
    case "destructive":
    case "unknown":
      return "owner"
    case "blocked":
    default:
      return null
  }
}

/** Whether `role` may run `sql` from the Explorer (advisory; backend re-checks). */
export function canRunExplorerStatement(role: string, sql: string): boolean {
  const min = minRoleForStmtClass(classifyExplorerStatement(sql))
  return min !== null && meetsRole(role, min)
}
