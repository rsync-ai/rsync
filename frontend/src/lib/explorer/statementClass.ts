/**
 * Client-side mirror of the api-gateway's statement classifier
 * (`validators.ClassifyStatement` / `classToMinRole`).
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

/** Uppercase leading SQL verb (e.g. "DROP"), or "" for empty/garbage input. */
export function firstSqlVerb(sql: string): string {
  const m = String(sql || "").trim().toUpperCase().match(/^[A-Z]+/)
  return m ? m[0] : ""
}

/** Matches validators.alterDropsObject — an ALTER carrying a DROP sub-clause
 *  (DROP COLUMN / CONSTRAINT / PARTITION / …). The leading verb is "ALTER", so a
 *  verb-only classifier reads it as ordinary DDL even though it destroys data
 *  irreversibly. Word-boundary match so identifiers like `drop_reason` don't trip it;
 *  over-matching is the safe direction (extra confirm prompt, never a silent drop). */
const ALTER_DROP_RE = /\bDROP\b/

/** Human label for the destructive warning + confirm dialog. The leading verb alone
 *  understates `ALTER … DROP COLUMN`, which reads as a routine "ALTER". */
export function destructiveLabel(sql: string): string {
  const verb = firstSqlVerb(sql)
  if (verb === "ALTER" && ALTER_DROP_RE.test(String(sql || "").toUpperCase())) {
    return "ALTER … DROP"
  }
  return verb
}

/** Mirrors validators.ClassifyStatement — reads stay SELECT/WITH; everything else maps
 *  to a write tier or is blocked. */
export function classifyExplorerStatement(sql: string): ExplorerStmtClass {
  switch (firstSqlVerb(sql)) {
    case "SELECT":
    case "WITH":
      return "read"
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
      return ALTER_DROP_RE.test(String(sql || "").toUpperCase()) ? "destructive" : "ddl"
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
