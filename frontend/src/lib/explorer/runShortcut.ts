"use client"

/**
 * Decides what the Data Explorer's page-level Cmd/Ctrl+Enter fallback should do.
 *
 * This lives outside the page component for two reasons:
 *
 * 1. It is testable in isolation, and the behaviour it encodes was a live prod defect
 *    (see runShortcut.test.ts).
 * 2. The dependency type deliberately offers `attemptRunQuery` and NOT `executeQuery`,
 *    so the shortcut physically cannot reach the database without first passing the
 *    destructive-statement confirmation. That used to be a convention; now it's a type.
 *
 * The defect: the SqlEditor's Mod-Enter keymap and the NL textarea's onKeyDown both call
 * preventDefault, but neither stops the event bubbling to the page root — so one
 * keystroke ran two handlers. For reads that was merely wasteful (the second call aborts
 * the first request in flight), but the root handler called executeQuery directly,
 * skipping the confirm dialog. On a DROP, the editor path stops at the dialog without
 * issuing a request, leaving nothing to abort — so the root handler's call went straight
 * through and a DROP the user *declined* still dropped the table.
 */

export interface ExplorerRunShortcutDeps {
  /** True when the natural-language box has content (NL generation wins over raw SQL). */
  hasNlInput: boolean
  /** True when the SQL editor has content. */
  hasSql: boolean
  generateSQL: () => void
  /**
   * Routes through the destructive-statement gate. Intentionally not `executeQuery`:
   * the fallback path must never be able to run DROP/TRUNCATE unconfirmed.
   */
  attemptRunQuery: () => void
}

/** What the handler did — returned for tests and for callers that want to log. */
export type ExplorerRunShortcutOutcome =
  | "ignored"
  | "already-handled"
  | "generate"
  | "run"

export interface ExplorerKeyboardEvent {
  metaKey: boolean
  ctrlKey: boolean
  key: string
  defaultPrevented: boolean
  preventDefault: () => void
}

export function handleExplorerRunShortcut(
  e: ExplorerKeyboardEvent,
  deps: ExplorerRunShortcutDeps
): ExplorerRunShortcutOutcome {
  // A control that owns this shortcut already claimed the keystroke. Both the SqlEditor
  // keymap and the NL textarea set preventDefault before this bubbles up.
  if (e.defaultPrevented) return "already-handled"

  if (!(e.metaKey || e.ctrlKey) || e.key !== "Enter") return "ignored"

  e.preventDefault()

  if (deps.hasNlInput) {
    deps.generateSQL()
    return "generate"
  }
  if (deps.hasSql) {
    deps.attemptRunQuery()
    return "run"
  }
  return "ignored"
}
