/**
 * Client for POST /api/v1/explorer/share/slack — registered at main.go:1211,
 * implemented at explorer.go:3973, and called from nowhere. The explorer used
 * to advertise "Share via Slack" as a next-step suggestion with no control
 * behind it; that dead suggestion is gone, and this is the control.
 *
 * Frontend-only on purpose: the handler takes the webhook URL per request, so
 * nothing has to be configured on the server. (The sibling email endpoint is
 * NOT frontend-only — ShareViaEmail needs operator SMTP — so it is out of
 * scope here rather than half-wired.)
 *
 * THE WEBHOOK IS A CREDENTIAL. Anyone holding it can post into that channel
 * indefinitely. It is therefore never persisted — not to localStorage, not to
 * the workspace, not to a saved query. It lives in component state for the
 * duration of the dialog and is gone when the dialog closes. If that feels
 * inconvenient, the right fix is a stored Slack integration on the server, not
 * a browser cache of a bearer secret.
 *
 * WHAT GETS SENT. Unlike the LLM paths, this is not bound by the metadata-only
 * rule — Slack is a destination the operator chose, and sharing numbers is the
 * point. But the default is still metadata (question, SQL, row count, column
 * names), because a channel is a wider audience than a query window and
 * attaching rows should be a deliberate act. `includeRows` is that act.
 */

import { authFetch } from "@/lib/api/auth-fetch"
import { formatDuration } from "@/lib/duration"

export const SLACK_WEBHOOK_PREFIX = "https://hooks.slack.com/"

/** How many rows an opt-in preview attaches, and how wide each cell may be. */
export const SLACK_ROW_PREVIEW_LIMIT = 10
const CELL_MAX = 48
/** Slack rejects a section whose text exceeds 3000 characters. */
const SECTION_MAX = 2900

/**
 * Mirrors the gateway's check at explorer.go:3986 so a typo costs no
 * round-trip. The prefix is also the only thing standing between this endpoint
 * and an arbitrary server-side POST, so it is matched exactly, not loosely.
 */
export function validateWebhookUrl(url: string): string | null {
  const u = url.trim()
  if (!u) return "Paste the Slack incoming-webhook URL."
  if (!u.startsWith(SLACK_WEBHOOK_PREFIX)) {
    return `A Slack webhook URL starts with ${SLACK_WEBHOOK_PREFIX} — this one does not.`
  }
  return null
}

function cell(v: unknown): string {
  if (v === null || v === undefined) return "NULL"
  const s = typeof v === "object" ? JSON.stringify(v) : String(v)
  return s.length > CELL_MAX ? `${s.slice(0, CELL_MAX - 1)}…` : s
}

/**
 * Fixed-width preview of the first rows. Plain text inside a Slack code fence:
 * Slack has no table primitive, and a bulleted list of key=value loses the
 * column alignment that makes a result set readable at a glance.
 */
export function formatRowPreview(
  columns: string[],
  rows: Record<string, unknown>[],
  limit = SLACK_ROW_PREVIEW_LIMIT
): string {
  const cols = columns.slice(0, 8)
  const shown = rows.slice(0, limit)
  const widths = cols.map((c, i) =>
    Math.max(c.length, ...shown.map((r) => cell(r[cols[i]]).length), 3)
  )
  const line = (cells: string[]) => cells.map((c, i) => c.padEnd(widths[i])).join("  ").trimEnd()

  const out = [line(cols), line(widths.map((w) => "-".repeat(w)))]
  for (const r of shown) out.push(line(cols.map((c) => cell(r[c]))))
  if (columns.length > cols.length) out.push(`… and ${columns.length - cols.length} more columns`)
  if (rows.length > shown.length) out.push(`… and ${rows.length - shown.length} more rows`)
  return out.join("\n")
}

export type SlackMessageArgs = {
  question: string
  sql: string
  rowCount: number
  columns: string[]
  executionMs?: number
  truncated?: boolean
  rows?: Record<string, unknown>[]
  includeRows: boolean
  sharedBy?: string
}

export type SlackMessage = { text: string; blocks: Array<Record<string, unknown>> }

/**
 * Build the Slack payload. `text` is the notification fallback — it is what
 * shows in the sidebar and in a push notification, so it carries the headline
 * rather than repeating the whole message.
 */
export function buildSlackMessage(args: SlackMessageArgs): SlackMessage {
  const title = args.question.trim() || "SQL query result"
  const rowsWord = args.rowCount === 1 ? "row" : "rows"
  const headline = `${title} — ${args.rowCount} ${rowsWord}`

  const facts: string[] = [`*${args.rowCount}* ${rowsWord}`]
  // The shared formatter, never a local one: src/__tests__/one-duration-formatter.test.ts
  // fails the build on a second copy, because every copy has eventually disagreed.
  if (typeof args.executionMs === "number") facts.push(formatDuration(args.executionMs))
  if (args.truncated) facts.push("_result was truncated_")
  if (args.columns.length > 0) {
    facts.push(`columns: ${args.columns.slice(0, 12).join(", ")}${args.columns.length > 12 ? ", …" : ""}`)
  }

  const blocks: Array<Record<string, unknown>> = [
    { type: "section", text: { type: "mrkdwn", text: `*${title}*` } },
    { type: "section", text: { type: "mrkdwn", text: facts.join("  ·  ") } },
  ]

  const sql = args.sql.trim()
  if (sql) {
    const clipped = sql.length > SECTION_MAX ? `${sql.slice(0, SECTION_MAX)}\n-- …truncated` : sql
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "```sql\n" + clipped + "\n```" } })
  }

  if (args.includeRows && args.rows && args.rows.length > 0) {
    const preview = formatRowPreview(args.columns, args.rows)
    const clipped = preview.length > SECTION_MAX ? `${preview.slice(0, SECTION_MAX)}\n…` : preview
    blocks.push({ type: "section", text: { type: "mrkdwn", text: "```\n" + clipped + "\n```" } })
  }

  const by = args.sharedBy?.trim()
  blocks.push({
    type: "context",
    elements: [{ type: "mrkdwn", text: by ? `Shared from rsync.ai Data Explorer by ${by}` : "Shared from rsync.ai Data Explorer" }],
  })

  return { text: headline, blocks }
}

export type SlackShareResult = { ok: true } | { ok: false; error: string }

/**
 * Post the message. The gateway's failure modes are distinct and worth keeping
 * apart: 400 is a bad URL shape, 503 is "we could not reach Slack at all", and
 * 502 means Slack itself refused — usually `no_service`, which is what a
 * revoked or deleted webhook returns.
 */
export async function shareToSlack(args: {
  webhookUrl: string
  message: SlackMessage
}): Promise<SlackShareResult> {
  const invalid = validateWebhookUrl(args.webhookUrl)
  if (invalid) return { ok: false, error: invalid }

  let res: Response
  try {
    res = await authFetch("/api/v1/explorer/share/slack", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        webhook_url: args.webhookUrl.trim(),
        text: args.message.text,
        blocks: args.message.blocks,
      }),
    })
  } catch {
    return { ok: false, error: "The API is unreachable — nothing was sent." }
  }

  const data = (await res.json().catch(() => null)) as
    | { error?: string; details?: string; status?: number }
    | null

  if (!res.ok) {
    if (res.status === 502) {
      const details = String(data?.details || "").trim()
      return {
        ok: false,
        error:
          details === "no_service"
            ? "Slack rejected the webhook (no_service) — it has been revoked or deleted. Create a new incoming webhook."
            : `Slack refused the message${details ? `: ${details}` : ""}.`,
      }
    }
    return { ok: false, error: data?.error || `Could not send to Slack (HTTP ${res.status}).` }
  }

  return { ok: true }
}
