"use client"

/**
 * "Share to Slack" for a result set — the UI for POST
 * /api/v1/explorer/share/slack, which shipped with no caller.
 *
 * Two deliberate choices, both explained on screen rather than only here:
 *
 *  1. The webhook URL is NOT saved. It is a bearer credential for a channel,
 *     and this dialog keeps it in component state that dies on close. See the
 *     header of lib/explorer/slackShare.ts.
 *
 *  2. Rows are opt-in. The default message carries the question, the SQL, the
 *     row count and the column names — enough for a colleague to re-run it —
 *     and attaching actual values is a separate, checked decision, because a
 *     channel is a wider and more permanent audience than a query window.
 *
 * The sibling email endpoint is intentionally absent: ShareViaEmail needs
 * operator SMTP configured on the server, so it is not a frontend-only gap.
 */

import { useMemo, useState } from "react"
import { toast } from "sonner"
import { Loader2, Send, Share2 } from "lucide-react"

import {
  buildSlackMessage,
  formatRowPreview,
  shareToSlack,
  SLACK_ROW_PREVIEW_LIMIT,
  validateWebhookUrl,
} from "@/lib/explorer/slackShare"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

export function ShareToSlackDialog({
  question,
  sql,
  columns,
  rows,
  rowCount,
  executionMs,
  truncated,
  disabled,
}: {
  question: string
  sql: string
  columns: string[]
  rows: Record<string, unknown>[]
  rowCount: number
  executionMs?: number
  truncated?: boolean
  disabled?: boolean
}) {
  const [open, setOpen] = useState(false)
  const [webhook, setWebhook] = useState("")
  const [includeRows, setIncludeRows] = useState(false)
  const [sending, setSending] = useState(false)

  const close = (next: boolean) => {
    setOpen(next)
    if (!next) {
      // Drop the credential the moment the dialog closes.
      setWebhook("")
      setIncludeRows(false)
    }
  }

  const message = useMemo(
    () =>
      buildSlackMessage({
        question,
        sql,
        rowCount,
        columns,
        executionMs,
        truncated,
        rows,
        includeRows,
      }),
    [columns, executionMs, includeRows, question, rowCount, rows, sql, truncated]
  )

  const rowPreview = useMemo(
    () => (rows.length > 0 ? formatRowPreview(columns, rows) : ""),
    [columns, rows]
  )

  const send = async () => {
    const invalid = validateWebhookUrl(webhook)
    if (invalid) {
      toast.error(invalid)
      return
    }
    setSending(true)
    try {
      const out = await shareToSlack({ webhookUrl: webhook, message })
      if (out.ok) {
        toast.success("Sent to Slack.")
        close(false)
      } else {
        toast.error(out.error)
      }
    } finally {
      setSending(false)
    }
  }

  return (
    <>
      {/* The trigger lives OUTSIDE <Dialog>: this Dialog renders null while
          closed (ui/dialog.tsx:84), so a trigger nested inside it would never
          be on screen to open it. */}
      <Button variant="outline" size="sm" disabled={disabled} onClick={() => setOpen(true)}>
        <Share2 className="h-4 w-4 mr-2" />
        Share to Slack
      </Button>

      <Dialog open={open} onOpenChange={close}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Share to Slack</DialogTitle>
            <DialogDescription>
              Posts this result to a Slack channel through an incoming webhook.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4">
            <div className="space-y-1">
              <Label htmlFor="slack-webhook">Incoming webhook URL</Label>
              <Input
                id="slack-webhook"
                placeholder="https://hooks.slack.com/services/…"
                value={webhook}
                onChange={(e) => setWebhook(e.target.value)}
                disabled={sending}
                autoComplete="off"
                spellCheck={false}
              />
              <p className="text-xs text-muted-foreground">
                Not saved anywhere. Anyone holding this URL can post to that channel, so it is kept only until this
                dialog closes — paste it again next time, or set up a stored Slack integration.
              </p>
            </div>

            <div className="space-y-2">
              <Label className="text-sm font-medium">What gets posted</Label>
              <pre className="max-h-40 overflow-auto rounded border border-zinc-200 bg-zinc-50 p-3 text-xs dark:border-zinc-800 dark:bg-zinc-900/60">
                {message.text}
                {"\n\n"}
                {sql.trim() || "(no SQL)"}
              </pre>
            </div>

            <div className="space-y-2">
              <label className="flex items-start gap-2 text-sm">
                <Checkbox
                  checked={includeRows}
                  onCheckedChange={(v) => setIncludeRows(v === true)}
                  disabled={sending || rows.length === 0}
                  aria-label="Include the first rows"
                />
                <span>
                  Also include the first {Math.min(SLACK_ROW_PREVIEW_LIMIT, rows.length)}{" "}
                  {rows.length === 1 ? "row" : "rows"}
                  <span className="block text-xs text-muted-foreground">
                    Off by default. The values go into the channel and stay in its history — check this only if
                    everyone who can read that channel may read this data.
                  </span>
                </span>
              </label>

              {includeRows && rowPreview && (
                <pre className="max-h-40 overflow-auto rounded border border-amber-200 bg-amber-50 p-3 text-xs text-amber-900 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-200">
                  {rowPreview}
                </pre>
              )}
            </div>
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => close(false)} disabled={sending}>
              Cancel
            </Button>
            <Button onClick={() => void send()} disabled={sending || !webhook.trim()}>
              {sending ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Send className="mr-2 h-4 w-4" />}
              Send to Slack
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
