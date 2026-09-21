"use client"

import { useEffect, useState } from "react"
import { toast } from "sonner"
import { AlertTriangle, Loader2 } from "lucide-react"
import { authFetch } from "@/lib/api/auth-fetch"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { formatSpan } from "@/components/explorer/liveState"

// "Alert if older than…" on the model page. The gateway stores the deadline in
// saved_queries.freshness_deadline_seconds (PUT /explorer/saved/:id/freshness,
// saved_query_freshness.go SetSavedQueryFreshness) and the sweep opens a breach when the
// last successful rebuild is older than it. Breaches already show on this page
// (OverdueBadge); this is the control that sets the promise they are measured against.

/** saved_query_freshness.go freshnessDeadlineMin / freshnessDeadlineMax, in seconds. */
export const FRESHNESS_MIN_SECONDS = 60
export const FRESHNESS_MAX_SECONDS = 365 * 86400

const UNITS = [
  { value: "minutes", label: "minutes", seconds: 60 },
  { value: "hours", label: "hours", seconds: 3600 },
  { value: "days", label: "days", seconds: 86400 },
] as const
type Unit = (typeof UNITS)[number]["value"]

const PRESETS: { label: string; seconds: number }[] = [
  { label: "1 hour", seconds: 3600 },
  { label: "6 hours", seconds: 6 * 3600 },
  { label: "12 hours", seconds: 12 * 3600 },
  { label: "1 day", seconds: 86400 },
  { label: "7 days", seconds: 7 * 86400 },
]

const unitSeconds = (u: Unit) => UNITS.find((x) => x.value === u)!.seconds

/** The largest unit that states a deadline exactly: 21600 → 6 hours, 5400 → 90 minutes. */
export function splitDeadline(seconds: number): { amount: string; unit: Unit } {
  if (seconds % 86400 === 0) return { amount: String(seconds / 86400), unit: "days" }
  if (seconds % 3600 === 0) return { amount: String(seconds / 3600), unit: "hours" }
  return { amount: String(Math.round((seconds / 60) * 100) / 100), unit: "minutes" }
}

/** The deadline an amount and unit ask for, or why it cannot be saved. */
export function deadlineFrom(amount: string, unit: Unit): { seconds: number } | { error: string } {
  const n = Number(amount.trim())
  if (amount.trim() === "" || !Number.isFinite(n) || n <= 0) return { error: "Enter a number greater than zero." }
  const seconds = Math.round(n * unitSeconds(unit))
  if (seconds < FRESHNESS_MIN_SECONDS || seconds > FRESHNESS_MAX_SECONDS) {
    return { error: "A deadline must be between 1 minute and 365 days." }
  }
  return { seconds }
}

// Tagged with the id it was read for, so a stale answer reads as loading, not as this model's.
type Loaded = { id: string; status: "ok"; deadline: number | null } | { id: string; status: "error" }

export function ModelFreshnessDeadline({
  savedQueryId,
  materialization,
  canEdit,
  disabledReason,
  onSaved,
}: {
  savedQueryId: string
  /** saved_queries.materialization: the sweep only checks models that rebuild a table. */
  materialization?: string
  canEdit: boolean
  /** Shown as the Edit control's tooltip when canEdit is false. */
  disabledReason?: string
  /** After a save lands, so the page can re-read breaches and its Updated time. */
  onSaved?: () => void
}) {
  const [answer, setAnswer] = useState<Loaded | null>(null)
  const [open, setOpen] = useState(false)
  // Bumped on every opening: the dialog remounts, so its form starts from what is stored.
  const [openings, setOpenings] = useState(0)

  useEffect(() => {
    let alive = true
    ;(async () => {
      try {
        const res = await authFetch(`/api/v1/explorer/saved/${encodeURIComponent(savedQueryId)}`, {
          cache: "no-store",
        })
        if (!res.ok) throw new Error(`HTTP ${res.status}`)
        const data = await res.json()
        const v = data?.freshness_deadline_seconds
        if (alive) setAnswer({ id: savedQueryId, status: "ok", deadline: typeof v === "number" ? v : null })
      } catch {
        if (alive) setAnswer({ id: savedQueryId, status: "error" })
      }
    })()
    return () => {
      alive = false
    }
  }, [savedQueryId])

  const loaded = answer?.id === savedQueryId ? answer : { status: "loading" as const }
  const tracked = materialization === "table"
  const deadline = loaded.status === "ok" ? loaded.deadline : null

  return (
    <div className="flex flex-wrap items-center gap-x-1.5 gap-y-0.5" data-testid="model-freshness-deadline">
      {loaded.status === "loading" ? (
        <span className="text-zinc-400">Loading…</span>
      ) : loaded.status === "error" ? (
        <span className="text-zinc-400">Could not load the deadline</span>
      ) : deadline != null ? (
        <>
          <span title={`${deadline.toLocaleString()} seconds`}>Alert if older than {formatSpan(deadline)}</span>
          {!tracked && (
            <span
              className="text-amber-600 dark:text-amber-400"
              title="The freshness check only watches models that rebuild a table"
            >
              · not checked
            </span>
          )}
        </>
      ) : (
        <span className="text-zinc-400">No deadline</span>
      )}
      {loaded.status === "ok" && (
        <button
          type="button"
          className="text-blue-600 hover:underline disabled:cursor-not-allowed disabled:text-zinc-400 disabled:no-underline dark:text-blue-400"
          onClick={() => {
            setOpenings((n) => n + 1)
            setOpen(true)
          }}
          disabled={!canEdit}
          title={canEdit ? undefined : disabledReason}
          aria-label={deadline != null ? "Edit freshness deadline" : "Set freshness deadline"}
        >
          {deadline != null ? "Edit" : "Set"}
        </button>
      )}
      {loaded.status === "ok" && (
        <FreshnessDeadlineDialog
          key={openings}
          open={open}
          onOpenChange={setOpen}
          savedQueryId={savedQueryId}
          current={deadline}
          tracked={tracked}
          onSaved={(next) => {
            setAnswer({ id: savedQueryId, status: "ok", deadline: next })
            setOpen(false)
            onSaved?.()
          }}
        />
      )}
    </div>
  )
}

function FreshnessDeadlineDialog({
  open,
  onOpenChange,
  savedQueryId,
  current,
  tracked,
  onSaved,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  savedQueryId: string
  current: number | null
  tracked: boolean
  onSaved: (deadline: number | null) => void
}) {
  // Keyed by opening, so these start from what is stored, not from an abandoned edit.
  const [initial] = useState(() => splitDeadline(current ?? 6 * 3600))
  const [amount, setAmount] = useState(initial.amount)
  const [unit, setUnit] = useState<Unit>(initial.unit)
  const [busy, setBusy] = useState<"save" | "remove" | null>(null)
  const [serverError, setServerError] = useState<string | null>(null)

  const parsed = deadlineFrom(amount, unit)
  const unchanged = "seconds" in parsed && parsed.seconds === current

  async function put(deadlineSeconds: number | null) {
    setBusy(deadlineSeconds == null ? "remove" : "save")
    setServerError(null)
    try {
      const res = await authFetch(`/api/v1/explorer/saved/${encodeURIComponent(savedQueryId)}/freshness`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ deadline_seconds: deadlineSeconds }),
      })
      const data = await res.json().catch(() => null)
      if (!res.ok) {
        setServerError(data?.error || `The deadline was not saved (HTTP ${res.status}).`)
        return
      }
      const stored = typeof data?.deadline_seconds === "number" ? data.deadline_seconds : null
      toast.success(stored == null ? "Freshness deadline removed" : `Alert if older than ${formatSpan(stored)}`)
      onSaved(stored)
    } catch {
      setServerError("The deadline was not saved: the request did not reach the server.")
    } finally {
      setBusy(null)
    }
  }

  return (
    <Dialog open={open} onOpenChange={(o) => !busy && onOpenChange(o)}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Freshness deadline</DialogTitle>
          <DialogDescription>
            Flag this model Overdue when its table has not been rebuilt successfully for longer than this.
            Checked every minute; the flag shows here and on Scheduled Queries.
          </DialogDescription>
        </DialogHeader>

        {!tracked && (
          <div
            role="note"
            className="flex gap-2 rounded-md border border-amber-200 bg-amber-50 p-2 text-xs text-amber-800 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-300"
          >
            <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
            <span>
              This model does not rebuild a table, so a deadline is saved but not checked until a run writes one.
            </span>
          </div>
        )}

        <form
          className="space-y-3"
          onSubmit={(e) => {
            e.preventDefault()
            if ("seconds" in parsed && !unchanged) void put(parsed.seconds)
          }}
        >
          <div className="flex flex-wrap gap-1.5" role="group" aria-label="Common deadlines">
            {PRESETS.map((p) => {
              const on = "seconds" in parsed && parsed.seconds === p.seconds
              return (
                <Button
                  key={p.label}
                  type="button"
                  size="sm"
                  variant={on ? "default" : "outline"}
                  aria-pressed={on}
                  className="h-7 px-2.5 text-xs"
                  onClick={() => {
                    const next = splitDeadline(p.seconds)
                    setAmount(next.amount)
                    setUnit(next.unit)
                    setServerError(null)
                  }}
                >
                  {p.label}
                </Button>
              )
            })}
          </div>

          <div className="space-y-1.5">
            <Label htmlFor="freshness-amount">Alert if older than</Label>
            <div className="flex gap-2">
              <Input
                id="freshness-amount"
                type="number"
                inputMode="decimal"
                min={0}
                step="any"
                className="w-28"
                value={amount}
                onChange={(e) => {
                  setAmount(e.target.value)
                  setServerError(null)
                }}
                aria-invalid={"error" in parsed}
                aria-describedby="freshness-hint"
              />
              <select
                aria-label="Unit"
                className="h-9 rounded-md border border-zinc-300 bg-white px-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 dark:border-zinc-700 dark:bg-zinc-900"
                value={unit}
                onChange={(e) => {
                  setUnit(e.target.value as Unit)
                  setServerError(null)
                }}
              >
                {UNITS.map((u) => (
                  <option key={u.value} value={u.value}>
                    {u.label}
                  </option>
                ))}
              </select>
            </div>
            <p id="freshness-hint" className={"error" in parsed ? "text-xs text-red-600" : "text-xs text-zinc-500 dark:text-zinc-400"}>
              {"error" in parsed
                ? parsed.error
                : current != null && parsed.seconds > current
                  ? "Widening the deadline closes an open alert as “deadline widened”, not as fixed."
                  : "Between 1 minute and 365 days."}
            </p>
          </div>

          {serverError && (
            <p role="alert" className="text-xs text-red-600">
              {serverError}
            </p>
          )}

          <DialogFooter className="gap-2 sm:justify-between">
            {current != null ? (
              <Button
                type="button"
                variant="ghost"
                className="text-red-600 hover:text-red-700"
                disabled={!!busy}
                onClick={() => void put(null)}
              >
                {busy === "remove" && <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" aria-hidden />}
                Remove deadline
              </Button>
            ) : (
              <span />
            )}
            <div className="flex gap-2">
              <Button type="button" variant="outline" disabled={!!busy} onClick={() => onOpenChange(false)}>
                Cancel
              </Button>
              <Button type="submit" disabled={!!busy || "error" in parsed || unchanged}>
                {busy === "save" && <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" aria-hidden />}
                Save
              </Button>
            </div>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}
