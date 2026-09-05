"use client"

import { useCallback, useEffect, useMemo, useState } from "react"
import { toast } from "sonner"
import { AlertTriangle, Check, Clock, History, Loader2, RotateCcw, X } from "lucide-react"

import { authFetch } from "@/lib/api/auth-fetch"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { useCurrentUser } from "@/contexts/CurrentUserContext"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"

import { SqlDiffView } from "./SqlDiffView"
import { updateSavedQuery } from "./savedQueryUpdate"
import { formatAbsoluteTime } from "./scheduleTime"

// Every edit to a saved query has snapshotted the previous text since migration 084,
// and until now nothing read those snapshots — GET /explorer/saved/:id/versions had no
// caller in the app at all. The history existed and was invisible, which is the same
// as not having it the moment somebody asks "what did this table's query used to say?"
//
// This panel is that answer, and it carries the review of a proposed edit too. Those
// belong on one surface: an open proposal IS the next version, just not yet, and an
// admin deciding whether to approve one wants the same diff view and the same list of
// who-changed-what-when that the history gives.

interface SavedQueryHistoryDialogProps {
  savedQueryId: string
  savedQueryName: string
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Refetch the list behind this — approving or restoring changes the live SQL. */
  onChanged: () => void
}

interface Version {
  version: number
  name: string
  sql_text: string
  statement_class: string
  changed_by: string
  created_at: string
}

interface CurrentText {
  name: string
  sql_text: string
  statement_class: string
  updated_at: string
}

interface PendingEdit {
  id: string
  sql_text: string
  statement_class: string
  note?: string
  status: string
  proposed_by: string
  proposed_at: string
  stale: boolean
}

export function SavedQueryHistoryDialog({
  savedQueryId,
  savedQueryName,
  open,
  onOpenChange,
  onChanged,
}: SavedQueryHistoryDialogProps) {
  const { user } = useCurrentUser()
  const { meets, activeWorkspace, role } = useWorkspaceRole()

  const [loading, setLoading] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [versions, setVersions] = useState<Version[]>([])
  const [current, setCurrent] = useState<CurrentText | null>(null)
  const [pending, setPending] = useState<PendingEdit | null>(null)
  const [selected, setSelected] = useState<number | null>(null)
  const [authors, setAuthors] = useState<Record<string, string>>({})
  const [busy, setBusy] = useState<"approve" | "reject" | "restore" | null>(null)
  const [reviewNote, setReviewNote] = useState("")

  const load = useCallback(async () => {
    setLoading(true)
    setLoadError(null)
    try {
      const res = await authFetch(`/api/v1/explorer/saved/${savedQueryId}/versions`, {
        cache: "no-store",
      })
      if (!res.ok) {
        setLoadError(`Could not load the history for this query (HTTP ${res.status}).`)
        return
      }
      const data = (await res.json()) as {
        versions?: Version[]
        current?: CurrentText
        pending_edit?: PendingEdit | null
      }
      setVersions(Array.isArray(data.versions) ? data.versions : [])
      setCurrent(data.current ?? null)
      setPending(data.pending_edit ?? null)
      setSelected((prev) => {
        // Keep the reader where they were across a refetch, unless that version is
        // gone. Snapping back to the newest after every approve is disorienting when
        // the whole task is comparing two specific versions.
        const list = Array.isArray(data.versions) ? data.versions : []
        if (prev !== null && list.some((v) => v.version === prev)) return prev
        return list.length > 0 ? list[0].version : null
      })
    } catch {
      setLoadError("Could not reach the server to load this query's history.")
    } finally {
      setLoading(false)
    }
  }, [savedQueryId])

  // Names for the user IDs the history is written in. Best effort and deliberately
  // separate from the load above: a roster that fails to fetch costs readable names,
  // and failing the whole panel over that would be trading the thing asked for
  // against a nicety.
  const loadAuthors = useCallback(async () => {
    const wsId = activeWorkspace?.id
    if (!wsId) return
    try {
      const res = await authFetch(`/api/v1/workspaces/${wsId}/members`, { cache: "no-store" })
      if (!res.ok) return
      const data = (await res.json()) as { members?: Array<{ user_id?: string; email?: string }> }
      const map: Record<string, string> = {}
      for (const m of data.members ?? []) {
        if (m.user_id && m.email) map[m.user_id] = m.email
      }
      setAuthors(map)
    } catch {
      // Leaves `authors` empty; describeAuthor falls back to "you" / "another member".
    }
  }, [activeWorkspace?.id])

  useEffect(() => {
    if (!open) return
    void load()
    void loadAuthors()
  }, [open, load, loadAuthors])

  /**
   * Who a user ID belongs to, in the reader's terms.
   *
   * "you" is the one that carries weight: it is how an admin notices they are about
   * to approve their own proposal. Unknown IDs say "a former member" rather than
   * showing a raw UUID — 092 keeps no foreign key precisely so a proposal outlives
   * the account that made it, so this is an expected case, not a lookup failure.
   */
  const describeAuthor = useCallback(
    (id: string) => {
      if (!id) return "someone"
      if (user?.id && id === user.id) return "you"
      return authors[id] ?? "a former member"
    },
    [authors, user?.id],
  )

  const canReview = meets("admin")
  const selectedVersion = useMemo(
    () => versions.find((v) => v.version === selected) ?? null,
    [versions, selected],
  )

  const review = async (approve: boolean) => {
    if (!pending) return
    setBusy(approve ? "approve" : "reject")
    try {
      const res = await authFetch(
        `/api/v1/explorer/saved/${savedQueryId}/pending/${approve ? "approve" : "reject"}`,
        { method: "POST", body: JSON.stringify({ note: reviewNote.trim() }) },
      )
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(data?.error || `Could not ${approve ? "approve" : "reject"} this edit.`)
        return
      }
      toast.success(
        approve
          ? "Approved. The schedule now runs the new SQL."
          : "Rejected. The query is unchanged, and the proposal is kept in its history.",
      )
      setReviewNote("")
      await load()
      onChanged()
    } catch {
      toast.error(`Could not ${approve ? "approve" : "reject"} this edit.`)
    } finally {
      setBusy(null)
    }
  }

  const restore = async () => {
    if (!selectedVersion) return
    setBusy("restore")
    try {
      // Routed through the ordinary edit path rather than a rewind endpoint. Two
      // things follow from that, and both are the point: restoring snapshots the
      // text it replaced, so the history stays append-only and a restore is itself
      // undoable; and a restore to a SCHEDULED query goes through the same approval
      // gate as any other SQL change, because "put the old SQL back" alters what
      // runs unattended exactly as much as any other edit does.
      const result = await updateSavedQuery(savedQueryId, {
        sql_text: selectedVersion.sql_text,
        note: `Restore of version ${selectedVersion.version}.`,
      })
      if (result.kind === "error") {
        toast.error(result.message)
        return
      }
      if (result.kind === "proposed") {
        toast.success(`Restore of version ${selectedVersion.version} submitted for approval.`, {
          description: result.reason,
        })
      } else {
        toast.success(`Restored version ${selectedVersion.version}.`)
      }
      await load()
      onChanged()
    } finally {
      setBusy(null)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <History className="h-4 w-4" />
            History of &ldquo;{savedQueryName}&rdquo;
          </DialogTitle>
          <DialogDescription>
            Every edit keeps the text it replaced. Compare any earlier version with what
            this query runs today, and put an earlier one back.
          </DialogDescription>
        </DialogHeader>

        {loading ? (
          <div className="flex items-center gap-2 py-8 text-sm text-zinc-500">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading history…
          </div>
        ) : loadError ? (
          <div className="flex flex-col items-start gap-2 py-6">
            <p className="text-sm text-red-600 dark:text-red-400">{loadError}</p>
            <p className="text-xs text-zinc-500">The query itself has not been changed.</p>
            <Button size="sm" variant="outline" onClick={() => void load()}>
              Retry
            </Button>
          </div>
        ) : (
          <div className="max-h-[65vh] space-y-5 overflow-y-auto pr-1">
            {pending && current && (
              <section className="rounded-lg border border-amber-500/50 bg-amber-500/5 p-3">
                <div className="mb-2 flex flex-wrap items-center gap-2">
                  <Clock className="h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
                  <h3 className="text-sm font-medium">Waiting for approval</h3>
                  <span className="text-xs text-zinc-500">
                    proposed by {describeAuthor(pending.proposed_by)}
                    {pending.proposed_at ? ` · ${formatAbsoluteTime(pending.proposed_at)}` : ""}
                  </span>
                </div>

                <p className="mb-2 text-xs text-zinc-600 dark:text-zinc-400">
                  This query runs on a schedule, so the SQL below is not running yet. The
                  schedule keeps using the approved SQL until an admin approves it.
                </p>

                {pending.note && (
                  <p className="mb-2 rounded-md border bg-white/60 p-2 text-xs italic text-zinc-700 dark:bg-zinc-900/60 dark:text-zinc-300">
                    &ldquo;{pending.note}&rdquo;
                  </p>
                )}

                {pending.stale && (
                  <div className="mb-2 flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-2">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
                    <p className="text-xs text-amber-700 dark:text-amber-300">
                      The query changed after this was proposed, so this is no longer the
                      change its author wrote. Approving replaces the current SQL with the
                      text below — including undoing whatever landed in between. The diff
                      shown here is what approving would actually do.
                    </p>
                  </div>
                )}

                <SqlDiffView
                  base={current.sql_text}
                  next={pending.sql_text}
                  baseLabel="Running now"
                  nextLabel="Proposed"
                  className="mb-3"
                />

                {canReview ? (
                  <div className="space-y-2">
                    <div className="space-y-1.5">
                      <Label htmlFor="review-note" className="text-xs">
                        Note for the proposer (optional)
                      </Label>
                      <Textarea
                        id="review-note"
                        value={reviewNote}
                        onChange={(e) => setReviewNote(e.target.value)}
                        placeholder="Why you approved, or what needs changing."
                        rows={2}
                        className="text-xs"
                      />
                    </div>
                    {user?.id && pending.proposed_by === user.id && (
                      <p className="text-xs text-zinc-500">
                        This is your own proposal. You can approve it — an admin has to be
                        able to change a query they own — and the record will show you
                        reviewed it yourself.
                      </p>
                    )}
                    <div className="flex flex-wrap gap-2">
                      <Button size="sm" disabled={busy !== null} onClick={() => void review(true)}>
                        {busy === "approve" ? (
                          <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                        ) : (
                          <Check className="mr-1 h-3.5 w-3.5" />
                        )}
                        Approve and apply
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={busy !== null}
                        onClick={() => void review(false)}
                      >
                        {busy === "reject" ? (
                          <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                        ) : (
                          <X className="mr-1 h-3.5 w-3.5" />
                        )}
                        Reject
                      </Button>
                    </div>
                  </div>
                ) : (
                  <p className="text-xs text-zinc-600 dark:text-zinc-400">
                    A workspace admin has to approve this before it runs. Your role is{" "}
                    {role || "unknown"}.
                  </p>
                )}
              </section>
            )}

            {versions.length === 0 ? (
              <p className="rounded-md border bg-zinc-50 p-3 text-xs text-zinc-500 dark:bg-zinc-900">
                This query has not been edited since it was saved, so there is nothing to
                compare yet. The next edit will keep a copy of what it replaced.
              </p>
            ) : (
              <section className="space-y-2">
                <h3 className="text-sm font-medium">Earlier versions</h3>
                <div className="flex flex-wrap gap-1.5">
                  {versions.map((v) => (
                    <Button
                      key={v.version}
                      size="sm"
                      variant={v.version === selected ? "default" : "outline"}
                      className="h-7 text-xs"
                      aria-pressed={v.version === selected}
                      onClick={() => setSelected(v.version)}
                      title={`Saved ${formatAbsoluteTime(v.created_at)} by ${describeAuthor(v.changed_by)}`}
                    >
                      v{v.version}
                    </Button>
                  ))}
                </div>

                {selectedVersion && current && (
                  <div className="rounded-lg border p-3">
                    <p className="mb-2 text-xs text-zinc-500">
                      Version {selectedVersion.version} — replaced{" "}
                      {formatAbsoluteTime(selectedVersion.created_at)} by{" "}
                      {describeAuthor(selectedVersion.changed_by)}
                      {selectedVersion.name !== current.name && (
                        <> · was named &ldquo;{selectedVersion.name}&rdquo;</>
                      )}
                    </p>

                    <SqlDiffView
                      base={selectedVersion.sql_text}
                      next={current.sql_text}
                      baseLabel={`Version ${selectedVersion.version}`}
                      nextLabel="Running now"
                      className="mb-3"
                    />

                    <Button
                      size="sm"
                      variant="outline"
                      disabled={busy !== null || selectedVersion.sql_text === current.sql_text}
                      title={
                        selectedVersion.sql_text === current.sql_text
                          ? "This version's SQL is already what the query runs."
                          : "Save this older SQL as a new edit — the current text is kept too."
                      }
                      onClick={() => void restore()}
                    >
                      {busy === "restore" ? (
                        <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" />
                      ) : (
                        <RotateCcw className="mr-1 h-3.5 w-3.5" />
                      )}
                      Restore this version
                    </Button>
                    <p className="mt-1.5 text-xs text-zinc-500">
                      Restoring is a normal edit: the text running now is kept as a version
                      too, so this can be undone.
                      {pending ? " It will need approval, like any edit to this query." : ""}
                    </p>
                  </div>
                )}
              </section>
            )}
          </div>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
