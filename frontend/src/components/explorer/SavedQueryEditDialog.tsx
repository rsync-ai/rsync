"use client"

import { useCallback, useEffect, useState } from "react"
import { toast } from "sonner"
import { authFetch } from "@/lib/api/auth-fetch"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Textarea } from "@/components/ui/textarea"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { AlertTriangle, Loader2, Lock } from "lucide-react"
import { useCurrentUser } from "@/contexts/CurrentUserContext"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { updateSavedQuery, type SavedQueryPatch } from "./savedQueryUpdate"

// A saved query was writable from the day it shipped — PATCH /explorer/saved/:id has
// always existed, snapshotting the previous version into saved_query_versions — but
// nothing in the UI called it. The only way to change a saved query's SQL was to
// delete it and save a new one, which loses its schedule, its target table and its
// history. This dialog is that missing button.
//
// It deliberately re-reads the row on open rather than trusting a list row: the SQL is
// the field most likely to have been edited by a teammate since the list was fetched,
// and an edit form seeded from stale text silently reverts their work on save.

interface SavedQueryEditDialogProps {
  savedQueryId: string
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Refetch whatever list this was opened from — name, SQL and class all change. */
  onSaved: () => void
}

interface LoadedQuery {
  name: string
  description: string
  sql_text: string
  visibility: string
  created_by: string
  statement_class: string
  materialization: string
  schedule_status: string
}

export function SavedQueryEditDialog({
  savedQueryId,
  open,
  onOpenChange,
  onSaved,
}: SavedQueryEditDialogProps) {
  const { user } = useCurrentUser()
  const { meets } = useWorkspaceRole()

  const [loading, setLoading] = useState(false)
  const [loadError, setLoadError] = useState<string | null>(null)
  const [saving, setSaving] = useState(false)
  const [original, setOriginal] = useState<LoadedQuery | null>(null)

  const [name, setName] = useState("")
  const [description, setDescription] = useState("")
  const [sqlText, setSqlText] = useState("")
  const [shareWithWorkspace, setShareWithWorkspace] = useState(true)
  const [note, setNote] = useState("")

  const load = useCallback(async () => {
    setLoading(true)
    setLoadError(null)
    try {
      const res = await authFetch(`/api/v1/explorer/saved/${savedQueryId}`, { cache: "no-store" })
      if (!res.ok) {
        // Never fall through to an empty form. Rendering blank fields over a query
        // that failed to load invites the user to save that blankness over the
        // real thing.
        setLoadError(`Could not load this query (HTTP ${res.status}).`)
        return
      }
      const data = (await res.json()) as Partial<LoadedQuery>
      const q: LoadedQuery = {
        name: String(data.name ?? ""),
        description: String(data.description ?? ""),
        sql_text: String(data.sql_text ?? ""),
        visibility: String(data.visibility ?? "workspace"),
        created_by: String(data.created_by ?? ""),
        statement_class: String(data.statement_class ?? ""),
        materialization: String(data.materialization ?? "none"),
        schedule_status: String(data.schedule_status ?? ""),
      }
      setOriginal(q)
      setName(q.name)
      setDescription(q.description)
      setSqlText(q.sql_text)
      setShareWithWorkspace(q.visibility !== "private")
    } catch {
      setLoadError("Could not reach the server to load this query.")
    } finally {
      setLoading(false)
    }
  }, [savedQueryId])

  useEffect(() => {
    if (open) void load()
  }, [open, load])

  // Mirrors the server's rule in UpdateSavedQuery: the creator, or a workspace admin.
  // Presentational only — the server decides — but it stops the dialog from offering a
  // Save whose only outcome is a 403, and says why instead.
  const isCreator = !!original && !!user?.id && original.created_by === user.id
  const canEdit = !original || isCreator || meets("admin")

  const visibility = shareWithWorkspace ? "workspace" : "private"
  const dirty =
    !!original &&
    (name.trim() !== original.name ||
      description.trim() !== original.description ||
      sqlText.trim() !== original.sql_text.trim() ||
      visibility !== original.visibility)

  const sqlChanged = !!original && sqlText.trim() !== original.sql_text.trim()
  // Editing the SQL of a query that runs unattended changes what runs unattended, so
  // since migration 092 that edit is a PROPOSAL: the server applies the name,
  // description and sharing immediately and parks the SQL for a workspace admin.
  //
  // schedule_status is non-empty for an active OR a paused schedule (the row's LEFT
  // JOIN excludes only 'deleted'), which is exactly the set the server's gate covers.
  // The two have to agree: if this said "applies immediately" for a paused schedule
  // that the server actually gates, the dialog would promise something it cannot do.
  const runsUnattended = !!original && original.schedule_status !== ""
  const needsApproval = sqlChanged && runsUnattended

  const handleSave = async () => {
    if (!original) return
    const trimmedName = name.trim()
    const trimmedSQL = sqlText.trim()
    if (!trimmedName) {
      toast.error("Name is required")
      return
    }
    if (!trimmedSQL) {
      toast.error("SQL is required")
      return
    }
    setSaving(true)
    try {
      // PATCH takes pointers server-side, so an omitted field is left alone. Send only
      // what changed: resending an untouched field would write a version snapshot for
      // an edit that did not happen.
      const body: SavedQueryPatch = {}
      if (trimmedName !== original.name) body.name = trimmedName
      if (description.trim() !== original.description) body.description = description.trim()
      if (trimmedSQL !== original.sql_text.trim()) body.sql_text = trimmedSQL
      if (visibility !== original.visibility) body.visibility = visibility
      if (needsApproval && note.trim()) body.note = note.trim()

      const result = await updateSavedQuery(savedQueryId, body)
      if (result.kind === "error") {
        toast.error(result.message)
        return
      }
      if (result.kind === "proposed") {
        // Deliberately not "Updated". The SQL did not change, and a success toast that
        // says it did sends someone away believing the schedule now runs their new
        // query — the exact misunderstanding the approval gate exists to prevent.
        toast.success("Sent for approval", { description: result.reason })
      } else {
        toast.success(`Updated "${trimmedName}"`)
      }
      onSaved()
      onOpenChange(false)
    } finally {
      setSaving(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>Edit query</DialogTitle>
          <DialogDescription>
            The previous version is kept, so an edit can be traced back. Any target table
            and schedule stay attached to this query.
          </DialogDescription>
        </DialogHeader>

        {loading ? (
          <div className="flex items-center gap-2 py-8 text-sm text-zinc-500">
            <Loader2 className="h-4 w-4 animate-spin" />
            Loading this query…
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
          <div className="space-y-4 max-h-[60vh] overflow-y-auto pr-1">
            {!canEdit && (
              <div className="flex items-start gap-2 rounded-md border bg-zinc-50 p-2 dark:bg-zinc-900">
                <Lock className="mt-0.5 h-3.5 w-3.5 shrink-0 text-zinc-500" />
                <p className="text-xs text-zinc-600 dark:text-zinc-400">
                  Only the person who saved this query, or a workspace admin, can edit it.
                  You can read it here and load it into the editor to run your own version.
                </p>
              </div>
            )}

            <div className="space-y-1.5">
              <Label htmlFor="edit-query-name">Name</Label>
              <Input
                id="edit-query-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                maxLength={200}
                disabled={!canEdit}
              />
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="edit-query-description">Description (optional)</Label>
              <Textarea
                id="edit-query-description"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="What this answers, and any caveats."
                rows={2}
                disabled={!canEdit}
              />
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="edit-query-sql">SQL</Label>
              <Textarea
                id="edit-query-sql"
                value={sqlText}
                onChange={(e) => setSqlText(e.target.value)}
                rows={10}
                spellCheck={false}
                className="font-mono text-xs"
                disabled={!canEdit}
              />
            </div>

            {needsApproval && (
              <div className="space-y-3 rounded-md border border-amber-500/40 bg-amber-500/10 p-2">
                <div className="flex items-start gap-2">
                  <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
                  <div className="space-y-1 text-xs text-amber-700 dark:text-amber-300">
                    <p>
                      This query runs on a schedule, so changing its SQL needs a workspace
                      admin&rsquo;s approval. Saving sends it for review — the schedule keeps
                      running the current SQL until someone approves the change.
                    </p>
                    <p>
                      The name, description and sharing you set here apply straight away.
                    </p>
                  </div>
                </div>
                <div className="space-y-1.5">
                  <Label htmlFor="edit-query-note" className="text-xs">
                    Why this should change (optional)
                  </Label>
                  <Textarea
                    id="edit-query-note"
                    value={note}
                    onChange={(e) => setNote(e.target.value)}
                    placeholder="What this fixes, and anything the reviewer should check."
                    rows={2}
                    className="bg-white text-xs dark:bg-zinc-950"
                    disabled={!canEdit}
                  />
                </div>
              </div>
            )}

            <div className="flex items-center justify-between rounded-md border p-3">
              <div className="space-y-0.5">
                <Label htmlFor="edit-query-share">Share with workspace</Label>
                <p className="text-xs text-zinc-500">Off means only you can see it.</p>
              </div>
              <Switch
                id="edit-query-share"
                checked={shareWithWorkspace}
                onCheckedChange={setShareWithWorkspace}
                disabled={!canEdit}
              />
            </div>
          </div>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={saving}>
            Cancel
          </Button>
          <Button
            onClick={() => void handleSave()}
            disabled={!canEdit || saving || loading || !!loadError || !dirty || !name.trim() || !sqlText.trim()}
          >
            {saving ? (
              <>
                <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" />
                {needsApproval ? "Sending…" : "Saving…"}
              </>
            ) : needsApproval ? (
              // The button says what the click does. "Save changes" on a click that
              // saves some changes and queues the important one is the wrong promise.
              "Send for approval"
            ) : (
              "Save changes"
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
