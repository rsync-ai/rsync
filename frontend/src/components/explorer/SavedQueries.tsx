"use client"

import { useCallback, useEffect, useState } from "react"
import { toast } from "sonner"
import { authFetch } from "@/lib/api/auth-fetch"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Switch } from "@/components/ui/switch"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  AlertTriangle,
  Bookmark,
  BookmarkPlus,
  Clock,
  History,
  Loader2,
  Lock,
  Sparkles,
  SquarePen,
  SquareTerminal,
  Table2,
  Trash2,
  Users,
} from "lucide-react"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { useCurrentUser } from "@/contexts/CurrentUserContext"
import { canEditSavedQuery } from "@/lib/workspace/roles"
import { SavedQueryEditDialog } from "./SavedQueryEditDialog"
import { SavedQueryHistoryDialog } from "./SavedQueryHistoryDialog"
import { SavedQueryModelDialog } from "./SavedQueryModelDialog"
import { formatAbsoluteTime, formatNextRun } from "./scheduleTime"

// Saved queries are workspace-scoped server-side records (migration 084), unlike
// Query History which stays a per-browser localStorage scratchpad. The two are
// deliberately separate: history is automatic and disposable, a saved query is
// named, shared with the workspace, and (stage 2) schedulable.

export interface SavedQuery {
  id: string
  connection_id: string
  name: string
  description?: string
  sql_text: string
  nl_prompt?: string
  statement_class: string
  visibility: "private" | "workspace"
  created_by: string
  created_at: string
  updated_at: string
  last_run_at?: string

  // Model fields (migrations 085, 088). "none" means this is a plain saved query;
  // "table" means running it rebuilds target_table in the same database;
  // "statement" means running it executes the SQL as written, which names its own
  // destination and therefore carries no target_table.
  materialization?: string
  target_table?: string
  last_run_status?: string
  last_run_error?: string
  /** The engine behind connection_id. "" once that connection is deleted. */
  connector_type?: string
  /**
   * Resolved server-side by the Explorer capability table — never re-derived from
   * connector_type here, which is the substring allowlist that resolver replaced.
   * Optional so an older payload reads as "unknown" rather than "unsupported".
   */
  supports_materialization?: boolean
  /** "" when the query has no schedule; otherwise the live schedule's status. */
  schedule_status?: string
  /**
   * When the schedule fires next, computed server-side. Absent unless the schedule
   * is active — a paused schedule has no next run, and showing one would promise a
   * rebuild that is not coming.
   */
  next_run_at?: string
}

interface SavedQueriesProps {
  /** Active connection; the list is scoped to it and new saves bind to it. */
  connectionId: string
  /** SQL currently in the editor — the candidate for "Save current query". */
  currentSql: string
  /** The NL prompt that produced currentSql, if any. Stored for provenance. */
  currentQuestion?: string
  /** Load a saved query back into the editor. */
  onLoad: (sql: string, question?: string) => void
}

/**
 * statementClassBadge renders the stored class. It is ADVISORY: the server
 * re-classifies the current sql_text on every execution, so this badge describes
 * the query, it does not authorize it. Destructive/DDL get a visible warning
 * because a saved name ("Nightly cleanup") hides what the SQL actually does.
 */
function statementClassBadge(cls: string) {
  switch (cls) {
    case "read":
      return null
    case "dml_write":
      return (
        <Badge variant="outline" className="text-[10px] border-amber-500 text-amber-600">
          writes
        </Badge>
      )
    case "ddl":
      return (
        <Badge variant="outline" className="text-[10px] border-amber-500 text-amber-600">
          DDL
        </Badge>
      )
    case "destructive":
      return (
        <Badge variant="outline" className="text-[10px] border-red-500 text-red-600">
          <AlertTriangle className="h-2.5 w-2.5 mr-0.5" />
          destructive
        </Badge>
      )
    default:
      return (
        <Badge variant="outline" className="text-[10px]">
          {cls}
        </Badge>
      )
  }
}

/**
 * modelBadges says what a saved query does when nobody is watching. A row that
 * rewrites a real table every hour must not look identical to one that only sits
 * in a list, and a failing scheduled rebuild is invisible unless the list says so —
 * nobody opens a dialog to check on something they believe is fine.
 */
function modelBadges(q: SavedQuery) {
  const hasTarget = q.materialization === "table" && !!q.target_table
  const isStatement = q.materialization === "statement"
  // Deliberately NOT gated on what the query does. A schedule and a materialization
  // are separate records, and clearing the latter does not stop the former — so
  // gating the whole badge block on it is how a query that still fires every hour
  // comes to render exactly like an inert one. The schedule badges answer "is
  // anything running unattended?", which stays true in every mode.
  if (!hasTarget && !isStatement && !q.schedule_status && q.last_run_status !== "failed") return null
  return (
    <>
      {hasTarget && (
        <Badge
          variant="outline"
          className="text-[10px] font-mono max-w-[140px] truncate"
          title={`Rebuilds ${q.target_table}`}
        >
          <Table2 className="h-2.5 w-2.5 mr-0.5 shrink-0" />
          {q.target_table}
        </Badge>
      )}
      {isStatement && (
        <Badge
          variant="outline"
          className="text-[10px]"
          title="Each run executes this query's SQL as written — it writes wherever the statement says"
        >
          <SquareTerminal className="h-2.5 w-2.5 mr-0.5 shrink-0" />
          runs as written
        </Badge>
      )}
      {q.schedule_status === "active" && !hasTarget && !isStatement && (
        <Badge
          variant="outline"
          className="text-[10px] border-amber-500 text-amber-600"
          title="This schedule is running, but a run of this query does nothing. It will fail until you choose what a run does."
        >
          <AlertTriangle className="h-2.5 w-2.5 mr-0.5" />
          does nothing
        </Badge>
      )}
      {q.schedule_status === "active" && (
        <Badge
          variant="outline"
          className="text-[10px] border-emerald-500 text-emerald-600"
          title={
            q.next_run_at
              ? `Next run ${formatAbsoluteTime(q.next_run_at)}`
              : "Scheduled"
          }
        >
          <Clock className="h-2.5 w-2.5 mr-0.5" />
          {q.next_run_at ? formatNextRun(q.next_run_at) : "scheduled"}
        </Badge>
      )}
      {q.schedule_status === "paused" && (
        <Badge variant="outline" className="text-[10px] text-zinc-500">
          <Clock className="h-2.5 w-2.5 mr-0.5" />
          paused
        </Badge>
      )}
      {q.last_run_status === "failed" && (
        <Badge
          variant="outline"
          className="text-[10px] border-red-500 text-red-600"
          title={q.last_run_error || "The last rebuild failed"}
        >
          <AlertTriangle className="h-2.5 w-2.5 mr-0.5" />
          last run failed
        </Badge>
      )}
    </>
  )
}

export function SavedQueries({
  connectionId,
  currentSql,
  currentQuestion,
  onLoad,
}: SavedQueriesProps) {
  const [items, setItems] = useState<SavedQuery[]>([])
  const [loading, setLoading] = useState(false)
  const [saveOpen, setSaveOpen] = useState(false)
  const [saving, setSaving] = useState(false)
  const [name, setName] = useState("")
  const [description, setDescription] = useState("")
  const [shareWithWorkspace, setShareWithWorkspace] = useState(true)
  const [modelFor, setModelFor] = useState<SavedQuery | null>(null)
  const [editFor, setEditFor] = useState<SavedQuery | null>(null)
  const [historyFor, setHistoryFor] = useState<SavedQuery | null>(null)
  // Separate from `items.length === 0`, which asserts the workspace has no saved
  // queries. This says the list could not be loaded, so their number is unknown.
  const [listError, setListError] = useState<string | null>(null)
  const [confirmDelete, setConfirmDelete] = useState<SavedQuery | null>(null)
  // Role gating for the row controls. The api-gateway is the real boundary (every
  // mutation below 403s for a viewer); this stops the UI offering a button whose
  // only possible outcome is that 403.
  const { role: workspaceRole, can } = useWorkspaceRole()
  const { user } = useCurrentUser()
  const canSaveQuery = can("save_query")
  const canSchedule = can("schedule_query")

  const load = useCallback(async () => {
    if (!connectionId) {
      setItems([])
      return
    }
    setLoading(true)
    setListError(null)
    try {
      const res = await authFetch(
        `/api/v1/explorer/saved?connection_id=${encodeURIComponent(connectionId)}`
      )
      if (res.status === 403) {
        // A 403 here just means the caller is below viewer in this workspace;
        // an empty panel is the honest rendering, not an error toast on mount.
        setItems([])
        return
      }
      if (!res.ok) {
        // Every other failure is a failure to LOAD, not a confirmed absence. Rendering
        // it as "No saved queries yet" tells the user their saved work is gone, and
        // invites them to re-save queries they already have.
        setListError(`Could not load saved queries (HTTP ${res.status}).`)
        return
      }
      const data = await res.json()
      setItems(Array.isArray(data?.saved_queries) ? data.saved_queries : [])
    } catch {
      setListError("Could not reach the server to load saved queries.")
    } finally {
      setLoading(false)
    }
  }, [connectionId])

  useEffect(() => {
    void load()
  }, [load])

  const handleSave = async () => {
    const trimmed = name.trim()
    if (!trimmed) {
      toast.error("Name is required")
      return
    }
    setSaving(true)
    try {
      const res = await authFetch("/api/v1/explorer/saved", {
        method: "POST",
        body: JSON.stringify({
          connection_id: connectionId,
          name: trimmed,
          description: description.trim(),
          sql_text: currentSql,
          nl_prompt: currentQuestion ?? "",
          visibility: shareWithWorkspace ? "workspace" : "private",
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(data?.error || "Could not save query")
        return
      }
      toast.success(`Saved "${trimmed}"`)
      setSaveOpen(false)
      setName("")
      setDescription("")
      await load()
    } catch {
      toast.error("Could not save query")
    } finally {
      setSaving(false)
    }
  }

  const handleDelete = async (q: SavedQuery) => {
    try {
      const res = await authFetch(`/api/v1/explorer/saved/${q.id}`, { method: "DELETE" })
      if (!res.ok) {
        const data = await res.json().catch(() => ({}))
        toast.error(data?.error || "Could not delete query")
        return
      }
      toast.success(`Deleted "${q.name}"`)
      await load()
    } catch {
      toast.error("Could not delete query")
    }
  }

  const hasSaveableSql = Boolean(connectionId && currentSql.trim())
  const canSaveCurrent = hasSaveableSql && canSaveQuery
  // Follow the refreshed row, falling back to the captured one if the list has not
  // come back yet.
  const modelQuery = modelFor ? items.find((i) => i.id === modelFor.id) ?? modelFor : null

  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <span className="text-xs text-zinc-500">
          Shared with your workspace. Query History stays local to this browser.
        </span>
        <Button
          size="sm"
          variant="outline"
          disabled={!canSaveCurrent}
          onClick={() => setSaveOpen(true)}
          title={
            !canSaveQuery
              ? `Saving a query needs the member role or higher. Your role is ${workspaceRole || "unknown"}.`
              : hasSaveableSql
                ? "Save the current query"
                : "Write or generate a query first"
          }
        >
          <BookmarkPlus className="h-3.5 w-3.5 mr-1" />
          Save current
        </Button>
      </div>

      <div className="h-[400px] overflow-auto rounded-md border p-1">
        {loading ? (
          <div className="flex items-center justify-center py-8 text-sm text-zinc-500">
            <Loader2 className="h-4 w-4 mr-2 animate-spin" />
            Loading saved queries…
          </div>
        ) : listError ? (
          <div className="flex flex-col items-center gap-2 py-6 text-center">
            <p className="text-sm text-red-600 dark:text-red-400">{listError}</p>
            <p className="text-xs text-zinc-500">
              Your saved queries have not been changed.
            </p>
            <Button size="sm" variant="outline" onClick={() => void load()}>
              Retry
            </Button>
          </div>
        ) : items.length === 0 ? (
          <div className="text-sm text-zinc-500 py-6 text-center">
            No saved queries yet. Run something useful, then Save current.
          </div>
        ) : (
          <div className="space-y-2">
            {items.map((q) => {
              // Ownership-aware, so a member still manages the queries they saved.
              const canEdit = canEditSavedQuery(workspaceRole, q.created_by, user?.id)
              const editDeniedReason = `Only the query's creator or a workspace admin can change it. Your role is ${workspaceRole || "unknown"}.`
              return (
              <div
                key={q.id}
                role="button"
                tabIndex={0}
                className="p-3 border rounded-lg hover:bg-zinc-50 dark:hover:bg-zinc-900 cursor-pointer transition-colors"
                onClick={() => {
                  onLoad(q.sql_text, q.nl_prompt)
                  toast.info(`Loaded "${q.name}"`)
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault()
                    onLoad(q.sql_text, q.nl_prompt)
                    toast.info(`Loaded "${q.name}"`)
                  }
                }}
              >
                <div className="flex items-center justify-between gap-2 mb-1">
                  <div className="flex items-center gap-1.5 min-w-0">
                    <Bookmark className="h-3 w-3 shrink-0 text-violet-600" />
                    <span className="text-sm font-medium truncate">{q.name}</span>
                    {q.visibility === "private" ? (
                      <Lock className="h-3 w-3 shrink-0 text-zinc-400" aria-label="Private" />
                    ) : (
                      <Users className="h-3 w-3 shrink-0 text-zinc-400" aria-label="Shared with workspace" />
                    )}
                    {statementClassBadge(q.statement_class)}
                    {modelBadges(q)}
                  </div>
                  <div className="flex items-center gap-0.5 shrink-0">
                    {/*
                      Labelled, not an icon. This is the only entry point to
                      scheduling, and as a bare 24px glyph it was reported as
                      "there are no options to schedule" — the control was on
                      screen the whole time. The word has to be the one people
                      look for, so: Schedule.
                    */}
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-6 px-1.5 text-xs text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100"
                      aria-label={`Schedule ${q.name}`}
                      disabled={!canSchedule}
                      title={
                        canSchedule
                          ? "Write results to a table, on a schedule"
                          : `Scheduling needs the admin role or higher. Your role is ${workspaceRole || "unknown"}.`
                      }
                      onClick={(e) => {
                        e.stopPropagation()
                        setModelFor(q)
                      }}
                    >
                      <Clock className="h-3 w-3 mr-1" />
                      Schedule
                    </Button>
                    {/*
                      Clicking the row loads the query into the editor, which is not
                      the same thing as changing the saved query itself — an edit made
                      that way has to be saved as a new one, losing the schedule and
                      the target table. This is the button that edits the row in place.
                    */}
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-6 px-1.5 text-xs text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100"
                      aria-label={`Edit ${q.name}`}
                      disabled={!canEdit}
                      title={
                        canEdit
                          ? "Edit the name, SQL or sharing of this saved query"
                          : editDeniedReason
                      }
                      onClick={(e) => {
                        e.stopPropagation()
                        setEditFor(q)
                      }}
                    >
                      <SquarePen className="h-3 w-3 mr-1" />
                      Edit
                    </Button>
                    {/*
                      Readable by anyone who can see the query, not gated on canEdit:
                      "what did this used to say, and who changed it" is a question a
                      viewer has as much right to ask as an editor. It is also where an
                      admin reviews a proposed edit, and the proposer may well be
                      someone without edit rights on the row.
                    */}
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-6 px-1.5 text-xs text-zinc-500 hover:text-zinc-900 dark:hover:text-zinc-100"
                      aria-label={`History of ${q.name}`}
                      title="Earlier versions, and any edit waiting for approval"
                      onClick={(e) => {
                        e.stopPropagation()
                        setHistoryFor(q)
                      }}
                    >
                      <History className="h-3 w-3 mr-1" />
                      History
                    </Button>
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-6 w-6 p-0"
                      aria-label={`Delete ${q.name}`}
                      disabled={!canEdit}
                      title={canEdit ? `Delete ${q.name}` : editDeniedReason}
                      onClick={(e) => {
                        e.stopPropagation()
                        setConfirmDelete(q)
                      }}
                    >
                      <Trash2 className="h-3 w-3 text-zinc-400" />
                    </Button>
                  </div>
                </div>
                {q.nl_prompt && (
                  <div className="text-xs text-violet-600 mb-1 flex items-center gap-1">
                    <Sparkles className="h-3 w-3 shrink-0" />
                    <span className="truncate">{q.nl_prompt}</span>
                  </div>
                )}
                <div className="font-mono text-xs text-zinc-600 dark:text-zinc-400 truncate">
                  {q.sql_text}
                </div>
              </div>
              )
            })}
          </div>
        )}
      </div>

      <Dialog open={saveOpen} onOpenChange={setSaveOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Save query</DialogTitle>
            <DialogDescription>
              Saved queries live in your workspace, so teammates can find and reuse them.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4">
            <div className="space-y-1.5">
              <Label htmlFor="saved-query-name">Name</Label>
              <Input
                id="saved-query-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Daily MRR by plan"
                maxLength={200}
              />
            </div>

            <div className="space-y-1.5">
              <Label htmlFor="saved-query-description">Description (optional)</Label>
              <Textarea
                id="saved-query-description"
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="What this answers, and any caveats."
                rows={2}
              />
            </div>

            <div className="flex items-center justify-between rounded-md border p-3">
              <div className="space-y-0.5">
                <Label htmlFor="saved-query-share">Share with workspace</Label>
                <p className="text-xs text-zinc-500">
                  Off means only you can see it.
                </p>
              </div>
              <Switch
                id="saved-query-share"
                checked={shareWithWorkspace}
                onCheckedChange={setShareWithWorkspace}
              />
            </div>

            <div className="rounded-md border bg-zinc-50 dark:bg-zinc-900 p-2">
              <div className="text-[10px] uppercase tracking-wide text-zinc-500 mb-1">SQL</div>
              <pre className="font-mono text-xs whitespace-pre-wrap break-all max-h-32 overflow-auto">
                {currentSql}
              </pre>
            </div>
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setSaveOpen(false)} disabled={saving}>
              Cancel
            </Button>
            <Button onClick={handleSave} disabled={saving || !name.trim()}>
              {saving ? (
                <>
                  <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" />
                  Saving…
                </>
              ) : (
                "Save"
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Kept keyed to the row in `items` rather than the captured object, so a
          refresh after Save/Run updates the dialog too instead of leaving it
          describing the state the row had when it was clicked. */}
      {modelQuery && (
        <SavedQueryModelDialog
          key={modelQuery.id}
          savedQueryId={modelQuery.id}
          savedQueryName={modelQuery.name}
          materialization={modelQuery.materialization ?? "none"}
          targetTable={modelQuery.target_table}
          statementClass={modelQuery.statement_class}
          supportsMaterialization={modelQuery.supports_materialization}
          connectorType={modelQuery.connector_type}
          lastRunStatus={modelQuery.last_run_status}
          lastRunError={modelQuery.last_run_error}
          open
          onOpenChange={(next) => {
            if (!next) setModelFor(null)
          }}
          onChanged={() => void load()}
        />
      )}

      {historyFor && (
        <SavedQueryHistoryDialog
          key={`history-${historyFor.id}`}
          savedQueryId={historyFor.id}
          savedQueryName={historyFor.name}
          open
          onOpenChange={(next) => {
            if (!next) setHistoryFor(null)
          }}
          onChanged={() => void load()}
        />
      )}

      {editFor && (
        <SavedQueryEditDialog
          key={`edit-${editFor.id}`}
          savedQueryId={editFor.id}
          open
          onOpenChange={(next) => {
            if (!next) setEditFor(null)
          }}
          onSaved={() => void load()}
        />
      )}

      {/* A saved query is a workspace resource: deleting one takes its version
          history and any schedule with it, for everyone, with no undo. That is too
          much to hang on one stray click on a small icon. */}
      <AlertDialog open={!!confirmDelete} onOpenChange={(next) => !next && setConfirmDelete(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete &ldquo;{confirmDelete?.name}&rdquo;?</AlertDialogTitle>
            <AlertDialogDescription>
              This deletes the query, its version history and any schedule attached to it,
              for everyone in the workspace. It cannot be undone.
              {confirmDelete?.materialization === "table" && confirmDelete?.target_table
                ? ` The table it writes to (${confirmDelete.target_table}) is left in place.`
                : ""}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={() => {
                const target = confirmDelete
                setConfirmDelete(null)
                if (target) void handleDelete(target)
              }}
            >
              Delete query
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
