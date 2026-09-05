"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { useRouter } from "next/navigation"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { useWorkspace } from "@/contexts/WorkspaceContext"
import { can, roleBadgeVariant, ROLE_LABELS, type WorkspaceRole } from "@/lib/workspace/roles"
import { toast } from "sonner"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
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
import { Building2, Check, Loader2, LogOut } from "lucide-react"
import type { ApiErrorBody } from "@/lib/api/types"

interface WorkspaceGeneralSettingsProps {
  workspaceId: string
  workspaceName: string
  slug?: string
  /** The caller's role in this workspace; gates rename. */
  currentRole: string
  /** Personal identity-anchor workspace — rename/leave are not offered. */
  isPersonal?: boolean
}

/**
 * General tab of the workspace settings hub: identity (name, slug, your role),
 * rename (admin+), and leave (any member, hidden on personal). Every mutation hits
 * an existing api-gateway endpoint and the backend re-enforces the guard; this
 * component mirrors the rules for UX and surfaces the last-owner 409 on leave.
 */
export function WorkspaceGeneralSettings({
  workspaceId,
  workspaceName,
  slug,
  currentRole,
  isPersonal = false,
}: WorkspaceGeneralSettingsProps) {
  const router = useRouter()
  const { refresh } = useWorkspace()

  // A personal workspace is the user's identity anchor — it can't be renamed or
  // left (the backend refuses both with 409). Gate the UI upfront so the user isn't
  // told only after clicking, matching how the Leave card hides itself.
  const canRename = can(currentRole, "rename_workspace") && !isPersonal
  const canLeave = can(currentRole, "leave_workspace") && !isPersonal

  const [name, setName] = useState(workspaceName)
  const [saving, setSaving] = useState(false)
  // Synchronous in-flight latch: the disabled-button/`saving` state updates async,
  // so Enter + a fast double-click can fire multiple PATCHes in one tick before the
  // re-render. The ref flips immediately, so re-entrant calls bail here.
  const savingRef = useRef(false)
  const [currentUserId, setCurrentUserId] = useState<string | null>(null)
  const [leaveOpen, setLeaveOpen] = useState(false)
  const [leaving, setLeaving] = useState(false)

  // Keep the field in sync if the active workspace changes under us.
  useEffect(() => setName(workspaceName), [workspaceName])

  // Leaving DELETEs the caller's OWN membership, so we need the caller's user id.
  // There is no client auth context; read it from /auth/me (same as WorkspaceMembers).
  const loadCurrentUser = useCallback(async () => {
    try {
      const res = await authFetch(API_ENDPOINTS.AUTH.ME)
      if (!res.ok) return
      const data = (await res.json()) as { user_id?: string }
      if (data.user_id) setCurrentUserId(data.user_id)
    } catch {
      // non-fatal; the Leave confirm stays disabled until we know who we are
    }
  }, [])

  useEffect(() => {
    if (canLeave) void loadCurrentUser()
  }, [canLeave, loadCurrentUser])

  const trimmed = name.trim()
  const nameChanged = trimmed !== "" && trimmed !== workspaceName
  const canSave = canRename && nameChanged && !saving

  const handleRename = async () => {
    if (!canSave || savingRef.current) return // re-entrant guard (double-submit race)
    savingRef.current = true
    setSaving(true)
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.UPDATE(workspaceId), {
        method: "PATCH",
        body: JSON.stringify({ name: trimmed }),
      })
      if (!res.ok) {
        const err = (await res.json().catch(() => ({}))) as ApiErrorBody
        toast.error(err.error ?? "Failed to rename workspace")
        return
      }
      toast.success(`Renamed to "${trimmed}"`)
      void refresh() // update the header switcher + other consumers
    } catch {
      toast.error("Failed to rename workspace")
    } finally {
      savingRef.current = false
      setSaving(false)
    }
  }

  const handleLeave = async () => {
    if (leaving || !currentUserId) return
    setLeaving(true)
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.MEMBER_DELETE(workspaceId, currentUserId), {
        method: "DELETE",
      })
      if (!res.ok) {
        const err = (await res.json().catch(() => ({}))) as ApiErrorBody
        const msg =
          res.status === 409
            ? err.error ??
              "You're the last owner. Transfer ownership to someone else, or delete the workspace instead."
            : res.status === 404
              ? "You're no longer a member of this workspace."
              : err.error ?? "Failed to leave workspace"
        toast.error(msg)
        return // keep the dialog open so the reason is visible
      }
      // 204 — left. Re-fetch the workspace list through the shared provider, which
      // re-resolves the active selection and re-points (or clears) it when it was the
      // workspace we just left — so the next request's X-Workspace-ID can't be a
      // workspace we no longer belong to. One refetch instead of three.
      await refresh()
      toast.success(`You left "${workspaceName}"`)
      setLeaveOpen(false)
      router.push("/")
    } catch {
      toast.error("Failed to leave workspace")
    } finally {
      setLeaving(false)
    }
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Building2 className="h-4 w-4 text-violet-500" />
            General
          </CardTitle>
          <CardDescription>Your workspace identity and your access.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-5">
          {/* Name + rename */}
          <div className="space-y-1.5">
            <Label htmlFor="ws-name">Workspace name</Label>
            <div className="flex items-center gap-2">
              <Input
                id="ws-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && canSave && void handleRename()}
                disabled={!canRename || saving}
                className="max-w-sm"
              />
              {canRename && (
                <Button onClick={() => void handleRename()} disabled={!canSave} className="gap-1.5">
                  {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Check className="h-3.5 w-3.5" />}
                  Save
                </Button>
              )}
            </div>
            {isPersonal ? (
              <p className="text-xs text-zinc-500">
                This is your personal workspace — its name is fixed and can&apos;t be changed.
              </p>
            ) : !canRename ? (
              <p className="text-xs text-zinc-500">Only admins and owners can rename this workspace.</p>
            ) : null}
          </div>

          {/* Slug (read-only — derived by the backend, stable identifier) */}
          <div className="space-y-1.5">
            <Label>Slug</Label>
            <code className="block w-fit rounded bg-zinc-100 px-2 py-1 font-mono text-sm text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300">
              {slug || "—"}
            </code>
          </div>

          {/* Your role */}
          <div className="space-y-1.5">
            <Label>Your role</Label>
            <div>
              <Badge variant={roleBadgeVariant(currentRole)}>
                {ROLE_LABELS[currentRole as WorkspaceRole] ?? currentRole}
              </Badge>
            </div>
          </div>
        </CardContent>
      </Card>

      {canLeave && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <LogOut className="h-4 w-4 text-zinc-500" />
              Leave workspace
            </CardTitle>
            <CardDescription>
              Remove yourself from {workspaceName ? `"${workspaceName}"` : "this workspace"}. You&apos;ll need a
              new invite to rejoin.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button variant="outline" onClick={() => setLeaveOpen(true)} className="gap-1.5">
              <LogOut className="h-3.5 w-3.5" />
              Leave workspace
            </Button>
          </CardContent>
        </Card>
      )}

      <AlertDialog open={leaveOpen} onOpenChange={(open) => !open && setLeaveOpen(false)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Leave this workspace?</AlertDialogTitle>
            <AlertDialogDescription>
              You&apos;ll immediately lose access to {workspaceName ? `"${workspaceName}"` : "this workspace"}{" "}
              and its connections, pipelines and runs. You can be re-invited later.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={leaving}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault() // keep open until the request settles / on error
                void handleLeave()
              }}
              disabled={leaving || !currentUserId}
              className="bg-red-600 text-white hover:bg-red-700 focus:ring-red-600 dark:bg-red-600 dark:hover:bg-red-700"
            >
              {leaving && <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" />}
              Leave
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
