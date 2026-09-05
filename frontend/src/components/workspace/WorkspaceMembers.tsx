"use client"

import { useCallback, useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import {
  getActiveWorkspaceId,
  resolveActiveAfterDelete,
  setActiveWorkspaceId,
} from "@/lib/workspace/active-workspace"
import { toast } from "sonner"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
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
import { Ban, Copy, Loader2, Mail, MoreHorizontal, Trash2, UserMinus, UserPlus, Users } from "lucide-react"
import type { ApiErrorBody } from "@/lib/api/types"
import { roleRank, roleBadgeVariant } from "@/lib/workspace/roles"

interface WorkspaceMember {
  id: string
  user_id: string
  email?: string
  role: string
  created_at?: string
}

interface WorkspaceInvite {
  id: string
  email: string
  role: string
  status: string
  expires_at?: string
  accept_url?: string
  created_at?: string
}

interface WorkspaceMembersProps {
  workspaceId: string
  workspaceName?: string
  /** The caller's role in this workspace; gates the admin-only actions. */
  currentRole: string
  /** Personal identity-anchor workspace — can't be deleted, so hide the danger zone. */
  isPersonal?: boolean
}

// A pending destructive action awaiting confirmation in the shared AlertDialog.
type ConfirmAction =
  | { kind: "remove-member"; userId: string; label: string }
  | { kind: "revoke-invite"; inviteId: string; label: string }

// A member can be invited as anything except owner (the backend CHECK bars
// 'owner' on workspace_invites; ownership transfer is a separate flow).
const INVITABLE_ROLES = ["member", "admin", "viewer"] as const

function formatDate(value?: string): string {
  if (!value) return "—"
  const d = new Date(value)
  return isNaN(d.getTime()) ? "—" : d.toLocaleDateString()
}

// Client-side email sanity check, mirroring the api-gateway's mail.ParseAddress
// guard (isValidEmailAddress in workspaces.go). Catches a malformed address before
// the request so we can't mint a dead pending invite and then falsely report "sent".
// The backend stays authoritative; this is a fail-fast UX guard.
function looksLikeEmail(value: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(value)
}

export function WorkspaceMembers({ workspaceId, workspaceName, currentRole, isPersonal = false }: WorkspaceMembersProps) {
  const router = useRouter()
  const canManage = roleRank(currentRole) >= roleRank("admin")
  const canGrantOwner = roleRank(currentRole) >= roleRank("owner")
  // Only an owner may delete a workspace, and personal workspaces can never be
  // deleted (backend refuses with 409) — so gate the danger zone on both, matching
  // how the Leave card hides itself on personal workspaces.
  const canDeleteWorkspace = canGrantOwner && !isPersonal
  // A personal workspace never gains members: CreateWorkspaceInvite refuses with
  // 409 "personal workspaces cannot be modified" BEFORE it even parses the email,
  // so no address can succeed. Gating here — rather than letting the user fill in
  // the whole dialog and learn from a server error — matches how rename and leave
  // are already gated on the General tab.
  const canInvite = canManage && !isPersonal

  const [members, setMembers] = useState<WorkspaceMember[]>([])
  const [invites, setInvites] = useState<WorkspaceInvite[]>([])
  const [loading, setLoading] = useState(true)
  const [membersError, setMembersError] = useState(false)
  const [currentUserId, setCurrentUserId] = useState<string | null>(null)

  // Destructive-action confirmation (remove member / revoke invite).
  const [confirmAction, setConfirmAction] = useState<ConfirmAction | null>(null)
  const [actionPending, setActionPending] = useState(false)
  // The member whose role change is in flight — disables that row's trigger so a
  // second selection can't fire a duplicate POST.
  const [roleChangingId, setRoleChangingId] = useState<string | null>(null)

  // Delete-workspace (owner-only) confirmation + in-flight guard.
  const [deleteOpen, setDeleteOpen] = useState(false)
  const [deletingWorkspace, setDeletingWorkspace] = useState(false)
  // Typed-confirmation: the caller must type the workspace name to arm the delete,
  // so an irreversible delete can't fire from a single mis-click.
  const [deleteConfirmText, setDeleteConfirmText] = useState("")

  // Invite dialog state.
  const [inviteOpen, setInviteOpen] = useState(false)
  const [inviteEmail, setInviteEmail] = useState("")
  const [inviteRole, setInviteRole] = useState<string>("member")
  const [sending, setSending] = useState(false)
  const [lastAcceptUrl, setLastAcceptUrl] = useState<string | null>(null)

  const loadMembers = useCallback(async () => {
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.MEMBERS(workspaceId))
      if (!res.ok) {
        setMembersError(true)
        return
      }
      const data = (await res.json()) as { members?: WorkspaceMember[] }
      setMembers(data.members ?? [])
      setMembersError(false)
    } catch {
      // The caller is always a member, so an empty members list means the load
      // failed — surface a retryable error rather than a misleading empty state.
      setMembersError(true)
    }
  }, [workspaceId])

  const loadInvites = useCallback(async () => {
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.INVITES(workspaceId))
      if (!res.ok) return
      const data = (await res.json()) as { invites?: WorkspaceInvite[] }
      setInvites(data.invites ?? [])
    } catch {
      // non-fatal
    }
  }, [workspaceId])

  // The logged-in user's id — there is no client auth context, so read it from
  // /auth/me. Used only to hide the row actions on the caller's OWN row (the
  // backend independently enforces every mutation; this is a UX guard).
  const loadCurrentUser = useCallback(async () => {
    try {
      const res = await authFetch(API_ENDPOINTS.AUTH.ME)
      if (!res.ok) return
      const data = (await res.json()) as { user_id?: string }
      if (data.user_id) setCurrentUserId(data.user_id)
    } catch {
      // non-fatal; row actions then fall back to backend enforcement
    }
  }, [])

  useEffect(() => {
    let active = true
    setLoading(true)
    void Promise.all([loadMembers(), loadInvites(), loadCurrentUser()]).finally(() => {
      if (active) setLoading(false)
    })
    return () => {
      active = false
    }
  }, [loadMembers, loadInvites, loadCurrentUser])

  const pendingInvites = invites.filter((i) => i.status === "pending")

  // Whether the caller may act on a given member row. Mirrors the backend guards
  // (admin+ to manage others; owners are owner-only) and never acts on yourself.
  const canActOnMember = (m: WorkspaceMember): boolean => {
    if (!canManage) return false
    if (currentUserId === null) return false // identity unknown → fail closed (can't honor the self-guard)
    if (m.user_id === currentUserId) return false // self-guard
    if (roleRank(m.role) >= roleRank("owner") && !canGrantOwner) return false // owner-only on owners
    return true
  }

  // Roles the caller may assign, minus the member's current role (a no-op).
  const rolesForChange = (memberRole: string): string[] => {
    const all = canGrantOwner ? ["owner", "admin", "member", "viewer"] : ["admin", "member", "viewer"]
    return all.filter((r) => r !== memberRole)
  }

  const changeRole = async (m: WorkspaceMember, role: string) => {
    if (roleChangingId !== null) return // a change is already in flight — ignore
    setRoleChangingId(m.user_id)
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.MEMBER_ROLE(workspaceId, m.user_id), {
        method: "POST",
        body: JSON.stringify({ role }),
      })
      if (!res.ok) {
        const err = (await res.json().catch(() => ({}))) as ApiErrorBody
        toast.error(err.error ?? "Failed to change role")
        return
      }
      toast.success(`${m.email ?? m.user_id} is now ${role}`)
      void loadMembers()
    } catch {
      toast.error("Failed to change role")
    } finally {
      setRoleChangingId(null)
    }
  }

  const runConfirmAction = async () => {
    if (!confirmAction) return
    setActionPending(true)
    try {
      if (confirmAction.kind === "remove-member") {
        const res = await authFetch(API_ENDPOINTS.WORKSPACES.MEMBER_DELETE(workspaceId, confirmAction.userId), {
          method: "DELETE",
        })
        if (!res.ok) {
          const err = (await res.json().catch(() => ({}))) as ApiErrorBody
          toast.error(err.error ?? "Failed to remove member")
          return
        }
        toast.success(`${confirmAction.label} removed`)
        void loadMembers()
      } else {
        const res = await authFetch(API_ENDPOINTS.WORKSPACES.INVITE_BY_ID(workspaceId, confirmAction.inviteId), {
          method: "DELETE",
        })
        if (!res.ok) {
          const err = (await res.json().catch(() => ({}))) as ApiErrorBody
          toast.error(err.error ?? "Failed to revoke invite")
          return
        }
        toast.success(`Invite to ${confirmAction.label} revoked`)
        void loadInvites()
      }
      setConfirmAction(null)
    } catch {
      toast.error("Action failed")
    } finally {
      setActionPending(false)
    }
  }

  const openInvite = () => {
    setInviteEmail("")
    setInviteRole("member")
    setLastAcceptUrl(null)
    setInviteOpen(true)
  }

  const handleSendInvite = async () => {
    const email = inviteEmail.trim()
    if (!email) return
    if (!looksLikeEmail(email)) {
      toast.error("Enter a valid email address")
      return
    }
    setSending(true)
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.INVITES(workspaceId), {
        method: "POST",
        body: JSON.stringify({ email, role: inviteRole }),
      })
      if (!res.ok) {
        const err = (await res.json().catch(() => ({}))) as ApiErrorBody
        toast.error(err.error ?? "Failed to send invite")
        return
      }
      const inv = (await res.json()) as WorkspaceInvite
      setLastAcceptUrl(inv.accept_url ?? null)
      setInviteEmail("")
      toast.success(`Invite sent to ${email}`)
      void loadInvites()
    } catch {
      toast.error("Failed to send invite")
    } finally {
      setSending(false)
    }
  }

  const copyAcceptUrl = async () => {
    if (!lastAcceptUrl) return
    try {
      await navigator.clipboard.writeText(lastAcceptUrl)
      toast.success("Invite link copied")
    } catch {
      // clipboard may be unavailable; the field is still selectable
    }
  }

  // Owner-only: delete the workspace, then re-point the active selection if it was
  // this one (so the next request's X-Workspace-ID isn't a tombstone) and leave the
  // now-dead members page. The backend stays authoritative (owner-only; refuses
  // personal or non-empty workspaces) — we surface its message on the blocked paths.
  const deleteWorkspace = async () => {
    if (deletingWorkspace) return // a delete is already in flight — ignore
    setDeletingWorkspace(true)
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.DELETE(workspaceId), { method: "DELETE" })
      if (!res.ok) {
        const err = (await res.json().catch(() => ({}))) as ApiErrorBody
        const msg =
          res.status === 409
            ? err.error ??
              "Remove this workspace's connections, pipelines and saved queries first. Personal workspaces can't be deleted."
            : res.status === 403
              ? "Only the workspace owner can delete it."
              : res.status === 404
                ? "This workspace no longer exists."
                : err.error ?? "Failed to delete workspace"
        toast.error(msg)
        return // keep the confirm dialog open so the user sees why
      }
      // 204 — deleted. If it was the active workspace, re-point the selection at a
      // surviving workspace (or clear it when none remain / the list can't load).
      const activeId = getActiveWorkspaceId()
      if (activeId === workspaceId) {
        let list: { id: string }[] = []
        try {
          const listRes = await authFetch(API_ENDPOINTS.WORKSPACES.LIST)
          if (listRes.ok) {
            const data = (await listRes.json()) as { workspaces?: { id: string }[] }
            list = data.workspaces ?? []
          }
        } catch {
          // couldn't refresh — fall through with an empty list so the stale active
          // selection is still cleared rather than left pointing at the tombstone.
        }
        const { nextActiveId } = resolveActiveAfterDelete(list, workspaceId, activeId)
        setActiveWorkspaceId(nextActiveId)
      }
      toast.success(`${workspaceName ? `"${workspaceName}"` : "Workspace"} deleted`)
      setDeleteOpen(false)
      router.push("/")
    } catch {
      toast.error("Failed to delete workspace")
    } finally {
      setDeletingWorkspace(false)
    }
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="flex flex-row items-start justify-between gap-4">
          <div>
            <CardTitle className="flex items-center gap-2">
              <Users className="h-4 w-4 text-violet-500" />
              Members
            </CardTitle>
            <CardDescription>
              {isPersonal
                ? "Your personal workspace is just for you — it always has exactly one member."
                : `People with access to ${workspaceName ? `"${workspaceName}"` : "this workspace"}.`}
            </CardDescription>
          </div>
          {canInvite && (
            <Button size="sm" onClick={openInvite} className="gap-1.5 shrink-0">
              <UserPlus className="h-3.5 w-3.5" />
              Invite member
            </Button>
          )}
        </CardHeader>
        {isPersonal && (
          <CardContent className="pt-0">
            <p className="rounded-md border border-zinc-200 bg-zinc-50 px-3 py-2 text-xs text-zinc-600 dark:border-zinc-800 dark:bg-zinc-900/50 dark:text-zinc-400">
              To work with teammates, create a team workspace from the workspace switcher in the top bar
              (&ldquo;New workspace&rdquo;). You can invite people there and give each of them a role.
            </p>
          </CardContent>
        )}
        <CardContent>
          {loading ? (
            <div className="flex items-center gap-2 py-6 text-sm text-zinc-500">
              <Loader2 className="h-4 w-4 animate-spin" />
              Loading members…
            </div>
          ) : membersError ? (
            <div className="flex items-center gap-3 py-6 text-sm">
              <span className="text-red-600 dark:text-red-400">Couldn&apos;t load members.</span>
              <Button variant="outline" size="sm" onClick={() => void loadMembers()}>
                Retry
              </Button>
            </div>
          ) : members.length === 0 ? (
            <p className="py-6 text-sm text-zinc-500">No members yet.</p>
          ) : (
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Member</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Joined</TableHead>
                  {canManage && (
                    <TableHead className="w-[1%] text-right">
                      <span className="sr-only">Actions</span>
                    </TableHead>
                  )}
                </TableRow>
              </TableHeader>
              <TableBody>
                {members.map((m) => (
                  <TableRow key={m.id}>
                    <TableCell className="font-medium">{m.email ?? m.user_id}</TableCell>
                    <TableCell>
                      <Badge variant={roleBadgeVariant(m.role)}>{m.role}</Badge>
                    </TableCell>
                    <TableCell className="text-zinc-500">{formatDate(m.created_at)}</TableCell>
                    {canManage && (
                      <TableCell className="text-right">
                        {canActOnMember(m) && (
                          <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                              <Button
                                variant="ghost"
                                size="icon-sm"
                                aria-label={`Manage ${m.email ?? m.user_id}`}
                                disabled={roleChangingId === m.user_id}
                              >
                                {roleChangingId === m.user_id ? (
                                  <Loader2 className="h-4 w-4 animate-spin" />
                                ) : (
                                  <MoreHorizontal className="h-4 w-4" />
                                )}
                              </Button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end">
                              {rolesForChange(m.role).map((r) => (
                                <DropdownMenuItem key={r} onSelect={() => void changeRole(m, r)}>
                                  Make {r.charAt(0).toUpperCase() + r.slice(1)}
                                </DropdownMenuItem>
                              ))}
                              <DropdownMenuSeparator />
                              <DropdownMenuItem
                                className="text-red-600 focus:text-red-600 dark:text-red-400 dark:focus:text-red-400"
                                onSelect={() =>
                                  setConfirmAction({
                                    kind: "remove-member",
                                    userId: m.user_id,
                                    label: m.email ?? m.user_id,
                                  })
                                }
                              >
                                <UserMinus className="mr-2 h-3.5 w-3.5" />
                                Remove from workspace
                              </DropdownMenuItem>
                            </DropdownMenuContent>
                          </DropdownMenu>
                        )}
                      </TableCell>
                    )}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          )}
        </CardContent>
      </Card>

      {pendingInvites.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2">
              <Mail className="h-4 w-4 text-violet-500" />
              Pending invitations
            </CardTitle>
            <CardDescription>Invites that haven&apos;t been accepted yet.</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Email</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Expires</TableHead>
                  {canManage && (
                    <TableHead className="w-[1%] text-right">
                      <span className="sr-only">Actions</span>
                    </TableHead>
                  )}
                </TableRow>
              </TableHeader>
              <TableBody>
                {pendingInvites.map((inv) => (
                  <TableRow key={inv.id}>
                    <TableCell className="font-medium">{inv.email}</TableCell>
                    <TableCell>
                      <Badge variant={roleBadgeVariant(inv.role)}>{inv.role}</Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant="outline">{inv.status}</Badge>
                    </TableCell>
                    <TableCell className="text-zinc-500">{formatDate(inv.expires_at)}</TableCell>
                    {canManage && (
                      <TableCell className="text-right">
                        <DropdownMenu>
                          <DropdownMenuTrigger asChild>
                            <Button
                              variant="ghost"
                              size="icon-sm"
                              aria-label={`Manage invite for ${inv.email}`}
                            >
                              <MoreHorizontal className="h-4 w-4" />
                            </Button>
                          </DropdownMenuTrigger>
                          <DropdownMenuContent align="end">
                            <DropdownMenuItem
                              className="text-red-600 focus:text-red-600 dark:text-red-400 dark:focus:text-red-400"
                              onSelect={() =>
                                setConfirmAction({ kind: "revoke-invite", inviteId: inv.id, label: inv.email })
                              }
                            >
                              <Ban className="mr-2 h-3.5 w-3.5" />
                              Revoke invite
                            </DropdownMenuItem>
                          </DropdownMenuContent>
                        </DropdownMenu>
                      </TableCell>
                    )}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>
      )}

      {canDeleteWorkspace && (
        <Card className="border-red-200 dark:border-red-900/60">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-red-600 dark:text-red-400">
              <Trash2 className="h-4 w-4" />
              Danger zone
            </CardTitle>
            <CardDescription>
              Permanently delete {workspaceName ? `"${workspaceName}"` : "this workspace"}. Remove its
              connections, pipelines and saved queries first. This cannot be undone.
            </CardDescription>
          </CardHeader>
          <CardContent>
            <Button
              variant="outline"
              className="border-red-300 text-red-600 hover:bg-red-50 hover:text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300 dark:hover:bg-red-950/60 dark:hover:text-red-200"
              onClick={() => {
                setConfirmAction(null) // never show the member/invite confirm and this dialog at once
                setDeleteConfirmText("") // require a fresh typed confirmation each time
                setDeleteOpen(true)
              }}
            >
              <Trash2 className="mr-2 h-3.5 w-3.5" />
              Delete workspace
            </Button>
          </CardContent>
        </Card>
      )}

      <Dialog open={inviteOpen} onOpenChange={setInviteOpen}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <UserPlus className="h-5 w-5 text-violet-500" />
              Invite a teammate
            </DialogTitle>
            <DialogDescription>
              They&apos;ll get access to {workspaceName ? `"${workspaceName}"` : "this workspace"} with
              the role you choose.
            </DialogDescription>
          </DialogHeader>

          <div className="space-y-4 py-2">
            <div className="space-y-1.5">
              <Label htmlFor="invite-email">Email address</Label>
              <Input
                id="invite-email"
                type="email"
                placeholder="teammate@company.com"
                value={inviteEmail}
                onChange={(e) => setInviteEmail(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && void handleSendInvite()}
                autoFocus
              />
              {inviteEmail.trim() !== "" && !looksLikeEmail(inviteEmail.trim()) && (
                <p className="text-xs text-red-600 dark:text-red-400">
                  Enter a valid email address, e.g. teammate@company.com
                </p>
              )}
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="invite-role">Role</Label>
              <Select value={inviteRole} onValueChange={setInviteRole}>
                <SelectTrigger id="invite-role">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {INVITABLE_ROLES.map((r) => (
                    <SelectItem key={r} value={r}>
                      {r.charAt(0).toUpperCase() + r.slice(1)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            {lastAcceptUrl && (
              <div className="space-y-1.5 rounded-md border border-zinc-200 bg-zinc-50 p-3 dark:border-zinc-800 dark:bg-zinc-900">
                <Label htmlFor="invite-accept-url" className="text-xs text-zinc-500">
                  Shareable invite link
                </Label>
                <div className="flex items-center gap-2">
                  <Input id="invite-accept-url" readOnly value={lastAcceptUrl} className="text-xs" />
                  <Button type="button" variant="outline" size="icon-sm" onClick={() => void copyAcceptUrl()}>
                    <Copy className="h-3.5 w-3.5" />
                    <span className="sr-only">Copy invite link</span>
                  </Button>
                </div>
              </div>
            )}
          </div>

          <DialogFooter>
            <Button variant="outline" onClick={() => setInviteOpen(false)}>
              {lastAcceptUrl ? "Done" : "Cancel"}
            </Button>
            <Button
              onClick={() => void handleSendInvite()}
              disabled={sending || !looksLikeEmail(inviteEmail.trim())}
              className="gap-1.5"
            >
              {sending ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Mail className="h-3.5 w-3.5" />}
              Send invite
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <AlertDialog
        open={confirmAction !== null}
        onOpenChange={(open) => {
          if (!open) setConfirmAction(null)
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>
              {confirmAction?.kind === "revoke-invite" ? "Revoke invitation?" : "Remove member?"}
            </AlertDialogTitle>
            <AlertDialogDescription>
              {confirmAction?.kind === "revoke-invite"
                ? `The invite to ${confirmAction?.label} will be cancelled and its link will stop working.`
                : `${confirmAction?.label} will lose access to this workspace. They can be re-invited later.`}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={actionPending}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault() // keep open until the request settles / on error
                void runConfirmAction()
              }}
              disabled={actionPending}
              className="bg-red-600 text-white hover:bg-red-700 focus:ring-red-600 dark:bg-red-600 dark:hover:bg-red-700"
            >
              {actionPending && <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" />}
              {confirmAction?.kind === "revoke-invite" ? "Revoke invite" : "Remove member"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      <AlertDialog
        open={deleteOpen}
        onOpenChange={(open) => {
          if (!open) {
            setDeleteOpen(false)
            setDeleteConfirmText("")
          }
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete this workspace?</AlertDialogTitle>
            <AlertDialogDescription>
              {workspaceName ? `"${workspaceName}"` : "This workspace"} and its members and pending
              invites will be permanently removed. This cannot be undone.
            </AlertDialogDescription>
          </AlertDialogHeader>
          {workspaceName && (
            <div className="space-y-1.5">
              <Label htmlFor="delete-confirm">
                Type <span className="font-semibold">{workspaceName}</span> to confirm
              </Label>
              <Input
                id="delete-confirm"
                value={deleteConfirmText}
                onChange={(e) => setDeleteConfirmText(e.target.value)}
                placeholder={workspaceName}
                autoComplete="off"
                disabled={deletingWorkspace}
              />
            </div>
          )}
          <AlertDialogFooter>
            <AlertDialogCancel disabled={deletingWorkspace}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault() // keep open until the request settles / on error
                void deleteWorkspace()
              }}
              disabled={deletingWorkspace || (!!workspaceName && deleteConfirmText.trim() !== workspaceName)}
              className="bg-red-600 text-white hover:bg-red-700 focus:ring-red-600 dark:bg-red-600 dark:hover:bg-red-700"
            >
              {deletingWorkspace && <Loader2 className="mr-2 h-3.5 w-3.5 animate-spin" />}
              Delete permanently
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
