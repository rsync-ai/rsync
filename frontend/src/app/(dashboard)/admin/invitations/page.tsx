"use client"

import { useEffect, useState, useCallback } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { Card } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
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
import { authFetch } from "@/lib/api/auth-fetch"
import { AccessDeniedState, LoadingState, RateLimitExceededState } from "@/components/admin/AdminStates"
import { toast } from "sonner"
import { Plus, Copy, Trash2 } from "lucide-react"
import type { AdminInvitation } from "@/lib/api/admin"
import { adminCreateInvitation, adminListInvitations, adminRevokeInvitation } from "@/lib/api/admin"

const statusBadgeVariant: Record<string, "default" | "secondary" | "outline" | "destructive"> = {
  pending: "default",
  used: "secondary",
  expired: "destructive",
}

export default function AdminInvitationsPage() {
  const [invitations, setInvitations] = useState<AdminInvitation[]>([])
  const [loading, setLoading] = useState(true)
  const [pageStatus, setPageStatus] = useState<number | null>(null)
  const [retryAfter, setRetryAfter] = useState<number | null>(null)

  // Create dialog
  const [showCreate, setShowCreate] = useState(false)
  const [emailHint, setEmailHint] = useState("")
  const [role, setRole] = useState("user")
  const [createdURL, setCreatedURL] = useState("")

  // Revoke dialog
  const [revokeTarget, setRevokeTarget] = useState<AdminInvitation | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setPageStatus(null)
    try {
      const res = await authFetch("/api/v1/admin/invitations", { method: "GET" })
      if (res.status === 401) { window.location.href = `/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`; return }
      if (res.status === 403) { setPageStatus(403); setLoading(false); return }
      if (res.status === 429) {
        setPageStatus(429)
        setRetryAfter(parseInt(res.headers.get("Retry-After") || "", 10))
        setLoading(false)
        return
      }
      const json = await res.json()
      setInvitations(json.data || [])
    } catch {
      toast.error("Failed to load invitations")
    }
    setLoading(false)
  }, [])

  useEffect(() => { load() }, [load])

  const handleCreate = async () => {
    try {
      const result = await adminCreateInvitation({
        email_hint: emailHint || undefined,
        role,
      })
      setCreatedURL(result.url)
      toast.success("Invitation created")
      load()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to create invitation")
    }
  }

  const handleRevoke = async () => {
    if (!revokeTarget) return
    try {
      await adminRevokeInvitation(revokeTarget.id)
      toast.success("Invitation revoked")
      setRevokeTarget(null)
      load()
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Failed to revoke invitation")
    }
  }

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text)
    toast.success("Link copied to clipboard")
  }

  return (
    <div className="space-y-6">
      <PageHeader heading="Admin" description="Invite link management" />
      <AdminNav />

      {loading ? (
        <LoadingState />
      ) : pageStatus === 403 ? (
        <AccessDeniedState />
      ) : pageStatus === 429 ? (
        <RateLimitExceededState retryAfterSeconds={retryAfter} />
      ) : (
        <Card className="p-4">
          <div className="flex items-center justify-between mb-4">
            <div className="font-semibold">Invitations</div>
            <Button onClick={() => { setShowCreate(true); setCreatedURL(""); setEmailHint(""); setRole("user") }}>
              <Plus className="h-4 w-4 mr-1.5" />
              Create Invitation
            </Button>
          </div>

          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Email Hint</TableHead>
                <TableHead>Role</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="hidden md:table-cell">Created By</TableHead>
                <TableHead className="hidden md:table-cell">Expires</TableHead>
                <TableHead className="hidden lg:table-cell">Used By</TableHead>
                <TableHead className="text-right">Actions</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {invitations.length === 0 ? (
                <TableRow>
                  <TableCell colSpan={7} className="text-center text-sm text-zinc-500 py-8">
                    No invitations yet
                  </TableCell>
                </TableRow>
              ) : (
                invitations.map((inv) => (
                  <TableRow key={inv.id}>
                    <TableCell className="text-sm">{inv.email_hint || "-"}</TableCell>
                    <TableCell>
                      <Badge variant="outline">{inv.role}</Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant={statusBadgeVariant[inv.status] || "outline"}>{inv.status}</Badge>
                    </TableCell>
                    <TableCell className="hidden md:table-cell text-sm text-zinc-500">
                      {inv.created_by_email}
                    </TableCell>
                    <TableCell className="hidden md:table-cell text-sm text-zinc-500">
                      {new Date(inv.expires_at).toLocaleString()}
                    </TableCell>
                    <TableCell className="hidden lg:table-cell text-sm text-zinc-500">
                      {inv.used_by_email || "-"}
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-1">
                        {inv.status === "pending" && (
                          <>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-8 w-8 p-0"
                              onClick={() => copyToClipboard(`${window.location.origin}/signup?invite=${inv.token}`)}
                            >
                              <Copy className="h-4 w-4" />
                            </Button>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="h-8 w-8 p-0 text-red-600"
                              onClick={() => setRevokeTarget(inv)}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))
              )}
            </TableBody>
          </Table>
        </Card>
      )}

      {/* Create Invitation Dialog */}
      <Dialog open={showCreate} onOpenChange={setShowCreate}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Invitation</DialogTitle>
            <DialogDescription>
              Generate a one-time invite link to share with a new user.
            </DialogDescription>
          </DialogHeader>

          {createdURL ? (
            <div className="space-y-3">
              <Label>Invite Link</Label>
              <div className="flex gap-2">
                <Input value={createdURL} readOnly className="font-mono text-xs" />
                <Button variant="outline" onClick={() => copyToClipboard(createdURL)}>
                  <Copy className="h-4 w-4" />
                </Button>
              </div>
              <p className="text-xs text-zinc-500">This link expires in 72 hours.</p>
            </div>
          ) : (
            <div className="space-y-4">
              <div className="space-y-2">
                <Label>Email Hint (optional)</Label>
                <Input
                  placeholder="user@example.com"
                  value={emailHint}
                  onChange={(e) => setEmailHint(e.target.value)}
                />
                <p className="text-xs text-zinc-500">Optional hint shown on the signup page.</p>
              </div>
              <div className="space-y-2">
                <Label>Role</Label>
                <Select value={role} onValueChange={setRole}>
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="viewer">Viewer</SelectItem>
                    <SelectItem value="user">User</SelectItem>
                    <SelectItem value="power_user">Power User</SelectItem>
                    <SelectItem value="admin">Admin</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          )}

          <DialogFooter>
            {createdURL ? (
              <Button onClick={() => setShowCreate(false)}>Done</Button>
            ) : (
              <>
                <Button variant="outline" onClick={() => setShowCreate(false)}>Cancel</Button>
                <Button onClick={handleCreate}>Create</Button>
              </>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Revoke Dialog */}
      <AlertDialog open={!!revokeTarget} onOpenChange={() => setRevokeTarget(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Revoke Invitation</AlertDialogTitle>
            <AlertDialogDescription>
              This will permanently delete this invitation. Anyone with the link will no longer be able to sign up.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction className="bg-red-600 hover:bg-red-700" onClick={handleRevoke}>
              Revoke
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
