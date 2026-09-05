"use client"

import { useEffect, useState, useCallback } from "react"
import { usePathname, useRouter } from "next/navigation"
import { useTheme } from "next-themes"
import { useUIStore } from "@/lib/store/useUIStore"
import { Button } from "@/components/ui/button"
import { Avatar, AvatarFallback, AvatarImage } from "@/components/ui/avatar"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Bell,
  Search,
  Settings,
  User,
  Moon,
  Sun,
  Command,
  LogOut,
  Building2,
  Check,
  Plus,
  ChevronDown,
  Menu,
  UserPlus,
  Cable,
  ArrowRight,
} from "lucide-react"
import { cn } from "@/lib/utils"
import { logout, getUserEmail } from "@/lib/auth"
import { authFetch } from "@/lib/api/auth-fetch"
import {
  captureWorkspace,
  getActiveWorkspaceId,
  setActiveWorkspaceId,
  onActiveWorkspaceChange,
} from "@/lib/workspace/active-workspace"
import { toWorkspaceSlug } from "@/lib/workspace/slug"
import { resolveWorkspaceSwitchTarget } from "@/lib/workspace/scoped-routes"
import { API_ENDPOINTS } from "@/lib/config/api"
import {
  listNotifications,
  getUnreadCount,
  markNotificationRead,
  markAllNotificationsRead,
  type Notification as AppNotification,
} from "@/lib/api/notifications"
import { toast } from "sonner"
import type { ApiErrorBody } from "@/lib/api/types"

interface Workspace {
  id: string
  name: string
  slug: string
  role: string
  // Optional to match WorkspaceSummary: ListWorkspaces returns it, but the field
  // post-dates migration 069 and an older cached payload may not carry it. Absent
  // reads as "not personal", which is the safe default — it only omits a tag.
  is_personal?: boolean
}

interface HeaderProps {
  user?: {
    email: string
    name?: string
    avatarUrl?: string
  }
}

/**
 * Colored severity dot for an unread notification.
 *
 * The notifier normalizes severity to {info,warning,critical} before writing
 * (api-gateway/internal/notifier/catalog.go normalizeSeverity), but rows
 * persisted before that carry the orchestrator's raw "error"/"fatal" — they used
 * to fall through to the violet info dot, painting a real failure as an FYI.
 */
function severityDot(severity: string): string {
  if (isCritical(severity)) return "bg-red-500"
  const s = severity.toLowerCase()
  if (s === "warning" || s === "warn") return "bg-amber-500"
  return "bg-violet-500"
}

/** True for a real failure, whichever vocabulary the row was written with. */
function isCritical(severity: string): boolean {
  const s = severity.toLowerCase()
  return s === "critical" || s === "error" || s === "fatal"
}

/** Compact relative time ("just now", "5m ago", "3h ago", "2d ago", or a date). */
function formatRelativeTime(iso: string): string {
  const then = new Date(iso).getTime()
  if (Number.isNaN(then)) return ""
  const diffMs = Date.now() - then
  const mins = Math.floor(diffMs / 60_000)
  if (mins < 1) return "just now"
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  const days = Math.floor(hrs / 24)
  if (days < 7) return `${days}d ago`
  return new Date(then).toLocaleDateString()
}

export function Header({ user }: HeaderProps) {
  const router = useRouter()
  const pathname = usePathname()
  const { theme, setTheme, resolvedTheme } = useTheme()
  const [mounted, setMounted] = useState(false)
  const collapsed = useUIStore((state) => state.sidebarCollapsed)
  const setSidebarOpen = useUIStore((state) => state.setSidebarOpen)
  const toggleCommandPalette = useUIStore((state) => state.toggleCommandPalette)
  // Notification bell — polls the unread count on an interval and lazily loads
  // the full list when the dropdown opens. Rows come from the notifier consumer
  // via /api/v1/notifications (schema-drift approvals, heal alerts, run failures).
  const [notifications, setNotifications] = useState<AppNotification[]>([])
  const [unreadCount, setUnreadCount] = useState(0)

  // Workspace state
  const [workspaces, setWorkspaces] = useState<Workspace[]>([])
  const [activeWorkspace, setActiveWorkspace] = useState<Workspace | null>(null)
  const [showCreateDialog, setShowCreateDialog] = useState(false)
  const [newWorkspaceName, setNewWorkspaceName] = useState("")
  const [creating, setCreating] = useState(false)
  // After a successful create we keep the dialog open on a "next steps" view rather
  // than just closing — guiding the owner to invite teammates / add a connection.
  const [createdWorkspace, setCreatedWorkspace] = useState<Workspace | null>(null)

  // No captureWorkspace() guard here, deliberately: /workspaces is USER-scoped, not
  // workspace-scoped — it returns the same list whichever workspace is active — and
  // `saved` below is read after the await, so the resolution already uses the
  // current selection. Everything that IS workspace-scoped does guard.
  const fetchWorkspaces = useCallback(async () => {
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.LIST)
      if (!res.ok) return
      const data = await res.json() as { workspaces: Workspace[] }
      const list = data.workspaces ?? []
      setWorkspaces(list)
      // Restore last selected or default to first
      const saved = getActiveWorkspaceId()
      const found = list.find((w) => w.id === saved) ?? list[0] ?? null
      setActiveWorkspace(found)
      // Keep the persisted selection in sync with what we resolved — but only when it
      // actually changed, so this fetch doesn't re-dispatch the change event and
      // re-enter the subscriber below (which would loop). This also CLEARS a stale
      // selection: if the stored active workspace has vanished (deleted here or
      // elsewhere) `found` falls back to a survivor, or to null when the list is empty
      // — either way we never leave the selection pointing at a deleted workspace.
      const nextId = found?.id ?? null
      if (nextId !== saved) setActiveWorkspaceId(nextId)
    } catch {
      // ignore — non-critical UI element
    }
  }, [])

  useEffect(() => {
    setMounted(true)
    void fetchWorkspaces()
    // Re-sync the switcher whenever the active workspace changes — e.g. after a
    // workspace is deleted, the delete flow re-points the active selection, and the
    // deleted entry must drop out of this list so it can't be re-selected. The guard
    // in fetchWorkspaces stops this once the resolved selection matches what's stored
    // (a concurrent external mutation may run a couple of cycles before settling).
    return onActiveWorkspaceChange(() => void fetchWorkspaces())
  }, [fetchWorkspaces])

  // Switching workspace re-scopes every request. Leaving a dead detail route is
  // NOT handled here — it belongs to the subscription below, which sees every
  // change to the active workspace, not just the ones this dropdown causes. All
  // this does is set the selection and refresh the routes that survive it.
  const switchWorkspace = (ws: Workspace) => {
    setActiveWorkspace(ws)
    // Dispatches ACTIVE_WORKSPACE_EVENT synchronously → the redirect effect runs.
    setActiveWorkspaceId(ws.id)
    if (!resolveWorkspaceSwitchTarget(pathname ?? "")) router.refresh()
  }

  // The ONE owner of "the active workspace changed, so this URL is now a dead
  // end". A detail route pins a resource id owned by the workspace we just left,
  // so re-rendering it can only 404 — leave it for the section's list root.
  //
  // Subscribing (rather than doing this inside switchWorkspace) is the whole
  // point: the selection also changes from ANOTHER TAB — the storage event —
  // and from the workspace-delete flow re-pointing it. Those paths used to skip
  // the redirect entirely, leaving the previous workspace's pipeline rendered,
  // with its live action buttons, under the new workspace's header.
  //
  // `replace`, not `push`: the detail URL is a dead end now, so Back must not
  // return to it. `pathname` is in the deps because the handler must see the
  // route the user is on WHEN the switch lands, not the one at first mount.
  useEffect(() => {
    return onActiveWorkspaceChange(() => {
      const target = resolveWorkspaceSwitchTarget(pathname ?? "")
      if (!target) return
      // A silent teleport reads as a bug; say which workspace we moved to and why.
      const name = workspaces.find((w) => w.id === getActiveWorkspaceId())?.name
      toast.info(name ? `Switched to ${name}` : "Switched workspace", {
        description: `Showing that workspace's ${target.label}.`,
      })
      router.replace(target.href)
    })
  }, [pathname, router, workspaces])

  // Load the full notification list (fires when the bell dropdown opens). Also
  // refreshes the badge from the list's authoritative unread_count.
  const loadNotifications = useCallback(async () => {
    // Both responses are workspace-scoped: drop them if a switch overtook the
    // request, or the previous workspace's rows land under the new one's header.
    const isStale = captureWorkspace()
    try {
      const data = await listNotifications()
      if (isStale()) return
      setNotifications(data.notifications)
      setUnreadCount(data.unread_count)
    } catch {
      // best-effort — keep the last known state
    }
  }, [])

  // Cheap unread-count poll for the badge (every 30s), independent of the list.
  const pollUnreadCount = useCallback(async () => {
    const isStale = captureWorkspace()
    try {
      const count = await getUnreadCount()
      if (isStale()) return
      setUnreadCount(count)
    } catch {
      // best-effort — keep the last known count
    }
  }, [])

  useEffect(() => {
    void pollUnreadCount()
    const interval = window.setInterval(pollUnreadCount, 30_000)
    // The bell is workspace-scoped, so a switch invalidates BOTH the count and the
    // loaded rows immediately. Clearing before the refetch is what makes that
    // visible-truthful: without it the badge shows the previous workspace's unread
    // number until the next 30s tick, and an open dropdown keeps painting the
    // previous workspace's notifications until it is reopened — a cross-tenant
    // number and cross-tenant rows on screen, exactly what scoping the bell was
    // meant to prevent. captureWorkspace above stops the in-flight response from
    // undoing this.
    const unsubscribe = onActiveWorkspaceChange(() => {
      setNotifications([])
      setUnreadCount(0)
      void pollUnreadCount()
    })
    return () => {
      window.clearInterval(interval)
      unsubscribe()
    }
  }, [pollUnreadCount])

  const handleNotificationClick = useCallback(
    async (n: AppNotification) => {
      if (!n.read) {
        // Optimistic: clear locally now; the server call is best-effort and the
        // next poll reconciles if it fails.
        setNotifications((prev) => prev.map((x) => (x.id === n.id ? { ...x, read: true } : x)))
        setUnreadCount((c) => Math.max(0, c - 1))
        try {
          await markNotificationRead(n.id)
        } catch {
          // best-effort
        }
      }
      if (n.action_url) router.push(n.action_url)
    },
    [router],
  )

  const handleMarkAllRead = useCallback(async () => {
    setNotifications((prev) => prev.map((x) => ({ ...x, read: true })))
    setUnreadCount(0)
    try {
      await markAllNotificationsRead()
    } catch {
      // best-effort
    }
  }, [])

  const handleCreateWorkspace = async () => {
    const name = newWorkspaceName.trim()
    if (!name) return
    setCreating(true)
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.CREATE, {
        method: "POST",
        body: JSON.stringify({ name }),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({})) as ApiErrorBody
        toast.error(err.error ?? "Failed to create workspace")
        return
      }
      const ws = await res.json() as Workspace
      setWorkspaces((prev) => [...prev, ws])
      switchWorkspace(ws) // make the new workspace active immediately
      setCreatedWorkspace(ws) // switch the dialog to its "next steps" view
      toast.success(`Workspace "${ws.name}" created`)
    } catch {
      toast.error("Failed to create workspace")
    } finally {
      setCreating(false)
    }
  }

  // Reset the dialog to a clean form whenever it closes, so the next open starts
  // fresh (not stuck on the previous success view).
  const handleCreateDialogChange = (open: boolean) => {
    setShowCreateDialog(open)
    if (!open) {
      setNewWorkspaceName("")
      setCreatedWorkspace(null)
    }
  }

  // From the post-create next-steps view: navigate, then close the dialog.
  const goAfterCreate = (href: string) => {
    handleCreateDialogChange(false)
    router.push(href)
  }

  // Avoid hydration mismatch
  // Get user from auth or props
  const userEmail = typeof window !== 'undefined' ? getUserEmail() : null
  const defaultUser = {
    email: userEmail || user?.email || "Account",
    name: user?.name || "Developer",
    avatarUrl: user?.avatarUrl,
  }

  const currentUser = user || defaultUser

  const initials = currentUser?.name
    ? currentUser.name.split(" ").map((n) => n[0]).join("").toUpperCase()
    : currentUser?.email?.[0]?.toUpperCase() || "U"

  return (
    <>
    <header
      className={cn(
        "fixed right-0 top-0 z-30 flex h-16 items-center justify-between border-b border-zinc-200 bg-white/80 backdrop-blur-sm px-4 md:px-6 transition-all duration-300 dark:border-zinc-800 dark:bg-zinc-950/80",
        "left-0 md:left-auto",
        collapsed ? "md:left-16" : "md:left-64"
      )}
    >
      {/* Left: hamburger (mobile) */}
      <div className="flex items-center">
        {/* Mobile hamburger */}
        <Button
          variant="ghost"
          size="icon-sm"
          className="md:hidden"
          onClick={() => setSidebarOpen(true)}
        >
          <Menu className="h-5 w-5" />
          <span className="sr-only">Open menu</span>
        </Button>
      </div>

      {/* Workspace Switcher */}
      {mounted && workspaces.length > 0 && (
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="outline" size="sm" aria-label="Switch workspace" className="gap-1.5 max-w-[160px] sm:max-w-[200px]">
              <Building2 className="h-3.5 w-3.5 shrink-0 text-violet-500" />
              <span className="hidden sm:block truncate text-xs">{activeWorkspace?.name ?? "Workspace"}</span>
              <ChevronDown className="h-3 w-3 shrink-0 opacity-50" />
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" className="w-56">
            <DropdownMenuLabel className="text-xs text-zinc-500">Switch workspace</DropdownMenuLabel>
            <DropdownMenuSeparator />
            {/* The "Personal" tag is the only place the switcher distinguishes the
                auto-provisioned identity-anchor workspace from a team one. Without it
                both read as ordinary workspaces, and the user has no way to know why
                renaming, inviting and deleting are unavailable in exactly one of them. */}
            {workspaces.map((ws) => (
              <DropdownMenuItem key={ws.id} onClick={() => switchWorkspace(ws)} className="gap-2">
                <Building2 className="h-3.5 w-3.5 text-zinc-400" />
                <span className="flex-1 truncate text-sm">{ws.name}</span>
                {ws.is_personal && (
                  <span className="shrink-0 rounded bg-zinc-100 px-1.5 py-0.5 text-[10px] font-medium uppercase tracking-wide text-zinc-500 dark:bg-zinc-800 dark:text-zinc-400">
                    Personal
                  </span>
                )}
                {ws.id === activeWorkspace?.id && <Check className="h-3.5 w-3.5 text-violet-500" />}
              </DropdownMenuItem>
            ))}
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => router.push("/workspace/settings")} className="gap-2">
              <Settings className="h-3.5 w-3.5 text-zinc-400" />
              <span className="text-sm">Workspace settings</span>
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => setShowCreateDialog(true)} className="gap-2 text-violet-600">
              <Plus className="h-3.5 w-3.5" />
              <span className="text-sm">New workspace</span>
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      )}

      {/* Right side */}
      <div className="flex items-center gap-3">
        {/* Theme Toggle */}
        <Button 
          variant="ghost" 
          size="icon-sm"
          onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}
          disabled={!mounted}
        >
          {mounted && resolvedTheme === "dark" ? (
            <Moon className="h-4 w-4" />
          ) : (
            <Sun className="h-4 w-4" />
          )}
          <span className="sr-only">Toggle theme</span>
        </Button>

        {/* Notifications - only render after mount to avoid hydration mismatch */}
        {mounted ? (
        <DropdownMenu onOpenChange={(open) => { if (open) void loadNotifications() }}>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" size="icon-sm" className="relative" aria-label="Notifications">
              <Bell className="h-4 w-4" />
              {unreadCount > 0 && (
                <span className="absolute -right-1 -top-1 flex h-4 w-4 items-center justify-center rounded-full bg-red-500 text-[10px] font-medium text-white">
                  {unreadCount > 9 ? "9+" : unreadCount}
                </span>
              )}
            </Button>
          </DropdownMenuTrigger>
          {/* w-96: the row now carries pipeline + impact + action, not just a title. */}
          <DropdownMenuContent align="end" className="w-96">
            <DropdownMenuLabel className="flex items-center justify-between">
              <span>Notifications</span>
              {unreadCount > 0 && (
                <button
                  type="button"
                  onClick={(e) => { e.preventDefault(); void handleMarkAllRead() }}
                  className="text-xs font-normal text-violet-600 hover:underline dark:text-violet-400"
                >
                  Mark all read
                </button>
              )}
            </DropdownMenuLabel>
            <DropdownMenuSeparator />
            <div className="max-h-96 overflow-y-auto">
              {notifications.length === 0 ? (
                <div className="p-4 text-center text-sm text-zinc-500">
                  No new notifications
                </div>
              ) : (
                notifications.map((n) => (
                  <DropdownMenuItem
                    key={n.id}
                    onClick={() => void handleNotificationClick(n)}
                    className="flex cursor-pointer items-start gap-2 whitespace-normal py-2"
                  >
                    <span
                      aria-hidden
                      className={cn(
                        "mt-1.5 h-2 w-2 shrink-0 rounded-full",
                        n.read ? "bg-transparent" : severityDot(n.severity),
                      )}
                    />
                    {/*
                      Row anatomy, in the order a worried user reads it:
                      what happened (title) → which pipeline & when → the detail
                      → what it means for my data (impact) → what to do next.
                      Every string is resolved server-side from the notifier's
                      copy catalog, so a new notification type needs no edit
                      here. Older rows have empty impact/action_label/
                      pipeline_name and simply render the two-line layout.
                    */}
                    <div className="min-w-0 flex-1">
                      <p
                        className={cn(
                          "line-clamp-2 text-sm",
                          n.read ? "text-zinc-600 dark:text-zinc-400" : "font-medium",
                        )}
                      >
                        {n.title}
                      </p>
                      <p className="mt-0.5 text-[11px] text-zinc-400">
                        {n.pipeline_name ? (
                          <>
                            <span className="font-medium text-zinc-500 dark:text-zinc-400">
                              {n.pipeline_name}
                            </span>
                            {" · "}
                          </>
                        ) : null}
                        {formatRelativeTime(n.created_at)}
                      </p>
                      {n.message ? (
                        <p className="mt-1 line-clamp-3 text-xs text-zinc-500">{n.message}</p>
                      ) : null}
                      {n.impact ? (
                        <p className="mt-1 line-clamp-2 text-xs text-zinc-600 dark:text-zinc-300">
                          {n.impact}
                        </p>
                      ) : null}
                      <div className="mt-1.5 flex items-center justify-between gap-2">
                        {/*
                          Not a <button>: the whole DropdownMenuItem already
                          navigates to action_url, and nesting an interactive
                          element inside a menuitem breaks keyboard semantics.
                        */}
                        <span className="text-xs font-medium text-violet-600 dark:text-violet-400">
                          {n.action_label || "View pipeline"} →
                        </span>
                        {/*
                          The error code that used to BE the headline. Kept only
                          as a support reference, and only where it earns its
                          space — on a failure the user may need to report.
                        */}
                        {n.reference_code && isCritical(n.severity) ? (
                          <span
                            title="Quote this code when contacting support"
                            className="shrink-0 font-mono text-[10px] text-zinc-400"
                          >
                            {n.reference_code}
                          </span>
                        ) : null}
                      </div>
                    </div>
                  </DropdownMenuItem>
                ))
              )}
            </div>
          </DropdownMenuContent>
        </DropdownMenu>
        ) : (
          <Button variant="ghost" size="icon-sm" className="relative">
            <Bell className="h-4 w-4" />
          </Button>
        )}

        {/* User Menu - only render after mount to avoid hydration mismatch */}
        {mounted ? (
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button variant="ghost" className="relative h-8 w-8 rounded-full">
              <Avatar className="h-8 w-8">
                <AvatarImage src={currentUser?.avatarUrl} alt={currentUser?.name || "User"} />
                <AvatarFallback className="bg-violet-100 text-violet-700">
                  {initials}
                </AvatarFallback>
              </Avatar>
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" className="w-56">
            <DropdownMenuLabel>
              <div className="flex flex-col space-y-1">
                <p className="text-sm font-medium">{currentUser?.name || "User"}</p>
                <p className="text-xs text-zinc-500 dark:text-zinc-400">
                  {currentUser?.email}
                </p>
              </div>
            </DropdownMenuLabel>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => router.push("/settings")}>
              <User className="mr-2 h-4 w-4" />
              Profile
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => router.push("/settings")}>
              <Settings className="mr-2 h-4 w-4" />
              Settings
            </DropdownMenuItem>
            <DropdownMenuSeparator />
            <DropdownMenuItem 
              onClick={() => logout()}
              className="text-red-600 focus:text-red-600"
            >
              <LogOut className="mr-2 h-4 w-4" />
              Logout
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
        ) : (
          <Button variant="ghost" className="relative h-8 w-8 rounded-full">
            <Avatar className="h-8 w-8">
              <AvatarFallback className="bg-violet-100 text-violet-700">
                {initials}
              </AvatarFallback>
            </Avatar>
          </Button>
        )}
      </div>
    </header>

    {/* Create Workspace Dialog */}
    <Dialog open={showCreateDialog} onOpenChange={handleCreateDialogChange}>
      <DialogContent className="sm:max-w-md">
        {createdWorkspace ? (
          // ── Post-create: next steps ──────────────────────────────────────────
          <>
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                <Check className="h-5 w-5 text-emerald-500" />
                {createdWorkspace.name} is ready
              </DialogTitle>
            </DialogHeader>
            <p className="text-sm text-zinc-500">
              You&apos;re the owner of this workspace. Here&apos;s how to get going:
            </p>
            <div className="space-y-2 py-1">
              <button
                type="button"
                onClick={() => goAfterCreate("/workspace/settings?tab=members")}
                className="flex w-full items-center gap-3 rounded-lg border border-zinc-200 p-3 text-left transition-colors hover:border-violet-300 hover:bg-violet-50 dark:border-zinc-800 dark:hover:border-violet-700 dark:hover:bg-violet-950/20"
              >
                <UserPlus className="h-4 w-4 shrink-0 text-violet-500" />
                <span className="flex-1">
                  <span className="block text-sm font-medium">Invite your team</span>
                  <span className="block text-xs text-zinc-500">Add teammates and set their roles</span>
                </span>
                <ArrowRight className="h-4 w-4 shrink-0 text-zinc-400" />
              </button>
              <button
                type="button"
                onClick={() => goAfterCreate("/connections")}
                className="flex w-full items-center gap-3 rounded-lg border border-zinc-200 p-3 text-left transition-colors hover:border-violet-300 hover:bg-violet-50 dark:border-zinc-800 dark:hover:border-violet-700 dark:hover:bg-violet-950/20"
              >
                <Cable className="h-4 w-4 shrink-0 text-violet-500" />
                <span className="flex-1">
                  <span className="block text-sm font-medium">Create your first connection</span>
                  <span className="block text-xs text-zinc-500">Connect a source to start a pipeline</span>
                </span>
                <ArrowRight className="h-4 w-4 shrink-0 text-zinc-400" />
              </button>
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => handleCreateDialogChange(false)}>
                Done
              </Button>
            </DialogFooter>
          </>
        ) : (
          // ── Create form ──────────────────────────────────────────────────────
          <>
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                <Building2 className="h-5 w-5 text-violet-500" />
                New Workspace
              </DialogTitle>
            </DialogHeader>
            <p className="text-sm text-zinc-500">
              A workspace keeps a team&apos;s connections, pipelines and runs together. You&apos;ll be its
              owner and can invite teammates with their own roles.
            </p>
            <div className="space-y-2 py-2">
              <Label htmlFor="new-workspace-name">Workspace name</Label>
              <Input
                id="new-workspace-name"
                placeholder="e.g. Acme Corp"
                value={newWorkspaceName}
                onChange={(e) => setNewWorkspaceName(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && newWorkspaceName.trim() && void handleCreateWorkspace()}
                autoFocus
              />
              {newWorkspaceName.trim() && (
                <p data-testid="slug-preview" className="text-xs text-zinc-500">
                  URL slug:{" "}
                  <code className="rounded bg-zinc-100 px-1 py-0.5 font-mono text-zinc-700 dark:bg-zinc-800 dark:text-zinc-300">
                    {toWorkspaceSlug(newWorkspaceName)}
                  </code>
                </p>
              )}
            </div>
            <DialogFooter>
              <Button variant="outline" onClick={() => handleCreateDialogChange(false)}>
                Cancel
              </Button>
              <Button
                onClick={() => void handleCreateWorkspace()}
                disabled={creating || !newWorkspaceName.trim()}
                className="bg-gradient-to-r from-violet-600 to-indigo-600"
              >
                {creating ? "Creating…" : "Create"}
              </Button>
            </DialogFooter>
          </>
        )}
      </DialogContent>
    </Dialog>
  </>
  )
}
