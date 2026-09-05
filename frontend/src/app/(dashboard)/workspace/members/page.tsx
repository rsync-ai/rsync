"use client"

import { useCallback, useEffect, useState } from "react"
import { PageHeader } from "@/components/layout/PageHeader"
import { WorkspaceMembers } from "@/components/workspace/WorkspaceMembers"
import { Button } from "@/components/ui/button"
import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { getActiveWorkspaceId, onActiveWorkspaceChange } from "@/lib/workspace/active-workspace"
import { Loader2 } from "lucide-react"

interface Workspace {
  id: string
  name: string
  slug?: string
  role: string
  is_personal?: boolean
}

// Members management always targets the ACTIVE workspace (chosen in the header
// switcher). We resolve it from the workspaces list so we also get the caller's
// role for that workspace, which gates the admin-only actions in the component.
export default function WorkspaceMembersPage() {
  const [workspace, setWorkspace] = useState<Workspace | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(false)

  // Resolve the ACTIVE workspace from the list (so we also get the caller's role).
  // Re-runnable: used on mount AND whenever the active workspace is switched in place.
  //
  // Deliberately needs no captureWorkspace() staleness guard, unlike the other
  // workspace-scoped pages: the response is the FULL workspace list (identical
  // whichever workspace was active when it was sent), and the selection is made
  // from getActiveWorkspaceId() read AFTER the await. A response that lands late
  // therefore still resolves to the current workspace rather than the old one.
  const resolve = useCallback(async () => {
    setError(false)
    try {
      const res = await authFetch(API_ENDPOINTS.WORKSPACES.LIST)
      if (!res.ok) {
        setError(true)
        return
      }
      const data = (await res.json()) as { workspaces?: Workspace[] }
      const list = data.workspaces ?? []
      const savedId = getActiveWorkspaceId()
      const found = list.find((w) => w.id === savedId) ?? list[0] ?? null
      setWorkspace(found)
    } catch {
      setError(true)
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void resolve()
  }, [resolve])

  // Re-resolve when the active workspace changes (header switcher, other tab) so the
  // page never shows stale members — and an invite never targets the old workspace.
  useEffect(() => onActiveWorkspaceChange(() => void resolve()), [resolve])

  return (
    <div className="space-y-6">
      <PageHeader
        heading="Workspace members"
        description="Manage who has access to your workspace and invite teammates."
      />
      {loading ? (
        <div className="flex items-center gap-2 text-sm text-zinc-500">
          <Loader2 className="h-4 w-4 animate-spin" />
          Loading…
        </div>
      ) : error ? (
        <div className="flex items-center gap-3 text-sm">
          <span className="text-red-600 dark:text-red-400">
            Couldn&apos;t load your workspaces — try again.
          </span>
          <Button variant="outline" size="sm" onClick={() => void resolve()}>
            Retry
          </Button>
        </div>
      ) : workspace ? (
        <WorkspaceMembers
          workspaceId={workspace.id}
          workspaceName={workspace.name}
          currentRole={workspace.role}
          isPersonal={workspace.is_personal}
        />
      ) : (
        <p className="text-sm text-zinc-500">
          No workspace selected. Use the workspace switcher in the header to pick one.
        </p>
      )}
    </div>
  )
}
