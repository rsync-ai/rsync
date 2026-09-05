"use client"

import { useSearchParams } from "next/navigation"
import { PageHeader } from "@/components/layout/PageHeader"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Badge } from "@/components/ui/badge"
import { Loader2 } from "lucide-react"
import { useWorkspace } from "@/contexts/WorkspaceContext"
import { WorkspaceGeneralSettings } from "@/components/workspace/WorkspaceGeneralSettings"
import { WorkspaceMembers } from "@/components/workspace/WorkspaceMembers"
import { WorkspaceRolesMatrix } from "@/components/workspace/WorkspaceRolesMatrix"
import { ROLE_LABELS, roleBadgeVariant, meetsRole, type WorkspaceRole } from "@/lib/workspace/roles"

const VALID_TABS = ["general", "members", "roles"] as const
type SettingsTab = (typeof VALID_TABS)[number]

/**
 * Workspace settings hub for the ACTIVE workspace (chosen in the header switcher).
 * Tabs: General (rename/leave), Members & invitations, Roles & permissions. The
 * active workspace + the caller's role come from the shared WorkspaceProvider, so
 * this page never re-fetches them. /workspace/members redirects here (?tab=members).
 */
export default function WorkspaceSettingsPage() {
  const { activeWorkspace, isLoading, error, refresh } = useWorkspace()
  const searchParams = useSearchParams()

  const requested = searchParams.get("tab")
  const initialTab: SettingsTab = (VALID_TABS as readonly string[]).includes(requested ?? "")
    ? (requested as SettingsTab)
    : "general"

  if (isLoading) {
    return (
      <div className="space-y-6">
        <PageHeader heading="Workspace settings" description="Manage your workspace, members and roles." />
        <div className="flex items-center gap-2 text-sm text-zinc-500">
          <Loader2 className="h-4 w-4 animate-spin" />
          Loading…
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="space-y-6">
        <PageHeader heading="Workspace settings" description="Manage your workspace, members and roles." />
        <div className="flex items-center gap-3 text-sm">
          <span className="text-red-600 dark:text-red-400">Couldn&apos;t load your workspaces — try again.</span>
          <button
            className="rounded border border-zinc-300 px-2 py-1 text-xs hover:bg-zinc-50 dark:border-zinc-700 dark:hover:bg-zinc-800"
            onClick={() => void refresh()}
          >
            Retry
          </button>
        </div>
      </div>
    )
  }

  if (!activeWorkspace) {
    return (
      <div className="space-y-6">
        <PageHeader heading="Workspace settings" description="Manage your workspace, members and roles." />
        <p className="text-sm text-zinc-500">
          No workspace selected. Use the workspace switcher in the header to pick one.
        </p>
      </div>
    )
  }

  const role = activeWorkspace.role
  const roleLabel = ROLE_LABELS[role as WorkspaceRole] ?? role
  const isReadOnly = !meetsRole(role, "member")

  return (
    <div className="space-y-6">
      <PageHeader
        heading="Workspace settings"
        description={`Manage "${activeWorkspace.name}" — members, roles and workspace details.`}
      />

      {/* Persistent role banner so the caller always knows their access level. */}
      <div className="flex flex-wrap items-center gap-2 rounded-lg border border-zinc-200 bg-zinc-50 px-3 py-2 text-sm dark:border-zinc-800 dark:bg-zinc-900/50">
        <span className="text-zinc-500">Your role in this workspace:</span>
        <Badge variant={roleBadgeVariant(role)}>{roleLabel}</Badge>
        {isReadOnly && (
          <span className="text-xs text-zinc-500">— read-only access; ask an admin for more.</span>
        )}
      </div>

      <Tabs defaultValue={initialTab} className="space-y-4">
        <TabsList>
          <TabsTrigger value="general">General</TabsTrigger>
          <TabsTrigger value="members">Members &amp; invitations</TabsTrigger>
          <TabsTrigger value="roles">Roles &amp; permissions</TabsTrigger>
        </TabsList>

        <TabsContent value="general">
          <WorkspaceGeneralSettings
            workspaceId={activeWorkspace.id}
            workspaceName={activeWorkspace.name}
            slug={activeWorkspace.slug}
            currentRole={role}
            isPersonal={activeWorkspace.is_personal}
          />
        </TabsContent>

        <TabsContent value="members">
          <WorkspaceMembers
            workspaceId={activeWorkspace.id}
            workspaceName={activeWorkspace.name}
            currentRole={role}
            isPersonal={activeWorkspace.is_personal}
          />
        </TabsContent>

        <TabsContent value="roles">
          <WorkspaceRolesMatrix currentRole={role} />
        </TabsContent>
      </Tabs>
    </div>
  )
}
