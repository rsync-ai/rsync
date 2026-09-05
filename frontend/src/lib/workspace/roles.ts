/**
 * Workspace roles & permissions — the single client-side source of truth.
 *
 * Mirrors the backend hierarchy in `api-gateway/internal/security/workspace.go`:
 *   owner(4) > admin(3) > member(2) > viewer(1)
 * and the backend's fail-closed `Role.Meets()` — an unknown/empty role ranks 0,
 * below every real role, so a malformed role can never satisfy a permission check.
 *
 * The UI is advisory only: every mutation is independently enforced by the
 * api-gateway. These helpers exist so role-gated controls (disabled buttons,
 * tooltips, hidden sections, the read-only permissions matrix) stay consistent and
 * can't drift per-component. Before this module each component carried its own copy
 * of `roleRank`; that duplication is the bug class this replaces.
 */

export const WORKSPACE_ROLES = ["owner", "admin", "member", "viewer"] as const
export type WorkspaceRole = (typeof WORKSPACE_ROLES)[number]

/**
 * Actions the UI gates on role. Kept coarse — each maps to a backend guard:
 *  - view                 → membership (viewer+); read-only access
 *  - create_pipeline      → member+ ; create/run connections & pipelines (write)
 *  - run_write_query      → admin+  ; Data Explorer INSERT/UPDATE/DELETE/CREATE/ALTER
 *                           (validators.ValidateExplorerStatement DML/DDL gate)
 *  - run_destructive_query→ owner   ; Data Explorer DROP/TRUNCATE (owner-only + typed
 *                           confirm; validators destructive gate)
 *  - save_query           → member+ ; create a saved query (CreateSavedQuery). Viewers get a
 *                           read-only Explorer and must not plant SQL an admin might later run.
 *  - schedule_query       → admin+  ; attach/change/pause a schedule, set materialization, or
 *                           run a saved-query model (saved_query_models.go modelRunMinRole)
 *  - manage_members       → admin+  ; invite / remove / change-role (RemoveMember, ChangeMemberRole)
 *  - rename_workspace     → admin+  ; UpdateWorkspace (owner/admin)
 *  - delete_workspace     → owner   ; DeleteWorkspace (owner-only)
 *  - leave_workspace      → any member; self-leave (last-owner / personal nuances are
 *                           enforced by the backend with a 409 and handled at the call site)
 */
export type WorkspaceAction =
  | "view"
  | "create_pipeline"
  | "run_write_query"
  | "run_destructive_query"
  | "save_query"
  | "schedule_query"
  | "manage_members"
  | "rename_workspace"
  | "delete_workspace"
  | "leave_workspace"

const RANK: Readonly<Record<WorkspaceRole, number>> = {
  owner: 4,
  admin: 3,
  member: 2,
  viewer: 1,
}

/** Numeric rank for a role string. Unknown/empty => 0 (fails closed). */
export function roleRank(role: string): number {
  return RANK[role as WorkspaceRole] ?? 0
}

/** True when `role` is at least `min` in the hierarchy. Fails closed for unknown roles. */
export function meetsRole(role: string, min: WorkspaceRole): boolean {
  return roleRank(role) >= roleRank(min)
}

/**
 * Whether a caller with `role` may perform `action`. Mirrors the backend guards;
 * an unknown role or unknown action returns false (fail closed).
 */
export function can(role: string, action: WorkspaceAction): boolean {
  switch (action) {
    case "view":
      return meetsRole(role, "viewer")
    case "create_pipeline":
    case "save_query":
      return meetsRole(role, "member")
    case "run_write_query":
    case "schedule_query":
    case "manage_members":
    case "rename_workspace":
      return meetsRole(role, "admin")
    case "run_destructive_query":
    case "delete_workspace":
      return meetsRole(role, "owner")
    case "leave_workspace":
      // Any current member may attempt to leave; the backend rejects the last
      // owner (409) and the call site hides it on personal workspaces.
      return roleRank(role) >= roleRank("viewer")
    default:
      return false
  }
}

/**
 * Whether the caller may edit or delete a saved query. Mirrors the backend guard in
 * `saved_queries.go` (UpdateSavedQuery / DeleteSavedQuery):
 *
 *   if existing.CreatedBy != userID && !role.Meets(security.WSAdmin) { 403 }
 *
 * i.e. the creator may always edit their own, and workspace admins may edit anyone's.
 * Deliberately NOT a plain `can()` action: it depends on row ownership, not role alone.
 * Fails closed while the current user is still loading (`userId` empty), so the control
 * never flashes enabled for someone who cannot use it.
 */
export function canEditSavedQuery(
  role: string,
  createdBy: string | null | undefined,
  userId: string | null | undefined,
): boolean {
  const uid = String(userId ?? "").trim()
  if (uid && uid === String(createdBy ?? "").trim()) return true
  return meetsRole(role, "admin")
}

export const ROLE_LABELS: Readonly<Record<WorkspaceRole, string>> = {
  owner: "Owner",
  admin: "Admin",
  member: "Member",
  viewer: "Viewer",
}

export const ROLE_DESCRIPTIONS: Readonly<Record<WorkspaceRole, string>> = {
  owner: "Full control, including renaming, deleting and transferring the workspace.",
  admin: "Invite and manage members, rename the workspace, and run pipelines.",
  member: "Create and run connections and pipelines. Cannot manage members.",
  viewer: "Read-only access to connections, pipelines and runs.",
}

/** One row of the read-only permissions matrix shown on the Roles tab. */
export interface CapabilityRow {
  key: string
  label: string
  /** Minimum role that has this capability. */
  minRole: WorkspaceRole
}

/** Capability rows, ordered least → most privileged (drives the matrix columns of ticks). */
export const WORKSPACE_CAPABILITY_ROWS: readonly CapabilityRow[] = [
  { key: "view", label: "View connections, pipelines & runs", minRole: "viewer" },
  { key: "run", label: "Create & run connections and pipelines", minRole: "member" },
  // Data Explorer write queries mirror the backend role-aware statement gate:
  // INSERT/UPDATE/DELETE/CREATE/ALTER require admin, DROP/TRUNCATE require owner.
  // The admin-gated write row sits with the other admin rows; the owner-only
  // DROP/TRUNCATE row lives below with the other owner-only capabilities so the
  // array stays ordered least → most privileged.
  { key: "explorer_write", label: "Run Explorer write queries (INSERT/UPDATE/DELETE, DDL)", minRole: "admin" },
  { key: "members", label: "Invite & manage members", minRole: "admin" },
  { key: "rename", label: "Rename the workspace", minRole: "admin" },
  // Connector generation is a workspace admin+ capability — mirrors the backend
  // WorkspaceGeneratorMiddleware gate (owner/admin) added in #430.
  { key: "generate", label: "Generate connectors", minRole: "admin" },
  // Owner-only capabilities (rank 4) — kept last to preserve the ascending order.
  { key: "explorer_destructive", label: "Run Explorer DROP / TRUNCATE", minRole: "owner" },
  { key: "delete", label: "Delete the workspace & transfer ownership", minRole: "owner" },
]

/** Whether `role` satisfies a capability row's minimum. Thin wrapper over meetsRole. */
export function roleHasCapability(role: string, minRole: WorkspaceRole): boolean {
  return meetsRole(role, minRole)
}

/** Badge styling tier for a role chip (kept here so the look is consistent everywhere). */
export function roleBadgeVariant(role: string): "default" | "secondary" | "outline" {
  if (role === "owner") return "default"
  if (role === "admin") return "secondary"
  return "outline"
}
