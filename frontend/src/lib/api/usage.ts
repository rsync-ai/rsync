import { authGet } from "./auth-fetch"

// Consumption / usage read clients. Mirrors admin.ts: typed interfaces + a
// relative-path authGet call (authFetch injects the X-Workspace-ID header, so
// GET /api/v1/usage is automatically scoped to the ACTIVE workspace).

// ── Per-workspace (any member) ──────────────────────────────────────────────

export interface PipelineUsage {
  id: string
  name: string
  status: string
  rows_read: number
  rows_written: number
  records_processed: number
  cdc_inserts: number
  cdc_updates: number
  cdc_deletes: number
  cdc_applied: number
  last_activity?: string | null
}

export interface WorkspaceUsage {
  workspace_id: string
  plan: string
  /** null = unlimited (pro). */
  pipeline_limit: number | null
  pipelines_used: number
  can_run: boolean
  blocked: boolean
  plan_expires_at?: string | null
  /** NL→SQL queries generated this month (direct SQL is unlimited, not counted). */
  queries_used: number
  /** plan's included monthly NL→SQL query allowance; null = unlimited/unknown */
  queries_limit?: number | null
  transfer: {
    rows_read: number
    rows_written: number
    records_processed: number
    cdc_events_applied: number
    /** destination-committed bytes this month */
    bytes?: number
    /** GB transferred this month (the billed dimension) */
    gb: number | null
    gb_metered: boolean
    /** plan's included GB allowance; null = unlimited/unknown */
    gb_limit?: number | null
  }
  pipelines: PipelineUsage[]
  /** When true, "to-date" totals only cover the last retention_days days. */
  retention_enabled: boolean
  retention_days: number
}

export async function getWorkspaceUsage(): Promise<WorkspaceUsage> {
  return authGet<WorkspaceUsage>("/api/v1/usage")
}

// ── Platform admin (cross-workspace / cross-user) ───────────────────────────

export interface AdminWorkspaceUsage {
  workspace_id: string
  name: string
  is_personal: boolean
  /** The plan as STORED on the workspace row. */
  plan: string
  /**
   * The plan after the expiry cascade — what is actually enforced right now.
   * Differs from `plan` when a dated plan has lapsed and not yet been persisted.
   */
  effective_plan: string
  /**
   * The limit that will actually be ENFORCED, resolved through the same code
   * enforcement uses: `pipeline_limit_override` when present, otherwise the
   * effective plan's catalogue limit. null = unlimited or unknown.
   */
  plan_limit: number | null
  /** The per-workspace grant, when one exists. When set it IS `plan_limit`. */
  pipeline_limit_override: number | null
  plan_expires_at?: string | null
  pipelines: number
  rows_read: number
  rows_written: number
  records_processed: number
  /** destination-committed transfer this month */
  transfer_bytes: number
  transfer_gb: number
  /** NL→SQL queries this month */
  queries: number
  last_activity?: string | null
}

export interface AdminUserUsage {
  user_id: string
  email: string
  plan: string
  pipelines: number
  rows_read: number
  rows_written: number
  records_processed: number
  last_activity?: string | null
}

export interface AdminUsageResponse {
  workspaces: AdminWorkspaceUsage[]
  users: AdminUserUsage[]
  gb_metered: boolean
  retention_enabled: boolean
  retention_days: number
}

export async function getAdminUsage(): Promise<AdminUsageResponse> {
  return authGet<AdminUsageResponse>("/api/v1/admin/usage")
}
