"use client"

/**
 * The editable Plan cell on /admin/usage.
 *
 * Split out of the page so the select and the plan-option union can be tested
 * without standing up the whole admin usage table behind its feature gate.
 */

import { useState } from "react"
import { toast } from "sonner"

import { adminSetWorkspacePlan, KNOWN_PLAN_NAMES } from "@/lib/api/admin"
import type { AdminWorkspaceUsage } from "@/lib/api/usage"

const nf = new Intl.NumberFormat()
const fmt = (n: number) => nf.format(n)

const selectClass =
  "h-7 rounded-md border border-zinc-300 bg-white px-1.5 text-xs capitalize disabled:opacity-50 dark:border-zinc-700 dark:bg-zinc-900 focus:outline-none focus:ring-2 focus:ring-blue-500"

/**
 * Options for the plan picker: the catalogue the migrations ship, plus any plan
 * name actually present on a workspace row.
 *
 * The union is the point. There is no endpoint that lists `plans`, so the
 * hard-coded half can only go stale one way — a plan added to the catalogue
 * later would be missing. Any workspace already ON such a plan puts it back in
 * the list, and the server rejects a name it does not know with a 400, so a
 * stale list can never silently write a bad tier.
 */
export function planOptionsFrom(workspaces: AdminWorkspaceUsage[]): string[] {
  const seen = new Set<string>(KNOWN_PLAN_NAMES)
  for (const w of workspaces) {
    if (w.plan) seen.add(w.plan)
    if (w.effective_plan) seen.add(w.effective_plan)
  }
  return Array.from(seen).sort()
}

/**
 * The Plan cell: the stored plan, editable, plus the read-only annotations that
 * were already here (effective plan when it differs, the limit, the override).
 *
 * POST /admin/workspaces/:id/plan existed with no caller — this is its UI. It is
 * a platform-admin action, not a workspace-role one, so it lives on this page
 * and nowhere a workspace admin could reach it.
 */
export function WorkspacePlanCell({
  w,
  options,
  onSaved,
}: {
  w: AdminWorkspaceUsage
  options: string[]
  onSaved: () => Promise<void> | void
}) {
  const [saving, setSaving] = useState(false)
  const current = w.plan || ""

  async function onChange(next: string) {
    if (!next || next === current) return
    setSaving(true)
    try {
      await adminSetWorkspacePlan(w.workspace_id, next)
      // Say the side effect out loud: the server also clears plan_expires_at, so
      // a manual grant stops being dated. An admin who just "fixed a tier" would
      // not otherwise know the expiry went with it.
      toast.success(`Plan set to ${next}. Any plan expiry was cleared.`)
      await onSaved()
    } catch (err) {
      // The 400 {"error":"unknown plan"} lands here verbatim — that is the
      // backstop for this list drifting behind the catalogue.
      toast.error(err instanceof Error ? err.message : "Failed to set plan")
      setSaving(false)
    }
  }

  return (
    <>
      <select
        aria-label={`Plan for ${w.name || w.workspace_id}`}
        className={selectClass}
        value={current}
        disabled={saving}
        onChange={(e) => onChange(e.target.value)}
      >
        {current === "" ? (
          <option value="" disabled>
            —
          </option>
        ) : null}
        {options.map((p) => (
          <option key={p} value={p}>
            {p}
          </option>
        ))}
      </select>
      {/* The stored plan has lapsed and something else is
          being enforced. Show both — the stored value is
          what an admin edits, the effective one is what
          the limit beside it refers to. */}
      {w.effective_plan && w.effective_plan !== w.plan ? (
        <span className="ml-1 text-xs text-amber-600 dark:text-amber-500">
          → <span className="capitalize">{w.effective_plan}</span>
        </span>
      ) : null}
      <span className="ml-2 text-xs text-zinc-500 dark:text-zinc-400">
        {w.plan_limit == null ? "∞" : `${fmt(w.pipelines)}/${fmt(w.plan_limit)}`}
      </span>
      {/* An override is invisible otherwise: the number
          to its left is the override's, and an admin
          reading the table could not tell which
          workspaces had been granted one. */}
      {w.pipeline_limit_override != null ? (
        <span
          className="ml-1 text-xs text-sky-600 dark:text-sky-400"
          title="Pipeline limit override — this workspace's limit is set per-workspace, not by its plan"
        >
          override
        </span>
      ) : null}
    </>
  )
}
