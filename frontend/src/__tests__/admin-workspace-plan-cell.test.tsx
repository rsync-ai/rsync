/**
 * The editable Plan cell on /admin/usage — the UI for
 * POST /admin/workspaces/:id/plan, which shipped with zero callers. Until this
 * cell existed, moving a workspace between tiers meant an UPDATE by hand
 * against the prod database.
 *
 * The interesting risk is the option list. There is NO endpoint that lists the
 * `plans` table, so the frontend cannot fetch the catalogue; the list is seeded
 * from migrations 060 and 071 and can only go stale in one direction — a plan
 * added later would be missing from the select. `planOptionsFrom` unions the
 * seed with every plan name observed on a workspace row, which puts such a plan
 * back in the list as soon as any workspace is on it, and the server's
 * 400 {"error":"unknown plan"} is the backstop for the rest. Both halves are
 * tested here.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

import { planOptionsFrom, WorkspacePlanCell } from "@/components/admin/WorkspacePlanCell"
import type { AdminWorkspaceUsage } from "@/lib/api/usage"

const mockFetch = vi.fn()
vi.stubGlobal("fetch", mockFetch)

const toastSuccess = vi.fn()
const toastError = vi.fn()
vi.mock("sonner", () => ({
  toast: {
    success: (...a: unknown[]) => toastSuccess(...a),
    error: (...a: unknown[]) => toastError(...a),
  },
}))

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    text: async () => JSON.stringify(body),
    json: async () => body,
  } as unknown as Response
}

function ws(over: Partial<AdminWorkspaceUsage> = {}): AdminWorkspaceUsage {
  return {
    workspace_id: "w1",
    name: "Acme",
    is_personal: false,
    plan: "free",
    effective_plan: "free",
    plan_limit: 3,
    pipeline_limit_override: null,
    pipelines: 1,
    rows_read: 0,
    rows_written: 0,
    records_processed: 0,
    transfer_bytes: 0,
    transfer_gb: 0,
    queries: 0,
    ...over,
  }
}

beforeEach(() => {
  mockFetch.mockReset()
  toastSuccess.mockReset()
  toastError.mockReset()
})

describe("planOptionsFrom", () => {
  it("offers the catalogue the migrations ship", () => {
    // 060_user_plans.sql (trial, free, pro) + 071 (starter).
    expect(planOptionsFrom([])).toEqual(["free", "pro", "starter", "trial"])
  })

  it("adds a plan name observed on a workspace but missing from the seed", () => {
    // The one direction the hard-coded half can go stale: a tier added to the
    // `plans` table after this list was written.
    const opts = planOptionsFrom([ws({ plan: "enterprise" }), ws({ workspace_id: "w2", effective_plan: "legacy" })])
    expect(opts).toContain("enterprise")
    expect(opts).toContain("legacy")
    expect(opts).toContain("free")
  })

  it("does not duplicate a plan that is both seeded and observed", () => {
    const opts = planOptionsFrom([ws({ plan: "pro" }), ws({ workspace_id: "w2", plan: "pro" })])
    expect(opts.filter((p) => p === "pro")).toHaveLength(1)
  })
})

describe("WorkspacePlanCell", () => {
  it("POSTs the chosen plan and reloads", async () => {
    mockFetch.mockResolvedValue(res(200, { success: true, workspace_id: "w1", plan: "pro" }))
    const onSaved = vi.fn()

    render(<WorkspacePlanCell w={ws()} options={planOptionsFrom([])} onSaved={onSaved} />)

    await userEvent.selectOptions(screen.getByLabelText("Plan for Acme"), "pro")

    await waitFor(() => expect(mockFetch).toHaveBeenCalled())
    const [url, init] = mockFetch.mock.calls[0] as [string, RequestInit]
    expect(String(url)).toContain("/api/v1/admin/workspaces/w1/plan")
    expect(init.method).toBe("POST")
    expect(JSON.parse(String(init.body))).toEqual({ plan: "pro" })
    await waitFor(() => expect(onSaved).toHaveBeenCalled())
  })

  it("says out loud that the expiry was cleared", async () => {
    // AdminSetWorkspacePlan also sets plan_expires_at = NULL. An admin who
    // just "fixed a tier" would not otherwise know a dated grant went with it.
    mockFetch.mockResolvedValue(res(200, { success: true, workspace_id: "w1", plan: "pro" }))

    render(
      <WorkspacePlanCell w={ws({ plan_expires_at: "2026-12-01T00:00:00Z" })} options={planOptionsFrom([])} onSaved={vi.fn()} />
    )

    await userEvent.selectOptions(screen.getByLabelText("Plan for Acme"), "pro")

    await waitFor(() => expect(toastSuccess).toHaveBeenCalled())
    expect(String(toastSuccess.mock.calls[0][0])).toMatch(/expiry was cleared/i)
  })

  it("surfaces the server's 'unknown plan' rejection instead of showing a silent success", async () => {
    // The backstop for the seeded list drifting behind the catalogue: if the
    // select ever offers a name the server does not know, the admin must see
    // the 400, not a cell that appears to have saved.
    mockFetch.mockResolvedValue(res(400, { error: "unknown plan", plan: "platinum" }))
    const onSaved = vi.fn()

    render(<WorkspacePlanCell w={ws()} options={["free", "platinum"]} onSaved={onSaved} />)

    await userEvent.selectOptions(screen.getByLabelText("Plan for Acme"), "platinum")

    await waitFor(() => expect(toastError).toHaveBeenCalledWith("unknown plan"))
    expect(onSaved).not.toHaveBeenCalled()
    expect(toastSuccess).not.toHaveBeenCalled()
  })

  it("keeps the read-only annotations that were already in this cell", async () => {
    render(
      <WorkspacePlanCell
        w={ws({ plan: "pro", effective_plan: "free", plan_limit: 10, pipeline_limit_override: 10, pipelines: 4 })}
        options={planOptionsFrom([])}
        onSaved={vi.fn()}
      />
    )

    // The stored plan is what the select edits; the effective one is what the
    // limit beside it refers to.
    expect(screen.getByLabelText("Plan for Acme")).toHaveValue("pro")
    // Scoped to the arrow annotation on purpose: a bare getByText("free")
    // also matches the <option>, so it would pass even with the annotation
    // deleted.
    expect(screen.getByText(/→/)).toHaveTextContent("free")
    expect(screen.getByText("4/10")).toBeInTheDocument()
    expect(screen.getByText("override")).toBeInTheDocument()
  })
})
