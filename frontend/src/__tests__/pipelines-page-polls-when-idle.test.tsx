/**
 * Prod retest 2026-09-18: a CDC pipeline read "Failed" on /pipelines and stayed
 * Failed after it recovered — the list polled only while some row was running, so
 * with the one stream failed nothing ever re-fetched; the stats cards fetched once
 * on mount and never again. The page now always polls (fast while a run is in
 * flight, slow otherwise) and refreshes the cards on every tick.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { act, render, screen, within } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({ role: "admin", can: () => true }),
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

import { authFetch } from "@/lib/api/auth-fetch"
import { PipelinesPageClient, pipelinesPollInterval } from "@/app/(dashboard)/pipelines/PipelinesPageClient"
import type { PipelineListItem } from "@/components/pipeline/PipelinesTable"

const mockAuthFetch = vi.mocked(authFetch)

function row(derived_status: string): PipelineListItem {
  return {
    id: "p1",
    name: "orders-sync",
    pipeline_status: "active",
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-01T00:00:00Z",
    derived_status,
    sync_mode: "cdc",
  }
}

function json(body: unknown): Response {
  return { ok: true, status: 200, json: async () => body } as unknown as Response
}

let listStatus = "failed"
let failedCount = 1

function calls(fragment: string): number {
  return mockAuthFetch.mock.calls.filter(([url]) => String(url).includes(fragment)).length
}

describe("pipelinesPollInterval", () => {
  it("polls fast while a run is in flight and slowly otherwise — never off", () => {
    expect(pipelinesPollInterval([row("running")])).toBe(5_000)
    expect(pipelinesPollInterval([row("waiting_for_user")])).toBe(5_000)
    expect(pipelinesPollInterval([row("failed")])).toBe(30_000)
    expect(pipelinesPollInterval([])).toBe(30_000)
  })
})

describe("/pipelines — a Failed row is not frozen", () => {
  beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true })
    listStatus = "failed"
    failedCount = 1
    mockAuthFetch.mockReset()
    mockAuthFetch.mockImplementation(async (url) => {
      if (String(url).includes("/pipelines/stats")) {
        return json({ pipelines: { total: 1, active: 1 }, executions: { running: 0, completed: 0, failed: failedCount } })
      }
      return json({ pipelines: [row(listStatus)], total: 1 })
    })
  })
  afterEach(() => {
    vi.useRealTimers()
  })

  it("re-fetches the list and the stats cards with nothing running, so recovery shows", async () => {
    render(<PipelinesPageClient />)
    const table = await screen.findByRole("table")
    expect(await within(table).findByText(/^failed$/i)).toBeInTheDocument()
    const listBefore = calls("/api/v1/pipelines?")
    const statsBefore = calls("/pipelines/stats")

    // The stream recovers out of band (healer / Reload / another tab).
    listStatus = "running"
    failedCount = 0
    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_000)
    })

    expect(calls("/api/v1/pipelines?")).toBeGreaterThan(listBefore)
    expect(calls("/pipelines/stats")).toBeGreaterThan(statsBefore)
    expect(await within(table).findByText(/^running$/i)).toBeInTheDocument()
    expect(within(table).queryByText(/^failed$/i)).not.toBeInTheDocument()
  })
})
