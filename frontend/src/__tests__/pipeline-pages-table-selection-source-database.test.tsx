/**
 * #14 — the table picker must say which database it lists, on the pipeline pages
 * as well as in chat.
 *
 * A table-selection pause carries the database in blocking_reason.details
 * (executor.go tableSelectionResult → PIPELINE_WAITING details → /state). The
 * live-state panel opens the picker by itself; the monitoring panel opens it from
 * "Select tables". Both must hand the name to the picker. A PostgreSQL source is
 * used on purpose: its tables carry the schema ("public"), so "Tables in billing"
 * can only come from source_database. Each page has a control without it.
 */

import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  useSearchParams: () => new URLSearchParams(),
  usePathname: () => "/pipelines/p1",
}))

vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: null, loading: false, error: null }),
}))

// The monitoring panel's tabs are not what is under test and pull in their own fetches.
vi.mock("@/components/pipeline/MonitoringOverviewTab", () => ({ MonitoringOverviewTab: () => null }))
vi.mock("@/components/pipeline/TableStatisticsPanel", () => ({ TableStatisticsPanel: () => null }))
vi.mock("@/components/pipeline/ReasoningTimeline", () => ({ ReasoningTimeline: () => null }))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
  authFetchOrThrow: (...args: unknown[]) => authFetch(...args),
}))

import { PipelineLiveStatePanel } from "@/components/pipeline/PipelineLiveStatePanel"
import { PipelineMonitoringPanel } from "@/components/pipeline/PipelineMonitoringPanel"

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

function jsonOk(body: unknown) {
  return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) }
}

const TABLES = [
  { name: "invoices", schema: "public", row_count: 120, columns: 6 },
  { name: "payments", schema: "public", row_count: 80, columns: 5 },
]

// The /state of a pipeline parked on table selection, as pipeline_state.go serves it.
function waitingState(details: Record<string, unknown>) {
  return {
    schema_version: 1,
    pipeline_id: "p1",
    execution_id: "e1",
    status: "waiting_for_user",
    current_stage: "executor",
    message: "Select which table(s)/resource(s) to sync (2 available).",
    progress: { percent: 40 },
    created_at: "2026-09-17T09:00:00Z",
    updated_at: "2026-09-17T09:01:00Z",
    blocking_reason: {
      type: "table_selection",
      description: "Select which table(s)/resource(s) to sync (2 available).",
      available_tables: TABLES,
      details: {
        request_type: "table_selection",
        action_needed: "table_selection",
        source_type: "postgresql",
        available_tables: TABLES,
        ...details,
      },
    },
  }
}

function routeWith(state: Record<string, unknown>, pipeline: Record<string, unknown> = {}) {
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/state")) return jsonOk(state)
    if (u.includes("/events")) return jsonOk({ events: [] })
    if (/\/pipelines\/p1(\?|$)/.test(u)) return jsonOk({ id: "p1", name: "billing sync", sync_mode: "full", ...pipeline })
    return jsonOk({})
  })
}

beforeEach(() => authFetch.mockReset())
afterEach(() => cleanup())

describe("PipelineLiveStatePanel table picker", () => {
  it("names the database the pause lists", async () => {
    routeWith(waitingState({ source_database: "billing" }))
    render(<PipelineLiveStatePanel pipelineId="p1" />)
    expect(await screen.findByText("Tables in billing")).toBeInTheDocument()
  })

  it("control: without source_database it can only name the schema", async () => {
    routeWith(waitingState({}))
    render(<PipelineLiveStatePanel pipelineId="p1" />)
    expect(await screen.findByText("Tables in schema public")).toBeInTheDocument()
    expect(screen.queryByText("Tables in billing")).toBeNull()
  })
})

// A server-level source (a connection naming no database) mirrors each source
// database at the destination even when one is picked, so the picker leaves the
// destination name optional instead of demanding one the executor would not use.
describe("PipelineLiveStatePanel destination name for a server-level source", () => {
  const intoPostgres = {
    destination_config: { namespace: "public", namespace_kind: "schema", create_if_not_exists: true },
    destination_connection: { connector_type: "postgresql" },
  }
  const pick = async () =>
    userEvent.click(within((await screen.findByText("public.invoices")).closest("label") as HTMLElement).getByRole("checkbox"))

  it("is optional and blank when the pause says the source is server-level", async () => {
    routeWith(waitingState({ source_type: "mysql", source_server_level: true }), intoPostgres)
    render(<PipelineLiveStatePanel pipelineId="p1" />)
    await pick()
    await waitFor(() => expect(screen.getByLabelText(/Schema name/i)).toHaveValue(""))
    expect(screen.getByText(/\(optional\)/i)).toBeInTheDocument()
  })

  it("control: without the flag the seeded name stays and is required", async () => {
    routeWith(waitingState({ source_type: "mysql" }), intoPostgres)
    render(<PipelineLiveStatePanel pipelineId="p1" />)
    await pick()
    await waitFor(() => expect(screen.getByLabelText(/Schema name/i)).toHaveValue("public"))
    expect(screen.queryByText(/\(optional\)/i)).toBeNull()
  })
})

// "Select tables" lives on the Table statistics page (variant="table_stats").
describe("PipelineMonitoringPanel table picker", () => {
  it("names the database the pause lists", async () => {
    routeWith(waitingState({ source_database: "billing" }))
    render(<PipelineMonitoringPanel pipelineId="p1" variant="table_stats" />)
    await userEvent.click(await screen.findByRole("button", { name: "Select tables" }))
    expect(await screen.findByText("Tables in billing")).toBeInTheDocument()
  })

  it("control: without source_database it can only name the schema", async () => {
    routeWith(waitingState({}))
    render(<PipelineMonitoringPanel pipelineId="p1" variant="table_stats" />)
    await userEvent.click(await screen.findByRole("button", { name: "Select tables" }))
    expect(await screen.findByText("Tables in public")).toBeInTheDocument()
    expect(screen.queryByText("Tables in billing")).toBeNull()
  })
})
