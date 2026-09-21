import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

// The menu polls /state (for CDC Stop visibility) and uses next/navigation.
const jsonRes = (data: unknown) => ({ ok: true, status: 200, json: async () => data })
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
const nav = vi.hoisted(() => ({ search: "", replace: vi.fn() }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: nav.replace, refresh: vi.fn() }),
  usePathname: () => "/pipelines/p1",
  useSearchParams: () => new URLSearchParams(nav.search),
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { PipelineHeaderOverflowMenu } from "../PipelineHeaderOverflowMenu"

// The live-status poll (CDC only) drives Stop visibility; default it to the
// pipeline's running state so tests read the state they set up.
function mockLiveStatus(status: string) {
  vi.mocked(authFetch).mockResolvedValue(jsonRes({ status }) as never)
}

async function openMenu(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: /open pipeline menu/i }))
}

// This is the ONE consolidated header overflow menu. The regression it guards:
// CDC pipelines used to render a SECOND kebab (CDCPipelineActions' own menu) next
// to this one. Stop was folded in here so CDC keeps a single "⋯".
describe("PipelineHeaderOverflowMenu — single consolidated menu", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    nav.search = ""
    mockLiveStatus("running")
  })

  it("always offers Rename, Edit Tables and Delete", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="P1" />)
    await openMenu(user)
    expect(screen.getByRole("menuitem", { name: /rename/i })).toBeInTheDocument()
    expect(screen.getByRole("menuitem", { name: /edit tables/i })).toBeInTheDocument()
    expect(screen.getByRole("menuitem", { name: /^delete$/i })).toBeInTheDocument()
  })

  // The drift policy card lives on the schema-changes page; the header's drift
  // badge only shows while something is pending, so this is the route to it.
  it("links Schema change alerts to the pipeline's schema-changes page", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="P1" />)
    await openMenu(user)
    expect(screen.getByRole("menuitem", { name: /schema change alerts/i })).toHaveAttribute(
      "href",
      "/pipelines/p1/schema-changes"
    )
  })

  it("offers Stop Pipeline for a running CDC pipeline (folded in from the old CDC kebab)", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="P1" pipelineType="cdc" status="running" />)
    await openMenu(user)
    expect(screen.getByRole("menuitem", { name: /stop pipeline/i })).toBeInTheDocument()
  })

  it("does NOT offer Stop for a non-CDC pipeline (ETL keeps its inline Stop button)", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="P1" pipelineType="etl" status="running" />)
    await openMenu(user)
    expect(screen.queryByRole("menuitem", { name: /stop pipeline/i })).not.toBeInTheDocument()
    // ...but the shared items are still there.
    expect(screen.getByRole("menuitem", { name: /edit tables/i })).toBeInTheDocument()
  })

  it("does NOT offer Stop for a CDC pipeline that is not running", async () => {
    mockLiveStatus("completed")
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="P1" pipelineType="cdc" status="completed" />)
    await openMenu(user)
    expect(screen.queryByRole("menuitem", { name: /stop pipeline/i })).not.toBeInTheDocument()
  })

  // Prod retest: a CDC pipeline whose dependency died read Failed yet offered an
  // inline Pause beside Resume. Pause is gone from a failed pipeline, so while
  // /state still says the stream is up, Stop here is the way to take it down.
  it("offers Stop for a CDC stream /runtime calls failed while /state still runs it", async () => {
    vi.mocked(authFetch).mockImplementation((async (url: string) =>
      jsonRes(String(url).includes("/runtime") ? { phase: "failed", mode: "cdc" } : { status: "running" })) as never)
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="P1" pipelineType="cdc" status="running" />)
    await vi.waitFor(() => expect(vi.mocked(authFetch).mock.calls.some(([u]) => String(u).includes("/runtime"))).toBe(true))
    await openMenu(user)
    expect(screen.getByRole("menuitem", { name: /stop pipeline/i })).toBeInTheDocument()
  })

  it("does NOT offer Stop for a CDC pipeline whose /state itself says failed", async () => {
    mockLiveStatus("failed")
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="P1" pipelineType="cdc" status="failed" />)
    await openMenu(user)
    expect(screen.queryByRole("menuitem", { name: /stop pipeline/i })).not.toBeInTheDocument()
  })
})

// #52: the Pipelines list's row menu sends Rename here as ?rename=1.
describe("PipelineHeaderOverflowMenu — ?rename=1", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockLiveStatus("running")
  })

  it("opens the rename dialog with the current name and drops the param", () => {
    nav.search = "rename=1&tab=overview"
    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="orders-sync" />)
    expect(screen.getByRole("dialog")).toBeInTheDocument()
    expect(screen.getByDisplayValue("orders-sync")).toBeInTheDocument()
    expect(nav.replace).toHaveBeenCalledWith("/pipelines/p1?tab=overview")
  })

  it("stays closed without the param", () => {
    nav.search = ""
    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="orders-sync" />)
    expect(screen.queryByRole("dialog")).toBeNull()
    expect(nav.replace).not.toHaveBeenCalled()
  })
})
