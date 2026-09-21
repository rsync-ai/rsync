import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

const nav = vi.hoisted(() => ({ push: vi.fn() }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: nav.push, refresh: vi.fn() }),
}))

import { PipelinesTable, type PipelineListItem } from "../PipelinesTable"

// #52: the list's row menu offered View Logs / Export Config while the detail
// page's ⋯ menu offered Rename / Edit Tables / Stop Pipeline / Delete.
function pipeline(over: Partial<PipelineListItem> = {}): PipelineListItem {
  return {
    id: "p1",
    name: "orders-sync",
    pipeline_status: "active",
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-01T00:00:00Z",
    derived_status: "running",
    sync_mode: "cdc",
    ...over,
  }
}

async function openRowMenu(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: /open actions for pipeline orders-sync/i }))
}

describe("PipelinesTable row menu", () => {
  beforeEach(() => vi.clearAllMocks())

  it("offers the detail menu's Rename and Edit Tables, opening them on the detail page", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(<PipelinesTable pipelines={[pipeline()]} />)
    await openRowMenu(user)
    await user.click(screen.getByRole("menuitem", { name: /^rename$/i }))
    expect(nav.push).toHaveBeenCalledWith("/pipelines/p1?rename=1")

    await openRowMenu(user)
    await user.click(screen.getByRole("menuitem", { name: /edit tables/i }))
    expect(nav.push).toHaveBeenCalledWith("/pipelines/p1?tab=table-stats&editTables=1")
  })

  it("offers Stop Pipeline for a running CDC pipeline", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    const onStop = vi.fn()
    render(<PipelinesTable pipelines={[pipeline()]} onStopPipeline={onStop} />)
    await openRowMenu(user)
    await user.click(screen.getByRole("menuitem", { name: /stop pipeline/i }))
    expect(onStop).toHaveBeenCalledWith("p1")
  })

  it("does not offer Stop for a batch pipeline or an idle CDC one", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    const { unmount } = render(
      <PipelinesTable pipelines={[pipeline({ sync_mode: "batch" })]} onStopPipeline={vi.fn()} />
    )
    await openRowMenu(user)
    expect(screen.queryByRole("menuitem", { name: /stop pipeline/i })).toBeNull()
    unmount()

    render(<PipelinesTable pipelines={[pipeline({ derived_status: "completed" })]} onStopPipeline={vi.fn()} />)
    await openRowMenu(user)
    expect(screen.queryByRole("menuitem", { name: /stop pipeline/i })).toBeNull()
    expect(screen.getByRole("menuitem", { name: /^delete$/i })).toBeInTheDocument()
  })
})
