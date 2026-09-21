import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

const jsonRes = (data: unknown) => ({ ok: true, status: 200, json: async () => data })
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
const nav = vi.hoisted(() => ({ push: vi.fn() }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: nav.push, replace: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/pipelines/p1",
  useSearchParams: () => new URLSearchParams(""),
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { PipelineHeaderOverflowMenu } from "../PipelineHeaderOverflowMenu"
import { PipelinesTable, type PipelineListItem } from "../PipelinesTable"
import { pipelineLogsHref } from "@/lib/pipeline/pipelineMenuActions"

// #52 prod retest: the list's row menu and the detail page's ⋯ menu still
// differed — the row hid Delete on a running pipeline, "Schema change alerts"
// was detail-only, and View Logs / Export Config were row-only. Both menus must
// offer the same items; only the run controls the detail page shows as inline
// buttons beside its menu may appear in the row menu alone.
const ROW_ONLY = new Set(["Run Now", "Pause", "Resume"])

function pipeline(over: Partial<PipelineListItem> = {}): PipelineListItem {
  return {
    id: "p1",
    name: "orders-sync",
    pipeline_status: "active",
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-01T00:00:00Z",
    derived_status: "running",
    sync_mode: "cdc",
    last_execution: { id: "e9", status: "running", started_at: "2026-09-18T08:00:00Z" },
    ...over,
  } as PipelineListItem
}

async function menuItemNames(user: ReturnType<typeof userEvent.setup>, trigger: RegExp): Promise<string[]> {
  await user.click(screen.getByRole("button", { name: trigger }))
  const names = (await screen.findAllByRole("menuitem")).map((el) => (el.textContent || "").trim())
  await user.keyboard("{Escape}")
  return names
}

describe("pipeline menus — row and detail offer the same items (#52)", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.mocked(authFetch).mockResolvedValue(jsonRes({ status: "running" }) as never)
  })

  it.each([
    ["a running CDC pipeline", pipeline(), "cdc", "running"],
    ["an idle batch pipeline", pipeline({ sync_mode: "batch", derived_status: "completed" }), "etl", "completed"],
  ])("for %s", async (_label, row, detailType, detailStatus) => {
    vi.mocked(authFetch).mockResolvedValue(jsonRes({ status: detailStatus }) as never)
    const user = userEvent.setup({ pointerEventsCheck: 0 })

    const { unmount } = render(<PipelinesTable pipelines={[row]} onStopPipeline={vi.fn()} />)
    const rowItems = await menuItemNames(user, /open actions for pipeline orders-sync/i)
    unmount()

    render(
      <PipelineHeaderOverflowMenu
        pipelineId="p1"
        pipelineName="orders-sync"
        pipelineType={detailType}
        status={detailStatus}
        lastExecutionId="e9"
      />
    )
    const detailItems = await menuItemNames(user, /open pipeline menu/i)

    expect(detailItems).toEqual(
      expect.arrayContaining(["Schema change alerts…", "View Logs", "Export Config", "Delete"])
    )
    expect(rowItems.filter((n) => !ROW_ONLY.has(n))).toEqual(detailItems)
  })

  it("offers the owner Delete on a running pipeline, and still hides it from others", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    const onDelete = vi.fn()
    const { unmount } = render(
      <PipelinesTable pipelines={[pipeline({ created_by: "u1" })]} currentUserId="u1" onDeletePipeline={onDelete} />
    )
    await user.click(screen.getByRole("button", { name: /open actions for pipeline orders-sync/i }))
    await user.click(screen.getByRole("menuitem", { name: /^delete$/i }))
    expect(onDelete).toHaveBeenCalledWith("p1")
    unmount()

    render(<PipelinesTable pipelines={[pipeline({ created_by: "u1" })]} currentUserId="u2" />)
    await user.click(screen.getByRole("button", { name: /open actions for pipeline orders-sync/i }))
    expect(screen.queryByRole("menuitem", { name: /^delete$/i })).toBeNull()
  })

  it("links Schema change alerts from the row and View Logs from the detail menu", async () => {
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    const { unmount } = render(<PipelinesTable pipelines={[pipeline()]} />)
    await user.click(screen.getByRole("button", { name: /open actions for pipeline orders-sync/i }))
    expect(screen.getByRole("menuitem", { name: /schema change alerts/i })).toHaveAttribute(
      "href",
      "/pipelines/p1/schema-changes"
    )
    unmount()

    render(<PipelineHeaderOverflowMenu pipelineId="p1" pipelineName="orders-sync" lastExecutionId="e9" />)
    await user.click(screen.getByRole("button", { name: /open pipeline menu/i }))
    expect(screen.getByRole("menuitem", { name: /view logs/i })).toHaveAttribute("href", "/executions/e9#logs")
  })

  it("sends View Logs to the run log, or the Monitor tab for a never-run pipeline", () => {
    expect(pipelineLogsHref("p1", "e9")).toBe("/executions/e9#logs")
    expect(pipelineLogsHref("p1", null)).toBe("/pipelines/p1?tab=monitor")
    expect(pipelineLogsHref("p1")).toBe("/pipelines/p1?tab=monitor")
  })
})
