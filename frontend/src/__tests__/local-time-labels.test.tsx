/**
 * Issue #23 — times shown in UTC with nothing saying so.
 *
 *  - The table-statistics panel printed `toISOString().slice(11, 19)`: the UTC
 *    wall-clock time ("10:00:00"), no date and no zone. A viewer in IST read it as
 *    their own 10:00; it was 15:30 for them.
 *  - The pipeline page's "Created" was formatted inside a server component, i.e. in
 *    the server's zone (UTC in every deployment), again with no zone in the string.
 *
 * Every assertion runs under a zone that is NOT UTC (Asia/Kolkata), because under
 * UTC the bug and the fix print the same hour. Node re-reads process.env.TZ when it
 * changes, including in vitest workers (probed: Aug 5 10:00Z formats as
 * "03:30 PM GMT+5:30" here after setting it).
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import { act } from "react"
import { hydrateRoot } from "react-dom/client"
import { renderToString } from "react-dom/server"
import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

import { formatAbsoluteTime } from "@/lib/utils"
import { formatAbsoluteTime as scheduleFormatAbsoluteTime } from "@/components/explorer/scheduleTime"
import { LocalDateTime } from "@/components/ui/local-date-time"
import { TableStatisticsPanel } from "@/components/pipeline/TableStatisticsPanel"

const TS = "2026-08-05T10:00:00Z"
// 10:00 UTC is 15:30 in Kolkata. Either clock convention, never "10:".
const KOLKATA_TIME = /\b(03:30\s?PM|15:30)\b/
const KOLKATA_ZONE = /GMT\+5:30/

let savedTZ: string | undefined
beforeEach(() => {
  vi.clearAllMocks()
  savedTZ = process.env.TZ
  process.env.TZ = "Asia/Kolkata"
})
afterEach(() => {
  if (savedTZ === undefined) delete process.env.TZ
  else process.env.TZ = savedTZ
})

function ok(body: unknown) {
  return { ok: true, status: 200, json: async () => body } as unknown as Response
}

function cellUnder(heading: string, rowIndex = 0): string {
  const table = document.querySelector("table")!
  const heads = Array.from(table.querySelectorAll("thead th")).map((h) => (h.textContent || "").trim())
  const col = heads.indexOf(heading)
  if (col < 0) throw new Error(`no "${heading}" column; headings are ${JSON.stringify(heads)}`)
  return (table.querySelectorAll("tbody tr")[rowIndex].querySelectorAll("td")[col]?.textContent || "").trim()
}

describe("#23 — one shared absolute-time formatter", () => {
  it("scheduleTime re-exports the lib/utils formatter rather than keeping its own copy", () => {
    expect(scheduleFormatAbsoluteTime).toBe(formatAbsoluteTime)
  })

  it("formats in the viewer's zone and names the zone", () => {
    const s = formatAbsoluteTime(TS)
    expect(s).toMatch(KOLKATA_TIME)
    expect(s).toMatch(KOLKATA_ZONE)
    expect(formatAbsoluteTime("not a time")).toBe("")
  })
})

describe("#23 — table statistics timestamps are local and labelled", () => {
  it("prints every CDC timestamp column in the viewer's zone with the zone named", async () => {
    authFetch.mockResolvedValue(
      ok({
        summary: { mode: "cdc", total_tables: 1 },
        tables: [
          {
            qualified_name: "public.events",
            table_name: "events",
            schema_name: "public",
            mode: "cdc",
            status: "running",
            inserts: 1,
            updates: 0,
            deletes: 0,
            total_events: 1,
            applied_inserts: 1,
            applied_updates: 0,
            applied_deletes: 0,
            applied_total_events: 1,
            dlq_rows: 0,
            started_at: TS,
            last_event_ts: TS,
            last_applied_ts: TS,
            updated_at: TS,
          },
        ],
        total: 1,
      }),
    )

    render(<TableStatisticsPanel pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(/events/)).toBeInTheDocument())

    for (const heading of ["Start", "Last Captured", "Last Applied", "Updated"]) {
      const cell = cellUnder(heading)
      expect(cell, heading).toBe(formatAbsoluteTime(TS))
      expect(cell, heading).toMatch(KOLKATA_TIME)
      expect(cell, heading).toMatch(KOLKATA_ZONE)
      expect(cell, heading).not.toContain("10:00:00")
    }
    // THE BOUND: a timestamp the backend did not send stays blank.
    expect(cellUnder("End")).toBe("")
  })
})

describe("#23 — <LocalDateTime> (the pipeline page's Created)", () => {
  it("server-renders the UTC time labelled as UTC, not the server's unlabelled local time", () => {
    const html = renderToString(<LocalDateTime value={new Date(TS)} />)
    expect(html).toContain("2026-08-05 10:00 UTC")
    expect(html).toContain('dateTime="2026-08-05T10:00:00.000Z"')
    expect(html).not.toMatch(KOLKATA_ZONE)
  })

  it("hydrates without a mismatch and then shows the viewer's zone", async () => {
    const node = <LocalDateTime value={TS} />
    const container = document.createElement("div")
    container.innerHTML = renderToString(node)
    document.body.appendChild(container)
    const recoverable = vi.fn()

    await act(async () => {
      hydrateRoot(container, node, { onRecoverableError: recoverable })
    })

    expect(recoverable).not.toHaveBeenCalled()
    const text = container.textContent || ""
    expect(text).toBe(formatAbsoluteTime(TS))
    expect(text).toMatch(KOLKATA_TIME)
    expect(text).toMatch(KOLKATA_ZONE)
    container.remove()
  })

  it("renders the fallback, not Invalid Date, when there is no usable time", () => {
    for (const value of [null, undefined, "", "garbage", new Date("garbage")]) {
      const { container, unmount } = render(<LocalDateTime value={value} fallback="Unknown" />)
      expect(container.textContent).toBe("Unknown")
      expect(container.querySelector("time")).toBeNull()
      unmount()
    }
  })

  // The page is an async server component that fetches with the request's cookies,
  // so it is not rendered here; this pins that Created goes through the client
  // component instead of a server-side formatter.
  it("is what the pipeline page renders for Created", () => {
    const page = readFileSync(resolve(__dirname, "../app/(dashboard)/pipelines/[id]/page.tsx"), "utf8")
    const created = page.slice(page.indexOf(">Created<"), page.indexOf(">Last Updated<"))
    expect(created.length).toBeGreaterThan(0)
    expect(created).toContain("<LocalDateTime value={pipeline.createdAt}")
    expect(created).not.toMatch(/format\w*\(pipeline\.createdAt/)
  })
})
