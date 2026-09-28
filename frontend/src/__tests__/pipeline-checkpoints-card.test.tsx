/**
 * `PipelineCheckpointsCard` — the UI for GET /pipelines/:id/checkpoints, which
 * has been served and workspace-gated since migration 026 with no caller.
 *
 * Two things are guarded here.
 *
 * 1. `formatPosition` stays shape-agnostic. `position` is free-form JSONB by
 *    design — Postgres writes an LSN, MongoDB a resume token, an API connector
 *    a nested `watermark.value` — so a renderer that assumed one engine's
 *    vocabulary would print "—" for the others.
 *
 * 2. A failed read must NOT render the empty state. "No checkpoint recorded
 *    yet. A restart would start this pipeline's tables from the beginning." is
 *    a positive claim about the pipeline's resume position; saying it because
 *    an HTTP call 500'd is the F-242 failure mode (a read error that looks like
 *    a healthy empty result), and here it would tell an operator to expect a
 *    full replay that is not actually going to happen.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react"
import "@testing-library/jest-dom"

import {
  PipelineCheckpointsCard,
  formatPosition,
  latestRunTotals,
  summarizeCheckpoint,
} from "@/components/pipeline/PipelineCheckpointsCard"

const mockFetch = vi.fn()
vi.stubGlobal("fetch", mockFetch)

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    text: async () => JSON.stringify(body),
    json: async () => body,
  } as unknown as Response
}

const EMPTY_STATE = /no checkpoint recorded yet/i

function checkpoint(over: Partial<Record<string, unknown>> = {}) {
  return {
    id: "cp1",
    pipeline_id: "p1",
    connection_id: "c1",
    source_table: "public.orders",
    position: { lsn: "0/1A2B3C4" },
    created_at: "2026-09-20T10:00:00Z",
    updated_at: "2026-09-22T11:30:00Z",
    ...over,
  }
}

beforeEach(() => {
  mockFetch.mockReset()
})

describe("formatPosition", () => {
  it("flattens a nested position one level — a connector watermark", () => {
    expect(formatPosition({ watermark: { value: "2026-09-01T00:00:00Z" } })).toBe(
      "watermark.value: 2026-09-01T00:00:00Z"
    )
  })

  it("prints a flat engine position as-is — a Postgres LSN, a Mongo resume token", () => {
    expect(formatPosition({ lsn: "0/1A2B3C4" })).toBe("lsn: 0/1A2B3C4")
    expect(formatPosition({ resume_token: "82650F..." })).toBe("resume_token: 82650F...")
  })

  it("joins multiple keys rather than showing only the first", () => {
    expect(formatPosition({ lsn: "0/1A", txid: 4711 })).toBe("lsn: 0/1A · txid: 4711")
  })

  it("distinguishes an empty position object from no position at all", () => {
    // The row exists but carries nothing — a real and different state from
    // "this table has no checkpoint row".
    expect(formatPosition({})).toBe("(empty)")
    expect(formatPosition(null)).toBe("—")
    expect(formatPosition(undefined)).toBe("—")
  })

  it("labels the batch executor's row and byte counts as the whole run's, not the table's (#13)", () => {
    // executor.go stamps the run's shared running totals into every table's
    // checkpoint; a bare "rows_so_far" beside one table read as that table's.
    expect(formatPosition({ rows_so_far: 1200, bytes_so_far: 4096 })).toBe(
      "run rows so far (all tables): 1200 · run bytes so far (all tables): 4096"
    )
    // Control: engine positions keep their own names.
    expect(formatPosition({ lsn: "0/1A", txid: 4711 })).toBe("lsn: 0/1A · txid: 4711")
    expect(formatPosition({ watermark: { rows_so_far: 3 } })).toBe("watermark.rows_so_far: 3")
  })

  it("does not lose an array value to [object Object]", () => {
    expect(formatPosition({ gtids: ["a:1-5", "b:1-2"] })).toBe('gtids: ["a:1-5","b:1-2"]')
  })
})

describe("PipelineCheckpointsCard", () => {
  it("renders a row per table with its position", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        pipeline_id: "p1",
        count: 2,
        checkpoints: [
          checkpoint(),
          checkpoint({ id: "cp2", source_table: "public.customers", position: { watermark: { value: "2026-09-22" } } }),
        ],
      })
    )

    render(<PipelineCheckpointsCard pipelineId="p1" />)

    expect(await screen.findByText("public.orders")).toBeInTheDocument()
    expect(screen.getByText("lsn: 0/1A2B3C4")).toBeInTheDocument()
    expect(screen.getByText("public.customers")).toBeInTheDocument()
    expect(screen.getByText("watermark.value: 2026-09-22")).toBeInTheDocument()
    expect(screen.queryByText(EMPTY_STATE)).not.toBeInTheDocument()
  })

  it("shows the empty state ONLY for a genuine empty list", async () => {
    mockFetch.mockResolvedValue(res(200, { pipeline_id: "p1", count: 0, checkpoints: [] }))

    render(<PipelineCheckpointsCard pipelineId="p1" />)

    expect(await screen.findByText(EMPTY_STATE)).toBeInTheDocument()
  })

  it("on a 500 says the read failed — and does NOT claim there are no checkpoints", async () => {
    mockFetch.mockResolvedValue(res(500, { error: "db down" }))

    render(<PipelineCheckpointsCard pipelineId="p1" />)

    expect(await screen.findByText(/could not load checkpoints \(http 500\)/i)).toBeInTheDocument()
    // The assertion this whole file exists for.
    expect(screen.queryByText(EMPTY_STATE)).not.toBeInTheDocument()
  })

  it("on a thrown fetch says the API is unreachable — still not the empty state", async () => {
    mockFetch.mockRejectedValue(new TypeError("Failed to fetch"))

    render(<PipelineCheckpointsCard pipelineId="p1" />)

    expect(await screen.findByText(/api is unreachable/i)).toBeInTheDocument()
    expect(screen.queryByText(EMPTY_STATE)).not.toBeInTheDocument()
  })

  it("reads the pipeline's own checkpoints endpoint", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: [] }))

    render(<PipelineCheckpointsCard pipelineId="p-42" />)

    await waitFor(() => expect(mockFetch).toHaveBeenCalled())
    expect(String(mockFetch.mock.calls[0][0])).toContain("/api/v1/pipelines/p-42/checkpoints")
  })
})

// Only the batch export loop writes pipeline_checkpoints (executor.go
// SaveCheckpoint). On a CDC pipeline an empty list is the normal state, and the
// batch sentence "a restart would start from the beginning" is false there: the
// stream resumes from its Kafka offsets. Each CDC test has an ETL control so
// the batch wording is proven unchanged, not merely absent.
describe("PipelineCheckpointsCard — CDC pipelines", () => {
  const CDC_EMPTY = /expected for a CDC pipeline/i

  it("on a CDC pipeline an empty list is explained as expected, not as a restart from zero", async () => {
    mockFetch.mockResolvedValue(res(200, { pipeline_id: "p1", count: 0, checkpoints: [] }))

    render(<PipelineCheckpointsCard pipelineId="p1" pipelineType="cdc" />)

    expect(await screen.findByText(CDC_EMPTY)).toBeInTheDocument()
    expect(screen.getByText(/does not mean a restart starts over/i)).toBeInTheDocument()
    expect(screen.queryByText(EMPTY_STATE)).not.toBeInTheDocument()
    expect(screen.queryByText(/start this pipeline's tables from the beginning/i)).not.toBeInTheDocument()
  })

  it("control: an ETL pipeline keeps the batch wording", async () => {
    mockFetch.mockResolvedValue(res(200, { pipeline_id: "p1", count: 0, checkpoints: [] }))

    render(<PipelineCheckpointsCard pipelineId="p1" pipelineType="etl" />)

    expect(await screen.findByText(EMPTY_STATE)).toBeInTheDocument()
    expect(screen.getByText(/where a restart resumes from/i)).toBeInTheDocument()
    expect(screen.getByText(/whole run's running total/i)).toBeInTheDocument()
    expect(screen.queryByText(CDC_EMPTY)).not.toBeInTheDocument()
  })

  it("the CDC description says where a stream actually resumes from", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: [] }))

    render(<PipelineCheckpointsCard pipelineId="p1" pipelineType="cdc" />)

    expect(await screen.findByText(/written by batch runs only/i)).toBeInTheDocument()
    expect(screen.getByText(/resumes from kafka offsets/i)).toBeInTheDocument()
    expect(screen.queryByText(/where a restart resumes from/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/whole run's running total/i)).not.toBeInTheDocument()
  })

  it("a CDC pipeline that does have rows (a hybrid backfill) still shows them", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: [checkpoint()] }))

    render(<PipelineCheckpointsCard pipelineId="p1" pipelineType="cdc" />)

    expect(await screen.findByText("public.orders")).toBeInTheDocument()
    expect(screen.queryByText(CDC_EMPTY)).not.toBeInTheDocument()
  })

  it("a failed read on a CDC pipeline is still a failed read, not the CDC empty state", async () => {
    mockFetch.mockResolvedValue(res(500, { error: "db down" }))

    render(<PipelineCheckpointsCard pipelineId="p1" pipelineType="cdc" />)

    expect(await screen.findByText(/could not load checkpoints \(http 500\)/i)).toBeInTheDocument()
    expect(screen.queryByText(CDC_EMPTY)).not.toBeInTheDocument()
  })
})

// The batch executor's real position shape (executor.go checkpointPosition).
function batchPos(over: Record<string, unknown> = {}) {
  return {
    batch_idx: 12,
    offset: 0,
    rows_so_far: 680000,
    bytes_so_far: 5 * 1024 * 1024,
    table_rows_so_far: 12000,
    paging_mode: "keyset",
    cursor_column: "id",
    cursor: 12000,
    table_complete: false,
    execution_id: "0321ff04-475f-4480-b045-7191969747fb",
    updated_at: "2026-09-27T15:58:00Z",
    ...over,
  }
}

describe("summarizeCheckpoint — one scannable line per table, not every key", () => {
  it("a mid-table batch position says where it resumes and this table's own rows", () => {
    const s = summarizeCheckpoint(batchPos())
    expect(s.state).toBe("in_progress")
    expect(s.resume).toBe("after batch 12 · id > 12000")
    expect(s.tableRows).toBe(12000)
    // The run-wide totals are NOT repeated on the row — they are hoisted once.
    expect(s.resume).not.toMatch(/run rows|680/)
  })

  it("a finished sweep says so, without a stale cursor", () => {
    const s = summarizeCheckpoint(batchPos({ table_complete: true }))
    expect(s.state).toBe("complete")
    expect(s.resume).toBe("sweep finished")
  })

  it("keeps an incremental watermark visible", () => {
    const s = summarizeCheckpoint(batchPos({ table_complete: true, watermark: { field: "updated_at", value: "2026-09-01" } }))
    expect(s.resume).toBe("sweep finished · since 2026-09-01")
  })

  it("control: a non-batch position (Postgres LSN) is shown whole and makes no state claim", () => {
    const s = summarizeCheckpoint({ lsn: "0/1A2B3C4" })
    expect(s.state).toBeNull()
    expect(s.resume).toBe("lsn: 0/1A2B3C4")
    expect(s.tableRows).toBeNull()
  })
})

describe("latestRunTotals", () => {
  it("reads the run-wide totals once for the run, not per table", () => {
    const t = latestRunTotals([
      checkpoint({ id: "a", updated_at: "2026-09-27T15:00:00Z", position: batchPos({ rows_so_far: 100 }) }),
      checkpoint({ id: "b", updated_at: "2026-09-27T15:59:00Z", position: batchPos({ rows_so_far: 900 }) }),
      checkpoint({ id: "c", updated_at: "2026-09-27T16:30:00Z", position: { lsn: "0/1" } }),
    ] as never)
    expect(t?.rows).toBe(900)
    expect(t?.executionId).toBe("0321ff04-475f-4480-b045-7191969747fb")
  })

  it("control: no batch rows → no totals line at all", () => {
    expect(latestRunTotals([checkpoint()] as never)).toBeNull()
  })

  // 0.1.7-rc1, 42-row batch run: the card said 32. Each table goroutine
  // snapshots the shared run totals and THEN saves, so a table that snapshotted
  // early can commit last. Within a run the totals only grow, so the run's
  // figure is its largest -- never the last-committed row's.
  it("a table that commits last with an older snapshot does not shrink the run total", () => {
    const run = "5a7d9e0c-1111-4d2a-9c3b-000000000042"
    const t = latestRunTotals([
      checkpoint({ id: "a", updated_at: "2026-09-28T07:00:01Z", position: batchPos({ execution_id: run, rows_so_far: 42, bytes_so_far: 4200 }) }),
      checkpoint({ id: "b", updated_at: "2026-09-28T07:00:02Z", position: batchPos({ execution_id: run, rows_so_far: 32, bytes_so_far: 3200 }) }),
    ] as never)
    expect(t?.rows).toBe(42)
    expect(t?.bytes).toBe(4200)
    expect(t?.executionId).toBe(run)
    expect(t?.at).toBe("2026-09-28T07:00:02Z")
  })

  it("an earlier, bigger run's leftover rows do not count toward the latest run", () => {
    const t = latestRunTotals([
      checkpoint({ id: "old", updated_at: "2026-09-27T07:00:00Z", position: batchPos({ execution_id: "run-1", rows_so_far: 9000 }) }),
      checkpoint({ id: "new", updated_at: "2026-09-28T07:00:00Z", position: batchPos({ execution_id: "run-2", rows_so_far: 12 }) }),
    ] as never)
    expect(t?.rows).toBe(12)
    expect(t?.executionId).toBe("run-2")
  })
})

describe("PipelineCheckpointsCard with many tables", () => {
  function manyTables(n: number) {
    return Array.from({ length: n }, (_, i) =>
      checkpoint({
        id: `cp${i}`,
        source_table: `public.t${String(i).padStart(2, "0")}`,
        position: batchPos({ table_complete: i % 3 === 0, table_rows_so_far: i * 10 }),
      })
    )
  }

  it("states the run total once — not on every row", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: manyTables(20) }))
    render(<PipelineCheckpointsCard pipelineId="p1" />)
    const totals = await screen.findByTestId("checkpoints-run-totals")
    expect(totals).toHaveTextContent("680,000 rows")
    expect(screen.getAllByText(/680,000/)).toHaveLength(1)
    // 20 tables: 7 finished (i % 3 === 0), 13 mid-table.
    expect(screen.getByTestId("checkpoints-summary")).toHaveTextContent(/20 tables.*13 mid-table.*7 finished/)
  })

  it("filters by table name and by state", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: manyTables(20) }))
    render(<PipelineCheckpointsCard pipelineId="p1" />)
    const box = await screen.findByLabelText(/filter checkpoints by table name/i)
    fireEvent.change(box, { target: { value: "t1" } })
    // t10..t19 — "t01" does not contain "t1"
    expect(screen.getByText("10 of 20")).toBeInTheDocument()
    expect(screen.getByText("public.t14")).toBeInTheDocument()
    expect(screen.queryByText("public.t01")).not.toBeInTheDocument()

    fireEvent.change(box, { target: { value: "" } })
    fireEvent.click(screen.getByRole("button", { name: "Finished" }))
    expect(screen.getByText("7 of 20")).toBeInTheDocument()
    expect(screen.getByText("public.t03")).toBeInTheDocument()
    expect(screen.queryByText("public.t01")).not.toBeInTheDocument()
  })

  it("the resume cell wraps between words, not inside them", async () => {
    // prod, narrow pane: `break-all` split "sweep finished" as "sweep f / inished".
    // jsdom has no layout, so this pins the class: wrap-anywhere breaks a word
    // only when it cannot fit whole.
    mockFetch.mockResolvedValue(res(200, { checkpoints: manyTables(3) }))
    render(<PipelineCheckpointsCard pipelineId="p1" />)
    const cell = (await screen.findAllByText("sweep finished"))[0]
    expect(cell).toHaveClass("wrap-anywhere")
    expect(cell).not.toHaveClass("break-all")
  })

  it("control: a short list shows no filter row", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: manyTables(3) }))
    render(<PipelineCheckpointsCard pipelineId="p1" />)
    await screen.findByText("public.t00")
    expect(screen.queryByLabelText(/filter checkpoints by table name/i)).not.toBeInTheDocument()
  })

  it("expands a row to show every raw position key, and collapses it again", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: manyTables(2) }))
    render(<PipelineCheckpointsCard pipelineId="p1" />)
    const toggle = await screen.findByRole("button", { name: /show the full position for public\.t01/i })
    expect(screen.queryByText("paging_mode")).not.toBeInTheDocument()
    fireEvent.click(toggle)
    expect(toggle).toHaveAttribute("aria-expanded", "true")
    const details = document.getElementById(toggle.getAttribute("aria-controls")!)!
    expect(within(details).getByText("paging_mode")).toBeInTheDocument()
    expect(within(details).getByText("run rows so far (all tables)")).toBeInTheDocument()
    fireEvent.click(toggle)
    expect(screen.queryByText("paging_mode")).not.toBeInTheDocument()
  })

  it("lists mid-table rows before finished ones", async () => {
    mockFetch.mockResolvedValue(res(200, { checkpoints: manyTables(4) }))
    render(<PipelineCheckpointsCard pipelineId="p1" />)
    await screen.findByText("public.t00")
    const names = screen.getAllByText(/^public\.t\d\d$/).map((el) => el.textContent)
    expect(names).toEqual(["public.t01", "public.t02", "public.t00", "public.t03"])
  })
})
