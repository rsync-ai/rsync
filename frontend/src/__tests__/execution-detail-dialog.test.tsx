/**
 * F-287 — THE EXECUTION "VIEW" DIALOG SHOWED NOTHING THE ROW DID NOT.
 *
 * The old dialog was `onClick={() => setSelectedExecution(execution)}` over the
 * object the LIST endpoint had already returned. It issued no request, so it was
 * structurally incapable of showing more than the six fields behind the row it
 * was opened from — and on a healthy run its most prominent line read
 * "Records —", because `pipelines.go`'s list handler suppresses a zero.
 *
 * These tests pin the two properties that make the redesign a fix rather than a
 * restyle:
 *
 *   1. IT ASKS. `GET /executions/{id}`, table-stats and events are fetched when
 *      the dialog opens. Nothing below matters if it is still re-rendering a row.
 *   2. IT DOES NOT COLLAPSE THREE ANSWERS INTO ONE EM DASH. "no statistics were
 *      recorded", "the source was empty" and "rows were read and none landed"
 *      are different facts that demand opposite operator responses, and the old
 *      dialog rendered all three identically.
 *
 * The derivation itself is unit-tested in
 * `components/pipeline/__tests__/executionSummary.test.ts`; this file is about
 * what reaches the screen.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))

import { ExecutionDetailDialog, type ExecutionRow } from "@/components/pipeline/ExecutionDetailDialog"

const EXEC_ID = "32b9dba8-1f4c-4a1e-9d0a-6f1b2c3d4e5f"

function row(overrides: Partial<ExecutionRow> = {}): ExecutionRow {
  return {
    id: EXEC_ID,
    pipeline_id: "20912e3b",
    status: "success",
    start_time: "2026-08-05T14:01:13.624Z",
    end_time: "2026-08-05T14:02:09.180Z",
    error_message: null,
    // What the LIST row carries on a run that moved no rows: #742's `> 0` guard
    // means the field is simply absent, which is the ambiguity under test.
    metrics: null,
    ...overrides,
  }
}

function ok(body: unknown) {
  return { ok: true, status: 200, json: async () => body }
}

/** Routes each URL the dialog requests to its own body. */
function respondWith({
  detail,
  stats,
  events,
}: {
  detail?: unknown
  stats?: unknown
  events?: unknown
}) {
  authFetch.mockImplementation((url: string) => {
    if (url.includes("/table-stats")) return Promise.resolve(stats ? ok(stats) : { ok: false, status: 500 })
    if (url.includes("/events")) return Promise.resolve(ok(events ?? { events: [] }))
    if (url.includes("/executions/")) return Promise.resolve(detail ? ok(detail) : { ok: false, status: 500 })
    return Promise.resolve({ ok: false, status: 404 })
  })
}

beforeEach(() => {
  vi.clearAllMocks()
  authFetch.mockResolvedValue({ ok: false, status: 500, json: async () => ({}) })
})

describe("F-287 — the dialog reads the execution instead of re-rendering the row", () => {
  it("fetches the execution, its table stats and its events on open", async () => {
    respondWith({ detail: row(), stats: { tables: [], summary: null, total: 0 } })

    render(<ExecutionDetailDialog execution={row()} pipelineId="20912e3b" onClose={() => {}} />)

    await waitFor(() => expect(authFetch).toHaveBeenCalled())
    const urls = authFetch.mock.calls.map((c) => String(c[0]))

    expect(urls.some((u) => u.endsWith(`/executions/${EXEC_ID}`))).toBe(true)
    expect(urls.some((u) => u.includes("/table-stats") && u.includes(`execution_id=${EXEC_ID}`))).toBe(true)
    expect(urls.some((u) => u.includes("/events") && u.includes(`execution_id=${EXEC_ID}`))).toBe(true)
  })

  it("shows the full execution id, not a 12-character prefix", async () => {
    respondWith({ detail: row(), stats: { tables: [], summary: null, total: 0 } })

    render(<ExecutionDetailDialog execution={row()} pipelineId="20912e3b" onClose={() => {}} />)

    // The one thing an operator needs to paste into a query.
    expect(await screen.findByText(EXEC_ID)).toBeInTheDocument()
  })

  it("labels a scheduled run as scheduled — a field the mapper used to drop", async () => {
    // `trigger_source` is emitted by the Go handler and was discarded by the
    // frontend interface, so every run looked manual.
    respondWith({
      detail: row({ trigger_source: "scheduled", scheduled_time: "2026-08-05T14:00:00Z" }),
      stats: { tables: [], summary: null, total: 0 },
    })

    render(<ExecutionDetailDialog execution={row()} pipelineId="20912e3b" onClose={() => {}} />)

    expect(await screen.findByText("Scheduled")).toBeInTheDocument()
  })
})

describe("F-287 — the three answers the old 'Records —' collapsed into one", () => {
  it("says statistics were not recorded rather than implying zero rows", async () => {
    respondWith({
      detail: row(),
      stats: { tables: [], summary: { total_tables: 0 }, total: 0 },
    })

    render(<ExecutionDetailDialog execution={row()} pipelineId="20912e3b" onClose={() => {}} />)

    expect(await screen.findByText(/no table statistics were recorded/i)).toBeInTheDocument()
    expect(screen.getByText(/not the same as zero/i)).toBeInTheDocument()
  })

  it("calls an empty source an empty source, without alarming", async () => {
    respondWith({
      detail: row(),
      stats: {
        tables: [
          {
            qualified_name: "rsync_public_20912e3b.demo_customers",
            mode: "batch",
            status: "completed",
            read_rows: 0,
            inserted_rows: 0,
            dlq_rows: 0,
          },
        ],
        summary: { total_tables: 1, total_read_rows: 0, total_inserted_rows: 0, total_dlq_rows: 0 },
        total: 1,
      },
    })

    render(<ExecutionDetailDialog execution={row()} pipelineId="20912e3b" onClose={() => {}} />)

    expect(await screen.findByText("Nothing to move")).toBeInTheDocument()
    expect(screen.getByText(/no rows at the source, so nothing needed copying/i)).toBeInTheDocument()
    // Not the alarm wording, and not the "we don't know" wording either.
    expect(screen.queryByText(/wrote none/i)).toBeNull()
    expect(screen.queryByText(/not the same as zero/i)).toBeNull()
  })

  it("raises the silent-drop shape: rows read, none written", async () => {
    respondWith({
      detail: row(),
      stats: {
        tables: [
          {
            qualified_name: "public.orders",
            mode: "batch",
            status: "completed",
            read_rows: 4200,
            inserted_rows: 0,
            dlq_rows: 0,
          },
        ],
        summary: { total_tables: 1, total_read_rows: 4200, total_inserted_rows: 0, total_dlq_rows: 0 },
        total: 1,
      },
    })

    render(<ExecutionDetailDialog execution={row()} pipelineId="20912e3b" onClose={() => {}} />)

    expect(await screen.findByText("Read 4,200 rows, wrote none")).toBeInTheDocument()
  })

  it("prefers the server's cross-table total over the returned page", async () => {
    // `/table-stats` pages at 50. Summing the page would report 50 tables' worth
    // of rows for a 300-table pipeline and disagree with the list row.
    respondWith({
      detail: row({ metrics: { records_processed: 900_000 } }),
      stats: {
        tables: [
          {
            qualified_name: "public.t1",
            mode: "batch",
            status: "completed",
            read_rows: 3_000,
            inserted_rows: 3_000,
          },
        ],
        summary: {
          total_tables: 300,
          total_read_rows: 900_000,
          total_inserted_rows: 900_000,
          total_dlq_rows: 0,
        },
        total: 300,
      },
    })

    render(<ExecutionDetailDialog execution={row()} pipelineId="20912e3b" onClose={() => {}} />)

    expect(await screen.findByText("Moved 900,000 rows")).toBeInTheDocument()
  })
})

describe("F-287 — a section that failed to load says so", () => {
  it("distinguishes a failed table-stats fetch from a run with no tables", async () => {
    respondWith({ detail: row(), stats: undefined })

    render(<ExecutionDetailDialog execution={row()} pipelineId="20912e3b" onClose={() => {}} />)

    // Rendering nothing here would read as "this run touched no tables"; the
    // unmeasured headline alone would read as "the run reported nothing".
    expect(
      await screen.findByText(/could not load table statistics.*fetch failure, not an empty result/i),
    ).toBeInTheDocument()
    // Said once. Two amber sentences read as two separate failures.
    expect(screen.getAllByText(/fetch failure, not an empty result/i)).toHaveLength(1)
  })
})

describe("F-287 follow-up — a section reports its own fetch, never someone else's", () => {
  /**
   * `/diagnose` is best-effort, runs only for failure statuses, and nothing else
   * on the screen depends on it. While it was awaited inside the single `loading`
   * flag, a failed run rendered "Loading…" under TABLES and "Where the time went"
   * with that data already in hand, and the headline could stay stuck on
   * "Reading table statistics…" — three sections reporting the state of a request
   * that is not theirs.
   *
   * Every test here holds `/diagnose` open forever. That is the whole point: if a
   * section still resolves, it is not gated on the diagnosis. Each also asserts
   * `/diagnose` was actually requested, so a regression that simply stops asking
   * cannot turn these green.
   */
  const hangs = () => new Promise<never>(() => {})

  function failedRunWithHangingDiagnosis(over: Partial<ExecutionRow> = {}, stats?: unknown) {
    const failed = row({ status: "failed", ...over })
    authFetch.mockImplementation((url: string) => {
      if (url.includes("/diagnose")) return hangs()
      if (url.includes("/table-stats"))
        return Promise.resolve(ok(stats ?? { tables: [], summary: null, total: 0 }))
      if (url.includes("/events")) return Promise.resolve(ok({ events: [] }))
      if (url.includes("/executions/")) return Promise.resolve(ok(failed))
      return Promise.resolve({ ok: false, status: 404 })
    })
    return failed
  }

  /** The positive control every test in this block leans on. */
  function expectDiagnoseWasRequested() {
    expect(authFetch.mock.calls.map((c) => String(c[0])).some((u) => u.includes("/diagnose"))).toBe(
      true,
    )
  }

  it("resolves the TABLES section while the diagnosis is still in flight", async () => {
    const failed = failedRunWithHangingDiagnosis()

    render(<ExecutionDetailDialog execution={failed} pipelineId="20912e3b" onClose={() => {}} />)

    expect(
      await screen.findByText("No per-table statistics were recorded for this run."),
    ).toBeInTheDocument()
    expect(screen.queryAllByText("Loading…")).toHaveLength(0)
    expectDiagnoseWasRequested()
  })

  it("states the outcome instead of staying on 'Reading table statistics…'", async () => {
    // `summary: null` is the sub-case that kept the headline pinned: its old gate
    // was `loading && !summary`, so a stats response carrying no summary left both
    // halves true for the entire diagnosis round-trip.
    const failed = failedRunWithHangingDiagnosis()

    render(<ExecutionDetailDialog execution={failed} pipelineId="20912e3b" onClose={() => {}} />)

    expect(await screen.findByText(/no table statistics were recorded/i)).toBeInTheDocument()
    expect(screen.queryByText("Reading table statistics…")).toBeNull()
    expectDiagnoseWasRequested()
  })

  it("resolves the stage-timing section while the diagnosis is still in flight", async () => {
    // No events and no end_time, so the timeline has nothing to break down and
    // falls through to its placeholder — the branch that was gated on `loading`.
    const failed = failedRunWithHangingDiagnosis({ end_time: null })

    render(<ExecutionDetailDialog execution={failed} pipelineId="20912e3b" onClose={() => {}} />)

    expect(await screen.findByText(/no stage timings were recorded/i)).toBeInTheDocument()
    expect(screen.queryAllByText("Loading…")).toHaveLength(0)
    expectDiagnoseWasRequested()
  })

  it("announces the pending diagnosis rather than letting the panel pop in", async () => {
    // Clearing the flag early must not make the diagnosis arrive unannounced —
    // that trades a stale placeholder for a panel that appears from nowhere.
    const failed = failedRunWithHangingDiagnosis()

    render(<ExecutionDetailDialog execution={failed} pipelineId="20912e3b" onClose={() => {}} />)

    expect(await screen.findByText(/looking for a diagnosis/i)).toBeInTheDocument()
    expectDiagnoseWasRequested()
  })

  it("asks for no diagnosis at all on a run that succeeded", async () => {
    respondWith({ detail: row(), stats: { tables: [], summary: null, total: 0 } })

    render(<ExecutionDetailDialog execution={row()} pipelineId="20912e3b" onClose={() => {}} />)

    expect(
      await screen.findByText("No per-table statistics were recorded for this run."),
    ).toBeInTheDocument()
    expect(authFetch.mock.calls.map((c) => String(c[0])).some((u) => u.includes("/diagnose"))).toBe(
      false,
    )
    // And no pending affordance on a run that will never have one.
    expect(screen.queryByText(/looking for a diagnosis/i)).toBeNull()
  })
})

describe("the dialog can be dismissed without knowing the Escape key exists", () => {
  it("offers a close control that calls onClose", async () => {
    // Reported from the live prod dialog: "there is no cancel X showing on top
    // right". Escape and backdrop-click already worked — the affordance did not
    // exist, so nothing on screen said so.
    respondWith({ detail: row(), stats: { tables: [], summary: null, total: 0 } })
    const onClose = vi.fn()

    render(<ExecutionDetailDialog execution={row()} pipelineId="20912e3b" onClose={onClose} />)

    const closeButton = await screen.findByRole("button", { name: /close/i })
    closeButton.click()

    expect(onClose).toHaveBeenCalledTimes(1)
  })
})
