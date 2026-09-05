/**
 * Regression tests for the Execution history tab — F-282 and F-286.
 *
 *  F-282  "RETRY" DOES NOT RETRY THE ROW IT SITS ON. The control lives in the
 *         actions cell of one failed execution, and the dialog version is
 *         literally captioned "Retry Execution" (`ExecutionHistoryTab.tsx:427`)
 *         over a comment promising "from its last checkpoint" — but the handler
 *         is `tryRun(runMode)` (`:117`), which closes over `pipelineId` alone
 *         and calls `executePipelineWithRunMode(pipelineId, runMode)`. The
 *         selected execution is never an input. Clicking Retry on the oldest
 *         failed run in the list does exactly what clicking it on the newest
 *         does: start a NEW run of the pipeline from the PIPELINE's current
 *         checkpoints.
 *
 *         There is no execution-scoped retry endpoint to call — the api-gateway
 *         exposes only pipeline-level run — so the fix is the promise, not the
 *         plumbing: the control must not claim to act on a row it cannot reach.
 *
 *  F-286  FILTERING WHILE PAGINATED STRANDS THE USER. `currentPage` is never
 *         reset when `statusFilter` changes (`:170` — the fetch callback depends
 *         on the filter, the page does not), and the pager only renders when
 *         `totalPages > 1` (`:320`). Filter to a status with fewer than one page
 *         of results while on page 3 and the slice at `:176` is empty: the "No
 *         execution history yet" empty state, with no control on screen to get
 *         back to page 1. The rows exist; the UI says they do not.
 *
 * The bound on F-282 is that the ACTION is correct and must not change — only
 * what it says about itself. `executePipelineWithRunMode(pipelineId, "resume")`
 * is still exactly the call we want to make.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

const push = vi.fn()
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push, refresh: vi.fn() }),
  usePathname: () => "/pipelines/p1",
}))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const listExecutionsResponse = vi.fn()
vi.mock("@/lib/api/executions", () => ({
  listExecutionsResponse: (...a: unknown[]) => listExecutionsResponse(...a),
}))

/**
 * The detail dialog now fetches (`GET /executions/{id}`, table-stats, events)
 * instead of re-rendering the list row it was opened from. Stub the transport so
 * these tests stay about the retry control: every section falls back to the row,
 * which is exactly the degraded path a fetch failure produces in the browser.
 */
const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...a: unknown[]) => authFetch(...a),
}))

const executePipelineWithRunMode = vi.fn()
// The class is declared INSIDE the factory on purpose: `vi.mock` is hoisted
// above every top-level statement, so a class declared outside it is still in
// its temporal dead zone when the factory runs, and the whole module fails to
// mock ("Cannot access 'AssessmentRequiredError' before initialization") — the
// file then collects ZERO tests, which looks like a pass, not a failure.
vi.mock("@/lib/api/pipelines", () => {
  class AssessmentRequiredError extends Error {
    report: unknown
    constructor(report: unknown) {
      super("assessment required")
      this.report = report
    }
  }
  return {
    executePipelineWithRunMode: (...a: unknown[]) => executePipelineWithRunMode(...a),
    AssessmentRequiredError,
  }
})

import { ExecutionHistoryTab } from "@/components/pipeline/ExecutionHistoryTab"

/**
 * Ids are exactly 8 characters because the id cell renders
 * `{execution.id.slice(0, 8)}...` (`:244`). At 8 chars the cell text is the
 * whole id plus the ellipsis, so tests can assert an EXACT string instead of a
 * substring — `/exec-1/` would also match exec-10 … exec-19, which is how a
 * paging assertion silently stops testing paging.
 */
function id(i: number) {
  return `e${String(i).padStart(7, "0")}`
}
function idCell(i: number) {
  return `${id(i)}...`
}

function exec(i: number, status: string) {
  return {
    id: id(i),
    pipeline_id: "p1",
    status,
    start_time: "2026-08-05T10:00:00Z",
    end_time: "2026-08-05T10:05:00Z",
    error_message: status === "failed" ? "connection refused" : null,
    metrics: { records_processed: 100 },
  }
}

/**
 * This used to scope through a control only the dialog rendered, because the
 * shared primitive drew the panel as a plain `<div>` with no `role="dialog"` —
 * `KI-DIALOG-PRIMITIVE-HAS-NO-ROLE-OR-ARIA-MODAL`, out of scope for this suite
 * and filed rather than fixed here. That is now fixed at the primitive, so the
 * workaround is gone and the panel is reached the way a screen reader reaches
 * it. Keeping the class-selector fallback would have meant this suite passed
 * whether or not the role was ever restored.
 */
function dialogPanel(): HTMLElement {
  return screen.getByRole("dialog")
}

async function openDetail(user: ReturnType<typeof userEvent.setup>) {
  await user.click(screen.getByRole("button", { name: /view/i }))
  return dialogPanel()
}

beforeEach(() => {
  vi.clearAllMocks()
  executePipelineWithRunMode.mockResolvedValue({ execution_id: "new-exec" })
  authFetch.mockResolvedValue({ ok: false, status: 500, json: async () => ({}) })
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  if (!Element.prototype.hasPointerCapture) Element.prototype.hasPointerCapture = () => false
  if (!Element.prototype.setPointerCapture) Element.prototype.setPointerCapture = () => {}
  if (!Element.prototype.releasePointerCapture) Element.prototype.releasePointerCapture = () => {}
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {}
})

describe("F-282 — the retry control does not claim to re-run the row it sits on", () => {
  it("does not label the per-row action as retrying that execution", async () => {
    listExecutionsResponse.mockResolvedValue({ executions: [exec(1, "failed")] })

    render(<ExecutionHistoryTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(idCell(1))).toBeInTheDocument())

    // A bare "Retry" in the row's action cell reads as "retry THIS run".
    // Whatever the control ends up saying, it has to name the pipeline, because
    // the pipeline is what actually gets run.
    const actions = screen.getAllByRole("button").map((b) => b.textContent || "")
    expect(actions.some((t) => /^\s*Retry\s*$/.test(t))).toBe(false)
    expect(actions.some((t) => /re-?run/i.test(t) && /pipeline/i.test(t))).toBe(true)
  })

  it("does not caption the dialog action 'Retry Execution'", async () => {
    listExecutionsResponse.mockResolvedValue({ executions: [exec(1, "failed")] })
    const user = userEvent.setup()

    render(<ExecutionHistoryTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(idCell(1))).toBeInTheDocument())
    const panel = await openDetail(user)

    expect(within(panel).queryByRole("button", { name: /retry execution/i })).toBeNull()
    expect(within(panel).getByRole("button", { name: /re-?run.*pipeline/i })).toBeInTheDocument()
  })

  it("says the run starts from the pipeline's current state, not this execution's", async () => {
    listExecutionsResponse.mockResolvedValue({ executions: [exec(1, "failed")] })
    const user = userEvent.setup()

    render(<ExecutionHistoryTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(idCell(1))).toBeInTheDocument())
    const panel = await openDetail(user)

    // The one thing an operator has to know before clicking: this starts a new
    // run of the pipeline as it stands now. It does not rewind to this row.
    expect(
      within(panel).getByText(/new run of the pipeline|does not re-?run this execution/i),
    ).toBeInTheDocument()
  })

  // THE BOUND. Renaming must not touch the call. Resuming the pipeline is still
  // the right action and still has to reach the API and route to the new run.
  it("still starts a resume run of the pipeline and routes to it", async () => {
    listExecutionsResponse.mockResolvedValue({ executions: [exec(1, "failed")] })
    const user = userEvent.setup()

    render(<ExecutionHistoryTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(idCell(1))).toBeInTheDocument())
    const panel = await openDetail(user)

    await user.click(within(panel).getByRole("button", { name: /re-?run.*pipeline/i }))

    await waitFor(() =>
      expect(executePipelineWithRunMode).toHaveBeenCalledWith(
        "p1",
        "resume",
        expect.objectContaining({ ackWarnings: false }),
      ),
    )
    await waitFor(() => expect(push).toHaveBeenCalledWith("/executions/new-exec"))
  })
})

describe("F-286 — changing the status filter returns to the first page", () => {
  it("shows the filtered rows instead of an empty page 3", async () => {
    listExecutionsResponse.mockResolvedValue({
      executions: Array.from({ length: 60 }, (_, i) => exec(i + 1, "success")),
    })
    const user = userEvent.setup()

    render(<ExecutionHistoryTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(idCell(1))).toBeInTheDocument())

    // 60 rows / 25 per page = 3 pages. Walk to the last one.
    await user.click(screen.getByRole("button", { name: /next/i }))
    await user.click(screen.getByRole("button", { name: /next/i }))
    await waitFor(() => expect(screen.getByText(idCell(51))).toBeInTheDocument())

    // Now filter to a status with a single page of results.
    listExecutionsResponse.mockResolvedValue({
      executions: [exec(101, "failed"), exec(102, "failed")],
    })
    await user.click(screen.getByRole("combobox"))
    await user.click(await screen.findByRole("option", { name: /^failed$/i }))

    // The two matching rows exist. They must be on screen.
    await waitFor(() => expect(screen.getByText(idCell(101))).toBeInTheDocument())
    expect(screen.getByText(idCell(102))).toBeInTheDocument()
    expect(screen.queryByText(/no execution history yet/i)).toBeNull()
  })

  // THE BOUND. Paging itself still has to work; the reset must not pin page 1.
  it("still pages forward within a single result set", async () => {
    listExecutionsResponse.mockResolvedValue({
      executions: Array.from({ length: 60 }, (_, i) => exec(i + 1, "success")),
    })
    const user = userEvent.setup()

    render(<ExecutionHistoryTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByText(idCell(1))).toBeInTheDocument())

    await user.click(screen.getByRole("button", { name: /next/i }))
    await waitFor(() => expect(screen.getByText(idCell(26))).toBeInTheDocument())
    expect(screen.queryByText(idCell(1))).toBeNull()
  })
})
