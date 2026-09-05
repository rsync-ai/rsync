/**
 * Regression tests for the "Live events" card — F-274 / F-275 / F-276 / F-284.
 *
 * The card rendered one line per row of `pipeline_run_events`, and that line was
 * the raw database token: `STAGE_PROGRESS`, `healer_decision`,
 * `DATA_PLANE_METRICS`. Four separate defects stacked on top of each other:
 *
 *  F-274  NOISE. `ProgressEmitter.StartStageHeartbeat` emits a STAGE_PROGRESS
 *         every 7 s and stamps it `metadata.heartbeat = true` specifically so
 *         the UI can compress it — the producer says so in its own doc comment
 *         (`backend-orchestrator/internal/workers/progress_events.go:89,102`).
 *         Nothing in the frontend ever read that flag. Together with
 *         DATA_PLANE_METRICS (whose numbers the Throughput card directly above
 *         already shows) that was 345 of 1614 prod rows — 21 % of a 60-row
 *         window spent repeating "still going".
 *
 *  F-275  RAW TOKENS. `MonitorTab.tsx:369` rendered `{ev.event_type}` and
 *         `{ev.stage_id}` verbatim. An operator saw `STAGE_PROGRESS · executor`.
 *
 *  F-276  DISCARDED PAYLOAD. `eventMessage()` read only `payload.message` and
 *         `payload.summary`. Everything else in the payload — the blocking
 *         reason that says WHY the run is parked, the table being copied, the
 *         step counter, the healer's rationale — was dropped on the floor. Most
 *         rows therefore had a title and nothing else.
 *
 *  F-284  DROPPED READ ERRORS. `usePolledJson` computes an `error`
 *         (`MonitorTab.tsx:122`) and the caller destructured only `data`, so a
 *         500 or a dropped connection rendered "No recent events." — the same
 *         pixels as a healthy pipeline that simply has not emitted anything.
 *         Same shape as F-280: a failed read must never borrow the empty state.
 *
 * The filtering in these tests is the part most able to do harm, so two of them
 * exist purely to bound it: a warn/error row is never hidden however it is
 * flagged, and whatever IS hidden stays reachable behind a disclosure.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

import { LiveEventsCard } from "@/components/pipeline/MonitorTab"

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

let evtId = 0
function evt(over: Record<string, unknown>) {
  evtId += 1
  return {
    event_id: `e${evtId}`,
    event_type: "STAGE_PROGRESS",
    received_at: "2026-08-05T12:00:00Z",
    payload: {},
    ...over,
  }
}

const HEARTBEAT = () =>
  evt({
    event_type: "STAGE_PROGRESS",
    stage_id: "executor",
    payload: { metadata: { heartbeat: true } },
  })

const NO_EVENTS = /no recent events/i
const READ_FAILED = /could not load/i

describe("Live events hides the noise the producer already flagged (F-274)", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    evtId = 0
  })

  it("does not list heartbeat rows or throughput rows", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        events: [
          evt({ event_type: "STAGE_STARTED", stage_id: "executor" }),
          HEARTBEAT(),
          HEARTBEAT(),
          evt({ event_type: "DATA_PLANE_METRICS", payload: { metadata: { metrics: { records_read: 10 } } } }),
        ],
      }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(/stage started/i)).toBeInTheDocument())
    expect(screen.queryByText(/stage in progress/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/throughput update/i)).not.toBeInTheDocument()
  })

  it("SAFETY: a warning or error row is never hidden, however it is flagged", async () => {
    // A filter that can swallow an alarm is worse than the noise it removes.
    mockFetch.mockResolvedValue(
      res(200, {
        events: [
          evt({
            event_type: "STAGE_PROGRESS",
            severity: "error",
            payload: { metadata: { heartbeat: true }, message: "sink worker crashed" },
          }),
          evt({
            event_type: "DATA_PLANE_METRICS",
            severity: "warn",
            payload: { message: "throughput collapsed to zero" },
          }),
        ],
      }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText("sink worker crashed")).toBeInTheDocument())
    expect(screen.getByText("throughput collapsed to zero")).toBeInTheDocument()
  })

  it("discloses how many rows it hid, and hands them back on request", async () => {
    // Hiding rows silently is its own lie. The count is the honest part.
    mockFetch.mockResolvedValue(
      res(200, {
        events: [evt({ event_type: "STAGE_STARTED", stage_id: "executor" }), HEARTBEAT(), HEARTBEAT()],
      }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    const toggle = await screen.findByRole("button", { name: /2 routine/i })
    await userEvent.click(toggle)

    await waitFor(() => expect(screen.getAllByText(/stage in progress/i).length).toBe(2))
  })

  it("positive control: a run that emits nothing but heartbeats does not claim there are no events", async () => {
    // "No recent events." would be false — there were events, all routine.
    mockFetch.mockResolvedValue(res(200, { events: [HEARTBEAT(), HEARTBEAT()] }))

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByRole("button", { name: /2 routine/i })).toBeInTheDocument())
    expect(screen.queryByText(NO_EVENTS)).not.toBeInTheDocument()
  })
})

describe("Live events names events in words, not database tokens (F-275)", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    evtId = 0
  })

  it("renders a human label and never the raw event_type", async () => {
    mockFetch.mockResolvedValue(
      res(200, { events: [evt({ event_type: "STAGE_COMPLETED", stage_id: "executor" })] }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(/stage completed/i)).toBeInTheDocument())
    expect(screen.queryByText(/STAGE_COMPLETED/)).not.toBeInTheDocument()
  })

  it("names the stage the way the rest of the product names it", async () => {
    // `executor` is the database's word. "Executing Pipeline" is the one the
    // Steps tab shows for the same stage, from the same registry.
    mockFetch.mockResolvedValue(
      res(200, { events: [evt({ event_type: "STAGE_STARTED", stage_id: "executor" })] }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(/Executing Pipeline/i)).toBeInTheDocument())
  })

  it("labels healer rows as self-healing rather than as raw table values", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        events: [
          evt({
            event_type: "healer_decision",
            severity: "warn",
            payload: { rationale: "Connector returned 401 three times", suggested_action: "refresh_auth" },
          }),
        ],
      }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(/self-healing/i)).toBeInTheDocument())
    expect(screen.queryByText(/healer_decision/)).not.toBeInTheDocument()
  })

  it("an unknown event type degrades to readable prose, not to a token", async () => {
    mockFetch.mockResolvedValue(res(200, { events: [evt({ event_type: "AGENT_THINKING" })] }))

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(/Agent thinking/i)).toBeInTheDocument())
    expect(screen.queryByText(/AGENT_THINKING/)).not.toBeInTheDocument()
  })
})

describe("Live events says what happened, using the payload it already has (F-276)", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    evtId = 0
  })

  it("surfaces the blocking reason on a parked run", async () => {
    // This is the single most useful string in the whole stream: it is the
    // answer to "why is my pipeline not moving".
    mockFetch.mockResolvedValue(
      res(200, {
        events: [
          evt({
            event_type: "PIPELINE_WAITING",
            payload: {
              blocking_reason: {
                type: "user_input_required",
                description: "Select the tables you want to copy",
              },
            },
          }),
        ],
      }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() =>
      expect(screen.getByText("Select the tables you want to copy")).toBeInTheDocument(),
    )
  })

  it("surfaces the step counter when there is no message", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        events: [
          evt({
            event_type: "STAGE_STARTED",
            stage_id: "discovery",
            payload: { progress: { current_step: 3, total_steps: 7, percent: 42 } },
          }),
        ],
      }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(/step 3 of 7/i)).toBeInTheDocument())
  })

  it("surfaces the table a stage is working on", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        events: [
          evt({
            event_type: "STAGE_PROGRESS",
            stage_id: "executor",
            payload: { metadata: { table_name: "public.orders" } },
          }),
        ],
      }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(/public\.orders/)).toBeInTheDocument())
  })

  it("surfaces the healer's rationale, which is the evidence for its decision", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        events: [
          evt({
            event_type: "healer_decision",
            payload: { rationale: "Connector returned 401 three times", confidence: 0.8 },
          }),
        ],
      }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() =>
      expect(screen.getByText(/Connector returned 401 three times/)).toBeInTheDocument(),
    )
  })

  it("positive control: an explicit message still wins over every derived detail", async () => {
    mockFetch.mockResolvedValue(
      res(200, {
        events: [
          evt({
            event_type: "STAGE_COMPLETED",
            payload: {
              message: "Copied 12 tables",
              progress: { current_step: 7, total_steps: 7 },
            },
          }),
        ],
      }),
    )

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText("Copied 12 tables")).toBeInTheDocument())
    expect(screen.queryByText(/step 7 of 7/i)).not.toBeInTheDocument()
  })
})

describe("Live events does not present a failed read as an empty stream (F-284)", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    evtId = 0
  })

  it("THE REGRESSION: a 500 says the read failed, not that there are no events", async () => {
    mockFetch.mockResolvedValue(res(500, { error: "database unavailable" }))

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(READ_FAILED)).toBeInTheDocument())
    expect(screen.queryByText(NO_EVENTS)).not.toBeInTheDocument()
  })

  it("THE REGRESSION: a dropped connection does the same", async () => {
    mockFetch.mockRejectedValue(new TypeError("Failed to fetch"))

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(READ_FAILED)).toBeInTheDocument())
    expect(screen.queryByText(NO_EVENTS)).not.toBeInTheDocument()
  })

  it("positive control: a genuinely empty stream still says so", async () => {
    // Without this the fix could pass by never showing the empty state at all.
    mockFetch.mockResolvedValue(res(200, { events: [] }))

    render(<LiveEventsCard pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText(NO_EVENTS)).toBeInTheDocument())
    expect(screen.queryByText(READ_FAILED)).not.toBeInTheDocument()
  })
})
