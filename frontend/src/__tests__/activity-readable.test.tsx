/**
 * Regression tests for the Data flow tab's event feed: F-274 / F-275 / F-276 /
 * F-284, plus the Activity sub-tab's own polling and alert severity.
 *
 * These guarded the "Live events" card until it was removed. It and the
 * Monitoring card's "Trace" sub-tab read the same `GET /pipelines/:id/events`
 * stream, so the card was dropped and Trace became "Activity"
 * (`ReasoningTimeline` inside `PipelineMonitoringPanel`). Every defect below was
 * fixed once for the card; the tests now hold Activity to the same bar.
 *
 *  F-274  NOISE. `ProgressEmitter.StartStageHeartbeat` emits a STAGE_PROGRESS
 *         every 7 s and stamps it `metadata.heartbeat = true` so the UI can
 *         compress it (`backend-orchestrator/internal/workers/progress_events.go:89,102`).
 *         With DATA_PLANE_METRICS and per-batch TABLE_STATS that is most of
 *         the stream saying "still going". Hidden by default, counted, and
 *         handed back by a disclosure; a warn/error row is never hidden.
 *
 *  F-275  RAW TOKENS. Rows showed `STAGE_PROGRESS · executor`. They now say
 *         what happened in words; the code, seq, stage id and trace id sit
 *         behind the row's Details toggle.
 *
 *  F-276  DISCARDED PAYLOAD. The blocking reason, step counter, table name and
 *         healer rationale were dropped. Each is now the row's detail line.
 *
 *  F-284  DROPPED READ ERRORS. A failed read must never borrow the empty state.
 *         With nothing loaded it says the read failed; with rows on screen it
 *         keeps them and says they may be out of date.
 *
 * The card refreshed every 5 s; Activity only on Reload. Removing the card
 * would have lost live updates, so Activity now polls the newest page while it
 * is open and merges it into what is shown (`mergeNewestEvents.ts`).
 *
 * SENTINEL_ALERT rows carry their level in `payload.status`, not the severity
 * column (the projector reads only `raw["severity"]`, `event_projector.go:736`;
 * the watchdogs write `status`, `cdc_wal_watchdog.go:329-347`,
 * `batch_sentinel.go:763-776`). They rendered as a green check and the Errors
 * filter could not find them.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { act, cleanup, render, screen, waitFor, within } from "@testing-library/react"
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

// Not under test, and each pulls in its own fetches. The Overview stand-in keeps
// only its "View in Activity" hook, to prove the panel switches sub-tab on it.
vi.mock("@/components/pipeline/MonitoringOverviewTab", () => ({
  MonitoringOverviewTab: ({ onOpenActivity }: { onOpenActivity?: () => void }) => (
    <button type="button" onClick={onOpenActivity}>
      View in Activity
    </button>
  ),
}))
vi.mock("@/components/pipeline/TableStatisticsPanel", () => ({ TableStatisticsPanel: () => null }))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
  authFetchOrThrow: (...args: unknown[]) => authFetch(...args),
}))

import { ReasoningTimeline } from "@/components/pipeline/ReasoningTimeline"
import { PipelineMonitoringPanel } from "@/components/pipeline/PipelineMonitoringPanel"
import { EventNormalizer, type PipelineRunEvent } from "@/lib/pipeline/eventNormalizer"
import { mergeNewestPage } from "@/lib/pipeline/mergeNewestEvents"
import { featureFlagsManager } from "@/config/features"

let evtId = 0
function evt(over: Partial<PipelineRunEvent> = {}): PipelineRunEvent {
  evtId += 1
  return {
    pipeline_id: "p1",
    event_id: `e${evtId}`,
    event_type: "STAGE_PROGRESS",
    received_at: "2026-09-18T12:00:00Z",
    payload: {},
    ...over,
  }
}

const HEARTBEAT = () =>
  evt({ event_type: "STAGE_PROGRESS", stage_id: "executor", payload: { metadata: { heartbeat: true } } })

const NO_EVENTS = /no events/i
const READ_FAILED = /could not load events/i

function rows() {
  return screen.queryAllByTestId("activity-row")
}

beforeEach(() => {
  evtId = 0
  // The panel's ScrollArea observes its viewport once there is enough to scroll.
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

afterEach(() => {
  cleanup()
  vi.useRealTimers()
})

describe("Activity hides the noise the producer already flagged (F-274)", () => {
  it("does not list heartbeat or throughput rows, and counts them", () => {
    render(
      <ReasoningTimeline
        events={[
          evt({ event_type: "STAGE_STARTED", stage_id: "executor" }),
          HEARTBEAT(),
          HEARTBEAT(),
          evt({ event_type: "DATA_PLANE_METRICS", payload: { metadata: { metrics: { records_read: 10 } } } }),
        ]}
      />,
    )

    expect(screen.getByText(/stage started/i)).toBeInTheDocument()
    expect(screen.queryByText(/stage in progress/i)).not.toBeInTheDocument()
    expect(screen.queryByText(/throughput update/i)).not.toBeInTheDocument()
    expect(rows()).toHaveLength(1)
    expect(screen.getByRole("button", { name: "Show 3 routine progress updates" })).toBeInTheDocument()
  })

  it("folds the sink's per-batch table statistics into the routine count (#56)", () => {
    const stats = () =>
      evt({
        event_type: "TABLE_STATS",
        stage_id: "executor",
        payload: { message: "CDC statistics update: public.orders", metadata: { qualified_name: "public.orders" } },
      })
    render(<ReasoningTimeline events={[evt({ event_type: "STAGE_STARTED", stage_id: "executor" }), stats(), stats(), stats()]} />)

    expect(screen.queryByText(/CDC statistics update/)).not.toBeInTheDocument()
    expect(screen.getByRole("button", { name: /show 3 routine progress updates/i })).toBeInTheDocument()
  })

  it("SAFETY: a warning or error row is never hidden, however it is flagged", () => {
    // A filter that can swallow an alarm is worse than the noise it removes.
    render(
      <ReasoningTimeline
        events={[
          evt({ severity: "error", payload: { metadata: { heartbeat: true }, message: "sink worker crashed" } }),
          evt({ event_type: "DATA_PLANE_METRICS", severity: "warn", payload: { message: "throughput collapsed to zero" } }),
        ]}
      />,
    )

    expect(screen.getByText("sink worker crashed")).toBeInTheDocument()
    expect(screen.getByText("throughput collapsed to zero")).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /routine/i })).not.toBeInTheDocument()
  })

  it("discloses how many rows it hid, and hands them back on request", async () => {
    // Hiding rows silently is its own lie. The count is the honest part.
    render(<ReasoningTimeline events={[evt({ event_type: "STAGE_STARTED", stage_id: "executor" }), HEARTBEAT(), HEARTBEAT()]} />)

    await userEvent.click(screen.getByRole("button", { name: "Show 2 routine progress updates" }))

    expect(screen.getAllByText(/stage in progress/i)).toHaveLength(2)
    expect(screen.getByRole("button", { name: "Hide 2 routine progress updates" })).toBeInTheDocument()
  })

  it("positive control: a run that emits nothing but heartbeats does not claim there are no events", () => {
    render(<ReasoningTimeline events={[HEARTBEAT(), HEARTBEAT()]} />)

    expect(screen.getByRole("button", { name: /2 routine/i })).toBeInTheDocument()
    expect(screen.getByText(/only routine progress updates so far/i)).toBeInTheDocument()
    expect(screen.queryByText(NO_EVENTS)).not.toBeInTheDocument()
  })
})

describe("Activity names events in words, not database tokens (F-275)", () => {
  it("renders a human label and never the raw event_type", () => {
    render(<ReasoningTimeline events={[evt({ event_type: "STAGE_COMPLETED", stage_id: "executor" })]} />)

    expect(screen.getByText(/stage completed/i)).toBeInTheDocument()
    expect(screen.queryByText(/STAGE_COMPLETED/)).not.toBeInTheDocument()
  })

  it("names the stage the way the rest of the product names it", () => {
    // `executor` is the database's word; the Steps tab calls it this.
    render(<ReasoningTimeline events={[evt({ event_type: "STAGE_STARTED", stage_id: "executor" })]} />)

    expect(within(rows()[0]).getByText("· Executing Pipeline")).toBeInTheDocument()
  })

  it("labels healer rows as self-healing rather than as raw table values", () => {
    render(
      <ReasoningTimeline
        events={[
          evt({
            event_type: "healer_decision",
            severity: "warn",
            payload: { rationale: "Connector returned 401 three times", suggested_action: "refresh_auth" },
          }),
        ]}
      />,
    )

    expect(within(rows()[0]).getByText("Self-healing decision")).toBeInTheDocument()
    expect(screen.queryByText(/healer_decision/)).not.toBeInTheDocument()
  })

  it("an unknown event type degrades to readable prose, not to a token", () => {
    render(<ReasoningTimeline events={[evt({ event_type: "AGENT_THINKING" })]} />)

    expect(screen.getByText(/Agent thinking/i)).toBeInTheDocument()
    expect(screen.queryByText(/AGENT_THINKING/)).not.toBeInTheDocument()
  })

  it("keeps the codes behind the row's Details toggle", async () => {
    render(
      <ReasoningTimeline
        events={[evt({ event_type: "STAGE_COMPLETED", stage_id: "executor", seq: 42, trace_id: "trace-abc" })]}
      />,
    )

    expect(screen.queryByText("seq: 42")).not.toBeInTheDocument()
    expect(screen.queryByText("trace: trace-abc")).not.toBeInTheDocument()

    const toggle = within(rows()[0]).getByRole("button", { name: "Details" })
    expect(toggle).toHaveAttribute("aria-expanded", "false")
    await userEvent.click(toggle)

    expect(screen.getByText("STAGE_COMPLETED")).toBeInTheDocument()
    expect(screen.getByText("seq: 42")).toBeInTheDocument()
    expect(screen.getByText("stage: executor")).toBeInTheDocument()
    expect(screen.getByText("trace: trace-abc")).toBeInTheDocument()
    expect(within(rows()[0]).getByRole("button", { name: "Hide details" })).toHaveAttribute("aria-expanded", "true")
  })
})

describe("Activity says what happened, using the payload it already has (F-276)", () => {
  it("surfaces the blocking reason on a parked run", () => {
    // The answer to "why is my pipeline not moving".
    render(
      <ReasoningTimeline
        events={[
          evt({
            event_type: "PIPELINE_WAITING",
            payload: { blocking_reason: { type: "user_input_required", description: "Select the tables you want to copy" } },
          }),
        ]}
      />,
    )

    expect(screen.getByText("Select the tables you want to copy")).toBeInTheDocument()
  })

  it("surfaces the step counter when there is no message", () => {
    render(
      <ReasoningTimeline
        events={[
          evt({
            event_type: "STAGE_STARTED",
            stage_id: "discovery",
            payload: { progress: { current_step: 3, total_steps: 7, percent: 42 } },
          }),
        ]}
      />,
    )

    expect(screen.getByText(/step 3 of 7/i)).toBeInTheDocument()
  })

  it("surfaces the table a stage is working on", () => {
    render(
      <ReasoningTimeline
        events={[evt({ event_type: "STAGE_PROGRESS", stage_id: "executor", payload: { metadata: { table_name: "public.orders" } } })]}
      />,
    )

    expect(within(rows()[0]).getByText(/public\.orders/)).toBeInTheDocument()
  })

  it("surfaces the healer's rationale on its row, not only in Key Decisions", () => {
    render(
      <ReasoningTimeline
        events={[evt({ event_type: "healer_decision", payload: { rationale: "Connector returned 401 three times", confidence: 0.8 } })]}
      />,
    )

    expect(within(rows()[0]).getByText("Connector returned 401 three times")).toBeInTheDocument()
  })

  it("positive control: an explicit message still wins over every derived detail", () => {
    render(
      <ReasoningTimeline
        events={[
          evt({
            event_type: "STAGE_COMPLETED",
            payload: { message: "Copied 12 tables", progress: { current_step: 7, total_steps: 7 } },
          }),
        ]}
      />,
    )

    expect(screen.getByText("Copied 12 tables")).toBeInTheDocument()
    expect(screen.queryByText(/step 7 of 7/i)).not.toBeInTheDocument()
  })
})

describe("SENTINEL_ALERT reads its level from payload.status", () => {
  const alert = (status: string) =>
    evt({
      event_type: "SENTINEL_ALERT",
      stage_id: "executor",
      payload: { status, message: "Replication slot is retaining 12 GB of WAL" },
    })

  it("a critical alert is an error row the Errors filter finds", async () => {
    render(
      <ReasoningTimeline
        events={[evt({ event_type: "STAGE_STARTED", stage_id: "executor", severity: "info" }), alert("critical")]}
      />,
    )

    await userEvent.click(screen.getByRole("button", { name: "Errors" }))

    expect(rows()).toHaveLength(1)
    expect(within(rows()[0]).getByText("Replication slot is retaining 12 GB of WAL")).toBeInTheDocument()
  })

  it("maps each watchdog status onto the timeline's severities", () => {
    expect(EventNormalizer.normalize(alert("critical")).severity).toBe("error")
    expect(EventNormalizer.normalize(alert("error")).severity).toBe("error")
    expect(EventNormalizer.normalize(alert("warning")).severity).toBe("warn")
  })

  it("a severity column still wins, and only SENTINEL_ALERT falls back to payload.status", () => {
    expect(EventNormalizer.normalize({ ...alert("critical"), severity: "info" }).severity).toBe("info")
    expect(EventNormalizer.normalize(evt({ payload: { status: "critical" } })).severity).toBe("unknown")
    expect(EventNormalizer.normalize(evt({ severity: "critical" })).severity).toBe("error")
  })
})

// ---------------------------------------------------------------------------
// The panel: failed reads and polling
// ---------------------------------------------------------------------------

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

const eventsCalls: string[] = []
const statusCalls: string[] = []

/**
 * Route every panel read; `events` answers the paged `/events` feed and may
 * throw. The stage-transition read (`/events?event_types=…`) is routed apart
 * and counted apart, so `eventsCalls` counts the feed and nothing else.
 */
function serveEvents(
  events: (url: string) => Response | Promise<Response>,
  transitions: (url: string) => Response | Promise<Response> = () => page([]),
) {
  authFetch.mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.includes("/events") && u.includes("event_types=")) {
      statusCalls.push(u)
      return transitions(u)
    }
    if (u.includes("/events")) {
      eventsCalls.push(u)
      return events(u)
    }
    if (u.includes("/state")) return res(200, { pipeline_id: "p1", status: "completed" })
    if (/\/pipelines\/p1(\?|$)/.test(u)) return res(200, { id: "p1", name: "orders", sync_mode: "batch" })
    return res(200, {})
  })
}

const page = (events: PipelineRunEvent[], more: Record<string, unknown> = {}) =>
  res(200, { events, has_more: false, next_cursor: null, ...more })

const said = (message: string, over: Partial<PipelineRunEvent> = {}) =>
  evt({ event_type: "STAGE_PROGRESS", stage_id: "executor", payload: { message }, ...over })

async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

describe("Activity does not present a failed read as an empty stream (F-284)", () => {
  beforeEach(() => {
    authFetch.mockReset()
    eventsCalls.length = 0
    statusCalls.length = 0
  })

  it("THE REGRESSION: a 500 says the read failed, not that there are no events", async () => {
    serveEvents(() => res(500, { error: "database unavailable" }))

    render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)

    expect(await screen.findByText("Could not load events (HTTP 500).")).toBeInTheDocument()
    expect(screen.queryByText(NO_EVENTS)).not.toBeInTheDocument()
  })

  it("THE REGRESSION: a dropped connection does the same", async () => {
    serveEvents(() => {
      throw new TypeError("Failed to fetch")
    })

    render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)

    expect(await screen.findByText("Could not load events (network error).")).toBeInTheDocument()
    expect(screen.queryByText(NO_EVENTS)).not.toBeInTheDocument()
  })

  it("positive control: a genuinely empty stream still says so", async () => {
    // Without this the fix could pass by never showing the empty state at all.
    serveEvents(() => page([]))

    render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)

    expect(await screen.findByText("No events to display yet.")).toBeInTheDocument()
    expect(screen.queryByText(READ_FAILED)).not.toBeInTheDocument()
  })
})

describe("The Monitoring sub-tabs are controlled", () => {
  beforeEach(() => {
    authFetch.mockReset()
    eventsCalls.length = 0
    statusCalls.length = 0
    featureFlagsManager.updateFlags({ monitoringOverview: true })
  })
  afterEach(() => featureFlagsManager.resetFlags())

  it("the Overview's 'View in Activity' opens the Activity sub-tab", async () => {
    serveEvents(() => page([]))
    render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)

    expect(await screen.findByRole("tab", { name: /Overview/ })).toHaveAttribute("aria-selected", "true")
    await userEvent.click(screen.getByRole("button", { name: "View in Activity" }))

    expect(screen.getByRole("tab", { name: /Activity/ })).toHaveAttribute("aria-selected", "true")
    expect(await screen.findByText("No events to display yet.")).toBeInTheDocument()
  })
})

describe("Activity keeps itself current while it is open", () => {
  beforeEach(() => {
    authFetch.mockReset()
    eventsCalls.length = 0
    statusCalls.length = 0
    vi.useFakeTimers({ shouldAdvanceTime: true })
  })

  it("shows a new row five seconds later without a Reload click", async () => {
    const first = said("Snapshot started")
    const second = said("Copied 12 tables")
    let calls = 0
    serveEvents(() => (++calls === 1 ? page([first]) : page([second, first])))

    render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)
    await screen.findByText("Snapshot started")
    expect(screen.queryByText("Copied 12 tables")).not.toBeInTheDocument()

    await advance(5000)

    expect(await screen.findByText("Copied 12 tables")).toBeInTheDocument()
    expect(screen.getByText("Snapshot started")).toBeInTheDocument()
    expect(eventsCalls.length).toBeGreaterThanOrEqual(2)
  })

  it("a failed poll keeps the rows and says they may be out of date", async () => {
    let calls = 0
    serveEvents(() => (++calls === 1 ? page([said("Snapshot started")]) : res(500, { error: "boom" })))

    render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)
    await screen.findByText("Snapshot started")

    await advance(5000)

    expect(
      await screen.findByText("Could not load events (HTTP 500). The list below may be out of date."),
    ).toBeInTheDocument()
    expect(screen.getByText("Snapshot started")).toBeInTheDocument()
  })

  // Moved here from monitor-tab-read-failures.test.tsx with the Live events
  // card (KI-POLLEDJSON-404-STOPS-POLLING-SILENTLY): a 404 is a failed read, not
  // an empty stream, and the feed must come back on its own when the pipeline does.
  it("a 404 says the read failed, and the next poll recovers", async () => {
    let missing = true
    serveEvents(() => (missing ? res(404, { error: "Pipeline not found" }) : page([said("Snapshot started")])))

    render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)
    expect(await screen.findByText("Could not load events (pipeline not found).")).toBeInTheDocument()
    expect(screen.queryByText(NO_EVENTS)).not.toBeInTheDocument()

    missing = false
    await advance(5000)

    expect(await screen.findByText("Snapshot started")).toBeInTheDocument()
    expect(screen.queryByText(READ_FAILED)).not.toBeInTheDocument()
  })

  it("rows pulled in with Load more survive a poll", async () => {
    const a = said("Row A")
    const b = said("Row B")
    const older = said("Older row")
    const newest = said("Newest row")
    const cursor = { before_ts: "2026-09-18T11:00:00Z", before_seq: 0, before_event_id: b.event_id }
    let firstPage = true
    serveEvents((url) => {
      if (url.includes("before_event_id")) return page([older])
      if (firstPage) {
        firstPage = false
        return page([a, b], { has_more: true, next_cursor: cursor })
      }
      return page([newest, a, b], { has_more: true, next_cursor: cursor })
    })

    render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)
    await screen.findByText("Row A")
    await userEvent.click(screen.getByRole("button", { name: /load more events/i }))
    await screen.findByText("Older row")

    await advance(5000)

    expect(await screen.findByText("Newest row")).toBeInTheDocument()
    expect(screen.getByText("Older row")).toBeInTheDocument()
  })

  it("does not poll while the page is hidden", async () => {
    serveEvents(() => page([said("Snapshot started")]))
    const visibility = vi.spyOn(document, "visibilityState", "get").mockReturnValue("hidden")
    try {
      render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)
      await screen.findByText("Snapshot started")
      const before = eventsCalls.length

      await advance(15000)

      expect(eventsCalls.length).toBe(before)
    } finally {
      visibility.mockRestore()
    }
  })

  it("the Table statistics variant does not poll events", async () => {
    serveEvents(() => page([]))

    render(<PipelineMonitoringPanel pipelineId="p1" variant="table_stats" />)
    await waitFor(() => expect(eventsCalls.length).toBe(1))

    await advance(15000)

    expect(eventsCalls.length).toBe(1)
  })
})

// The two display defects from the prod retest: a 21-hour stage printed as
// "1262.1m", and a stage badge that changed with the pages loaded.
describe("Activity stage headers", () => {
  const stage = (event_type: string, occurred_at: string, over: Partial<PipelineRunEvent> = {}) =>
    evt({ event_type, stage_id: "executor", occurred_at, ...over })
  const started = stage("STAGE_STARTED", "2026-09-15T13:00:00Z")
  const completed = stage("STAGE_COMPLETED", "2026-09-16T10:02:06Z")
  const late = stage("STAGE_PROGRESS", "2026-09-16T10:03:00Z", { payload: { message: "Wrote table stats" } })

  it("prints a long stage in hours and minutes, not thousands of minutes", () => {
    render(<ReasoningTimeline events={[completed, started]} />)
    expect(screen.getByText("21h 2m")).toBeInTheDocument()
    expect(screen.queryByText("1262.1m")).not.toBeInTheDocument()
  })

  it("takes the badge from the transitions read, and shows none it cannot back", () => {
    const { rerender } = render(<ReasoningTimeline events={[late]} />)
    // Nothing loaded says where the stage is: no badge, and never "Pending".
    expect(screen.queryByText("Pending")).not.toBeInTheDocument()
    expect(screen.queryByText("Completed")).not.toBeInTheDocument()

    rerender(<ReasoningTimeline events={[late]} statusEvents={[completed, started]} />)
    expect(screen.getByText("Completed")).toBeInTheDocument()
    expect(screen.getByText("21h 2m")).toBeInTheDocument()
  })
})

describe("Activity reads stage transitions apart from the paged feed", () => {
  beforeEach(() => {
    authFetch.mockReset()
    eventsCalls.length = 0
    statusCalls.length = 0
  })

  const failedRun = [
    evt({ event_type: "STAGE_FAILED", stage_id: "executor", occurred_at: "2026-09-16T10:05:00Z" }),
    evt({ event_type: "STAGE_STARTED", stage_id: "executor", occurred_at: "2026-09-16T10:00:00Z" }),
  ]

  it("asks for every transition type and badges the stage from the answer", async () => {
    serveEvents(
      () => page([said("Wrote table stats", { occurred_at: "2026-09-16T10:06:00Z" })]),
      () => page(failedRun),
    )

    render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)

    await screen.findByText("Wrote table stats")
    expect(await screen.findByText("Failed")).toBeInTheDocument()
    expect(statusCalls).toHaveLength(1)
    const qs = new URL(statusCalls[0], "http://x").searchParams
    expect(qs.get("event_types")!.split(",").sort()).toEqual([
      "PIPELINE_COMPLETED",
      "PIPELINE_FAILED",
      "PIPELINE_WAITING",
      "STAGE_COMPLETED",
      "STAGE_FAILED",
      "STAGE_STARTED",
    ])
    expect(qs.get("limit")).toBe("500")
  })

  it("a failed transitions read keeps the rows and adds no badge", async () => {
    serveEvents(
      () => page([said("Wrote table stats", { occurred_at: "2026-09-16T10:06:00Z" })]),
      () => res(500, { error: "boom" }),
    )

    render(<PipelineMonitoringPanel pipelineId="p1" variant="monitoring" />)

    await screen.findByText("Wrote table stats")
    await waitFor(() => expect(statusCalls).toHaveLength(1))
    expect(screen.queryByText("Failed")).not.toBeInTheDocument()
    expect(screen.queryByText(READ_FAILED)).not.toBeInTheDocument()
  })
})

describe("mergeNewestPage", () => {
  const id = (event_id: string) => ({ event_id })

  it("prepends new rows and keeps older loaded pages when the fresh page overlaps", () => {
    const out = mergeNewestPage([id("b"), id("a")], [id("c"), id("b")])
    expect(out).toEqual({ events: [id("c"), id("b"), id("a")], replaced: false })
  })

  it("an overlapping poll with nothing new leaves the list as it was", () => {
    const out = mergeNewestPage([id("b"), id("a")], [id("b"), id("a")])
    expect(out).toEqual({ events: [id("b"), id("a")], replaced: false })
  })

  it("nothing shown yet: the fresh page is the list", () => {
    expect(mergeNewestPage([], [id("a")])).toEqual({ events: [id("a")], replaced: true })
  })

  it("no overlap: replaces rather than stitch across a gap", () => {
    // A full page with no overlap means more arrived than one page holds; a
    // short one means the shown rows are gone. Either way, not continuous.
    expect(mergeNewestPage([id("b"), id("a")], [id("d"), id("c")])).toEqual({
      events: [id("d"), id("c")],
      replaced: true,
    })
  })
})
