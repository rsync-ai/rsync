import { afterEach, describe, expect, it } from "vitest"
import { cleanup, render, screen, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import {
  MIN_BAR_PERCENT,
  RunDurationChart,
  SKIPPED_BAR_PERCENT,
  barHeightPercent,
  longestRunMs,
} from "@/components/explorer/RunDurationChart"
import { formatAbsoluteTime } from "@/components/explorer/scheduleTime"
import type { SavedQueryRun } from "@/components/explorer/scheduledModel"

function run(id: string, startedAt: string, overrides: Partial<SavedQueryRun> = {}): SavedQueryRun {
  return {
    run_id: id,
    saved_query_id: "q-1",
    trigger_source: "scheduled",
    status: "succeeded",
    started_at: startedAt,
    finished_at: startedAt,
    duration_ms: 60_000,
    ...overrides,
  }
}

// Newest first, the order GET /explorer/saved/:id/runs sends them in.
const RUNS = [
  run("r-newest", "2026-09-16T03:00:00Z", { status: "skipped", duration_ms: -1, skip_reason: "waiting_on_upstreams" }),
  run("r-failed", "2026-09-15T03:00:00Z", { status: "failed", duration_ms: 30_000 }),
  run("r-tiny", "2026-09-14T03:00:00Z", { duration_ms: 5 }),
  run("r-oldest", "2026-09-13T03:00:00Z", { duration_ms: 120_000 }),
]

function bars() {
  return within(screen.getByRole("list", { name: "Run durations, oldest first" })).getAllByRole("listitem")
}

function fill(bar: HTMLElement): HTMLElement {
  return within(bar).getByTestId("run-bar")
}

describe("RunDurationChart", () => {
  afterEach(cleanup)

  it("draws one bar per run, oldest on the left", () => {
    render(<RunDurationChart runs={RUNS} />)

    expect(bars().map((b) => b.getAttribute("aria-label"))).toEqual([
      `${formatAbsoluteTime("2026-09-13T03:00:00Z")}: succeeded in 2m`,
      `${formatAbsoluteTime("2026-09-14T03:00:00Z")}: succeeded in 5ms`,
      `${formatAbsoluteTime("2026-09-15T03:00:00Z")}: failed in 30s`,
      `${formatAbsoluteTime("2026-09-16T03:00:00Z")}: skipped`,
    ])
  })

  it("colours succeeded green, failed red and skipped amber", () => {
    render(<RunDurationChart runs={[...RUNS, run("r-future", "2026-09-12T03:00:00Z", { status: "cancelled" })]} />)

    const byStatus = Object.fromEntries(bars().map((b) => [b.dataset.status, fill(b).className]))
    expect(byStatus.succeeded).toMatch(/\bbg-emerald-500\b/)
    expect(byStatus.failed).toMatch(/\bbg-red-500\b/)
    expect(byStatus.skipped).toMatch(/\bbg-amber-400\b/)
    // A status this client does not know is still drawn, in grey, not dropped.
    expect(byStatus.cancelled).toMatch(/\bbg-zinc-400\b/)
  })

  it("scales each bar to the longest run on the page", () => {
    render(<RunDurationChart runs={RUNS} />)
    const [oldest, tiny, failed, skipped] = bars().map((b) => fill(b).style.height)

    expect(oldest).toBe("100%")
    expect(failed).toBe("25%")
    // 5 ms against 2 minutes is still a visible bar.
    expect(tiny).toBe(`${MIN_BAR_PERCENT}%`)
    // A skip is a stub whatever its recorded duration, here -1 ms of clock jitter.
    expect(skipped).toBe(`${SKIPPED_BAR_PERCENT}%`)
    expect(screen.getByText(/longest 2m/)).toBeInTheDocument()
  })

  it("does not let a skip set the scale", () => {
    // A skipped row with a large recorded duration must not shrink the runs that did work.
    const runs = [
      run("r-skip", "2026-09-16T03:00:00Z", { status: "skipped", duration_ms: 999_999 }),
      run("r-ok", "2026-09-15T03:00:00Z", { duration_ms: 1_000 }),
    ]
    expect(longestRunMs(runs)).toBe(1_000)
    expect(barHeightPercent(runs[1], longestRunMs(runs))).toBe(100)
  })

  // Seen in a browser on 2026-09-16: stg_orders ran in 16 to 181 ms, its bars stood at 9%
  // and 100%, and both said "in 0s", as did the caption's "longest 0s".
  it("gives sub-second runs drawn at different heights different durations", () => {
    const runs = [
      run("r-slow", "2026-09-16T03:00:00Z", { duration_ms: 181 }),
      run("r-fast", "2026-09-15T03:00:00Z", { status: "failed", duration_ms: 16 }),
    ]
    render(<RunDurationChart runs={runs} />)

    const [fast, slow] = bars()
    expect(fill(fast).style.height).not.toBe(fill(slow).style.height)
    expect(fast).toHaveAttribute("aria-label", `${formatAbsoluteTime("2026-09-15T03:00:00Z")}: failed in 16ms`)
    expect(slow).toHaveAttribute("aria-label", `${formatAbsoluteTime("2026-09-16T03:00:00Z")}: succeeded in 181ms`)
    expect(screen.getByText(/longest 181ms/)).toBeInTheDocument()
  })

  it("gives no scale when nothing on the page did work", () => {
    const skips = [
      run("r-2", "2026-09-16T03:00:00Z", { status: "skipped", duration_ms: 0 }),
      run("r-1", "2026-09-15T03:00:00Z", { status: "skipped", duration_ms: 0 }),
    ]
    render(<RunDurationChart runs={skips} />)

    expect(bars()).toHaveLength(2)
    expect(screen.queryByText(/longest/)).not.toBeInTheDocument()
  })

  it("names the run under the pointer", async () => {
    const user = userEvent.setup()
    render(<RunDurationChart runs={RUNS} />)
    const failedLabel = `${formatAbsoluteTime("2026-09-15T03:00:00Z")}: failed in 30s`

    // Until the pointer arrives the sentence is only the bar's accessible name, not text.
    expect(screen.queryByText(failedLabel)).not.toBeInTheDocument()
    await user.hover(bars()[2])
    expect(screen.getByText(failedLabel)).toBeInTheDocument()
    await user.unhover(bars()[2])
    expect(screen.queryByText(failedLabel)).not.toBeInTheDocument()
  })

  it("draws nothing for no runs", () => {
    const { container } = render(<RunDurationChart runs={[]} />)
    expect(container).toBeEmptyDOMElement()
  })
})
