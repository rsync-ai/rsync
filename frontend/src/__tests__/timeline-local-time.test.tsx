/**
 * Stage and reasoning timelines printed `toISOString().slice(11, 19)` — the UTC
 * clock time ("10:00:00"), with no date and no zone, which a viewer outside UTC
 * reads as their own time. Same defect #23 fixed for the table-statistics panel;
 * these two use the same <LocalDateTime> now.
 *
 * Every assertion runs under Asia/Kolkata, because under UTC the bug and the fix
 * print the same hour. 10:00 UTC is 15:30 there.
 *
 * ReasoningTimeline also crashed outright on an event whose timestamp does not
 * parse: `new Date("garbage").toISOString()` throws a RangeError during render.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen } from "@testing-library/react"
import { act } from "react"
import { hydrateRoot } from "react-dom/client"
import { renderToString } from "react-dom/server"
import "@testing-library/jest-dom"

import { StageTimeline } from "@/components/pipeline/StageTimeline"
import { ReasoningTimeline } from "@/components/pipeline/ReasoningTimeline"
import type { StageExecution } from "@/lib/pipeline/stageDefinitions"
import type { PipelineRunEvent } from "@/lib/pipeline/eventNormalizer"
import { formatAbsoluteTime } from "@/lib/utils"

const TS = "2026-08-05T10:00:00Z"
const KOLKATA_TIME = /\b(03:30\s?PM|15:30)\b/
const KOLKATA_ZONE = /GMT\+5:30/
// What both components printed before.
const OLD_UTC_SLICE = new Date(TS).toISOString().slice(11, 19)

let savedTZ: string | undefined
beforeEach(() => {
  savedTZ = process.env.TZ
  process.env.TZ = "Asia/Kolkata"
})
afterEach(() => {
  cleanup()
  if (savedTZ === undefined) delete process.env.TZ
  else process.env.TZ = savedTZ
})

function stage(over: Partial<StageExecution> = {}): StageExecution {
  return {
    stage: "schema_discovery",
    label: "Schema discovery",
    description: "Reads the source schema",
    icon: "",
    status: "completed",
    attempts: [],
    startedAt: Date.parse(TS),
    completedAt: Date.parse(TS) + 42_000,
    durationMs: 42_000,
    ...over,
  }
}

function runEvent(over: Partial<PipelineRunEvent> = {}): PipelineRunEvent {
  return {
    pipeline_id: "p1",
    execution_id: "e1",
    event_id: "ev-1",
    seq: 1,
    event_type: "STAGE_STARTED",
    stage_group: "discovery",
    severity: "info",
    occurred_at: TS,
    received_at: TS,
    payload: {},
    ...over,
  }
}

function timeText(container: HTMLElement): string {
  const times = container.querySelectorAll("time")
  expect(times.length).toBeGreaterThan(0)
  return Array.from(times)
    .map((t) => t.textContent || "")
    .join(" | ")
}

describe("control — the zone matters for these assertions", () => {
  it("the old UTC slice is not the Kolkata time", () => {
    expect(OLD_UTC_SLICE).toBe("10:00:00")
    expect(OLD_UTC_SLICE).not.toMatch(KOLKATA_TIME)
    expect(formatAbsoluteTime(TS)).toMatch(KOLKATA_TIME)
  })
})

describe("StageTimeline — stage start time", () => {
  it("shows the viewer's local time with the zone named", () => {
    const { container } = render(<StageTimeline stages={[stage()]} />)
    const text = timeText(container)
    expect(text).toMatch(KOLKATA_TIME)
    expect(text).toMatch(KOLKATA_ZONE)
    expect(container.textContent).not.toContain(OLD_UTC_SLICE)
    expect(container.querySelector("time")).toHaveAttribute("dateTime", "2026-08-05T10:00:00.000Z")
    // The duration next to it, in the Steps graph's format ("42.0s" before the
    // shared formatter dropped the trailing zero).
    expect(screen.getByText("42s")).toBeInTheDocument()
  })

  it("server-renders UTC labelled as UTC and hydrates without a mismatch", async () => {
    const node = <StageTimeline stages={[stage()]} />
    const html = renderToString(node)
    expect(html).toContain("2026-08-05 10:00 UTC")
    expect(html).not.toMatch(KOLKATA_ZONE)

    const container = document.createElement("div")
    container.innerHTML = html
    document.body.appendChild(container)
    const recoverable = vi.fn()
    await act(async () => {
      hydrateRoot(container, node, { onRecoverableError: recoverable })
    })
    expect(recoverable).not.toHaveBeenCalled()
    expect(timeText(container)).toMatch(KOLKATA_ZONE)
    container.remove()
  })

  it("shows no time for a stage that has not started", () => {
    const { container } = render(
      <StageTimeline stages={[stage({ status: "pending", startedAt: undefined, completedAt: undefined })]} />,
    )
    expect(container.querySelector("time")).toBeNull()
    expect(screen.getByText("Schema discovery")).toBeInTheDocument()
  })
})

describe("ReasoningTimeline — event time", () => {
  it("shows the viewer's local time with the zone named", () => {
    const { container } = render(<ReasoningTimeline events={[runEvent()]} />)
    const text = timeText(container)
    expect(text).toMatch(KOLKATA_TIME)
    expect(text).toMatch(KOLKATA_ZONE)
    expect(container.textContent).not.toContain(OLD_UTC_SLICE)
  })

  it("server-renders UTC labelled as UTC and hydrates without a mismatch", async () => {
    const node = <ReasoningTimeline events={[runEvent()]} />
    const html = renderToString(node)
    expect(html).toContain("2026-08-05 10:00 UTC")

    const container = document.createElement("div")
    container.innerHTML = html
    document.body.appendChild(container)
    const recoverable = vi.fn()
    await act(async () => {
      hydrateRoot(container, node, { onRecoverableError: recoverable })
    })
    expect(recoverable).not.toHaveBeenCalled()
    expect(timeText(container)).toMatch(KOLKATA_TIME)
    container.remove()
  })

  it("renders an event with an unparseable timestamp instead of crashing", () => {
    // Control: this is what the old render expression did with such a value.
    expect(() => new Date("garbage").toISOString()).toThrow(RangeError)

    const events = [
      runEvent({ event_id: "bad", occurred_at: "garbage", received_at: "garbage", event_type: "STAGE_STARTED" }),
      runEvent({ event_id: "good", seq: 2 }),
    ]
    const { container } = render(<ReasoningTimeline events={events} />)
    expect(screen.getByText("Time unknown")).toBeInTheDocument()
    // The valid event beside it still gets its local time.
    expect(timeText(container)).toMatch(KOLKATA_TIME)
    expect(container.querySelectorAll("time")).toHaveLength(1)
  })
})
