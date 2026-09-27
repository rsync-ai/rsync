/**
 * The Consumers card on the pipeline's Data flow tab.
 *
 * It exists because Admin → Health's consumer table answers a different question
 * for different people: those rows come from sentinel_component_health, keyed by
 * TOPIC, written from a hardcoded list of the topics the orchestrator process
 * itself consumes. Not one pipeline data topic is in it,
 * and the route is admin-only because that table has no workspace column. This
 * card reads the pipeline's own census.
 *
 * What the tests hold:
 *  - a failed read is not "no consumers", and an unmeasured pipeline is not an
 *    empty one — three outcomes, three different sentences;
 *  - `Empty` with 0 members is surfaced as a fault even at lag 0, because that is
 *    the state lag cannot express: nobody is consuming;
 *  - a missing `state` renders as unknown, never as Stable.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { cleanup, render, screen, within } from "@testing-library/react"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
}))

import { MonitorTab } from "@/components/pipeline/MonitorTab"

type Reply = { status?: number; body?: unknown }

const ago = (ms: number) => new Date(Date.now() - ms).toISOString()

let replies: { runtime: Reply; consumers: Reply; tables: Reply; diagnose: Reply }

const cdcRuntime = (): Reply => ({
  body: {
    pipeline_id: "p1",
    execution_id: "p1",
    mode: "cdc",
    phase: "streaming",
    health: "healthy",
    dependencies: [],
    updated_at: ago(0),
    liveness: { pending_events: 0 },
  },
})

const consumer = (extra: Record<string, unknown> = {}) => ({
  group: "rsync.sink-aa4c1a3c",
  role: "sink",
  state: "Stable",
  members: 1,
  total_lag: 0,
  measured_at: ago(20_000),
  topics: [{ topic: "rsync.cdc-aa4c1a3c.public.orders", table: "public.orders", lag: 0, committed: 98000 }],
  ...extra,
})

beforeEach(() => {
  replies = {
    runtime: cdcRuntime(),
    consumers: { body: { consumers: [consumer()], measured: true } },
    tables: { body: { summary: { mode: "cdc", total_tables: 1 }, tables: [], total: 0 } },
    diagnose: { body: {} },
  }
  authFetch.mockImplementation(async (url: string) => {
    const pick = (): Reply =>
      url.includes("/consumers")
        ? replies.consumers
        : url.includes("/table-stats")
          ? replies.tables
          : url.includes("/diagnose")
            ? replies.diagnose
            : replies.runtime
    const r = pick()
    const status = r.status ?? 200
    return { ok: status >= 200 && status < 300, status, json: async () => r.body ?? {} } as unknown as Response
  })
})

afterEach(() => {
  cleanup()
  authFetch.mockReset()
})

describe("ConsumersCard", () => {
  it("lists the pipeline's consumer groups with per-topic lag", async () => {
    replies.consumers = {
      body: {
        measured: true,
        consumers: [
          consumer({
            total_lag: 120,
            topics: [
              { topic: "rsync.cdc-aa4c1a3c.public.orders", table: "public.orders", lag: 120, committed: 98000 },
              { topic: "rsync.cdc-aa4c1a3c.public.users", table: "public.users", lag: 0, committed: 4100 },
            ],
          }),
        ],
      },
    }
    render(<MonitorTab pipelineId="p1" />)

    await screen.findByText("rsync.sink-aa4c1a3c")
    const card = screen.getByTestId("consumers-card")
    expect(within(card).getByText("rsync.sink-aa4c1a3c")).toBeInTheDocument()
    expect(within(card).getByText("Writes changes to the destination")).toBeInTheDocument()
    expect(within(card).getByText("120 behind")).toBeInTheDocument()
    // The table name, not the topic, when the topic is one of this pipeline's.
    expect(within(card).getByText("public.orders")).toBeInTheDocument()
    expect(within(card).getByText("public.users")).toBeInTheDocument()
  })

  it("surfaces an Empty group as a fault even though its lag is 0", async () => {
    // The reading lag alone cannot give: the queue is drained because nobody is
    // reading it, not because the work is done.
    replies.consumers = {
      body: { measured: true, consumers: [consumer({ state: "Empty", members: 0, total_lag: 0 })] },
    }
    render(<MonitorTab pipelineId="p1" />)

    await screen.findByText("Empty · 0 members")
    const card = screen.getByTestId("consumers-card")
    expect(within(card).getByText("Empty · 0 members")).toBeInTheDocument()
    expect(within(card).getByText(/no consumer is reading this/)).toBeInTheDocument()
  })

  it("renders a missing state as unknown, never as Stable", async () => {
    const c = consumer()
    delete (c as Record<string, unknown>).state
    delete (c as Record<string, unknown>).members
    replies.consumers = { body: { measured: true, consumers: [c] } }
    render(<MonitorTab pipelineId="p1" />)

    await screen.findByText("Unknown")
    const card = screen.getByTestId("consumers-card")
    expect(within(card).getByText("Unknown")).toBeInTheDocument()
    expect(within(card).queryByText(/Stable/)).toBeNull()
  })

  it("says nothing has measured yet, rather than showing an empty list", async () => {
    replies.consumers = { body: { consumers: [], measured: false } }
    render(<MonitorTab pipelineId="p1" />)

    await screen.findByText(/has not been measured/)
    const card = screen.getByTestId("consumers-card")
  })

  it("distinguishes a measured pipeline with no consumers from an unmeasured one", async () => {
    replies.consumers = { body: { consumers: [], measured: true } }
    render(<MonitorTab pipelineId="p1" />)

    await screen.findByText(/No consumers running yet/)
    const card = screen.getByTestId("consumers-card")
  })

  it("says a failed read failed instead of reporting no consumers", async () => {
    // An empty list here reads as "nothing is moving your data" — the F-280
    // failure mode, which must never be drawn from a read that did not come back.
    replies.consumers = { status: 500, body: {} }
    render(<MonitorTab pipelineId="p1" />)

    await screen.findByText(/Couldn't load consumers/)
    const card = screen.getByTestId("consumers-card")
    expect(within(card).getByText(/Couldn't load consumers/)).toBeInTheDocument()
    expect(within(card).queryByText(/No consumers running yet/)).toBeNull()
  })

  it("is not rendered for a batch pipeline", async () => {
    // A batch pipeline moves rows through the executor, not a consumer group.
    replies.runtime = {
      body: {
        pipeline_id: "p1",
        execution_id: "e1",
        mode: "batch",
        phase: "completed",
        health: "healthy",
        dependencies: [],
        updated_at: ago(0),
      },
    }
    render(<MonitorTab pipelineId="p1" />)

    await screen.findByText("Throughput")
    expect(screen.queryByTestId("consumers-card")).toBeNull()
    expect(authFetch.mock.calls.filter((c) => String(c[0]).includes("/consumers"))).toHaveLength(0)
  })
})
