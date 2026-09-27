/**
 * Steps/DAG for a CDC pipeline leads with the live stream, not with a frozen plan.
 *
 * The tab renders `pipeline_progress.metadata.execution_plan`, built ONCE by the
 * Temporal workflow before any data moves: eight agent setup stages from
 * "Understanding Request" to "Executing Pipeline". For a stream the workflow then
 * completes `executor`, marks the execution `streaming_active` and RETURNS
 * (`nl_pipeline_v2_workflow.go:2063-2096`) — no further stage is ever written. So
 * the tab showed a permanent all-green record of a setup that finished hours ago,
 * with nothing on it about the data now moving.
 *
 * Those steps still matter for a setup that FAILED, which is the "my pipeline
 * hangs" case, and for batch pipelines, where the plan does describe the run. So
 * they are folded away rather than removed — and folded open again the moment one
 * of them failed.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { cleanup, render, screen, fireEvent, within } from "@testing-library/react"
import "@testing-library/jest-dom"

const push = vi.fn()
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push, refresh: vi.fn() }),
}))

// React Flow needs layout APIs jsdom does not provide; the graph's own rendering
// is not what this file is about.
vi.mock("@/components/pipeline/DAGVisualizationV2", () => ({
  DAGVisualizationV2: ({ stages }: { stages: { id: string }[] }) => (
    <div data-testid="dag">{stages.map((s) => s.id).join(",")}</div>
  ),
}))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
}))

import { StepsDAGTab } from "@/components/pipeline/StepsDAGTab"

const SETUP_STAGES = [
  { id: "intent", display_name: "Understanding Request", status: "complete" },
  { id: "planner", display_name: "Creating Plan", status: "complete" },
  { id: "executor", display_name: "Executing Pipeline", status: "complete" },
]

let planStages: { id: string; display_name: string; status: string }[]
let planMetadata: Record<string, unknown> | undefined
let connectorTypes: Record<string, string>
// data_loading_strategy is an OBJECT, and normalizeStrategyMode only reads the
// literal "cdc" — the plan's own `mode` is hardcoded "batch" by the workflow
// (nl_pipeline_v2_workflow.go:325), which is why the tab re-fetches this at all.
let strategy: { mode: string }

function reply(url: string) {
  if (url.includes("/state")) {
    return {
      execution_id: "e1",
      execution_plan: { pipeline_id: "p1", mode: "batch", stages: planStages, metadata: planMetadata },
    }
  }
  if (url.includes("/events")) return { events: [] }
  if (url.includes("/consumers")) return { consumers: [], measured: true }
  if (url.includes("/runtime")) {
    return {
      pipeline_id: "p1",
      mode: "cdc",
      phase: "streaming",
      health: "healthy",
      dependencies: [],
      updated_at: new Date().toISOString(),
      liveness: { pending_events: 0 },
    }
  }
  if (url.includes("/connections/")) {
    const id = url.split("/connections/")[1]?.split(/[/?]/)[0] ?? ""
    return { connector_type: connectorTypes[id] ?? "postgresql" }
  }
  // GET /pipelines/:id
  return {
    id: "p1",
    name: "Chat Pipeline",
    data_loading_strategy: strategy,
    source_connection_id: "c-src",
    destination_connection_id: "c-dst",
  }
}

beforeEach(() => {
  planStages = [...SETUP_STAGES]
  planMetadata = undefined
  connectorTypes = {}
  strategy = { mode: "cdc" }
  authFetch.mockImplementation(async (url: string) => ({
    ok: true,
    status: 200,
    json: async () => reply(String(url)),
  }))
})

afterEach(() => {
  cleanup()
  authFetch.mockReset()
  push.mockReset()
})

describe("Steps/DAG for a stream", () => {
  it("leads with the live stream and folds the one-time setup away", async () => {
    render(<StepsDAGTab pipelineId="p1" />)

    // The live view is present…
    expect(await screen.findByTestId("live-stream-graph")).toBeInTheDocument()
    expect(screen.getByText(/The steps below ran once/)).toBeInTheDocument()

    // …and the setup steps are collapsed behind a summary that says what they are.
    const toggle = screen.getByTestId("setup-toggle")
    expect(toggle).toHaveAttribute("aria-expanded", "false")
    expect(toggle).toHaveTextContent(/run once when this pipeline was created/)
  })

  it("opens the setup steps on demand", async () => {
    render(<StepsDAGTab pipelineId="p1" />)
    const toggle = await screen.findByTestId("setup-toggle")

    fireEvent.click(toggle)
    expect(toggle).toHaveAttribute("aria-expanded", "true")
  })

  // Prod, 2026-09-25: opening Setup drew the graph tiny and jammed at the left
  // edge. The graph was mounted inside a display:none box, so React Flow's
  // fitView ran on first node measurement against the pane size it had then,
  // not the 540px box it was about to get. Mount it only when it can be seen.
  it("does not mount the setup graph while Setup is folded", async () => {
    render(<StepsDAGTab pipelineId="p1" />)
    const toggle = await screen.findByTestId("setup-toggle")
    expect(screen.queryByTestId("dag")).toBeNull()

    fireEvent.click(toggle)
    expect(screen.getByTestId("dag")).toHaveTextContent("intent,planner,executor")

    fireEvent.click(toggle)
    expect(screen.queryByTestId("dag")).toBeNull()
  })

  it("opens the setup steps by itself when one of them failed", async () => {
    // A failed setup is the "my pipeline hangs" case, and this graph is then the
    // most useful thing on the page — it must not be hidden behind a click.
    planStages = [
      { id: "intent", display_name: "Understanding Request", status: "complete" },
      { id: "connection_validation", display_name: "Validating Connections", status: "failed" },
    ]
    render(<StepsDAGTab pipelineId="p1" />)

    const toggle = await screen.findByTestId("setup-toggle")
    expect(toggle).toHaveAttribute("aria-expanded", "true")
    expect(toggle).toHaveTextContent(/one did not finish/)
  })

  it("leaves a batch pipeline's plan exactly as it was", async () => {
    // Batch runs ARE described by their plan, so nothing is folded and no live
    // stream view is offered — a batch pipeline has no consumer group.
    strategy = { mode: "batch" }
    render(<StepsDAGTab pipelineId="p1" />)

    await screen.findByTestId("dag")
    expect(screen.queryByTestId("setup-toggle")).toBeNull()
    expect(screen.queryByTestId("live-stream-graph")).toBeNull()
  })
})

/**
 * The Graph/Timeline switch, the connector names and the step count.
 *
 * Prod, 2026-09-25: on a CDC pipeline, Timeline did nothing. The switch sat in the
 * tab header and flipped a view that lives inside the folded-away Setup section,
 * so the only visible effect was the button's own colour. It now sits on the
 * Setup row and only while Setup is open — the only time it has something to
 * switch — and says which view is on with aria-pressed rather than colour alone.
 */
describe("Steps/DAG view switch", () => {
  it("CDC: offers no Graph/Timeline switch while Setup is folded", async () => {
    render(<StepsDAGTab pipelineId="p1" />)
    await screen.findByTestId("setup-toggle")
    expect(screen.queryByRole("button", { name: /timeline/i })).toBeNull()
  })

  it("CDC: Timeline switches the Setup view once it is open", async () => {
    render(<StepsDAGTab pipelineId="p1" />)
    fireEvent.click(await screen.findByTestId("setup-toggle"))

    const graph = screen.getByRole("button", { name: /graph/i })
    const timeline = screen.getByRole("button", { name: /timeline/i })
    expect(graph).toHaveAttribute("aria-pressed", "true")
    expect(timeline).toHaveAttribute("aria-pressed", "false")
    expect(screen.getByTestId("dag")).toBeInTheDocument()

    fireEvent.click(timeline)
    expect(timeline).toHaveAttribute("aria-pressed", "true")
    expect(graph).toHaveAttribute("aria-pressed", "false")
    expect(screen.queryByTestId("dag")).toBeNull()
    expect(screen.getByText("Understanding Request")).toBeInTheDocument()
  })

  it("control: a batch pipeline keeps the switch in the header, and it works", async () => {
    strategy = { mode: "batch" }
    render(<StepsDAGTab pipelineId="p1" />)
    await screen.findByTestId("dag")
    const timeline = screen.getByRole("button", { name: /timeline/i })
    expect(timeline).toHaveAttribute("aria-pressed", "false")
    fireEvent.click(timeline)
    expect(timeline).toHaveAttribute("aria-pressed", "true")
    expect(screen.queryByTestId("dag")).toBeNull()
  })
})

describe("Steps/DAG labels", () => {
  it("names connectors the way the rest of the app does", async () => {
    // prettyConnectorLabel title-cased the id, so the live strip read
    // "Capture from Mongodb" and "Write to Gcs".
    connectorTypes = { "c-src": "mongodb", "c-dst": "gcs" }
    render(<StepsDAGTab pipelineId="p1" />)
    const live = await screen.findByTestId("live-stream-graph")
    expect(await within(live).findByText("Capture from MongoDB")).toBeInTheDocument()
    expect(within(live).getByText("Write to Google Cloud Storage")).toBeInTheDocument()
    expect(live.textContent).not.toMatch(/Mongodb|Gcs/)
  })

  it("counts the steps it draws, not the plan's stored node_count", async () => {
    // The plan's metadata is written once, by the planner; the tab then drops
    // passed-over stages and synthesises source/destination nodes, so the stored
    // count disagreed with the picture above it.
    planMetadata = { node_count: 8, edge_count: 7 }
    strategy = { mode: "batch" }
    render(<StepsDAGTab pipelineId="p1" />)
    await screen.findByTestId("dag")
    expect(screen.getByTestId("steps-count")).toHaveTextContent("Steps: 3")
    expect(screen.queryByText(/Nodes:/)).toBeNull()
  })

  it("CDC: the setup-only prompts live inside Setup, not above the live stream", async () => {
    // "Why is 'Understanding Request' the slowest?" is a question about a setup
    // that finished long ago; above the live view it read as being about the stream.
    render(<StepsDAGTab pipelineId="p1" />)
    await screen.findByTestId("live-stream-graph")
    expect(screen.queryByText("Ask about this pipeline")).toBeNull()
    fireEvent.click(screen.getByTestId("setup-toggle"))
    expect(screen.getByText("Ask about this pipeline")).toBeInTheDocument()
  })
})

describe("Live stream strip", () => {
  it("draws Source → Kafka → Destination as a three-step list, not a second graph canvas", async () => {
    // Three nodes in a fixed line do not need a pan/zoom canvas; React Flow gave
    // them a tall empty box and a minimap above the part of the tab that matters.
    render(<StepsDAGTab pipelineId="p1" />)
    const live = await screen.findByTestId("live-stream-graph")
    expect(within(live).queryByTestId("dag")).toBeNull()
    const steps = within(live).getAllByRole("listitem")
    expect(steps.map((s) => s.getAttribute("data-testid"))).toEqual([
      "live-node-live_source",
      "live-node-live_kafka",
      "live-node-live_destination",
    ])
  })

  it("says each step's state in words, not only in colour", async () => {
    // The reply carries no dependencies, so nothing has checked the source yet.
    render(<StepsDAGTab pipelineId="p1" />)
    const source = await screen.findByTestId("live-node-live_source")
    expect(source).toHaveAttribute("data-status", "waiting")
    expect(source).toHaveTextContent(/Waiting/)
    expect(source).toHaveTextContent(/waiting for the first check/)
  })
})

/**
 * Prod, 2026-09-25: "these stages are taking unusually long: Executing Pipeline"
 * got the chat's generic pipeline examples back. The chip's prompt named no
 * pipeline, so it went to intent classification instead of the per-pipeline
 * diagnosis. The tab had the id all along and never handed it to the insights
 * bar; both places the bar renders must now pass it.
 */
describe("Steps/DAG insight chips name the pipeline", () => {
  const PIPELINE_ID = "5f2c9a1e-8b3d-4e6f-a7c0-1d2e3f4a5b6c"

  it.each([["batch"], ["cdc"]])("%s: the failures chip's prompt carries the pipeline id", async (mode) => {
    strategy = { mode }
    // A failed stage shows the "Investigate failures" chip, and for a stream it
    // also opens Setup by itself, where the bar renders.
    planStages = [
      { id: "intent", display_name: "Understanding Request", status: "complete" },
      { id: "connection_validation", display_name: "Validating Connections", status: "failed" },
    ]
    render(<StepsDAGTab pipelineId={PIPELINE_ID} />)

    fireEvent.click(await screen.findByRole("button", { name: /investigate failures/i }))
    expect(push).toHaveBeenCalledTimes(1)
    const prompt = new URL(push.mock.calls[0][0] as string, "http://x").searchParams.get("prompt") ?? ""
    expect(prompt).toContain(`(pipeline ${PIPELINE_ID})`)
  })
})
