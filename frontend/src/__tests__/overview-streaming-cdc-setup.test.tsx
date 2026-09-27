/**
 * A streaming CDC pipeline's Overview, from the events prod pipeline c228373b
 * actually has (execution 53037fdb, 2026-09-25, read off the domain-events topic).
 *
 * After #1212 the list was meant to fold to one line once setup finished; on
 * this pipeline it did not, and Infra Preflight still said "Retry 2/2":
 *
 *   - the orchestrator's CDC counter sends TABLE_STATS as stage `cdc_stats` with
 *     the run's execution id for as long as the pipeline streams
 *     (cdcstats/events.go). Each made a "Cdc Stats" row that never started or
 *     finished, so the fold (every row completed) never happened, and the
 *     header read "Step 8/9";
 *   - `infra_preflight` ran at 14:58:29, the run stopped for a table-selection
 *     question, and it ran again at 14:59:03 once that was answered. Both runs
 *     completed. Two starts are two runs, not a retry.
 *
 * The lifecycle rows below are prod's, both producers' copies included: the
 * orchestrator's whole-second rows carry the message, the adapter's precise ones
 * do not. The TABLE_STATS rows follow cdcstats/events.go and the sink's shape;
 * their times are illustrative.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, cleanup } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: null, loading: false, error: null }),
}))

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
  authFetchOrThrow: (...args: unknown[]) => authFetch(...args),
}))

import {
  PipelineLiveStatePanel,
  buildAgenticStagesFromEvents,
  finishedSetupSummary,
  stepInfoFromEvents,
} from "@/components/pipeline/PipelineLiveStatePanel"
import type { PipelineRunEvent } from "@/lib/pipeline/eventNormalizer"
import { stageTiming } from "@/lib/duration"

const PIPELINE = "c228373b-a698-422d-8864-4b60592a118f"
const EXEC = "53037fdb-run"

let seq = 0
function ev(
  event_type: string,
  stage_id: string | undefined,
  occurred_at: string,
  payload: Record<string, unknown> = {},
  over: Partial<PipelineRunEvent> = {}
): PipelineRunEvent {
  seq += 1
  return {
    pipeline_id: PIPELINE,
    execution_id: EXEC,
    event_id: /\.\d+Z$/.test(occurred_at) ? `sha256:${seq}` : `evt-c228373b-${seq}`,
    seq,
    event_type,
    stage_id,
    occurred_at,
    received_at: occurred_at,
    payload,
    ...over,
  }
}

const msg = (message: string) => ({ message })

const LIFECYCLE: PipelineRunEvent[] = [
  ev("PIPELINE_CREATED", undefined, "2026-09-25T14:58:24Z"),
  ev("STAGE_STARTED", "intent", "2026-09-25T14:58:24.518690Z"),
  ev("STAGE_STARTED", "intent", "2026-09-25T14:58:24Z", msg("Understanding your data movement request")),
  ev("STAGE_COMPLETED", "intent", "2026-09-25T14:58:24Z", msg("Understood: sync postgresql to mongodb")),
  ev("STAGE_COMPLETED", "intent", "2026-09-25T14:58:24.996224Z"),
  ev("STAGE_STARTED", "capability_resolver", "2026-09-25T14:58:25.368349Z"),
  ev("STAGE_STARTED", "capability_resolver", "2026-09-25T14:58:25Z", msg("Checking connector availability")),
  ev("STAGE_COMPLETED", "capability_resolver", "2026-09-25T14:58:25Z", msg("Connections resolved from pipeline")),
  ev("STAGE_COMPLETED", "capability_resolver", "2026-09-25T14:58:25.857933Z"),
  ev("STAGE_STARTED", "connector_check", "2026-09-25T14:58:26.055083Z"),
  ev("STAGE_COMPLETED", "connector_check", "2026-09-25T14:58:26.380889Z"),
  ev("STAGE_STARTED", "connection_validation", "2026-09-25T14:58:26.592041Z"),
  ev("STAGE_COMPLETED", "connection_validation", "2026-09-25T14:58:26.916159Z"),
  ev("STAGE_STARTED", "planner", "2026-09-25T14:58:27.126083Z"),
  ev("STAGE_STARTED", "planner", "2026-09-25T14:58:27Z", msg("Creating an optimal execution plan for your data transfer")),
  ev("STAGE_COMPLETED", "planner", "2026-09-25T14:58:27Z", msg("Execution plan created successfully")),
  ev("STAGE_COMPLETED", "planner", "2026-09-25T14:58:27.717400Z"),
  ev("STAGE_STARTED", "validator", "2026-09-25T14:58:28.200445Z"),
  ev("STAGE_STARTED", "validator", "2026-09-25T14:58:28Z", msg("Validating your pipeline plan")),
  ev("STAGE_COMPLETED", "validator", "2026-09-25T14:58:28Z", msg("Plan check complete")),
  ev("STAGE_COMPLETED", "validator", "2026-09-25T14:58:28.471041Z"),
  ev("STAGE_STARTED", "executor", "2026-09-25T14:58:28.706695Z"),
  // First preflight, before the table-selection question.
  ev("STAGE_STARTED", "infra_preflight", "2026-09-25T14:58:29Z", msg("Checking infrastructure")),
  ev("STAGE_PROGRESS", "infra_preflight", "2026-09-25T14:58:29Z", msg("Checking kafka-mcp-sink…")),
  ev("STAGE_PROGRESS", "infra_preflight", "2026-09-25T14:58:29Z", msg("kafka-mcp-sink ready")),
  ev("STAGE_COMPLETED", "infra_preflight", "2026-09-25T14:58:29Z", msg("All required services are running")),
  ev("STAGE_STARTED", "executor", "2026-09-25T14:58:29Z", msg("Preparing pipeline")),
  ev("PIPELINE_WAITING", "executor", "2026-09-25T14:58:29Z", msg("Select which table(s)/resource(s) to sync (9 available).")),
  ev("PIPELINE_WAITING", "executor", "2026-09-25T14:58:29.328656Z", msg("Select table(s) to sync (9 available)")),
  // Second preflight, once the question was answered. It never failed.
  ev("STAGE_STARTED", "infra_preflight", "2026-09-25T14:59:03Z", msg("Checking infrastructure")),
  ev("STAGE_PROGRESS", "infra_preflight", "2026-09-25T14:59:03Z", msg("Checking kafka-mcp-sink…")),
  ev("STAGE_PROGRESS", "infra_preflight", "2026-09-25T14:59:03Z", msg("kafka-mcp-sink ready")),
  ev("STAGE_COMPLETED", "infra_preflight", "2026-09-25T14:59:03Z", msg("All required services are running")),
  ev("STAGE_STARTED", "executor", "2026-09-25T14:59:03Z", msg("Preparing pipeline")),
  ev("STAGE_PROGRESS", "executor", "2026-09-25T14:59:10Z", msg("Executor working…")),
  ev("STAGE_COMPLETED", "executor", "2026-09-25T14:59:17Z", msg("Streaming pipeline started")),
  ev("STAGE_COMPLETED", "executor", "2026-09-25T14:59:17.719781Z"),
]

const cdcStats = (at: string, table: string) =>
  ev("TABLE_STATS", "cdc_stats", at, {
    message: `CDC table stats update: ${table}`,
    metadata: { source: "cdc_stats_consumer", mode: "cdc", status: "running" },
  }, { stage_group: "executing" })

// The sink stamps its TABLE_STATS with the pipeline id as the execution id.
const sinkStats = (at: string) =>
  ev("TABLE_STATS", "executor", at, { metadata: { source: "kafka_mcp_sink" } }, {
    execution_id: PIPELINE,
    stage_group: "executing",
  })

const STREAMING: PipelineRunEvent[] = [
  ...LIFECYCLE,
  cdcStats("2026-09-25T15:00:05Z", "public.orders"),
  sinkStats("2026-09-25T15:00:06Z"),
  cdcStats("2026-09-25T15:10:05Z", "public.customers"),
  sinkStats("2026-09-25T15:27:49Z"),
]

const SETUP = [
  "intent",
  "capability_resolver",
  "connector_check",
  "connection_validation",
  "planner",
  "validator",
  "infra_preflight",
  "executor",
]

const STATE = {
  schema_version: 1,
  pipeline_id: PIPELINE,
  execution_id: EXEC,
  status: "running",
  message: "",
  created_at: "2026-09-25T14:58:24Z",
  updated_at: "2026-09-25T15:27:49Z",
}

const stagesOf = (events: PipelineRunEvent[]) => buildAgenticStagesFromEvents(events, STATE as never)

describe("a streaming CDC pipeline's Overview once setup is over", () => {
  it("the fixture carries what broke it — the probe is armed", () => {
    // A stats row stamped with this run's execution id and naming a stage.
    expect(STREAMING.some((e) => e.event_type === "TABLE_STATS" && e.execution_id === EXEC && e.stage_id === "cdc_stats")).toBe(true)
    // Preflight really started twice, so a count of starts says 2.
    const preflight = STREAMING.filter((e) => e.stage_id === "infra_preflight").map((e) => ({
      type: e.event_type,
      at: new Date(e.occurred_at!).getTime(),
    }))
    expect(stageTiming(preflight).attempts).toBe(2)
  })

  it("lists the setup stages and no stage for the stats counter", () => {
    const stages = stagesOf(STREAMING)
    expect(stages.map((s) => s.stage)).toEqual(SETUP)
    expect(stages.every((s) => s.status === "completed")).toBe(true)
  })

  it("folds the finished setup into one line", () => {
    expect(finishedSetupSummary(stagesOf(STREAMING), "running")?.steps).toBe(8)
  })

  it("counts eight steps in the header, not nine", () => {
    expect(stepInfoFromEvents(STREAMING, STATE as never)?.total_steps).toBe(8)
  })

  it("does not call a stage that ran twice without failing retried", () => {
    const preflight = stagesOf(STREAMING).find((s) => s.stage === "infra_preflight")!
    expect(preflight.currentAttempt).toBe(1)
    expect(preflight.maxAttempts).toBe(1)
  })

  it("control: a run that failed and started again is still a retry", () => {
    const retried = [
      ...LIFECYCLE.filter((e) => e.stage_id !== "infra_preflight"),
      ev("STAGE_STARTED", "infra_preflight", "2026-09-25T14:58:29Z"),
      ev("STAGE_FAILED", "infra_preflight", "2026-09-25T14:58:31Z", { error: "kafka-connect not ready" }),
      ev("STAGE_STARTED", "infra_preflight", "2026-09-25T14:59:03Z"),
      ev("STAGE_COMPLETED", "infra_preflight", "2026-09-25T14:59:04Z"),
    ]
    const preflight = stagesOf(retried).find((s) => s.stage === "infra_preflight")!
    expect(preflight.currentAttempt).toBe(2)
    expect(preflight.maxAttempts).toBe(2)
  })
})

function jsonOk(body: unknown) {
  return { ok: true, status: 200, json: async () => body, text: async () => JSON.stringify(body) }
}

describe("the rendered Overview for that pipeline", () => {
  const eventUrls: string[] = []

  beforeEach(() => {
    authFetch.mockReset()
    eventUrls.length = 0
    authFetch.mockImplementation(async (url: string) => {
      const u = String(url)
      if (u.includes("/state")) return jsonOk(STATE)
      if (u.includes("/events")) {
        eventUrls.push(u)
        // Filter like the gateway does (`event_types` is an allowlist), newest first.
        const types = new URL(u, "http://gateway").searchParams.get("event_types")
        const allowed = types ? new Set(types.split(",")) : null
        return jsonOk({ events: STREAMING.filter((e) => !allowed || allowed.has(e.event_type)).reverse() })
      }
      return jsonOk({})
    })
  })
  afterEach(() => cleanup())

  it("shows the one-line setup summary instead of the stage list", async () => {
    render(<PipelineLiveStatePanel pipelineId={PIPELINE} />)

    expect(await screen.findByTestId("setup-summary")).toHaveTextContent("Setup completed: 8 steps")
    expect(screen.queryByText("Agentic pipeline stages")).not.toBeInTheDocument()
    expect(screen.queryByText(/Cdc Stats/)).not.toBeInTheDocument()
  })

  it("asks for stage transitions only, heartbeats and the table-selection prompt included", async () => {
    render(<PipelineLiveStatePanel pipelineId={PIPELINE} />)
    await screen.findByTestId("setup-summary")

    expect(eventUrls.length).toBeGreaterThan(0)
    for (const u of eventUrls) {
      const types = (new URL(u, "http://gateway").searchParams.get("event_types") || "").split(",")
      expect(types).toEqual(expect.arrayContaining(["STAGE_STARTED", "STAGE_PROGRESS", "STAGE_COMPLETED", "PIPELINE_WAITING"]))
      expect(types).not.toContain("TABLE_STATS")
    }
  })
})
