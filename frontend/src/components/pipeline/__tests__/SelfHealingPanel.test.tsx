import { describe, expect, it, vi, beforeEach, afterEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import { SelfHealingPanel } from "@/components/pipeline/SelfHealingPanel"

// authFetch is the only thing this component touches outside React.
const authFetch = vi.hoisted(() => vi.fn())
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch }))

/**
 * The payloads below are the exact shapes the orchestrator writes — see
 * `writeDecisionEvent` / `writeVerdictEvent` in
 * backend-orchestrator/internal/agents/heal/worker.go, and the assertions in
 * worker_decision_event_test.go which pin those payloads from the other side.
 */
const DECISION = {
  pipeline_id: "p1",
  execution_id: "exec-1",
  event_id: "d1",
  event_type: "healer_decision",
  received_at: "2026-08-01T10:00:00Z",
  severity: "warn",
  payload: {
    category: "orchestration",
    suggested_action: "backoff_retry",
    confidence: 0.8,
    rationale: "the workflow backing this run is gone",
    outcome: "hitl_requested",
    action_executed: false,
    hitl_prompt: "Start a fresh run for this pipeline?",
    failure_signature: "orchestration|workflow_gone|postgresql->postgresql|…",
    error_message: "pipeline run is no longer active (workflow not found)",
    executor_status: "workflow_gone",
    memory_note: "",
    attempt_id: 42,
  },
}

const VERDICT = {
  pipeline_id: "p1",
  execution_id: "exec-1",
  event_id: "v1",
  event_type: "healer_verified",
  received_at: "2026-08-01T10:30:00Z",
  severity: "info",
  payload: {
    attempt_id: 42,
    attempt_no: 1,
    verdict: "healed",
    action: "backoff_retry",
    failure_signature: "orchestration|workflow_gone|postgresql->postgresql|…",
    successor_execution_id: "exec-2",
  },
}

/** A decision written before the payload carried its evidence. */
const LEGACY_DECISION = {
  pipeline_id: "p1",
  event_id: "d0",
  event_type: "healer_decision",
  received_at: "2026-07-16T18:52:00Z",
  payload: {
    category: "unknown",
    suggested_action: "escalate_to_human",
    confidence: 0.3,
    rationale: "no rule matched",
    outcome: "escalated",
    action_executed: false,
    error: "",
  },
}

function respond(events: unknown[], status = 200) {
  authFetch.mockResolvedValue({
    ok: status === 200,
    status,
    json: async () => ({ events }),
  })
}

beforeEach(() => {
  authFetch.mockReset()
})
afterEach(() => {
  vi.useRealTimers()
})

describe("SelfHealingPanel", () => {
  it("renders the healer's reasoning and the evidence behind it", async () => {
    respond([DECISION, VERDICT])
    render(<SelfHealingPanel pipelineId="p1" />)

    expect(await screen.findByText("Self-healing activity")).toBeInTheDocument()

    // Verdict first — newest first.
    expect(screen.getByText("Retry the run — Healed")).toBeInTheDocument()
    // Decision reads category → action, not raw tokens.
    expect(screen.getByText("Orchestration → Retry the run")).toBeInTheDocument()
    expect(
      screen.getByText("the workflow backing this run is gone")
    ).toBeInTheDocument()

    // 0.8 sits in the medium band: the healer recommends, it does not act.
    // The band is what makes 80% legible as "deliberately under the auto bar".
    expect(screen.getByText("80% · asks first")).toBeInTheDocument()
    expect(
      screen.getByText("Start a fresh run for this pipeline?")
    ).toBeInTheDocument()
  })

  it("shows the failure it diagnosed once the evidence is expanded", async () => {
    respond([DECISION])
    render(<SelfHealingPanel pipelineId="p1" />)

    const toggle = await screen.findByRole("button", { name: /show evidence/i })
    // Collapsed by default — the panel leads with the verdict, not a wall of text.
    expect(
      screen.queryByText("pipeline run is no longer active (workflow not found)")
    ).not.toBeInTheDocument()

    await userEvent.click(toggle)

    expect(screen.getByText("Error it diagnosed")).toBeInTheDocument()
    expect(
      screen.getByText("pipeline run is no longer active (workflow not found)")
    ).toBeInTheDocument()
    expect(screen.getByText("Failure signature")).toBeInTheDocument()
    expect(screen.getByText("workflow_gone")).toBeInTheDocument()
    // The id that joins this decision to its verdict.
    expect(screen.getByText(/attempt #42/)).toBeInTheDocument()
  })

  it("counts healed runs and the ones still waiting on a person", async () => {
    respond([DECISION, VERDICT])
    render(<SelfHealingPanel pipelineId="p1" />)

    expect(await screen.findByText("1 healed")).toBeInTheDocument()
    expect(screen.getByText("1 needs you")).toBeInTheDocument()
  })

  it("does not count a HITL request or an escalation as a heal", async () => {
    // The failure mode this guards: a self-healing panel that reports work it
    // never did. Two decisions, nothing executed, no verdict — zero heals.
    respond([DECISION, LEGACY_DECISION])
    render(<SelfHealingPanel pipelineId="p1" />)

    await screen.findByText("Self-healing activity")
    expect(screen.queryByText(/healed/)).not.toBeInTheDocument()
    expect(screen.getByText("2 need you")).toBeInTheDocument()
  })

  it("renders a pre-fix decision event rather than a blank row", async () => {
    respond([LEGACY_DECISION])
    render(<SelfHealingPanel pipelineId="p1" />)

    expect(
      await screen.findByText("Unclassified → Escalate to a human")
    ).toBeInTheDocument()
    expect(screen.getByText("no rule matched")).toBeInTheDocument()
    expect(screen.getByText("30% · escalates")).toBeInTheDocument()
    expect(screen.getByText("Escalated")).toBeInTheDocument()
  })

  it("asks the API for healer events specifically, not a general page", async () => {
    // Healer rows carry seq NULL and sort last, so an unfiltered limit=200 page
    // on a busy pipeline can contain none of them.
    respond([DECISION])
    render(<SelfHealingPanel pipelineId="p1" />)
    await screen.findByText("Self-healing activity")

    const url = String(authFetch.mock.calls[0][0])
    expect(url).toContain("/events")
    expect(url).toContain("event_types=")
    expect(url).toContain("healer_decision")
    expect(url).toContain("healer_verified")
  })

  it("renders nothing for a pipeline the healer never looked at", async () => {
    respond([])
    const { container } = render(<SelfHealingPanel pipelineId="p1" />)
    await waitFor(() => expect(authFetch).toHaveBeenCalled())
    await waitFor(() => expect(container).toBeEmptyDOMElement())
  })

  it("stays silent for a viewer who cannot read this pipeline's events", async () => {
    // 403 is not actionable by the person seeing it.
    respond([], 403)
    const { container } = render(<SelfHealingPanel pipelineId="p1" />)
    await waitFor(() => expect(authFetch).toHaveBeenCalled())
    await waitFor(() => expect(container).toBeEmptyDOMElement())
  })

  it("says so when the request fails for a reason the operator can act on", async () => {
    respond([], 500)
    render(<SelfHealingPanel pipelineId="p1" />)
    expect(
      await screen.findByText(/Could not load self-healing activity \(500\)/)
    ).toBeInTheDocument()
  })

  it("collapses a long history behind a single toggle", async () => {
    const many = Array.from({ length: 8 }, (_, i) => ({
      ...DECISION,
      event_id: `d${i}`,
      received_at: `2026-08-01T1${i}:00:00Z`,
    }))
    respond(many)
    render(<SelfHealingPanel pipelineId="p1" />)

    const showAll = await screen.findByRole("button", { name: /show all 8/i })
    expect(screen.getAllByText("Orchestration → Retry the run")).toHaveLength(5)

    await userEvent.click(showAll)
    expect(screen.getAllByText("Orchestration → Retry the run")).toHaveLength(8)
  })
})
