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

/**
 * One body per endpoint: the card reads healer events and, for a CDC pipeline,
 * the open Sentinel alerts. `alerts` is the alerts list, or a status code to fail
 * that one request with.
 */
function respond(events: unknown[], status = 200, alerts: unknown[] | number = []) {
  authFetch.mockImplementation(async (url: string) => {
    if (String(url).includes("/alerts")) {
      if (typeof alerts === "number") return { ok: false, status: alerts, json: async () => ({}) }
      return { ok: true, status: 200, json: async () => ({ alerts }) }
    }
    return { ok: status === 200, status, json: async () => ({ events }) }
  })
}

/** A decision on attempt `id`, and the verdict the verifier later wrote for it. */
function escalation(id: number, at: string, outcome = "escalated") {
  return {
    ...DECISION,
    event_id: `d-${id}`,
    received_at: at,
    payload: { ...DECISION.payload, outcome, attempt_id: id },
  }
}
function verdict(id: number, v: string, at: string) {
  return { ...VERDICT, event_id: `v-${id}`, received_at: at, payload: { ...VERDICT.payload, attempt_id: id, verdict: v } }
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

    expect(await screen.findByText("Self-healing")).toBeInTheDocument()

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

    const errorLabel = screen.getByText("Error it diagnosed")
    expect(
      screen.getByText("pipeline run is no longer active (workflow not found)")
    ).toBeInTheDocument()
    // The signature is a grouping key (ids and numbers replaced, error part cut
    // at 160 characters), so it comes after the error and says what it is.
    const signatureLabel = screen.getByText(/^Grouped as/)
    expect(signatureLabel).toHaveTextContent("ids and numbers replaced, error cut at 160 characters")
    expect(errorLabel.compareDocumentPosition(signatureLabel) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
    expect(screen.getByText("workflow_gone")).toBeInTheDocument()
    // The id that joins this decision to its verdict.
    expect(screen.getByText(/attempt #42/)).toBeInTheDocument()
  })

  it("counts a healed run, and no longer counts the request it answered as needing you", async () => {
    // Attempt 42 asked for approval, then the verifier graded it healed.
    respond([DECISION, VERDICT])
    render(<SelfHealingPanel pipelineId="p1" />)

    expect(await screen.findByText("1 healed")).toBeInTheDocument()
    expect(screen.queryByText(/needs? you/)).not.toBeInTheDocument()
  })

  it("counts only the escalations still open, not every one in the history", async () => {
    // Prod shape: two old escalations the pipeline has since recovered from, one
    // superseded by a newer failure, and one still waiting. It used to read "4 need you".
    respond([
      escalation(4, "2026-08-04T10:00:00Z", "hitl_requested"),
      verdict(3, "superseded", "2026-08-03T11:00:00Z"),
      escalation(3, "2026-08-03T10:00:00Z"),
      verdict(2, "self_resolved", "2026-08-02T11:00:00Z"),
      escalation(2, "2026-08-02T10:00:00Z"),
      verdict(1, "failed_again", "2026-08-01T11:00:00Z"),
      escalation(1, "2026-08-01T10:00:00Z"),
    ])
    render(<SelfHealingPanel pipelineId="p1" />)

    expect(await screen.findByText("1 needs you")).toBeInTheDocument()
    // Recovered without an action: not a heal the agent can take credit for.
    expect(screen.queryByText(/\d healed/)).not.toBeInTheDocument()
    expect(screen.getByText("Retry the run — Recovered on its own")).toBeInTheDocument()
  })

  it("keeps an escalation open when its verdict is inconclusive or not written yet", async () => {
    respond([
      verdict(2, "inconclusive", "2026-08-02T11:00:00Z"),
      escalation(2, "2026-08-02T10:00:00Z"),
      escalation(1, "2026-08-01T10:00:00Z"),
    ])
    render(<SelfHealingPanel pipelineId="p1" />)

    expect(await screen.findByText("2 need you")).toBeInTheDocument()
  })

  it("does not count a HITL request or an escalation as a heal", async () => {
    // The failure mode this guards: a self-healing panel that reports work it
    // never did. Two decisions, nothing executed, no verdict — zero heals.
    respond([DECISION, LEGACY_DECISION])
    render(<SelfHealingPanel pipelineId="p1" />)

    await screen.findByText("Self-healing")
    expect(screen.queryByText(/healed/)).not.toBeInTheDocument()
    // Attempt 42 has no verdict: open. The legacy row cannot be joined to a
    // verdict and the healer has spoken since, so it is not counted.
    expect(screen.getByText("1 needs you")).toBeInTheDocument()
  })

  it("counts a legacy escalation only while it is the healer's latest word", async () => {
    respond([LEGACY_DECISION])
    render(<SelfHealingPanel pipelineId="p1" />)

    expect(await screen.findByText("1 needs you")).toBeInTheDocument()
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
    await screen.findByText("Self-healing")

    const url = String(authFetch.mock.calls[0][0])
    expect(url).toContain("/events")
    expect(url).toContain("event_types=")
    expect(url).toContain("healer_decision")
    expect(url).toContain("healer_verified")
  })

  it("shows on a pipeline the healer never had to touch, saying what watches it", async () => {
    // It used to render nothing here, so a healthy pipeline gave no sign that
    // anything was watching it at all.
    respond([])
    render(<SelfHealingPanel pipelineId="p1" pipelineType="etl" />)

    expect(await screen.findByTestId("self-healing-card")).toBeInTheDocument()
    expect(screen.getByText("The heal agent has not had to act on this pipeline.")).toBeInTheDocument()
    expect(screen.getByText(/The heal agent reads every failed run/)).toBeInTheDocument()
    // Batch: no Sentinel, and no alerts request.
    expect(screen.queryByText(/Sentinel/)).not.toBeInTheDocument()
    expect(authFetch.mock.calls.some(([u]) => String(u).includes("/alerts"))).toBe(false)
  })

  it("on a CDC pipeline names the Sentinel and links its open alerts to Data flow", async () => {
    respond([], 200, [{ id: "a1" }, { id: "a2" }])
    render(<SelfHealingPanel pipelineId="p1" pipelineType="cdc" />)

    expect(await screen.findByText(/2 open Sentinel alerts/)).toBeInTheDocument()
    expect(screen.getByText(/The Sentinel watches the running stream/)).toBeInTheDocument()
    expect(screen.getByRole("link", { name: "See them in Data flow" })).toHaveAttribute(
      "href",
      "/pipelines/p1?tab=monitor"
    )
    const alertsUrl = String(authFetch.mock.calls.find(([u]) => String(u).includes("/alerts"))?.[0])
    expect(alertsUrl).toContain("resolved=false")
  })

  it("says there are no open alerts only when it could read them", async () => {
    respond([], 200, [])
    const { unmount } = render(<SelfHealingPanel pipelineId="p1" pipelineType="cdc" />)
    expect(await screen.findByText("No open Sentinel alerts.")).toBeInTheDocument()
    unmount()

    // A failed alerts read is not "no alerts": the line is left out.
    respond([], 200, 500)
    render(<SelfHealingPanel pipelineId="p1" pipelineType="cdc" />)
    await screen.findByTestId("self-healing-card")
    expect(screen.queryByText(/open Sentinel alert/)).not.toBeInTheDocument()
    expect(screen.getByText(/The Sentinel watches the running stream/)).toBeInTheDocument()
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
