import { describe, expect, it } from "vitest"
import { readFileSync } from "node:fs"
import { resolve } from "node:path"

import {
  HEALER_EVENT_TYPES,
  extractHealerActivity,
  healSucceeded,
  isHealerEvent,
  type PipelineRunEvent,
} from "@/lib/pipeline/eventNormalizer"

import {
  ACTION_TOKENS,
  CATEGORY_TOKENS,
} from "@/components/pipeline/SelfHealingPanel"

// ---------------------------------------------------------------------------
// Cross-language parity.
//
// The healer's vocabulary is defined in Go and consumed in TypeScript, which is
// exactly the shape this codebase keeps getting bitten by: two pieces of code
// answer the same question and disagree. These tests read the Go source so a new
// event type, action or category cannot be added on the backend without the
// panel that renders it failing loudly here.
// ---------------------------------------------------------------------------

// vitest runs with `root` = the frontend package (vitest.config.ts), so the
// repo is one level up. import.meta.url is not usable here — under the jsdom
// environment it resolves to an http: URL.
const repoRoot = resolve(process.cwd(), "..")

function goSource(rel: string): string {
  return readFileSync(`${repoRoot}/backend-orchestrator/${rel}`, "utf8")
}

/**
 * Every `healer_*` string the orchestrator uses as an event_type: SQL literals
 * (`'healer_decision'`) and writeHealEvent arguments (`"healer_backoff_retry"`).
 *
 * The negative lookahead excludes `"healer_action":` — that one is a payload KEY
 * on the generic action events (executors.go), not a type. Filtering it by name
 * would go stale; filtering it by the colon that makes it a map key does not.
 */
function goHealerEventTypes(): string[] {
  const files = [
    "internal/agents/heal/worker.go",
    "internal/agents/heal/auto_executors.go",
    "internal/agents/heal/executors.go",
    "internal/agents/heal/verifier.go",
  ]
  const found = new Set<string>()
  for (const f of files) {
    let src = ""
    try {
      src = goSource(f)
    } catch {
      continue
    }
    for (const m of src.matchAll(/'(healer_[a-z0-9_]+)'/g)) found.add(m[1])
    for (const m of src.matchAll(/"(healer_[a-z0-9_]+)"(?!\s*:)/g)) found.add(m[1])
  }
  return [...found].sort()
}

function goTokens(src: string, goType: "Action" | "Category"): string[] {
  const re = new RegExp(
    `\\b\\w+\\s+(?:diagnose\\.)?${goType}\\s*=\\s*"([a-z0-9_]+)"`,
    "g"
  )
  return [...src.matchAll(re)].map((m) => m[1]).sort()
}

describe("healer vocabulary parity with the Go orchestrator", () => {
  it("HEALER_EVENT_TYPES covers every healer_* event the orchestrator writes", () => {
    const fromGo = goHealerEventTypes()
    expect(fromGo.length).toBeGreaterThan(0)

    const missing = fromGo.filter(
      (t) => !(HEALER_EVENT_TYPES as readonly string[]).includes(t)
    )
    expect(
      missing,
      `Go writes healer event types the panel never asks for. The ?event_types= ` +
        `filter is an allowlist, so these are invisible in the UI: ${missing.join(", ")}`
    ).toEqual([])
  })

  it("every event type in the allowlist is one Go actually writes", () => {
    const fromGo = new Set(goHealerEventTypes())
    const stale = (HEALER_EVENT_TYPES as readonly string[]).filter(
      (t) => !fromGo.has(t)
    )
    expect(
      stale,
      `the panel filters on event types nothing emits: ${stale.join(", ")}`
    ).toEqual([])
  })

  it("ACTION_TOKENS covers every diagnose.Action the healer can suggest", () => {
    const fromGo = [
      ...goTokens(goSource("pkg/diagnose/diagnose.go"), "Action"),
      ...goTokens(goSource("internal/agents/heal/auto_executors.go"), "Action"),
    ]
    expect(fromGo.length).toBeGreaterThan(5)

    const missing = fromGo.filter((t) => !ACTION_TOKENS.includes(t))
    expect(
      missing,
      `actions with no label in SelfHealingPanel: ${missing.join(", ")}`
    ).toEqual([])
  })

  it("CATEGORY_TOKENS covers every diagnose.Category", () => {
    const fromGo = goTokens(goSource("pkg/diagnose/diagnose.go"), "Category")
    expect(fromGo.length).toBeGreaterThan(5)

    const missing = fromGo.filter((t) => !CATEGORY_TOKENS.includes(t))
    expect(
      missing,
      `categories with no label in SelfHealingPanel: ${missing.join(", ")}`
    ).toEqual([])
  })
})

// ---------------------------------------------------------------------------
// isHealerEvent
// ---------------------------------------------------------------------------

describe("isHealerEvent", () => {
  it("matches on the prefix, not a fixed list, so a new Go event still surfaces", () => {
    expect(isHealerEvent("healer_something_shipped_next_quarter")).toBe(true)
  })

  it("does not claim ordinary pipeline events", () => {
    for (const t of ["stage_started", "row_batch", "execution_failed", "self_heal"]) {
      expect(isHealerEvent(t)).toBe(false)
    }
  })

  it("tolerates the empty and the odd", () => {
    expect(isHealerEvent("")).toBe(false)
    expect(isHealerEvent(undefined as unknown as string)).toBe(false)
    expect(isHealerEvent("HEALER_DECISION")).toBe(true)
  })
})

// ---------------------------------------------------------------------------
// extractHealerActivity
// ---------------------------------------------------------------------------

function evt(over: Partial<PipelineRunEvent>): PipelineRunEvent {
  return {
    pipeline_id: "p1",
    event_id: "e1",
    event_type: "healer_decision",
    received_at: "2026-08-01T10:00:00Z",
    payload: {},
    ...over,
  }
}

describe("extractHealerActivity", () => {
  it("carries the evidence a decision reacted to, not just its verdict", () => {
    const [a] = extractHealerActivity([
      evt({
        payload: {
          category: "orchestration",
          suggested_action: "backoff_retry",
          confidence: 0.8,
          rationale: "the workflow backing this run is gone",
          outcome: "hitl_requested",
          action_executed: false,
          failure_signature: "sig-abc",
          error_message: "pipeline run is no longer active (workflow not found)",
          executor_status: "workflow_gone",
          memory_note: "tried twice, failed twice",
          attempt_id: 42,
        },
      }),
    ])

    expect(a.kind).toBe("decision")
    expect(a.category).toBe("orchestration")
    expect(a.action).toBe("backoff_retry")
    expect(a.confidence).toBe(0.8)
    expect(a.outcome).toBe("hitl_requested")
    expect(a.actionExecuted).toBe(false)
    expect(a.failureSignature).toBe("sig-abc")
    expect(a.errorMessage).toBe("pipeline run is no longer active (workflow not found)")
    expect(a.executorStatus).toBe("workflow_gone")
    expect(a.memoryNote).toBe("tried twice, failed twice")
    expect(a.attemptId).toBe(42)
  })

  it("prefers the diagnosed failure over the executor's own error", () => {
    // Both keys populated only when an executor threw. `error` describes the
    // healer's action failing; `error_message` describes what it was fixing.
    // Showing the former as "the failure" reframes a healer bug as a pipeline bug.
    const [a] = extractHealerActivity([
      evt({
        payload: {
          error: "retry endpoint returned 503",
          error_message: "connection refused",
        },
      }),
    ])
    expect(a.errorMessage).toBe("connection refused")
  })

  it("falls back to the executor error when there is no diagnosed one", () => {
    // Events written before the payload carried error_message. They must still
    // render something rather than a blank row.
    const [a] = extractHealerActivity([
      evt({ payload: { error: "retry endpoint returned 503" } }),
    ])
    expect(a.errorMessage).toBe("retry endpoint returned 503")
  })

  it("renders a pre-fix decision event rather than dropping it", () => {
    const [a] = extractHealerActivity([
      evt({ payload: { category: "unknown", suggested_action: "escalate_to_human" } }),
    ])
    expect(a.kind).toBe("decision")
    expect(a.action).toBe("escalate_to_human")
    expect(a.errorMessage).toBeUndefined()
    expect(a.attemptId).toBeUndefined()
  })

  it("reads a verdict event as a verdict, with the id that joins it to its decision", () => {
    const [a] = extractHealerActivity([
      evt({
        event_type: "healer_verified",
        payload: {
          verdict: "healed",
          attempt_id: 42,
          attempt_no: 2,
          action: "backoff_retry",
          successor_execution_id: "exec-9",
        },
      }),
    ])
    expect(a.kind).toBe("verdict")
    expect(a.verdict).toBe("healed")
    expect(a.attemptId).toBe(42)
    expect(a.attemptNo).toBe(2)
    expect(a.successorExecutionId).toBe("exec-9")
  })

  it("names an action event from its type when the payload does not", () => {
    const [a] = extractHealerActivity([
      evt({ event_type: "healer_cleanup_cdc_resources", payload: {} }),
    ])
    expect(a.kind).toBe("action")
    expect(a.action).toBe("cleanup_cdc_resources")
  })

  it("ignores non-healer events entirely", () => {
    expect(
      extractHealerActivity([
        evt({ event_type: "stage_started" }),
        evt({ event_type: "execution_failed" }),
      ])
    ).toEqual([])
  })

  it("survives a null payload", () => {
    // Healer rows are written with seq NULL and no stage_id; a defensive
    // normaliser here is cheaper than a crashed Overview tab.
    const rows = extractHealerActivity([
      evt({ payload: null as unknown as Record<string, unknown> }),
    ])
    expect(rows).toHaveLength(1)
    expect(rows[0].kind).toBe("decision")
  })

  it("orders newest first and prefers occurred_at over received_at", () => {
    const rows = extractHealerActivity([
      evt({ event_id: "old", received_at: "2026-08-01T10:00:00Z" }),
      evt({
        event_id: "new",
        received_at: "2026-08-01T09:00:00Z",
        occurred_at: "2026-08-01T12:00:00Z",
      }),
    ])
    expect(rows.map((r) => r.id)).toEqual(["new", "old"])
  })
})

// ---------------------------------------------------------------------------
// healSucceeded — the claim the panel makes to the operator
// ---------------------------------------------------------------------------

describe("healSucceeded", () => {
  const decision = (over: Record<string, unknown>) =>
    extractHealerActivity([evt({ payload: over })])[0]

  it("counts an executed auto-action", () => {
    expect(
      healSucceeded(decision({ outcome: "auto_executed", action_executed: true }))
    ).toBe(true)
  })

  it("does not count an auto-execute that never ran", () => {
    expect(
      healSucceeded(decision({ outcome: "auto_executed", action_executed: false }))
    ).toBe(false)
  })

  it("does not count asking a human as healing", () => {
    // The whole point of the medium confidence band: BackoffRetryExecutor is not
    // HITL-safe, so nothing was executed. Counting it as a heal is how a
    // self-healing UI overstates itself.
    expect(healSucceeded(decision({ outcome: "hitl_requested" }))).toBe(false)
    expect(healSucceeded(decision({ outcome: "escalated" }))).toBe(false)
    expect(healSucceeded(decision({ outcome: "action_failed" }))).toBe(false)
    expect(healSucceeded(decision({ outcome: "no_action_defined" }))).toBe(false)
  })

  it("counts only a 'healed' verdict, never an inconclusive one", () => {
    const verdict = (v: string) =>
      extractHealerActivity([
        evt({ event_type: "healer_verified", payload: { verdict: v } }),
      ])[0]

    expect(healSucceeded(verdict("healed"))).toBe(true)
    for (const v of ["failed_again", "inconclusive", "superseded"]) {
      expect(healSucceeded(verdict(v)), `verdict ${v} must not read as healed`).toBe(
        false
      )
    }
  })

  it("reads _failed and _skipped action markers as non-successes", () => {
    const action = (t: string) =>
      extractHealerActivity([evt({ event_type: t, payload: {} })])[0]

    expect(healSucceeded(action("healer_cleanup_cdc_resources"))).toBe(true)
    expect(healSucceeded(action("healer_cleanup_cdc_failed"))).toBe(false)
    expect(healSucceeded(action("healer_cleanup_cdc_skipped"))).toBe(false)
    expect(healSucceeded(action("healer_repair_ownership_failed"))).toBe(false)
    expect(healSucceeded(action("healer_repair_ownership_skipped"))).toBe(false)
  })
})
