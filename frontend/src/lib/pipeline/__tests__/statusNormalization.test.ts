import { describe, expect, it } from "vitest"
import {
  reconcilePipelineStatus,
  type NormalizedPipelineStatus,
} from "@/lib/pipeline/statusNormalization"

// The finding this guards (CDC-D2 / A4): the Overview tab and the Table statistics
// tab showed "idle" and "running" for the SAME execution on the SAME page load,
// because four components each carried their own copy of this escalation and the
// monitoring panel carried none. These tests pin the one definition they now share
// — in particular that escalation happens ONLY from "running", so a copy that
// forgets that check (or that compares the raw status word instead of the
// normalized one) goes red here rather than on someone's screen.

describe("reconcilePipelineStatus", () => {
  it("escalates a frozen 'running' to what /runtime observed", () => {
    expect(reconcilePipelineStatus("running", "failed")).toBe("failed")
    expect(reconcilePipelineStatus("running", "idle")).toBe("idle")
  })

  it("leaves 'running' alone for every other runtime phase", () => {
    for (const phase of ["syncing", "completed", "paused", "initializing", "validating", "running", ""]) {
      expect(reconcilePipelineStatus("running", phase)).toBe("running")
    }
  })

  it("passes /state through untouched when /runtime is unavailable", () => {
    // Still loading, or a 404 from an older backend — no verdict, no escalation.
    expect(reconcilePipelineStatus("running", undefined)).toBe("running")
    expect(reconcilePipelineStatus("running", null)).toBe("running")
  })

  it("never overrides a /state value more specific than 'running'", () => {
    // A paused pipeline whose feed is stale is still PAUSED; a completed run whose
    // dependencies went down afterwards did not retroactively fail. Escalating any
    // of these would be the bug in the opposite direction.
    const specific: NormalizedPipelineStatus[] = [
      "waiting_for_user",
      "paused",
      "completed",
      "failed",
      "cancelled",
      "idle",
      "unknown",
    ]
    for (const status of specific) {
      expect(reconcilePipelineStatus(status, "failed")).toBe(status)
      expect(reconcilePipelineStatus(status, "idle")).toBe(status)
    }
  })

  it("is idempotent — reconciling its own output changes nothing", () => {
    // Two of the five surfaces read a status that has already been reconciled
    // upstream; a second pass must not walk it further.
    const once = reconcilePipelineStatus("running", "idle")
    expect(reconcilePipelineStatus(once, "idle")).toBe(once)
    expect(reconcilePipelineStatus(once, "failed")).toBe(once)
  })
})
