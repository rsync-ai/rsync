import { describe, expect, it } from "vitest"
import {
  isWaitingForFirstData,
  reconcilePipelineStatus,
  runtimePhaseLabel,
  RUNTIME_PHASE_WAITING_FOR_DATA,
  WAITING_FOR_FIRST_DATA_LABEL,
  type NormalizedPipelineStatus,
} from "@/lib/pipeline/statusNormalization"

// Issue #20: a CDC stream that finished setting up but delivered nothing read
// "Running · Streaming pipeline active" for as long as it stayed empty. The backend
// now reports runtime phase waiting_for_data; these pin how the UI reads it.

describe("isWaitingForFirstData", () => {
  it("relabels a running pipeline whose runtime phase is waiting_for_data", () => {
    expect(RUNTIME_PHASE_WAITING_FOR_DATA).toBe("waiting_for_data") // the Go wire value
    expect(isWaitingForFirstData("running", "waiting_for_data")).toBe(true)
  })

  it("does not relabel a running pipeline in any other phase (#7: a quiet stream stays streaming)", () => {
    for (const phase of ["streaming", "idle", "syncing", "failed", "completed", "", undefined, null]) {
      expect(isWaitingForFirstData("running", phase)).toBe(false)
    }
  })

  it("never relabels a /state verdict other than running, even with a stale waiting_for_data poll", () => {
    const others: NormalizedPipelineStatus[] = [
      "paused",
      "failed",
      "completed",
      "cancelled",
      "waiting_for_user",
      "idle",
      "unknown",
    ]
    for (const status of others) {
      expect(isWaitingForFirstData(status, "waiting_for_data")).toBe(false)
    }
  })

  it("leaves reconcilePipelineStatus at running, so every existing status pill is unchanged", () => {
    expect(reconcilePipelineStatus("running", "waiting_for_data")).toBe("running")
  })
})

describe("runtimePhaseLabel", () => {
  it("reads waiting_for_data as a sentence, not as its wire value", () => {
    expect(runtimePhaseLabel("waiting_for_data")).toBe(WAITING_FOR_FIRST_DATA_LABEL)
    expect(WAITING_FOR_FIRST_DATA_LABEL).toBe("Waiting for first data")
  })

  it("keeps the capitalized single-word labels the health header always showed", () => {
    expect(runtimePhaseLabel("streaming")).toBe("Streaming")
    expect(runtimePhaseLabel("idle")).toBe("Idle")
    expect(runtimePhaseLabel("paused")).toBe("Paused")
    expect(runtimePhaseLabel("")).toBe("Unknown")
    expect(runtimePhaseLabel(undefined)).toBe("Unknown")
  })
})
