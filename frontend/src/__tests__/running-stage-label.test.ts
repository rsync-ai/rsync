import { describe, expect, it } from "vitest"
import {
  executorRunningMessage,
  findConnectionNames,
  positiveDurationMs,
  runningStageTimeLabel,
} from "@/components/chat/runningStageLabel"

describe("running stage time label", () => {
  it("a zero measured duration is not a duration", () => {
    expect(positiveDurationMs(0)).toBeNull()
    expect(positiveDurationMs(-5)).toBeNull()
    expect(positiveDurationMs(undefined)).toBeNull()
    expect(positiveDurationMs(1500)).toBe(1500)
  })

  it("prefers the live elapsed time over a stale duration (was: \"Preparing… (0ms)\")", () => {
    expect(runningStageTimeLabel("42s", "0ms")).toBe("42s")
    expect(runningStageTimeLabel("42s", null)).toBe("42s")
  })

  it("never shows a zero", () => {
    expect(runningStageTimeLabel("0s", "0ms")).toBeNull()
    expect(runningStageTimeLabel(null, "0ms")).toBeNull()
    expect(runningStageTimeLabel("0ms", "3s")).toBe("3s")
  })
})

describe("executor subtitle", () => {
  const validation = {
    id: "connection_validation",
    status: "complete",
    metadata: { source_connection_name: "datingapp mongo", destination_connection_name: "gcs lake" },
  }
  const executor = { id: "executor", status: "running", progress: 0, metadata: {} }

  it("finds connection names on the stage that emitted them", () => {
    expect(findConnectionNames(executor, [validation, executor])).toEqual({
      srcName: "datingapp mongo",
      dstName: "gcs lake",
    })
  })

  it("says Preparing (with the route) before there is transfer evidence", () => {
    const { srcName, dstName } = findConnectionNames(executor, [validation])
    expect(executorRunningMessage({ stage: executor, srcName, dstName, currentStage: "executor" })).toBe(
      'Preparing "datingapp mongo" → "gcs lake"…',
    )
  })

  it("says Syncing once rows or per-table progress exist", () => {
    expect(
      executorRunningMessage({
        stage: executor,
        srcName: "a",
        dstName: "b",
        stateMessage: "Transferred 1 of 3 tables",
      }),
    ).toBe('Syncing "a" → "b"…')
    expect(
      executorRunningMessage({ stage: { ...executor, metadata: { rows_synced: 10 } }, srcName: null, dstName: null }),
    ).toBe("Syncing data…")
  })

  it("waits for infrastructure while infra_preflight runs", () => {
    expect(
      executorRunningMessage({ stage: executor, srcName: "a", dstName: "b", currentStage: "infra_preflight" }),
    ).toBe("Waiting for infrastructure…")
  })
})
