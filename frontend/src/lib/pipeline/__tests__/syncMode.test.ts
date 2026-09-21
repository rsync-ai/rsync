import { describe, expect, it } from "vitest"
import { connectionIsCDC, pipelineIsCDC } from "../syncMode"

describe("pipelineIsCDC", () => {
  it("a batch pipeline carrying the connection's default cdc_mode is not CDC", () => {
    expect(pipelineIsCDC({ sync_mode: "batch", cdc_mode: "initial" })).toBe(false)
  })
  it("explicit cdc is CDC", () => {
    expect(pipelineIsCDC({ sync_mode: "CDC", cdc_mode: null })).toBe(true)
  })
  it("legacy row with only cdc_mode is CDC", () => {
    expect(pipelineIsCDC({ sync_mode: "", cdc_mode: "streaming_only" })).toBe(true)
  })
  it("no mode at all is undecided", () => {
    expect(pipelineIsCDC({})).toBeNull()
    expect(pipelineIsCDC(null)).toBeNull()
  })
})

describe("connectionIsCDC", () => {
  it("ignores the cdc_mode column default on a batch connection", () => {
    expect(connectionIsCDC({ sync_mode: "batch", cdc_mode: "initial" })).toBe(false)
    expect(connectionIsCDC({ cdc_mode: "initial" })).toBe(false)
  })
  it("honours sync_mode cdc", () => {
    expect(connectionIsCDC({ sync_mode: "cdc" })).toBe(true)
  })
})
