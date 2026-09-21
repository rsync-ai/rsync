import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, it, expect } from "vitest"
import { computeStrategySteps, type DataLoadingStrategy } from "../dataLoadingStrategy"

/**
 * Pins the frontend's fallback step builder to the fixture the api-gateway's
 * dataLoadingStrategySteps is also pinned to (data_loading_strategy_golden_test.go).
 * The server sends these lines as explanation_steps; this builder writes them when
 * a strategy arrives without them (chat confirmation, older payloads), so a change
 * to one and not the other shows the same pipeline two ways.
 */
type GoldenCase = { name: string; input: DataLoadingStrategy; steps: string[] }

const goldenPath = resolve(__dirname, "../../../../../shared/data_loading_strategy_golden.json")
const golden: GoldenCase[] = JSON.parse(readFileSync(goldenPath, "utf8"))

describe("computeStrategySteps matches the server's shared golden", () => {
  it("golden fixture is not empty", () => {
    expect(golden.length).toBeGreaterThan(0)
  })

  it.each(golden.map((g) => [g.name, g.input, g.steps] as const))("%s", (_name, input, want) => {
    expect(computeStrategySteps(input)).toEqual(want)
  })

  it("server-sent steps win over the fallback", () => {
    const sent = ["from the server"]
    expect(computeStrategySteps({ ...golden[0].input, explanation_steps: sent })).toEqual(sent)
  })
})
