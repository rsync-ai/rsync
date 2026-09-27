import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, fireEvent, cleanup } from "@testing-library/react"

const push = vi.fn()
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push, refresh: vi.fn() }),
}))

import type { ExecutionPlanStage } from "@/components/pipeline/dagTypes"
import { PipelineInsightsBar } from "@/components/pipeline/PipelineInsightsBar"

/**
 * The problem chips must send the prompts in the shared golden, which the
 * api-gateway test (chat_insight_chip_prompts_golden_test.go) requires to reach
 * the chat's per-pipeline diagnosis. Without the pipeline id, "these stages are
 * taking unusually long" went to intent classification, which cannot tell which
 * pipeline is meant, and got the generic pipeline examples back.
 */
type Golden = { pipeline_id: string; prompts: string[] }
const goldenPath = resolve(__dirname, "../../../../../shared/chat_insight_chip_prompts_golden.json")
const golden: Golden = JSON.parse(readFileSync(goldenPath, "utf8"))

// One stage runs 10x its peer, so it is the anomaly.
const slowStages: ExecutionPlanStage[] = [
  { id: "read", display_name: "Read Source", status: "complete", node_kind: "transform", actual_duration_ms: 1_000 },
  { id: "exec", display_name: "Executing Pipeline", status: "complete", node_kind: "transform", actual_duration_ms: 10_000 },
]
// Nothing is slow (no stage has a peer to compare with), one stage failed.
const failedStages: ExecutionPlanStage[] = [
  { id: "extract", display_name: "Extract", status: "complete", node_kind: "source", actual_duration_ms: 5_000 },
  { id: "load", display_name: "Load", status: "failed", node_kind: "destination" },
]

function promptsFromProblemChips(stages: ExecutionPlanStage[], pipelineId?: string): string[] {
  render(<PipelineInsightsBar pipelineName="orders-sync" pipelineId={pipelineId} stages={stages} />)
  const prompts: string[] = []
  for (const chip of screen.getAllByRole("button")) {
    if (/explain this pipeline/i.test(chip.textContent ?? "")) continue
    push.mockClear()
    fireEvent.click(chip)
    expect(push).toHaveBeenCalledTimes(1)
    const url = new URL(push.mock.calls[0][0] as string, "http://x")
    expect(url.pathname).toBe("/chat")
    expect(url.searchParams.get("autosend")).toBe("1")
    prompts.push(url.searchParams.get("prompt") ?? "")
  }
  cleanup()
  return prompts
}

describe("the pipeline page's problem chips name the pipeline", () => {
  beforeEach(() => push.mockReset())

  it("the golden fixture has prompts", () => {
    expect(golden.prompts.length).toBeGreaterThan(0)
  })

  it("send exactly the golden prompts, each carrying the pipeline id", () => {
    const sent = [
      ...promptsFromProblemChips(slowStages, golden.pipeline_id),
      ...promptsFromProblemChips(failedStages, golden.pipeline_id),
    ]
    expect(sent).toEqual(golden.prompts)
    for (const p of sent) expect(p).toContain(golden.pipeline_id)
  })

  it("the failures chip asks about the failed stage, not about performance", () => {
    render(<PipelineInsightsBar pipelineName="orders-sync" pipelineId={golden.pipeline_id} stages={failedStages} />)
    fireEvent.click(screen.getByRole("button", { name: /investigate failures/i }))
    const prompt = new URL(push.mock.calls[0][0] as string, "http://x").searchParams.get("prompt") ?? ""
    expect(prompt).toMatch(/^Why did this stage fail/)
    expect(prompt).toContain("Load")
    expect(prompt).not.toMatch(/performance anomalies/i)
  })

  it("without an id the prompts carry no id suffix", () => {
    for (const p of promptsFromProblemChips(slowStages)) expect(p).not.toMatch(/\(pipeline /)
  })
})
