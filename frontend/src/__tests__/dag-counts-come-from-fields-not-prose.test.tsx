/**
 * Regression guard for KI-DAG-METADATA-KEYS-NOBODY-EMITS.
 *
 * The DAG screen used to derive two numbers from things that never carry them:
 *
 *   - a record count, scraped out of `result_summary` **prose** by
 *     `parseRecordCount`. That field only ever holds "Plan created",
 *     "Plan validated" or "Pipeline executed"
 *     (`nl_pipeline_v2_workflow.go:1168`, `:1237`, `:1995`) — digit-free, so
 *     the parse returned null on every stage of every pipeline. The struct that
 *     writes it documents it as `"2-step plan created"`
 *     (`types/execution_plan.go:55`); only the TypeScript comment ever claimed
 *     `"12,450 rows transferred"`, and the helper was written against that.
 *
 *   - a masked-field count, read by `extractPiiInfo` from five metadata keys
 *     (`pii_fields`, `masked_fields`, `redacted_fields`, `pii_count`,
 *     `pii_fields_masked`). A repo-wide census finds zero writers of any of
 *     them.
 *
 * Both readers are deleted. These tests fail if either comes back, and they are
 * written so that a reader restored against a *plausible-looking* payload is
 * what trips them — a stage here deliberately carries all five PII keys and a
 * `result_summary` full of countable prose. Nothing may turn that into a claim.
 *
 * The bound at the end matters as much: `result_summary` must still be rendered
 * verbatim. The bug was never "show less"; it was "stop inventing numbers".
 */

import { describe, it, expect, vi } from "vitest"
import { render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

import type { ExecutionPlanStage } from "@/components/pipeline/DAGVisualization"
import { StageDetailPanel } from "@/components/pipeline/StageDetailPanel"
import { PipelineInsightsBar } from "@/components/pipeline/PipelineInsightsBar"

/**
 * The bait. Every key the deleted readers looked for, plus prose the deleted
 * parser would have matched twice over ("12,450 rows", and the "3.5K" fallback).
 * A correct build ignores all of it and shows the sentence itself.
 */
function baitedStage(over: Partial<ExecutionPlanStage> = {}): ExecutionPlanStage {
  return {
    id: "dest-1",
    display_name: "Load to warehouse",
    status: "complete",
    node_kind: "destination",
    result_summary: "Loaded 12,450 rows and skipped 3.5K events",
    metadata: {
      pii_fields: ["email", "ssn"],
      masked_fields: ["email"],
      redacted_fields: ["ssn"],
      pii_count: 2,
      pii_fields_masked: 2,
    },
    ...over,
  }
}

describe("a record count is never scraped out of result_summary prose", () => {
  it("StageDetailPanel shows the sentence, not a headline number parsed from it", () => {
    render(
      <StageDetailPanel
        stage={baitedStage()}
        allStages={[]}
        onClose={() => {}}
        onSelectStage={() => {}}
      />
    )

    // The bound: the backend's own words survive.
    expect(
      screen.getByText("Loaded 12,450 rows and skipped 3.5K events")
    ).toBeInTheDocument()

    // The guard: no "Records loaded / extracted / processed" headline, and no
    // formatted count. `formatCount(12450)` rendered "12.5K"; the raw
    // `toLocaleString()` echo rendered "(12,450)".
    expect(screen.queryByText(/records (loaded|extracted|processed)/i)).toBeNull()
    expect(screen.queryByText("12.5K")).toBeNull()
    expect(screen.queryByText("(12,450)")).toBeNull()
  })

  it("PipelineInsightsBar claims no total and offers no breakdown chip", () => {
    render(<PipelineInsightsBar pipelineName="warehouse-sync" stages={[baitedStage()]} />)

    expect(screen.queryByText(/records loaded/i)).toBeNull()
    expect(screen.queryByRole("button", { name: /record breakdown/i })).toBeNull()
  })
})

describe("a masked-field count is never read from metadata keys nobody writes", () => {
  it("StageDetailPanel raises no PII protection claim from the five keys", () => {
    render(
      <StageDetailPanel
        stage={baitedStage()}
        allStages={[]}
        onClose={() => {}}
        onSelectStage={() => {}}
      />
    )

    // Gone: the interpreted claim — a cyan "PII protection" callout reading
    // "2 fields masked: email, ssn".
    expect(screen.queryByText(/pii protection/i)).toBeNull()
    expect(screen.queryByText(/email, ssn/)).toBeNull()

    // Still here, and correctly so: the panel's generic "Metadata" section
    // echoes whatever primitive keys the payload carried, under a heading that
    // says exactly what it is. Echoing `pii_fields_masked: 2` is honest;
    // rendering "2 fields masked" as a protection claim is what was not. This
    // assertion is the line between the two — deleting the echo would be
    // over-correcting, and this test would catch that too.
    expect(screen.getByText("Metadata")).toBeInTheDocument()
    expect(screen.getByText("Pii Fields Masked")).toBeInTheDocument()
  })

  it("PipelineInsightsBar offers no PII audit chip", () => {
    render(<PipelineInsightsBar pipelineName="warehouse-sync" stages={[baitedStage()]} />)

    expect(screen.queryByText(/pii fields? masked/i)).toBeNull()
    expect(screen.queryByRole("button", { name: /audit pii handling/i })).toBeNull()
  })
})

describe("the bound — everything the DAG legitimately knows still renders", () => {
  it("keeps the insights bar useful: it still explains and still names the slowest stage", () => {
    const slow = baitedStage({ id: "slow", display_name: "Extract", actual_duration_ms: 42_000 })
    render(<PipelineInsightsBar pipelineName="warehouse-sync" stages={[slow]} />)

    expect(screen.getByRole("button", { name: /explain this pipeline/i })).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /Extract.*slowest/i })).toBeInTheDocument()
  })

  it("a failed stage still shows its error, which is a real field", () => {
    render(
      <StageDetailPanel
        stage={baitedStage({ status: "failed", error_message: "connection refused" })}
        allStages={[]}
        onClose={() => {}}
        onSelectStage={() => {}}
      />
    )
    expect(screen.getByText("connection refused")).toBeInTheDocument()
  })
})
