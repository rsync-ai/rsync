/**
 * The header's load chip: "Full load in progress · 3 / 5 tables", then "Load
 * completed, replication ongoing", the way AWS DMS reports a task. It reads the
 * initial load the orchestrator recorded (runtime.load); without one it says
 * nothing rather than guess that a load finished.
 */

import { afterEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

import type { PipelineRuntime, RuntimeLoad } from "@/lib/hooks/usePipelineRuntime"

let runtimeValue: PipelineRuntime | null = null
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({
  usePipelineRuntime: () => ({ runtime: runtimeValue, loading: false, error: null }),
}))
vi.mock("@/components/pipeline/DiagnosePanel", () => ({ DiagnosePanel: () => null }))

import { PipelineHealthHeader } from "@/components/pipeline/PipelineHealthHeader"

function cdc(load?: RuntimeLoad, over: Partial<PipelineRuntime> = {}): PipelineRuntime {
  return {
    pipeline_id: "p1",
    mode: "cdc",
    phase: "streaming",
    health: "healthy",
    dependencies: [],
    updated_at: "2026-09-25T10:00:00Z",
    load,
    ...over,
  }
}

const started: RuntimeLoad = {
  status: "started",
  mode: "blocking",
  tables_total: 5,
  tables_done: 3,
  started_at: "2026-09-25T09:00:00Z",
  reloading_tables: 0,
}

afterEach(() => {
  cleanup()
  runtimeValue = null
})

describe("PipelineHealthHeader load chip", () => {
  it("counts tables while the full load runs", () => {
    runtimeValue = cdc(started)
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.getByTestId("pipeline-load-status")).toHaveTextContent("Full load in progress · 3 / 5 tables")
  })

  it("says load completed, replication ongoing once it has", () => {
    runtimeValue = cdc({ ...started, status: "completed", tables_done: 5, completed_at: "2026-09-25T09:30:00Z" })
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.getByTestId("pipeline-load-status")).toHaveTextContent("Load completed, replication ongoing")
  })

  // The source read ends before the destination has every row; the chip waits
  // for the rows, not for Debezium's last snapshot marker.
  it("does not say load completed while its rows are still being written", () => {
    runtimeValue = cdc({
      ...started,
      status: "completed",
      tables_done: 5,
      completed_at: "2026-09-25T09:30:00Z",
      snapshot_rows_waiting: 17819,
    })
    render(<PipelineHealthHeader pipelineId="p1" />)
    const chip = screen.getByTestId("pipeline-load-status")
    expect(chip).toHaveTextContent("Full load read · writing 17,819 rows")
    expect(chip).not.toHaveTextContent("Load completed")
  })

  it("puts a failed load's recorded reason on the chip", () => {
    runtimeValue = cdc({ ...started, status: "unconfirmed", last_error: "no snapshot rows for 1h" })
    render(<PipelineHealthHeader pipelineId="p1" />)
    const chip = screen.getByTestId("pipeline-load-status")
    expect(chip).toHaveTextContent("Full load not confirmed")
    expect(chip).toHaveAttribute("title", "no snapshot rows for 1h")
  })

  // The chip's title is a mouse-only tooltip: a keyboard or screen-reader user
  // cannot reach it, and the header shows on every tab.
  it("says a failed load's reason in words, not only in a tooltip", () => {
    runtimeValue = cdc({ ...started, status: "failed", last_error: "the batch historical load failed" })
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.getByTestId("pipeline-load-reason")).toHaveTextContent("the batch historical load failed")
  })

  it("control: a load with no recorded reason prints no reason line", () => {
    runtimeValue = cdc({ ...started, status: "completed", tables_done: 5, completed_at: "2026-09-25T09:30:00Z" })
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.getByTestId("pipeline-load-status")).toBeInTheDocument()
    expect(screen.queryByTestId("pipeline-load-reason")).toBeNull()
  })

  it("shows no chip without a recorded load, or on a batch pipeline", () => {
    runtimeValue = cdc(undefined)
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.queryByTestId("pipeline-load-status")).toBeNull()
    cleanup()
    runtimeValue = cdc(started, { mode: "batch" })
    render(<PipelineHealthHeader pipelineId="p1" />)
    expect(screen.queryByTestId("pipeline-load-status")).toBeNull()
  })
})
