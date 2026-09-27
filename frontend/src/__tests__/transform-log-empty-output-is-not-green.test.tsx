/**
 * A transform step that consumed rows and emitted none used to be written to
 * transform_execution_logs as status="success" and rendered with the green
 * badge. Every silent data-destroying defect in the engine -- a filter on a
 * renamed column, a required column no row carries, a typo in a select_columns
 * list -- landed there looking like a healthy run over an empty table.
 *
 * The engine no longer produces those, and the status is now its own value. This
 * pins the rendering, because the badge is the only place an operator sees it.
 */
import { describe, expect, it } from "vitest"
import { render, screen } from "@testing-library/react"
import { TransformExecutionLogsPanel } from "@/components/transforms/TransformExecutionLogsPanel"

function log(overrides: Record<string, unknown> = {}) {
  return {
    id: "1",
    table_name: "orders",
    transform_order: 0,
    transform_type: "filter",
    status: "empty_output",
    input_rows: 40045,
    output_rows: 0,
    duration_ms: 12,
    config_snapshot: {},
    ...overrides,
  } as any
}

describe("transform execution log status badge", () => {
  it("does not paint an emptied batch green", () => {
    const { container } = render(<TransformExecutionLogsPanel logs={[log()]} />)
    const badge = screen.getByText("no rows out")
    expect(badge).toBeInTheDocument()
    expect(badge.className).toContain("amber")
    expect(badge.className).not.toContain("emerald")
    // ...and the row still reports the counts that explain it.
    expect(container.textContent).toContain("40,045 → 0")
  })

  it("still paints a real success green and a failure red", () => {
    const { container } = render(
      <TransformExecutionLogsPanel
        logs={[
          log({ id: "2", status: "success", output_rows: 40045 }),
          log({ id: "3", status: "failed", transform_order: 1, error_message: "boom" }),
        ]}
      />
    )
    const ok = screen.getByText("success")
    expect(ok.className).toContain("emerald")
    const bad = screen.getByText("failed")
    expect(bad.className).toContain("red")
    expect(container).toBeTruthy()
  })
})
