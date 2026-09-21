import { describe, it, expect, vi } from "vitest"
import { render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
}))

import { PipelinesTable, type PipelineListItem } from "../PipelinesTable"

// #48 prod retest: the /pipelines status badge read 1.85:1 in dark mode. The row
// passes its colours to <Badge> through className with no variant, so the default
// variant's `dark:text-zinc-900` survives unless the colour names its own dark text —
// and running / passed / paused / scheduled named a light-mode text colour only.
// Dark zinc-900 text on a dark tint is the failure, whatever the status.

const STATUSES: Array<[string, RegExp]> = [
  ["running", /^running$/i],
  ["passed", /^completed$/i],
  ["paused", /^paused$/i],
  ["stopped", /^stopped$/i],
  ["scheduled", /^scheduled$/i],
  ["idle", /^idle$/i],
  ["failed", /^failed$/i],
  ["waiting_for_user", /awaiting user/i],
  ["cancelled", /^cancelled$/i],
  ["pending", /^pending$/i],
]

function pipeline(derived_status: string): PipelineListItem {
  return {
    id: "p1",
    name: "orders-sync",
    pipeline_status: "active",
    created_at: "2026-09-01T00:00:00Z",
    updated_at: "2026-09-01T00:00:00Z",
    derived_status,
    sync_mode: "cdc",
  }
}

describe("PipelinesTable status badge — dark-mode text colour (#48)", () => {
  it.each(STATUSES)("%s names its own dark text colour", (status, label) => {
    render(<PipelinesTable pipelines={[pipeline(status)]} />)
    const badge = screen.getByText(label).closest(".rounded-full")
    expect(badge).not.toBeNull()
    const cls = badge!.className
    expect(cls).toMatch(/(?:^|\s)dark:text-/)
    expect(cls).not.toContain("dark:text-zinc-900")
  })
})
