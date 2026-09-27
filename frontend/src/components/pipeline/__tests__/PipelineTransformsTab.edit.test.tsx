/**
 * The editable half of the pipeline Transforms tab — PUT/DELETE
 * /api/v1/transforms/:id, which existed with no caller until this change.
 *
 * The invariant worth a test is the one that is invisible on screen: a logical
 * transform is usually TWO rows (a `producer` copy for batch and a `consumer`
 * copy for CDC, merged on read), so every mutation must address both. Hitting
 * one id would leave the other lane running while the tab shows one switch —
 * and for a masking rule that is the difference between "masking is off" and
 * "the operator believes masking is off on both lanes".
 *
 * The other invariant: disabling a mask is guarded. `enabled = TRUE` is what
 * the Mongo CDC fail-closed check loads (nl_transforms_gate.go:584), so
 * switching a mask off removes it from the runtime AND from the check that
 * would have refused the run.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

let mockRole = "member"

const authGet = vi.fn()
const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authGet: (...args: unknown[]) => authGet(...args),
  authFetch: (...args: unknown[]) => authFetch(...args),
}))

const toastError = vi.fn()
const toastSuccess = vi.fn()
vi.mock("sonner", () => ({
  toast: {
    error: (...a: unknown[]) => toastError(...a),
    success: (...a: unknown[]) => toastSuccess(...a),
  },
}))

vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: mockRole,
    isLoading: false,
    error: false,
    activeWorkspace: null,
    can: () => false,
    meets: (min: string) => realMeetsRole(mockRole, min as never),
  }),
}))

// The monitoring/log panels fetch on their own and are not what this file is
// about; stub them so a failure here is always about the transform rows.
vi.mock("@/components/transforms/TransformMonitoringPanel", () => ({
  TransformMonitoringPanel: () => null,
}))
vi.mock("@/components/transforms/TransformExecutionLogsPanel", () => ({
  TransformExecutionLogsPanel: () => null,
}))

import { meetsRole as realMeetsRole } from "@/lib/workspace/roles"
import { PipelineTransformsTab } from "@/components/pipeline/PipelineTransformsTab"

type Def = {
  id: string
  transform_type: string
  transform_order: number
  transform_config: Record<string, unknown>
  enabled: boolean
}

const MASK_CFG = { operation: "mask_pii", column: "email", mask_type: "hash" }

/** One masking rule persisted twice — the shape the creation flow writes. */
function bothLanes(cfg: Record<string, unknown> = MASK_CFG, enabled = true) {
  return {
    producer_transforms: [
      { id: "batch-id", transform_type: "producer", transform_order: 0, transform_config: cfg, enabled },
    ] as Def[],
    consumer_transforms: [
      { id: "cdc-id", transform_type: "consumer", transform_order: 0, transform_config: cfg, enabled },
    ] as Def[],
  }
}

function serveGet(resp: unknown) {
  authGet.mockImplementation(async (url: string) => {
    if (String(url).includes("/executions")) return { executions: [] }
    return resp
  })
}

function calls() {
  return authFetch.mock.calls.map(([url, init]) => ({
    url: String(url),
    method: (init as RequestInit | undefined)?.method,
    body: (init as RequestInit | undefined)?.body ? JSON.parse(String((init as RequestInit).body)) : undefined,
  }))
}

beforeEach(() => {
  mockRole = "member"
  authGet.mockReset()
  authFetch.mockReset()
  authFetch.mockResolvedValue({ ok: true, status: 200, json: async () => ({}) })
  toastError.mockReset()
  toastSuccess.mockReset()
})
afterEach(() => cleanup())

describe("PipelineTransformsTab — editing", () => {
  it("shows a member a switch and a delete control per transform", async () => {
    serveGet(bothLanes())
    render(<PipelineTransformsTab pipelineId="p1" />)

    await waitFor(() => expect(screen.getByLabelText("Disable email")).toBeInTheDocument())
    expect(screen.getByLabelText("Delete email")).toBeInTheDocument()
  })

  it("gives a viewer the old read-only text and no controls", async () => {
    mockRole = "viewer"
    serveGet(bothLanes())
    render(<PipelineTransformsTab pipelineId="p1" />)

    await waitFor(() => expect(screen.getByText("enabled")).toBeInTheDocument())
    expect(screen.queryByLabelText("Disable email")).not.toBeInTheDocument()
    expect(screen.queryByLabelText("Delete email")).not.toBeInTheDocument()
  })

  it("warns before a mask is switched off, and sends nothing until confirmed", async () => {
    serveGet(bothLanes())
    render(<PipelineTransformsTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Disable email")).toBeInTheDocument())

    const user = userEvent.setup()
    await user.click(screen.getByLabelText("Disable email"))

    expect(await screen.findByText("Turn off this masking rule?")).toBeInTheDocument()
    expect(screen.getByText(/copies these columns to the destination UNMASKED/)).toBeInTheDocument()
    // The sentence that explains why this is not merely a display toggle.
    expect(screen.getByText(/pre-run check that would otherwise have refused to start/)).toBeInTheDocument()
    expect(authFetch).not.toHaveBeenCalled()

    await user.click(screen.getByRole("button", { name: "Cancel" }))
    expect(authFetch).not.toHaveBeenCalled()
  })

  it("PUTs both ids — batch and CDC — when the disable is confirmed", async () => {
    serveGet(bothLanes())
    render(<PipelineTransformsTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Disable email")).toBeInTheDocument())

    const user = userEvent.setup()
    await user.click(screen.getByLabelText("Disable email"))
    await user.click(await screen.findByRole("button", { name: /turn off masking/i }))

    await waitFor(() => expect(calls()).toHaveLength(2))
    expect(calls().map((c) => c.url.replace(/^.*\/transforms\//, ""))).toEqual(["batch-id", "cdc-id"])
    expect(calls().every((c) => c.method === "PUT")).toBe(true)
    // `enabled` alone: UpdateTransform writes only non-nil fields, so sending
    // the config back would be a chance to clobber it for no reason.
    expect(calls()[0].body).toEqual({ enabled: false })
  })

  it("turns masking back ON without a confirmation — that restores protection", async () => {
    serveGet(bothLanes(MASK_CFG, false))
    render(<PipelineTransformsTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Enable email")).toBeInTheDocument())

    await userEvent.setup().click(screen.getByLabelText("Enable email"))

    await waitFor(() => expect(calls()).toHaveLength(2))
    expect(calls()[0].body).toEqual({ enabled: true })
    expect(screen.queryByText("Turn off this masking rule?")).not.toBeInTheDocument()
  })

  it("does not warn about unmasked data when a non-masking rule is disabled", async () => {
    serveGet(bothLanes({ operation: "rename_columns", mappings: { old_name: "new_name" } }))
    render(<PipelineTransformsTab pipelineId="p1" />)
    await waitFor(() =>
      expect(screen.getByLabelText("Disable old_name → new_name")).toBeInTheDocument()
    )

    await userEvent.setup().click(screen.getByLabelText("Disable old_name → new_name"))

    // Straight through: no dialog, both lanes updated.
    await waitFor(() => expect(calls()).toHaveLength(2))
    expect(screen.queryByText(/UNMASKED/)).not.toBeInTheDocument()
  })

  it("always confirms a delete, then DELETEs both ids", async () => {
    serveGet(bothLanes({ operation: "rename_columns", mappings: { old_name: "new_name" } }))
    render(<PipelineTransformsTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Delete old_name → new_name")).toBeInTheDocument())

    const user = userEvent.setup()
    await user.click(screen.getByLabelText("Delete old_name → new_name"))
    expect(await screen.findByText("Delete this transform?")).toBeInTheDocument()
    expect(screen.getByText(/both rows \(producer and consumer\)/)).toBeInTheDocument()
    expect(authFetch).not.toHaveBeenCalled()

    await user.click(screen.getByRole("button", { name: /delete transform/i }))
    await waitFor(() => expect(calls()).toHaveLength(2))
    expect(calls().every((c) => c.method === "DELETE")).toBe(true)
  })

  it("says so when one lane changed and the other refused", async () => {
    serveGet(bothLanes({ operation: "rename_columns", mappings: { old_name: "new_name" } }))
    authFetch
      .mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({}) })
      .mockResolvedValueOnce({ ok: false, status: 500, json: async () => ({ error: "write failed" }) })
    render(<PipelineTransformsTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Disable old_name → new_name")).toBeInTheDocument())

    await userEvent.setup().click(screen.getByLabelText("Disable old_name → new_name"))

    await waitFor(() => expect(toastError).toHaveBeenCalled())
    const msg = String(toastError.mock.calls[0][0])
    expect(msg).toContain("write failed")
    expect(msg).toContain("1 of 2 rows changed")
    expect(msg).toContain("batch and CDC copies now disagree")
  })

  it("addresses only the id it has when a transform lives on one lane", async () => {
    authGet.mockImplementation(async (url: string) => {
      if (String(url).includes("/executions")) return { executions: [] }
      return {
        producer_transforms: [],
        consumer_transforms: [
          {
            id: "cdc-only",
            transform_type: "consumer",
            transform_order: 0,
            transform_config: { operation: "filter", condition: "amount > 0" },
            enabled: true,
          },
        ],
      }
    })
    render(<PipelineTransformsTab pipelineId="p1" />)
    await waitFor(() => expect(screen.getByLabelText("Disable amount > 0")).toBeInTheDocument())

    await userEvent.setup().click(screen.getByLabelText("Disable amount > 0"))

    await waitFor(() => expect(calls()).toHaveLength(1))
    expect(calls()[0].url).toContain("cdc-only")
  })
})
