/**
 * The "Schema change alerts" card on /pipelines/:id/schema-changes
 * (GET/PUT /pipelines/:id/schema-drift-policy, api-gateway schema_evolution.go).
 *
 * The PUT resets any field it is not sent to true, so every save must carry all
 * three fields — otherwise unchecking one box would silently re-check another.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

let mockRole = "member"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
  authFetch: (...args: unknown[]) => authFetch(...args),
}))
const toastError = vi.fn()
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: (...a: unknown[]) => toastError(...a) } }))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: mockRole,
    isLoading: false,
    error: false,
    activeWorkspace: null,
    can: () => false,
    // Delegate to the real ladder so this test can't drift from roles.ts.
    meets: (min: string) => realMeetsRole(mockRole, min as never),
  }),
}))

import { meetsRole as realMeetsRole } from "@/lib/workspace/roles"
import { SchemaDriftPolicyCard } from "@/components/pipeline/SchemaDriftPolicyCard"

type Policy = { enabled?: boolean; notify_on_add?: boolean; notify_on_drop?: boolean }

function json(status: number, body: unknown) {
  return { ok: status < 400, status, json: async () => body }
}

/** GET answers with `policy`; PUT echoes the body back (or fails with `putStatus`). */
function serve(policy: Policy, opts: { detector?: boolean; putStatus?: number } = {}) {
  authFetch.mockImplementation(async (_url: string, init?: RequestInit) => {
    const detector = opts.detector === undefined ? {} : { detector_enabled: opts.detector }
    if (init?.method === "PUT") {
      if (opts.putStatus) return json(opts.putStatus, { error: "insufficient workspace role" })
      return json(200, { schema_drift_policy: JSON.parse(String(init.body)), ...detector })
    }
    return json(200, { schema_drift_policy: policy, ...detector })
  })
}

function putBodies() {
  return authFetch.mock.calls
    .filter(([, init]) => (init as RequestInit | undefined)?.method === "PUT")
    .map(([, init]) => JSON.parse(String((init as RequestInit).body)))
}

beforeEach(() => {
  mockRole = "member"
  authFetch.mockReset()
  toastError.mockReset()
})
afterEach(() => cleanup())

describe("SchemaDriftPolicyCard", () => {
  it("renders the stored policy, with type changes locked on", async () => {
    serve({ enabled: true, notify_on_add: true, notify_on_drop: false }, { detector: true })
    render(<SchemaDriftPolicyCard pipelineId="p1" isCDC={false} />)

    expect(await screen.findByRole("switch", { name: "Watch for schema changes" })).toBeChecked()
    expect(screen.getByRole("checkbox", { name: "New tables and columns" })).toBeChecked()
    expect(screen.getByRole("checkbox", { name: "Dropped tables and columns" })).not.toBeChecked()
    const typeChange = screen.getByRole("checkbox", { name: /column type changes/i })
    expect(typeChange).toBeChecked()
    expect(typeChange).toBeDisabled()
    expect(authFetch.mock.calls[0][0]).toContain("/pipelines/p1/schema-drift-policy")
  })

  it("sends all three fields on every save", async () => {
    serve({ enabled: true, notify_on_add: true, notify_on_drop: true }, { detector: true })
    const user = userEvent.setup()
    render(<SchemaDriftPolicyCard pipelineId="p1" isCDC={false} />)

    await user.click(await screen.findByRole("checkbox", { name: "New tables and columns" }))

    await waitFor(() => expect(screen.getByText("Saved")).toBeInTheDocument())
    expect(putBodies()).toEqual([{ enabled: true, notify_on_add: false, notify_on_drop: true }])
    expect(screen.getByRole("checkbox", { name: "New tables and columns" })).not.toBeChecked()
  })

  it("rolls the box back and says why when the save fails", async () => {
    serve({ enabled: true, notify_on_add: true, notify_on_drop: true }, { detector: true, putStatus: 403 })
    const user = userEvent.setup()
    render(<SchemaDriftPolicyCard pipelineId="p1" isCDC={false} />)

    await user.click(await screen.findByRole("checkbox", { name: "Dropped tables and columns" }))

    await waitFor(() => expect(toastError).toHaveBeenCalledWith("insufficient workspace role"))
    expect(screen.getByRole("checkbox", { name: "Dropped tables and columns" })).toBeChecked()
    expect(screen.queryByText("Saved")).toBeNull()
  })

  it("greys the two options out when watching is off for the pipeline", async () => {
    serve({ enabled: false, notify_on_add: true, notify_on_drop: true }, { detector: true })
    render(<SchemaDriftPolicyCard pipelineId="p1" isCDC={false} />)

    expect(await screen.findByRole("switch", { name: "Watch for schema changes" })).not.toBeChecked()
    expect(screen.getByText("Off for this pipeline: no changes are checked.")).toBeInTheDocument()
    expect(screen.getByRole("checkbox", { name: "New tables and columns" })).toBeDisabled()
    expect(screen.getByRole("checkbox", { name: "Dropped tables and columns" })).toBeDisabled()
    // The switch itself stays usable, so it can be turned back on.
    expect(screen.getByRole("switch", { name: "Watch for schema changes" })).toBeEnabled()
  })

  it("is read-only for a viewer, and says what access is needed", async () => {
    mockRole = "viewer"
    serve({ enabled: true, notify_on_add: true, notify_on_drop: true }, { detector: true })
    render(<SchemaDriftPolicyCard pipelineId="p1" isCDC={false} />)

    expect(await screen.findByRole("switch", { name: "Watch for schema changes" })).toBeDisabled()
    expect(screen.getByRole("checkbox", { name: "New tables and columns" })).toBeDisabled()
    expect(screen.getByTestId("drift-policy-readonly")).toHaveTextContent("Needs member access to change.")
  })

  it("warns only when the gateway says detection is off for the installation", async () => {
    serve({}, { detector: false })
    render(<SchemaDriftPolicyCard pipelineId="p1" isCDC={false} />)
    expect(await screen.findByText(/detection is off for this installation/i)).toBeInTheDocument()
    cleanup()

    // An older gateway that doesn't report the switch must not trigger the warning.
    serve({})
    render(<SchemaDriftPolicyCard pipelineId="p1" isCDC={false} />)
    await screen.findByRole("switch", { name: "Watch for schema changes" })
    expect(screen.queryByText(/detection is off for this installation/i)).toBeNull()
  })

  it("explains the CDC scope on a CDC pipeline only", async () => {
    serve({}, { detector: true })
    render(<SchemaDriftPolicyCard pipelineId="p1" isCDC />)
    expect(await screen.findByTestId("drift-cdc-note")).toHaveTextContent(/apply only to a batch initial load/)
    cleanup()

    serve({}, { detector: true })
    render(<SchemaDriftPolicyCard pipelineId="p1" isCDC={false} />)
    await screen.findByRole("switch", { name: "Watch for schema changes" })
    expect(screen.queryByTestId("drift-cdc-note")).toBeNull()
  })

  it("offers a retry instead of vanishing when the load fails", async () => {
    authFetch.mockResolvedValueOnce(json(500, { error: "boom" }))
    serve({}, { detector: true })
    const user = userEvent.setup()
    render(<SchemaDriftPolicyCard pipelineId="p1" isCDC={false} />)

    await user.click(await screen.findByRole("button", { name: "Retry" }))
    expect(await screen.findByRole("switch", { name: "Watch for schema changes" })).toBeInTheDocument()
  })
})
