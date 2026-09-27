/**
 * The "Saved-query version history" card on /workspace/settings
 * (GET/PUT /api/v1/explorer/version-retention, saved_query_retention.go).
 *
 * This file lives in src/__tests__ rather than beside the component because
 * vitest.config.ts's `include` globs do not cover components/workspace/**.
 *
 * Two behaviours here are the whole point of the card and are pinned hardest:
 * every PUT carries BOTH axes (the handler 400s on a missing `min_versions`,
 * deliberately — a client that sent only `retention_days` would silently widen
 * what the age axis may delete), and a failed GET renders as an error rather
 * than as the "keep forever" default, which would tell an admin their history
 * is safe while the stored policy was deleting it.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"
import { cleanup, render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

const authFetch = vi.fn()
vi.mock("@/lib/api/auth-fetch", () => ({
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

import {
  RETENTION_ENDPOINT,
  SavedQueryRetentionCard,
} from "@/components/workspace/SavedQueryRetentionCard"

type Policy = { retention_days: number | null; min_versions: number }

function json(status: number, body: unknown) {
  return { ok: status < 400, status, json: async () => body }
}

/** GET answers with `policy`; PUT echoes the body back unless `putStatus` is set. */
function serve(policy: Policy, opts: { putStatus?: number; putBody?: unknown } = {}) {
  authFetch.mockImplementation(async (_url: string, init?: RequestInit) => {
    if (init?.method === "PUT") {
      if (opts.putStatus) {
        return json(opts.putStatus, opts.putBody ?? { error: "insufficient workspace role" })
      }
      return json(200, JSON.parse(String(init.body)))
    }
    return json(200, policy)
  })
}

function putBodies() {
  return authFetch.mock.calls
    .filter(([, init]) => (init as RequestInit | undefined)?.method === "PUT")
    .map(([, init]) => JSON.parse(String((init as RequestInit).body)))
}

beforeEach(() => {
  authFetch.mockReset()
  toastError.mockReset()
  toastSuccess.mockReset()
})
afterEach(() => cleanup())

describe("SavedQueryRetentionCard", () => {
  it("reads the policy from the version-retention endpoint and states it in words", async () => {
    serve({ retention_days: 30, min_versions: 10 })
    render(<SavedQueryRetentionCard currentRole="admin" />)

    await waitFor(() =>
      expect(
        screen.getByText(/Versions older than 30 days are deleted/)
      ).toBeInTheDocument()
    )
    expect(screen.getByText(/newest 10 versions of each saved query are always kept/)).toBeInTheDocument()
    expect(authFetch.mock.calls[0][0]).toBe(RETENTION_ENDPOINT)
  })

  it("says nothing is deleted when retention_days is null", async () => {
    serve({ retention_days: null, min_versions: 5 })
    render(<SavedQueryRetentionCard currentRole="admin" />)

    await waitFor(() =>
      expect(screen.getByText("Every version is kept forever. Nothing is deleted.")).toBeInTheDocument()
    )
  })

  it("sends BOTH axes on a save that only changed the age limit", async () => {
    serve({ retention_days: null, min_versions: 25 })
    render(<SavedQueryRetentionCard currentRole="admin" />)
    await waitFor(() => expect(screen.getByLabelText("Retention days")).toBeInTheDocument())

    const user = userEvent.setup()
    await user.clear(screen.getByLabelText("Retention days"))
    await user.type(screen.getByLabelText("Retention days"), "90")
    await user.click(screen.getByRole("button", { name: /save policy/i }))

    await waitFor(() => expect(putBodies()).toHaveLength(1))
    // min_versions is untouched by this edit and still must be on the wire.
    expect(putBodies()[0]).toEqual({ retention_days: 90, min_versions: 25 })
  })

  it("sends retention_days: null — not a missing key — for 'keep forever'", async () => {
    serve({ retention_days: 30, min_versions: 5 })
    render(<SavedQueryRetentionCard currentRole="admin" />)
    await waitFor(() => expect(screen.getByLabelText("Minimum versions")).toBeInTheDocument())

    const user = userEvent.setup()
    await user.click(screen.getByRole("radio", { name: /keep every version forever/i }))
    await user.click(screen.getByRole("button", { name: /save policy/i }))

    await waitFor(() => expect(putBodies()).toHaveLength(1))
    const body = putBodies()[0]
    expect(body.retention_days).toBeNull()
    expect(Object.keys(body).sort()).toEqual(["min_versions", "retention_days"])
  })

  it("refuses a min_versions below the floor locally, without a round-trip", async () => {
    serve({ retention_days: null, min_versions: 5 })
    render(<SavedQueryRetentionCard currentRole="admin" />)
    await waitFor(() => expect(screen.getByLabelText("Minimum versions")).toBeInTheDocument())

    const user = userEvent.setup()
    await user.clear(screen.getByLabelText("Minimum versions"))
    await user.type(screen.getByLabelText("Minimum versions"), "4")
    await user.click(screen.getByRole("button", { name: /save policy/i }))

    await waitFor(() => expect(toastError).toHaveBeenCalled())
    expect(String(toastError.mock.calls[0][0])).toContain("between 5 and 1000")
    expect(putBodies()).toHaveLength(0)
  })

  it("says the prune is not scheduled, because nothing else says it", async () => {
    serve({ retention_days: 30, min_versions: 5 })
    render(<SavedQueryRetentionCard currentRole="admin" />)
    await waitFor(() => expect(screen.getByLabelText("Minimum versions")).toBeInTheDocument())

    await userEvent.setup().click(screen.getByRole("button", { name: /save policy/i }))

    await waitFor(() => expect(toastSuccess).toHaveBeenCalled())
    expect(String(toastSuccess.mock.calls[0][0])).toContain("not on a schedule")
  })

  it("surfaces the server's message when the PUT is rejected", async () => {
    serve({ retention_days: null, min_versions: 5 }, { putStatus: 403 })
    render(<SavedQueryRetentionCard currentRole="admin" />)
    await waitFor(() => expect(screen.getByLabelText("Minimum versions")).toBeInTheDocument())

    await userEvent.setup().click(screen.getByRole("button", { name: /save policy/i }))

    await waitFor(() => expect(toastError).toHaveBeenCalledWith("insufficient workspace role"))
  })

  it("renders a failed read as an error, never as 'keep forever'", async () => {
    authFetch.mockResolvedValue(json(500, { error: "database is down" }))
    render(<SavedQueryRetentionCard currentRole="admin" />)

    await waitFor(() => expect(screen.getByRole("alert")).toHaveTextContent("database is down"))
    expect(screen.queryByText(/Every version is kept forever/)).not.toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /save policy/i })).not.toBeInTheDocument()
  })

  it("shows a member the policy but no form — the PUT is admin-only", async () => {
    serve({ retention_days: 30, min_versions: 5 })
    render(<SavedQueryRetentionCard currentRole="member" />)

    await waitFor(() =>
      expect(screen.getByText(/Versions older than 30 days are deleted/)).toBeInTheDocument()
    )
    expect(screen.getByText("Only workspace admins can change this.")).toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /save policy/i })).not.toBeInTheDocument()
  })
})
