/**
 * Regression tests for F-242 — "infra failure renders as empty state".
 *
 * `authFetch` returns the Response without throwing, while its own siblings
 * `authGet`/`authPost` throw on `!res.ok`. Every handler that wrapped it in a
 * bare try/catch therefore treated HTTP 403/409/500 as success. Found on prod
 * across nine surfaces; the two shapes proven here are the two that matter:
 *
 *   1. an ACTION that silently does nothing (Apply migration, Stop, Pause,
 *      Resume) — pixel-identical to success, so the operator believes the
 *      pipeline stopped when the role gate refused;
 *   2. a READ that renders "none" — indistinguishable from a genuine empty
 *      state.
 *
 * The fix is a sibling helper, `authFetchOrThrow`, NOT a change to `authFetch`
 * itself: measured 200 call sites, 92 of which check `.ok` and 15 of which
 * branch on specific status codes (CDCLagAlertsPanel's `404 || 403` branch
 * would stop running and its `catch` would then render the green all-clear —
 * making the P0 strictly worse). Widening the contract of the shared helper
 * would have broken those; adding a sibling cannot.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest"
import { render, screen, waitFor, act } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

// Stub the network, NOT the module. `authFetchOrThrow` calls `authFetch`
// through a module-internal reference, so a module mock of `authFetch` would
// leave the helper talking to a real socket — and the test would then be
// measuring ECONNREFUSED rather than the 403 it means to assert on.
vi.mock("sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn(), warning: vi.fn() },
}))

import { authFetchOrThrow } from "@/lib/api/auth-fetch"
import { classifyError } from "@/lib/utils/error-handling"
import { toast } from "sonner"
import { SchemaEvolutionPanel } from "@/components/pipeline/SchemaEvolutionPanel"
import { PipelineExecutionStatusBadge } from "@/components/pipeline/PipelineExecutionStatusBadge"
import { SchemaDriftBadge } from "@/components/pipeline/SchemaDriftBadge"

const mockAuthFetch = vi.fn()
vi.stubGlobal("fetch", mockAuthFetch)

/** A Response that is exactly what the api-gateway's role gate returns. */
function forbidden(message = "insufficient workspace role") {
  return {
    ok: false,
    status: 403,
    statusText: "Forbidden",
    text: async () => JSON.stringify({ error: message }),
    json: async () => ({ error: message }),
  } as unknown as Response
}

function okJson(body: unknown) {
  return {
    ok: true,
    status: 200,
    statusText: "OK",
    text: async () => JSON.stringify(body),
    json: async () => body,
  } as unknown as Response
}

describe("authFetchOrThrow — the helper the silent sites were missing", () => {
  beforeEach(() => vi.clearAllMocks())

  it("throws on a non-2xx, carrying the status AND the server's own message", async () => {
    mockAuthFetch.mockResolvedValue(forbidden("insufficient workspace role"))

    await expect(authFetchOrThrow("/api/v1/pipelines/p1/stop", { method: "POST" })).rejects.toThrow()

    // The status has to survive the throw, or classifyError cannot tell a role
    // refusal (403 — actionable by the user) from a server fault (500).
    const err = await authFetchOrThrow("/api/v1/pipelines/p1/stop", { method: "POST" }).catch((e) => e)
    const classified = classifyError(err, "general")
    expect(classified.title).toBe("Authentication error")
    expect(classified.message).toContain("insufficient workspace role")
  })

  it("returns the Response untouched on 2xx, with the body still unread", async () => {
    // If the helper consumed the body to look for an error, every existing
    // `await res.json()` at the call site would throw on an already-used stream.
    mockAuthFetch.mockResolvedValue(okJson({ schema_changes: [] }))

    const res = await authFetchOrThrow("/api/v1/pipelines/p1/schema-changes")

    expect(res.ok).toBe(true)
    await expect(res.json()).resolves.toEqual({ schema_changes: [] })
  })
})

const PENDING_CHANGE = {
  id: "sc-1",
  pipeline_id: "p1",
  change_type: "add_column",
  table_name: "public.orders",
  ddl: "ALTER TABLE public.orders ADD COLUMN note TEXT",
  reasoning: "source added a column",
  risks: "[]",
  user_message: "New column detected",
  status: "pending" as const,
  reviewed_by: null,
  reviewed_at: null,
  applied_at: null,
}

describe("SchemaEvolutionPanel — Apply migration must not fail silently (F-242)", () => {
  beforeEach(() => vi.clearAllMocks())

  it("THE REGRESSION: a 403 on Apply tells the user, instead of looking like success", async () => {
    // First call = the initial fetch (succeeds, so the button renders at all).
    // Second call = the approve POST, refused by the role gate.
    mockAuthFetch
      .mockResolvedValueOnce(okJson({ schema_changes: [PENDING_CHANGE] }))
      .mockResolvedValueOnce(forbidden("insufficient workspace role"))
      .mockResolvedValue(okJson({ schema_changes: [PENDING_CHANGE] }))

    render(<SchemaEvolutionPanel pipelineId="p1" />)

    const apply = await screen.findByRole("button", { name: /apply migration/i })
    await userEvent.click(apply)

    await waitFor(() => {
      expect(toast.error).toHaveBeenCalled()
    })
  })

  it("a failed initial load is reported, not rendered as 'no schema changes'", async () => {
    mockAuthFetch.mockResolvedValue(forbidden("workspace not found"))

    render(<SchemaEvolutionPanel pipelineId="p1" />)

    // The panel renders nothing when there are genuinely no changes, which is
    // correct — but "the request failed" must not reuse those same pixels.
    await waitFor(() => {
      expect(screen.getByText(/could not load schema changes/i)).toBeInTheDocument()
    })
  })

  it("the happy path still renders the pending change and stays quiet", async () => {
    // Positive control: without this, both assertions above could pass on a
    // component that renders an error unconditionally.
    mockAuthFetch.mockResolvedValue(okJson({ schema_changes: [PENDING_CHANGE] }))

    render(<SchemaEvolutionPanel pipelineId="p1" />)

    expect(await screen.findByText("public.orders")).toBeInTheDocument()
    expect(screen.queryByText(/could not load schema changes/i)).not.toBeInTheDocument()
    expect(toast.error).not.toHaveBeenCalled()
  })
})

/**
 * The two remaining F-242 instances are POLLING BADGES, and they need the
 * opposite treatment from the action sites above: a badge that toasts every
 * 4 s because the tab lost its session would be worse than the bug. What they
 * must not do is keep *asserting* — a spinning "Running" pill is a claim that
 * the pipeline was confirmed alive just now, and after the read starts failing
 * that claim is no longer backed by anything.
 */
describe("polling badges must stop asserting once the read fails (F-242)", () => {
  beforeEach(() => vi.clearAllMocks())
  afterEach(() => vi.useRealTimers())

  /** Route by URL: the status badge also drives usePipelineRuntime. */
  function routeStatus(stateResponse: () => Response) {
    mockAuthFetch.mockImplementation(async (url: string) => {
      if (String(url).includes("/runtime")) return okJson({ pipeline_id: "p1", mode: "batch" })
      return stateResponse()
    })
  }

  it("THE REGRESSION: a status badge whose refresh starts 403-ing must not keep claiming 'Running'", async () => {
    let failing = false
    routeStatus(() => (failing ? forbidden("session expired") : okJson({ status: "running" })))

    // Fake timers must be installed BEFORE render: the badge registers its 4 s
    // `window.setInterval` on mount, and switching to fake timers afterwards
    // does not take over an already-scheduled real one — the poll then never
    // fires and the test fails for a reason that has nothing to do with the bug.
    // For the same reason `findBy*`/`waitFor` are avoided from here on: they
    // schedule their own timers and would never resolve.
    vi.useFakeTimers()
    render(<PipelineExecutionStatusBadge pipelineId="p1" />)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0)
    })
    expect(screen.getByText("Running")).toBeInTheDocument()

    // Now the reads start failing.
    failing = true
    await act(async () => {
      await vi.advanceTimersByTimeAsync(4_100)
    })

    // Not asserting "the pipeline stopped" — only that the pill no longer
    // presents a stale value as a live one.
    expect(screen.getByTitle(/out of date/i)).toBeInTheDocument()
  })

  it("positive control: while the reads succeed the badge carries no staleness marker", async () => {
    routeStatus(() => okJson({ status: "running" }))

    render(<PipelineExecutionStatusBadge pipelineId="p1" />)

    expect(await screen.findByText("Running")).toBeInTheDocument()
    expect(screen.queryByTitle(/out of date/i)).not.toBeInTheDocument()
  })

  it("THE REGRESSION: a drift badge that could not read renders 'unknown', not 'nothing pending'", async () => {
    // Rendering nothing is the badge's own signal for "no drift awaiting
    // approval". Reusing those exact pixels for "the request was refused"
    // tells the operator there is nothing to review.
    mockAuthFetch.mockResolvedValue(forbidden("insufficient workspace role"))

    render(<SchemaDriftBadge pipelineId="p1" />)

    await waitFor(() => {
      expect(screen.getByText(/drift status unavailable/i)).toBeInTheDocument()
    })
  })

  it("positive control: the drift badge stays invisible when there is genuinely nothing pending", async () => {
    mockAuthFetch.mockResolvedValue(okJson({ schema_changes: [] }))

    const { container } = render(<SchemaDriftBadge pipelineId="p1" />)

    await waitFor(() => {
      expect(container).toBeEmptyDOMElement()
    })
  })
})
