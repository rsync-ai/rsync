/**
 * Regression test for KI-CDC-CONTROL-502-BODY-NOT-IN-TOAST.
 *
 * Found on live prod 2026-07-30: pressing Pause on a CDC pipeline whose Kafka
 * Connect connector had gone missing produced a toast reading exactly
 * "Request failed". The orchestrator had answered with a perfectly good reason —
 * "kafka connect refused the pause of cdc-a9d7f773 (HTTP 404)" — and the operator
 * never saw a word of it.
 *
 * ROOT CAUSE, and the reason these tests are built on REAL `Response` objects:
 * the old per-component helper did `await res.json().catch(() => null)` and then
 * fell back to `await res.text().catch(() => "")`. Per the Fetch spec,
 * `Response.json()` marks the body disturbed BEFORE parsing, so that `text()`
 * fallback always rejects with "Body has already been read"; combined with an
 * empty `statusText` on HTTP/2 the expression collapsed to the bare literal.
 *
 * A hand-rolled `{ json, text }` object mock — the style used by the sibling
 * suite cdc-pipeline-actions-pause.test.tsx — CANNOT reproduce that: nothing
 * marks its body disturbed, so its `text()` happily answers and the tests pass
 * against the broken code. Every case below therefore constructs a genuine
 * `new Response(...)`. That is what makes this suite able to fail.
 *
 * Verified RED against the pre-fix components: both Layer-B cases fail with
 * `toast.error` called with exactly "Request failed".
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import "@testing-library/jest-dom"

import { readResponseErrorMessage } from "@/lib/utils/error-handling"

// ---------------------------------------------------------------------------
// Layer A — the shared helper, over real Response bodies.
// ---------------------------------------------------------------------------

describe("readResponseErrorMessage (KI-CDC-CONTROL-502-BODY-NOT-IN-TOAST)", () => {
  const orchestratorReason = "kafka connect refused the pause of cdc-a9d7f773 (HTTP 404)"

  it("THE REGRESSION: surfaces the orchestrator's `error` field from a JSON 502", async () => {
    const res = new Response(
      JSON.stringify({
        success: false,
        pipeline_id: "d13898e0-094a-422c-ab62-8e752235957f",
        connector_name: "cdc-a9d7f773",
        error: orchestratorReason,
        result: { status_code: 404 },
      }),
      { status: 502 }
    )
    expect(await readResponseErrorMessage(res, "Pause")).toBe(orchestratorReason)
  })

  it("an EMPTY 502 body still names the status — never the bare literal", async () => {
    const msg = await readResponseErrorMessage(new Response("", { status: 502 }), "Pause")
    expect(msg).toContain("502")
    expect(msg).not.toBe("Request failed")
  })

  it("a NON-JSON 502 body reaches the caller (the fallback that was dead before)", async () => {
    const res = new Response("upstream connect error", { status: 502 })
    expect(await readResponseErrorMessage(res, "Pause")).toBe("upstream connect error")
  })

  it("JSON with no human-readable field falls through to the status line, not a blob", async () => {
    const res = new Response(JSON.stringify({ success: false }), { status: 502 })
    const msg = await readResponseErrorMessage(res, "Pause")
    expect(msg).toBe("Pause failed (HTTP 502)")
    expect(msg).not.toContain("{")
  })

  it("`message` wins over `error` when both are present", async () => {
    const res = new Response(JSON.stringify({ message: "Connector is already paused", error: "conflict" }), {
      status: 409,
    })
    expect(await readResponseErrorMessage(res, "Pause")).toBe("Connector is already paused")
  })

  it("without an action the last resort is still status-bearing", async () => {
    const msg = await readResponseErrorMessage(new Response("", { status: 500 }))
    expect(msg).toBe("Request failed (HTTP 500)")
  })

  it("a huge plain-text body is capped so it fits a toast", async () => {
    const res = new Response("x".repeat(5000), { status: 502 })
    const msg = await readResponseErrorMessage(res, "Pause")
    expect(msg.length).toBeLessThanOrEqual(301)
    expect(msg.endsWith("…")).toBe(true)
  })

  it("PROOF the old ordering was broken: json() disturbs the body, text() then throws", async () => {
    // This is the control for the whole suite. If this ever stops throwing, the
    // platform changed and the reasoning above needs revisiting.
    const res = new Response("plain text", { status: 502 })
    expect(await res.json().catch(() => null)).toBeNull()
    await expect(res.text()).rejects.toThrow()
    expect(res.statusText).toBe("")
  })
})

// ---------------------------------------------------------------------------
// Layer B — the component the KI was filed against.
// ---------------------------------------------------------------------------

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ refresh: vi.fn(), push: vi.fn() }),
}))
vi.mock("@/lib/hooks/usePipelineRuntime", () => ({ usePipelineRuntime: vi.fn() }))
vi.mock("@/lib/api/pipelines", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api/pipelines")>()
  return {
    ...actual,
    getPipelineCDCStatus: vi.fn(async () => ({ recovery_enabled: false })),
    executePipelineWithRunMode: vi.fn(),
    recoverPipelineCDC: vi.fn(),
    restartPipelineCDC: vi.fn(),
  }
})

import { toast } from "sonner"
import { authFetch } from "@/lib/api/auth-fetch"
import { usePipelineRuntime } from "@/lib/hooks/usePipelineRuntime"
import { CDCPipelineActions } from "@/components/pipeline/CDCPipelineActions"

const mockAuthFetch = vi.mocked(authFetch)
const mockRuntime = vi.mocked(usePipelineRuntime)
const mockToastError = vi.mocked(toast.error)

const PIPELINE_ID = "d13898e0-094a-422c-ab62-8e752235957f"

/**
 * /state answers "running" so Pause renders; the Pause POST answers with
 * `pauseResponse` — a REAL Response, which is the whole point.
 */
function wireFetch(pauseResponse: () => Response) {
  mockAuthFetch.mockImplementation(async (url: string) => {
    if (String(url).includes("/cdc/pause")) return pauseResponse()
    return { ok: true, json: async () => ({ status: "running" }) } as unknown as Response
  })
}

async function clickPause() {
  render(<CDCPipelineActions pipelineId={PIPELINE_ID} pipelineName="cdc test" status="running" />)
  const pause = await screen.findByRole("button", { name: /pause/i })
  await userEvent.click(pause)
  await waitFor(() => expect(mockToastError).toHaveBeenCalled())
  return String(mockToastError.mock.calls[0][0])
}

describe("CDCPipelineActions — the Pause toast carries the reason (KI-CDC-CONTROL-502-BODY-NOT-IN-TOAST)", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockRuntime.mockReturnValue({ runtime: { phase: "running" } } as unknown as ReturnType<typeof usePipelineRuntime>)
  })

  it("THE REGRESSION: an EMPTY 502 no longer collapses to \"Request failed\"", async () => {
    wireFetch(() => new Response("", { status: 502 }))
    const shown = await clickPause()
    expect(shown).not.toBe("Request failed")
    expect(shown).toContain("502")
  })

  it("a plain-text 502 body reaches the toast verbatim", async () => {
    wireFetch(() => new Response("upstream connect error", { status: 502 }))
    expect(await clickPause()).toBe("upstream connect error")
  })

  it("the orchestrator's JSON reason reaches the toast (the prod capture)", async () => {
    const reason = "kafka connect refused the pause of cdc-a9d7f773 (HTTP 404)"
    wireFetch(() => new Response(JSON.stringify({ success: false, error: reason }), { status: 502 }))
    expect(await clickPause()).toBe(reason)
  })
})
