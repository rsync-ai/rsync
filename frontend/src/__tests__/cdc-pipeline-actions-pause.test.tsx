/**
 * Regression test for KI-CDC-PAUSE-UNREACHABLE-WHEN-IDLE.
 *
 * Found on live prod 2026-07-30: a CDC stream that was genuinely running — connector
 * RUNNING, all 309 events applied, zero lag — lost its Pause button after ~20 minutes
 * of source quiet, because `effectiveStatus` demotes running → idle on /runtime's
 * `phase: "idle"` and Pause rendered only under `isRunning`. The only remaining
 * teardown was deleting the whole pipeline.
 *
 * The invariant these tests hold: **whether Pause renders is decided by the RAW
 * /state verdict (`liveStatus`), never by the /runtime-escalated one** — while the
 * recovery cluster keeps reacting to the escalated verdict. The two are independent,
 * so in the disagreement case BOTH render.
 *
 * Verified RED against the pre-fix code (`{isRunning ? <Pause/> : …}`): the
 * "quiet source" and "runtime failed" cases fail with Pause absent.
 */

import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

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
    // One-shot flag probe; recovery stays hidden, which is the prod default
    // (CDC_RECOVERY_ENABLED is OFF).
    getPipelineCDCStatus: vi.fn(async () => ({ recovery_enabled: false })),
    executePipelineWithRunMode: vi.fn(),
    recoverPipelineCDC: vi.fn(),
    restartPipelineCDC: vi.fn(),
  }
})

import { authFetch } from "@/lib/api/auth-fetch"
import { usePipelineRuntime } from "@/lib/hooks/usePipelineRuntime"
import { CDCPipelineActions } from "@/components/pipeline/CDCPipelineActions"

const mockAuthFetch = vi.mocked(authFetch)
const mockRuntime = vi.mocked(usePipelineRuntime)

/** /state answers with `status`; that is the raw verdict Pause must follow. */
function stateSays(status: string) {
  mockAuthFetch.mockResolvedValue({
    ok: true,
    json: async () => ({ status }),
  } as unknown as Response)
}

/** /runtime answers with `phase`; that is the escalation the recovery cluster follows. */
function runtimeSays(phase: string | undefined) {
  mockRuntime.mockReturnValue({
    runtime: phase ? { phase } : null,
  } as unknown as ReturnType<typeof usePipelineRuntime>)
}

/**
 * `status` is the server-rendered seed for `liveStatus`; /state then reconciles it.
 * Seeding it with the same value the mocked /state returns keeps each case at a
 * single steady state instead of racing the first poll.
 */
function renderActions(status: string) {
  return render(
    <CDCPipelineActions pipelineId="d13898e0-094a-422c-ab62-8e752235957f" pipelineName="cdc test" status={status} />
  )
}

describe("CDCPipelineActions — Pause availability (KI-CDC-PAUSE-UNREACHABLE-WHEN-IDLE)", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("THE REGRESSION: a running stream over a QUIET source still offers Pause", async () => {
    // Exactly the prod state: /state says running, /runtime says idle because no
    // events arrived recently. A quiet stream is not a dead stream.
    stateSays("running")
    runtimeSays("idle")
    renderActions("running")

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /pause/i })).toBeInTheDocument()
    })
  })

  it("offers the recovery cluster ALONGSIDE Pause when /state and /runtime disagree", async () => {
    // The escalation still does its job — the user is offered recovery — but it no
    // longer costs them the ability to stop the stream. Both answers, honestly.
    stateSays("running")
    runtimeSays("idle")
    renderActions("running")

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /pause/i })).toBeInTheDocument()
    })
    expect(screen.getByRole("button", { name: /restart cdc/i })).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /reload/i })).toBeInTheDocument()
    // "Resume" is the recovery cluster's primary label for an idle/failed stream.
    expect(screen.getByRole("button", { name: /^resume$/i })).toBeInTheDocument()
  })

  it("still offers Pause when /runtime reports failed dependencies on a running stream", async () => {
    // The same argument: a stream whose dependencies look unhealthy is exactly the
    // one an operator most wants to be able to stop.
    stateSays("running")
    runtimeSays("failed")
    renderActions("running")

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /pause/i })).toBeInTheDocument()
    })
    expect(screen.getByRole("button", { name: /restart cdc/i })).toBeInTheDocument()
  })

  it("healthy running stream is UNCHANGED: Pause only, no recovery cluster", async () => {
    stateSays("running")
    runtimeSays("running")
    renderActions("running")

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /pause/i })).toBeInTheDocument()
    })
    expect(screen.queryByRole("button", { name: /restart cdc/i })).not.toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /reload/i })).not.toBeInTheDocument()
  })

  it("a PAUSED pipeline is UNCHANGED: Resume, and no Pause", async () => {
    stateSays("paused")
    runtimeSays("idle")
    renderActions("paused")

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /^resume$/i })).toBeInTheDocument()
    })
    expect(screen.queryByRole("button", { name: /pause/i })).not.toBeInTheDocument()
  })

  it("a genuinely FAILED pipeline is UNCHANGED: recovery cluster, and no Pause", async () => {
    // /state itself says failed — not an escalation — so there is nothing to pause.
    stateSays("failed")
    runtimeSays("failed")
    renderActions("failed")

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /restart cdc/i })).toBeInTheDocument()
    })
    expect(screen.queryByRole("button", { name: /pause/i })).not.toBeInTheDocument()
  })

  it("a pristine draft is UNCHANGED: Start only", async () => {
    stateSays("draft")
    runtimeSays(undefined)
    renderActions("draft")

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /^start$/i })).toBeInTheDocument()
    })
    expect(screen.queryByRole("button", { name: /pause/i })).not.toBeInTheDocument()
    expect(screen.queryByRole("button", { name: /restart cdc/i })).not.toBeInTheDocument()
  })
})
