import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import type { Mock } from "vitest"

import { AgenticChatInterfaceV2 } from "@/components/chat/AgenticChatInterfaceV2"
import { PipelineTableSelector } from "@/components/pipeline/PipelineTableSelector"
import { authFetch } from "@/lib/api/auth-fetch"
import { sendChatMessage } from "@/lib/api/chat"

// #16: pressing Cancel in the table picker while a pipeline was being set up only
// hid the dialog. Nothing told the backend, so the run stayed parked on the
// selection, /state kept answering waiting_for_user, the composer stayed disabled
// and the picker would not come back. The chat was stuck.
//
// These render the real chat and the real picker. Only the network, the socket,
// and child panels that play no part in the flow are stubbed.

// The chat's own record of the run (its reducer state) has no label of its own on
// screen: "completed" and "cancelled" both show "Start new pipeline". The real hook
// and reducer run unchanged; this only reads back the state after each render.
// awaitingInput is whether the chat still holds the park's request for input: the
// HITL actions refuse to resume a run without one (useHITLState.ts, handleConnectorGeneration).
const chatRun = vi.hoisted(() => ({ executionState: "", awaitingInput: false }))
vi.mock("@/lib/pipeline/usePipelineState", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/pipeline/usePipelineState")>()
  return {
    ...actual,
    usePipelineState: () => {
      const hook = actual.usePipelineState()
      chatRun.executionState = hook.state.executionState
      chatRun.awaitingInput = hook.state.hitl !== null
      return hook
    },
  }
})
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("@/lib/api/chat", () => ({
  sendChatMessage: vi.fn(),
  resetSessionId: vi.fn(() => "session-reset"),
}))
vi.mock("@/lib/api/pipelines", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@/lib/api/pipelines")>()
  return { ...actual, getPipeline: vi.fn(async () => ({})) }
})
vi.mock("@/contexts/WebSocketContext", () => ({
  useWebSocket: () => ({ subscribe: () => () => {} }),
}))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/chat",
  useSearchParams: () => new URLSearchParams(),
}))
vi.mock("@/components/chat/AgenticPipelineHome", () => ({
  AgenticPipelineHome: ({ onSubmit }: { onSubmit: (p: string) => void }) => (
    <button type="button" onClick={() => onSubmit("Move my orders collection to cloud storage")}>
      Start from home
    </button>
  ),
}))
vi.mock("@/components/chat/ChatMessageItem", () => ({
  ChatMessageItem: ({ message }: { message: { role: string; content: string } }) => (
    <div data-testid={`msg-${message.role}`}>{message.content}</div>
  ),
}))
// The card is stubbed down to its Cancel button, which is all the chat hands it here.
vi.mock("@/components/chat/PipelineAccordionView", () => ({
  PipelineAccordionView: ({ onCancel }: { onCancel?: () => void }) => (
    <button type="button" onClick={() => onCancel?.()}>
      Cancel Pipeline
    </button>
  ),
}))
vi.mock("@/components/chat/ChatRightPanel", () => ({ ChatRightPanel: () => null }))
vi.mock("@/components/chat/ActivePipelinesList", () => ({ ActivePipelinesList: () => null }))
vi.mock("@/components/chat/SuggestionsReviewDialog", () => ({ SuggestionsReviewDialog: () => null }))
vi.mock("@/components/pipeline/PipelineMonitoringPanel", () => ({ PipelineMonitoringPanel: () => null }))
vi.mock("@/components/pipeline/PipelineConnectionSelector", () => ({
  PipelineConnectionSelector: () => null,
}))

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

const PIPELINE_ID = "3c9f1d2e-0000-4000-8000-000000000016"

type StopReply = () => Promise<Response>

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  })
}

// What a proxy in front of the gateway answers when the gateway is down: HTML, no JSON.
function proxyErrorPage(): Response {
  return new Response(
    "<html>\r\n<head><title>502 Bad Gateway</title></head>\r\n<body>\r\n<center><h1>502 Bad Gateway</h1></center>\r\n</body>\r\n</html>\r\n",
    { status: 502, statusText: "Bad Gateway", headers: { "Content-Type": "text/html" } }
  )
}

// A stand-in for the gateway: /state reports the table-selection park until a stop
// succeeds, and then reports the pipeline stopped, as the real handler does.
// holdParkedState keeps /state on the park after the stop, like a read that raced
// it. stopElsewhere stops the pipeline without this chat (the pipeline page, another tab).
// parked: false reports a run that is past setup and syncing, with no picker.
function installBackend(stopReply: StopReply, { holdParkedState = false, parked = true } = {}) {
  let stopped = false
  let stateReadsAfterStop = 0
  const stopCalls: { url: string; method: string }[] = []
  ;(authFetch as Mock).mockImplementation(async (url: string, init?: RequestInit) => {
    const u = String(url)
    if (u.endsWith(`/pipelines/${PIPELINE_ID}/stop`)) {
      stopCalls.push({ url: u, method: String(init?.method || "GET") })
      const res = await stopReply()
      if (res.ok) stopped = true
      return res
    }
    if (u.endsWith(`/pipelines/${PIPELINE_ID}/state`)) {
      if (stopped) stateReadsAfterStop++
      if (stopped && !holdParkedState) {
        return json(200, { pipeline_id: PIPELINE_ID, status: "stopped", current_stage: "execution" })
      }
      if (!parked) {
        return json(200, {
          pipeline_id: PIPELINE_ID,
          execution_id: "exec-16",
          status: "processing",
          current_stage: "executor",
          message: "Syncing orders",
        })
      }
      return json(200, {
        pipeline_id: PIPELINE_ID,
        execution_id: "exec-16",
        status: "waiting_for_user",
        current_stage: "execution",
        blocking_reason: {
          type: "table_selection",
          description: "Select tables to continue",
          details: {
            available_tables: [
              { name: "orders", row_count: 10 },
              { name: "customers", row_count: 4 },
            ],
            suggested_tables: [],
            source_connection_id: "src-16",
          },
        },
      })
    }
    return json(404, { error: "not found" })
  })
  return {
    stopCalls,
    stateReadsAfterStop: () => stateReadsAfterStop,
    stopElsewhere: () => {
      stopped = true
    },
  }
}

async function openPickerFromChat() {
  ;(sendChatMessage as Mock).mockResolvedValue({
    type: "pipeline_started",
    message: "Setting up your pipeline.",
    data: { pipeline_id: PIPELINE_ID, execution_id: "exec-16" },
    metadata: {},
  })
  const user = userEvent.setup({ pointerEventsCheck: 0 })
  render(<AgenticChatInterfaceV2 />)
  await user.click(screen.getByRole("button", { name: "Start from home" }))
  const dialog = await screen.findByRole("dialog", {}, { timeout: 5000 })
  // The picker is the one this test is about, with the parked run's tables in it.
  await within(dialog).findByText("orders")
  return { user, dialog }
}

describe("chat: Cancel in the table picker during pipeline setup", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    try {
      window.localStorage.clear()
    } catch {
      // storage unavailable: nothing persisted to clear
    }
  })
  afterEach(() => {
    vi.clearAllMocks()
  })

  it("stops the pipeline, says setup was cancelled and leaves the chat ready for a new pipeline", async () => {
    const { stopCalls } = installBackend(async () =>
      json(200, { message: "Pipeline stopped", pipeline_id: PIPELINE_ID, status: "stopped" })
    )
    const { user, dialog } = await openPickerFromChat()

    // While parked, the composer is locked: this is the state the user was stuck in.
    expect(screen.getByPlaceholderText(/Describe what data you want to move/i)).toBeDisabled()
    expect(stopCalls).toHaveLength(0)

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))

    await waitFor(() => expect(stopCalls).toHaveLength(1))
    expect(stopCalls[0].method).toBe("POST")
    expect(stopCalls[0].url).toContain(`/api/v1/pipelines/${PIPELINE_ID}/stop`)

    expect(await screen.findByText(/Pipeline setup cancelled/i)).toBeInTheDocument()
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument())
    expect(screen.queryByText(/Unable to connect/i)).not.toBeInTheDocument()

    // Usable for the next request: the composer offers a new pipeline, and taking
    // it brings the chat back to the start.
    const startNew = await screen.findByRole("button", { name: "Start new pipeline" })
    await user.click(startNew)
    expect(await screen.findByRole("button", { name: "Start from home" })).toBeInTheDocument()
  })

  it("ends the chat's run as cancelled on the stop's answer, while /state still reports the park", async () => {
    const backend = installBackend(
      async () => json(200, { message: "Pipeline stopped", pipeline_id: PIPELINE_ID, status: "stopped" }),
      { holdParkedState: true }
    )
    const { user, dialog } = await openPickerFromChat()
    expect(chatRun.executionState).toBe("waiting_for_user")
    expect(chatRun.awaitingInput).toBe(true)

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))
    expect(await screen.findByText(/Pipeline setup cancelled/i)).toBeInTheDocument()

    // Two /state reads after the stop, both still naming the park: by the second,
    // the chat has fully handled the first.
    await waitFor(() => expect(backend.stateReadsAfterStop()).toBeGreaterThan(1), { timeout: 5000 })

    // The run is over anyway: not parked again, picker not reopened, no request for
    // input left open, and recorded as cancelled rather than completed.
    expect(screen.getByRole("button", { name: "Start new pipeline" })).toBeInTheDocument()
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument()
    expect(chatRun.awaitingInput).toBe(false)
    expect(chatRun.executionState).toBe("cancelled")
  })

  it("frees the chat when the parked pipeline is stopped somewhere else", async () => {
    const backend = installBackend(async () => json(500, { error: "not expected in this test" }))
    const { user } = await openPickerFromChat()

    // Dismissing the picker only hides it: the run is still parked and the composer locked.
    await user.keyboard("{Escape}")
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument())
    expect(screen.getByPlaceholderText(/Describe what data you want to move/i)).toBeDisabled()
    expect(chatRun.executionState).toBe("waiting_for_user")
    expect(chatRun.awaitingInput).toBe(true)

    // Stopped from the pipeline page or another tab: only /state tells this chat.
    backend.stopElsewhere()

    expect(
      await screen.findByRole("button", { name: "Start new pipeline" }, { timeout: 5000 })
    ).toBeInTheDocument()
    expect(backend.stateReadsAfterStop()).toBeGreaterThan(0)
    expect(chatRun.executionState).toBe("cancelled")
    await waitFor(() => expect(chatRun.awaitingInput).toBe(false))
    expect(backend.stopCalls).toHaveLength(0)
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument()
  })

  it("keeps the picker open with a plain reason when the stop is refused", async () => {
    const { stopCalls } = installBackend(async () =>
      json(500, { error: "Failed to stop pipeline" })
    )
    const { user, dialog } = await openPickerFromChat()

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))

    await waitFor(() => expect(stopCalls).toHaveLength(1))
    expect(
      await within(dialog).findByText(
        "Could not cancel pipeline setup: Failed to stop pipeline. Try again, or open the pipeline to check its state."
      )
    ).toBeInTheDocument()
    expect(screen.getByRole("dialog")).toBeInTheDocument()
    expect(screen.queryByText(/Pipeline setup cancelled/i)).not.toBeInTheDocument()
    expect(screen.queryByRole("button", { name: "Start new pipeline" })).not.toBeInTheDocument()
    // The button is usable again for a retry.
    expect(within(dialog).getByRole("button", { name: "Cancel" })).toBeEnabled()
  })

  it("keeps the picker open when a proxy answers the stop with an HTML error page", async () => {
    const { stopCalls } = installBackend(async () => proxyErrorPage())
    const { user, dialog } = await openPickerFromChat()

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))

    await waitFor(() => expect(stopCalls).toHaveLength(1))
    expect(
      await within(dialog).findByText(
        "Could not cancel pipeline setup: Cancel failed: Bad Gateway (HTTP 502). Try again, or open the pipeline to check its state."
      )
    ).toBeInTheDocument()
    expect(screen.getByRole("dialog")).toBeInTheDocument()
    expect(screen.queryByText(/<html>|<h1>/)).not.toBeInTheDocument()
    expect(screen.queryByText(/Pipeline setup cancelled/i)).not.toBeInTheDocument()
    expect(chatRun.executionState).toBe("waiting_for_user")
  })

  it("says the server could not be reached, not 'Unable to connect', when the stop request never lands", async () => {
    const { stopCalls } = installBackend(async () => {
      throw new TypeError("Failed to fetch")
    })
    const { user, dialog } = await openPickerFromChat()

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))

    await waitFor(() => expect(stopCalls).toHaveLength(1))
    expect(
      await within(dialog).findByText(
        "Could not reach the server to cancel pipeline setup. Check your connection and try again."
      )
    ).toBeInTheDocument()
    expect(screen.queryByText(/Pipeline setup cancelled/i)).not.toBeInTheDocument()
  })

  it("treats a pipeline that is already stopped as cancelled", async () => {
    const { stopCalls, stopElsewhere } = installBackend(async () =>
      json(400, { error: "Pipeline is not running", current_status: "stopped" })
    )
    const { user, dialog } = await openPickerFromChat()

    // Stopped in another tab just before this Cancel, so the gateway refuses the stop.
    stopElsewhere()
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))

    await waitFor(() => expect(stopCalls).toHaveLength(1))
    expect(await screen.findByText(/Pipeline setup cancelled/i)).toBeInTheDocument()
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument())
    expect(await screen.findByRole("button", { name: "Start new pipeline" })).toBeInTheDocument()
  })

  // A paused run is still parked on the selection (the gateway refuses to stop
  // anything but running/pending), so saying "cancelled" there would be false.
  it.each(["completed", "paused"])(
    "control: a refusal naming status %s (not stopped) is not treated as cancelled",
    async (currentStatus) => {
      const { stopCalls } = installBackend(async () =>
        json(400, { error: "Pipeline is not running", current_status: currentStatus })
      )
      const { user, dialog } = await openPickerFromChat()

      await user.click(within(dialog).getByRole("button", { name: "Cancel" }))

      await waitFor(() => expect(stopCalls).toHaveLength(1))
      expect(await within(dialog).findByText(/Could not cancel pipeline setup: Pipeline is not running\./)).toBeInTheDocument()
      expect(screen.getByRole("dialog")).toBeInTheDocument()
      expect(screen.queryByText(/Pipeline setup cancelled/i)).not.toBeInTheDocument()
      expect(screen.queryByRole("button", { name: "Start new pipeline" })).not.toBeInTheDocument()
    }
  )

  it("shows the cancel in flight and locks both buttons until the stop answers", async () => {
    let answerStop: (res: Response) => void = () => {}
    const { stopCalls } = installBackend(
      () => new Promise<Response>((resolve) => {
        answerStop = resolve
      })
    )
    const { user, dialog } = await openPickerFromChat()

    // Pick a table and name the destination so Confirm is enabled: only the cancel
    // may lock it below.
    await user.click(within(dialog).getByRole("checkbox", { name: /^orders/ }))
    const destinationName = within(dialog).getByRole("textbox", { name: / name\b/i })
    await user.clear(destinationName)
    await user.type(destinationName, "analytics")
    const confirm = within(dialog).getByRole("button", { name: "Sync 1 table" })
    expect(confirm).toBeEnabled()

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))
    await waitFor(() => expect(stopCalls).toHaveLength(1))

    // The stop is still unanswered.
    const inFlight = await within(dialog).findByRole("button", { name: "Cancelling…" })
    expect(inFlight).toBeDisabled()
    expect(confirm).toBeDisabled()
    expect(screen.queryByText(/Pipeline setup cancelled/i)).not.toBeInTheDocument()

    answerStop(json(200, { message: "Pipeline stopped", pipeline_id: PIPELINE_ID, status: "stopped" }))

    expect(await screen.findByText(/Pipeline setup cancelled/i)).toBeInTheDocument()
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument())
    expect(stopCalls).toHaveLength(1)
  })

  it("clears the earlier refusal as soon as a retry starts", async () => {
    const refusal =
      "Could not cancel pipeline setup: Failed to stop pipeline. Try again, or open the pipeline to check its state."
    let answerRetry: (res: Response) => void = () => {}
    const replies: StopReply[] = [
      async () => json(500, { error: "Failed to stop pipeline" }),
      () => new Promise<Response>((resolve) => {
        answerRetry = resolve
      }),
    ]
    let replyIndex = 0
    const { stopCalls } = installBackend(() => replies[replyIndex++]())
    const { user, dialog } = await openPickerFromChat()

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))
    expect(await within(dialog).findByText(refusal)).toBeInTheDocument()

    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))
    await waitFor(() => expect(stopCalls).toHaveLength(2))

    // While the retry is unanswered the old reason is gone, not shown next to "Cancelling…".
    expect(await within(dialog).findByRole("button", { name: "Cancelling…" })).toBeDisabled()
    expect(within(dialog).queryByText(refusal)).not.toBeInTheDocument()

    answerRetry(json(200, { message: "Pipeline stopped", pipeline_id: PIPELINE_ID, status: "stopped" }))
    expect(await screen.findByText(/Pipeline setup cancelled/i)).toBeInTheDocument()
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument())
  })
})

// The Cancel on the pipeline card discarded the stop's answer, so a refused stop
// looked exactly like one that worked: nothing was said, and the run kept going.
describe("chat: Cancel on the pipeline card", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    try {
      window.localStorage.clear()
    } catch {
      // storage unavailable: nothing persisted to clear
    }
  })

  async function openCardFromChat() {
    ;(sendChatMessage as Mock).mockResolvedValue({
      type: "pipeline_started",
      message: "Setting up your pipeline.",
      data: { pipeline_id: PIPELINE_ID, execution_id: "exec-16" },
      metadata: {},
    })
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(<AgenticChatInterfaceV2 />)
    await user.click(screen.getByRole("button", { name: "Start from home" }))
    const cancel = await screen.findByRole("button", { name: "Cancel Pipeline" }, { timeout: 5000 })
    return { user, cancel }
  }

  const COULD_NOT = /Could not (cancel|reach the server to cancel) the pipeline/

  it.each([
    [
      "a proxy's HTML error page",
      () => proxyErrorPage(),
      "Could not cancel the pipeline: Cancel failed: Bad Gateway (HTTP 502). Try again, or open the pipeline to check its state.",
    ],
    [
      "a refusal from the role gate",
      () => json(403, { error: "Your role cannot stop pipelines in this workspace" }),
      "Could not cancel the pipeline: Your role cannot stop pipelines in this workspace. Try again, or open the pipeline to check its state.",
    ],
  ])("says the stop did not happen when it is answered with %s", async (_label, reply, message) => {
    const { stopCalls } = installBackend(async () => reply(), { parked: false })
    const { user, cancel } = await openCardFromChat()

    await user.click(cancel)

    await waitFor(() => expect(stopCalls).toHaveLength(1))
    expect(stopCalls[0].method).toBe("POST")
    expect(await screen.findByText(message)).toBeInTheDocument()
    expect(screen.queryByText(/<html>|<h1>/)).not.toBeInTheDocument()
  })

  it("says the server could not be reached when the stop request never lands", async () => {
    const { stopCalls } = installBackend(
      async () => {
        throw new TypeError("Failed to fetch")
      },
      { parked: false }
    )
    const { user, cancel } = await openCardFromChat()

    await user.click(cancel)

    await waitFor(() => expect(stopCalls).toHaveLength(1))
    expect(
      await screen.findByText(
        "Could not reach the server to cancel the pipeline. Check your connection and try again."
      )
    ).toBeInTheDocument()
  })

  it.each([
    ["a stop that lands", () => json(200, { message: "Pipeline stopped", pipeline_id: PIPELINE_ID, status: "stopped" })],
    ["a pipeline that was already stopped", () => json(400, { error: "Pipeline is not running", current_status: "stopped" })],
  ])("control: %s adds no failure message", async (_label, reply) => {
    const backend = installBackend(async () => reply(), { parked: false })
    const { user, cancel } = await openCardFromChat()

    await user.click(cancel)

    await waitFor(() => expect(backend.stopCalls).toHaveLength(1))
    // The chat has read /state since the stop answered, so any message is already rendered.
    backend.stopElsewhere()
    await waitFor(() => expect(backend.stateReadsAfterStop()).toBeGreaterThan(0), { timeout: 5000 })
    expect(screen.queryByText(COULD_NOT)).not.toBeInTheDocument()
  })
})

describe("PipelineTableSelector Cancel without an onCancel handler", () => {
  it("only closes, and sends no request (other pickers keep their behaviour)", async () => {
    ;(authFetch as Mock).mockImplementation(async () => json(200, {}))
    const onClose = vi.fn()
    const user = userEvent.setup({ pointerEventsCheck: 0 })
    render(
      <PipelineTableSelector
        isOpen
        onClose={onClose}
        pipelineId={PIPELINE_ID}
        availableTables={[{ name: "orders" }] as never}
        suggestedTables={[]}
      />
    )
    const dialog = screen.getByRole("dialog")
    const callsBefore = (authFetch as Mock).mock.calls.length
    await user.click(within(dialog).getByRole("button", { name: "Cancel" }))
    expect(onClose).toHaveBeenCalledTimes(1)
    const stopCalls = (authFetch as Mock).mock.calls
      .slice(callsBefore)
      .filter(([url]) => String(url).endsWith("/stop"))
    expect(stopCalls).toHaveLength(0)
  })
})
