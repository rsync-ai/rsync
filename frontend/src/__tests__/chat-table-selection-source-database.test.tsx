import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import type { Mock } from "vitest"

import { AgenticChatInterfaceV2 } from "@/components/chat/AgenticChatInterfaceV2"
import { authFetch } from "@/lib/api/auth-fetch"
import { sendChatMessage } from "@/lib/api/chat"
import { getPipeline } from "@/lib/api/pipelines"
import { primeNamespaceModels } from "@/lib/pipeline/namespaceModel"
import { repoNamespaceModels } from "@/lib/pipeline/__tests__/repoNamespaceModels"

// #14: the table picker must say which database it lists. The backend names it in
// the table-selection pause (blocking_reason.details.source_database). This renders
// the real chat, its HITL hook and the real picker, and checks the name the
// backend sent reaches the picker's header. A PostgreSQL source is used on
// purpose: its tables carry the PG schema ("public"), so the database name can
// only come from source_database, never from the tables.
//
// Only the network, the socket, and child panels that play no part are stubbed.

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
    <button type="button" onClick={() => onSubmit("Move my billing tables to cloud storage")}>
      Start from home
    </button>
  ),
}))
vi.mock("@/components/chat/ChatMessageItem", () => ({
  ChatMessageItem: ({ message }: { message: { role: string; content: string } }) => (
    <div data-testid={`msg-${message.role}`}>{message.content}</div>
  ),
}))
vi.mock("@/components/chat/PipelineAccordionView", () => ({ PipelineAccordionView: () => <div /> }))
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

const PIPELINE_ID = "3c9f1d2e-0000-4000-8000-000000000014"

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  })
}

// A stand-in for the gateway: /state reports the table-selection pause with the
// given details, and the source connection answers with its display name.
function installBackend(details: Record<string, unknown>) {
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    const u = String(url)
    if (u.endsWith(`/pipelines/${PIPELINE_ID}/state`)) {
      return json(200, {
        pipeline_id: PIPELINE_ID,
        execution_id: "exec-14",
        status: "waiting_for_user",
        current_stage: "execution",
        blocking_reason: {
          type: "table_selection",
          description: "Select tables to continue",
          details,
        },
      })
    }
    if (/\/api\/v1\/connections\/src-14$/.test(u)) {
      return json(200, { id: "src-14", name: "Billing replica", type: "source", connector_type: "postgresql" })
    }
    return json(404, { error: "not found" })
  })
}

const pauseDetails = {
  available_tables: [
    { name: "invoices", schema: "public", row_count: 10 },
    { name: "payments", schema: "public", row_count: 4 },
  ],
  suggested_tables: [],
  source_type: "postgresql",
  source_database: "billing",
  source_connection_id: "src-14",
}

async function openPickerFromChat() {
  ;(sendChatMessage as Mock).mockResolvedValue({
    type: "pipeline_started",
    message: "Setting up your pipeline.",
    data: { pipeline_id: PIPELINE_ID, execution_id: "exec-14" },
    metadata: {},
  })
  const user = userEvent.setup({ pointerEventsCheck: 0 })
  render(<AgenticChatInterfaceV2 />)
  await user.click(screen.getByRole("button", { name: "Start from home" }))
  return screen.findByRole("dialog", {}, { timeout: 5000 })
}

describe("chat: the table picker names the source database", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    try {
      window.localStorage.clear()
    } catch {
      // storage unavailable: nothing persisted to clear
    }
  })

  it("shows the database the pause named, with the connection and source type", async () => {
    installBackend(pauseDetails)
    const dialog = await openPickerFromChat()

    const heading = await within(dialog).findByTestId("table-source-heading")
    expect(heading).toHaveTextContent("Tables in billing")
    await waitFor(() =>
      expect(within(dialog).getByTestId("table-source-connection")).toHaveTextContent(
        "Connection: Billing replica · PostgreSQL"
      )
    )
    // Control: the picker is showing this pause's tables.
    expect(within(dialog).getByTestId("table-source-header")).toHaveTextContent("2 tables found")
  })

  it("without a database in the pause, falls back to the tables' schema", async () => {
    installBackend({ ...pauseDetails, source_database: undefined })
    const dialog = await openPickerFromChat()

    const heading = await within(dialog).findByTestId("table-source-heading")
    expect(heading).toHaveTextContent("Tables in schema public")
    expect(heading).not.toHaveTextContent("billing")
  })
})

// A server-level source (a connection naming no database) mirrors each source
// database at the destination even when one database is picked. The pause says
// so (source_server_level), and the picker must not demand a destination name
// the executor would not use.
describe("chat: a server-level source leaves the destination name optional", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    try {
      window.localStorage.clear()
    } catch {
      // storage unavailable: nothing persisted to clear
    }
    primeNamespaceModels(repoNamespaceModels())
    ;(getPipeline as Mock).mockResolvedValue({
      destination_config: { namespace: "public", namespace_kind: "schema", create_if_not_exists: true },
      destination_connection: { connector_type: "postgresql" },
    })
  })

  const serverPause = {
    ...pauseDetails,
    available_tables: [{ name: "orders", schema: "shop", row_count: 10 }],
    source_type: "mysql",
    source_database: undefined,
  }

  it("blanks the seeded name when the pause says the source is server-level", async () => {
    installBackend({ ...serverPause, source_server_level: true })
    const dialog = await openPickerFromChat()
    await waitFor(() => expect(within(dialog).getByLabelText(/Schema name/i)).toHaveValue(""))
    expect(within(dialog).getByText(/\(optional\)/i)).toBeInTheDocument()
  })

  it("keeps it when the pause does not (control)", async () => {
    installBackend(serverPause)
    const dialog = await openPickerFromChat()
    await waitFor(() => expect(within(dialog).getByLabelText(/Schema name/i)).toHaveValue("public"))
    expect(within(dialog).queryByText(/\(optional\)/i)).toBeNull()
  })
})
