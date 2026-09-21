import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { useState } from "react"

import { SuggestTablesCard, parseSuggestions, qualifiedName } from "@/components/explorer/SuggestTablesCard"
import ExplorerPage from "@/app/(dashboard)/explorer/page"
import { authFetch } from "@/lib/api/auth-fetch"

// POST /api/v1/explorer/connections/:id/tables/recommend (explorer.go) had no caller. The
// Explorer's empty state now asks what the user wants to find out and suggests tables, each
// of which can start a query or be handed to the AI prompt.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({
  toast: { success: vi.fn(), error: vi.fn(), info: vi.fn(), message: vi.fn() },
}))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn(), replace: vi.fn() }),
  usePathname: () => "/explorer",
  useSearchParams: () => new URLSearchParams(),
}))
vi.mock("next-themes", () => ({
  useTheme: () => ({ resolvedTheme: "light", setTheme: vi.fn() }),
}))
vi.mock("@/contexts/WorkspaceContext", () => ({
  useWorkspaceRole: () => ({
    role: "admin",
    isLoading: false,
    error: false,
    activeWorkspace: null,
    can: () => true,
    meets: () => true,
  }),
}))
vi.mock("@/contexts/CurrentUserContext", () => ({
  useCurrentUser: () => ({ user: { id: "u-1" } }),
}))

const mockFetch = authFetch as unknown as Mock
const CONN_ID = "11111111-1111-1111-1111-111111111111"
const RECOMMEND = `/api/v1/explorer/connections/${CONN_ID}/tables/recommend`

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

const LLM = {
  connection_id: CONN_ID,
  intent: "revenue by customer",
  recommendations: [
    {
      name: "orders",
      schema: "shop",
      row_count: 120000,
      key_columns: ["id", "customer_id", "total"],
      reason: "Holds each order with its total and customer.",
      confidence: 0.92,
      category: "transactions",
      has_pii: false,
    },
    {
      name: "customers",
      schema: "shop",
      row_count: 0,
      key_columns: null,
      reason: "Names the customer behind each order.",
      confidence: 0.81,
      category: "user_data",
      has_pii: true,
    },
  ],
  total_available: 42,
  ranking_method: "llm",
}

beforeAll(() => {
  // CodeMirror measures text through Range rects, which jsdom does not implement.
  const empty = () => Object.assign([], { item: () => null }) as unknown as DOMRectList
  Range.prototype.getClientRects ||= empty
  Range.prototype.getBoundingClientRect ||= () => new DOMRect(0, 0, 0, 0)
})

beforeEach(() => {
  mockFetch.mockReset()
})

function Harness({ onStartQuery = vi.fn() }: { onStartQuery?: (sql: string) => void }) {
  const [picked, setPicked] = useState<string[]>([])
  return (
    <SuggestTablesCard
      connectionId={CONN_ID}
      selectedTables={picked}
      tableKey={qualifiedName}
      onToggleTable={(k) => setPicked((p) => (p.includes(k) ? p.filter((x) => x !== k) : [...p, k]))}
      onStartQuery={onStartQuery}
    />
  )
}

function rowFor(name: string): HTMLElement {
  const row = document.querySelector<HTMLElement>(`[data-suggestion="${name}"]`)
  if (!row) throw new Error(`no suggestion ${name}`)
  return row
}

describe("parseSuggestions", () => {
  it("reads a Go nil slice as empty and drops nameless rows", () => {
    expect(parseSuggestions({ recommendations: null }).recommendations).toEqual([])
    const parsed = parseSuggestions({ recommendations: [{ name: "" }, { name: "t", key_columns: null }] })
    expect(parsed.recommendations).toEqual([
      { name: "t", schema: undefined, row_count: 0, key_columns: [], reason: "", category: "", has_pii: false },
    ])
  })
})

describe("SuggestTablesCard", () => {
  it("asks only when told to, sends the intent, and lists what came back", async () => {
    mockFetch.mockResolvedValue(res(200, LLM))
    render(<Harness />)
    expect(mockFetch).not.toHaveBeenCalled()

    await userEvent.type(screen.getByRole("textbox", { name: "What do you want to find out?" }), "  revenue by customer ")
    await userEvent.click(screen.getByRole("button", { name: "Suggest tables" }))

    const list = await screen.findByRole("list", { name: "Suggested tables" })
    expect(mockFetch).toHaveBeenCalledTimes(1)
    const [url, init] = mockFetch.mock.calls[0]
    expect(url).toBe(RECOMMEND)
    expect(init.method).toBe("POST")
    expect(JSON.parse(init.body)).toEqual({ intent: "revenue by customer", max_tables: 8 })

    expect([...list.querySelectorAll("[data-suggestion]")].map((r) => r.getAttribute("data-suggestion"))).toEqual([
      "shop.orders",
      "shop.customers",
    ])
    const orders = rowFor("shop.orders")
    expect(orders).toHaveTextContent(`${(120000).toLocaleString()} rows`)
    expect(orders).toHaveTextContent("Columns: id, customer_id, total")
    expect(within(orders).getByText("transactions")).toBeInTheDocument()
    expect(within(orders).queryByText("May hold personal data")).toBeNull()

    // A row count of 0 is not shown as "0 rows"; the category reads as words; PII is flagged.
    const customers = rowFor("shop.customers")
    expect(customers).not.toHaveTextContent(/rows/)
    expect(within(customers).getByText("user data")).toBeInTheDocument()
    expect(within(customers).getByText("May hold personal data")).toBeInTheDocument()
    expect(customers).not.toHaveTextContent("Columns:")

    expect(screen.getByText(/2 of 42 tables\./)).toHaveTextContent("no row values are sent")
    expect(screen.queryByText(/AI ranking was unavailable/)).toBeNull()
  })

  it("starts a query and hands a table to the AI prompt", async () => {
    mockFetch.mockResolvedValue(res(200, LLM))
    const onStartQuery = vi.fn()
    render(<Harness onStartQuery={onStartQuery} />)
    await userEvent.click(screen.getByRole("button", { name: "Suggest tables" }))
    await screen.findByRole("list", { name: "Suggested tables" })

    await userEvent.click(screen.getByRole("button", { name: "Start a query on shop.orders" }))
    expect(onStartQuery).toHaveBeenCalledWith("SELECT * FROM shop.orders LIMIT 100")

    const use = screen.getByRole("button", { name: "Use shop.customers in the AI prompt" })
    expect(use).toHaveAttribute("aria-pressed", "false")
    await userEvent.click(use)
    expect(use).toHaveAttribute("aria-pressed", "true")
    expect(use).toHaveTextContent("In the AI prompt")
    await userEvent.click(use)
    expect(use).toHaveAttribute("aria-pressed", "false")
  })

  it("says when the suggestions came from the keyword fallback", async () => {
    mockFetch.mockResolvedValue(res(200, { ...LLM, ranking_method: "heuristic" }))
    render(<Harness />)
    await userEvent.click(screen.getByRole("button", { name: "Suggest tables" }))
    expect(await screen.findByText(/AI ranking was unavailable/)).toBeInTheDocument()
    expect(screen.getByText(/2 of 42 tables\./)).toHaveTextContent("Ranked by name and size.")
  })

  it("says so when nothing stood out", async () => {
    mockFetch.mockResolvedValue(res(200, { intent: "moon phases", recommendations: null, total_available: 3, ranking_method: "llm" }))
    render(<Harness />)
    await userEvent.click(screen.getByRole("button", { name: "Suggest tables" }))
    expect(await screen.findByText(/No table stood out/)).toHaveTextContent("No table stood out for moon phases.")
  })

  it("shows the backend's reason on failure and retries", async () => {
    mockFetch.mockResolvedValueOnce(res(503, { error: "Schema discovery failed", details: "connection refused" }))
    mockFetch.mockResolvedValueOnce(res(200, LLM))
    render(<Harness />)

    await userEvent.click(screen.getByRole("button", { name: "Suggest tables" }))
    expect(await screen.findByRole("alert")).toHaveTextContent("Schema discovery failed: connection refused")
    await userEvent.click(screen.getByRole("button", { name: "Retry" }))
    await screen.findByRole("list", { name: "Suggested tables" })
    expect(screen.queryByRole("alert")).toBeNull()
  })

  it("says it could not reach the server, and waits while loading", async () => {
    let fail: (e: Error) => void = () => {}
    mockFetch.mockReturnValueOnce(new Promise((_, reject) => (fail = reject)))
    render(<Harness />)

    await userEvent.click(screen.getByRole("button", { name: "Suggest tables" }))
    expect(screen.getByRole("status")).toHaveTextContent("can take a minute")
    expect(screen.getByRole("button", { name: "Suggest tables" })).toBeDisabled()

    fail(new Error("network"))
    expect(await screen.findByRole("alert")).toHaveTextContent("Could not reach the server to suggest tables.")
  })
})

describe("Explorer empty state", () => {
  it("offers suggestions for a SQL connection, feeds the AI prompt, and steps aside for a query", async () => {
    mockFetch.mockImplementation(async (url: string) => {
      if (url.includes("/schema-index"))
        return res(200, {
          connection_id: CONN_ID,
          table_count: 2,
          tables: [
            { name: "orders", schema: "shop", columns: [] },
            { name: "customers", schema: "shop", columns: [] },
          ],
          foreign_keys: [],
        })
      if (url === RECOMMEND) return res(200, LLM)
      if (/\/api\/v1\/connections$/.test(url))
        return res(200, {
          connections: [
            {
              id: CONN_ID,
              name: "shop db",
              connector_type: "postgresql",
              type: "source",
              status: "active",
              is_connected: true,
              supports_explorer: true,
              explorer_mode: "sql",
            },
          ],
        })
      return res(200, {})
    })
    render(<ExplorerPage />)

    const card = await screen.findByTestId("suggest-tables")
    await userEvent.click(within(card).getByRole("button", { name: "Suggest tables" }))
    await within(card).findByRole("list", { name: "Suggested tables" })

    // The pick is the same one the schema browser makes: the "Selected:" chips show it.
    await userEvent.click(within(card).getByRole("button", { name: "Use shop.orders in the AI prompt" }))
    expect(screen.getByText("Selected:").parentElement).toHaveTextContent("shop.orders")

    // Starting a query fills the editor, and the empty-state card gets out of the way.
    await userEvent.click(within(card).getByRole("button", { name: "Start a query on shop.customers" }))
    await waitFor(() => expect(card.parentElement).not.toBeVisible())
    expect(document.querySelector(".cm-content")?.textContent).toBe("SELECT * FROM shop.customers LIMIT 100")
  })
})
