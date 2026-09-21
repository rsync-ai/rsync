import { beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { act, render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import ExplorerPage from "@/app/(dashboard)/explorer/page"
import { authFetch } from "@/lib/api/auth-fetch"
import { ACTIVE_WORKSPACE_EVENT } from "@/lib/workspace/active-workspace"

// The Data Explorer's header count for a document connection. When the collection
// list could not be loaded (a login the cluster refuses, a host that does not answer,
// an address not on the allowlist) the gateway answers with an error, and the page
// must say so and name what to check. It used to render "0 databases · 0 collections",
// which reads as an empty cluster.

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

const CONN_ID = "11111111-1111-1111-1111-111111111111"
const LOCKED_ID = "22222222-2222-2222-2222-222222222222"
const mockFetch = authFetch as unknown as Mock

function mongoConnection(id: string, name: string) {
  return {
    id,
    name,
    connector_type: "mongodb",
    type: "source",
    status: "active",
    is_connected: true,
    supports_explorer: true,
    explorer_mode: "document",
  }
}

function postgresConnection(id: string, name: string) {
  return {
    id,
    name,
    connector_type: "postgresql",
    type: "source",
    status: "active",
    is_connected: true,
    supports_explorer: true,
    explorer_mode: "sql",
  }
}

const ONE_COLLECTION = {
  connection_id: CONN_ID,
  table_count: 1,
  tables: [{ name: "orders", schema: "shop", columns: [] }],
  foreign_keys: [],
}

const AUTH_REFUSED = {
  error: "Failed to build schema index: could not list collections: bad auth : authentication failed",
}

const TWO_COLLECTIONS = {
  connection_id: CONN_ID,
  table_count: 2,
  tables: [
    { name: "customers", schema: "shop", columns: [] },
    { name: "orders", schema: "shop", columns: [] },
  ],
  foreign_keys: [],
}

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

function serve(
  schemaIndex: (url: string) => Response | Promise<Response>,
  connections = [mongoConnection(CONN_ID, "orders cluster")],
) {
  mockFetch.mockImplementation(async (url: string) => {
    if (url.includes("/schema-index")) return schemaIndex(url)
    if (/\/api\/v1\/connections$/.test(url)) return res(200, { connections })
    return res(200, {})
  })
}

function schemaIndexUrls(connectionId = "") {
  return mockFetch.mock.calls.map(([url]) => String(url)).filter((url) => url.includes(`${connectionId}/schema-index`))
}

function schemaIndexCalls(connectionId = "") {
  return schemaIndexUrls(connectionId).length
}

describe("Explorer header for a document connection", () => {
  beforeEach(() => {
    mockFetch.mockReset()
  })

  it("says the databases could not be loaded, and why, when the login is refused", async () => {
    serve(() =>
      res(500, {
        error: "Failed to build schema index: could not list collections: bad auth : authentication failed",
      }),
    )
    render(<ExplorerPage />)

    const label = await screen.findByTestId("schema-count-label")
    await waitFor(() => expect(label).toHaveTextContent("Could not load databases"))
    expect(schemaIndexCalls()).toBeGreaterThan(0)
    expect(document.body.textContent).not.toMatch(/\b0 databases/i)

    const alert = await screen.findByTestId("doc-collections-error")
    expect(alert).toHaveTextContent("bad auth : authentication failed")
    expect(alert).toHaveTextContent("The database rejected the login")
    expect(alert).toHaveTextContent(/username and password/)
    expect(within(alert).getByRole("button", { name: "Retry" })).toBeInTheDocument()
  })

  it("points at network access when the cluster does not answer", async () => {
    serve(() =>
      res(500, {
        error:
          "Failed to build schema index: could not list collections: cluster0-shard-00-00.example.net:27017: timed out, Timeout: 30s",
      }),
    )
    render(<ExplorerPage />)

    const alert = await screen.findByTestId("doc-collections-error")
    expect(alert).toHaveTextContent("timed out")
    expect(alert).toHaveTextContent("The database did not answer")
    expect(alert).toHaveTextContent(/IP allowlist/)
    expect(screen.getByTestId("schema-count-label")).toHaveTextContent("Could not load databases")
  })

  it("names both things to check when the reason is not recognised", async () => {
    serve(() => res(500, { error: "Failed to build schema index: could not list collections: the connector gave no reason" }))
    render(<ExplorerPage />)

    const alert = await screen.findByTestId("doc-collections-error")
    expect(alert).toHaveTextContent("the connector gave no reason")
    expect(alert).toHaveTextContent(/username and password/)
    expect(alert).toHaveTextContent(/IP allowlist/)
    expect(alert).not.toHaveTextContent("The database rejected the login")
    expect(alert).not.toHaveTextContent("The database did not answer")
  })

  // Switching back to a connection whose collections are already cached must not keep
  // the other connection's failure on screen over them.
  it("drops a failure when switching back to a connection that loaded", async () => {
    serve(
      (url) =>
        url.includes(LOCKED_ID)
          ? res(500, { error: "Failed to build schema index: could not list collections: bad auth : authentication failed" })
          : res(200, TWO_COLLECTIONS),
      [mongoConnection(CONN_ID, "a healthy cluster"), mongoConnection(LOCKED_ID, "b locked cluster")],
    )
    const user = userEvent.setup()
    render(<ExplorerPage />)

    const label = await screen.findByTestId("schema-count-label")
    await waitFor(() => expect(label).toHaveTextContent("1 databases · 2 collections"))

    const connectionPicker = () => {
      const trigger = screen.getAllByRole("combobox").find((el) => el.textContent?.includes("cluster"))
      expect(trigger).toBeDefined()
      return trigger as HTMLElement
    }
    await user.click(connectionPicker())
    await user.click(await screen.findByRole("option", { name: /b locked cluster/ }))
    await screen.findByTestId("doc-collections-error")
    expect(screen.getByTestId("schema-count-label")).toHaveTextContent("Could not load databases")
    expect(schemaIndexCalls(LOCKED_ID)).toBe(1)

    await user.click(connectionPicker())
    await user.click(await screen.findByRole("option", { name: /a healthy cluster/ }))
    await waitFor(() => expect(screen.getByTestId("schema-count-label")).toHaveTextContent("1 databases · 2 collections"))
    // Served from the page's cache: the healthy connection was fetched once, on first load.
    expect(schemaIndexCalls(CONN_ID)).toBe(1)
    expect(screen.queryByTestId("doc-collections-error")).not.toBeInTheDocument()
    expect(screen.getByTestId("schema-count-label")).not.toHaveTextContent("Could not load")
  })

  // A workspace switch clears the selected connection; the schema panel must not keep
  // showing the previous workspace's connection failure.
  it("drops a failure when the workspace changes", async () => {
    const failed = () =>
      res(500, { error: "Failed to build schema index: could not list collections: bad auth : authentication failed" })
    serve(failed)
    render(<ExplorerPage />)
    await screen.findByTestId("doc-collections-error")
    expect(document.body.textContent).toContain("bad auth : authentication failed")

    serve(failed, [])
    act(() => {
      window.dispatchEvent(new Event(ACTIVE_WORKSPACE_EVENT))
    })
    await screen.findByText("Select a connection to browse tables")
    expect(document.body.textContent).not.toContain("bad auth : authentication failed")
    expect(screen.queryByText("Failed to load schema")).not.toBeInTheDocument()
  })

  // Control: a successful load still shows the count, and no error.
  it("still shows the count when the collections load", async () => {
    serve(() => res(200, TWO_COLLECTIONS))
    render(<ExplorerPage />)

    const label = await screen.findByTestId("schema-count-label")
    await waitFor(() => expect(label).toHaveTextContent("1 databases · 2 collections"))
    expect(schemaIndexCalls()).toBeGreaterThan(0)
    expect(screen.queryByTestId("doc-collections-error")).not.toBeInTheDocument()
    expect(label).not.toHaveTextContent("Could not load")
  })

  // The Retry next to the failure must ask the gateway again past its cache, and the
  // failure must go once the collections load.
  it("Retry loads the collections again and drops the failure", async () => {
    let refuse = true
    serve(() => (refuse ? res(500, AUTH_REFUSED) : res(200, ONE_COLLECTION)))
    const user = userEvent.setup()
    render(<ExplorerPage />)

    const alert = await screen.findByTestId("doc-collections-error")
    expect(screen.getByTestId("schema-count-label")).toHaveTextContent("Could not load databases")
    const before = schemaIndexCalls()
    expect(before).toBeGreaterThan(0)

    refuse = false
    await user.click(within(alert).getByRole("button", { name: "Retry" }))

    await waitFor(() => expect(screen.getByTestId("schema-count-label")).toHaveTextContent("1 databases · 1 collections"))
    const urls = schemaIndexUrls(CONN_ID)
    expect(urls).toHaveLength(before + 1)
    expect(urls[urls.length - 1]).toContain("refresh=true")
    expect(screen.getByTestId("schema-count-label")).not.toHaveTextContent("Could not load")
    expect(screen.queryByTestId("doc-collections-error")).not.toBeInTheDocument()
  })

  // While the list is on its way there is nothing to count yet.
  it("says it is loading, not a count of zero, until the gateway answers", async () => {
    const pending: Array<(r: Response) => void> = []
    serve(() => new Promise<Response>((resolve) => pending.push(resolve)))
    render(<ExplorerPage />)

    await waitFor(() => expect(screen.getByTestId("schema-count-label")).toHaveTextContent("Loading databases…"))
    expect(pending.length).toBeGreaterThan(0)
    expect(document.body.textContent).not.toMatch(/\b0 databases/i)
    expect(screen.queryByTestId("doc-collections-error")).not.toBeInTheDocument()

    await act(async () => {
      pending.forEach((answer) => answer(res(500, AUTH_REFUSED)))
    })
    await waitFor(() => expect(screen.getByTestId("schema-count-label")).toHaveTextContent("Could not load databases"))
    expect(screen.getByTestId("schema-count-label")).not.toHaveTextContent("Loading")
    expect(await screen.findByTestId("doc-collections-error")).toHaveTextContent("bad auth : authentication failed")
  })

  // A database host name can contain "llm"; a schema load never involves the AI service.
  it("does not blame the AI service for a host name that contains llm", async () => {
    serve(() =>
      res(500, { error: "Failed to build schema index: could not list collections: shard-00.smallmart.example.net:27017: timed out" }),
    )
    render(<ExplorerPage />)

    const alert = await screen.findByTestId("doc-collections-error")
    expect(alert).toHaveTextContent("smallmart.example.net")
    expect(alert).toHaveTextContent("The database did not answer")
    expect(document.body.textContent).not.toMatch(/LLM service|AI service/)
  })
})

// Each wording a driver uses for a refused login or a silent host gets its own hint.
// Every case matches one wording only, so no case passes on the back of another.
describe("Explorer hint for each driver wording", () => {
  beforeEach(() => {
    mockFetch.mockReset()
  })

  const LOGIN_REFUSED = "The database rejected the login"
  const NO_ANSWER = "The database did not answer"

  it.each([
    ["authentication failed", "password authentication failed for user reader", LOGIN_REFUSED, NO_ANSWER],
    ["bad auth", "bad auth", LOGIN_REFUSED, NO_ANSWER],
    ["not authorized", "not authorized on shop to execute command { listCollections: 1 }", LOGIN_REFUSED, NO_ANSWER],
    ["access denied for user", "Error 1045: Access denied for user reader (using password: YES)", LOGIN_REFUSED, NO_ANSWER],
    ["timed out", "shard-00.example.net:27017: timed out", NO_ANSWER, LOGIN_REFUSED],
    ["no reachable servers", "no reachable servers", NO_ANSWER, LOGIN_REFUSED],
    ["server selection", "server selection error: context deadline exceeded", NO_ANSWER, LOGIN_REFUSED],
    ["i/o timeout", "dial tcp [ip-redacted]:27017: i/o timeout", NO_ANSWER, LOGIN_REFUSED],
    ["Timeout: (Python driver)", "No replica set members found yet, Timeout: 30.0s", NO_ANSWER, LOGIN_REFUSED],
  ])("%s", async (_wording, reason, wantHint, otherHint) => {
    serve(() => res(500, { error: `Failed to build schema index: could not list collections: ${reason}` }))
    render(<ExplorerPage />)

    const alert = await screen.findByTestId("doc-collections-error")
    expect(alert).toHaveTextContent(reason)
    expect(alert).toHaveTextContent(wantHint)
    expect(alert).not.toHaveTextContent(otherHint)
  })
})

// A SQL connection's schema load fails the same way; its header must not count zero.
describe("Explorer header for a SQL connection", () => {
  beforeEach(() => {
    mockFetch.mockReset()
  })

  it("says the schemas could not be loaded", async () => {
    serve(
      () => res(500, { error: "Failed to build schema index: failed to connect: password authentication failed for user reader" }),
      [postgresConnection(CONN_ID, "warehouse db")],
    )
    render(<ExplorerPage />)

    await waitFor(() => expect(screen.getByTestId("schema-count-label")).toHaveTextContent("Could not load schemas"))
    expect(schemaIndexCalls(CONN_ID)).toBeGreaterThan(0)
    expect(document.body.textContent).not.toMatch(/\b0 schemas/i)
    expect(screen.getByText(/The database rejected the login/)).toBeInTheDocument()
  })

  // Control: the SQL header still counts when the schema loads.
  it("still counts schemas and tables when the schema loads", async () => {
    serve(() => res(200, TWO_COLLECTIONS), [postgresConnection(CONN_ID, "warehouse db")])
    render(<ExplorerPage />)

    await waitFor(() => expect(screen.getByTestId("schema-count-label")).toHaveTextContent("1 schemas · 2 tables"))
    expect(schemaIndexCalls(CONN_ID)).toBeGreaterThan(0)
  })
})
