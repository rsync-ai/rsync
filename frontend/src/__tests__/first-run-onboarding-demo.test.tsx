/**
 * The zero-credential demo ("Start with sample data") seeds a sample-data source
 * and a postgresql destination, then used to drop the user on a blank /chat. With
 * no LLM configured — the normal state of a fresh install — nothing they could
 * type there was guaranteed to work, so the demo stopped one step short of a
 * pipeline. It now lands on /chat with a prompt that names the pair and both
 * seeded connections, sent on arrival. The api-gateway reads that exact prompt
 * without an LLM (chat_nl_sample_data_test.go demoChatPrompt pins the same text).
 *
 * The chat home also offers the pair as a quick pipeline, but only once both
 * demo connections exist.
 */

import { afterEach, beforeAll, beforeEach, describe, expect, it, vi, type Mock } from "vitest"
import { fireEvent, render, screen, waitFor } from "@testing-library/react"
import "@testing-library/jest-dom"

const { push } = vi.hoisted(() => ({ push: vi.fn() }))

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push, replace: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("framer-motion", () => ({
  motion: new Proxy({} as Record<string, unknown>, {
    get: () => (props: Record<string, unknown>) => {
      const { children, onClick } = props as { children?: React.ReactNode; onClick?: () => void }
      return <div onClick={onClick}>{children}</div>
    },
  }),
}))
vi.mock("@/components/connectors/ConnectionLogo", () => ({
  ConnectionLogo: () => <span />,
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { FirstRunOnboarding } from "@/components/onboarding/FirstRunOnboarding"
import { AgenticPipelineHome } from "@/components/chat/AgenticPipelineHome"

const mockFetch = authFetch as unknown as Mock

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    statusText: String(status),
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as unknown as Response
}

const empty = { sourceCount: 0, destinationCount: 0, pipelineCount: 0, queryCount: 0 }

// GET /api/v1/demo/status and POST /api/v1/demo/seed as demo.go answers them.
const demoStatus = {
  available: true,
  source_connector: "sample-data",
  source_name: "Sample data (demo)",
  destination_connector: "postgresql",
  destination_name: "Demo warehouse",
  destination_database: "demo",
}
const seeded = {
  source_connection_id: "src-1",
  destination_connection_id: "dst-1",
  source_name: "Sample data (demo)",
  destination_name: "Demo warehouse",
}

function serveDemo(status: unknown, seed: { status: number; body: unknown }) {
  mockFetch.mockImplementation(async (path: string) => {
    if (path.startsWith("/api/v1/demo/status")) return res(200, status)
    if (path.startsWith("/api/v1/demo/seed")) return res(seed.status, seed.body)
    return res(200, { total: 0, connections: [], pipelines: [], queries_used: 0 })
  })
}

const chatHref = (prompt: string) => `/chat?prompt=${encodeURIComponent(prompt)}&autosend=1`

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

beforeEach(() => {
  mockFetch.mockReset()
  push.mockReset()
  vi.spyOn(console, "warn").mockImplementation(() => {})
  vi.spyOn(console, "error").mockImplementation(() => {})
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe("FirstRunOnboarding — Start with sample data", () => {
  it("lands on the chat with a sent prompt naming the pair and both seeded connections", async () => {
    serveDemo(demoStatus, { status: 200, body: seeded })
    render(<FirstRunOnboarding initial={empty} />)

    fireEvent.click(await screen.findByRole("button", { name: /Start with sample data/ }))

    await waitFor(() => expect(push).toHaveBeenCalledTimes(1))
    expect(push).toHaveBeenCalledWith(
      chatHref(
        "Sync sample-data to postgresql; source connection: Sample data (demo); destination connection: Demo warehouse",
      ),
    )
    const url = new URL(push.mock.calls[0][0] as string, "http://x")
    expect(url.pathname).toBe("/chat")
    expect(url.searchParams.get("autosend")).toBe("1")
  })

  it("names the connections the seed reports over the status defaults", async () => {
    serveDemo(demoStatus, {
      status: 200,
      body: { ...seeded, source_name: "Sample data (demo) 2", destination_name: "Demo warehouse 2" },
    })
    render(<FirstRunOnboarding initial={empty} />)

    fireEvent.click(await screen.findByRole("button", { name: /Start with sample data/ }))

    await waitFor(() =>
      expect(push).toHaveBeenCalledWith(
        chatHref(
          "Sync sample-data to postgresql; source connection: Sample data (demo) 2; destination connection: Demo warehouse 2",
        ),
      ),
    )
  })

  it("falls back to demo.go's names when an older gateway reports only availability", async () => {
    serveDemo({ available: true, destination_database: "demo" }, { status: 200, body: {} })
    render(<FirstRunOnboarding initial={empty} />)

    fireEvent.click(await screen.findByRole("button", { name: /Start with sample data/ }))

    await waitFor(() =>
      expect(push).toHaveBeenCalledWith(
        chatHref(
          "Sync sample-data to postgresql; source connection: Sample data (demo); destination connection: Demo warehouse",
        ),
      ),
    )
  })

  // Control: the navigation is tied to a successful seed, so a test that only
  // checked "push was called" could not pass on a broken seed.
  it("stays put and says why when seeding fails", async () => {
    serveDemo(demoStatus, { status: 502, body: { message: "Destination did not answer" } })
    render(<FirstRunOnboarding initial={empty} />)

    fireEvent.click(await screen.findByRole("button", { name: /Start with sample data/ }))

    expect(await screen.findByRole("alert")).toHaveTextContent("Destination did not answer")
    expect(push).not.toHaveBeenCalled()
  })
})

describe("AgenticPipelineHome — demo quick pipeline", () => {
  function serveConnections(connections: Array<{ connector_type: string; type: "source" | "destination" }>) {
    mockFetch.mockImplementation(async (url: string) => {
      if (String(url).includes("/connections")) {
        return res(200, {
          connections: connections.map((c, i) => ({ id: `c${i}`, name: `conn ${i}`, status: "active", ...c })),
        })
      }
      return res(200, { pipelines: [], total: 0 })
    })
  }

  it("offers Sample data → Postgres once both demo connections exist, and sends the pair", async () => {
    serveConnections([
      { connector_type: "sample-data", type: "source" },
      { connector_type: "postgresql", type: "destination" },
    ])
    const onSubmit = vi.fn()
    render(<AgenticPipelineHome onSubmit={onSubmit} />)

    fireEvent.click(await screen.findByText("Sample data → Postgres"))
    expect(onSubmit).toHaveBeenCalledWith("sync sample-data to postgresql")
  })

  it("does not offer it without a sample-data connection", async () => {
    serveConnections([
      { connector_type: "mysql", type: "source" },
      { connector_type: "postgresql", type: "destination" },
    ])
    render(<AgenticPipelineHome onSubmit={() => {}} />)

    // The matching chip proves the connections loaded before the absence check.
    await screen.findByText("MySQL → Postgres")
    expect(screen.queryByText("Sample data → Postgres")).not.toBeInTheDocument()
  })

  it("leaves it out of the generic examples shown to a user with no connections", async () => {
    serveConnections([])
    render(<AgenticPipelineHome onSubmit={() => {}} />)

    await waitFor(() => expect(mockFetch).toHaveBeenCalled())
    await screen.findByText("MySQL → S3")
    expect(screen.queryByText("Sample data → Postgres")).not.toBeInTheDocument()
  })
})
