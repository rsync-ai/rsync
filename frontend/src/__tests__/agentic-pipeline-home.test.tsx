/**
 * The chat home page: recent pipelines read the list endpoint's real shape and
 * open their detail page, quick pipelines come from the user's connections, and
 * the first load shows a skeleton instead of an empty page.
 */
import { afterEach, describe, expect, it, vi, type Mock } from "vitest"
import { render, screen, waitFor, within, fireEvent } from "@testing-library/react"

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/chat",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("@/components/connectors/ConnectionLogo", () => ({
  ConnectionLogo: ({ connectorType }: { connectorType: string }) => <span data-testid="logo">{connectorType}</span>,
}))

import { authFetch } from "@/lib/api/auth-fetch"
import { AgenticPipelineHome } from "@/components/chat/AgenticPipelineHome"

const mockFetch = authFetch as unknown as Mock
const res = (status: number, body: unknown) =>
  ({ ok: status >= 200 && status < 300, status, json: async () => body }) as Response

const CONNECTIONS = [
  { id: "c1", name: "Postgresql Source", connector_type: "postgresql", type: "source", status: "active" },
  { id: "c2", name: "MongoDB Source", connector_type: "mongodb", type: "source", status: "active" },
  { id: "c3", name: "MongoDB Dest", connector_type: "mongodb", type: "destination", status: "active" },
  { id: "c4", name: "GCS Dest", connector_type: "gcs", type: "destination", status: "active" },
]

const PIPELINE = {
  id: "p-123",
  name: "Chat Pipeline 18:11:26",
  pipeline_status: "active",
  created_at: "2026-09-26T10:00:00Z",
  updated_at: "2026-09-26T10:00:00Z",
  sync_mode: "cdc",
  cdc_mode: "initial",
  source_connection: { name: "Postgresql Source", connector_type: "postgresql" },
  destination_connection: { name: "MongoDB Dest", connector_type: "mongodb" },
  last_execution: { status: "running", started_at: new Date(Date.now() - 2 * 3600_000).toISOString() },
  derived_status: "running",
}

function serve(connections: unknown[], pipelines: unknown[]) {
  mockFetch.mockImplementation(async (url: string) =>
    String(url).includes("/connections")
      ? res(200, { connections })
      : res(200, { pipelines, total: pipelines.length }),
  )
}

afterEach(() => mockFetch.mockReset())

describe("AgenticPipelineHome", () => {
  it("shows a skeleton until the first load lands", async () => {
    let release!: () => void
    mockFetch.mockImplementation(
      () => new Promise<Response>((r) => { release = () => r(res(200, { connections: [] })) }),
    )
    render(<AgenticPipelineHome onSubmit={() => {}} />)
    expect(screen.getByTestId("home-skeleton")).toBeInTheDocument()
    release()
    mockFetch.mockImplementation(async () => res(200, { pipelines: [], total: 0 }))
    await waitFor(() => expect(screen.queryByTestId("home-skeleton")).not.toBeInTheDocument())
  })

  it("renders recent pipelines from the nested connection shape and links each row to its detail page", async () => {
    serve(CONNECTIONS, [PIPELINE])
    render(<AgenticPipelineHome onSubmit={() => {}} />)

    const row = await screen.findByTestId("recent-pipeline-row")
    expect(row).toHaveAttribute("href", "/pipelines/p-123")
    const logos = within(row).getAllByTestId("logo").map((l) => l.textContent)
    expect(logos).toEqual(["postgresql", "mongodb"])
    expect(row).toHaveTextContent("Postgresql Source → MongoDB Dest")
    expect(row).toHaveTextContent("Running")
    expect(row).toHaveTextContent("Backfill+CDC")
    // Last run comes from the execution, not the row's updated_at.
    expect(row).toHaveTextContent("Last run 2h ago")
    expect(screen.queryByRole("button", { name: /^run$/i })).not.toBeInTheDocument()
  })

  it("builds quick pipelines from the connected source → destination pairs", async () => {
    serve(CONNECTIONS, [])
    const onSubmit = vi.fn()
    render(<AgenticPipelineHome onSubmit={onSubmit} />)

    fireEvent.click(await screen.findByText("Postgres → GCS"))
    expect(onSubmit).toHaveBeenCalledWith("sync postgresql to gcs")
    expect(screen.getByText("MongoDB → GCS")).toBeInTheDocument()
    // Templates that need connectors the user lacks are not offered as ready.
    expect(screen.queryByText("MySQL → S3")).not.toBeInTheDocument()
  })

  it("marks the generic templates as needing a connection when nothing matches", async () => {
    serve([], [])
    render(<AgenticPipelineHome onSubmit={() => {}} />)

    await screen.findByText("Example Pipelines")
    expect(screen.getByText("MySQL → S3")).toBeInTheDocument()
    expect(screen.getAllByText("Needs connection").length).toBeGreaterThan(0)
  })

  it("prefills the prompt from an example chip without sending it", async () => {
    serve(CONNECTIONS, [])
    const onSubmit = vi.fn()
    render(<AgenticPipelineHome onSubmit={onSubmit} />)

    await screen.findByText("Postgres → GCS")
    const chip = screen.getByRole("button", { name: /stream postgres changes to/i })
    fireEvent.click(chip)
    expect(screen.getByLabelText("Describe your pipeline")).toHaveValue(chip.textContent)
    expect(onSubmit).not.toHaveBeenCalled()
  })
})
