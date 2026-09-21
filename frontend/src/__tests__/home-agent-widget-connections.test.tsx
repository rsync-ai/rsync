import { afterEach, describe, expect, it, vi } from "vitest"
import { act, render, screen } from "@testing-library/react"
import "@testing-library/jest-dom"
import type { Mock } from "vitest"

import { HomeAgentWidget, buildQuickPrompts, exampleRequest } from "@/components/home/HomeAgentWidget"
import { authFetch } from "@/lib/api/auth-fetch"

// Issue #2: the Home "Create with AI" card flashed "No connections configured"
// while connections loaded, and its quick-start chips ignored real connections.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/",
}))

const mongo = { id: "c1", name: "Atlas", type: "source", connector_type: "mongodb", status: "active" }
const gcs = { id: "c2", name: "Bucket", type: "destination", connector_type: "gcs", status: "active" }

afterEach(() => vi.clearAllMocks())

describe("buildQuickPrompts", () => {
  it("builds one chip per distinct source→destination type pair", () => {
    const chips = buildQuickPrompts([mongo, gcs, { ...mongo, id: "c3", name: "Atlas 2" }])
    expect(chips.map((c) => c.label)).toEqual(["MongoDB → Google Cloud Storage"])
    expect(chips[0].prompt).toBe("Create a pipeline from mongodb to gcs")
  })

  it("caps the chip count and skips expired connections", () => {
    const pg = { id: "c4", name: "PG", type: "source", connector_type: "postgresql", status: "active" }
    const my = { id: "c5", name: "My", type: "source", connector_type: "mysql", status: "active" }
    const s3 = { id: "c6", name: "S3", type: "destination", connector_type: "aws-s3", status: "active" }
    expect(buildQuickPrompts([mongo, pg, my, gcs, s3], 3)).toHaveLength(3)
    expect(buildQuickPrompts([{ ...mongo, is_expired: true }, gcs])).toEqual([])
  })
})

// #55: a PostgreSQL source that could also write to MongoDB and GCS took every
// chip, so the MongoDB → GCS flow never showed; the "Try:" hint named S3.
describe("buildQuickPrompts across several sources", () => {
  const pg = {
    id: "p1", name: "PG", type: "source", connector_type: "postgresql", status: "active",
    supports_source: true, supports_destination: true,
  }
  const atlas = { ...mongo, supports_source: true, supports_destination: true }
  const pg2 = { ...pg, id: "p2", name: "PG replica" }

  it("gives each source a turn, sinks first, same-type pairs last", () => {
    const chips = buildQuickPrompts([pg, pg2, atlas, gcs]).map((c) => c.label)
    expect(chips).toEqual([
      "PostgreSQL → Google Cloud Storage",
      "MongoDB → Google Cloud Storage",
      "PostgreSQL → MongoDB",
    ])
  })

  it("names a real pair in the example request", () => {
    const chips = buildQuickPrompts([atlas, gcs])
    expect(exampleRequest(chips, true)).toBe(
      "Sync my orders table from MongoDB to Google Cloud Storage in real-time"
    )
    expect(exampleRequest([], false)).toMatch(/PostgreSQL to S3/)
  })
})

describe("HomeAgentWidget", () => {
  it("does not flash the empty-state warning while connections load", async () => {
    let release: (() => void) | undefined
    ;(authFetch as Mock).mockImplementation(
      () =>
        new Promise((resolve) => {
          release = () =>
            resolve({ ok: true, json: async () => ({ connections: [mongo, gcs] }) })
        })
    )
    render(<HomeAgentWidget />)
    expect(screen.queryByText("No connections configured")).toBeNull()
    expect(screen.queryByText("PostgreSQL → S3")).toBeNull()

    await act(async () => {
      release?.()
    })
    expect(await screen.findByRole("button", { name: /MongoDB → Google Cloud Storage/ })).toBeInTheDocument()
    expect(screen.queryByText("No connections configured")).toBeNull()
    expect(screen.queryByText("PostgreSQL → S3")).toBeNull()
    expect(screen.queryByText(/S3/)).toBeNull()
  })

  it("shows the warning and generic chips only after an empty result", async () => {
    ;(authFetch as Mock).mockResolvedValue({ ok: true, json: async () => ({ connections: [] }) })
    render(<HomeAgentWidget />)
    expect(await screen.findByText("No connections configured")).toBeInTheDocument()
    expect(screen.getByRole("button", { name: /PostgreSQL → S3/ })).toBeInTheDocument()
  })
})
