import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen, within } from "@testing-library/react"

import AdminHealthPage from "@/app/(dashboard)/admin/health/page"
import { authFetch } from "@/lib/api/auth-fetch"
import { formatAbsoluteTime } from "@/lib/utils"

// The health route grew two things the page ignored: a fourth status, "unknown", which
// means the sweep was never asked (and which fell through to the same grey as a status
// nobody had heard of), and a `detail` object carrying what the freshness sweep knows
// beyond whether it answered (admin_health.go serviceHealth.Detail).

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/admin/health",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const mockFetch = authFetch as unknown as Mock

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
})

beforeEach(() => {
  mockFetch.mockReset()
})

function serve(services: unknown[]) {
  mockFetch.mockResolvedValue(res(200, { services }))
}

async function card(title: string): Promise<HTMLElement> {
  const heading = await screen.findByText(title)
  // CardTitle → CardHeader → Card
  return heading.closest("[class*='rounded']") as HTMLElement
}

describe("admin health: the freshness sweep's own card", () => {
  it("labels the sweep in words and gives unknown its own dot and badge", async () => {
    serve([
      { service: "postgresql", status: "up", latency_ms: 2 },
      {
        service: "explorer-freshness-sweep",
        status: "unknown",
        latency_ms: 0,
        error: "orchestration is not available to this gateway; the sweep was not asked",
      },
    ])
    render(<AdminHealthPage />)

    const sweep = await card("Explorer freshness sweep")
    expect(screen.queryByText("Explorer-freshness-sweep")).toBeNull()

    const dot = sweep.querySelector("[data-status-dot]") as HTMLElement
    expect(dot.dataset.statusDot).toBe("unknown")
    // Not the fallback grey a status this page has never heard of would get.
    expect(dot.className).not.toMatch(/\bbg-zinc-400\b/)
    expect(dot.className).toMatch(/ring-zinc-400/)

    const badge = within(sweep).getByText("unknown")
    expect(badge.className).not.toMatch(/bg-zinc-100/) // secondary, the old fallthrough
    expect(within(sweep).getByText(/the sweep was not asked/).className).toMatch(/text-red-600/)
  })

  // #53: the card showed the raw keys ("Effective interval seconds 300") and an ISO
  // timestamp in UTC.
  it("names the sweep's facts in words and shows the last sweep in local time", async () => {
    serve([
      {
        service: "explorer-freshness-sweep",
        status: "up",
        latency_ms: 14,
        detail: {
          detail_available: true,
          effective_interval_seconds: 300,
          ticks_this_run: 7,
          ticks_per_run: 288,
          last_sweep_ok: false,
          last_sweep_at: "2026-09-15T14:00:00Z",
          last_result: { scanned: 4, opened: 1, resolved: 0, failed: 0 },
          some_new_fact: 3,
        },
      },
    ])
    render(<AdminHealthPage />)

    const sweep = await card("Explorer freshness sweep")
    const list = sweep.querySelector("dl") as HTMLElement
    expect(list).not.toBeNull()

    const pairs = Array.from(list.querySelectorAll("dt")).map((dt) => [
      dt.textContent,
      dt.nextElementSibling?.textContent,
    ])
    expect(pairs).toEqual([
      ["Runs every", "5 min"],
      ["Sweeps since the workflow last restarted", "7 of 288"],
      ["Last sweep succeeded", "no"],
      ["Last sweep", formatAbsoluteTime("2026-09-15T14:00:00Z")],
      ["Last sweep's models", "4 scanned, 1 opened, 0 resolved, 0 failed"],
      // A key this page has never heard of still shows, humanised.
      ["Some new fact", "3"],
    ])
    expect(list.textContent).not.toMatch(/2026-09-15T14:00:00Z|_/)
  })

  it("shows a caveat on an up row in grey, and a failure on a down row in red", async () => {
    serve([
      {
        service: "explorer-freshness-sweep",
        status: "up",
        latency_ms: 3,
        error: "the sweep is running but did not answer this query",
        detail: { detail_available: false },
      },
      { service: "redis", status: "down", latency_ms: 0, error: "connection refused" },
    ])
    render(<AdminHealthPage />)

    const sweep = await card("Explorer freshness sweep")
    const caveat = within(sweep).getByText(/did not answer this query/)
    expect(caveat.className).toMatch(/text-zinc-500/)
    expect(caveat.className).toMatch(/dark:text-zinc-400/)
    // detail_available=false is the only key, and it is not a row of its own.
    expect(sweep.querySelector("dl")).toBeNull()
    expect(caveat.className).not.toMatch(/text-red-600/)

    const redis = await card("Redis")
    expect(within(redis).getByText("connection refused").className).toMatch(/text-red-600/)
    // A socket check has no detail, so no empty list is drawn under it.
    expect(redis.querySelector("dl")).toBeNull()
  })

  it("draws no empty list under a check that sent an empty detail", async () => {
    serve([{ service: "explorer-freshness-sweep", status: "up", latency_ms: 5, detail: {} }])
    render(<AdminHealthPage />)

    const sweep = await card("Explorer freshness sweep")
    expect(sweep.querySelector("dl")).toBeNull()
  })

  it("gives degraded the warning badge rather than the neutral one", async () => {
    serve([{ service: "explorer-freshness-sweep", status: "degraded", latency_ms: 9, error: "last tick failed" }])
    render(<AdminHealthPage />)

    const sweep = await card("Explorer freshness sweep")
    expect(within(sweep).getByText("degraded").className).toMatch(/bg-amber-100/)
  })
})

describe("admin health: the CDC path's services (#53)", () => {
  it("names the orchestrator, Kafka Connect and the CDC sink", async () => {
    serve([
      { service: "orchestrator", status: "up", latency_ms: 4 },
      { service: "kafka-connect", status: "down", latency_ms: 0, error: "health check returned HTTP 503" },
      { service: "kafka-mcp-sink", status: "up", latency_ms: 6 },
    ])
    render(<AdminHealthPage />)

    expect(await screen.findByText("Orchestrator")).toBeInTheDocument()
    const connect = await card("Kafka Connect")
    expect(within(connect).getByText("health check returned HTTP 503")).toBeInTheDocument()
    expect(screen.getByText("CDC sink (kafka-mcp-sink)")).toBeInTheDocument()
    expect(screen.queryByText("Kafka-connect")).toBeNull()
  })
})
