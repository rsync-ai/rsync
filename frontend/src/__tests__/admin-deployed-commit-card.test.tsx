import { beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"
import { render, screen, waitFor, within } from "@testing-library/react"
import userEvent from "@testing-library/user-event"

import {
  DeployedCommitCard,
  driftVerdict,
  shortCommit,
  uptimeLabel,
  type DriftReport,
  type DriftServiceResult,
} from "@/components/admin/DeployedCommitCard"
import AdminHealthPage from "@/app/(dashboard)/admin/health/page"
import { authFetch } from "@/lib/api/auth-fetch"

// GET /api/v1/admin/drift (admin_drift.go AdminDriftCheck) had no reader. admin/health now
// draws it as the "Deployed commit" card.

vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/admin/health",
}))
vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("sonner", () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const mockFetch = authFetch as unknown as Mock
const DRIFT = "/api/v1/admin/drift"

const NEW = "a915247a0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f"
const OLD = "3f89a7670c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f"
const NEW_BUILD = "2026-09-18T10:00:00Z"
const OLD_BUILD = "2026-09-10T10:00:00Z"

function res(status: number, body: unknown) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => body,
  } as unknown as Response
}

const hosts: Record<string, string> = {
  "api-gateway": "http://api-gateway:8080/version",
  "backend-orchestrator": "http://orchestrator:8080/version",
  "backend-temporal-adapter": "http://temporal-adapter:8082/version",
  "llm-service": "http://llm-service:5000/version",
}

function svc(service: string, over: Partial<DriftServiceResult> = {}): DriftServiceResult {
  return {
    service,
    url: hosts[service] ?? `http://${service}/version`,
    ok: true,
    status_code: 200,
    commit: NEW,
    built_at: NEW_BUILD,
    started_at: "2026-09-18T10:05:00Z",
    uptime_secs: 3 * 3600 + 20 * 60,
    latency_ms: 4,
    ...over,
  }
}

function report(services: DriftServiceResult[], over: Partial<DriftReport> = {}): DriftReport {
  return {
    gathered_at: "2026-09-19T08:00:00Z",
    all_agree: false,
    unique_commits: [],
    suspicions: [],
    services,
    ...over,
  }
}

const inSync = report(
  ["api-gateway", "backend-orchestrator", "backend-temporal-adapter", "llm-service"].map((s) => svc(s)),
  { all_agree: true, unique_commits: [NEW] },
)

function rowFor(service: string): HTMLElement {
  const row = document.querySelector<HTMLElement>(`tr[data-service="${service}"]`)
  if (!row) throw new Error(`no row for ${service}`)
  return row
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

describe("driftVerdict", () => {
  it("is in sync when every service answered with the same commit", () => {
    expect(driftVerdict(inSync)).toEqual({ kind: "in_sync", commit: NEW })
  })

  it("names the newest build when commits differ, even when most services are on the old one", () => {
    const v = driftVerdict(
      report([
        svc("api-gateway"),
        svc("backend-orchestrator", { commit: OLD, built_at: OLD_BUILD }),
        svc("backend-temporal-adapter", { commit: OLD, built_at: OLD_BUILD }),
        svc("llm-service", { commit: OLD, built_at: OLD_BUILD }),
      ]),
    )
    expect(v).toEqual({ kind: "mixed", commits: [NEW, OLD], newest: NEW })
  })

  it("names no newest build when the build times cannot say", () => {
    const v = driftVerdict(
      report([svc("api-gateway", { built_at: "unknown" }), svc("llm-service", { commit: OLD, built_at: "" })]),
    )
    expect(v).toEqual({ kind: "mixed", commits: [NEW, OLD], newest: undefined })

    const tie = driftVerdict(report([svc("api-gateway"), svc("llm-service", { commit: OLD })]))
    expect(tie).toMatchObject({ kind: "mixed", newest: undefined })
  })

  it("calls two commits drift even when another service did not answer", () => {
    const v = driftVerdict(
      report([
        svc("api-gateway"),
        svc("llm-service", { commit: OLD }),
        svc("backend-orchestrator", { ok: false, commit: undefined, error: "connection refused" }),
      ]),
    )
    expect(v.kind).toBe("mixed")
  })

  it("can't tell when a service does not know its commit or did not answer", () => {
    const v = driftVerdict(
      report([
        svc("api-gateway"),
        svc("backend-orchestrator", { commit: "dev" }),
        svc("llm-service", { commit: "" }),
        svc("backend-temporal-adapter", { ok: false, commit: undefined, status_code: 404 }),
      ]),
    )
    expect(v).toEqual({
      kind: "unknown",
      reasons: [
        "Orchestrator does not know its commit.",
        "LLM service does not know its commit.",
        "Temporal adapter did not answer.",
      ],
    })
  })

  it("repeats the gateway's reason when it disagrees and the card sees none", () => {
    const v = driftVerdict(report([svc("api-gateway")], { suspicions: ["something new"] }))
    expect(v).toEqual({ kind: "unknown", reasons: ["something new"] })
  })

  it("can't tell when no service was checked", () => {
    expect(driftVerdict(report([], { all_agree: true }))).toEqual({
      kind: "unknown",
      reasons: ["The gateway checked no services."],
    })
  })
})

describe("labels", () => {
  it("shortens a full sha and leaves a short one alone", () => {
    expect(shortCommit(NEW)).toBe("a915247")
    expect(shortCommit("v1.2.3")).toBe("v1.2.3")
  })

  it("formats uptime", () => {
    expect(uptimeLabel(undefined)).toBe("—")
    expect(uptimeLabel(0)).toBe("—")
    // Was "<1m". A service thirty seconds old has just restarted, which is the
    // one fact this card exists to surface; "<1m" filed it with the one that
    // has been up for fifty-nine seconds.
    expect(uptimeLabel(30)).toBe("30s")
    expect(uptimeLabel(4 * 60)).toBe("4m")
    expect(uptimeLabel(5 * 3600 + 12 * 60)).toBe("5h 12m")
    expect(uptimeLabel(3 * 86400 + 4 * 3600)).toBe("3d 4h")
  })
})

describe("DeployedCommitCard", () => {
  it("reads the admin drift route and says all services agree", async () => {
    mockFetch.mockResolvedValueOnce(res(200, inSync))
    render(<DeployedCommitCard refreshToken={0} />)

    expect(await screen.findByText("In sync")).toBeInTheDocument()
    expect(mockFetch).toHaveBeenCalledWith(DRIFT, { method: "GET" })
    expect(screen.getByText(/All 4 services run commit/)).toBeInTheDocument()
    const row = rowFor("backend-temporal-adapter")
    expect(within(row).getByText("Temporal adapter")).toBeInTheDocument()
    expect(within(row).getByText("temporal-adapter")).toBeInTheDocument()
    expect(within(row).getByTitle(NEW)).toHaveTextContent("a915247")
    expect(within(row).getByText("3h 20m")).toBeInTheDocument()
    expect(document.querySelector("[data-status-dot='in_sync']")).not.toBeNull()
  })

  it("marks the services that are not on the newest build", async () => {
    mockFetch.mockResolvedValueOnce(
      res(
        200,
        report([
          svc("api-gateway"),
          svc("backend-orchestrator", { commit: OLD, built_at: OLD_BUILD }),
          svc("llm-service"),
        ]),
      ),
    )
    render(<DeployedCommitCard refreshToken={0} />)

    expect(await screen.findByText("Mixed commits")).toBeInTheDocument()
    expect(screen.getByText(/2 different commits are running/)).toBeInTheDocument()
    expect(within(rowFor("backend-orchestrator")).getByText(/Older than the newest build \(a915247\)/)).toBeInTheDocument()
    expect(within(rowFor("api-gateway")).queryByText(/Older than/)).toBeNull()
    expect(within(rowFor("llm-service")).queryByText(/Older than/)).toBeNull()
  })

  it("says it can't tell, and why, for a dev build and an unreachable service", async () => {
    mockFetch.mockResolvedValueOnce(
      res(
        200,
        report([
          svc("api-gateway"),
          svc("backend-orchestrator", { commit: "dev", built_at: "unknown" }),
          svc("llm-service", { ok: false, commit: undefined, status_code: undefined, error: "dial tcp: i/o timeout" }),
        ]),
      ),
    )
    render(<DeployedCommitCard refreshToken={0} />)

    expect(await screen.findByText("Can't tell")).toBeInTheDocument()
    expect(screen.getByText("Orchestrator does not know its commit.")).toBeInTheDocument()
    expect(screen.getByText("LLM service did not answer.")).toBeInTheDocument()
    expect(within(rowFor("backend-orchestrator")).getByText("unknown")).toBeInTheDocument()
    expect(within(rowFor("llm-service")).getByText("dial tcp: i/o timeout")).toBeInTheDocument()
    expect(screen.getByText("scripts/deploy-service.sh")).toBeInTheDocument()
    // No reading is not a bad reading: hollow dot, not red.
    expect(document.querySelector("[data-status-dot='unknown']")).not.toBeNull()
    expect(screen.queryByText("Mixed commits")).toBeNull()
  })

  it("treats a body that is not a drift report as a failed read, and retries", async () => {
    mockFetch.mockResolvedValueOnce(res(200, { services: [{ service: "postgresql", status: "up" }] }))
    mockFetch.mockResolvedValueOnce(res(200, inSync))
    render(<DeployedCommitCard refreshToken={0} />)

    await screen.findByText("Could not check the deployed commits.")
    await userEvent.click(screen.getByRole("button", { name: "Retry" }))
    expect(await screen.findByText("In sync")).toBeInTheDocument()
    expect(mockFetch).toHaveBeenCalledTimes(2)
  })

  it("shows the error state on a 403", async () => {
    mockFetch.mockResolvedValueOnce(res(403, { error: "forbidden" }))
    render(<DeployedCommitCard refreshToken={0} />)
    expect(await screen.findByText("Could not check the deployed commits.")).toBeInTheDocument()
  })

  it("keeps the last answer when a refresh fails, and says so", async () => {
    mockFetch.mockResolvedValueOnce(res(200, inSync))
    mockFetch.mockRejectedValueOnce(new Error("network"))
    const { rerender } = render(<DeployedCommitCard refreshToken={0} />)
    await screen.findByText("In sync")

    rerender(<DeployedCommitCard refreshToken={1} />)
    expect(await screen.findByRole("status")).toHaveTextContent(/Could not refresh/)
    expect(screen.getByText("In sync")).toBeInTheDocument()
  })
})

describe("admin/health page", () => {
  it("draws the deployed-commit card and refreshes it with the page", async () => {
    mockFetch.mockImplementation(async (url: string) =>
      url === DRIFT
        ? res(200, inSync)
        : res(200, { services: [{ service: "postgresql", status: "up", latency_ms: 2 }] }),
    )
    render(<AdminHealthPage />)

    expect(await screen.findByText("Deployed commit")).toBeInTheDocument()
    await screen.findByText("In sync")
    const driftCalls = () => mockFetch.mock.calls.filter(([u]) => u === DRIFT).length
    expect(driftCalls()).toBe(1)

    await userEvent.click(screen.getByRole("button", { name: /Refresh/ }))
    await waitFor(() => expect(driftCalls()).toBe(2))
  })
})
