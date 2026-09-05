import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest"
import { act, render, screen, waitFor } from "@testing-library/react"
import type { Mock } from "vitest"

import { AgenticPipelineHome } from "@/components/chat/AgenticPipelineHome"
import { HomeAgentWidget } from "@/components/home/HomeAgentWidget"
import { authFetch } from "@/lib/api/auth-fetch"
import { ACTIVE_WORKSPACE_EVENT, ACTIVE_WORKSPACE_KEY } from "@/lib/workspace/active-workspace"

// Found on prod: after switching demo → personal, /chat kept rendering demo's
// connections (Demo Mysql / Demo Postgresql) and demo's pipelines — each with a
// live Run button — under the personal workspace's name in the header. It
// persisted indefinitely, because the hero fetched once on mount with an empty
// dependency array and never subscribed to the workspace-change signal. Ten list
// surfaces were converted in #712; these two were missed.
//
// These pin both halves of the contract every workspace-scoped list owes:
//   1. refetch when the active workspace changes (same tab AND cross tab), and
//      clear first so the previous tenant's rows don't linger during the refetch
//   2. drop a response that a switch overtook mid-flight
//
// Blast radius was display-only — RunPipeline is workspace-gated server-side, so
// the Run button would have 404'd — but the rows themselves are another tenant's
// data on screen with nothing to indicate it.

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))
vi.mock("next/navigation", () => ({
  useRouter: () => ({ push: vi.fn(), replace: vi.fn(), refresh: vi.fn() }),
  usePathname: () => "/chat",
}))
vi.mock("framer-motion", () => ({
  motion: new Proxy({} as Record<string, unknown>, {
    get: () => (props: Record<string, unknown>) => {
      const { children, ...rest } = props as { children?: React.ReactNode }
      void rest
      return <div>{children}</div>
    },
  }),
}))
vi.mock("@/components/connectors/ConnectionLogo", () => ({
  ConnectionLogo: () => <span />,
}))

beforeAll(() => {
  window.ResizeObserver ||= class {
    observe() {}
    unobserve() {}
    disconnect() {}
  }
  if (!Element.prototype.scrollIntoView) Element.prototype.scrollIntoView = () => {}
})

const DEMO_WS = "0df2f291-0000-0000-0000-000000000001"
const PERSONAL_WS = "f71b4b51-0000-0000-0000-000000000002"

type Fixture = { connections: string[]; pipelines: string[] }

const DEMO: Fixture = {
  connections: ["Demo Mysql", "Demo Postgresql"],
  pipelines: ["Chat Pipeline 08:56:18", "Chat Pipeline 08:08:30"],
}
const PERSONAL: Fixture = {
  connections: ["Personal Warehouse"],
  pipelines: ["Nightly Export"],
}

function conn(name: string, i: number) {
  return {
    id: `conn-${name}-${i}`,
    name,
    connector_type: "postgresql",
    type: i % 2 === 0 ? "source" : "destination",
    status: "active",
  }
}

function pipe(name: string, i: number) {
  return {
    id: `pipe-${name}-${i}`,
    name,
    status: "completed",
    updated_at: "2026-07-29T00:00:00Z",
  }
}

/** Serves whichever fixture matches the workspace currently in localStorage. */
function serveByActiveWorkspace(byWorkspace: Record<string, Fixture>) {
  ;(authFetch as Mock).mockImplementation(async (url: string) => {
    const active = window.localStorage.getItem(ACTIVE_WORKSPACE_KEY) ?? ""
    const fx = byWorkspace[active] ?? { connections: [], pipelines: [] }
    const u = String(url)
    if (u.includes("/connections")) {
      return {
        ok: true,
        status: 200,
        json: async () => ({ connections: fx.connections.map(conn) }),
      }
    }
    if (u.includes("/pipelines")) {
      return {
        ok: true,
        status: 200,
        json: async () => ({ pipelines: fx.pipelines.map(pipe), total: fx.pipelines.length }),
      }
    }
    return { ok: true, status: 200, json: async () => ({}) }
  })
}

/** Switches the active workspace the way the header does: write, then announce. */
function switchWorkspace(id: string) {
  window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, id)
  window.dispatchEvent(new Event(ACTIVE_WORKSPACE_EVENT))
}

beforeEach(() => {
  window.localStorage.clear()
  window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, DEMO_WS)
  vi.clearAllMocks()
})

afterEach(() => {
  vi.restoreAllMocks()
})

describe("AgenticPipelineHome — active workspace", () => {
  it("replaces the previous workspace's connections and pipelines on switch", async () => {
    serveByActiveWorkspace({ [DEMO_WS]: DEMO, [PERSONAL_WS]: PERSONAL })
    render(<AgenticPipelineHome onSubmit={() => {}} />)

    await screen.findByText("Demo Mysql")
    expect(screen.getByText("Chat Pipeline 08:56:18")).toBeTruthy()

    await act(async () => {
      switchWorkspace(PERSONAL_WS)
    })

    await screen.findByText("Personal Warehouse")
    await waitFor(() => {
      // The exact prod symptom: demo's rows still on screen under personal.
      expect(screen.queryByText("Demo Mysql")).toBeNull()
      expect(screen.queryByText("Demo Postgresql")).toBeNull()
      expect(screen.queryByText("Chat Pipeline 08:56:18")).toBeNull()
      expect(screen.queryByText("Chat Pipeline 08:08:30")).toBeNull()
    })
    expect(screen.getByText("Nightly Export")).toBeTruthy()
  })

  it("refetches when the workspace changes in another tab", async () => {
    serveByActiveWorkspace({ [DEMO_WS]: DEMO, [PERSONAL_WS]: PERSONAL })
    render(<AgenticPipelineHome onSubmit={() => {}} />)
    await screen.findByText("Demo Mysql")

    // Cross-tab: the other tab writes localStorage, this tab only sees `storage`.
    await act(async () => {
      window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, PERSONAL_WS)
      window.dispatchEvent(new StorageEvent("storage", { key: ACTIVE_WORKSPACE_KEY }))
    })

    await screen.findByText("Personal Warehouse")
    expect(screen.queryByText("Demo Mysql")).toBeNull()
  })

  it("drops a response that a switch overtook mid-flight", async () => {
    // demo's fetch is held open, then released AFTER the switch to personal. Its
    // rows must never reach the screen: authFetch stamped X-Workspace-ID at call
    // time, so this response is demo's data arriving under personal.
    let releaseDemo: (() => void) | undefined
    const demoInFlight = new Promise<void>((resolve) => {
      releaseDemo = resolve
    })

    ;(authFetch as Mock).mockImplementation(async (url: string) => {
      const active = window.localStorage.getItem(ACTIVE_WORKSPACE_KEY) ?? ""
      const u = String(url)
      if (active === DEMO_WS) await demoInFlight
      const fx = active === DEMO_WS ? DEMO : PERSONAL
      if (u.includes("/connections")) {
        return { ok: true, status: 200, json: async () => ({ connections: fx.connections.map(conn) }) }
      }
      return {
        ok: true,
        status: 200,
        json: async () => ({ pipelines: fx.pipelines.map(pipe), total: fx.pipelines.length }),
      }
    })

    render(<AgenticPipelineHome onSubmit={() => {}} />)

    await act(async () => {
      switchWorkspace(PERSONAL_WS)
    })
    await screen.findByText("Personal Warehouse")

    await act(async () => {
      releaseDemo?.()
      await Promise.resolve()
    })

    await waitFor(() => {
      expect(screen.getByText("Personal Warehouse")).toBeTruthy()
    })
    expect(screen.queryByText("Demo Mysql")).toBeNull()
    expect(screen.queryByText("Chat Pipeline 08:56:18")).toBeNull()
  })
})

describe("HomeAgentWidget — active workspace", () => {
  it("replaces the previous workspace's connections on switch", async () => {
    serveByActiveWorkspace({ [DEMO_WS]: DEMO, [PERSONAL_WS]: PERSONAL })
    render(<HomeAgentWidget />)

    await screen.findByText(/Demo Mysql/)

    await act(async () => {
      switchWorkspace(PERSONAL_WS)
    })

    await screen.findByText(/Personal Warehouse/)
    expect(screen.queryByText(/Demo Mysql/)).toBeNull()
  })
})
