import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

import {
  ACTIVE_WORKSPACE_KEY,
  getActiveWorkspaceId,
  resolveActiveAfterDelete,
  setActiveWorkspaceId,
} from "@/lib/workspace/active-workspace"
import { authFetch } from "@/lib/api/auth-fetch"
import { tracedFetch } from "@/lib/api/traced-client"

// P5a: the active workspace selected in the UI must reach the backend. The backend
// reads X-Workspace-ID (primary) and the rsync_active_workspace_id cookie (SSR
// fallback); without this plumbing every request silently used the personal
// workspace. active-workspace.ts centralizes storage (localStorage primary, cookie
// fallback) and authFetch injects the header on every client call.

// jsdom's localStorage is not fully functional under this harness, so install a
// minimal in-memory Storage before each test for determinism.
function installMemoryStorage() {
  const store: Record<string, string> = {}
  const ls: Storage = {
    getItem: (k) => (k in store ? store[k] : null),
    setItem: (k, v) => {
      store[k] = String(v)
    },
    removeItem: (k) => {
      delete store[k]
    },
    clear: () => {
      for (const k of Object.keys(store)) delete store[k]
    },
    key: (i) => Object.keys(store)[i] ?? null,
    get length() {
      return Object.keys(store).length
    },
  }
  Object.defineProperty(window, "localStorage", { value: ls, configurable: true, writable: true })
}

beforeEach(() => {
  installMemoryStorage()
  document.cookie = `${ACTIVE_WORKSPACE_KEY}=; path=/; max-age=0`
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe("active-workspace storage", () => {
  it("returns null when nothing is set", () => {
    expect(getActiveWorkspaceId()).toBeNull()
  })

  it("reads the id from localStorage (primary)", () => {
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-ls")
    expect(getActiveWorkspaceId()).toBe("ws-ls")
  })

  it("falls back to the cookie when localStorage is empty", () => {
    document.cookie = `${ACTIVE_WORKSPACE_KEY}=ws-cookie; path=/`
    expect(getActiveWorkspaceId()).toBe("ws-cookie")
  })

  it("setActiveWorkspaceId writes both localStorage and the cookie", () => {
    setActiveWorkspaceId("ws-new")
    expect(window.localStorage.getItem(ACTIVE_WORKSPACE_KEY)).toBe("ws-new")
    expect(document.cookie).toContain(`${ACTIVE_WORKSPACE_KEY}=ws-new`)
  })

  it("setActiveWorkspaceId(null) clears both", () => {
    setActiveWorkspaceId("ws-x")
    setActiveWorkspaceId(null)
    expect(window.localStorage.getItem(ACTIVE_WORKSPACE_KEY)).toBeNull()
    expect(getActiveWorkspaceId()).toBeNull()
  })
})

// The cookie name is a cross-process contract: the Go api-gateway hardcodes the
// same literal (api-gateway/internal/handlers/workspace_context.go: workspaceCookie
// = "rsync_active_workspace_id"). The two codebases share no type system, so a
// rename here would move every TS test in lockstep and stay green while silently
// breaking SSR workspace scoping. Pin the literal so a rename fails loudly.
describe("ACTIVE_WORKSPACE_KEY cross-service contract", () => {
  it("matches the cookie name the api-gateway reads", () => {
    expect(ACTIVE_WORKSPACE_KEY).toBe("rsync_active_workspace_id")
  })
})

describe("authFetch X-Workspace-ID injection", () => {
  function stubFetch() {
    const f = vi.fn().mockResolvedValue({ status: 200 } as Response)
    vi.stubGlobal("fetch", f)
    return f
  }

  function capturedHeaders(f: ReturnType<typeof stubFetch>): Record<string, string> {
    return (f.mock.calls[0][1] as RequestInit).headers as Record<string, string>
  }

  it("attaches X-Workspace-ID from the active workspace", async () => {
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-123")
    const f = stubFetch()
    await authFetch("/api/v1/workspaces")
    expect(capturedHeaders(f)["X-Workspace-ID"]).toBe("ws-123")
  })

  it("omits X-Workspace-ID when no active workspace is set", async () => {
    const f = stubFetch()
    await authFetch("/api/v1/workspaces")
    expect(capturedHeaders(f)["X-Workspace-ID"]).toBeUndefined()
  })

  it("does not override a caller-supplied X-Workspace-ID header", async () => {
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-123")
    const f = stubFetch()
    await authFetch("/api/v1/workspaces", { headers: { "X-Workspace-ID": "ws-explicit" } })
    expect(capturedHeaders(f)["X-Workspace-ID"]).toBe("ws-explicit")
  })
})

// tracedFetch is a SECOND request path (mcp-connectors connection-test, telemetry).
// It must carry X-Workspace-ID too, or connection-USE calls on a shared connection
// would silently hit the personal workspace and 404. It builds a Headers object.
describe("tracedFetch X-Workspace-ID injection", () => {
  function stubFetch() {
    const f = vi.fn().mockResolvedValue({ status: 200 } as Response)
    vi.stubGlobal("fetch", f)
    return f
  }

  function capturedHeaders(f: ReturnType<typeof stubFetch>): Headers {
    return (f.mock.calls[0][1] as RequestInit).headers as Headers
  }

  it("attaches X-Workspace-ID from the active workspace", async () => {
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-traced")
    const f = stubFetch()
    await tracedFetch("http://localhost:5001/api/v1/connections/x/test")
    expect(capturedHeaders(f).get("X-Workspace-ID")).toBe("ws-traced")
  })

  it("does not override a caller-supplied X-Workspace-ID header", async () => {
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-traced")
    const f = stubFetch()
    await tracedFetch("http://localhost:5001/x", { headers: { "X-Workspace-ID": "ws-explicit" } })
    expect(capturedHeaders(f).get("X-Workspace-ID")).toBe("ws-explicit")
  })
})

// P5b-UI #3 (final piece): when a workspace is deleted, the active selection must
// never keep pointing at the tombstone — the api-gateway would reject the stale
// X-Workspace-ID. resolveActiveAfterDelete is the pure decision: re-point to a
// surviving workspace ONLY when the deleted one was the active one.
describe("resolveActiveAfterDelete", () => {
  const list = [{ id: "ws-a" }, { id: "ws-b" }, { id: "ws-c" }]

  it("leaves the selection untouched when a NON-active workspace is deleted", () => {
    const r = resolveActiveAfterDelete(list, "ws-b", "ws-a")
    expect(r.changed).toBe(false)
    expect(r.nextActiveId).toBe("ws-a")
  })

  it("re-points to a surviving workspace when the ACTIVE one is deleted", () => {
    // list is the post-delete list (ws-a already gone) → pick the first survivor
    const r = resolveActiveAfterDelete([{ id: "ws-b" }, { id: "ws-c" }], "ws-a", "ws-a")
    expect(r.changed).toBe(true)
    expect(r.nextActiveId).toBe("ws-b")
  })

  it("never returns the deleted id even if a stale list still contains it", () => {
    // defensive: a not-yet-refreshed list might still include the deleted ws
    const r = resolveActiveAfterDelete([{ id: "ws-a" }, { id: "ws-b" }], "ws-a", "ws-a")
    expect(r.changed).toBe(true)
    expect(r.nextActiveId).toBe("ws-b")
  })

  it("clears the selection when the deleted active workspace was the only one", () => {
    const r = resolveActiveAfterDelete([], "ws-a", "ws-a")
    expect(r.changed).toBe(true)
    expect(r.nextActiveId).toBeNull()
  })

  it("does nothing when there is no active selection", () => {
    const r = resolveActiveAfterDelete(list, "ws-b", null)
    expect(r.changed).toBe(false)
    expect(r.nextActiveId).toBeNull()
  })
})
