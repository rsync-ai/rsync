/**
 * KI-CDC-STATUS-401-FORCES-LOGOUT.
 *
 * `authFetch` treated every 401 as "your session expired" and navigated to
 * /logout, which deletes the session row server-side. But the api-gateway's CDC
 * proxies forward the orchestrator's answer verbatim, and the orchestrator says
 * `401 {"error":"authentication required"}` when a proxied call carries no
 * principal — the default whenever `INTERNAL_SERVICE_SECRET` is unset. So
 * opening a CDC pipeline's detail page signed the operator out.
 *
 * The fix asks /auth/me before destroying anything. These tests pin all three
 * answers, because getting any one of them wrong is a different outage:
 * logging out on a resource 401 is the bug above; NOT logging out on a real
 * expiry strands the user on a dead page; logging out on a network blip signs
 * people out for losing wifi.
 */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

vi.mock("@/lib/auth", () => ({ getAuthHeaders: () => ({}) }))
vi.mock("@/lib/workspace/active-workspace", () => ({ getActiveWorkspaceId: () => null }))

import { authFetch, __resetAuthFetchSessionState } from "@/lib/api/auth-fetch"

const ME = "/api/v1/auth/me"

function res(status: number): Response {
  return { ok: status < 400, status, json: async () => ({}) } as unknown as Response
}

let hrefs: string[]

beforeEach(() => {
  __resetAuthFetchSessionState()
  hrefs = []
  Object.defineProperty(window, "location", {
    configurable: true,
    value: {
      pathname: "/pipelines/4cb7b6b0",
      search: "",
      set href(v: string) {
        hrefs.push(v)
      },
      get href() {
        return hrefs.at(-1) ?? ""
      },
    },
  })
})

afterEach(() => {
  vi.unstubAllGlobals()
})

/** Serve 401 for the endpoint under test and `meStatus` for the probe. */
function serve(meStatus: number | "network-error") {
  const fetchMock = vi.fn(async (url: string) => {
    if (String(url).includes(ME)) {
      if (meStatus === "network-error") throw new TypeError("Failed to fetch")
      return res(meStatus)
    }
    return res(401)
  })
  vi.stubGlobal("fetch", fetchMock)
  return fetchMock
}

describe("authFetch — a 401 is not automatically a logout", () => {
  it("does NOT log out when /auth/me says the session is fine", async () => {
    const fetchMock = serve(200)

    const r = await authFetch("/api/v1/pipelines/4cb7b6b0/cdc/status")

    // The 401 is handed back so the caller can render an error.
    expect(r.status).toBe(401)
    expect(hrefs).toEqual([])
    // It really did ask — otherwise this test would pass on a helper that
    // simply stopped logging out, which is a different bug.
    expect(fetchMock.mock.calls.some(([u]) => String(u).includes(ME))).toBe(true)
  })

  it("DOES log out when /auth/me also says 401 — a genuine expiry", async () => {
    serve(401)

    await authFetch("/api/v1/pipelines/4cb7b6b0/cdc/status")

    expect(hrefs).toEqual(["/logout?next=%2Fpipelines%2F4cb7b6b0"])
  })

  it("does NOT log out when the probe cannot reach the API", async () => {
    serve("network-error")

    await authFetch("/api/v1/pipelines/4cb7b6b0/cdc/status")

    expect(hrefs).toEqual([])
  })

  it("probes once and redirects once when a page 401s several times at once", async () => {
    const fetchMock = serve(401)

    await Promise.all([
      authFetch("/api/v1/pipelines/4cb7b6b0/cdc/status"),
      authFetch("/api/v1/pipelines/4cb7b6b0/checkpoints"),
      authFetch("/api/v1/pipelines/4cb7b6b0/transforms"),
    ])

    expect(fetchMock.mock.calls.filter(([u]) => String(u).includes(ME))).toHaveLength(1)
    expect(hrefs).toHaveLength(1)
  })

  it("leaves /auth/ endpoints and skipAuth callers alone", async () => {
    const fetchMock = serve(401)

    await authFetch("/api/v1/auth/login", { method: "POST" })
    await authFetch("/api/v1/pipelines/4cb7b6b0/cdc/status", { skipAuth: true })

    expect(fetchMock.mock.calls.filter(([u]) => String(u).includes(ME))).toHaveLength(0)
    expect(hrefs).toEqual([])
  })
})
