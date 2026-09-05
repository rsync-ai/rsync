import { afterEach, beforeEach, describe, expect, it, vi } from "vitest"

// P5b review gap: the /proxy/explorer/* server routes proxy to WORKSPACE-SCOPED
// api-gateway endpoints (explorer.go: connections WHERE id=$1 AND workspace_id=$2,
// scoped by activeWorkspaceID(c)). They forward auth via a `Cookie: session_token`
// header but DROPPED the rsync_active_workspace_id selection, so an export or BI
// dashboard 404'd ("Connection not found") for any connection living in a shared
// (non-personal) workspace. These reproducers pin that the upstream Cookie header
// now carries BOTH session_token AND the active-workspace selection, merged into
// a single header (a naive spread of activeWorkspaceCookieHeader's `cookie` key
// would clobber session_token).

// Mutable cookie jar shared with the next/headers mock (hoisted above imports).
const hoisted = vi.hoisted(() => ({ jar: {} as Record<string, string> }))

vi.mock("next/headers", () => ({
  cookies: async () => ({
    get: (name: string) =>
      name in hoisted.jar ? { name, value: hoisted.jar[name] } : undefined,
  }),
}))

// The route handlers only call request.json(); a minimal stub avoids depending on
// a global Request implementation in the test environment.
function makeReq(body: unknown) {
  return { json: async () => body } as unknown as Request
}

function upstreamCookieHeader(fetchMock: ReturnType<typeof vi.fn>): string {
  const init = fetchMock.mock.calls[0]?.[1] as RequestInit | undefined
  const headers = (init?.headers ?? {}) as Record<string, string>
  return headers.Cookie ?? ""
}

beforeEach(() => {
  for (const k of Object.keys(hoisted.jar)) delete hoisted.jar[k]
})

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe("/proxy/explorer/export forwards the active-workspace selection", () => {
  function stubFetch() {
    const f = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      text: async () => JSON.stringify({ columns: ["a"], rows: [{ a: 1 }] }),
    })
    vi.stubGlobal("fetch", f)
    return f
  }

  it("merges session_token and rsync_active_workspace_id into the upstream Cookie", async () => {
    hoisted.jar.session_token = "sess-1"
    hoisted.jar.rsync_active_workspace_id = "ws-1"
    const f = stubFetch()

    const { POST } = await import("@/app/proxy/explorer/export/route")
    await POST(makeReq({ connection_id: "c1", sql: "SELECT 1", format: "csv" }))

    const cookie = upstreamCookieHeader(f)
    expect(cookie).toContain("session_token=sess-1")
    expect(cookie).toContain("rsync_active_workspace_id=ws-1")
  })

  it("still forwards session_token when no workspace is selected", async () => {
    hoisted.jar.session_token = "sess-1"
    const f = stubFetch()

    const { POST } = await import("@/app/proxy/explorer/export/route")
    await POST(makeReq({ connection_id: "c1", sql: "SELECT 1", format: "csv" }))

    const cookie = upstreamCookieHeader(f)
    expect(cookie).toContain("session_token=sess-1")
    expect(cookie).not.toContain("rsync_active_workspace_id")
  })
})

describe("/proxy/explorer/metabase/dashboard forwards the active-workspace selection", () => {
  it("merges session_token and rsync_active_workspace_id into the upstream Cookie", async () => {
    hoisted.jar.session_token = "sess-2"
    hoisted.jar.rsync_active_workspace_id = "ws-2"
    const f = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({ id: "dash-1" }),
    })
    vi.stubGlobal("fetch", f)

    const { POST } = await import("@/app/proxy/explorer/metabase/dashboard/route")
    await POST(makeReq({ sql: "SELECT 1", name: "D", connection_id: "c1" }))

    const cookie = upstreamCookieHeader(f)
    expect(cookie).toContain("session_token=sess-2")
    expect(cookie).toContain("rsync_active_workspace_id=ws-2")
  })
})
