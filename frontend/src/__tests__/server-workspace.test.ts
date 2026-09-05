import { describe, expect, it } from "vitest"

import {
  activeWorkspaceCookieHeader,
  activeWorkspaceCookiePair,
  getActiveWorkspaceIdFromCookies,
} from "@/lib/workspace/server-workspace"
import { ACTIVE_WORKSPACE_KEY } from "@/lib/workspace/active-workspace"

// P5b: server-side parity with the client X-Workspace-ID injection. Server
// components fetch workspace-scoped data (executions, pipeline detail) straight
// from the api-gateway; without forwarding the active-workspace selection, SSR
// silently renders the caller's personal workspace. We forward the selection as
// the rsync_active_workspace_id COOKIE (not the X-Workspace-ID header) so a
// stale selection soft-falls-back to the personal workspace instead of
// hard-404ing the whole render (see api-gateway workspace_context.go: an
// explicit header => 404 on a non-member workspace, a stale cookie => fallback).

// Minimal stand-in for Next's cookies() ReadonlyRequestCookies (only .get(name)
// is used here; it returns { name, value } | undefined just like Next does).
function cookieStore(entries: Record<string, string>) {
  return {
    get(name: string) {
      return name in entries ? { name, value: entries[name] } : undefined
    },
  }
}

describe("getActiveWorkspaceIdFromCookies", () => {
  it("returns null when the cookie is absent", () => {
    expect(getActiveWorkspaceIdFromCookies(cookieStore({}))).toBeNull()
  })

  it("returns null when the cookie value is blank/whitespace", () => {
    expect(
      getActiveWorkspaceIdFromCookies(cookieStore({ [ACTIVE_WORKSPACE_KEY]: "   " })),
    ).toBeNull()
  })

  it("returns the trimmed id when present", () => {
    expect(
      getActiveWorkspaceIdFromCookies(cookieStore({ [ACTIVE_WORKSPACE_KEY]: " ws-123 " })),
    ).toBe("ws-123")
  })
})

describe("activeWorkspaceCookieHeader", () => {
  it("returns an empty object when no active workspace cookie is set", () => {
    expect(activeWorkspaceCookieHeader(cookieStore({}))).toEqual({})
  })

  it("forwards the selection as the rsync_active_workspace_id cookie", () => {
    expect(
      activeWorkspaceCookieHeader(cookieStore({ [ACTIVE_WORKSPACE_KEY]: "ws-abc" })),
    ).toEqual({ cookie: `${ACTIVE_WORKSPACE_KEY}=ws-abc` })
  })

  it("uses the cookie mirror, never X-Workspace-ID (soft-fallback for stale selections)", () => {
    const headers = activeWorkspaceCookieHeader(
      cookieStore({ [ACTIVE_WORKSPACE_KEY]: "ws-abc" }),
    )
    expect(headers["X-Workspace-ID"]).toBeUndefined()
  })
})

// The cookie PAIR (name=value) is the merge-friendly form, used by callers that
// already build their own Cookie header (the explorer proxy routes forward
// `Cookie: session_token=...` and must append the selection without clobbering).
describe("activeWorkspaceCookiePair", () => {
  it("returns null when no active workspace cookie is set", () => {
    expect(activeWorkspaceCookiePair(cookieStore({}))).toBeNull()
  })

  it("returns the name=value pair when present", () => {
    expect(
      activeWorkspaceCookiePair(cookieStore({ [ACTIVE_WORKSPACE_KEY]: "ws-abc" })),
    ).toBe(`${ACTIVE_WORKSPACE_KEY}=ws-abc`)
  })
})
