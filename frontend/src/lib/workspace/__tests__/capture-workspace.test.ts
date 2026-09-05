import { beforeEach, describe, expect, it } from "vitest"

import {
  ACTIVE_WORKSPACE_KEY,
  captureWorkspace,
  setActiveWorkspaceId,
} from "../active-workspace"

// captureWorkspace is the guard against a cross-tenant render: authFetch stamps
// X-Workspace-ID at call time, so a response for workspace A can land after the
// user has already switched to B and paint A's rows under B's header. Callers
// capture before the await and drop the result if the selection moved.

describe("captureWorkspace", () => {
  beforeEach(() => {
    window.localStorage.clear()
  })

  it("reports not-stale while the workspace is unchanged", () => {
    setActiveWorkspaceId("ws-a")
    const isStale = captureWorkspace()
    expect(isStale()).toBe(false)
  })

  it("reports stale once the workspace switches", () => {
    setActiveWorkspaceId("ws-a")
    const isStale = captureWorkspace()
    setActiveWorkspaceId("ws-b")
    expect(isStale()).toBe(true)
  })

  it("reports stale when the workspace is cleared", () => {
    setActiveWorkspaceId("ws-a")
    const isStale = captureWorkspace()
    setActiveWorkspaceId(null)
    expect(isStale()).toBe(true)
  })

  it("treats a switch away from no-selection as stale", () => {
    // First load before any explicit pick: null is a real captured value, and a
    // response that predates the user's first choice is just as stale.
    const isStale = captureWorkspace()
    setActiveWorkspaceId("ws-a")
    expect(isStale()).toBe(true)
  })

  it("survives a switch made directly through storage (other tab)", () => {
    // The cross-tab path writes localStorage without going through the setter.
    setActiveWorkspaceId("ws-a")
    const isStale = captureWorkspace()
    window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, "ws-b")
    expect(isStale()).toBe(true)
  })
})
