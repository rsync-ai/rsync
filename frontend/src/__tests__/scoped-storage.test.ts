import { beforeEach, describe, expect, it } from "vitest"

import { ACTIVE_WORKSPACE_KEY } from "@/lib/workspace/active-workspace"
import {
  clearAllWorkspaceScopes,
  dropLegacyUnscopedValue,
  workspaceScopedKey,
} from "@/lib/workspace/scoped-storage"

// Found on prod: switching from `demo` to the personal workspace left the chat
// sidebar listing demo's pipelines and reusing demo's conversation id. The cache
// lives in localStorage, which is per-origin — the api-gateway's workspace gates
// never see the read, so the key itself has to carry the tenant.

// jsdom's localStorage is not fully functional under this harness (same reason
// the header tests install one); a minimal in-memory Storage keeps key()/length
// honest, which clearAllWorkspaceScopes iterates over.
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
  document.cookie = `${ACTIVE_WORKSPACE_KEY}=; path=/; max-age=0; SameSite=Lax`
})

function setActive(id: string | null) {
  if (id) window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, id)
  else window.localStorage.removeItem(ACTIVE_WORKSPACE_KEY)
}

describe("workspaceScopedKey", () => {
  it("gives each workspace its own slot for the same base key", () => {
    setActive("ws-a")
    const a = workspaceScopedKey("rsync_chat_session_id")
    setActive("ws-b")
    const b = workspaceScopedKey("rsync_chat_session_id")

    expect(a).not.toBe(b)
    expect(a).toBe("rsync_chat_session_id:ws-a")
    expect(b).toBe("rsync_chat_session_id:ws-b")
  })

  it("falls back to the bare key before a workspace is selected", () => {
    setActive(null)
    expect(workspaceScopedKey("rsync_active_pipelines")).toBe("rsync_active_pipelines")
  })

  it("keeps a value written under ws-a invisible to ws-b", () => {
    setActive("ws-a")
    window.localStorage.setItem(workspaceScopedKey("rsync_active_pipelines"), '["demo-pipeline"]')

    setActive("ws-b")
    expect(window.localStorage.getItem(workspaceScopedKey("rsync_active_pipelines"))).toBeNull()

    // ...and ws-a still has its own, so scoping isolates rather than destroys.
    setActive("ws-a")
    expect(window.localStorage.getItem(workspaceScopedKey("rsync_active_pipelines"))).toBe('["demo-pipeline"]')
  })
})

describe("dropLegacyUnscopedValue", () => {
  it("drops a pre-scoping value once a workspace is active", () => {
    // Written by a build that had no scoping: it belongs to whichever workspace
    // was active then, which is unknowable now — so it can only be dropped, never
    // migrated into a scoped slot.
    window.localStorage.setItem("rsync_chat_ui_v2_state", '{"messages":[]}')
    setActive("ws-a")

    dropLegacyUnscopedValue("rsync_chat_ui_v2_state")

    expect(window.localStorage.getItem("rsync_chat_ui_v2_state")).toBeNull()
  })

  it("leaves the value alone while no workspace is selected", () => {
    // That window is also the fallback slot workspaceScopedKey writes to, so
    // dropping here would delete the value the caller just wrote.
    window.localStorage.setItem("rsync_chat_ui_v2_state", '{"messages":[]}')
    setActive(null)

    dropLegacyUnscopedValue("rsync_chat_ui_v2_state")

    expect(window.localStorage.getItem("rsync_chat_ui_v2_state")).toBe('{"messages":[]}')
  })
})

describe("chat session id (the consumer that surfaced this)", () => {
  it("hands each workspace its own conversation instead of resuming the previous one's", async () => {
    // The reported symptom: switch from `demo` to personal and the copilot keeps
    // the same session_id, so the backend continues demo's conversation — with
    // demo's pipeline names already in its history — under the new workspace.
    const { getSessionId } = await import("@/lib/api/chat")

    setActive("ws-a")
    const a = getSessionId()
    expect(getSessionId()).toBe(a) // stable within a workspace

    setActive("ws-b")
    const b = getSessionId()

    expect(b).not.toBe(a)
    // Switching back resumes ws-a's own conversation, not a third one.
    setActive("ws-a")
    expect(getSessionId()).toBe(a)
  })
})

describe("clearAllWorkspaceScopes", () => {
  it("removes every workspace's slot plus the legacy bare key", () => {
    // Logout means "this browser keeps nothing" — for all workspaces, not just
    // the one that happened to be active.
    window.localStorage.setItem("rsync_chat_session_id", "legacy")
    window.localStorage.setItem("rsync_chat_session_id:ws-a", "a")
    window.localStorage.setItem("rsync_chat_session_id:ws-b", "b")
    window.localStorage.setItem("rsync_chat_session_id:ws-c", "c")
    window.localStorage.setItem("unrelated_key", "keep")

    clearAllWorkspaceScopes("rsync_chat_session_id")

    expect(window.localStorage.getItem("rsync_chat_session_id")).toBeNull()
    expect(window.localStorage.getItem("rsync_chat_session_id:ws-a")).toBeNull()
    expect(window.localStorage.getItem("rsync_chat_session_id:ws-b")).toBeNull()
    // Removing during the scan re-indexes the store and skips entries; ws-c is
    // the one a naive loop leaves behind.
    expect(window.localStorage.getItem("rsync_chat_session_id:ws-c")).toBeNull()
    expect(window.localStorage.getItem("unrelated_key")).toBe("keep")
  })
})
