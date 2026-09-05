/**
 * Workspace-scoped localStorage keys.
 *
 * localStorage is per-origin, not per-tenant. Anything cached under a bare key
 * survives a workspace switch, so the next workspace reads the previous one's
 * data back out of the browser — pipeline names in the chat sidebar, another
 * tenant's chat transcript, a conversation id the new workspace never started.
 * The server never sees it, so no gate catches it; the only fix is to make the
 * key itself carry the tenant.
 *
 * Rule: any localStorage value derived from workspace-scoped API data must be
 * written through `workspaceScopedKey`. Values keyed by a resource id that is
 * already workspace-unique (`connection_tested_${id}`) and pure UI preferences
 * (`pipeline-copilot-dock-open`) are safe as they are.
 */

import { getActiveWorkspaceId } from "./active-workspace"

/** Separator between the base key and the workspace id. */
const SCOPE_SEPARATOR = ":"

/**
 * Suffixes `baseKey` with the active workspace id, so each workspace reads and
 * writes its own slot.
 *
 * Falls back to the bare base key when no workspace is selected yet (pre-login,
 * or before the switcher's first fetch resolves). That window predates any
 * tenant data, so there is nothing to leak — and `dropLegacyUnscopedValue`
 * deliberately leaves that value alone for the same reason.
 */
export function workspaceScopedKey(baseKey: string): string {
  const workspaceId = getActiveWorkspaceId()
  return workspaceId ? `${baseKey}${SCOPE_SEPARATOR}${workspaceId}` : baseKey
}

/**
 * Removes a value written under the bare `baseKey` by a build that predates
 * scoping. Call it from the load path, once a workspace is selected: the value
 * belongs to whichever workspace happened to be active when it was written, so
 * it can't be migrated into a scoped slot — it can only be dropped.
 *
 * No-op while no workspace is selected, so the fallback slot above stays usable.
 */
export function dropLegacyUnscopedValue(baseKey: string): void {
  if (typeof window === "undefined") return
  if (!getActiveWorkspaceId()) return
  try {
    window.localStorage.removeItem(baseKey)
  } catch {
    // Ignore — private mode / quota. A stale legacy value is inert once every
    // read goes through workspaceScopedKey.
  }
}

/**
 * Clears every slot of `baseKey` — the bare legacy key and all `base:<ws>`
 * variants. For logout, where "this browser should keep nothing" applies to all
 * workspaces, not just the one that happened to be active.
 */
export function clearAllWorkspaceScopes(baseKey: string): void {
  if (typeof window === "undefined") return
  try {
    const prefix = `${baseKey}${SCOPE_SEPARATOR}`
    const doomed: string[] = []
    for (let i = 0; i < window.localStorage.length; i++) {
      const key = window.localStorage.key(i)
      if (key === baseKey || (key && key.startsWith(prefix))) doomed.push(key)
    }
    // Collected first: removing during the scan re-indexes the store.
    for (const key of doomed) window.localStorage.removeItem(key)
  } catch {
    // Ignore — private mode / quota.
  }
}
