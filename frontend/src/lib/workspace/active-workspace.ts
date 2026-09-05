/**
 * Active-workspace selection (client side).
 *
 * The api-gateway scopes every request to the caller's active workspace, chosen by
 * the `X-Workspace-ID` header (primary) or the `rsync_active_workspace_id` cookie
 * (fallback, used by server-rendered requests that can't read localStorage). This
 * module is the single source of truth for that selection so the switcher, the
 * fetch wrapper, and SSR all agree.
 *
 * Storage strategy:
 *   - localStorage — the client's authoritative selection; read first.
 *   - cookie       — mirror of the same value so SSR/server components forward it.
 */

export const ACTIVE_WORKSPACE_KEY = "rsync_active_workspace_id"

// Same-tab change signal. The cross-tab `storage` event only fires in OTHER tabs,
// so a same-tab switch needs an explicit dispatch for live components to re-resolve.
export const ACTIVE_WORKSPACE_EVENT = "rsync:active-workspace-changed"

// ~1 year; the selection is a convenience, not a credential.
const COOKIE_MAX_AGE_SECONDS = 60 * 60 * 24 * 365

function readCookie(name: string): string | null {
  if (typeof document === "undefined") return null
  const match = document.cookie.match(new RegExp(`(?:^|;\\s*)${name}=([^;]+)`))
  return match ? decodeURIComponent(match[1]) : null
}

/**
 * Returns the active workspace id, or null when none is selected (or on the
 * server). localStorage is the primary source; the cookie is the fallback.
 */
export function getActiveWorkspaceId(): string | null {
  if (typeof window === "undefined") return null
  try {
    const stored = window.localStorage.getItem(ACTIVE_WORKSPACE_KEY)
    if (stored) return stored
  } catch {
    // localStorage can throw in private/locked-down browser modes — fall through.
  }
  return readCookie(ACTIVE_WORKSPACE_KEY)
}

/**
 * Persists the active workspace to BOTH localStorage (client) and a cookie (so
 * server-rendered requests forward it as the X-Workspace-ID fallback). Passing
 * null clears both.
 */
export function setActiveWorkspaceId(id: string | null): void {
  if (typeof window === "undefined") return
  try {
    if (id) window.localStorage.setItem(ACTIVE_WORKSPACE_KEY, id)
    else window.localStorage.removeItem(ACTIVE_WORKSPACE_KEY)
  } catch {
    // Ignore storage failures — the cookie below still carries the selection.
  }
  if (id) {
    document.cookie = `${ACTIVE_WORKSPACE_KEY}=${encodeURIComponent(id)}; path=/; max-age=${COOKIE_MAX_AGE_SECONDS}; SameSite=Lax`
  } else {
    document.cookie = `${ACTIVE_WORKSPACE_KEY}=; path=/; max-age=0; SameSite=Lax`
  }
  // Notify same-tab listeners so client pages re-resolve the selection immediately.
  try {
    window.dispatchEvent(new Event(ACTIVE_WORKSPACE_EVENT))
  } catch {
    // Event constructor unavailable in exotic environments — non-fatal.
  }
}

/**
 * Subscribes to active-workspace changes — same-tab (via the custom event above)
 * and cross-tab (via the storage event). Returns an unsubscribe function. No-op on
 * the server. Lets client pages stay in sync when the workspace is switched in place.
 */
export function onActiveWorkspaceChange(handler: () => void): () => void {
  if (typeof window === "undefined") return () => {}
  const onCustom = () => handler()
  const onStorage = (e: StorageEvent) => {
    if (e.key === ACTIVE_WORKSPACE_KEY || e.key === null) handler()
  }
  window.addEventListener(ACTIVE_WORKSPACE_EVENT, onCustom)
  window.addEventListener("storage", onStorage)
  return () => {
    window.removeEventListener(ACTIVE_WORKSPACE_EVENT, onCustom)
    window.removeEventListener("storage", onStorage)
  }
}

/**
 * Guards a fetch against a workspace switch that lands mid-flight.
 *
 * authFetch stamps X-Workspace-ID at call time, so a request issued under
 * workspace A stays scoped to A even if the user switches to B before it
 * resolves. The switch fires its own refetch for B, but the two are racing: if
 * A's response arrives last, the page renders A's rows under B's header —
 * another tenant's data on screen, with no error to hint at it.
 *
 * Call before awaiting and check after; drop the response when it reports true:
 *
 *   const isStale = captureWorkspace()
 *   const res = await authFetch(...)
 *   if (isStale()) return          // a switch overtook this request
 *
 * Returns false on the server (no selection to compare against), so SSR paths
 * are unaffected.
 */
export function captureWorkspace(): () => boolean {
  const capturedId = getActiveWorkspaceId()
  return () => {
    if (typeof window === "undefined") return false
    return getActiveWorkspaceId() !== capturedId
  }
}

/**
 * Decides the active workspace after one is deleted. The active selection must
 * never keep pointing at a deleted workspace — the api-gateway rejects a stale
 * X-Workspace-ID. Pure (no storage access) so callers can unit-test the decision:
 *
 *   - the deleted workspace was NOT active → leave the selection untouched
 *   - the deleted workspace WAS active     → re-point to the first surviving
 *     workspace, or null when none remain
 *
 * `workspaces` should be the post-delete list, but the deleted id is filtered out
 * defensively in case the list hasn't refreshed yet.
 */
export function resolveActiveAfterDelete(
  workspaces: ReadonlyArray<{ id: string }>,
  deletedId: string,
  currentActiveId: string | null,
): { changed: boolean; nextActiveId: string | null } {
  if (currentActiveId !== deletedId) {
    return { changed: false, nextActiveId: currentActiveId }
  }
  const next = workspaces.find((w) => w.id !== deletedId)?.id ?? null
  return { changed: true, nextActiveId: next }
}
