/**
 * Active-workspace selection (server side).
 *
 * The client mirrors its active-workspace selection into the
 * `rsync_active_workspace_id` cookie (see active-workspace.ts). Server
 * components and route handlers that fetch workspace-scoped data straight from
 * the api-gateway must forward that selection, or the backend defaults every
 * SSR request to the caller's personal workspace.
 *
 * Why the cookie, not the X-Workspace-ID header: the api-gateway
 * (workspace_context.go) treats an explicit X-Workspace-ID naming a workspace
 * the caller is NOT a member of as a hard 404 on non-optional routes, while a
 * stale *cookie* soft-falls-back to the personal workspace. An SSR render should
 * degrade gracefully (show personal-workspace data) rather than 404 the whole
 * page, so we forward the cookie mirror.
 */

import { ACTIVE_WORKSPACE_KEY } from "./active-workspace"

/**
 * Minimal read-only view of Next's `cookies()` ReadonlyRequestCookies — only
 * `.get(name)` is needed. Kept structural so callers can pass the real store or
 * a test stub without importing next/headers here.
 */
export interface ServerCookieReader {
  get(name: string): { value?: string } | undefined
}

/**
 * Reads the active workspace id from the incoming request cookies. Returns null
 * when the cookie is absent or blank.
 */
export function getActiveWorkspaceIdFromCookies(cookies: ServerCookieReader): string | null {
  const value = cookies.get(ACTIVE_WORKSPACE_KEY)?.value?.trim()
  return value ? value : null
}

/**
 * The active-workspace selection as a `name=value` cookie PAIR, or null when no
 * selection is present. This is the merge-friendly form for callers that already
 * build their own Cookie header (e.g. the explorer proxy routes forward
 * `Cookie: session_token=...` and must append the selection without clobbering
 * it). Workspace ids are UUIDs, so the value needs no escaping.
 */
export function activeWorkspaceCookiePair(cookies: ServerCookieReader): string | null {
  const id = getActiveWorkspaceIdFromCookies(cookies)
  return id ? `${ACTIVE_WORKSPACE_KEY}=${id}` : null
}

/**
 * Server-side parity with the client fetch wrappers' X-Workspace-ID injection.
 * Returns the request headers that propagate the active-workspace selection to
 * the api-gateway, or an empty object when no selection is present (the backend
 * then defaults to the personal workspace).
 *
 * Forwards the selection as the `rsync_active_workspace_id` cookie mirror (see
 * module doc for why not X-Workspace-ID). Callers MUST NOT already set a `cookie`
 * header — this returns a fresh `cookie` key that a naive spread would clobber.
 * If you already build a Cookie header, use activeWorkspaceCookiePair() and merge
 * the pairs into one header instead.
 */
export function activeWorkspaceCookieHeader(cookies: ServerCookieReader): Record<string, string> {
  const pair = activeWorkspaceCookiePair(cookies)
  return pair ? { cookie: pair } : {}
}
