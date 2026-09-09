/**
 * Read a count endpoint for a dashboard-style widget, keeping "we could not
 * read this" distinct from "the answer is zero".
 *
 * Two defects live behind this helper, and they compounded.
 *
 * 1. The call sites used a bare relative `fetch("/api/v1/...")`. A relative URL
 *    resolves against the *page* origin, which is the Next.js frontend on :3000
 *    — not the api-gateway on :5001. On a self-host install where the two are
 *    separate origins, every one of those requests 404s at the frontend and
 *    never reaches the backend at all. It works in development and behind a
 *    single-origin reverse proxy, which is why it survived. `authFetch` resolves
 *    the base through `@/lib/config/api`, which already handles the runtime-
 *    injected `window.__RSYNC_RUNTIME__.apiUrl` that self-host installs use
 *    because `NEXT_PUBLIC_*` is baked at build time.
 *
 * 2. The failure was then swallowed into a zero. `r.ok ? r.json() : null`
 *    followed by `payload?.total ?? 0` turns a 404 into a count of 0, and the
 *    widget *overwrites* its server-rendered value with that zero. The `catch`
 *    blocks that claimed to "keep SSR values on failure" could not fire:
 *    `Promise.allSettled` does not reject, and a non-2xx response is not a
 *    thrown error. So the one failure mode the components were written to defend
 *    against — a stat card silently reading zero — was the failure mode they
 *    produced.
 *
 * Returning `undefined` for an unreadable stat lets the caller leave its
 * existing value alone, which is the behaviour the original comments described.
 */

import { authFetch } from "@/lib/api/auth-fetch"

/**
 * GET `path` through the API base resolver and parse it as JSON.
 *
 * Resolves to `undefined` — never a zero-shaped object — when the read fails for
 * any reason: a non-2xx status, a network fault, or a body that is not JSON.
 * Callers must treat `undefined` as "leave the current value in place".
 */
export async function readStatOrUnknown<T = unknown>(path: string): Promise<T | undefined> {
  try {
    const res = await authFetch(path)
    if (!res.ok) {
      // Loud enough to find in a browser console, quiet enough not to be a toast:
      // the user's view is unchanged, so this is diagnostic, not an interruption.
      console.warn(`[stats] ${path} → HTTP ${res.status}; leaving the current value unchanged`)
      return undefined
    }
    return (await res.json()) as T
  } catch (err) {
    console.warn(`[stats] ${path} could not be read; leaving the current value unchanged`, err)
    return undefined
  }
}
