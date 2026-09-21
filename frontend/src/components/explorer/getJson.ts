// A GET whose deadline covers the body as well as the headers.

import { authFetch } from "@/lib/api/auth-fetch"

/**
 * Runs `work` under one deadline that also covers reading the body. authFetch's own
 * timeoutMs stops counting once the headers are in, so a body that stalled after them
 * would hold the Graph tab or its run panel on "Loading" for good. The signal `work` gets is aborted when the
 * deadline passes or the load is abandoned, which stops the download; the race settles
 * the promise even where a body does not listen to it.
 */
async function withDeadline<T>(parent: AbortSignal, ms: number, work: (signal: AbortSignal) => Promise<T>): Promise<T> {
  const controller = new AbortController()
  const onParentAbort = () => controller.abort(parent.reason)
  if (parent.aborted) onParentAbort()
  else parent.addEventListener("abort", onParentAbort, { once: true })
  const timer = setTimeout(
    () => controller.abort(new DOMException("The request timed out.", "TimeoutError")),
    ms,
  )
  const stopped = new Promise<never>((_, reject) => {
    const fail = () => reject(controller.signal.reason)
    if (controller.signal.aborted) fail()
    else controller.signal.addEventListener("abort", fail, { once: true })
  })
  const pending = work(controller.signal)
  // Whichever loses the race still settles later, and must not surface as unhandled.
  stopped.catch(() => {})
  pending.catch(() => {})
  try {
    return await Promise.race([pending, stopped])
  } finally {
    clearTimeout(timer)
    parent.removeEventListener("abort", onParentAbort)
  }
}

/** One GET and its body. A body that is not JSON reads as none; the caller decides what that means. */
export function getJson(url: string, signal: AbortSignal, timeoutMs: number): Promise<{ ok: boolean; status: number; body: unknown }> {
  return withDeadline(signal, timeoutMs, async (s) => {
    // authFetch does not throw on a non-2xx.
    const res = await authFetch(url, { cache: "no-store", signal: s })
    let body: unknown = null
    try {
      body = await res.json()
    } catch (err) {
      if (s.aborted) throw err
    }
    return { ok: res.ok, status: res.status, body }
  })
}
