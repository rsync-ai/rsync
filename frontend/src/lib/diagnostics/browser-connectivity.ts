/**
 * The two checks only a browser can run: can THIS browser reach the API gateway, and can it
 * open the live-updates WebSocket. Every other check on admin/health runs server-side, so
 * it stays green through a proxy, CORS or public-URL mistake that leaves the app itself
 * unable to load a thing. These are what remains of the old System Test Suite page; its
 * other probes (env vars, list endpoints) restated what the Health page already shows, and
 * its env-var check failed on every published image, whose URLs arrive at runtime
 * (window.__RSYNC_RUNTIME__, lib/config/api.ts) rather than as NEXT_PUBLIC_* build values.
 */

export type BrowserProbeContext = {
  apiUrl: string
  wsUrl: string
}

export type BrowserProbe = {
  id: "api-health" | "websocket"
  name: string
  target: (ctx: BrowserProbeContext) => string
  run: (ctx: BrowserProbeContext) => Promise<string>
}

// How much of an error body is worth showing. Enough for a real API error message,
// short of pasting a document into the card.
const MAX_ERROR_BODY = 300

const WS_TIMEOUT_MS = 5000

async function fetchOk(url: string, init?: RequestInit): Promise<Response> {
  // credentials:"include" is what every authFetch call sends, and it changes what the
  // gateway's CORS answer must contain (a named origin plus Allow-Credentials, never "*").
  // Probing in the same mode is what makes a pass here mean the app's own requests pass.
  // It also caught the original defect: without it, the cookie was dropped cross-origin
  // (frontend :3000, gateway :5001) and a working backend read as broken. It goes BEFORE
  // the spread so an explicit per-call value still wins.
  const res = await fetch(url, { cache: "no-store", credentials: "include", ...init })
  if (!res.ok) {
    const text = (await res.text().catch(() => "")).trim()

    // An HTML body means the request was answered by something other than the API
    // — a 404 page, a proxy error page, a login redirect. Its text is a whole
    // document and says nothing useful; the status plus the URL is the actual
    // diagnostic.
    const isHTML = /^\s*(<!doctype html|<html)/i.test(text)
    if (!text || isHTML) {
      throw new Error(`HTTP ${res.status} ${res.statusText} from ${url}`.trim())
    }

    throw new Error(
      text.length > MAX_ERROR_BODY ? `${text.slice(0, MAX_ERROR_BODY)}… (truncated)` : text,
    )
  }
  return res
}

export const BROWSER_PROBES: BrowserProbe[] = [
  {
    id: "api-health",
    name: "API gateway",
    // /api/health, not /health. apiUrl is an ORIGIN (docker-compose.prod.yml), and only
    // PathPrefix(`/api`) is routed to the gateway — everything else goes to Next.js. A bare
    // /health asks the frontend for a page it does not have, and the resulting 404 would
    // read as the gateway being unreachable.
    target: (ctx) => `${ctx.apiUrl}/api/health`,
    run: async (ctx) => {
      await fetchOk(`${ctx.apiUrl}/api/health`)
      return "Reachable"
    },
  },
  {
    id: "websocket",
    name: "Live updates (WebSocket)",
    target: (ctx) => ctx.wsUrl,
    run: async (ctx) =>
      await new Promise<string>((resolve, reject) => {
        const ws = new WebSocket(ctx.wsUrl)
        const timeout = window.setTimeout(() => {
          try {
            ws.close()
          } catch {
            // ignore
          }
          reject(new Error(`No answer in ${WS_TIMEOUT_MS / 1000}s`))
        }, WS_TIMEOUT_MS)

        ws.onopen = () => {
          window.clearTimeout(timeout)
          ws.close()
          resolve("Connected")
        }
        ws.onerror = () => {
          window.clearTimeout(timeout)
          // The browser withholds the reason from script on purpose; the console has it.
          reject(new Error("Connection failed (the browser console has the reason)"))
        }
      }),
  },
]
