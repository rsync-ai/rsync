export type DiagnosticTestStatus = "pending" | "running" | "passed" | "failed"

export type DiagnosticContext = {
  apiUrl: string
  wsUrl: string
  authHeaders: Record<string, string>
}

export type DiagnosticTest = {
  id: string
  name: string
  description?: string
  run: (ctx: DiagnosticContext) => Promise<string>
}

function assertEnv(name: string): boolean {
  return !!process.env[name]
}

// How much of an error body is worth showing. Enough for a real API error message,
// short of pasting a document into the results panel.
const MAX_ERROR_BODY = 300

async function fetchOk(url: string, init?: RequestInit): Promise<Response> {
  const res = await fetch(url, { cache: "no-store", ...init })
  if (!res.ok) {
    const text = (await res.text().catch(() => "")).trim()

    // An HTML body means the request was answered by something other than the API
    // — a 404 page, a proxy error page, a login redirect. Its text is a whole
    // document and says nothing useful; the status plus the URL is the actual
    // diagnostic. This is what made a mis-routed probe render as a wall of Next.js
    // 404 markup instead of "HTTP 404".
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

export const DIAGNOSTIC_TESTS: DiagnosticTest[] = [
  {
    id: "env",
    name: "Environment variables",
    description: "Checks client-side NEXT_PUBLIC_* config used by the UI.",
    run: async () => {
      const missing: string[] = []
      if (!assertEnv("NEXT_PUBLIC_API_URL")) missing.push("NEXT_PUBLIC_API_URL")
      if (!assertEnv("NEXT_PUBLIC_WS_URL")) missing.push("NEXT_PUBLIC_WS_URL")
      if (missing.length) throw new Error(`Missing: ${missing.join(", ")}`)
      return "Environment variables are set"
    },
  },
  {
    id: "backend-health",
    name: "Backend /health",
    description: "Verifies API Gateway health endpoint is reachable.",
    run: async (ctx) => {
      // /api/health, not /health. apiUrl is an ORIGIN (docker-compose.prod.yml:692),
      // and only PathPrefix(`/api`) is routed to the gateway (:397) — everything else
      // goes to Next.js (:706). A bare /health therefore asked the frontend for a page
      // it does not have, and this check reported the resulting 404 as the API Gateway
      // being unreachable. Every other check here already appends /api/v1/…; this one
      // was the odd one out.
      await fetchOk(`${ctx.apiUrl}/api/health`, { headers: ctx.authHeaders })
      return "API Gateway is reachable"
    },
  },
  {
    id: "websocket",
    name: "WebSocket connection",
    description: "Verifies the browser can connect to the API Gateway websocket.",
    run: async (ctx) =>
      await new Promise<string>((resolve, reject) => {
        const ws = new WebSocket(ctx.wsUrl)
        const timeout = window.setTimeout(() => {
          try {
            ws.close()
          } catch {
            // ignore
          }
          reject(new Error("Connection timeout"))
        }, 5000)

        ws.onopen = () => {
          window.clearTimeout(timeout)
          ws.close()
          resolve("WebSocket connected")
        }
        ws.onerror = () => {
          window.clearTimeout(timeout)
          reject(new Error("Connection failed"))
        }
      }),
  },
  {
    id: "connectors-api",
    name: "Connectors API",
    description: "Verifies /api/v1/connectors returns a list.",
    run: async (ctx) => {
      const res = await fetchOk(`${ctx.apiUrl}/api/v1/connectors`, { headers: ctx.authHeaders })
      const data = await res.json().catch(() => null) as Record<string, unknown> | null
      const count = (Array.isArray(data?.["connectors"]) && (data?.["connectors"] as unknown[]).length) || 0
      return `Found ${count} connectors`
    },
  },
  {
    id: "connections-api",
    name: "Connections API",
    description: "Verifies /api/v1/connections returns a list.",
    run: async (ctx) => {
      const res = await fetchOk(`${ctx.apiUrl}/api/v1/connections`, { headers: ctx.authHeaders })
      const data = await res.json().catch(() => null) as Record<string, unknown> | null
      const count = (Array.isArray(data?.["connections"]) && (data?.["connections"] as unknown[]).length) || 0
      return `Found ${count} connections`
    },
  },
  {
    id: "pipelines-api",
    name: "Pipelines API",
    description: "Verifies /api/v1/pipelines returns a list.",
    run: async (ctx) => {
      const res = await fetchOk(`${ctx.apiUrl}/api/v1/pipelines`, { headers: ctx.authHeaders })
      const data = await res.json().catch(() => null) as Record<string, unknown> | null
      const count = (Array.isArray(data?.["pipelines"]) && (data?.["pipelines"] as unknown[]).length) || 0
      return `Found ${count} pipelines`
    },
  },
]


