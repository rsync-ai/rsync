import { API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL, WS_ENDPOINTS, getApiUrl } from "@/lib/config/api"
import { frontendStartupProblems } from "@/instrumentation"

export const dynamic = "force-dynamic"

type HealthResponse = {
  status: "healthy" | "unhealthy"
  timestamp: string
  checks: {
    frontend: { status: "ok" }
    environment: {
      status: "ok" | "error"
      variables: {
        NEXT_PUBLIC_API_URL: boolean
        NEXT_PUBLIC_WS_URL: boolean
      }
      // Names (never values) of server settings a production frontend needs but
      // was not given; the startup log says what each one breaks. Settings whose
      // absence says something about a credential are left out: this route needs no login.
      missingSettings: string[]
    }
    backend: { status: "ok" | "error"; code?: number; message?: string; data?: unknown }
  }
}

export async function GET() {
  const results: HealthResponse = {
    status: "unhealthy",
    timestamp: new Date().toISOString(),
    checks: {
      frontend: { status: "ok" },
      environment: {
        status: "error",
        variables: {
          NEXT_PUBLIC_API_URL: !!process.env.NEXT_PUBLIC_API_URL,
          NEXT_PUBLIC_WS_URL: !!process.env.NEXT_PUBLIC_WS_URL,
        },
        missingSettings: frontendStartupProblems(process.env)
          .filter((problem) => !problem.sensitive)
          .map((problem) => problem.setting),
      },
      backend: { status: "error" },
    },
  }

  // Env check
  const envOk = Object.values(results.checks.environment.variables).every(Boolean)
  results.checks.environment.status = envOk ? "ok" : "error"

  // Backend health check (use internal URL on server)
  const gatewayBase = getApiUrl(API_GATEWAY_URL, API_GATEWAY_URL_INTERNAL)
  try {
    const res = await fetch(`${gatewayBase}/health`, {
      cache: "no-store",
      // AbortSignal.timeout is available in modern runtimes, but keep a safe fallback.
      signal: (AbortSignal as any).timeout ? (AbortSignal as any).timeout(5000) : undefined,
    })

    if (res.ok) {
      const data = await res.json().catch(() => null)
      results.checks.backend = { status: "ok", data }
    } else {
      results.checks.backend = { status: "error", code: res.status }
    }
  } catch (e) {
    results.checks.backend = {
      status: "error",
      message: e instanceof Error ? e.message : "Connection failed",
    }
  }

  // Overall status: backend must be ok; env may be error in some deployments but we surface it.
  const allOk = results.checks.frontend.status === "ok" && results.checks.backend.status === "ok"
  results.status = allOk ? "healthy" : "unhealthy"

  // Include computed URLs for debugging (non-sensitive)
  const responseBody = {
    ...results,
    urls: {
      apiUrl: API_GATEWAY_URL,
      wsUrl: WS_ENDPOINTS.API_GATEWAY,
    },
  }

  return Response.json(responseBody, { status: allOk ? 200 : 503 })
}


