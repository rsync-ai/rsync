"use client"

import { useEffect, useMemo, useState } from "react"
import Link from "next/link"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { PageHeader } from "@/components/layout/PageHeader"
import { AdminNav } from "@/components/admin/AdminNav"
import { AccessDeniedState, RateLimitExceededState } from "@/components/admin/AdminStates"
import { getAuthHeaders } from "@/lib/auth"
import { API_GATEWAY_URL, WS_ENDPOINTS } from "@/lib/config/api"
import { DIAGNOSTIC_TESTS, type DiagnosticTestStatus } from "@/lib/diagnostics/tests"
import { authFetch } from "@/lib/api/auth-fetch"

type TestResult = {
  status: DiagnosticTestStatus
  message?: string
  durationMs?: number
}

export default function TestSuitePage() {
  const [results, setResults] = useState<Record<string, TestResult>>({})
  const [isRunning, setIsRunning] = useState(false)
  const [accessStatus, setAccessStatus] = useState<"loading" | "allowed" | "denied" | "rate_limited" | "error">("loading")
  const [retryAfter, setRetryAfter] = useState<number | null>(null)

  const ctx = useMemo(
    () => ({
      apiUrl: API_GATEWAY_URL,
      wsUrl: WS_ENDPOINTS.API_GATEWAY,
      authHeaders: getAuthHeaders(),
    }),
    []
  )

  useEffect(() => {
    // Gate the diagnostics UI behind the backend allowlist (no frontend allowlist logic).
    // If a user isn't allowlisted, the admin API will 403 and we render "Access denied".
    const check = async () => {
      const res = await authFetch("/api/v1/admin/overview", { method: "GET" }).catch(() => null)
      if (!res) {
        setAccessStatus("error")
        return
      }
      if (res.status === 401) {
        window.location.href = `/logout?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`
        return
      }
      if (res.status === 403) {
        setAccessStatus("denied")
        return
      }
      if (res.status === 429) {
        const ra = parseInt(res.headers.get("Retry-After") || "", 10)
        setRetryAfter(Number.isFinite(ra) ? ra : null)
        setAccessStatus("rate_limited")
        return
      }
      if (!res.ok) {
        setAccessStatus("error")
        return
      }
      setAccessStatus("allowed")
    }
    check()
  }, [])

  const runAll = async () => {
    setIsRunning(true)
    setResults({})

    for (const test of DIAGNOSTIC_TESTS) {
      setResults((prev) => ({ ...prev, [test.id]: { status: "running" } }))
      const start = Date.now()
      try {
        const message = await test.run(ctx)
        setResults((prev) => ({
          ...prev,
          [test.id]: { status: "passed", message, durationMs: Date.now() - start },
        }))
      } catch (e) {
        setResults((prev) => ({
          ...prev,
          [test.id]: {
            status: "failed",
            message: e instanceof Error ? e.message : "Unknown error",
            durationMs: Date.now() - start,
          },
        }))
      }

      // Small delay between tests for readability
      // eslint-disable-next-line no-await-in-loop
      await new Promise((r) => setTimeout(r, 100))
    }

    setIsRunning(false)
  }

  const passed = Object.values(results).filter((r) => r.status === "passed").length
  const failed = Object.values(results).filter((r) => r.status === "failed").length

  return (
    <div className="space-y-6">
      <PageHeader heading="System Test Suite" description="Run diagnostics to verify system components are working" />
      <AdminNav />

      {accessStatus === "denied" ? <AccessDeniedState /> : null}
      {accessStatus === "rate_limited" ? <RateLimitExceededState retryAfterSeconds={retryAfter} /> : null}
      {accessStatus === "error" ? (
        <Card className="border border-red-200 bg-red-50 p-4 dark:border-red-900/40 dark:bg-red-950/20">
          <div className="font-semibold text-red-800 dark:text-red-200">Unable to verify admin access</div>
          <div className="mt-1 text-sm text-red-700 dark:text-red-300">
            Please refresh the page. If the problem persists, check API Gateway connectivity.
          </div>
        </Card>
      ) : null}

      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <Button
          onClick={runAll}
          disabled={isRunning || accessStatus !== "allowed"}
          className="bg-gradient-to-r from-violet-600 to-indigo-600"
        >
          {isRunning ? "Running tests..." : "Run all tests"}
        </Button>

        <div className="flex items-center gap-2 text-sm text-zinc-600 dark:text-zinc-400">
          <span>Passed: {passed}</span>
          <span>Failed: {failed}</span>
          <Button variant="outline" asChild>
            <Link href="/api/health" target="_blank">
              Open /api/health
            </Link>
          </Button>
        </div>
      </div>

      <div className="space-y-3">
        {DIAGNOSTIC_TESTS.map((t) => {
          const r = results[t.id]
          const status = r?.status || "pending"
          const styles =
            status === "passed"
              ? "border-emerald-200 bg-emerald-50 dark:border-emerald-900/40 dark:bg-emerald-950/20"
              : status === "failed"
                ? "border-red-200 bg-red-50 dark:border-red-900/40 dark:bg-red-950/20"
                : status === "running"
                  ? "border-blue-200 bg-blue-50 animate-pulse dark:border-blue-900/40 dark:bg-blue-950/20"
                  : "border-zinc-200 bg-white dark:border-zinc-800 dark:bg-zinc-900"

          const icon = status === "passed" ? "✅" : status === "failed" ? "❌" : status === "running" ? "🔄" : "⏸️"

          return (
            <Card key={t.id} className={`border-2 p-4 ${styles}`}>
              <div className="flex items-start justify-between gap-4">
                <div className="flex items-start gap-3">
                  <div className="text-2xl leading-none">{icon}</div>
                  <div>
                    <div className="font-semibold text-zinc-900 dark:text-white">{t.name}</div>
                    {t.description && <div className="text-sm text-zinc-600 dark:text-zinc-400">{t.description}</div>}
                    {r?.message && (
                      <div className={`mt-2 text-sm ${status === "failed" ? "text-red-700 dark:text-red-300" : "text-zinc-700 dark:text-zinc-300"}`}>
                        {r.message}
                      </div>
                    )}
                  </div>
                </div>

                {typeof r?.durationMs === "number" && (
                  <div className="shrink-0 font-mono text-xs text-zinc-500 dark:text-zinc-400">{r.durationMs}ms</div>
                )}
              </div>
            </Card>
          )
        })}
      </div>

      <Card className="border border-zinc-200 p-4 text-sm text-zinc-700 dark:border-zinc-800 dark:text-zinc-300">
        <div className="font-mono">
          <div>API_URL: {ctx.apiUrl}</div>
          <div>WS_URL: {ctx.wsUrl}</div>
        </div>
      </Card>
    </div>
  )
}


