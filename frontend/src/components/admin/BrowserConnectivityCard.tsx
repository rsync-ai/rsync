"use client"

import { useEffect, useState, type ComponentProps } from "react"
import { Badge } from "@/components/ui/badge"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { API_GATEWAY_URL, WS_ENDPOINTS } from "@/lib/config/api"
import { BROWSER_PROBES, type BrowserProbeContext } from "@/lib/diagnostics/browser-connectivity"
import { cn } from "@/lib/utils"

/**
 * "From this browser": the two probes in lib/diagnostics/browser-connectivity.ts, run on
 * mount and again with the page's refresh. The service cards above are the server's view;
 * this is the one a user's browser has, and the two disagree exactly when a proxy, CORS or
 * PUBLIC_URL mistake blocks the app while every container is up.
 */

type ProbeResult =
  | { status: "running" }
  | { status: "passed" | "failed"; message: string; durationMs: number }

type Overall = "checking" | "ok" | "blocked"

const overallBadges: Record<Overall, { label: string; variant: ComponentProps<typeof Badge>["variant"] }> = {
  checking: { label: "Checking", variant: "outline" },
  ok: { label: "Reachable", variant: "success" },
  blocked: { label: "Blocked", variant: "destructive" },
}

// Both URLs are resolved once at module load (lib/config/api.ts), so this may be too.
const PROBE_CTX: BrowserProbeContext = { apiUrl: API_GATEWAY_URL, wsUrl: WS_ENDPOINTS.API_GATEWAY }

const dots: Record<ProbeResult["status"], string> = {
  running: "bg-transparent ring-2 ring-inset ring-zinc-400",
  passed: "bg-green-500",
  failed: "bg-red-500",
}

export function BrowserConnectivityCard({ refreshToken }: { refreshToken: number }) {
  const [results, setResults] = useState<Record<string, ProbeResult>>({})

  useEffect(() => {
    let cancelled = false
    // No reset to "running" here: a probe with no result yet already renders as running, and
    // a re-run keeps the last answer on screen until the new one lands, so the 30 s refresh
    // does not blink every row back to "checking".
    for (const probe of BROWSER_PROBES) {
      const start = performance.now()
      probe
        .run(PROBE_CTX)
        .then(
          (message): ProbeResult => ({ status: "passed", message, durationMs: performance.now() - start }),
          (e: unknown): ProbeResult => ({
            status: "failed",
            message: e instanceof Error ? e.message : "Unknown error",
            durationMs: performance.now() - start,
          }),
        )
        .then((result) => {
          if (!cancelled) setResults((prev) => ({ ...prev, [probe.id]: result }))
        })
    }
    return () => {
      cancelled = true
    }
  }, [refreshToken])

  const statuses = BROWSER_PROBES.map((p) => results[p.id]?.status ?? "running")
  const overall: Overall = statuses.includes("failed")
    ? "blocked"
    : statuses.every((s) => s === "passed")
      ? "ok"
      : "checking"

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center justify-between gap-3 text-base">
          <span>From this browser</span>
          <Badge variant={overallBadges[overall].variant}>{overallBadges[overall].label}</Badge>
        </CardTitle>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          The checks above run on the server. These run in your browser, so they catch a proxy, CORS or public-URL
          mistake that blocks the app while every service is up.
        </p>
      </CardHeader>
      <CardContent>
        <ul className="space-y-2 text-sm">
          {BROWSER_PROBES.map((probe) => {
            const result = results[probe.id] ?? { status: "running" as const }
            return (
              <li key={probe.id} data-probe={probe.id} className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5">
                <span
                  aria-hidden
                  className={cn("inline-block h-2 w-2 shrink-0 translate-y-[-1px] rounded-full", dots[result.status])}
                />
                <span className="font-medium">{probe.name}</span>
                <code className="break-all font-mono text-xs text-zinc-500 dark:text-zinc-400">{probe.target(PROBE_CTX)}</code>
                <span
                  className={cn(
                    "text-xs",
                    result.status === "failed"
                      ? "break-all text-red-600 dark:text-red-400"
                      : "text-zinc-500 dark:text-zinc-400",
                  )}
                >
                  {result.status === "running"
                    ? "Checking…"
                    : `${result.message} · ${Math.round(result.durationMs)} ms`}
                </span>
              </li>
            )
          })}
        </ul>
      </CardContent>
    </Card>
  )
}
