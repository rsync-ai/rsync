"use client"

// First-run onboarding shown to a fresh workspace: a "Get your first sync
// running" checklist that auto-ticks as the user completes each step and
// retires itself once they've run a successful sync (activation).
//
// It re-verifies counts from the browser on mount: the dashboard's SSR count
// fetch can silently return zeros when the auth cookie isn't forwarded (same
// reason DashboardStatsRefresher re-fetches), so we must never keep showing
// onboarding to a user who has actually activated.

import { useCallback, useEffect, useMemo, useState } from "react"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { Card } from "@/components/ui/card"
import { authFetch } from "@/lib/api/auth-fetch"
import { Check, Database, ArrowRightLeft, Sparkles, Search, Wand2, Loader2, LucideIcon } from "lucide-react"

export interface OnboardingCounts {
  sourceCount: number
  destinationCount: number
  pipelineCount: number
  queryCount: number
}

// Shape of GET /api/v1/demo/status. `available` is false on cloud and on any
// self-host that hasn't set RSYNC_DEMO_DESTINATION_DSN, so the card below simply
// never renders there — no edition check on the client.
interface DemoStatus {
  available: boolean
  destination_database?: string
}

interface Step {
  id: string
  icon: LucideIcon
  title: string
  desc: string
  href: string
  cta: string
  done: boolean
}

export function FirstRunOnboarding({ initial }: { initial: OnboardingCounts }) {
  const router = useRouter()
  const [counts, setCounts] = useState<OnboardingCounts>(initial)
  const [demo, setDemo] = useState<DemoStatus | null>(null)
  const [seeding, setSeeding] = useState(false)
  const [seedError, setSeedError] = useState<string | null>(null)

  const fetchCounts = useCallback(async (): Promise<OnboardingCounts | null> => {
    try {
      const [pipesR, srcR, dstR, usageR] = await Promise.allSettled([
        fetch("/api/v1/pipelines", { credentials: "include" }).then((r) => (r.ok ? r.json() : null)),
        fetch("/api/v1/connections?type=source", { credentials: "include" }).then((r) => (r.ok ? r.json() : null)),
        fetch("/api/v1/connections?type=destination", { credentials: "include" }).then((r) => (r.ok ? r.json() : null)),
        fetch("/api/v1/usage", { credentials: "include" }).then((r) => (r.ok ? r.json() : null)),
      ])
      const val = (r: PromiseSettledResult<any>) => (r.status === "fulfilled" ? r.value : null)
      const p = val(pipesR)
      const s = val(srcR)
      const d = val(dstR)
      const u = val(usageR)
      return {
        pipelineCount: p?.total ?? p?.pipelines?.length ?? 0,
        sourceCount: s?.total ?? s?.connections?.length ?? 0,
        destinationCount: d?.total ?? d?.connections?.length ?? 0,
        // Step 4 = "ask a question of your data". queries_used is the NL→SQL
        // query count (usage.go), which fires for CDC pipelines too — unlike a
        // "successful execution", which CDC/streaming syncs never record.
        queryCount: u?.queries_used ?? 0,
      }
    } catch {
      /* keep SSR values on failure */
      return null
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    ;(async () => {
      const fresh = await fetchCounts()
      if (!cancelled && fresh) setCounts(fresh)
    })()
    return () => {
      cancelled = true
    }
  }, [fetchCounts])

  // Is a bundled demo destination available? Only self-host stacks that set
  // RSYNC_DEMO_DESTINATION_DSN answer yes; a failure is treated as "no" so the
  // checklist degrades to exactly what it was before.
  useEffect(() => {
    let cancelled = false
    ;(async () => {
      try {
        const res = await authFetch("/api/v1/demo/status")
        if (cancelled || !res.ok) return
        setDemo(await res.json())
      } catch {
        /* demo card just doesn't appear */
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  const startDemo = useCallback(async () => {
    setSeeding(true)
    setSeedError(null)
    try {
      const res = await authFetch("/api/v1/demo/seed", {
        method: "POST",
        // Seeding tests both connections before saving them, and the destination
        // connector may have to be started on demand first. That is the slow
        // path this timeout has to cover, not a round trip.
        timeoutMs: 180_000,
      })
      const body = await res.json().catch(() => null)
      if (!res.ok) {
        setSeedError(body?.message || "Could not set up the demo. Check that the stack finished starting, then try again.")
        return
      }
      const fresh = await fetchCounts()
      if (fresh) setCounts(fresh)
      router.push("/chat")
    } catch {
      setSeedError("Could not reach the server. Check that the stack finished starting, then try again.")
    } finally {
      setSeeding(false)
    }
  }, [fetchCounts, router])

  const steps: Step[] = useMemo(
    () => [
      {
        id: "source",
        icon: Database,
        title: "Connect a source",
        desc: "Where your data lives — Postgres, MySQL, Shopify, and more",
        href: "/connections?type=source",
        cta: "Add source",
        done: counts.sourceCount > 0,
      },
      {
        id: "dest",
        icon: ArrowRightLeft,
        title: "Connect a destination",
        desc: "Where it should land — Postgres, BigQuery, Snowflake, S3…",
        href: "/connections?type=destination",
        cta: "Add destination",
        done: counts.destinationCount > 0,
      },
      {
        id: "build",
        icon: Sparkles,
        title: "Build your first pipeline in plain English",
        desc: "The agent picks the tables, schema, and sync mode for you",
        href: "/chat",
        cta: "Open builder",
        done: counts.pipelineCount > 0,
      },
      {
        id: "explore",
        icon: Search,
        title: "Run it, then ask a question of your data",
        desc: "Query the synced rows in natural language in the Explorer",
        href: "/explorer",
        cta: "Open Explorer",
        done: counts.queryCount > 0,
      },
    ],
    [counts],
  )

  // Retire onboarding once every step is done — the last step (asking a question
  // in the Explorer) is the real activation signal, and unlike a "successful
  // execution" it also fires for CDC/streaming pipelines, which never record an
  // executions row. The client re-verifies counts above, so this also covers the
  // SSR-false-positive-zeros case for established users.
  if (steps.every((s) => s.done)) return null

  const doneCount = steps.filter((s) => s.done).length
  const currentIndex = steps.findIndex((s) => !s.done)

  return (
    <section aria-label="Get started with rsync" className="space-y-4">
      {/* Zero-credential try-it path. Rendered only when this deployment ships a
          demo destination and the user is still missing a half. On cloud
          /demo/status answers available:false, so none of this ever appears —
          there is no edition check here, just the absence of a destination. */}
      {demo?.available && (counts.sourceCount === 0 || counts.destinationCount === 0) && (
        <Card className="border-violet-200 bg-violet-50/50 p-4 dark:border-violet-900/50 dark:bg-violet-950/20 sm:p-5">
          <div className="flex flex-wrap items-start gap-3">
            <span className="flex h-9 w-9 flex-none items-center justify-center rounded-lg bg-violet-100 text-violet-600 dark:bg-violet-900/40 dark:text-violet-300">
              <Wand2 className="h-4 w-4" />
            </span>
            <div className="min-w-0 flex-1">
              <p className="font-semibold text-zinc-900 dark:text-zinc-100">No credentials handy? Try it with sample data</p>
              <p className="mt-0.5 text-sm text-zinc-600 dark:text-zinc-400">
                Adds a built-in sample source and the{" "}
                <span className="font-mono text-xs">{demo.destination_database ?? "demo"}</span> database that ships with this
                stack, so you can build and run a real pipeline end to end — nothing to sign up for.
              </p>
              {seedError && (
                <p role="alert" className="mt-2 text-sm text-red-600 dark:text-red-400">
                  {seedError}
                </p>
              )}
            </div>
            <button
              type="button"
              onClick={startDemo}
              disabled={seeding}
              className="inline-flex flex-none items-center gap-1.5 rounded-md bg-violet-600 px-3 py-2 text-sm font-semibold text-white hover:bg-violet-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-violet-500 focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-70"
            >
              {seeding ? (
                <>
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  Setting up…
                </>
              ) : (
                "Start with sample data"
              )}
            </button>
          </div>
        </Card>
      )}

      {/* Activation checklist */}
      <Card className="overflow-hidden border-violet-100 dark:border-violet-900/50">
        <div className="flex items-center justify-between border-b border-zinc-100 bg-zinc-50/70 px-4 py-3 dark:border-zinc-800 dark:bg-zinc-900/40 sm:px-5">
          <div>
            <p className="font-semibold text-zinc-900 dark:text-zinc-100">Get your first sync running</p>
            <div className="mt-1.5 h-1.5 w-40 overflow-hidden rounded-full bg-zinc-200 dark:bg-zinc-800">
              <div
                className="h-full rounded-full bg-gradient-to-r from-violet-500 to-indigo-500 transition-[width] duration-500"
                style={{ width: `${(doneCount / steps.length) * 100}%` }}
              />
            </div>
          </div>
          <span className="whitespace-nowrap text-sm tabular-nums text-zinc-500 dark:text-zinc-400">
            {doneCount} of {steps.length} done
          </span>
        </div>

        <ol className="divide-y divide-zinc-100 dark:divide-zinc-800">
          {steps.map((step, i) => {
            const isUpcoming = currentIndex !== -1 && i > currentIndex
            const StepIcon = step.icon
            return (
              <li key={step.id} className="flex items-center gap-3 px-4 py-3 sm:px-5">
                <span
                  className={
                    "flex h-6 w-6 flex-none items-center justify-center rounded-full " +
                    (step.done
                      ? "bg-emerald-100 text-emerald-600 dark:bg-emerald-900/40 dark:text-emerald-400"
                      : i === currentIndex
                        ? "border-[1.5px] border-violet-500 text-violet-600 dark:text-violet-400"
                        : "border-[1.5px] border-dashed border-zinc-300 text-zinc-400 dark:border-zinc-700")
                  }
                >
                  {step.done ? <Check className="h-3.5 w-3.5" strokeWidth={3} /> : <StepIcon className="h-3.5 w-3.5" />}
                </span>
                <div className="min-w-0 flex-1">
                  <p
                    className={
                      "text-sm font-medium " +
                      (step.done
                        ? "text-zinc-400 line-through decoration-zinc-300 dark:text-zinc-500 dark:decoration-zinc-700"
                        : "text-zinc-900 dark:text-zinc-100")
                    }
                  >
                    {step.title}
                  </p>
                  <p className="truncate text-xs text-zinc-400 dark:text-zinc-500">{step.desc}</p>
                </div>
                {step.done ? (
                  <span className="flex-none text-xs font-medium text-emerald-600 dark:text-emerald-400">Done</span>
                ) : (
                  <Link
                    href={step.href}
                    className={
                      "flex-none whitespace-nowrap rounded-md px-1 text-xs font-semibold focus:outline-none focus-visible:ring-2 focus-visible:ring-violet-500 " +
                      (isUpcoming
                        ? "text-zinc-400 hover:text-violet-600 dark:text-zinc-500 dark:hover:text-violet-400"
                        : "text-violet-600 hover:text-violet-700 dark:text-violet-400")
                    }
                  >
                    {step.cta} →
                  </Link>
                )}
              </li>
            )
          })}
        </ol>
      </Card>
    </section>
  )
}
