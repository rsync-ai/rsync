"use client"

import { useCallback, useEffect, useState, type ComponentProps } from "react"
import { RefreshCw } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { authFetch } from "@/lib/api/auth-fetch"
import { cn, formatAbsoluteTime } from "@/lib/utils"
import { formatSpanCoarse } from "@/lib/duration"

/**
 * Which commit each backend service is running (GET /api/v1/admin/drift, admin only;
 * api-gateway admin_drift.go AdminDriftCheck). The gateway asks every service's /version
 * and passes back what each one said.
 *
 * The card shows three verdicts where the route has a single `all_agree` flag, because
 * that flag is false for two very different reasons:
 *  - Two commits answered. A container was not rebuilt or restarted: the stale-deploy
 *    alarm this route exists for.
 *  - Nothing contradicts, but a service did not answer, or answered "dev" because its
 *    image was built without GIT_COMMIT (every local build that does not export it).
 *    That is not drift, only the absence of a reading, so it renders like the page's
 *    other "unknown" states rather than in red.
 */

export interface DriftServiceResult {
  service: string
  url: string
  ok: boolean
  status_code?: number
  commit?: string
  built_at?: string
  started_at?: string
  uptime_secs?: number
  error?: string
  latency_ms: number
}

export interface DriftReport {
  gathered_at: string
  all_agree: boolean
  unique_commits: string[]
  suspicions: string[]
  services: DriftServiceResult[]
}

export type DriftVerdict =
  | { kind: "in_sync"; commit: string }
  // newest: the commit of the most recently built image, when the build times can say.
  | { kind: "mixed"; commits: string[]; newest?: string }
  | { kind: "unknown"; reasons: string[] }

const serviceLabels: Record<string, string> = {
  "api-gateway": "API gateway",
  "backend-orchestrator": "Orchestrator",
  "backend-temporal-adapter": "Temporal adapter",
  "llm-service": "LLM service",
}

export function serviceLabel(service: string): string {
  return serviceLabels[service] ?? service
}

/** The compose service to rebuild or restart: the host the gateway asked. */
function composeHost(s: DriftServiceResult): string {
  try {
    return new URL(s.url).hostname || s.service
  } catch {
    return s.service
  }
}

/** The commit a service reported, or null when it did not answer or does not know. */
export function knownCommit(s: DriftServiceResult): string | null {
  // "dev" is the default every /version falls back to when GIT_COMMIT was not set at build
  // time; admin_drift.go skips it and "" for the same reason.
  return s.ok && s.commit && s.commit !== "dev" ? s.commit : null
}

function buildTime(s: DriftServiceResult): number | null {
  if (!s.built_at) return null
  const t = new Date(s.built_at).getTime()
  return Number.isNaN(t) ? null : t
}

/** The commit whose image was built last, or undefined if no single commit holds that time. */
function newestCommit(services: DriftServiceResult[]): string | undefined {
  let newest: { t: number; commits: Set<string> } | undefined
  for (const s of services) {
    const commit = knownCommit(s)
    const t = buildTime(s)
    if (!commit || t === null) continue
    if (!newest || t > newest.t) newest = { t, commits: new Set([commit]) }
    else if (t === newest.t) newest.commits.add(commit)
  }
  return newest && newest.commits.size === 1 ? [...newest.commits][0] : undefined
}

export function driftVerdict(report: DriftReport): DriftVerdict {
  const commits = [...new Set(report.services.map(knownCommit).filter((c): c is string => c !== null))]
  if (commits.length > 1) return { kind: "mixed", commits, newest: newestCommit(report.services) }

  const reasons: string[] = []
  if (report.services.length === 0) reasons.push("The gateway checked no services.")
  for (const s of report.services) {
    if (!s.ok) reasons.push(`${serviceLabel(s.service)} did not answer.`)
    else if (!knownCommit(s)) reasons.push(`${serviceLabel(s.service)} does not know its commit.`)
  }
  // The gateway may know of a reason this card does not; say what it said rather than
  // contradicting it.
  if (reasons.length === 0 && !report.all_agree) {
    reasons.push(...(report.suspicions.length > 0 ? report.suspicions : ["The gateway did not report agreement."]))
  }
  if (reasons.length > 0 || commits.length === 0) return { kind: "unknown", reasons }
  return { kind: "in_sync", commit: commits[0] }
}

export function shortCommit(commit: string): string {
  return commit.length > 12 ? commit.slice(0, 7) : commit
}

/**
 * "3d 4h", "5h 12m", "4m", "45s"; "—" when the service did not say.
 *
 * Was a copy of the same shape used by every other span in the app. Under a
 * minute it used to read "<1m"; it now says "45s", because on a drift card the
 * service that restarted forty seconds ago is the one you are looking for, and
 * "<1m" hid it behind the one that has been up for fifty-nine.
 *
 * Note the dash is for an ABSENT figure: a service that reports zero uptime has
 * just restarted, which is worth seeing, not hiding.
 */
export function uptimeLabel(secs: number | undefined): string {
  if (!secs || secs <= 0) return "—"
  return formatSpanCoarse(secs)
}

function isDriftReport(v: unknown): v is DriftReport {
  const r = v as DriftReport | null
  return !!r && typeof r.all_agree === "boolean" && Array.isArray(r.services)
}

const verdictDots: Record<DriftVerdict["kind"], string> = {
  in_sync: "bg-green-500",
  mixed: "bg-red-500",
  // Hollow, as on the service cards: no reading, not a bad one.
  unknown: "bg-transparent ring-2 ring-inset ring-zinc-400",
}

const verdictBadges: Record<DriftVerdict["kind"], { label: string; variant: ComponentProps<typeof Badge>["variant"] }> = {
  in_sync: { label: "In sync", variant: "success" },
  mixed: { label: "Mixed commits", variant: "destructive" },
  unknown: { label: "Can't tell", variant: "outline" },
}

type Load =
  | { state: "loading" }
  | { state: "ready"; report: DriftReport; fetchedAt: number; refreshFailed: boolean }
  | { state: "error" }

export function DeployedCommitCard({ refreshToken }: { refreshToken: number }) {
  const [load, setLoad] = useState<Load>({ state: "loading" })
  const [retryToken, setRetryToken] = useState(0)

  const fetchDrift = useCallback(async (): Promise<Load | null> => {
    try {
      const res = await authFetch("/api/v1/admin/drift", { method: "GET" })
      if (!res.ok) return null
      const json: unknown = await res.json()
      if (!isDriftReport(json)) return null
      return { state: "ready", report: json, fetchedAt: Date.now(), refreshFailed: false }
    } catch {
      return null
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    void fetchDrift().then((next) => {
      if (cancelled) return
      setLoad((prev) => {
        if (next) return next
        // Keep the last good answer on a failed refresh and say so.
        return prev.state === "ready" ? { ...prev, refreshFailed: true } : { state: "error" }
      })
    })
    return () => {
      cancelled = true
    }
  }, [fetchDrift, refreshToken, retryToken])

  const verdict = load.state === "ready" ? driftVerdict(load.report) : null

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex items-center justify-between gap-3 text-base">
          <span>Deployed commit</span>
          {verdict && (
            <span className="flex shrink-0 items-center gap-1.5">
              <span
                aria-hidden
                data-status-dot={verdict.kind}
                className={cn("inline-block h-2.5 w-2.5 rounded-full", verdictDots[verdict.kind])}
              />
              <Badge variant={verdictBadges[verdict.kind].variant}>{verdictBadges[verdict.kind].label}</Badge>
            </span>
          )}
        </CardTitle>
        <p className="text-sm text-zinc-500 dark:text-zinc-400">
          The commit each backend service was built from. They should all match; a mismatch means a container
          was not rebuilt or restarted.
        </p>
      </CardHeader>
      <CardContent>
        {load.state === "loading" ? (
          <p className="text-sm text-zinc-500 dark:text-zinc-400">Asking each service for its commit…</p>
        ) : load.state === "error" ? (
          <div className="flex flex-wrap items-center gap-3 text-sm">
            <span className="text-red-600 dark:text-red-400">Could not check the deployed commits.</span>
            <Button variant="outline" size="sm" onClick={() => setRetryToken((n) => n + 1)}>
              <RefreshCw className="mr-1.5 h-3.5 w-3.5" />
              Retry
            </Button>
          </div>
        ) : (
          <ReadyReport
            report={load.report}
            verdict={verdict as DriftVerdict}
            fetchedAt={load.fetchedAt}
            refreshFailed={load.refreshFailed}
          />
        )}
      </CardContent>
    </Card>
  )
}

function Summary({ verdict, report }: { verdict: DriftVerdict; report: DriftReport }) {
  if (verdict.kind === "in_sync") {
    return (
      <p className="text-sm">
        All {report.services.length} services run commit{" "}
        <code className="font-mono text-xs" title={verdict.commit}>
          {shortCommit(verdict.commit)}
        </code>
        .
      </p>
    )
  }
  if (verdict.kind === "mixed") {
    return (
      <p className="text-sm text-red-700 dark:text-red-400">
        {verdict.commits.length} different commits are running, so at least one container is on an old build.
        {verdict.newest
          ? " Rebuild and restart the services marked below."
          : " Rebuild and restart the services that are not on the commit you meant to deploy."}
      </p>
    )
  }
  const noCommit = report.services.some((s) => s.ok && !knownCommit(s))
  return (
    <div className="space-y-1.5 text-sm">
      <p>Can&apos;t confirm that every service runs the same commit:</p>
      <ul className="list-disc space-y-0.5 pl-5 text-zinc-600 dark:text-zinc-300">
        {verdict.reasons.map((r) => (
          <li key={r}>{r}</li>
        ))}
      </ul>
      {noCommit && (
        <p className="text-xs text-zinc-500 dark:text-zinc-400">
          An image built without <code className="font-mono">GIT_COMMIT</code> reports{" "}
          <code className="font-mono">dev</code>. Published images carry it, and{" "}
          <code className="font-mono">scripts/deploy-service.sh</code> exports it for local builds.
        </p>
      )}
    </div>
  )
}

function ReadyReport({
  report,
  verdict,
  fetchedAt,
  refreshFailed,
}: {
  report: DriftReport
  verdict: DriftVerdict
  fetchedAt: number
  refreshFailed: boolean
}) {
  const newest = verdict.kind === "mixed" ? verdict.newest : undefined

  return (
    <div className="space-y-3">
      {refreshFailed && (
        <p role="status" className="text-xs text-amber-700 dark:text-amber-400">
          Could not refresh. Showing the answer from {new Date(fetchedAt).toLocaleTimeString()}.
        </p>
      )}

      <Summary verdict={verdict} report={report} />

      {report.services.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[640px] text-sm">
            <thead>
              <tr className="border-b text-left text-xs text-zinc-500 dark:text-zinc-400">
                <th scope="col" className="py-2 pr-3 font-medium">Service</th>
                <th scope="col" className="py-2 pr-3 font-medium">Commit</th>
                <th scope="col" className="py-2 pr-3 font-medium">Built</th>
                <th scope="col" className="py-2 pr-3 font-medium">Running for</th>
                <th scope="col" className="py-2 font-medium">Check</th>
              </tr>
            </thead>
            <tbody>
              {report.services.map((s) => {
                const commit = knownCommit(s)
                const behind = !!newest && !!commit && commit !== newest
                const built = s.built_at ? formatAbsoluteTime(s.built_at) : ""
                return (
                  <tr key={s.service} data-service={s.service} className="border-b align-top last:border-0">
                    <td className="py-2 pr-3">
                      <div>{serviceLabel(s.service)}</div>
                      <div className="font-mono text-xs text-zinc-500 dark:text-zinc-400">{composeHost(s)}</div>
                    </td>
                    <td className="py-2 pr-3">
                      {commit ? (
                        <code
                          title={commit}
                          className={cn("font-mono text-xs", behind && "font-semibold text-red-600 dark:text-red-400")}
                        >
                          {shortCommit(commit)}
                        </code>
                      ) : s.ok ? (
                        <span
                          className="text-xs italic text-zinc-500 dark:text-zinc-400"
                          title="The image was built without GIT_COMMIT."
                        >
                          unknown
                        </span>
                      ) : (
                        <span className="text-zinc-400">—</span>
                      )}
                      {behind && (
                        <div className="text-xs text-red-600 dark:text-red-400">
                          Older than the newest build ({shortCommit(newest as string)})
                        </div>
                      )}
                    </td>
                    <td className="whitespace-nowrap py-2 pr-3">{built || <span className="text-zinc-400">—</span>}</td>
                    <td className="whitespace-nowrap py-2 pr-3">
                      <span title={s.started_at ? formatAbsoluteTime(s.started_at) : undefined}>
                        {uptimeLabel(s.uptime_secs)}
                      </span>
                    </td>
                    <td className="py-2">
                      {s.ok ? (
                        <span className="text-zinc-500 dark:text-zinc-400">answered</span>
                      ) : (
                        <span className="break-all text-xs text-red-600 dark:text-red-400">
                          {s.status_code ? `HTTP ${s.status_code}` : s.error || "did not answer"}
                        </span>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      )}

      <p className="text-xs text-zinc-500 dark:text-zinc-400">
        The web app is not checked: its image does not record a commit.
      </p>
    </div>
  )
}
