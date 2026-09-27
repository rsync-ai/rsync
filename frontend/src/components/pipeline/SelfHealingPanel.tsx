"use client"

/**
 * SelfHealingPanel — the operator's view of what the heal agent did and why.
 *
 * Until this panel existed the healer was writing a complete audit trail that no
 * screen read. Every decision, every executed action and every verdict landed in
 * pipeline_run_events and stayed there: the only way to find out whether the
 * agent had looked at a broken pipeline was to query Postgres by hand. On
 * production that hid a real defect for weeks — seven of the first eight
 * decisions were the "no rule matched" fallback, which is exactly the kind of
 * thing a status board is supposed to make obvious.
 *
 * Three things about healer rows shape this component (see eventNormalizer.ts):
 *
 *   1. They carry no stage_id, so anything that groups by stage drops them.
 *   2. They carry no seq, so on an unfiltered page `ORDER BY seq DESC NULLS
 *      LAST` sorts them behind every ordinary event — on a busy pipeline a
 *      limit=200 fetch can miss them entirely. Hence the server-side
 *      `event_types` filter rather than fetching everything and filtering here.
 *   3. They never reach the WebSocket — the healer writes straight to Postgres
 *      with no Kafka producer. So this polls. A socket subscription would look
 *      correct and never fire.
 */

import { useCallback, useEffect, useState } from "react"
import Link from "next/link"
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  HelpCircle,
  RefreshCw,
  ShieldQuestion,
  Wrench,
  XCircle,
} from "lucide-react"

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { API_ENDPOINTS } from "@/lib/config/api"
import { authFetch } from "@/lib/api/auth-fetch"
import {
  extractHealerActivity,
  healSucceeded,
  HEALER_EVENT_TYPES,
  openEscalations,
  type HealerActivity,
  type PipelineRunEvent,
} from "@/lib/pipeline/eventNormalizer"
import { formatAbsoluteTime } from "@/lib/utils"

/** Matches the healer's own PollInterval; there is nothing faster to see. */
const POLL_MS = 60_000

/** Human labels. Falling back to the raw token is deliberate — a new action
 *  should show up as an unpolished string, never vanish.
 *
 *  Keys are the wire tokens, NOT the Go identifiers: `escalate_to_human`, not
 *  `ActionEscalate`. Sourced from `pkg/diagnose/diagnose.go` (regenerate_connector
 *  … re_snapshot) and `internal/agents/heal/auto_executors.go` (the three
 *  auto-executor actions). ACTION_TOKENS below is the same list as data, so a
 *  test can hold this map to it. */
const ACTION_LABELS: Record<string, string> = {
  regenerate_connector: "Regenerate connector",
  refresh_auth: "Refresh credentials",
  backoff_retry: "Retry the run",
  request_user_config: "Ask for configuration",
  cleanup_cdc_resources: "Clean up CDC resources",
  repair_ownership_row: "Repair ownership row",
  sweep_zombie_execution: "Close a stalled run",
  re_snapshot: "Re-snapshot the source",
  escalate_to_human: "Escalate to a human",
  no_op: "Take no action",
}

/** Every `Action` token the orchestrator can put in a decision payload. */
export const ACTION_TOKENS = Object.keys(ACTION_LABELS)

const CATEGORY_LABELS: Record<string, string> = {
  connector_bug: "Connector defect",
  schema_drift: "Schema drift",
  auth_expired: "Credentials expired",
  auth_scope: "Insufficient permissions",
  network: "Network / transport",
  rate_limit: "Rate limit",
  dest_capacity: "Destination capacity",
  user_config: "Configuration",
  orchestration: "Orchestration",
  unknown: "Unclassified",
}

/** Every `Category` token the orchestrator can put in a decision payload. */
export const CATEGORY_TOKENS = Object.keys(CATEGORY_LABELS)

const VERDICT_LABELS: Record<string, string> = {
  healed: "Healed",
  self_resolved: "Recovered on its own",
  failed_again: "Failed again",
  inconclusive: "Inconclusive",
  superseded: "Superseded",
}

function label(map: Record<string, string>, key?: string): string {
  if (!key) return "—"
  return map[key] || key.replace(/_/g, " ")
}

function timeAgo(iso: string): string {
  const t = new Date(iso).getTime()
  if (!Number.isFinite(t)) return ""
  const secs = Math.max(0, Math.floor((Date.now() - t) / 1000))
  if (secs < 60) return `${secs}s ago`
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`
  return `${Math.floor(secs / 86400)}d ago`
}

/**
 * The confidence bands are the healer's own, not display thresholds:
 * >= 0.85 (diagnose.AutoExecuteBand) it acts; >= 0.50 (heal.HITLBand) it asks;
 * below that it escalates. Showing the band alongside the number is what makes a
 * 0.80 legible as "deliberately just under the auto bar" rather than "low".
 */
function confidenceBand(c?: number): { text: string; cls: string } {
  if (typeof c !== "number") return { text: "—", cls: "text-gray-500 dark:text-gray-400" }
  const pct = `${Math.round(c * 100)}%`
  if (c >= 0.85) return { text: `${pct} · acts automatically`, cls: "text-green-700 dark:text-green-400" }
  if (c >= 0.5) return { text: `${pct} · asks first`, cls: "text-amber-700 dark:text-amber-400" }
  return { text: `${pct} · escalates`, cls: "text-gray-600 dark:text-gray-400" }
}

function OutcomeBadge({ a }: { a: HealerActivity }) {
  if (a.kind === "verdict") {
    const good = a.verdict === "healed" || a.verdict === "self_resolved"
    const bad = a.verdict === "failed_again"
    return (
      <Badge
        variant="outline"
        className={
          good
            ? "border-green-400 text-green-700 dark:text-green-300"
            : bad
              ? "border-red-400 text-red-700 dark:text-red-300"
              : "border-gray-300 text-gray-600 dark:text-gray-400"
        }
      >
        {label(VERDICT_LABELS, a.verdict)}
      </Badge>
    )
  }

  const o = a.outcome
  if (o === "auto_executed")
    return (
      <Badge variant="outline" className="border-green-400 text-green-700 dark:text-green-300">
        Acted
      </Badge>
    )
  if (o === "hitl_requested")
    return (
      <Badge variant="outline" className="border-amber-400 text-amber-700 dark:text-amber-300">
        Needs approval
      </Badge>
    )
  if (o === "action_failed")
    return (
      <Badge variant="outline" className="border-red-400 text-red-700 dark:text-red-300">
        Action failed
      </Badge>
    )
  if (o === "escalated")
    return (
      <Badge variant="outline" className="border-gray-300 text-gray-600 dark:text-gray-400">
        Escalated
      </Badge>
    )
  if (o === "no_action_defined")
    return (
      <Badge variant="outline" className="border-gray-300 text-gray-600 dark:text-gray-400">
        No action available
      </Badge>
    )
  return null
}

function KindIcon({ a }: { a: HealerActivity }) {
  const cls = "h-4 w-4 shrink-0 mt-0.5"
  if (a.kind === "verdict") {
    if (a.verdict === "healed" || a.verdict === "self_resolved") return <CheckCircle2 className={`${cls} text-green-600 dark:text-green-400`} />
    if (a.verdict === "failed_again") return <XCircle className={`${cls} text-red-600 dark:text-red-400`} />
    return <HelpCircle className={`${cls} text-gray-400`} />
  }
  if (a.kind === "action") return <Wrench className={`${cls} text-blue-600 dark:text-blue-400`} />
  if (a.outcome === "escalated" || a.category === "unknown")
    return <ShieldQuestion className={`${cls} text-gray-500 dark:text-gray-400`} />
  if (a.outcome === "action_failed") return <XCircle className={`${cls} text-red-600 dark:text-red-400`} />
  if (a.outcome === "hitl_requested") return <AlertTriangle className={`${cls} text-amber-600 dark:text-amber-400`} />
  return <Activity className={`${cls} text-blue-600 dark:text-blue-400`} />
}

function ActivityRow({ a }: { a: HealerActivity }) {
  const [open, setOpen] = useState(false)
  const band = confidenceBand(a.confidence)

  const headline =
    a.kind === "verdict"
      ? `${label(ACTION_LABELS, a.action)} — ${label(VERDICT_LABELS, a.verdict)}`
      : a.kind === "action"
        ? label(ACTION_LABELS, a.action)
        : `${label(CATEGORY_LABELS, a.category)} → ${label(ACTION_LABELS, a.action)}`

  const hasDetail = Boolean(
    a.rationale || a.errorMessage || a.failureSignature || a.hitlPrompt || a.memoryNote || a.description
  )

  return (
    <div className="border-b border-gray-200 dark:border-gray-800 last:border-b-0 py-2.5">
      <div className="flex items-start gap-2.5">
        <KindIcon a={a} />
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-sm font-medium text-gray-900 dark:text-gray-100">{headline}</span>
            <OutcomeBadge a={a} />
            {a.kind === "decision" && typeof a.confidence === "number" && (
              <span className={`text-xs ${band.cls}`}>{band.text}</span>
            )}
            <span className="text-xs text-gray-400 dark:text-gray-500">{timeAgo(a.timestamp)}</span>
          </div>

          {a.rationale && (
            <p className="mt-1 text-xs text-gray-600 dark:text-gray-400">{a.rationale}</p>
          )}
          {a.kind === "action" && a.description && (
            <p className="mt-1 text-xs text-gray-600 dark:text-gray-400">{a.description}</p>
          )}

          {/* The HITL prompt is the one thing here that is addressed to a person.
              There is no approve endpoint for the heal agent — the prompt tells
              the operator what the healer would do, and they act on it
              themselves. Presenting it as a button would be a lie. */}
          {a.hitlPrompt && (
            <div className="mt-1.5 rounded border border-amber-200 bg-amber-50 px-2 py-1.5 text-xs text-amber-800 dark:border-amber-800 dark:bg-amber-950/30 dark:text-amber-300">
              {a.hitlPrompt}
            </div>
          )}

          {a.memoryNote && (
            <p className="mt-1 text-xs italic text-purple-700 dark:text-purple-400">
              Adjusted from past attempts: {a.memoryNote}
            </p>
          )}

          {hasDetail && (
            <button
              type="button"
              onClick={() => setOpen((v) => !v)}
              className="mt-1 inline-flex items-center gap-1 text-xs text-gray-500 hover:text-gray-800 dark:text-gray-400 dark:hover:text-gray-200"
            >
              {open ? <ChevronDown className="h-3 w-3" /> : <ChevronRight className="h-3 w-3" />}
              {open ? "Hide evidence" : "Show evidence"}
            </button>
          )}

          {open && (
            <dl className="mt-1.5 space-y-1.5 rounded bg-gray-50 p-2 text-xs dark:bg-gray-900/50">
              {a.errorMessage && (
                <div>
                  <dt className="font-medium text-gray-700 dark:text-gray-300">Error it diagnosed</dt>
                  <dd className="break-words font-mono text-[11px] text-gray-600 dark:text-gray-400">
                    {a.errorMessage}
                  </dd>
                </div>
              )}
              {/* signature.go FailureSignature: category|status|source->dest|error,
                  with ids, times and numbers replaced and the error part cut at 160
                  characters. It groups repeats; it does not quote the error, which
                  is the row above. */}
              {a.failureSignature && (
                <div>
                  <dt className="font-medium text-gray-700 dark:text-gray-300">
                    Grouped as{" "}
                    <span className="font-normal text-gray-500 dark:text-gray-400">
                      (ids and numbers replaced, error cut at 160 characters)
                    </span>
                  </dt>
                  <dd className="break-words font-mono text-[11px] text-gray-600 dark:text-gray-400">
                    {a.failureSignature}
                  </dd>
                </div>
              )}
              {a.executorStatus && (
                <div>
                  <dt className="font-medium text-gray-700 dark:text-gray-300">Executor status</dt>
                  <dd className="font-mono text-[11px] text-gray-600 dark:text-gray-400">{a.executorStatus}</dd>
                </div>
              )}
              {a.successorExecutionId && (
                <div>
                  <dt className="font-medium text-gray-700 dark:text-gray-300">Verified against run</dt>
                  <dd className="font-mono text-[11px] text-gray-600 dark:text-gray-400">
                    {a.successorExecutionId}
                  </dd>
                </div>
              )}
              <div className="text-[11px] text-gray-400 dark:text-gray-500">
                {a.eventType}
                {a.attemptId ? ` · attempt #${a.attemptId}` : ""}
                {" · "}
                {formatAbsoluteTime(a.timestamp)}
              </div>
            </dl>
          )}
        </div>
      </div>
    </div>
  )
}

/**
 * What watches this pipeline, in one sentence per watcher. Both are on unless an
 * operator turned them off (cmd/orchestrator/main.go: ENABLE_HEAL_WORKER,
 * ENABLE_CDC_SENTINEL), and neither says so to the UI, so this states the
 * default rather than a live flag.
 *
 * - The heal agent (heal/worker.go) reads every run that ended in failure, in
 *   either mode, and acts or asks depending on its confidence.
 * - The CDC Sentinel (sentinel/cdc_sentinel.go) polls a running stream: it
 *   restarts a FAILED change-capture connector a bounded number of times, then
 *   stops the pipeline and escalates, and it raises alerts for source lag, sink
 *   lag and replication-slot WAL growth.
 */
function Watchers({ pipelineType }: { pipelineType?: "etl" | "cdc" }) {
  return (
    <ul className="space-y-0.5 text-xs text-gray-600 dark:text-gray-400">
      <li>
        The heal agent reads every failed run, fixes it when it is confident, and asks you when it is not.
      </li>
      {pipelineType === "cdc" && (
        <li>
          The Sentinel watches the running stream: it restarts a failed change-capture connector a few times
          before stopping the pipeline and asking you, and raises an alert when capture or delivery falls
          behind, or a Postgres replication slot holds too much WAL.
        </li>
      )}
    </ul>
  )
}

/** The alerts endpoint caps a page at 100 (pipeline_alerts.go pipelineAlertsMaxLimit). */
const ALERTS_PAGE = 100

/**
 * Open Sentinel alerts for a CDC pipeline, read from the same endpoint the Data
 * flow tab lists them from. null = not asked, or the read failed: the line is
 * left out rather than claiming "no open alerts" on a failed read.
 */
async function fetchOpenAlertCount(pipelineId: string): Promise<number | null> {
  try {
    const res = await authFetch(
      `${API_ENDPOINTS.PIPELINES.ALERTS(pipelineId)}?resolved=false&limit=${ALERTS_PAGE}`,
      { cache: "no-store" }
    )
    if (!res.ok) return null
    const data = await res.json()
    return Array.isArray(data?.alerts) ? data.alerts.length : null
  } catch {
    return null
  }
}

function OpenAlertsLine({ pipelineId, count }: { pipelineId: string; count: number }) {
  if (count === 0) {
    return <p className="text-xs text-gray-500 dark:text-gray-400">No open Sentinel alerts.</p>
  }
  const n = count >= ALERTS_PAGE ? `${ALERTS_PAGE}+` : String(count)
  return (
    <p className="text-xs text-amber-700 dark:text-amber-300">
      {n} open Sentinel alert{count === 1 ? "" : "s"}.{" "}
      <Link href={`/pipelines/${pipelineId}?tab=monitor`} className="text-blue-600 hover:underline dark:text-blue-400">
        See them in Data flow
      </Link>
    </p>
  )
}

export function SelfHealingPanel({
  pipelineId,
  pipelineType,
}: {
  pipelineId: string
  /** "cdc" adds the Sentinel and its open alerts. Omitted reads as batch. */
  pipelineType?: "etl" | "cdc"
}) {
  const [activity, setActivity] = useState<HealerActivity[]>([])
  const [openAlerts, setOpenAlerts] = useState<number | null>(null)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [forbidden, setForbidden] = useState(false)
  const [showAll, setShowAll] = useState(false)

  const fetchActivity = useCallback(
    async (isRefresh = false) => {
      if (isRefresh) setRefreshing(true)
      try {
        // Server-side type filter, not a client-side filter over a general
        // fetch: healer rows have a NULL seq and sort last, so a limit=200
        // unfiltered page can contain none of them on a busy pipeline.
        const url =
          `${API_ENDPOINTS.PIPELINES.EVENTS(pipelineId)}` +
          `?limit=200&event_types=${HEALER_EVENT_TYPES.join(",")}`
        const [res, alerts] = await Promise.all([
          authFetch(url, { cache: "no-store" }),
          pipelineType === "cdc" ? fetchOpenAlertCount(pipelineId) : Promise.resolve(null),
        ])
        setOpenAlerts(alerts)
        if (!res.ok) {
          // 403 is a viewer without access to this pipeline's events; there is
          // nothing for them to do about it, so say nothing rather than showing
          // an error they cannot act on.
          setForbidden(res.status === 403)
          setError(res.status === 403 ? null : `Could not load self-healing activity (${res.status})`)
          setActivity([])
          return
        }
        const data = await res.json()
        const events: PipelineRunEvent[] = data.events ?? []
        setActivity(extractHealerActivity(events))
        setForbidden(false)
        setError(null)
      } catch {
        setError("Could not load self-healing activity")
      } finally {
        setLoading(false)
        setRefreshing(false)
      }
    },
    [pipelineId, pipelineType]
  )

  useEffect(() => {
    fetchActivity()
    const timer = setInterval(() => fetchActivity(true), POLL_MS)
    return () => clearInterval(timer)
  }, [fetchActivity])

  // Nothing to say before the first response lands, or to a viewer who may not
  // read this pipeline's events. Otherwise the card always shows: a healthy
  // pipeline still has watchers, and "nothing needed healing" is itself news.
  if (loading || forbidden) return null
  if (error) {
    return (
      <Card>
        <CardContent className="py-3 px-4 text-sm text-gray-500 dark:text-gray-400">{error}</CardContent>
      </Card>
    )
  }

  const healed = activity.filter((a) => a.kind === "verdict" && healSucceeded(a)).length
  const needsAttention = openEscalations(activity).length
  const visible = showAll ? activity : activity.slice(0, 5)

  return (
    <Card data-testid="self-healing-card">
      <CardHeader className="py-3 px-4 pb-2">
        <div className="flex items-center justify-between gap-2">
          <CardTitle className="flex items-center gap-2 text-sm font-semibold">
            <Activity className="h-4 w-4 text-blue-600 dark:text-blue-400" />
            Self-healing
            {activity.length > 0 && (
              <Badge variant="outline" className="text-xs">
                {activity.length}
              </Badge>
            )}
            {healed > 0 && (
              <Badge variant="outline" className="border-green-400 text-xs text-green-700 dark:text-green-300">
                {healed} healed
              </Badge>
            )}
            {needsAttention > 0 && (
              <Badge variant="outline" className="border-amber-400 text-xs text-amber-700 dark:text-amber-300">
                {needsAttention} need{needsAttention === 1 ? "s" : ""} you
              </Badge>
            )}
          </CardTitle>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => fetchActivity(true)}
            disabled={refreshing}
            aria-label="Refresh self-healing activity"
          >
            <RefreshCw className={`h-3.5 w-3.5 ${refreshing ? "animate-spin" : ""}`} />
          </Button>
        </div>
      </CardHeader>
      <CardContent className="space-y-2 px-4 pb-3 pt-0">
        <Watchers pipelineType={pipelineType} />
        {pipelineType === "cdc" && openAlerts !== null && (
          <OpenAlertsLine pipelineId={pipelineId} count={openAlerts} />
        )}
        {activity.length === 0 ? (
          <p className="text-xs text-gray-500 dark:text-gray-400">
            The heal agent has not had to act on this pipeline.
          </p>
        ) : (
          <div>
            {visible.map((a) => (
              <ActivityRow key={a.id} a={a} />
            ))}
          </div>
        )}
        {activity.length > 5 && (
          <button
            type="button"
            onClick={() => setShowAll((v) => !v)}
            className="text-xs text-blue-600 hover:underline dark:text-blue-400"
          >
            {showAll ? "Show less" : `Show all ${activity.length}`}
          </button>
        )}
      </CardContent>
    </Card>
  )
}
