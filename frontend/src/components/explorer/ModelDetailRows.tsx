"use client"

// Two values in a model page's Details card that need a request of their own: whose
// role a schedule's runs are checked against, and how many models and pipelines are
// chained to this one. Each loads on its own and fails on its own, into its own row;
// neither is worth holding up or failing the page for.

import { useEffect, useRef, useState } from "react"
import { useCurrentUser } from "@/contexts/CurrentUserContext"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"
import { getJson } from "@/components/explorer/getJson"
import { SCHEDULE_LIST_LIMIT, buildModelLineage } from "@/components/explorer/modelLineage"
import type { ScheduledQuery } from "@/components/explorer/scheduledModel"

const LOOKUP_TIMEOUT_MS = 15_000

/**
 * Every run a schedule starts, on its cron or woken by an upstream, is authorized as its
 * run_as_user_id: the member who attached it (CreateSavedQuerySchedule), unchanged by an
 * edit. authorizeModelRun re-reads that member's role on each run, and a refusal pauses
 * the schedule (runSavedQueryModel, autoPauseModelSchedule). Admin is the floor; SQL whose
 * statement class needs more (a destructive statement is owner-only) needs that role.
 */
export const RUN_AS_HINT =
  "Every run this schedule starts is checked against this member's workspace role. " +
  "If they leave the workspace, or their role drops below admin or below what this model's SQL needs, " +
  "the next run is refused and the schedule pauses. " +
  "Run now runs as whoever clicks it."

type RunAsResult =
  | { key: string; status: "ok"; runAs: string; email?: string; roster: "listed" | "absent" | "unavailable" }
  | { key: string; status: "error"; message: string }

export function RunAsValue({ savedQueryId, scheduleId }: { savedQueryId: string; scheduleId: string }) {
  const { user, isLoading: userLoading } = useCurrentUser()
  const { activeWorkspace, isLoading: workspaceLoading } = useWorkspaceRole()
  const userId = user?.id ?? ""
  const workspaceId = activeWorkspace?.id ?? ""
  // A schedule deleted and attached again is a new schedule_id, and a new run-as member.
  const key = `${savedQueryId}|${scheduleId}|${userId}|${workspaceId}`
  const [result, setResult] = useState<RunAsResult | null>(null)
  // Until both have loaded, the key is about to change: a read started now would be
  // aborted and sent again the moment they arrive, and a network log records the
  // abandoned one as a 503.
  const contextsLoading = userLoading || workspaceLoading

  useEffect(() => {
    if (contextsLoading) return
    const controller = new AbortController()
    let cancelled = false
    ;(async () => {
      try {
        // The workspace list leaves run_as_user_id out; the model's own schedule route has it.
        const read = await getJson(
          `/api/v1/explorer/saved/${encodeURIComponent(savedQueryId)}/schedule`,
          controller.signal,
          LOOKUP_TIMEOUT_MS,
        )
        if (cancelled) return
        const runAs = (read.body as { run_as_user_id?: unknown } | null)?.run_as_user_id
        if (!read.ok || typeof runAs !== "string" || runAs === "") {
          setResult({
            key,
            status: "error",
            message: read.ok ? "Could not read who runs this schedule." : `Could not load who runs this schedule (HTTP ${read.status}).`,
          })
          return
        }
        if (runAs === userId) {
          setResult({ key, status: "ok", runAs, roster: "listed" })
          return
        }
        // The roster turns the id into an email. It is best effort: without it the row
        // still says the schedule runs as someone else, just not who.
        let roster: "listed" | "absent" | "unavailable" = "unavailable"
        let email: string | undefined
        if (workspaceId) {
          try {
            const members = await getJson(
              `/api/v1/workspaces/${encodeURIComponent(workspaceId)}/members`,
              controller.signal,
              LOOKUP_TIMEOUT_MS,
            )
            const list = (members.body as { members?: unknown } | null)?.members
            if (members.ok && Array.isArray(list)) {
              const found = (list as Array<{ user_id?: unknown; email?: unknown }>).find((m) => m.user_id === runAs)
              roster = found ? "listed" : "absent"
              if (typeof found?.email === "string" && found.email) email = found.email
            }
          } catch {
            if (cancelled) return
          }
        }
        if (cancelled) return
        setResult({ key, status: "ok", runAs, email, roster })
      } catch {
        if (cancelled) return
        setResult({ key, status: "error", message: "Could not reach the server to load who runs this schedule." })
      }
    })()
    return () => {
      cancelled = true
      controller.abort()
    }
  }, [contextsLoading, key, savedQueryId, userId, workspaceId])

  if (contextsLoading || !result || result.key !== key) return <span className="text-zinc-400">Loading…</span>
  if (result.status === "error") return <span className="text-zinc-500 dark:text-zinc-400">{result.message}</span>

  let label: string
  if (result.runAs === userId) label = "you"
  // The roster lists current members only. Nothing removes run_as_user_id when a member
  // leaves, and the next run is refused for exactly that reason.
  else if (result.roster === "absent") label = "a former member"
  else if (result.email) label = result.email
  else label = "another member"
  return (
    <span className="break-words" title={RUN_AS_HINT}>
      {label}
    </span>
  )
}

type LineageResult =
  | { key: string; status: "ok"; upstreams: number; downstreams: number; capped: boolean }
  | { key: string; status: "error"; message: string }

/**
 * The Graph tab's own counts, from the same list and the same walk
 * (ModelLineageGraph), so the row and the graph's header cannot disagree.
 */
export function LineageCountValue({
  modelId,
  rootSchedule,
  reloadTick,
  onOpenGraph,
}: {
  modelId: string
  rootSchedule: ScheduledQuery | null
  reloadTick: number
  onOpenGraph: () => void
}) {
  // What the root contributes to the walk. Anything else about the schedule changing
  // (a run finishing, a pause) changes no count.
  const rootKey = rootSchedule
    ? JSON.stringify([
        rootSchedule.schedule_id,
        rootSchedule.schedule_type,
        (rootSchedule.upstreams ?? []).map((u) => `${u.kind}:${u.id}`),
      ])
    : ""
  const key = `${modelId}|${reloadTick}|${rootKey}`
  const [result, setResult] = useState<LineageResult | null>(null)
  // Read when the list arrives rather than being a dependency: the page hands over a new
  // object on every schedule reload, and rootKey carries every part of it the walk uses.
  const rootRef = useRef(rootSchedule)
  useEffect(() => {
    rootRef.current = rootSchedule
  })

  useEffect(() => {
    const controller = new AbortController()
    let cancelled = false
    ;(async () => {
      try {
        const list = await getJson("/api/v1/explorer/schedules", controller.signal, LOOKUP_TIMEOUT_MS)
        if (cancelled) return
        const body = list.body as { schedules?: unknown; count?: unknown } | null
        if (!list.ok || !body || !Array.isArray(body.schedules)) {
          setResult({
            key,
            status: "error",
            message: list.ok ? "Could not read the linked models." : `Could not load the linked models (HTTP ${list.status}).`,
          })
          return
        }
        const schedules = body.schedules as ScheduledQuery[]
        const count = typeof body.count === "number" ? body.count : schedules.length
        const lineage = buildModelLineage(modelId, schedules, { rootSchedule: rootRef.current })
        setResult({
          key,
          status: "ok",
          upstreams: lineage.upstreamsFound,
          downstreams: lineage.downstreamsFound,
          capped: Math.max(count, schedules.length) >= SCHEDULE_LIST_LIMIT,
        })
      } catch {
        if (cancelled) return
        setResult({ key, status: "error", message: "Could not reach the server to load the linked models." })
      }
    })()
    return () => {
      cancelled = true
      controller.abort()
    }
  }, [key, modelId])

  if (!result || result.key !== key) return <span className="text-zinc-400">Counting…</span>
  if (result.status === "error") return <span className="text-zinc-500 dark:text-zinc-400">{result.message}</span>
  // A schedule left out of a capped list can only hide a link, never add one.
  const atLeast = result.capped ? "at least " : ""
  const text = `${atLeast}${result.upstreams} upstream · ${atLeast}${result.downstreams} downstream`
  return (
    <button
      type="button"
      onClick={onOpenGraph}
      className="text-left text-blue-600 hover:underline dark:text-blue-400"
      title={
        result.capped
          ? `Counted from the first ${SCHEDULE_LIST_LIMIT} schedules in this workspace. Open the graph.`
          : "Open the graph"
      }
    >
      {text.charAt(0).toUpperCase() + text.slice(1)}
    </button>
  )
}
