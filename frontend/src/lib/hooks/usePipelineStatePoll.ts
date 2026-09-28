"use client"

import { useCallback, useSyncExternalStore } from "react"

import { authFetch } from "@/lib/api/auth-fetch"
import { API_ENDPOINTS } from "@/lib/config/api"
import { emitPipelineRefresh, onPipelineRefresh } from "@/lib/events/pipelineRefresh"
import {
  isTerminalPipelineStatus,
  normalizePipelineStatus,
  type NormalizedPipelineStatus,
} from "@/lib/pipeline/statusNormalization"

// One /state poller per pipeline, shared by every reader in the page header.
//
// The status badge, the run/pause/stop actions, the CDC actions and the overflow
// menu each ran their own 4 s interval on /pipelines/:id/state, forever: nothing
// slowed them on a finished pipeline or paused them in a background tab, so an
// open pipeline page sent two (batch) or three (CDC) identical GETs every 4 s for
// as long as it stayed open. This is the same shape as usePipelineRuntime: the first reader
// starts the poll, the last one to leave stops it and drops it.
//
// It never stops for a status, only slows down: a finished pipeline can be run
// again from a schedule or another tab, and the header has to notice. A run
// started from this page announces itself on the refresh bus, which reads at once.

export const ACTIVE_POLL_MS = 4000
export const SETTLED_POLL_MS = 30_000
// Paused or idle: a schedule or another tab moves these on more often than a
// finished run, and 30 s left the page trailing the database (item 35).
export const PAUSED_POLL_MS = 10_000

/**
 * How long to wait before the next read. A finished run is read every 30 s, a
 * paused or idle one every 10 s; a live or unknown one (including no answer yet)
 * every 4 s, as each reader used to.
 */
export function statePollMs(status: NormalizedPipelineStatus | null): number {
  if (status === null) return ACTIVE_POLL_MS
  if (status === "idle" || status === "paused") return PAUSED_POLL_MS
  if (isTerminalPipelineStatus(status)) return SETTLED_POLL_MS
  return ACTIVE_POLL_MS
}

export interface PipelineStatePoll {
  // null until the first successful read.
  status: NormalizedPipelineStatus | null
  // The last read failed. `status` keeps the last good answer.
  error: string | null
  // Successful reads so far. A reader holding a local override (the overflow
  // menu hides Stop the moment it is clicked) drops it on the next read, even one
  // that returns the same status.
  reads: number
}

const IDLE: PipelineStatePoll = Object.freeze({ status: null, error: null, reads: 0 })

interface Poller {
  state: PipelineStatePoll
  listeners: Set<() => void>
  stop: () => void
}

const pollers = new Map<string, Poller>()

function isHidden(): boolean {
  return typeof document !== "undefined" && document.visibilityState === "hidden"
}

function startPoller(pipelineId: string): Poller {
  const poller: Poller = { state: IDLE, listeners: new Set(), stop: () => {} }
  let stopped = false
  // A 404 means the pipeline is gone; nothing re-arms that, not even a refresh.
  let notFound = false
  let inflight: AbortController | null = null
  let timer: ReturnType<typeof setTimeout> | undefined
  // Set while this poller announces a change, so it does not answer itself.
  let announcing = false

  const update = (next: Partial<PipelineStatePoll>, announce = false) => {
    if (stopped) return
    const merged = { ...poller.state, ...next }
    const cur = poller.state
    if (merged.status === cur.status && merged.error === cur.error && merged.reads === cur.reads) return
    poller.state = merged
    poller.listeners.forEach((l) => l())
    // The status moved (a run finished, a schedule started one, another tab
    // paused it): tell /runtime and the panels, which otherwise wait out their
    // own slower timers — the page trailed the database by ~20 s (item 35).
    // The first answer is not a change, and a read the refresh bus asked for
    // was already heard by every listener.
    if (announce && cur.status !== null && merged.status !== cur.status) {
      announcing = true
      try {
        emitPipelineRefresh(pipelineId)
      } finally {
        announcing = false
      }
    }
  }

  const schedule = () => {
    if (stopped || notFound) return
    clearTimeout(timer)
    timer = setTimeout(() => {
      // A hidden tab skips its tick; the visibilitychange handler reads on return.
      if (isHidden()) schedule()
      else void fetchOnce()
    }, statePollMs(poller.state.status))
  }

  // `announce`: this read was not asked for on the refresh bus, so a change it
  // finds is news to the other panels.
  const fetchOnce = async (announce = true) => {
    if (stopped || notFound) return
    inflight?.abort()
    const ac = new AbortController()
    inflight = ac
    // The answer schedules the next read, so a refresh does not leave a second
    // timer running beside it.
    clearTimeout(timer)
    try {
      const res = await authFetch(`${API_ENDPOINTS.PIPELINES.GET(pipelineId)}/state`, {
        cache: "no-store",
        signal: ac.signal,
      })
      if (!res.ok) {
        if (res.status === 404) notFound = true
        update({ error: `state ${res.status}` })
        return
      }
      const data = (await res.json()) as { status?: string }
      if (stopped) return
      update({ status: normalizePipelineStatus(data?.status), error: null, reads: poller.state.reads + 1 }, announce)
    } catch (e) {
      // Aborted by a newer read (which owns the schedule now) or by the last
      // reader leaving.
      if ((e as { name?: string })?.name === "AbortError") return
      update({ error: String((e as Error)?.message ?? e) })
    } finally {
      if (inflight === ac) {
        inflight = null
        schedule()
      }
    }
  }

  void fetchOnce()

  // Run, Pause, Resume and Stop announce themselves on the refresh bus, so the
  // header flips as soon as the POST lands, not a poll later (#13).
  const unsubscribeRefresh = onPipelineRefresh((pid) => {
    if (pid === pipelineId && !announcing) void fetchOnce(false)
  })

  const onVisibility = () => {
    if (!isHidden()) void fetchOnce()
  }
  if (typeof document !== "undefined") document.addEventListener("visibilitychange", onVisibility)

  poller.stop = () => {
    stopped = true
    unsubscribeRefresh()
    clearTimeout(timer)
    if (typeof document !== "undefined") document.removeEventListener("visibilitychange", onVisibility)
    inflight?.abort()
  }
  return poller
}

function subscribePoller(pipelineId: string, listener: () => void) {
  let poller = pollers.get(pipelineId)
  if (!poller) {
    poller = startPoller(pipelineId)
    pollers.set(pipelineId, poller)
  }
  const p = poller
  p.listeners.add(listener)
  return () => {
    p.listeners.delete(listener)
    if (p.listeners.size > 0) return
    p.stop()
    if (pollers.get(pipelineId) === p) pollers.delete(pipelineId)
  }
}

/** The pipeline's /state status, read by one poller however many readers mount it. Pass null to read nothing. */
export function usePipelineStatePoll(pipelineId: string | null | undefined): PipelineStatePoll {
  const id = pipelineId || null
  const subscribe = useCallback(
    (listener: () => void) => (id ? subscribePoller(id, listener) : () => {}),
    [id],
  )
  // Keyed by pipeline, so navigating to another pipeline never shows the previous
  // one's status for a render (see usePipelineRuntime).
  const getSnapshot = useCallback(() => (id ? pollers.get(id)?.state ?? IDLE : IDLE), [id])
  return useSyncExternalStore(subscribe, getSnapshot, getSnapshot)
}
