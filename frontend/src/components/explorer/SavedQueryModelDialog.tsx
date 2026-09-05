"use client"

import { type ReactNode, useCallback, useEffect, useMemo, useState } from "react"
import { toast } from "sonner"
import { authFetch } from "@/lib/api/auth-fetch"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { AlertTriangle, Clock, Loader2, Lock, Pause, Play, SquareTerminal, Table2, Trash2 } from "lucide-react"
import { useWorkspaceRole } from "@/contexts/WorkspaceContext"

/** Why a control is disabled for a non-admin. One string, so every control agrees. */
const MANAGE_ROLE_HINT =
  "Changing what a query does, or its schedule, requires the Admin or Owner role in this workspace"

// A model is a saved query plus a decision about what running it DOES, plus
// (optionally) a schedule — the in-warehouse "T" of ELT. The rows never leave the
// warehouse; rsync only sends the statement, so this dialog is about what the result
// does and how often, never about moving data.
//
// Two ordering rules are enforced by the backend and mirrored here so the UI never
// offers a control that will be refused:
//
//  1. A run has to do something before it can be scheduled. Scheduling a query that
//     writes nowhere is a schedule that can only fail.
//  2. Resume can be refused. An auto-pause is a security stop (the run-as user lost
//     the workspace role, or the SQL stopped being a read), and the server re-checks
//     the condition on resume rather than trusting the click.

export type ModelScheduleType = "cron" | "interval" | "after_pipeline"

export interface ModelScheduleSpec {
  cron?: string
  every_seconds?: number
  timezone?: string
}

export interface ModelSchedule {
  schedule_id: string
  saved_query_id: string
  schedule_type: ModelScheduleType
  schedule_spec: ModelScheduleSpec
  status: "active" | "paused" | "deleted"
  run_as_user_id: string
  created_at: string
  updated_at: string
  /** Set only for schedule_type "after_pipeline"; the upstream that wakes this model. */
  trigger_pipeline_id?: string
  /** Joined server-side, so this list never has to resolve an id to a name itself. */
  trigger_pipeline_name?: string
  paused_at?: string
  paused_reason?: string
  // Auto-pause is kept in its own pair of columns server-side so a Resume click can
  // never silently clear a security stop; it reads differently here too.
  auto_paused_at?: string
  auto_paused_reason?: string
  // Computed per request, never stored: an `active` schedule whose next fire would be
  // refused reports it here, because the status badge alone would say "active" while
  // nothing runs.
  blocked?: boolean
  blocked_reason?: string
}

interface SavedQueryModelDialogProps {
  savedQueryId: string
  savedQueryName: string
  /** Current server state, used as the initial form values. */
  materialization: string
  targetTable?: string
  /**
   * The class the server stored for this query's SQL, if known. Purely advisory: the
   * server re-classifies the live SQL on every run and is the thing that decides. It
   * is passed in so a mode that cannot work for this SQL can be flagged before the
   * click rather than refused after it.
   */
  statementClass?: string
  /**
   * Whether this query's engine can back a model at all, resolved server-side by the
   * Explorer capability table. Same contract as `supports_explorer`: only an explicit
   * `false` gates anything, because an older payload that omits the field must not
   * lock a control that works.
   *
   * This is a harder limit than `statementClass` above — no SQL makes a BigQuery
   * connection materializable, so the two write modes are disabled outright rather
   * than flagged. "Nothing on its own" stays live: SetSavedQueryMaterialization
   * returns before its dialect check for mode "none", so turning an existing model
   * off is allowed on every engine and must remain reachable.
   */
  supportsMaterialization?: boolean
  /** Names the engine in that refusal. Advisory text only; never re-derived into a gate. */
  connectorType?: string
  lastRunStatus?: string
  lastRunError?: string
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Refetch the saved-query list — badges are derived from it. */
  onChanged: () => void
}

/**
 * "after_pipeline" sits in the same control as the cadences because to the person
 * filling this in it answers the same question — when does this rebuild? — but it is
 * not a cadence: it has no clock, no timezone and no next run, so every branch below
 * that reads one of those has to exclude it.
 */
type Cadence = "hour" | "day" | "week" | "interval" | "custom" | "after_pipeline"

/**
 * What a run of this query does. These are not three preferences over one behaviour;
 * they are three different behaviours, and which ones a given SQL text may have is
 * decided server-side by authorizeModelRun: "table" requires a read, "statement"
 * requires a write.
 */
export type ModelMode = "none" | "table" | "statement"

/**
 * Anything this build does not recognize becomes "none". A newer server could store a
 * mode this UI has never heard of, and rendering that as an ordinary table model would
 * both misdescribe it and offer to save it back as one.
 */
function normalizeMode(raw: string | undefined): ModelMode {
  return raw === "table" || raw === "statement" ? raw : "none"
}

/**
 * Mirrors validators.IsWriteClass, including its treatment of "unknown" as a write —
 * the server fails closed there, and a hint that disagreed with the gate would be
 * worse than no hint.
 */
function isWriteClass(cls: string | undefined): boolean {
  return cls === "dml_write" || cls === "ddl" || cls === "destructive" || cls === "unknown"
}

const MODE_CHOICES: { value: ModelMode; label: string; icon: ReactNode; hint: string }[] = [
  {
    value: "none",
    label: "Nothing on its own",
    icon: null,
    hint: "A plain saved query. Teammates can find it and run it by hand; it cannot be scheduled.",
  },
  {
    value: "table",
    label: "Write the results to a table",
    icon: <Table2 className="h-3.5 w-3.5" />,
    hint: "For a SELECT. Each run builds the new table beside the old one and swaps at the end, so a failing query costs you a failed run, not yesterday's data.",
  },
  {
    value: "statement",
    label: "Run the SQL as written",
    icon: <SquareTerminal className="h-3.5 w-3.5" />,
    hint: "For a MERGE, UPDATE, DELETE or INSERT … SELECT. The statement already names what it writes, so there is no target table to give.",
  },
]

function pad2(n: number | string): string {
  return String(n).padStart(2, "0")
}

function buildCron(cadence: Cadence, hour: string, minute: string, weekday: string): string {
  switch (cadence) {
    case "hour":
      return `${Number(minute)} * * * *`
    case "week":
      return `${Number(minute)} ${Number(hour)} * * ${weekday}`
    default:
      return `${Number(minute)} ${Number(hour)} * * *`
  }
}

/** Recover the simple form from a stored cron, or null if it is hand-written. */
function parseCron(cron: string): { cadence: Cadence; hour: string; minute: string; weekday: string } | null {
  const parts = String(cron || "").trim().split(/\s+/).filter(Boolean)
  if (parts.length < 5) return null
  const [min, hour, dom, mon, dow] = parts
  if (!/^\d+$/.test(min) || dom !== "*" || mon !== "*") return null
  if (hour === "*" && dow === "*") {
    return { cadence: "hour", hour: "02", minute: pad2(min), weekday: "1" }
  }
  if (!/^\d+$/.test(hour)) return null
  if (dow === "*") return { cadence: "day", hour: pad2(hour), minute: pad2(min), weekday: "1" }
  if (/^[0-6]$/.test(dow)) return { cadence: "week", hour: pad2(hour), minute: pad2(min), weekday: dow }
  return null
}

function describeSchedule(s: ModelSchedule): string {
  if (s.schedule_type === "after_pipeline") {
    // The name is joined server-side. Falling back to the id keeps the sentence true
    // when the pipeline was deleted out from under the trigger.
    return `After ${s.trigger_pipeline_name || s.trigger_pipeline_id || "a pipeline"} runs`
  }
  if (s.schedule_type === "interval") {
    const seconds = s.schedule_spec.every_seconds || 0
    if (seconds % 3600 === 0) return `Every ${seconds / 3600} hour${seconds === 3600 ? "" : "s"}`
    if (seconds % 60 === 0) return `Every ${seconds / 60} minute${seconds === 60 ? "" : "s"}`
    return `Every ${seconds} seconds`
  }
  return `${s.schedule_spec.cron} (${s.schedule_spec.timezone || "UTC"})`
}

const WEEKDAYS = [
  { value: "0", label: "Sunday" },
  { value: "1", label: "Monday" },
  { value: "2", label: "Tuesday" },
  { value: "3", label: "Wednesday" },
  { value: "4", label: "Thursday" },
  { value: "5", label: "Friday" },
  { value: "6", label: "Saturday" },
]

/** The two fields of a pipeline this dialog needs: what to store, and what to show. */
/** One inferred producer of a table this query reads. Mirrors the Go handler's JSON. */
interface UpstreamCandidate {
  pipeline_id: string
  pipeline_name: string
  table: string
  matched_reference: string
  /** False when the match was on table name alone, because the SQL named no schema. */
  qualified: boolean
}

interface UpstreamSuggestion {
  references: string[]
  unresolved: string[]
  candidates: UpstreamCandidate[]
  /** Some table has more than one producer, so there is no single right answer. */
  ambiguous: boolean
}

/**
 * Offers the pipelines that produce this query's inputs, and never picks one.
 *
 * The inference reads the query's SQL and asks which pipeline last wrote those tables.
 * It is right often enough to save the user a hunt through a list, and wrong often
 * enough that it must not act on its own: a table can have two producers, a name can
 * be matched without a schema, and the SQL can be edited five minutes from now. So
 * every candidate is a button the user presses, the reason for each is shown next to
 * it, and an ambiguous answer says so instead of quietly offering the first row.
 */
function UpstreamHint({
  upstreams,
  pipelineOptions,
  selectedId,
  onPick,
  disabled,
}: {
  upstreams: UpstreamSuggestion | null
  pipelineOptions: TriggerPipeline[]
  selectedId: string
  onPick: (id: string) => void
  disabled: boolean
}): ReactNode {
  if (!upstreams) return null

  // Only offer pipelines the picker itself can hold. A candidate missing from the list
  // (deleted, or filtered by the pipelines endpoint) would set an id the Select cannot
  // display, leaving the user looking at a placeholder after a successful click.
  const offerable = upstreams.candidates.filter((c) =>
    pipelineOptions.some((p) => p.id === c.pipeline_id)
  )
  if (offerable.length === 0) return null

  // One button per pipeline, not per matched table: two inputs from the same pipeline
  // is one choice, and listing it twice would imply otherwise.
  const byPipeline = new Map<string, { name: string; tables: string[]; qualified: boolean }>()
  for (const c of offerable) {
    const seen = byPipeline.get(c.pipeline_id)
    if (seen) {
      if (!seen.tables.includes(c.table)) seen.tables.push(c.table)
      seen.qualified = seen.qualified && c.qualified
    } else {
      byPipeline.set(c.pipeline_id, {
        name: c.pipeline_name || c.pipeline_id,
        tables: [c.table],
        qualified: c.qualified,
      })
    }
  }

  return (
    <div className="rounded-md border border-zinc-200 bg-zinc-50 p-2.5 dark:border-zinc-800 dark:bg-zinc-900/50">
      <p className="text-xs text-zinc-600 dark:text-zinc-400">
        {byPipeline.size === 1
          ? "This query reads a table one of your pipelines produces:"
          : "This query reads tables these pipelines produce:"}
      </p>
      <ul className="mt-1.5 space-y-1.5">
        {Array.from(byPipeline.entries()).map(([id, info]) => (
          <li key={id} className="flex flex-wrap items-center gap-2">
            <Button
              type="button"
              size="sm"
              variant={selectedId === id ? "secondary" : "outline"}
              className="h-7 text-xs"
              disabled={disabled || selectedId === id}
              onClick={() => onPick(id)}
            >
              {selectedId === id ? `Following ${info.name}` : `Follow ${info.name}`}
            </Button>
            <span className="text-xs text-zinc-500">
              writes {info.tables.join(", ")}
              {!info.qualified && " (matched on table name only — no schema in the SQL)"}
            </span>
          </li>
        ))}
      </ul>
      {upstreams.ambiguous && (
        <p className="mt-2 text-xs text-amber-600 dark:text-amber-400">
          More than one pipeline writes the same table, so which one this should follow is
          your call — nothing is selected for you.
        </p>
      )}
      {upstreams.unresolved.length > 0 && (
        <p className="mt-2 text-xs text-zinc-500">
          No producer found for {upstreams.unresolved.join(", ")}. If this query depends on
          that table being fresh, pick the pipeline that loads it instead.
        </p>
      )}
    </div>
  )
}

interface TriggerPipeline {
  id: string
  name: string
}

function browserTimezone(): string {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC"
  } catch {
    return "UTC"
  }
}

export function SavedQueryModelDialog({
  savedQueryId,
  savedQueryName,
  materialization,
  targetTable,
  statementClass,
  supportsMaterialization,
  connectorType,
  lastRunStatus,
  lastRunError,
  open,
  onOpenChange,
  onChanged,
}: SavedQueryModelDialogProps) {
  // Mirrors the backend's modelRunMinRole (= WSAdmin), which guards the
  // materialization PUT, Run now, and every schedule mutation. Materializing is
  // treated as a DDL act, so the bar is admin rather than member. Read paths (GET
  // schedule) are viewer, which is why the editor still renders for everyone —
  // a viewer can see what is scheduled, just not change it.
  //
  // The gate is presentational. The server is still the thing that decides; this
  // only stops the UI from offering a button whose sole outcome is a 403.
  const { meets, isLoading: roleLoading } = useWorkspaceRole()
  const canManage = meets("admin")
  // Fail closed while the role is still loading: briefly-enabled buttons are worse
  // than briefly-disabled ones, because the click lands.
  const readOnly = !canManage

  // Deliberately `=== false`, not `!supportsMaterialization`: the same contract the
  // Explorer connection list applies to supports_explorer. A payload from a server
  // that predates the field leaves this undefined, and treating that as "blocked"
  // would disable materialization for every engine, including the ones it works on.
  //
  // Orthogonal to readOnly. That one is about who you are and lifts when your role
  // changes; this one is about what the engine can do and never lifts.
  const engineBlocked = supportsMaterialization === false
  const engineName = connectorType?.trim() || "this connection's engine"
  const ENGINE_HINT = `${engineName} connections cannot back a materialized model yet`

  const [mode, setMode] = useState<ModelMode>(() => normalizeMode(materialization))
  const [target, setTarget] = useState(targetTable ?? "")
  const [savingMode, setSavingMode] = useState(false)
  const [running, setRunning] = useState(false)

  // The SERVER's view of what this query does, as distinct from the form above it.
  // Seeded from props, then advanced by each successful PUT using that response's own
  // body — the server normalises the table name, so echoing back what was typed would
  // eventually disagree with what is stored.
  //
  // This used to be read straight off the props, which meant the schedule controls
  // stayed locked until the parent refetched and re-rendered. That gap is the whole
  // reason saving a target and scheduling it read as two separate stages.
  const [savedMode, setSavedMode] = useState<{ materialization: ModelMode; targetTable: string }>({
    materialization: normalizeMode(materialization),
    targetTable: targetTable ?? "",
  })

  const [schedule, setSchedule] = useState<ModelSchedule | null>(null)
  // Distinct from `schedule === null`, which asserts there is no schedule. This says
  // the lookup failed, so nothing about the schedule is known.
  const [scheduleError, setScheduleError] = useState<string | null>(null)
  const [loadingSchedule, setLoadingSchedule] = useState(false)
  const [scheduleBusy, setScheduleBusy] = useState(false)
  const [confirmDeleteSchedule, setConfirmDeleteSchedule] = useState(false)

  const [cadence, setCadence] = useState<Cadence>("day")
  const [hour, setHour] = useState("02")
  const [minute, setMinute] = useState("00")
  const [weekday, setWeekday] = useState("1")
  const [everySeconds, setEverySeconds] = useState("3600")
  const [cronExpr, setCronExpr] = useState("0 2 * * *")
  const [timezone, setTimezone] = useState(browserTimezone)
  const [triggerPipelineId, setTriggerPipelineId] = useState("")
  const [pipelines, setPipelines] = useState<TriggerPipeline[]>([])
  // Separate from `pipelines.length === 0`, which cannot tell "this workspace has no
  // pipelines" from "the list never arrived". The first is a fair thing to say in the
  // picker; the second would be a lie.
  const [pipelinesError, setPipelinesError] = useState(false)
  const [loadingPipelines, setLoadingPipelines] = useState(false)
  // Which pipelines produce the tables this query reads, inferred from its SQL. A hint
  // beside the picker, never a value: see the effect below and `UpstreamHint`.
  const [upstreams, setUpstreams] = useState<UpstreamSuggestion | null>(null)

  const hours = useMemo(() => Array.from({ length: 24 }, (_, i) => pad2(i)), [])
  const minutes = useMemo(() => Array.from({ length: 60 }, (_, i) => pad2(i)), [])
  const timezones = useMemo(() => {
    try {
      const list = (Intl as unknown as { supportedValuesOf?: (k: string) => string[] }).supportedValuesOf?.("timeZone")
      if (Array.isArray(list) && list.length > 0) return [...list].sort((a, b) => a.localeCompare(b))
    } catch {
      // fall through
    }
    return ["UTC", browserTimezone()].filter((v, i, a) => a.indexOf(v) === i)
  }, [])

  // Re-seed from the server state each time the dialog opens: the row behind it may
  // have changed since the last time this component mounted.
  useEffect(() => {
    if (!open) return
    setMode(normalizeMode(materialization))
    setTarget(targetTable ?? "")
    setSavedMode({ materialization: normalizeMode(materialization), targetTable: targetTable ?? "" })
  }, [open, materialization, targetTable])

  const loadSchedule = useCallback(async () => {
    setLoadingSchedule(true)
    setScheduleError(null)
    try {
      const res = await authFetch(`/api/v1/explorer/saved/${savedQueryId}/schedule`, { cache: "no-store" })
      if (res.status === 404) {
        // The only response that actually means "there is no schedule".
        setSchedule(null)
        return
      }
      if (!res.ok) {
        // Anything else means we do not KNOW. Rendering that as "no schedule" would
        // offer Create for a schedule that may be live and firing — the user then
        // either hits a duplicate error or, worse, believes nothing is scheduled.
        setScheduleError(`Could not load this query's schedule (HTTP ${res.status}).`)
        return
      }
      const data = (await res.json()) as ModelSchedule
      setSchedule(data)

      // Seed the editor from the live schedule so "Update" starts from what is
      // actually running, not from the defaults.
      if (data.schedule_type === "after_pipeline") {
        setCadence("after_pipeline")
        setTriggerPipelineId(data.trigger_pipeline_id || "")
      } else if (data.schedule_type === "interval") {
        setCadence("interval")
        setEverySeconds(String(data.schedule_spec.every_seconds ?? 3600))
      } else {
        const parsed = parseCron(data.schedule_spec.cron || "")
        if (parsed) {
          setCadence(parsed.cadence)
          setHour(parsed.hour)
          setMinute(parsed.minute)
          setWeekday(parsed.weekday)
        } else {
          setCadence("custom")
          setCronExpr(data.schedule_spec.cron || "")
        }
      }
      if (data.schedule_spec.timezone) setTimezone(data.schedule_spec.timezone)
    } catch {
      // Network failure — same reasoning as a non-OK status: unknown, not absent.
      setScheduleError("Could not reach the server to load this query's schedule.")
    } finally {
      setLoadingSchedule(false)
    }
  }, [savedQueryId])

  useEffect(() => {
    if (open) void loadSchedule()
  }, [open, loadSchedule])

  // Fetched once per open rather than lazily on picking the cadence, so the option is
  // never offered against a list that has not arrived. Scoped to the caller's active
  // workspace by the endpoint itself — this dialog does no filtering of its own, and
  // must not, since the server re-checks the chosen pipeline on save anyway.
  useEffect(() => {
    if (!open) return
    let cancelled = false
    setLoadingPipelines(true)
    setPipelinesError(false)
    void (async () => {
      try {
        const res = await authFetch("/api/v1/pipelines", { cache: "no-store" })
        if (cancelled) return
        if (!res.ok) {
          setPipelinesError(true)
          return
        }
        const data = (await res.json()) as { pipelines?: { id?: string; name?: string }[] }
        if (cancelled) return
        setPipelines(
          (data.pipelines ?? [])
            .filter((p): p is { id: string; name?: string } => typeof p.id === "string" && p.id !== "")
            .map((p) => ({ id: p.id, name: p.name?.trim() || p.id }))
        )
      } catch {
        if (!cancelled) setPipelinesError(true)
      } finally {
        if (!cancelled) setLoadingPipelines(false)
      }
    })()
    return () => {
      cancelled = true
    }
  }, [open])

  // Ask which pipelines produce this query's inputs, but only once the user has said
  // they want to follow a pipeline. Most schedules are clock schedules, and this parses
  // SQL and hits pipeline_run_table_stats to answer — no reason to spend that on every
  // dialog open.
  //
  // A failure here sets nothing and says nothing. The suggestion is a shortcut past a
  // picker that already works; an error message about a shortcut would be noise in a
  // dialog whose actual job is unaffected.
  useEffect(() => {
    if (!open || cadence !== "after_pipeline") return
    if (upstreams) return
    let cancelled = false
    void (async () => {
      try {
        const res = await authFetch(`/api/v1/explorer/saved/${savedQueryId}/upstreams`, {
          cache: "no-store",
        })
        if (cancelled || !res.ok) return
        const data = (await res.json()) as UpstreamSuggestion
        if (cancelled) return
        setUpstreams({
          references: data.references ?? [],
          unresolved: data.unresolved ?? [],
          candidates: data.candidates ?? [],
          ambiguous: Boolean(data.ambiguous),
        })
      } catch {
        // Deliberately silent — see above.
      }
    })()
    return () => {
      cancelled = true
    }
  }, [open, cadence, savedQueryId, upstreams])

  const specForRequest = (): {
    schedule_type: ModelScheduleType
    schedule_spec: ModelScheduleSpec
    trigger_pipeline_id?: string
  } => {
    if (cadence === "after_pipeline") {
      // No spec at all: an event trigger has no cadence to describe, and sending a
      // leftover cron would be stored beside a type that never reads it.
      return {
        schedule_type: "after_pipeline",
        schedule_spec: {},
        trigger_pipeline_id: triggerPipelineId,
      }
    }
    if (cadence === "interval") {
      return { schedule_type: "interval", schedule_spec: { every_seconds: parseInt(everySeconds, 10) } }
    }
    const cron = cadence === "custom" ? cronExpr.trim() : buildCron(cadence, hour, minute, weekday)
    return { schedule_type: "cron", schedule_spec: { cron, timezone } }
  }

  /**
   * Returns whether a run of this query now does something, so a caller that needs
   * that in place before its own request (see handleCreateSchedule) can stop on
   * failure instead of firing a call the server is bound to refuse.
   *
   * `silent` suppresses the success toast only — every failure is always reported,
   * because the caller cannot say anything more useful than the server's own reason.
   */
  const handleSaveMode = async (opts?: { silent?: boolean }): Promise<boolean> => {
    setSavingMode(true)
    try {
      const res = await authFetch(`/api/v1/explorer/saved/${savedQueryId}/materialization`, {
        method: "PUT",
        body: JSON.stringify({
          materialization: mode,
          // Only table mode has a target to send. Passing the field along in the other
          // two modes would ask the server to keep a table this query no longer writes,
          // and the row would then describe two destinations, one of them stale.
          target_table: mode === "table" ? target.trim() : "",
        }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(data?.error || "Could not save what a run of this query does")
        return false
      }
      // Trust the response over the form: the server normalises the table name, and
      // this value is what unlocks the schedule controls.
      const stored = normalizeMode(data?.materialization)
      setSavedMode({
        materialization: stored,
        targetTable: stored === "table" ? String(data?.target_table ?? target.trim()) : "",
      })
      if (!opts?.silent) {
        toast.success(
          stored === "table"
            ? `This query now rebuilds ${data?.target_table || target.trim()}`
            : stored === "statement"
              ? "This query now runs its SQL as written"
              : "This query no longer runs on its own"
        )
      }
      onChanged()
      return stored !== "none"
    } catch {
      toast.error("Could not save what a run of this query does")
      return false
    } finally {
      setSavingMode(false)
    }
  }

  const handleRunNow = async () => {
    setRunning(true)
    try {
      const res = await authFetch(`/api/v1/explorer/saved/${savedQueryId}/run`, { method: "POST" })
      const data = await res.json().catch(() => ({}))
      if (res.ok) {
        // Two modes, two different things to report. "Rebuilt …" is a lie about a
        // MERGE, and a statement model has no target table to name — what it has is
        // a row count, which the server only reports for a DML write.
        const rows = typeof data?.rows_affected === "number" ? (data.rows_affected as number) : null
        toast.success(
          savedMode.materialization === "statement"
            ? rows === null
              ? "Statement ran"
              : `Statement ran — ${rows} row${rows === 1 ? "" : "s"} affected`
            : `Rebuilt ${data?.target_table || "the target table"}`
        )
      } else {
        // 400 = rsync refused (wrong class for the mode, no target, unsupported
        // connector); 422 = the engine rejected the statement. Both carry a usable
        // message, and neither is a server fault worth a generic "something went wrong".
        toast.error(data?.error || "The run did not complete")
      }
      onChanged()
    } catch {
      toast.error("Could not run the model")
    } finally {
      setRunning(false)
    }
  }

  const scheduleAction = async (
    path: string,
    method: string,
    body?: unknown,
    successMessage?: string
  ) => {
    if (scheduleBusy) return
    setScheduleBusy(true)
    try {
      const res = await authFetch(`/api/v1/explorer/saved/${savedQueryId}/schedule${path}`, {
        method,
        ...(body ? { body: JSON.stringify(body) } : {}),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) {
        toast.error(data?.error || "The schedule change did not apply")
        return
      }
      if (successMessage) toast.success(successMessage)
      await loadSchedule()
      onChanged()
    } catch {
      toast.error("The schedule change did not apply")
    } finally {
      setScheduleBusy(false)
    }
  }

  // What the SERVER has: whether a run, right now, would do anything. Run now and
  // the schedule controls act on this, not on the form.
  const runnable =
    savedMode.materialization === "statement" ||
    (savedMode.materialization === "table" && savedMode.targetTable !== "")
  const modeDirty =
    mode !== savedMode.materialization ||
    (mode === "table" && target.trim() !== savedMode.targetTable)
  const shortInterval = cadence === "interval" && Number(everySeconds) > 0 && Number(everySeconds) < 300

  // The list plus whatever the live schedule already points at. Without the union, a
  // trigger whose pipeline has since left this list — deleted, or moved to another
  // workspace — renders as an empty picker, which reads as "nothing selected" for a
  // trigger that is in fact still firing.
  const pipelineOptions = useMemo<TriggerPipeline[]>(() => {
    const stored = schedule?.trigger_pipeline_id
    if (!stored || pipelines.some((p) => p.id === stored)) return pipelines
    return [...pipelines, { id: stored, name: schedule?.trigger_pipeline_name?.trim() || stored }]
  }, [pipelines, schedule?.trigger_pipeline_id, schedule?.trigger_pipeline_name])

  // An event trigger is only a trigger once it names an upstream. Checked here rather
  // than left to the server's 400 so the button explains itself before the click.
  const triggerIncomplete = cadence === "after_pipeline" && triggerPipelineId === ""

  // What a schedule needs before it can be created: the form must describe something
  // for a run to do, whether or not it has been saved yet. The saving is this
  // component's job to sequence, not the user's to remember.
  const canCreateSchedule =
    (mode === "statement" || (mode === "table" && target.trim() !== "")) && !triggerIncomplete

  /**
   * The stored class is advisory — the server re-classifies the live SQL on every run
   * and is the only thing that decides. Saying it here moves the refusal from after
   * the click to before it, which is the difference between a mistake and a dead end.
   */
  const modeConflict =
    mode === "table" && isWriteClass(statementClass)
      ? `This query is stored as ${statementClass}, not a read. Writing results to a table wraps the SQL in CREATE TABLE … AS, which only works for a SELECT — pick “Run the SQL as written” instead.`
      : mode === "statement" && statementClass === "read"
        ? "This query is stored as a read. Running a SELECT on a schedule delivers its rows nowhere — pick “Write the results to a table” instead."
        : ""

  /**
   * Creating a schedule needs the mode saved first — the server enforces that with a
   * 400, and that invariant is unchanged. What changed is who does the sequencing:
   * this saves the choice and then creates the schedule on one click, instead of
   * disabling the schedule editor until the user works out that a separate Save
   * button had to be pressed first.
   */
  const handleCreateSchedule = async () => {
    if (scheduleBusy || savingMode) return
    if (modeDirty || !runnable) {
      // Silent: on the combined path saving the mode is a step, not an outcome, and
      // two success toasts for one click describes the old two-stage flow.
      if (!(await handleSaveMode({ silent: true }))) return
    }
    await scheduleAction("", "POST", specForRequest(), "Schedule created")
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg">
        <DialogHeader>
          <DialogTitle className="truncate">{savedQueryName}</DialogTitle>
          <DialogDescription>
            Decide what running this query does — build a table from its results, or run the
            SQL as written — and how often. Everything happens inside the same database:
            rsync only sends the statement, so no rows leave the warehouse.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-5 max-h-[60vh] overflow-y-auto pr-1">
          {/* Say it once, at the top, rather than leaving someone to discover it one
              greyed-out button at a time. Suppressed while the role is still loading:
              the controls are disabled then too, but "you need Admin" would be an
              assertion we cannot yet make. */}
          {readOnly && !roleLoading && (
            <div className="flex items-start gap-2 rounded-md border bg-zinc-50 p-2 dark:bg-zinc-900">
              <Lock className="mt-0.5 h-3.5 w-3.5 shrink-0 text-zinc-500" />
              <p className="text-xs text-zinc-600 dark:text-zinc-400">
                You can see what this query does and when it runs, but changing either needs
                the Admin or Owner role in this workspace — a scheduled run writes to the
                warehouse unattended. Ask a workspace owner to make the change or to raise
                your role.
              </p>
            </div>
          )}

          {/* Said once at the top for the same reason the role notice is: the alternative
              is a user reading three greyed-out radio buttons and concluding rsync is
              broken. Shown regardless of role — a viewer looking at a BigQuery query
              should not be told the only obstacle is their role. */}
          {engineBlocked && (
            <div className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-2">
              <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
              <p className="text-xs text-amber-700 dark:text-amber-300">
                {ENGINE_HINT}. Running a query against {engineName} works, but rsync has no
                way to execute the CREATE TABLE the rebuild needs, so this query can be saved
                and run by hand but not turned into a table or put on a schedule.
              </p>
            </div>
          )}

          {/* ---------------- What a run does ---------------- */}
          <section className="space-y-3">
            <div className="space-y-1">
              <Label>What a run of this query does</Label>
              <p className="text-xs text-zinc-500">
                This follows from the SQL rather than from a preference: a SELECT has to be
                given somewhere to land, while a MERGE, UPDATE or INSERT already names its
                own destination.
              </p>
            </div>

            <RadioGroup
              value={mode}
              onValueChange={(v) => setMode(v as ModelMode)}
              disabled={readOnly}
            >
              {MODE_CHOICES.map((choice) => {
                // "none" survives an engine block on purpose — the server allows it for
                // every connector, and it is the only way to switch off a model that was
                // configured before this connection's engine was known not to support one.
                const choiceBlocked = engineBlocked && choice.value !== "none"
                const disabled = readOnly || choiceBlocked
                return (
                  <label
                    key={choice.value}
                    htmlFor={`model-mode-${choice.value}`}
                    className={`flex items-start gap-3 rounded-md border p-3 ${
                      mode === choice.value ? "border-violet-500/60 bg-violet-500/5" : ""
                    } ${disabled ? "cursor-not-allowed opacity-70" : "cursor-pointer"}`}
                    title={choiceBlocked ? ENGINE_HINT : undefined}
                  >
                    <RadioGroupItem
                      id={`model-mode-${choice.value}`}
                      value={choice.value}
                      disabled={choiceBlocked}
                      className="mt-0.5 shrink-0"
                    />
                    <div className="space-y-0.5">
                      <div className="flex items-center gap-1.5 text-sm font-medium">
                        {choice.icon}
                        {choice.label}
                      </div>
                      <p className="text-xs text-zinc-500">{choice.hint}</p>
                    </div>
                  </label>
                )
              })}
            </RadioGroup>

            {mode === "table" && (
              <div className="space-y-1.5">
                <Label htmlFor="model-target">Target table</Label>
                <Input
                  id="model-target"
                  value={target}
                  onChange={(e) => setTarget(e.target.value)}
                  placeholder="analytics.daily_mrr"
                  spellCheck={false}
                  className="font-mono text-sm"
                  disabled={readOnly}
                />
                <p className="text-xs text-zinc-500">
                  <code>schema.table</code> or <code>table</code>. Letters, digits and underscores
                  only, 52 characters per part.
                </p>
              </div>
            )}

            {/* Before the click, not after the refusal. */}
            {modeConflict && !readOnly && (
              <div className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-2">
                <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
                <p className="text-xs text-amber-700 dark:text-amber-300">{modeConflict}</p>
              </div>
            )}

            <div className="flex items-center gap-2">
              <Button
                size="sm"
                onClick={() => void handleSaveMode()}
                disabled={
                  readOnly ||
                  savingMode ||
                  !modeDirty ||
                  (mode === "table" && !target.trim()) ||
                  // Saving "none" stays open even on a blocked engine: that PUT succeeds.
                  (engineBlocked && mode !== "none")
                }
                title={readOnly ? MANAGE_ROLE_HINT : engineBlocked && mode !== "none" ? ENGINE_HINT : undefined}
              >
                {savingMode ? <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" /> : null}
                Save
              </Button>
              <Button
                size="sm"
                variant="outline"
                onClick={() => void handleRunNow()}
                // A stored model on a blocked engine can still exist — it was configured
                // before the gate, or the connection was re-typed under it. The rebuild
                // would refuse, so this refuses first.
                disabled={readOnly || running || !runnable || engineBlocked}
                title={
                  readOnly
                    ? MANAGE_ROLE_HINT
                    : engineBlocked
                      ? ENGINE_HINT
                      : !runnable
                      ? "Choose what a run does, and save it, first"
                      : savedMode.materialization === "statement"
                        ? "Run the SQL now, as written"
                        : "Rebuild the table now"
                }
              >
                {running ? (
                  <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" />
                ) : (
                  <Play className="h-3.5 w-3.5 mr-1" />
                )}
                Run now
              </Button>
            </div>

            {lastRunStatus === "failed" && lastRunError && (
              <div className="flex items-start gap-2 rounded-md border border-red-500/40 bg-red-500/10 p-2">
                <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-red-600 dark:text-red-400" />
                <p className="text-xs text-red-700 dark:text-red-300 break-words">
                  Last run failed: {lastRunError}
                </p>
              </div>
            )}
          </section>

          {/* ---------------- Schedule ---------------- */}
          <section className="space-y-3 border-t pt-4">
            <div className="flex items-center gap-2">
              <Clock className="h-3.5 w-3.5 text-zinc-500" />
              <span className="text-sm font-medium">Schedule</span>
              {schedule && (
                <Badge variant={schedule.status === "active" ? "default" : "secondary"}>
                  {schedule.status}
                </Badge>
              )}
              {schedule?.blocked && (
                <Badge variant="outline" className="border-amber-500/60 text-amber-600 dark:text-amber-400">
                  not running
                </Badge>
              )}
            </div>

            {loadingSchedule ? (
              <div className="flex items-center text-xs text-zinc-500">
                <Loader2 className="h-3.5 w-3.5 mr-1.5 animate-spin" />
                Loading schedule…
              </div>
            ) : scheduleError ? (
              /* Never fall through to the create/empty state on an unknown: a schedule
                 may well be live, and offering "Create schedule" would misreport that. */
              <div className="flex items-start gap-2 rounded-md border border-red-500/40 bg-red-500/10 p-2">
                <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-red-600 dark:text-red-400" />
                <div className="space-y-1.5">
                  <p className="text-xs text-red-700 dark:text-red-300">
                    {scheduleError} Any existing schedule is unaffected and may still be running.
                  </p>
                  <Button size="sm" variant="outline" onClick={() => void loadSchedule()}>
                    Retry
                  </Button>
                </div>
              </div>
            ) : (
              <>
                {/* A schedule outlives the thing it runs: setting the mode back to
                    "nothing on its own" does not delete the schedule row or its Temporal
                    counterpart, it only makes each fire refuse. Hiding these controls in
                    that state stranded a schedule the user could still see firing but
                    not stop. */}
                {!runnable && schedule && (
                  <div className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-2">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
                    <p className="text-xs text-amber-700 dark:text-amber-300">
                      A run of this query does nothing, so every scheduled run will refuse.
                      Choose what a run does above, or delete the schedule below.
                    </p>
                  </div>
                )}

                {schedule && (
                  <div className="rounded-md border p-2 text-xs text-zinc-600 dark:text-zinc-400">
                    {describeSchedule(schedule)}
                  </div>
                )}

                {/* An auto-pause is a security stop, not an operator pause: the run-as
                    user lost their role, or the SQL stopped being a read. Resume
                    re-checks the condition server-side and can refuse. */}
                {schedule?.auto_paused_reason && (
                  <div className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-2">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
                    <p className="text-xs text-amber-700 dark:text-amber-300">
                      Paused automatically: {schedule.auto_paused_reason}
                    </p>
                  </div>
                )}
                {schedule?.blocked_reason && (
                  <div className="flex items-start gap-2 rounded-md border border-amber-500/40 bg-amber-500/10 p-2">
                    <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
                    <p className="text-xs text-amber-700 dark:text-amber-300">{schedule.blocked_reason}</p>
                  </div>
                )}
                {schedule?.paused_reason && (
                  <p className="text-xs text-zinc-500">Paused: {schedule.paused_reason}</p>
                )}

                <div className="grid gap-3">
                  <div className="space-y-1.5">
                    {/* htmlFor/id throughout this section: a bare <Label> beside a Radix
                        trigger is visually a label and programmatically nothing, so the
                        control reads to a screen reader as an unnamed combobox. */}
                    <Label htmlFor="model-cadence">Runs</Label>
                    <Select value={cadence} onValueChange={(v) => setCadence(v as Cadence)} disabled={readOnly}>
                      <SelectTrigger id="model-cadence">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="hour">Every hour</SelectItem>
                        <SelectItem value="day">Every day</SelectItem>
                        <SelectItem value="week">Every week</SelectItem>
                        <SelectItem value="interval">Fixed interval</SelectItem>
                        <SelectItem value="custom">Custom cron</SelectItem>
                        <SelectItem value="after_pipeline">After a pipeline runs</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  {cadence === "after_pipeline" && (
                    <div className="space-y-1.5">
                      <Label htmlFor="model-trigger-pipeline">Pipeline</Label>
                      {pipelinesError ? (
                        <p className="text-xs text-amber-600 dark:text-amber-400">
                          Could not load this workspace&apos;s pipelines. Close and reopen this
                          dialog to try again, or pick a clock schedule instead.
                        </p>
                      ) : loadingPipelines ? (
                        <p className="text-xs text-zinc-500">Loading pipelines…</p>
                      ) : pipelineOptions.length === 0 ? (
                        <p className="text-xs text-zinc-500">
                          This workspace has no pipelines yet. A pipeline is what loads the
                          tables this query reads, so there is nothing for it to follow — pick a
                          clock schedule instead.
                        </p>
                      ) : (
                        <Select
                          value={triggerPipelineId}
                          onValueChange={setTriggerPipelineId}
                          disabled={readOnly}
                        >
                          <SelectTrigger id="model-trigger-pipeline">
                            <SelectValue placeholder="Choose a pipeline" />
                          </SelectTrigger>
                          <SelectContent className="max-h-64">
                            {pipelineOptions.map((p) => (
                              <SelectItem key={p.id} value={p.id}>
                                {p.name}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                      )}
                      {!pipelinesError && !loadingPipelines && pipelineOptions.length > 0 && (
                        <UpstreamHint
                          upstreams={upstreams}
                          pipelineOptions={pipelineOptions}
                          selectedId={triggerPipelineId}
                          onPick={setTriggerPipelineId}
                          disabled={readOnly}
                        />
                      )}
                      <p className="text-xs text-zinc-500">
                        The query rebuilds each time that pipeline finishes successfully — so it
                        reads the data the pipeline just loaded, rather than whatever happened to
                        be there when a clock struck. A failed pipeline run rebuilds nothing.
                      </p>
                    </div>
                  )}

                  {cadence === "week" && (
                    <div className="space-y-1.5">
                      <Label htmlFor="model-weekday">On</Label>
                      <Select value={weekday} onValueChange={setWeekday} disabled={readOnly}>
                        <SelectTrigger id="model-weekday">
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          {WEEKDAYS.map((d) => (
                            <SelectItem key={d.value} value={d.value}>
                              {d.label}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  )}

                  {(cadence === "day" || cadence === "week" || cadence === "hour") && (
                    <div className="space-y-1.5">
                      <Label>At</Label>
                      <div className="flex items-center gap-2">
                        {cadence !== "hour" && (
                          <>
                            {/* aria-label rather than htmlFor: two controls share the one
                                visible "At" label, which can only point at one of them. */}
                            <Select value={hour} onValueChange={setHour} disabled={readOnly}>
                              <SelectTrigger className="w-[88px]" aria-label="Hour">
                                <SelectValue />
                              </SelectTrigger>
                              <SelectContent className="max-h-64">
                                {hours.map((h) => (
                                  <SelectItem key={h} value={h}>
                                    {h}
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                            <span className="text-sm text-zinc-500">:</span>
                          </>
                        )}
                        <Select value={minute} onValueChange={setMinute} disabled={readOnly}>
                          <SelectTrigger className="w-[88px]" aria-label="Minute">
                            <SelectValue />
                          </SelectTrigger>
                          <SelectContent className="max-h-64">
                            {minutes.map((m) => (
                              <SelectItem key={m} value={m}>
                                {m}
                              </SelectItem>
                            ))}
                          </SelectContent>
                        </Select>
                        {cadence === "hour" && (
                          <span className="text-sm text-zinc-500">minutes past the hour</span>
                        )}
                      </div>
                    </div>
                  )}

                  {cadence === "custom" && (
                    <div className="space-y-1.5">
                      <Label htmlFor="model-cron">Cron expression</Label>
                      <Input
                        id="model-cron"
                        value={cronExpr}
                        onChange={(e) => setCronExpr(e.target.value)}
                        placeholder="0 2 * * *"
                        className="font-mono text-sm"
                        disabled={readOnly}
                      />
                    </div>
                  )}

                  {cadence === "interval" ? (
                    <div className="space-y-1.5">
                      <Label htmlFor="model-interval">Run every (seconds)</Label>
                      <Input
                        id="model-interval"
                        type="number"
                        min="60"
                        value={everySeconds}
                        onChange={(e) => setEverySeconds(e.target.value)}
                        disabled={readOnly}
                      />
                      {/* The 60s floor is a validation bound, not a promised cadence:
                          a tick arriving while the previous rebuild is still running is
                          dropped, not queued. */}
                      {shortInterval && (
                        <p className="text-xs text-amber-600 dark:text-amber-400">
                          A tick that lands while the previous rebuild is still going is skipped,
                          not queued — a rebuild slower than {everySeconds}s will effectively run
                          at half this rate or less.
                        </p>
                      )}
                    </div>
                  ) : cadence === "after_pipeline" ? null : (
                    // Not shown for an event trigger: there is no clock to place in a
                    // zone, and offering one would suggest the trigger has a time.
                    <div className="space-y-1.5">
                      <Label htmlFor="model-timezone">Timezone</Label>
                      <Select value={timezone} onValueChange={setTimezone} disabled={readOnly}>
                        <SelectTrigger id="model-timezone">
                          <SelectValue placeholder="UTC" />
                        </SelectTrigger>
                        <SelectContent className="max-h-64">
                          {timezones.map((tz) => (
                            <SelectItem key={tz} value={tz}>
                              {tz}
                            </SelectItem>
                          ))}
                        </SelectContent>
                      </Select>
                    </div>
                  )}
                </div>

                {/* The disabled Create button carries this same reason in its title,
                    which a keyboard user never sees and a touch user cannot hover.
                    State it in the page for the one case that blocks them. */}
                {!schedule && !canCreateSchedule && !readOnly && !engineBlocked && !triggerIncomplete && (
                  <p className="text-xs text-zinc-500">
                    Choose what a run of this query does above — and name the table, if it
                    writes one. A scheduled query that writes nowhere can only fail. Creating
                    the schedule saves that choice for you.
                  </p>
                )}
                {/* Its own line rather than a second reason folded into the one above:
                    the two block the button for unrelated reasons, and a missing pipeline
                    is not fixed by anything in the "what a run does" section. */}
                {triggerIncomplete && !readOnly && !engineBlocked && pipelineOptions.length > 0 && (
                  <p className="text-xs text-zinc-500">
                    Choose the pipeline this query should follow.
                  </p>
                )}

                <div className="flex flex-wrap items-center gap-2">
                  {schedule ? (
                    <>
                      <Button
                        size="sm"
                        disabled={readOnly || scheduleBusy || triggerIncomplete}
                        title={
                          readOnly
                            ? MANAGE_ROLE_HINT
                            : triggerIncomplete
                              ? "Choose the pipeline this query should follow"
                              : undefined
                        }
                        onClick={() => void scheduleAction("", "PUT", specForRequest(), "Schedule updated")}
                      >
                        Update schedule
                      </Button>
                      {schedule.status === "active" ? (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={readOnly || scheduleBusy}
                          onClick={() =>
                            void scheduleAction("/pause", "POST", { reason: "Paused by user" }, "Schedule paused")
                          }
                        >
                          <Pause className="h-3.5 w-3.5 mr-1" />
                          Pause
                        </Button>
                      ) : (
                        <Button
                          size="sm"
                          variant="outline"
                          disabled={readOnly || scheduleBusy}
                          onClick={() => void scheduleAction("/resume", "POST", undefined, "Schedule resumed")}
                        >
                          <Play className="h-3.5 w-3.5 mr-1" />
                          Resume
                        </Button>
                      )}
                      {/* Deleting deregisters the Temporal schedule. Pause is the
                          reversible option and sits right beside it, so this one asks
                          first rather than acting on a single click. */}
                      <Button
                        size="sm"
                        variant="outline"
                        disabled={readOnly || scheduleBusy}
                        onClick={() => setConfirmDeleteSchedule(true)}
                      >
                        <Trash2 className="h-3.5 w-3.5 mr-1" />
                        Delete
                      </Button>
                    </>
                  ) : (
                    <Button
                      size="sm"
                      // engineBlocked is checked on top of canCreateSchedule rather than
                      // folded into it: a query stored as a model before this gate existed
                      // still satisfies the form test, and creating a schedule saves the
                      // mode first — which is the PUT that would 400.
                      disabled={readOnly || scheduleBusy || savingMode || !canCreateSchedule || engineBlocked}
                      title={
                        readOnly
                          ? MANAGE_ROLE_HINT
                          : engineBlocked
                            ? ENGINE_HINT
                            : triggerIncomplete
                              ? "Choose the pipeline this query should follow"
                              : canCreateSchedule
                                ? modeDirty || !runnable
                                  ? "Saves what a run does, then creates the schedule"
                                  : undefined
                                : "Choose what a run of this query does above — a scheduled query that writes nowhere can only fail"
                      }
                      onClick={() => void handleCreateSchedule()}
                    >
                      {scheduleBusy || savingMode ? (
                        <Loader2 className="h-3.5 w-3.5 mr-1 animate-spin" />
                      ) : null}
                      Create schedule
                    </Button>
                  )}
                </div>
              </>
            )}
          </section>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>

        <AlertDialog open={confirmDeleteSchedule} onOpenChange={setConfirmDeleteSchedule}>
          <AlertDialogContent>
            <AlertDialogHeader>
              <AlertDialogTitle>Delete this schedule?</AlertDialogTitle>
              <AlertDialogDescription>
                {schedule ? describeSchedule(schedule) : "This schedule"} will stop running and
                be deregistered. This cannot be undone — recreating it later starts a new
                schedule. The saved query and the table it built are not affected. To stop it
                temporarily instead, use Pause.
              </AlertDialogDescription>
            </AlertDialogHeader>
            <AlertDialogFooter>
              <AlertDialogCancel>Cancel</AlertDialogCancel>
              <AlertDialogAction
                onClick={() => void scheduleAction("", "DELETE", undefined, "Schedule deleted")}
              >
                Delete schedule
              </AlertDialogAction>
            </AlertDialogFooter>
          </AlertDialogContent>
        </AlertDialog>
      </DialogContent>
    </Dialog>
  )
}
