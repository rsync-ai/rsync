/**
 * The one place that answers "how long did this take" — both the number and the
 * words for it.
 *
 * Why one place: the prod retest kept reporting the same shape of bug — two
 * panels showing different durations for the same stage of the same run. It was
 * never one bug. `src/` held NINE independent duration formatters, and they
 * disagreed with each other — five of them like this:
 *
 *   input        dagHelpers  transform-format  scheduledModel  accordion  PipelinesTable
 *   500ms        "500ms"     "500ms"           "500ms"         "500ms"    "0s"
 *   5_500ms      "5.5s"      "5.5s"            "5.5s"          "5s"       "5s"
 *   179_600ms    "2m 60s"    "2m 59s"          "3m 00s"        "2m 59s"   "2m 59s"
 *   7_200_000ms  "120m"      "2h"              "2h 00m"        "120m 0s"  "2h 0m"
 *
 * The "2m 60s" in that table is the giveaway: #37 fixed exactly that rounding —
 * in ONE copy. The other eight kept the bug, and a later GUI round finds it again
 * in a panel nobody opened last time. Copies are why the fix/test/fix loop never
 * ends, so there is one implementation here and a guard
 * (`__tests__/one-duration-formatter.test.ts`) that fails CI on a tenth. That
 * guard found the last three itself, in panels this consolidation had missed.
 *
 * The algorithm kept is `transform-format`'s — it carries the #37 fix and is the
 * only one that renders hours correctly — plus two behaviours the copies got
 * right and it did not: rounding up into the next unit (999.6 ms is "1s", not
 * "1000ms") and dropping a trailing ".0" (a run "succeeded in 3s", not "3.0s").
 */

/**
 * Rendered form of a span of milliseconds: "184ms", "4.2s", "3m 28s", "1h 5m",
 * "3d 4h". Always two units at most — a third is noise at any scale where the
 * first two are large.
 */
export function formatDuration(ms: number | null | undefined): string {
  const v = Number(ms ?? 0)
  if (!Number.isFinite(v) || v <= 0) return "0ms"
  // Rounded before the unit is chosen, so 999.6 ms reads "1s" rather than the
  // "1000ms" that a per-branch round produces at the top of each unit.
  const whole = Math.round(v)
  if (whole < 1000) return `${whole}ms`
  // Tenths below a minute, with a trailing ".0" dropped: "1s" and "12.9s", not
  // "1.0s". The dropped zero is not cosmetic — three panels print this string
  // inside a sentence ("succeeded in 3s"), where "3.0s" claims a precision the
  // run timer does not have.
  const tenths = Math.round(v / 100)
  if (tenths < 600) return `${tenths / 10}s`
  // Having rounded up to a minute above, never fall back to "0m 59s" for the
  // 50 ms below it.
  const span = Math.max(v, 60_000)
  // Every unit below is floored, never rounded: rounding the remainder printed
  // "2m 60s" for 2m59.6s and "1h 60m" for 1h59m40s (#37).
  if (span < 3_600_000) {
    const m = Math.floor(span / 60_000)
    const s = Math.floor((span % 60_000) / 1000)
    return s ? `${m}m ${s}s` : `${m}m`
  }
  // Days, so a model three days past its deadline reads "3d 4h" rather than
  // "76h" — the freshness surface counts in days and had its own formatter for
  // exactly this branch.
  if (v < 86_400_000) {
    const h = Math.floor(v / 3_600_000)
    const m = Math.floor((v % 3_600_000) / 60_000)
    return m ? `${h}h ${m}m` : `${h}h`
  }
  const d = Math.floor(v / 86_400_000)
  const h = Math.floor((v % 86_400_000) / 3_600_000)
  return h ? `${d}d ${h}h` : `${d}d`
}

/**
 * The same words, for the callers that hold two timestamps rather than a span.
 * An absent `end` means "still running", measured to `nowMs`. Returns `"—"`
 * rather than a misleading "0ms" when either side is unreadable, because these
 * callers render into a table cell where a dash is the honest empty.
 */
export function formatDurationBetween(
  start?: string | null,
  end?: string | null,
  nowMs: number = Date.now(),
): string {
  if (!start) return "—"
  const s = new Date(start).getTime()
  const e = end ? new Date(end).getTime() : nowMs
  if (!Number.isFinite(s) || !Number.isFinite(e) || e < s) return "—"
  return formatDuration(e - s)
}

/**
 * The same words, but `"—"` instead of `"0ms"` when there is no measurement.
 *
 * Several panels render a duration into a table cell or a chart axis, where a
 * literal "0ms" reads as a measured zero rather than as "we never timed this".
 * They each grew their own guard around their own formatter; this is the one
 * guard, so the empty state is as consistent as the filled one.
 */
export function formatDurationOrDash(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(Number(ms)) || Number(ms) < 0) return "—"
  return formatDuration(ms)
}

/**
 * The coarse rendering, for "how long has this been true" rather than "how long
 * did this take": a service's uptime, a model's staleness. Takes SECONDS, since
 * every caller of this shape is reading a seconds field off an API.
 *
 * It differs from `formatDuration` on purpose, in one way: below an hour it
 * prints a single unit ("12m", not "12m 34s"). A freshness lag measured to the
 * second is false precision — the number moves while you read it — and the
 * panels that show it put it in a column two words wide.
 *
 * It lives here, next to the fine rendering, so the two cannot drift apart at
 * the hour and day boundaries, which is where the formatters this module
 * replaced disagreed most ("120m" / "2h" / "2h 00m" / "120m 0s").
 */
export function formatSpanCoarse(seconds: number): string {
  const s = Number.isFinite(seconds) ? Math.max(0, Math.floor(seconds)) : 0
  if (s < 60) return `${s}s`
  const d = Math.floor(s / 86_400)
  const h = Math.floor((s % 86_400) / 3600)
  const m = Math.floor((s % 3600) / 60)
  if (d > 0) return h > 0 ? `${d}d ${h}h` : `${d}d`
  if (h > 0) return m > 0 ? `${h}h ${m}m` : `${h}h`
  return `${m}m`
}

/** One transition of a stage, reduced to the two facts timing needs. */
export interface StageTransitionPoint {
  /** `STAGE_STARTED` / `STAGE_COMPLETED` / `STAGE_FAILED` … — case-insensitive. */
  type: string
  /** Epoch milliseconds. */
  at: number
}

export interface StageTiming {
  /**
   * How long the stage was actually working: the sum of its attempts. This is
   * the number to put in front of a user under the word "duration".
   */
  activeMs?: number
  /**
   * First start to last end, gaps included. Equal to `activeMs` for a stage that
   * ran once, and much larger for one that was retried fifteen minutes later.
   */
  elapsedMs?: number
  /** How many times the stage started. 0 when it never did. */
  attempts: number
  /** Start of the first attempt / end of the last one. */
  startedAt?: number
  completedAt?: number
  /** True while the newest attempt has no terminal transition yet. */
  running: boolean
}

const STARTED = "STAGE_STARTED"
const TERMINAL = new Set(["STAGE_COMPLETED", "STAGE_FAILED", "PIPELINE_COMPLETED", "PIPELINE_FAILED"])

/**
 * Stage timing read from the stage's own transitions, attempts and all.
 *
 * This exists because the Overview and the Activity feed each derived it, and
 * their rules only agreed for a stage that ran exactly once. On the live demo
 * pipeline `infra_preflight` ran twice — 15:58:29→15:58:46 and
 * 16:13:40→16:13:55 — and the two panels reported the same stage as "15m 26s"
 * and "15.0 s":
 *
 *   - the Overview took the FIRST start and the LAST end, so it billed the stage
 *     for the fifteen minutes between the attempts, when it was doing nothing;
 *   - the Activity feed walked back to the LAST start, so it reported the second
 *     attempt and silently dropped the first.
 *
 * Neither is the honest answer, which is "32s of work, over two attempts,
 * spanning 15m 26s". All three numbers are returned and the panels agree because
 * they no longer each invent one.
 *
 * @param points  Transitions for ONE stage. Sorted here, so callers need not.
 * @param nowMs   Used to measure an attempt that has not ended yet.
 */
export function stageTiming(
  points: StageTransitionPoint[],
  nowMs: number = Date.now(),
): StageTiming {
  const sorted = points
    .filter((p) => Number.isFinite(p.at))
    .slice()
    .sort((a, b) => a.at - b.at)

  let attempts = 0
  let activeMs = 0
  let openedAt: number | undefined
  let startedAt: number | undefined
  let completedAt: number | undefined

  const close = (end: number) => {
    if (openedAt === undefined) return
    // Clock skew between producers can put a terminal event a hair before its
    // own start; a negative attempt would subtract from the total.
    activeMs += Math.max(0, end - openedAt)
    completedAt = end
    openedAt = undefined
  }

  for (const p of sorted) {
    const type = String(p.type || "").toUpperCase()
    if (type === STARTED) {
      // A second start with no terminal in between means the previous attempt's
      // ending was never recorded. Close it where the evidence stops rather than
      // letting it swallow the gap up to this start.
      close(p.at)
      openedAt = p.at
      attempts += 1
      startedAt = startedAt ?? p.at
    } else if (TERMINAL.has(type)) {
      if (openedAt === undefined) {
        // A terminal with no start of its own: the start is on a feed page that
        // was never loaded. It still dates the stage's end.
        completedAt = p.at
        startedAt = startedAt ?? p.at
      } else {
        close(p.at)
      }
    }
  }

  const running = openedAt !== undefined
  if (running) activeMs += Math.max(0, nowMs - openedAt!)

  const end = running ? nowMs : completedAt
  const elapsedMs =
    startedAt !== undefined && end !== undefined ? Math.max(0, end - startedAt) : undefined

  return {
    activeMs: attempts > 0 || completedAt !== undefined ? activeMs : undefined,
    elapsedMs,
    attempts,
    startedAt,
    completedAt: running ? undefined : completedAt,
    running,
  }
}

/**
 * What a panel puts next to a stage. The attempt count rides along whenever
 * there was more than one, so the gap `activeMs` leaves out is visible instead
 * of merely missing.
 */
export function formatStageDuration(timing: StageTiming): string | null {
  if (timing.activeMs === undefined) return null
  const base = formatDuration(timing.activeMs)
  return timing.attempts > 1 ? `${base} · ${timing.attempts} attempts` : base
}
