/**
 * A model's cron schedule, in words: "Every day at 03:00", "Every 15 minutes, Monday to
 * Friday". Anything this cannot say exactly returns null, and the caller shows the
 * expression as the user typed it. A sentence that is nearly right is worse than the
 * raw cron, because nobody re-reads the cron after reading the sentence.
 *
 * The grammar is the gateway's, not generic cron: the gateway validates every schedule
 * with robfig/cron v1.2.0's five-field parser (validateScheduleSpec in
 * pipeline_schedules.go, which validateModelScheduleSpec wraps, and nextScheduleRun in
 * saved_query_schedules.go), so no seconds field, no @daily descriptors, three-letter
 * month and weekday names only, weekdays 0-6. Anything outside that is refused here too.
 * Temporal then fires the same string (createTemporalModelSchedule), and the two differ
 * on one point, the day fields; only schedules both agree on get a sentence.
 *
 * This describes the pattern only. It never computes when a run happens; the server's
 * next_run_at does that (see scheduleTime.ts).
 */

interface Field {
  /**
   * Some part of the field is "*" or "?", with or without a step. The parser marks such a
   * field, and whether a day field is marked decides how the two day fields combine.
   */
  star: boolean
  /** Every value the field matches, ascending. */
  values: number[]
}

interface Bounds {
  min: number
  max: number
  names?: Record<string, number>
}

const MINUTES: Bounds = { min: 0, max: 59 }
const HOURS: Bounds = { min: 0, max: 23 }
const DAYS_OF_MONTH: Bounds = { min: 1, max: 31 }
const MONTHS: Bounds = {
  min: 1,
  max: 12,
  names: { jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12 },
}
const DAYS_OF_WEEK: Bounds = {
  min: 0,
  max: 6,
  names: { sun: 0, mon: 1, tue: 2, wed: 3, thu: 4, fri: 5, sat: 6 },
}

const MONTH_NAMES = [
  "",
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
]
const WEEKDAY_NAMES = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]

/** More listed times than this and the sentence is harder to read than the cron. */
const MAX_LISTED_TIMES = 6

function parseNumber(text: string, bounds: Bounds): number | null {
  const named = bounds.names?.[text.toLowerCase()]
  if (named !== undefined) return named
  return /^\d+$/.test(text) ? Number(text) : null
}

/** One field, the way robfig's getField reads it, or null if the parser would refuse it. */
function parseField(text: string, bounds: Bounds): Field | null {
  const matched = new Set<number>()
  let star = false
  for (const part of text.split(",")) {
    const pieces = part.split("/")
    if (pieces.length > 2) return null
    const [range, stepText] = pieces
    let step = 1
    if (stepText !== undefined) {
      if (!/^\d+$/.test(stepText) || Number(stepText) === 0) return null
      step = Number(stepText)
    }
    let start: number
    let end: number
    if (range === "*" || range === "?") {
      star = true
      start = bounds.min
      end = bounds.max
    } else {
      const ends = range.split("-")
      if (ends.length > 2) return null
      const low = parseNumber(ends[0], bounds)
      const high = ends.length === 2 ? parseNumber(ends[1], bounds) : low
      if (low === null || high === null) return null
      start = low
      // "N/step" means N through the maximum, in steps.
      end = ends.length === 1 && stepText !== undefined ? bounds.max : high
    }
    if (start < bounds.min || end > bounds.max || start > end) return null
    for (let v = start; v <= end; v += step) matched.add(v)
  }
  return { star, values: [...matched].sort((a, b) => a - b) }
}

function covers(field: Field, bounds: Bounds): boolean {
  return field.values.length === bounds.max - bounds.min + 1
}

/** The common step if the values are evenly spaced, else null. */
function evenStep(values: number[]): number | null {
  if (values.length < 2) return null
  const step = values[1] - values[0]
  for (let i = 2; i < values.length; i++) {
    if (values[i] - values[i - 1] !== step) return null
  }
  return step
}

/** Evenly spaced and wrapping around exactly: every n, all day or all hour long. */
function repeatsEvery(values: number[], period: number): number | null {
  if (values.length === period) return 1
  const step = evenStep(values)
  if (step === null || period % step !== 0 || values.length !== period / step) return null
  return step
}

function pad2(n: number): string {
  return String(n).padStart(2, "0")
}

function clock(hour: number, minute: number): string {
  return `${pad2(hour)}:${pad2(minute)}`
}

function joinWords(words: string[]): string {
  if (words.length <= 1) return words.join("")
  return `${words.slice(0, -1).join(", ")} and ${words[words.length - 1]}`
}

/** A contiguous run of three or more, which reads better as "Monday to Friday" than as a list. */
function isRange(values: number[]): boolean {
  return values.length >= 3 && evenStep(values) === 1
}

/** "Monday to Friday" for a range, else "Mondays and Fridays". */
function namedSet(values: number[], names: string[], plural: string): string {
  if (isRange(values)) return `${names[values[0]]} to ${names[values[values.length - 1]]}`
  return joinWords(values.map((v) => `${names[v]}${plural}`))
}

type TimePart = { kind: "listed"; text: string } | { kind: "repeating"; text: string }

function listedTimes(minutes: number[], hours: number[]): TimePart | null {
  if (minutes.length * hours.length > MAX_LISTED_TIMES) return null
  const times = hours.flatMap((h) => minutes.map((m) => clock(h, m)))
  return { kind: "listed", text: joinWords(times) }
}

function describeTimes(minute: Field, hour: Field): TimePart | null {
  const minutes = minute.values
  const hours = hour.values

  if (minutes.length * hours.length <= 2) return listedTimes(minutes, hours)

  if (minutes.length === 1) {
    const m = minutes[0]
    if (covers(hour, HOURS)) {
      return {
        kind: "repeating",
        text: m === 0 ? "Every hour on the hour" : `Every hour at ${m} minute${m === 1 ? "" : "s"} past`,
      }
    }
    const allDay = repeatsEvery(hours, 24)
    if (allDay !== null) {
      return { kind: "repeating", text: `Every ${allDay} hours from ${clock(hours[0], m)}` }
    }
    const step = evenStep(hours)
    if (step !== null && hours.length >= 3) {
      const every = step === 1 ? "Every hour" : `Every ${step} hours`
      return { kind: "repeating", text: `${every} from ${clock(hours[0], m)} to ${clock(hours[hours.length - 1], m)}` }
    }
    return listedTimes(minutes, hours)
  }

  const allHour = repeatsEvery(minutes, 60)
  if (allHour !== null) {
    const every = allHour === 1 ? "Every minute" : `Every ${allHour} minutes`
    if (covers(hour, HOURS)) {
      return { kind: "repeating", text: minutes[0] === 0 ? every : `${every} from ${clock(0, minutes[0])}` }
    }
    // Across contiguous hours the minutes run on without a gap, so the window is exact.
    if (hours.length === 1 || evenStep(hours) === 1) {
      const last = minutes[minutes.length - 1]
      return {
        kind: "repeating",
        text: `${every} from ${clock(hours[0], minutes[0])} to ${clock(hours[hours.length - 1], last)}`,
      }
    }
  }
  return listedTimes(minutes, hours)
}

/** More listed days of the month than this and the cron reads better. */
const MAX_LISTED_DAYS = 4

/** The days a schedule fires on: "" for every day, null if it cannot be said exactly. */
function describeDays(dayOfMonth: Field, dayOfWeek: Field): string | null {
  const allDom = covers(dayOfMonth, DAYS_OF_MONTH)
  const allDow = covers(dayOfWeek, DAYS_OF_WEEK)
  // Temporal, which fires the schedule, always needs a day to match BOTH fields.
  // robfig's dayMatches, which the gateway's next run comes from, agrees only when one of
  // them is starred; with neither starred it fires on a day matching EITHER. Say nothing
  // where they could disagree.
  if (!dayOfMonth.star && !dayOfWeek.star && !(allDom && allDow)) return null
  if (!allDom && !allDow) return null
  if (!allDow) {
    const days = dayOfWeek.values
    return isRange(days) ? namedSet(days, WEEKDAY_NAMES, "") : `on ${namedSet(days, WEEKDAY_NAMES, "s")}`
  }
  if (!allDom) {
    const days = dayOfMonth.values
    if (isRange(days)) return `on days ${days[0]} to ${days[days.length - 1]} of the month`
    if (days.length > MAX_LISTED_DAYS) return null
    return `on day${days.length === 1 ? "" : "s"} ${joinWords(days.map(String))} of the month`
  }
  return ""
}

export function describeCron(cron: string | undefined | null): string | null {
  const fields = String(cron ?? "").trim().split(/\s+/)
  if (fields.length !== 5) return null
  const minute = parseField(fields[0], MINUTES)
  const hour = parseField(fields[1], HOURS)
  const dayOfMonth = parseField(fields[2], DAYS_OF_MONTH)
  const month = parseField(fields[3], MONTHS)
  const dayOfWeek = parseField(fields[4], DAYS_OF_WEEK)
  if (!minute || !hour || !dayOfMonth || !month || !dayOfWeek) return null

  const times = describeTimes(minute, hour)
  const days = describeDays(dayOfMonth, dayOfWeek)
  if (!times || days === null) return null

  const months = covers(month, MONTHS)
    ? ""
    : `${isRange(month.values) ? "from" : "in"} ${namedSet(month.values, MONTH_NAMES, "")}`

  let sentence: string
  if (times.kind === "listed") {
    sentence = days ? `At ${times.text}, ${days}` : `Every day at ${times.text}`
  } else {
    sentence = days ? `${times.text}, ${days}` : times.text
  }
  return months ? `${sentence}, ${months}` : sentence
}
