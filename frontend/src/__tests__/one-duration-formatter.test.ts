import { describe, expect, it } from "vitest"
import { readFileSync } from "node:fs"
import { relative } from "node:path"
import { SRC, sourceFiles } from "./class-literals"

/**
 * One formatter for "how long did this take".
 *
 * `src/` held EIGHT independent duration formatters, and they disagreed: 179_600 ms
 * read "2m 60s", "2m 59s" and "3m 00s" depending on which panel you opened, and
 * 7_200_000 ms read "120m", "2h", "2h 00m" and "120m 0s". That is why the same bug
 * kept coming back from the GUI: #37 fixed the "2m 60s" rounding in ONE copy, and
 * the next test round found it again somewhere else.
 *
 * This guard fails when a ninth appears. It matches on the *rendering signature* —
 * the template literals that spell a duration out — rather than on the unit
 * constants, which also appear in poll intervals and staleness thresholds and would
 * make the guard noisy enough to be disabled.
 *
 * Adding a duration to a panel: import `formatDuration` / `formatDurationBetween` /
 * `formatDurationOrDash` / `formatStageDuration` from `@/lib/duration`. If you need
 * a rendering none of them produces, change the shared one so every panel moves
 * together — that is the entire point.
 */

/** The one module allowed to spell a duration out, plus the "N ago" family. */
const CANON = "lib/duration.ts"

/**
 * What this guard matches, and what it deliberately does not.
 *
 * It looks for a TWO-UNIT render — `${m}m ${s}s`, `${h}h ${m}m`, `${d}d ${h}h` —
 * plus the sub-minute forms `${x.toFixed(1)}s` and `${x}ms`. Every one of the
 * eight copies this consolidated had at least one of those lines, so matching
 * them catches a new formatter at file granularity.
 *
 * It does NOT match a lone `${n}m` or `${n}s`, for two reasons: `${noun}s` is how
 * this codebase pluralizes, and a single unit is how the OTHER two time facts are
 * written — an age ("5m", from `formatAge`) and a countdown ("in 12m", from
 * `scheduleTime`). Those are different questions with different empty states and
 * their own shared homes; folding them in here would make this guard noisy enough
 * that someone would eventually turn it off, which is how the last one died.
 */
const RENDERS_DURATION = [
  /\}(ms|s|m|h|d) \$\{/,
  /toFixed\(\d\)\}s`/,
  /\}ms`/,
]

/**
 * `${count} items ${x}s` style lines that happen to have a unit-looking suffix.
 * Only the two known shapes; a broad exclusion would hide real copies.
 */
const NOT_A_DURATION = /plural|noun|\bunits\b/i

interface Offender {
  where: string
  line: string
}

function offenders(): Offender[] {
  const found: Offender[] = []
  for (const file of sourceFiles(SRC)) {
    const rel = relative(SRC, file)
    if (rel === CANON) continue
    const lines = readFileSync(file, "utf8").split("\n")
    lines.forEach((line, i) => {
      if (NOT_A_DURATION.test(line)) return
      if (!RENDERS_DURATION.some((re) => re.test(line))) return
      found.push({ where: `${rel}:${i + 1}`, line: line.trim() })
    })
  }
  return found
}

describe("one duration formatter", () => {
  it("is the only place in src/ that spells a duration out", () => {
    const found = offenders()
    expect(
      found.map((o) => `${o.where}  ${o.line}`),
      `A duration is rendered outside @/lib/duration. Import the shared formatter instead — ` +
        `every copy of this code has eventually disagreed with the others.`,
    ).toEqual([])
  })

  it("would catch a copy — the probe is armed", () => {
    // Without a non-zero control, a guard whose regexes silently stopped matching
    // would keep passing forever on an empty result.
    for (const sample of [
      "return `${m}m ${s}s`",
      "return `${h}h ${m}m`",
      "return `${d}d ${h}h`",
      "return `${(v / 1000).toFixed(1)}s`",
      "return `${Math.round(v)}ms`",
    ]) {
      expect(RENDERS_DURATION.some((re) => re.test(sample)), sample).toBe(true)
    }
    for (const innocent of [
      "const POLL_MS = 60_000",
      'const units = typeKnown ? `${noun}s` : "databases"',
      "if (mins < 60) return `${mins}m ago`",
    ]) {
      expect(
        RENDERS_DURATION.some((re) => re.test(innocent)) && !NOT_A_DURATION.test(innocent),
        innocent,
      ).toBe(false)
    }
  })
})
