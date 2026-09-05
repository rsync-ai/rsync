// A line differ for saved-query SQL, written here rather than pulled from npm.
//
// This is the one place in the Explorer where a wrong answer is a safety problem
// rather than a cosmetic one. An admin approving a proposed edit to a scheduled
// query (migration 092) decides yes or no by reading this diff. A diff that
// under-reports — showing a change as smaller or more innocent than it is — gets a
// rewrite approved that nobody actually reviewed. So two rules run through the
// whole file:
//
//   1. Never normalize the input. No trimming, no case folding, no collapsing
//      whitespace, no unifying CRLF. Two texts that differ must produce a diff that
//      differs. Normalizing CRLF away is the tempting one, and it is exactly the bug:
//      the reviewer would be shown "no changes" for a text that is not the same text.
//   2. When the inputs are too large to align, degrade to a coarse but CORRECT diff
//      (whole base removed, whole proposal added) and say so via `exact: false`.
//      Never silently show a partial alignment.
//
// Why no dependency: this repo serves production, and the alternative was adding a
// diff package to it for one panel. The algorithm below is a textbook LCS — the part
// worth reviewing is the size guard and the honesty rules above, not the recurrence.

/** What happened to one line. "context" means present and identical in both texts. */
export type DiffOp = "context" | "added" | "removed"

export interface DiffLine {
  op: DiffOp
  text: string
  /** 1-based line number in the base text. null for an added line. */
  baseLine: number | null
  /** 1-based line number in the proposed text. null for a removed line. */
  nextLine: number | null
}

export interface LineDiff {
  /** Every line of both texts, in reading order. */
  lines: DiffLine[]
  added: number
  removed: number
  /**
   * True when `lines` is a minimal line-by-line alignment.
   *
   * False when the inputs exceeded the alignment budget and `lines` is the whole
   * base removed followed by the whole proposal added. That is still a correct
   * description of the change — it just isn't a useful one, and a caller showing it
   * to a reviewer must say so rather than let it read as "every line changed".
   */
  exact: boolean
}

// The LCS table is (n+1)·(m+1) cells of Uint32. 4M cells is 16MB and fills in tens
// of milliseconds; past that a browser tab servicing a review panel starts to stall.
// Saved SQL is capped at 256KB server-side (savedQueryMaxSQLBytes), so a pathological
// pair of inputs can reach five figures of lines on each side and this cap is
// reachable in principle — hence a real fallback below rather than an assertion.
const LCS_MAX_CELLS = 4_000_000

/**
 * Align two SQL texts line by line.
 *
 * Splits on "\n" only, and compares the resulting strings exactly — a line that
 * differs solely by a trailing "\r" is a changed line here, because it is one.
 */
export function diffLines(base: string, next: string): LineDiff {
  const a = base.split("\n")
  const b = next.split("\n")

  // Equal prefixes and suffixes are emitted as context without entering the table.
  // This is not just an optimization: a real edit touches a few lines of a long
  // query, so trimming is what keeps the aligned region small enough to be exact.
  let head = 0
  while (head < a.length && head < b.length && a[head] === b[head]) head++

  let tail = 0
  while (
    tail < a.length - head &&
    tail < b.length - head &&
    a[a.length - 1 - tail] === b[b.length - 1 - tail]
  ) {
    tail++
  }

  const aMid = a.slice(head, a.length - tail)
  const bMid = b.slice(head, b.length - tail)

  const lines: DiffLine[] = []
  for (let i = 0; i < head; i++) {
    lines.push({ op: "context", text: a[i], baseLine: i + 1, nextLine: i + 1 })
  }

  const middle = alignMiddle(aMid, bMid, head)
  lines.push(...middle.lines)

  for (let i = 0; i < tail; i++) {
    const baseIdx = a.length - tail + i
    const nextIdx = b.length - tail + i
    lines.push({
      op: "context",
      text: a[baseIdx],
      baseLine: baseIdx + 1,
      nextLine: nextIdx + 1,
    })
  }

  let added = 0
  let removed = 0
  for (const line of lines) {
    if (line.op === "added") added++
    else if (line.op === "removed") removed++
  }

  return { lines, added, removed, exact: middle.exact }
}

/**
 * Align the region left after common prefix/suffix removal. `offset` is how many
 * lines were trimmed from the front, so emitted line numbers stay absolute.
 */
function alignMiddle(
  a: string[],
  b: string[],
  offset: number,
): { lines: DiffLine[]; exact: boolean } {
  const n = a.length
  const m = b.length
  const lines: DiffLine[] = []

  // One side empty: pure insertion or pure deletion, no table needed. Also the
  // n === 0 && m === 0 case, where an untouched query produces no middle at all.
  if (n === 0 || m === 0) {
    for (let i = 0; i < n; i++) {
      lines.push({ op: "removed", text: a[i], baseLine: offset + i + 1, nextLine: null })
    }
    for (let j = 0; j < m; j++) {
      lines.push({ op: "added", text: b[j], baseLine: null, nextLine: offset + j + 1 })
    }
    return { lines, exact: true }
  }

  if ((n + 1) * (m + 1) > LCS_MAX_CELLS) {
    // Too large to align. Emit the honest coarse answer and let the caller tell the
    // reader it is coarse. Deliberately NOT a partial alignment of the first N lines:
    // a diff that stops early looks complete and is the failure mode this whole file
    // is arranged to avoid.
    for (let i = 0; i < n; i++) {
      lines.push({ op: "removed", text: a[i], baseLine: offset + i + 1, nextLine: null })
    }
    for (let j = 0; j < m; j++) {
      lines.push({ op: "added", text: b[j], baseLine: null, nextLine: offset + j + 1 })
    }
    return { lines, exact: false }
  }

  // dp[i][j] = length of the LCS of a[i..] and b[j..]. The suffix formulation (rather
  // than the usual prefix one) means the recovery walk below runs forward from 0,0 and
  // emits lines already in reading order — no build-then-reverse step to get wrong.
  const width = m + 1
  const dp = new Uint32Array((n + 1) * width)
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i * width + j] =
        a[i] === b[j]
          ? dp[(i + 1) * width + (j + 1)] + 1
          : Math.max(dp[(i + 1) * width + j], dp[i * width + (j + 1)])
    }
  }

  let i = 0
  let j = 0
  while (i < n || j < m) {
    if (i < n && j < m && a[i] === b[j]) {
      lines.push({
        op: "context",
        text: a[i],
        baseLine: offset + i + 1,
        nextLine: offset + j + 1,
      })
      i++
      j++
    } else if (i < n && (j === m || dp[(i + 1) * width + j] >= dp[i * width + (j + 1)])) {
      // Ties favour taking from the base first, so a replaced line shows as the
      // removal above its replacement — the order every diff tool has trained
      // people to read.
      lines.push({ op: "removed", text: a[i], baseLine: offset + i + 1, nextLine: null })
      i++
    } else {
      lines.push({ op: "added", text: b[j], baseLine: null, nextLine: offset + j + 1 })
      j++
    }
  }

  return { lines, exact: true }
}

export interface DiffHunk {
  lines: DiffLine[]
  /** 1-based start and length in the base text, for a "@@ -baseStart,baseCount" header. */
  baseStart: number
  baseCount: number
  /** 1-based start and length in the proposed text. */
  nextStart: number
  nextCount: number
}

/**
 * Group a diff into unified-diff hunks: runs of changed lines with `context` lines
 * of unchanged text around them, merged where they overlap.
 *
 * Hunks are what makes a long query reviewable — but collapsing is also the step
 * where a reviewer can be misled by omission, so an empty result means exactly one
 * thing: the two texts were identical. A caller must not render "no changes" from an
 * empty hunk list without having checked that, because `diffLines` on two different
 * texts always yields at least one changed line.
 */
export function toHunks(diff: LineDiff, context = 3): DiffHunk[] {
  const { lines } = diff
  const changed: number[] = []
  for (let k = 0; k < lines.length; k++) {
    if (lines[k].op !== "context") changed.push(k)
  }
  if (changed.length === 0) return []

  // Merge the context windows around each changed line. Adjacent windows are joined
  // too (gap of one unchanged line): emitting two hunks separated by a single line of
  // context is noisier to read than one hunk containing it.
  const ranges: Array<[number, number]> = []
  for (const k of changed) {
    const start = Math.max(0, k - context)
    const end = Math.min(lines.length - 1, k + context)
    const last = ranges[ranges.length - 1]
    if (last && start <= last[1] + 1) last[1] = Math.max(last[1], end)
    else ranges.push([start, end])
  }

  // Running counts of how much of each text precedes index k, so a hunk that opens
  // on an added line (no baseLine of its own) still reports where it sits in the base.
  const basesBefore = new Uint32Array(lines.length + 1)
  const nextsBefore = new Uint32Array(lines.length + 1)
  for (let k = 0; k < lines.length; k++) {
    basesBefore[k + 1] = basesBefore[k] + (lines[k].op === "added" ? 0 : 1)
    nextsBefore[k + 1] = nextsBefore[k] + (lines[k].op === "removed" ? 0 : 1)
  }

  return ranges.map(([start, end]) => ({
    lines: lines.slice(start, end + 1),
    baseStart: basesBefore[start] + 1,
    baseCount: basesBefore[end + 1] - basesBefore[start],
    nextStart: nextsBefore[start] + 1,
    nextCount: nextsBefore[end + 1] - nextsBefore[start],
  }))
}
