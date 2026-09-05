import { describe, expect, it } from "vitest"

import { diffLines, toHunks, type DiffHunk, type LineDiff } from "../sqlDiff"

// The differ feeds the approval panel for scheduled-query edits, where an admin says
// yes or no based on what it renders. So these tests are mostly not about producing a
// *pretty* diff — they are about the two ways a diff can lie to a reviewer:
//
//   dropping a line   → the reviewer approves a change they were never shown
//   inventing a line  → the reviewer rejects over something that is not there
//
// expectLossless below rules out both at once, and every meaningful case runs through
// it. The minimality assertions are the quality bar on top of that floor.

/**
 * The core invariant: the non-added lines must reconstruct the base exactly, and the
 * non-removed lines must reconstruct the proposal exactly. Any dropped, duplicated,
 * reordered, or invented line breaks one of the two.
 */
function expectLossless(diff: LineDiff, base: string, next: string) {
  const rebuiltBase = diff.lines
    .filter((l) => l.op !== "added")
    .map((l) => l.text)
    .join("\n")
  const rebuiltNext = diff.lines
    .filter((l) => l.op !== "removed")
    .map((l) => l.text)
    .join("\n")
  expect(rebuiltBase).toBe(base)
  expect(rebuiltNext).toBe(next)
}

describe("diffLines", () => {
  it("reports no change for identical text, and no hunks to render", () => {
    const sql = "SELECT id, name\nFROM revenue\nWHERE region = 'apac'"
    const diff = diffLines(sql, sql)

    expect(diff.added).toBe(0)
    expect(diff.removed).toBe(0)
    expect(diff.exact).toBe(true)
    expect(diff.lines.every((l) => l.op === "context")).toBe(true)
    expect(toHunks(diff)).toEqual([])
    expectLossless(diff, sql, sql)
  })

  it("keeps a one-line edit to a one-line edit", () => {
    // The property that makes the panel usable at all: touching the WHERE clause of a
    // long query must not render as "the whole query changed".
    const base = ["SELECT id,", "  amount,", "  region", "FROM revenue", "WHERE region = 'apac'", "ORDER BY amount"].join("\n")
    const next = ["SELECT id,", "  amount,", "  region", "FROM revenue", "WHERE region = 'emea'", "ORDER BY amount"].join("\n")

    const diff = diffLines(base, next)

    expect(diff.removed).toBe(1)
    expect(diff.added).toBe(1)
    expect(diff.exact).toBe(true)
    expectLossless(diff, base, next)

    const removed = diff.lines.find((l) => l.op === "removed")
    const added = diff.lines.find((l) => l.op === "added")
    expect(removed?.text).toBe("WHERE region = 'apac'")
    expect(added?.text).toBe("WHERE region = 'emea'")
  })

  it("numbers lines against the original texts, not the aligned region", () => {
    // Common prefix and suffix are trimmed before alignment. If the offset were lost,
    // every line number after the trim would be wrong — and a reviewer navigating by
    // line number would be looking at the wrong line of a long query.
    const base = ["a", "b", "OLD", "d", "e"].join("\n")
    const next = ["a", "b", "NEW", "d", "e"].join("\n")

    const diff = diffLines(base, next)

    expect(diff.lines.find((l) => l.op === "removed")).toMatchObject({
      text: "OLD",
      baseLine: 3,
      nextLine: null,
    })
    expect(diff.lines.find((l) => l.op === "added")).toMatchObject({
      text: "NEW",
      baseLine: null,
      nextLine: 3,
    })
    // The trailing context must carry both numbers, and they must be the real ones.
    expect(diff.lines[diff.lines.length - 1]).toMatchObject({
      op: "context",
      text: "e",
      baseLine: 5,
      nextLine: 5,
    })
    expectLossless(diff, base, next)
  })

  it("shows a replaced line as the removal above its replacement", () => {
    const diff = diffLines("x\nOLD\ny", "x\nNEW\ny")
    const ops = diff.lines.map((l) => l.op)
    expect(ops).toEqual(["context", "removed", "added", "context"])
  })

  it("handles a pure insertion without marking untouched lines as changed", () => {
    const base = ["SELECT 1", "FROM t"].join("\n")
    const next = ["SELECT 1", "  , 2", "FROM t"].join("\n")

    const diff = diffLines(base, next)

    expect(diff.added).toBe(1)
    expect(diff.removed).toBe(0)
    expectLossless(diff, base, next)
  })

  it("handles a pure deletion", () => {
    const base = ["SELECT 1", "  , 2", "FROM t"].join("\n")
    const next = ["SELECT 1", "FROM t"].join("\n")

    const diff = diffLines(base, next)

    expect(diff.removed).toBe(1)
    expect(diff.added).toBe(0)
    expectLossless(diff, base, next)
  })

  it("treats a cleared query as every line removed", () => {
    const base = "SELECT 1\nFROM t"
    const diff = diffLines(base, "")

    expect(diff.removed).toBe(2)
    expect(diff.added).toBe(1) // the single empty line "" that is the new text
    expect(diff.exact).toBe(true)
    expectLossless(diff, base, "")
  })

  it("does NOT normalize CRLF away", () => {
    // The tempting normalization, and the one that would make the differ lie. These
    // two texts are not the same text; a reviewer shown "no changes" for a proposal
    // that rewrote every line ending has been told something false.
    const lf = "SELECT 1\nFROM t"
    const crlf = "SELECT 1\r\nFROM t"

    const diff = diffLines(lf, crlf)

    expect(diff.added + diff.removed).toBeGreaterThan(0)
    expectLossless(diff, lf, crlf)
  })

  it("does not hide a changed trailing newline", () => {
    const diff = diffLines("SELECT 1", "SELECT 1\n")
    expect(diff.added + diff.removed).toBeGreaterThan(0)
    expectLossless(diff, "SELECT 1", "SELECT 1\n")
  })

  it("preserves leading whitespace, which is the whole content of an indent change", () => {
    const base = "SELECT 1\nFROM t"
    const next = "SELECT 1\n    FROM t"
    const diff = diffLines(base, next)

    expect(diff.lines.find((l) => l.op === "added")?.text).toBe("    FROM t")
    expectLossless(diff, base, next)
  })

  it("falls back to a coarse but complete diff past the alignment budget", () => {
    // 2001 unrelated lines a side puts the LCS table over its 4M-cell cap, with no
    // common prefix or suffix to trim it back under. The fallback must stay lossless:
    // "we could not align this" is an acceptable answer, "here is part of it" is not.
    const base = Array.from({ length: 2001 }, (_, i) => `base line ${i}`).join("\n")
    const next = Array.from({ length: 2001 }, (_, i) => `next line ${i}`).join("\n")

    const diff = diffLines(base, next)

    expect(diff.exact).toBe(false)
    expect(diff.removed).toBe(2001)
    expect(diff.added).toBe(2001)
    expectLossless(diff, base, next)
  })

  it("stays exact for a large query whose edit is small", () => {
    // The realistic large case. Trimming the shared prefix and suffix must bring even
    // a 5000-line query back under the cap, so the common path never degrades.
    const lines = Array.from({ length: 5000 }, (_, i) => `  col_${i},`)
    const base = lines.join("\n")
    const changed = [...lines]
    changed[2500] = "  col_2500_renamed,"
    const next = changed.join("\n")

    const diff = diffLines(base, next)

    expect(diff.exact).toBe(true)
    expect(diff.removed).toBe(1)
    expect(diff.added).toBe(1)
    expectLossless(diff, base, next)
  })
})

/**
 * A hunk header must agree with the lines under it. Whenever a hunk opens on a line
 * that carries its own number, the counted start has to match it — which is the check
 * that catches a start computed from the wrong space (the diff's own index) rather
 * than from the base and proposal texts.
 */
function expectHeadersAgreeWithLines(hunks: DiffHunk[]) {
  for (const h of hunks) {
    const first = h.lines[0]
    if (first.baseLine !== null) expect(h.baseStart).toBe(first.baseLine)
    if (first.nextLine !== null) expect(h.nextStart).toBe(first.nextLine)
    expect(h.baseCount).toBe(h.lines.filter((l) => l.op !== "added").length)
    expect(h.nextCount).toBe(h.lines.filter((l) => l.op !== "removed").length)
  }
}

describe("toHunks", () => {
  it("collapses untouched bulk to context around each change", () => {
    const lines = Array.from({ length: 60 }, (_, i) => `line ${i}`)
    const base = lines.join("\n")
    const changed = [...lines]
    changed[30] = "line 30 edited"
    const next = changed.join("\n")

    const hunks = toHunks(diffLines(base, next), 3)

    expect(hunks).toHaveLength(1)
    // 3 lines of context each side, plus the removal and the addition.
    expect(hunks[0].lines).toHaveLength(8)
    expect(hunks[0].baseStart).toBe(28)
    expect(hunks[0].nextStart).toBe(28)
    expect(hunks[0].baseCount).toBe(7)
    expect(hunks[0].nextCount).toBe(7)
    expectHeadersAgreeWithLines(hunks)
  })

  it("separates changes that are far apart and merges ones that are close", () => {
    const lines = Array.from({ length: 60 }, (_, i) => `line ${i}`)

    const far = [...lines]
    far[5] = "line 5 edited"
    far[50] = "line 50 edited"
    expect(toHunks(diffLines(lines.join("\n"), far.join("\n")), 3)).toHaveLength(2)

    const near = [...lines]
    near[20] = "line 20 edited"
    near[23] = "line 23 edited"
    expect(toHunks(diffLines(lines.join("\n"), near.join("\n")), 3)).toHaveLength(1)
  })

  it("reports where a hunk sits in the base even when it opens on an added line", () => {
    // An insertion at the very top has no baseLine to read a start from, so the start
    // has to be counted rather than looked up. Getting this wrong puts the hunk header
    // at line 0 or at the wrong offset.
    const base = ["a", "b", "c"].join("\n")
    const next = ["inserted", "a", "b", "c"].join("\n")

    const hunks = toHunks(diffLines(base, next), 1)

    expect(hunks).toHaveLength(1)
    expect(hunks[0].lines[0]).toMatchObject({ op: "added", text: "inserted" })
    expect(hunks[0].baseStart).toBe(1)
    expect(hunks[0].nextStart).toBe(1)
    expectHeadersAgreeWithLines(hunks)
  })

  it("keeps a later hunk's header in base coordinates after an earlier insertion", () => {
    // The case where the base position and the diff's own line index stop agreeing:
    // an insertion near the top shifts everything below it in the proposal but not in
    // the base. A header computed from the diff index reads correctly for the first
    // hunk and is off by the insertion count for every hunk after it — so a reviewer
    // jumping to "line 39" of the original lands on the wrong line.
    const lines = Array.from({ length: 60 }, (_, i) => `line ${i}`)
    const next = [...lines]
    next.splice(5, 0, "inserted")
    next[41] = "line 40 edited" // index 41 in `next` is base line 41 after the splice

    const diff = diffLines(lines.join("\n"), next.join("\n"))
    const hunks = toHunks(diff, 3)

    expect(hunks).toHaveLength(2)
    expectHeadersAgreeWithLines(hunks)

    // Base and proposal have drifted apart by the one inserted line, and each header
    // must be right in its own text rather than both reporting the same number.
    expect(hunks[1].baseStart).toBe(38)
    expect(hunks[1].nextStart).toBe(39)
    expect(hunks[1].baseCount).toBe(7)
    expect(hunks[1].nextCount).toBe(7)
  })

  it("covers every changed line it was given", () => {
    // The collapse step is the other place a change can go missing. Whatever the
    // grouping does, no changed line may be left out of every hunk.
    const lines = Array.from({ length: 40 }, (_, i) => `line ${i}`)
    const next = [...lines]
    for (const i of [0, 7, 8, 25, 39]) next[i] = `line ${i} edited`

    const diff = diffLines(lines.join("\n"), next.join("\n"))
    const hunks = toHunks(diff, 2)
    expectHeadersAgreeWithLines(hunks)

    const inHunks = hunks.flatMap((h) => h.lines).filter((l) => l.op !== "context").length
    expect(inHunks).toBe(diff.added + diff.removed)
  })
})
