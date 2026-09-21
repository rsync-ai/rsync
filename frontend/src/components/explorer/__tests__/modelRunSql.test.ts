import { describe, expect, it } from "vitest"
import { NEAR_EDIT_MS, parseSqlHistory, sqlForRun, type SqlHistory } from "@/components/explorer/modelRunSql"

const t = (min: number) => new Date(Date.UTC(2026, 8, 16, 3, min)).toISOString()

// Edited at 03:10 (v1 → v2) and 03:20 (v2 → v3). Each version is the text before its edit.
const HISTORY: SqlHistory = {
  current: { sql_text: "select 3" },
  versions: [
    { version: 2, sql_text: "select 2", created_at: t(20) },
    { version: 1, sql_text: "select 1", created_at: t(10) },
  ],
}

describe("sqlForRun", () => {
  it("gives the text as it was when the run started", () => {
    expect(sqlForRun(HISTORY, t(5))).toEqual({ kind: "at_run", sql: "select 1", editedSince: true, nearEdit: false })
    expect(sqlForRun(HISTORY, t(15))).toEqual({ kind: "at_run", sql: "select 2", editedSince: true, nearEdit: false })
    expect(sqlForRun(HISTORY, t(25))).toEqual({ kind: "at_run", sql: "select 3", editedSince: false, nearEdit: false })
  })

  it("flags a run that started within a minute of an edit, since the read could land either side", () => {
    const start = new Date(Date.parse(t(10)) - NEAR_EDIT_MS / 2).toISOString()
    expect(sqlForRun(HISTORY, start)).toMatchObject({ kind: "at_run", sql: "select 1", nearEdit: true })
  })

  it("says it can't tell once the history no longer reaches back to the run", () => {
    // Only the newest edit is kept: nothing says what the text was before 03:10.
    const pruned: SqlHistory = { current: HISTORY.current, versions: [{ version: 5, sql_text: "select 5", created_at: t(20) }] }
    expect(sqlForRun(pruned, t(5))).toEqual({ kind: "unknown", current: "select 3" })
    // A run after the one edit that is kept is still known.
    expect(sqlForRun(pruned, t(25))).toMatchObject({ kind: "at_run", sql: "select 3" })
  })

  it("falls back to the current text with no start time to place", () => {
    for (const start of [null, undefined, "", "not a time"]) {
      expect(sqlForRun(HISTORY, start)).toEqual({ kind: "current", sql: "select 3" })
    }
  })
})

describe("parseSqlHistory", () => {
  it("reads the versions route and drops rows of the wrong shape", () => {
    expect(
      parseSqlHistory({
        versions: [{ version: 1, sql_text: "select 1", created_at: t(10), name: "x" }, { version: "2" }, null],
        count: 3,
        current: { name: "x", sql_text: "select 2" },
      }),
    ).toEqual({ current: { sql_text: "select 2" }, versions: [{ version: 1, sql_text: "select 1", created_at: t(10), name: "x" }] })
  })

  it("refuses a body that is not the route's", () => {
    for (const body of [null, "x", {}, { versions: [] }, { versions: [], current: {} }, { versions: {}, current: { sql_text: "" } }]) {
      expect(parseSqlHistory(body)).toBeNull()
    }
  })
})
