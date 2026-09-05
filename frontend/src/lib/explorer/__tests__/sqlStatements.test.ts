import { describe, it, expect } from "vitest"
import {
  splitSqlStatements,
  resolveRunTarget,
  spliceRunTarget,
} from "../sqlStatements"
import {
  classifyExplorerStatement,
  canRunExplorerStatement,
} from "../statementClass"

/** Offset of the first character of `needle` in `doc`. Keeps the caret positions in
 *  these tests readable — and, more importantly, correct when the fixture is edited. */
const at = (doc: string, needle: string) => {
  const i = doc.indexOf(needle)
  if (i === -1) throw new Error(`fixture bug: ${JSON.stringify(needle)} not in doc`)
  return i
}

describe("splitSqlStatements", () => {
  it("returns one statement for a bare query, with no terminator", () => {
    expect(splitSqlStatements("SELECT 1")).toEqual([{ text: "SELECT 1", from: 0, to: 8 }])
  })

  it("splits on top-level semicolons", () => {
    const stmts = splitSqlStatements("SELECT 1;\nSELECT 2;")
    expect(stmts.map((s) => s.text)).toEqual(["SELECT 1", "SELECT 2"])
  })

  it("drops empty and comment-only spans", () => {
    // `;;` and a trailing note must not become phantom statements — otherwise the UI
    // would claim "statement 1 of 3" for what the user sees as one query.
    const stmts = splitSqlStatements("SELECT 1;;\n-- just a note\n")
    expect(stmts.map((s) => s.text)).toEqual(["SELECT 1"])
  })

  it("returns an empty list for whitespace and comments only", () => {
    expect(splitSqlStatements("   \n-- nothing\n/* here */\n")).toEqual([])
    expect(splitSqlStatements("")).toEqual([])
  })

  it("ignores a semicolon inside a single-quoted literal", () => {
    const stmts = splitSqlStatements("SELECT 'a;b' AS x; SELECT 2")
    expect(stmts.map((s) => s.text)).toEqual(["SELECT 'a;b' AS x", "SELECT 2"])
  })

  it("treats a doubled quote as a literal quote, not a terminator", () => {
    const stmts = splitSqlStatements("SELECT 'it''s; fine' AS x")
    expect(stmts.map((s) => s.text)).toEqual(["SELECT 'it''s; fine' AS x"])
  })

  it("honours backslash escapes only inside E'…'", () => {
    // In E'…' a backslash escapes the next char, so the `\'` does NOT close the string
    // and the `;` stays inside it.
    const stmts = splitSqlStatements("SELECT E'a\\'; b' AS x; SELECT 2")
    expect(stmts.map((s) => s.text)).toEqual(["SELECT E'a\\'; b' AS x", "SELECT 2"])
  })

  it("does not treat a trailing identifier char + E as an escape-string prefix", () => {
    // `NOTE'…'` is an identifier followed by a literal, not an E-string: the backslash
    // is an ordinary character and the quote closes at the next `'`.
    const stmts = splitSqlStatements("SELECT 1 AS \"x\" FROM t WHERE a = 'p' AND b = 'q'")
    expect(stmts).toHaveLength(1)
  })

  it("ignores a semicolon inside a quoted identifier", () => {
    const stmts = splitSqlStatements('SELECT * FROM "weird;name"; SELECT 2')
    expect(stmts.map((s) => s.text)).toEqual(['SELECT * FROM "weird;name"', "SELECT 2"])
  })

  it("ignores a semicolon inside a backtick identifier", () => {
    const stmts = splitSqlStatements("SELECT * FROM `weird;name`; SELECT 2")
    expect(stmts.map((s) => s.text)).toEqual(["SELECT * FROM `weird;name`", "SELECT 2"])
  })

  it("ignores a semicolon inside a line comment", () => {
    const stmts = splitSqlStatements("SELECT 1 -- a; b\nFROM t")
    expect(stmts.map((s) => s.text)).toEqual(["SELECT 1 -- a; b\nFROM t"])
  })

  it("ignores a semicolon inside a block comment", () => {
    const stmts = splitSqlStatements("SELECT 1 /* a; b */ FROM t")
    expect(stmts).toHaveLength(1)
  })

  it("ignores semicolons inside dollar-quoted bodies", () => {
    const body = "CREATE FUNCTION f() RETURNS int AS $$ BEGIN SELECT 1; RETURN 2; END $$ LANGUAGE plpgsql"
    const stmts = splitSqlStatements(`${body}; SELECT 9`)
    expect(stmts.map((s) => s.text)).toEqual([body, "SELECT 9"])
  })

  it("ignores semicolons inside tagged dollar quotes", () => {
    const stmts = splitSqlStatements("SELECT $tag$ a; b $tag$ AS x; SELECT 2")
    expect(stmts.map((s) => s.text)).toEqual(["SELECT $tag$ a; b $tag$ AS x", "SELECT 2"])
  })

  it("does not mistake a $1 placeholder for a dollar quote", () => {
    // If `$1` opened a quote, everything after it would be swallowed and the two
    // statements would collapse into one.
    const stmts = splitSqlStatements("SELECT * FROM t WHERE id = $1; SELECT 2")
    expect(stmts.map((s) => s.text)).toEqual(["SELECT * FROM t WHERE id = $1", "SELECT 2"])
  })

  it("reports offsets that slice back to the statement text", () => {
    const doc = "  SELECT 1;\n\n  DELETE FROM t;\n"
    for (const s of splitSqlStatements(doc)) {
      expect(doc.slice(s.from, s.to)).toBe(s.text)
    }
  })
})

describe("resolveRunTarget", () => {
  it("runs the whole buffer when it holds a single statement", () => {
    const t = resolveRunTarget("SELECT 1")
    expect(t).toMatchObject({ sql: "SELECT 1", source: "buffer", index: 1, total: 1, multi: false })
  })

  it("strips the trailing semicolon from a lone statement", () => {
    // The api-gateway's single-statement validator sees the body verbatim; sending a
    // bare `;` back is pointless noise.
    expect(resolveRunTarget("SELECT 1;").sql).toBe("SELECT 1")
  })

  it("runs the statement under the caret when the buffer holds several", () => {
    const doc = "SELECT 1;\nSELECT 2;\nSELECT 3;"
    const t = resolveRunTarget(doc, at(doc, "SELECT 2") + 3, at(doc, "SELECT 2") + 3)
    expect(t).toMatchObject({ sql: "SELECT 2", source: "statement", index: 2, total: 3 })
  })

  it("attributes a caret in the whitespace after a statement to that statement", () => {
    const doc = "SELECT 1;\nSELECT 2;"
    // Caret immediately after the first `;` — the user just finished typing statement 1.
    const t = resolveRunTarget(doc, 9, 9)
    expect(t).toMatchObject({ sql: "SELECT 1", index: 1, total: 2 })
  })

  it("falls back to the first statement for a caret before all of them", () => {
    const doc = "\n\nSELECT 1;\nSELECT 2;"
    expect(resolveRunTarget(doc, 0, 0)).toMatchObject({ sql: "SELECT 1", index: 1 })
  })

  it("prefers a non-empty selection over the caret rule", () => {
    const doc = "SELECT 1;\nSELECT 2;"
    const from = at(doc, "SELECT 2")
    const t = resolveRunTarget(doc, from, from + "SELECT 2".length)
    expect(t).toMatchObject({ sql: "SELECT 2", source: "selection", multi: false })
  })

  it("trims whitespace and comments off the edges of a selection", () => {
    const doc = "SELECT 1;\n\n  SELECT 2;  \n"
    const t = resolveRunTarget(doc, at(doc, "\n\n"), doc.length)
    expect(t.sql).toBe("SELECT 2")
    expect(doc.slice(t.from, t.to)).toBe(t.sql)
  })

  it("ignores a whitespace-only selection and uses the caret", () => {
    const doc = "SELECT 1;\n\n\nSELECT 2;"
    const from = at(doc, "\n\n\n")
    const t = resolveRunTarget(doc, from, from + 3)
    expect(t.source).toBe("statement")
    expect(t.sql).toBe("SELECT 1")
  })

  it("flags a selection spanning several statements as multi", () => {
    const doc = "SELECT 1;\nSELECT 2;"
    const t = resolveRunTarget(doc, 0, doc.length)
    expect(t.multi).toBe(true)
    expect(t.total).toBe(2)
  })

  it("does not flag a lone statement as multi even when fully selected", () => {
    const doc = "SELECT 1;"
    expect(resolveRunTarget(doc, 0, doc.length).multi).toBe(false)
  })

  it("normalises a reversed selection (caret dragged backwards)", () => {
    const doc = "SELECT 1;\nSELECT 2;"
    const from = at(doc, "SELECT 2")
    const t = resolveRunTarget(doc, from + "SELECT 2".length, from)
    expect(t).toMatchObject({ sql: "SELECT 2", source: "selection" })
  })

  it("reports an empty target for an empty or comment-only buffer", () => {
    expect(resolveRunTarget("").sql).toBe("")
    expect(resolveRunTarget("   \n-- nope\n").sql).toBe("")
    expect(resolveRunTarget("   \n-- nope\n").total).toBe(0)
  })

  it("returns offsets that slice back to sql for every source", () => {
    const doc = "SELECT 1;\n  SELECT 2;\n"
    for (const t of [
      resolveRunTarget(doc),
      resolveRunTarget(doc, at(doc, "SELECT 2") + 2, at(doc, "SELECT 2") + 2),
      resolveRunTarget(doc, at(doc, "SELECT 2"), at(doc, "SELECT 2") + 8),
    ]) {
      expect(doc.slice(t.from, t.to)).toBe(t.sql)
    }
  })
})

/**
 * The reason `statementClass` was extracted from the page: the gates and the fetch must
 * read the SAME string. Classifying the buffer while executing one statement is the
 * defect shape documented in `runShortcut.ts` — a declined DROP that still ran.
 */
describe("gate/execute alignment (resolveRunTarget × classifyExplorerStatement)", () => {
  const doc = "SELECT 1;\nDROP TABLE users;"

  it("classifies the DROP as destructive when the caret sits in it", () => {
    const caret = at(doc, "DROP") + 2
    const t = resolveRunTarget(doc, caret, caret)
    expect(t.sql).toBe("DROP TABLE users")
    expect(classifyExplorerStatement(t.sql)).toBe("destructive")
  })

  it("never lets the leading SELECT wave the DROP through", () => {
    // The whole buffer classifies as `read` — the exact trap. Gating on the resolved
    // target instead of the buffer is what closes it.
    expect(classifyExplorerStatement(doc)).toBe("read")
    const caret = at(doc, "DROP") + 2
    expect(classifyExplorerStatement(resolveRunTarget(doc, caret, caret).sql)).toBe("destructive")
  })

  it("never lets the DROP leak into a run of the SELECT", () => {
    const caret = at(doc, "SELECT") + 3
    const t = resolveRunTarget(doc, caret, caret)
    expect(t.sql).toBe("SELECT 1")
    expect(t.sql).not.toContain("DROP")
    expect(classifyExplorerStatement(t.sql)).toBe("read")
  })

  it("gates the role check on the target, not the buffer", () => {
    const caret = at(doc, "DROP") + 2
    const t = resolveRunTarget(doc, caret, caret)
    // An admin may run the SELECT but not the DROP; the buffer would have said yes.
    expect(canRunExplorerStatement("admin", doc)).toBe(true)
    expect(canRunExplorerStatement("admin", t.sql)).toBe(false)
    expect(canRunExplorerStatement("owner", t.sql)).toBe(true)
  })

  it("classifies ALTER … DROP COLUMN under the caret as destructive", () => {
    const d = "SELECT 1;\nALTER TABLE t DROP COLUMN c;"
    const caret = at(d, "ALTER") + 2
    expect(classifyExplorerStatement(resolveRunTarget(d, caret, caret).sql)).toBe("destructive")
  })

  it("keeps a multi-statement selection flagged so the caller can refuse it", () => {
    // The server rejects stacked statements by design; the UI must not send this.
    const t = resolveRunTarget(doc, 0, doc.length)
    expect(t.multi).toBe(true)
  })
})

describe("spliceRunTarget", () => {
  it("replaces only the target statement, leaving the rest of the buffer intact", () => {
    const doc = "SELECT 1;\nSELECT * FROM orders;\nSELECT 3;"
    const caret = at(doc, "SELECT * FROM orders")
    const t = resolveRunTarget(doc, caret, caret)
    const out = spliceRunTarget(doc, t, "SELECT * FROM public.orders LIMIT 100")
    expect(out).toBe("SELECT 1;\nSELECT * FROM public.orders LIMIT 100;\nSELECT 3;")
  })

  it("round-trips when the replacement is unchanged", () => {
    const doc = "SELECT 1;\nSELECT 2;"
    const t = resolveRunTarget(doc, at(doc, "SELECT 2"), at(doc, "SELECT 2"))
    expect(spliceRunTarget(doc, t, t.sql)).toBe(doc)
  })

  it("rewrites a single-statement buffer in place", () => {
    const doc = "SELECT * FROM orders"
    const t = resolveRunTarget(doc)
    expect(spliceRunTarget(doc, t, "SELECT * FROM public.orders")).toBe("SELECT * FROM public.orders")
  })

  it("no-ops when the buffer changed under a stale target", () => {
    // The destructive-confirm dialog can hold a target across an edit. Splicing by raw
    // offsets would then rewrite whatever now occupies that range.
    const doc = "SELECT 1;\nDROP TABLE users;"
    const t = resolveRunTarget(doc, at(doc, "DROP"), at(doc, "DROP"))
    const edited = "SELECT 1;\nSELECT 2;\nDROP TABLE users;"
    expect(spliceRunTarget(edited, t, "DROP TABLE public.users")).toBe(edited)
  })

  it("returns the document unchanged for an out-of-range target", () => {
    const doc = "SELECT 1"
    expect(spliceRunTarget(doc, { ...resolveRunTarget(doc), from: 0, to: 999 }, "X")).toBe(doc)
    expect(spliceRunTarget(doc, { ...resolveRunTarget(doc), from: 5, to: 2 }, "X")).toBe(doc)
  })
})
