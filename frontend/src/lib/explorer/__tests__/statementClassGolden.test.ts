import { readFileSync } from "node:fs"
import { resolve } from "node:path"
import { describe, it, expect } from "vitest"
import { classifyExplorerStatement, type ExplorerStmtClass } from "../statementClass"

/**
 * Pins the UI classifier to the fixture the api-gateway's
 * validators.ClassifyStatementSQL is also pinned to
 * (statement_class_golden_test.go). The UI gates Run, the role message and the
 * destructive-confirm dialog on this class before the request is sent, so a
 * disagreement means the UI refuses statements the server would run, or offers
 * ones it will refuse.
 */
type GoldenCase = { sql: string; class: string }

const goldenPath = resolve(__dirname, "../../../../../shared/explorer_statement_class_golden.json")
const golden: GoldenCase[] = JSON.parse(readFileSync(goldenPath, "utf8"))

// The server calls a DML write "dml_write"; the UI calls it "write".
const toUiClass = (c: string): ExplorerStmtClass => (c === "dml_write" ? "write" : (c as ExplorerStmtClass))

describe("classifyExplorerStatement matches the server's shared golden", () => {
  it("golden fixture is not empty", () => {
    expect(golden.length).toBeGreaterThan(0)
  })

  it.each(golden.map((g) => [g.sql, toUiClass(g.class)] as const))("%j → %s", (sql, want) => {
    expect(classifyExplorerStatement(sql)).toBe(want)
  })
})
