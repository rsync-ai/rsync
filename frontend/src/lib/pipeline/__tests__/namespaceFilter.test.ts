import { describe, it, expect } from "vitest"
import fs from "node:fs"
import path from "node:path"
import { fileURLToPath } from "node:url"
import {
  applyNamespaceFilter,
  NAMESPACE_NO_MATCH_WARNING,
  NamespaceFilterError,
  parseNamespaceFilter,
} from "../namespaceFilter"

// The cases the Python connector filter and the Go orchestrator filter are
// pinned to: the preview must keep exactly what the server will keep.
const VECTORS = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../../../../shared/mcp-connectors/public/namespace_filter_vectors.json",
)

type Case = {
  name: string
  config: Record<string, unknown>
  expect_error?: boolean
  expect_mode?: string
  expect_patterns?: string[]
  candidates?: string[]
  system_names?: string[]
  expect_kept?: string[]
  expect_warning?: boolean
}

const { cases } = JSON.parse(fs.readFileSync(VECTORS, "utf8")) as { cases: Case[] }

describe("namespace filter matches the shared vectors", () => {
  it("loads them", () => {
    expect(cases.length).toBeGreaterThanOrEqual(28)
    expect(cases.filter((c) => c.expect_error).length).toBeGreaterThan(0)
  })

  for (const c of cases) {
    it(c.name, () => {
      if (c.expect_error) {
        expect(() => parseNamespaceFilter(c.config)).toThrow(NamespaceFilterError)
        return
      }
      const f = parseNamespaceFilter(c.config)
      expect(f.mode).toBe(c.expect_mode)
      expect(f.patterns).toEqual(c.expect_patterns)
      const candidates = c.candidates ?? []
      const got = applyNamespaceFilter(candidates, f, c.system_names ?? [])
      expect(got.kept).toEqual(c.expect_kept)
      expect(got.excluded).toEqual(candidates.filter((n) => !got.kept.includes(n)))
      expect(got.warning).toBe(c.expect_warning ? NAMESPACE_NO_MATCH_WARNING : "")
    })
  }
})

describe("parseNamespaceFilter", () => {
  it("refuses a non-string value instead of reading it as all", () => {
    expect(() => parseNamespaceFilter({ namespace_filter_mode: 1 })).toThrow(NamespaceFilterError)
    expect(() => parseNamespaceFilter({ namespace_filter_patterns: ["a"] })).toThrow(NamespaceFilterError)
  })

  it("reads a missing config as all", () => {
    expect(parseNamespaceFilter(undefined)).toEqual({ mode: "all", patterns: [] })
  })
})
