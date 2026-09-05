import { describe, it, expect } from "vitest"
import { EditorState } from "@codemirror/state"
import { sql, PostgreSQL, MySQL } from "@codemirror/lang-sql"
import {
  CompletionContext,
  type CompletionResult,
  type CompletionSource,
} from "@codemirror/autocomplete"
import { buildSqlCompletionSources, createSchemaCompletionSource } from "../sqlCompletionSources"
import { buildSchemaLookup, type CompletionTable } from "../sqlCompletions"

// The schema-aware source (tables/columns/FK hints) is unit-tested in
// sqlCompletions.test.ts. Here we use a no-op schema source and assert only
// that the dialect KEYWORD source is wired into the override list — the gap
// that shipped in #256, where `autocompletion({ override })` silently dropped
// the language-data keyword completer.
const noSchemaSource: CompletionSource = () => null

/** Run every override source headlessly and collect the option labels. */
function labelsAt(doc: string, dialect = PostgreSQL): string[] {
  const state = EditorState.create({ doc, extensions: [sql({ dialect })] })
  const ctx = new CompletionContext(state, doc.length, true /* explicit */)
  const labels: string[] = []
  for (const src of buildSqlCompletionSources(noSchemaSource, dialect)) {
    const result = src(ctx)
    if (result && !(result instanceof Promise)) {
      for (const opt of (result as CompletionResult).options) {
        labels.push(opt.label.toUpperCase())
      }
    }
  }
  return labels
}

describe("buildSqlCompletionSources", () => {
  it("keeps the schema-aware source first, then the dialect keyword source", () => {
    const sources = buildSqlCompletionSources(noSchemaSource, PostgreSQL)
    expect(sources[0]).toBe(noSchemaSource)
    expect(sources).toHaveLength(2)
  })

  it("completes SQL keywords mid-statement (e.g. WHERE after FROM)", () => {
    expect(labelsAt("SELECT * FROM t WHE")).toContain("WHERE")
  })

  it("completes a keyword at the start of a statement (SELECT)", () => {
    expect(labelsAt("SEL")).toContain("SELECT")
  })

  it("wires keywords for the MySQL dialect too", () => {
    expect(labelsAt("SELECT * FROM t WHE", MySQL)).toContain("WHERE")
  })
})

// ── Schema-aware source + BUG-A regression (schema-qualified prefix) ──────────
const SCHEMA_TABLES: CompletionTable[] = [
  { name: "_rsync_cdc_offsets", schema: "public", columns: [{ name: "id" }, { name: "offset", type: "bigint" }] },
  { name: "orders", schema: "public", columns: [{ name: "id" }, { name: "total", type: "decimal" }] },
  { name: "users", schema: "public", columns: [{ name: "id" }, { name: "email", type: "varchar" }] },
]

/** Collect labels from ALL override sources (schema source + guarded keywords). */
function allLabelsAt(doc: string, dialect = PostgreSQL): string[] {
  const lookup = buildSchemaLookup(SCHEMA_TABLES)
  const schemaSource = createSchemaCompletionSource(lookup, [])
  const state = EditorState.create({ doc, extensions: [sql({ dialect })] })
  const ctx = new CompletionContext(state, doc.length, true)
  const labels: string[] = []
  for (const src of buildSqlCompletionSources(schemaSource, dialect)) {
    const result = src(ctx)
    if (result && !(result instanceof Promise)) {
      for (const opt of (result as CompletionResult).options) labels.push(opt.label)
    }
  }
  return labels
}

describe("createSchemaCompletionSource — schema-qualified completion (BUG-A)", () => {
  it("completes the tables in a schema after `schema.prefix`", () => {
    const lookup = buildSchemaLookup(SCHEMA_TABLES)
    const src = createSchemaCompletionSource(lookup, [])
    const doc = "SELECT * FROM public._rs"
    const state = EditorState.create({ doc, extensions: [sql({ dialect: PostgreSQL })] })
    const res = src(new CompletionContext(state, doc.length, true))
    expect(res).not.toBeNull()
    expect((res as CompletionResult).options.map((o) => o.label)).toContain("_rsync_cdc_offsets")
  })

  it("does NOT leak SQL keyword garbage for `public._rs` (the reported bug)", () => {
    const labels = allLabelsAt("SELECT * FROM public._rs")
    expect(labels).toContain("_rsync_cdc_offsets")
    expect(labels.some((l) => /PRINT_STRICT_PARAMS|PARAMETER_ORDINAL_POSITION|CURRENT_/.test(l))).toBe(
      false,
    )
  })

  it("suppresses keywords after an alias dot, surfacing only columns", () => {
    const labels = allLabelsAt("SELECT * FROM public.orders o WHERE o.")
    expect(labels).toContain("id")
    expect(labels).toContain("total")
    expect(labels).not.toContain("WHERE")
    expect(labels).not.toContain("SELECT")
  })

  it("still completes keywords mid-statement (no over-suppression)", () => {
    expect(allLabelsAt("SELECT * FROM public.orders WHE")).toContain("WHERE")
  })
})
