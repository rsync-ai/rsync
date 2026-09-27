import { describe, it, expect } from "vitest"
import { isInternalExplorerTable } from "../internalTables"

describe("isInternalExplorerTable", () => {
  it("flags rsync internal bookkeeping tables (bare names)", () => {
    expect(isInternalExplorerTable("_rsync_cdc_offsets")).toBe(true)
    expect(isInternalExplorerTable("_rsync_pipelines")).toBe(true)
    expect(isInternalExplorerTable("_RSYNC_meta")).toBe(true) // case-insensitive
  })

  it("flags internal tables even when schema-qualified", () => {
    expect(isInternalExplorerTable("public._rsync_cdc_offsets")).toBe(true)
    expect(isInternalExplorerTable("sales._rsync_pipelines")).toBe(true)
  })

  it("does NOT flag ordinary business tables", () => {
    expect(isInternalExplorerTable("orders")).toBe(false)
    expect(isInternalExplorerTable("public.customers")).toBe(false)
    // underscore-prefixed but not rsync-internal — leave the user's table alone
    expect(isInternalExplorerTable("_staging_users")).toBe(false)
  })

  it("is null/undefined safe", () => {
    expect(isInternalExplorerTable(undefined)).toBe(false)
    expect(isInternalExplorerTable(null)).toBe(false)
    expect(isInternalExplorerTable("")).toBe(false)
  })
})
