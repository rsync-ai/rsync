import { describe, it, expect } from "vitest"
import {
  buildFindBody,
  cellKind,
  clampLimit,
  DOCUMENT_FIND_DEFAULT_LIMIT,
  DOCUMENT_FIND_MAX_LIMIT,
  formatCell,
  mergeColumns,
  nextPageParams,
  normalizeResult,
} from "./documentSpec"

const base = { connectionId: "conn-1", collection: "orders" }

describe("buildFindBody", () => {
  it("sends only the collection and the default limit when nothing else is typed", () => {
    const b = buildFindBody({ ...base, filter: "  ", projection: "", sort: "null" })
    expect(b).toEqual({ ok: true, body: `{"connection_id":"conn-1","collection":"orders","limit":50}` })
  })

  it("forwards the filter text verbatim so a 64-bit integer keeps its precision", () => {
    const b = buildFindBody({ ...base, filter: `{"n":{"$gte":9007199254740993}}` })
    expect(b.ok && b.body).toContain(`"filter":{"n":{"$gte":9007199254740993}}`)
    // The body is still valid JSON.
    expect(() => JSON.parse(b.ok ? b.body : "")).not.toThrow()
  })

  it("accepts both sort forms and carries paging parameters", () => {
    const obj = buildFindBody({ ...base, sort: `{"total":-1}`, skip: 50 })
    expect(obj.ok && JSON.parse(obj.body)).toMatchObject({ sort: { total: -1 }, skip: 50 })
    const list = buildFindBody({ ...base, sort: `[["_id",1]]`, cursor: "abc" })
    expect(list.ok && JSON.parse(list.body)).toMatchObject({ sort: [["_id", 1]], cursor: "abc" })
  })

  it("names the field that is wrong", () => {
    expect(buildFindBody({ ...base, collection: " " })).toMatchObject({ ok: false, field: "collection" })
    expect(buildFindBody({ ...base, filter: `{"a":` })).toMatchObject({ ok: false, field: "filter" })
    expect(buildFindBody({ ...base, filter: `[1]` })).toMatchObject({ ok: false, field: "filter" })
    expect(buildFindBody({ ...base, projection: `["a"]` })).toMatchObject({ ok: false, field: "projection" })
    expect(buildFindBody({ ...base, sort: `"total"` })).toMatchObject({ ok: false, field: "sort" })
  })

  it("names the database only when there is one", () => {
    const b = buildFindBody({ ...base, database: " shop " })
    expect(b.ok && JSON.parse(b.body)).toEqual({ connection_id: "conn-1", collection: "orders", database: "shop", limit: 50 })
    const none = buildFindBody({ ...base, database: "  " })
    expect(none.ok && JSON.parse(none.body)).not.toHaveProperty("database")
  })

  it("escapes the collection name", () => {
    const b = buildFindBody({ ...base, collection: `we"ird` })
    expect(b.ok && JSON.parse(b.body).collection).toBe(`we"ird`)
  })
})

describe("clampLimit", () => {
  it("defaults and clamps", () => {
    expect(clampLimit(undefined)).toBe(DOCUMENT_FIND_DEFAULT_LIMIT)
    expect(clampLimit(0)).toBe(DOCUMENT_FIND_DEFAULT_LIMIT)
    expect(clampLimit(Number.NaN)).toBe(DOCUMENT_FIND_DEFAULT_LIMIT)
    expect(clampLimit(10_000)).toBe(DOCUMENT_FIND_MAX_LIMIT)
    expect(clampLimit(12.7)).toBe(12)
  })
})

describe("nextPageParams", () => {
  const r = normalizeResult({ documents: [], has_more: true })
  it("uses the cursor in keyset mode and the skip in skip mode", () => {
    expect(nextPageParams({ ...r, paging_mode: "keyset", next_cursor: "c1" })).toEqual({ cursor: "c1" })
    expect(nextPageParams({ ...r, paging_mode: "skip", next_skip: 100 })).toEqual({ skip: 100 })
  })
  it("returns null when there is nothing more to fetch", () => {
    expect(nextPageParams({ ...r, has_more: false, next_cursor: "c1" })).toBeNull()
    // Skip paging past its ceiling: has_more but no next_skip.
    expect(nextPageParams({ ...r, paging_mode: "skip", next_skip: null })).toBeNull()
  })
})

describe("normalizeResult / mergeColumns", () => {
  it("fills every field and puts _id first", () => {
    const r = normalizeResult({
      documents: [{ b: 1, _id: 1 }, "junk", { c: 2 }],
      columns: ["b", "_id"],
      paging_mode: "skip",
      warnings: ["w", 3],
    })
    expect(r.documents).toHaveLength(2)
    expect(r.columns).toEqual(["_id", "b", "c"])
    expect(r.paging_mode).toBe("skip")
    expect(r.warnings).toEqual(["w"])
    expect(r.has_more).toBe(false)
  })
  it("merges without duplicates in first-seen order", () => {
    expect(mergeColumns(["_id", "a"], ["b", "a", "_id"])).toEqual(["_id", "a", "b"])
  })
})

describe("formatCell / cellKind", () => {
  it.each([
    [{ $oid: "65f000000000000000000001" }, `ObjectId("65f000000000000000000001")`, "objectId"],
    [{ $date: "2026-01-01T00:00:00Z" }, "2026-01-01T00:00:00Z", "date"],
    [{ $date: { $numberLong: "-62135596800000" } }, "Date(-62135596800000)", "date"],
    [{ $numberDecimal: "12.50" }, "12.50", "number"],
    [{ $binary: { base64: "AAE=", subType: "00" } }, "Binary(subtype 00)", "special"],
    [{ $regularExpression: { pattern: "^a", options: "i" } }, "/^a/i", "special"],
    [null, "null", "null"],
    [true, "true", "boolean"],
    [[1, "a"], `[1,"a"]`, "array"],
    [{ city: "Pune" }, `{"city":"Pune"}`, "object"],
  ])("%j", (value, text, kind) => {
    expect(formatCell(value)).toBe(text)
    expect(cellKind(value)).toBe(kind)
  })

  it("marks an absent field as missing and truncates long values", () => {
    expect(formatCell(undefined)).toBe("")
    expect(cellKind(undefined)).toBe("missing")
    const long = formatCell("x".repeat(500))
    expect(long.length).toBe(160)
    expect(long.endsWith("…")).toBe(true)
  })

  it("does not unwrap an object that only looks like a wrapper", () => {
    expect(cellKind({ $oid: "x", other: 1 })).toBe("object")
    expect(formatCell({ $oid: "x", other: 1 })).toBe(`{"$oid":"x","other":1}`)
  })

  it("renders wrappers nested in arrays and objects by value", () => {
    expect(formatCell([{ qty: 5, unit_price: { $numberDecimal: "35.10" } }])).toBe(
      `[{"qty":5,"unit_price":35.10}]`,
    )
    expect(
      formatCell({
        ref: { $oid: "65f000000000000000000001" },
        at: { $date: "2026-01-01T00:00:00Z" },
        big: { $numberLong: "9007199254740993" },
        tags: [],
      }),
    ).toBe(`{"ref":ObjectId("65f000000000000000000001"),"at":"2026-01-01T00:00:00Z","big":9007199254740993,"tags":[]}`)
  })
})
