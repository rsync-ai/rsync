import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"

import { authFetch } from "@/lib/api/auth-fetch"
import {
  deIdentifyWarning,
  deleteTransform,
  idsOf,
  isDeIdentifying,
  partialFailureMessage,
  setTransformEnabled,
  transformUrl,
} from "@/lib/pipeline/transformMutations"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

const mockFetch = authFetch as Mock
afterEach(() => vi.clearAllMocks())

const ok = () => ({ ok: true, status: 200, json: async () => ({ message: "Transform updated" }) })
const fail = (status: number, body: unknown) => ({ ok: false, status, json: async () => body })

describe("idsOf", () => {
  it("returns both lanes in a stable order", () => {
    expect(idsOf({ batch: "b1", cdc: "c1" })).toEqual(["b1", "c1"])
  })

  it("drops the lane a pipeline does not have", () => {
    expect(idsOf({ cdc: "c1" })).toEqual(["c1"])
    expect(idsOf({})).toEqual([])
  })
})

describe("isDeIdentifying", () => {
  it("matches exactly the aliases the pre-run gate folds", () => {
    // nl_transforms_gate.go isMaskTransformConfig —
    // `case "mask", "mask_pii", "hash":`. Nothing else.
    expect(isDeIdentifying("mask")).toBe(true)
    expect(isDeIdentifying("mask_pii")).toBe(true)
    expect(isDeIdentifying("  MASK_PII ")).toBe(true)
  })

  it("counts the builder's hash card, which the gate now folds", () => {
    // This used to assert false, on the grounds that the gate did not fold
    // `hash` so warning about it would describe a protection that is not there.
    // That was true and is no longer: `hash` normalizes to mask_pii with
    // mask_type=hash (shared/go/transforms normalizeType), the gate matches it,
    // and disabling one really does drop a protection worth warning about.
    expect(isDeIdentifying("hash")).toBe(true)
    expect(isDeIdentifying(" Hash ")).toBe(true)
  })

  it("does not claim other operations de-identify", () => {
    expect(isDeIdentifying("hash_join")).toBe(false)
    expect(isDeIdentifying("filter")).toBe(false)
    expect(isDeIdentifying("")).toBe(false)
  })
})

describe("deIdentifyWarning", () => {
  it("names the consequence and when it happens", () => {
    const s = deIdentifyWarning("disable", ["batch", "cdc"])
    expect(s).toContain("UNMASKED")
    expect(s).toContain("the next run")
    expect(s).toContain("both the batch and the CDC lane")
  })

  it("names only the lane that is actually affected", () => {
    expect(deIdentifyWarning("delete", ["cdc"])).toContain("on the CDC lane")
    expect(deIdentifyWarning("delete", ["cdc"])).not.toContain("batch")
    expect(deIdentifyWarning("disable", ["batch"])).toContain("on the batch lane")
  })

  it("uses the verb of the action taken", () => {
    expect(deIdentifyWarning("delete", ["batch"]).startsWith("Deleting")).toBe(true)
    expect(deIdentifyWarning("disable", ["batch"]).startsWith("Disabling")).toBe(true)
  })
})

describe("setTransformEnabled", () => {
  it("PUTs enabled alone to every id behind the row", async () => {
    mockFetch.mockResolvedValue(ok())
    const out = await setTransformEnabled({ batch: "b1", cdc: "c1" }, false)

    expect(out).toEqual({ ok: true })
    expect(mockFetch).toHaveBeenCalledTimes(2)
    expect(mockFetch.mock.calls[0][0]).toBe(transformUrl("b1"))
    expect(mockFetch.mock.calls[1][0]).toBe(transformUrl("c1"))
    const body = JSON.parse(mockFetch.mock.calls[0][1].body)
    // UpdateTransform writes only non-nil fields; sending anything else here
    // would rewrite transform_config or transform_order as a side effect.
    expect(body).toEqual({ enabled: false })
    expect(mockFetch.mock.calls[0][1].method).toBe("PUT")
  })

  it("reports how far it got when the second lane refuses", async () => {
    mockFetch.mockResolvedValueOnce(ok()).mockResolvedValueOnce(fail(403, { error: "forbidden" }))
    const out = await setTransformEnabled({ batch: "b1", cdc: "c1" }, false)

    expect(out).toEqual({ ok: false, error: "forbidden", applied: 1 })
  })

  it("does not touch the second lane when the first refuses", async () => {
    mockFetch.mockResolvedValueOnce(fail(404, { error: "Transform not found" }))
    const out = await setTransformEnabled({ batch: "b1", cdc: "c1" }, true)

    expect(mockFetch).toHaveBeenCalledTimes(1)
    expect(out).toEqual({ ok: false, error: "Transform not found", applied: 0 })
  })

  it("refuses a row with no id instead of issuing a request", async () => {
    const out = await setTransformEnabled({}, false)
    expect(mockFetch).not.toHaveBeenCalled()
    expect(out).toMatchObject({ ok: false, applied: 0 })
  })
})

describe("deleteTransform", () => {
  it("DELETEs every id behind the row", async () => {
    mockFetch.mockResolvedValue(ok())
    await deleteTransform({ batch: "b1", cdc: "c1" })

    expect(mockFetch.mock.calls.map((c) => [c[0], c[1].method])).toEqual([
      [transformUrl("b1"), "DELETE"],
      [transformUrl("c1"), "DELETE"],
    ])
  })
})

describe("partialFailureMessage", () => {
  it("says the two lanes now disagree", () => {
    const s = partialFailureMessage(1, 2, "forbidden")
    expect(s).toContain("1 of 2 rows changed")
    expect(s).toContain("disagree")
  })

  it("adds nothing when nothing changed or everything changed", () => {
    expect(partialFailureMessage(0, 2, "forbidden")).toBe("forbidden")
    expect(partialFailureMessage(2, 2, "forbidden")).toBe("forbidden")
  })
})
