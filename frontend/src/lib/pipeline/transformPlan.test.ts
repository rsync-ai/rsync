import { afterEach, describe, expect, it, vi } from "vitest"
import type { Mock } from "vitest"

import { authFetch } from "@/lib/api/auth-fetch"
import {
  countMasks,
  describeReplace,
  fromDefinitions,
  isPlanDirty,
  loadPipelineTransformPlan,
  planFingerprint,
  savePipelineTransformPlan,
  toDefinitions,
  type PlanRule,
} from "@/lib/pipeline/transformPlan"

vi.mock("@/lib/api/auth-fetch", () => ({ authFetch: vi.fn() }))

const mockFetch = authFetch as Mock
afterEach(() => vi.clearAllMocks())

const UUID_A = "11111111-1111-4111-8111-111111111111"
const UUID_B = "22222222-2222-4222-8222-222222222222"

const rule = (over: Partial<PlanRule> = {}): PlanRule => ({
  id: UUID_A,
  order: 0,
  type: "producer",
  operation: "filter",
  enabled: true,
  config: { column: "amount", operator: ">", value: "100" },
  ...over,
})

describe("toDefinitions", () => {
  it("folds the operation into transform_config, where every reader looks", () => {
    const [d] = toDefinitions([rule()])
    // PipelineTransformsTab's operationOf() reads transform_config.operation;
    // a row saved without it shows up there as a generic "transform".
    expect(d.transform_config).toEqual({ operation: "filter", column: "amount", operator: ">", value: "100" })
    expect(d.transform_type).toBe("producer")
    expect(d.enabled).toBe(true)
  })

  it("drops a client-minted id that is not a UUID, so the server mints one", () => {
    // The column is uuid: a bad id fails the INSERT and rolls the whole
    // transaction back, taking every other row with it.
    expect(toDefinitions([rule({ id: "local-3" })])[0].id).toBe("")
    expect(toDefinitions([rule({ id: UUID_A })])[0].id).toBe(UUID_A)
  })

  it("numbers rows by position, which is what the handler stores anyway", () => {
    const out = toDefinitions([rule({ id: UUID_A }), rule({ id: UUID_B, order: 99 })])
    expect(out.map((d) => d.transform_order)).toEqual([0, 1])
  })
})

describe("fromDefinitions", () => {
  const response = {
    pipeline_id: "p1",
    producer_transforms: [
      { id: UUID_A, transform_type: "producer", transform_order: 0, transform_config: { operation: "filter", column: "amount" }, enabled: true },
    ],
    consumer_transforms: [
      { id: UUID_B, transform_type: "consumer", transform_order: 1, transform_config: { operation: "mask_pii", column: "email" }, enabled: false },
    ],
  }

  it("lifts the operation back out of the config", () => {
    const rules = fromDefinitions(response)
    expect(rules[0].operation).toBe("filter")
    expect(rules[0].config).toEqual({ column: "amount" })
    expect(rules[0].config).not.toHaveProperty("operation")
  })

  it("keeps both lanes and their stored order and enabled flag", () => {
    const rules = fromDefinitions(response)
    expect(rules.map((r) => r.type)).toEqual(["producer", "consumer"])
    expect(rules[1].enabled).toBe(false)
  })

  it("survives an empty or null response", () => {
    expect(fromDefinitions(null)).toEqual([])
    expect(fromDefinitions({ producer_transforms: null, consumer_transforms: null })).toEqual([])
  })

  it("round-trips a rule without changing it", () => {
    const rules = fromDefinitions(response)
    const back = fromDefinitions({ producer_transforms: toDefinitions(rules), consumer_transforms: [] })
    expect(back.map((r) => [r.operation, r.config])).toEqual(rules.map((r) => [r.operation, r.config]))
  })
})

describe("countMasks", () => {
  // "hash" counts. This assertion used to be 2, excluding it — which was the
  // frontend half of the same defect: the Hash Column card writes operation
  // "hash", it folds to mask_pii server-side, and a de-identifying rule the
  // replace warning does not count is one it silently lets you delete.
  it("counts every masking alias, including the builder's hash card", () => {
    expect(
      countMasks([rule({ operation: "mask" }), rule({ operation: "MASK_PII" }), rule({ operation: "hash" })])
    ).toBe(3)
  })

  it("counts nothing else", () => {
    expect(countMasks([rule({ operation: "filter" }), rule({ operation: "hash_join" })])).toBe(0)
  })
})

describe("describeReplace warns about a dropped hash rule", () => {
  it("names the column and the consequence", () => {
    const existing = [rule({ id: UUID_B, operation: "hash", config: { column: "email" } })]
    const s = describeReplace(existing, [rule({ id: UUID_A })])
    expect(s).toContain("email")
    expect(s).toContain("UNMASKED")
  })
})

describe("describeReplace", () => {
  it("says a first save adds rather than replaces", () => {
    expect(describeReplace([], [rule()])).toBe("This pipeline has no transforms yet. Saving writes 1 rule.")
  })

  it("puts the deletion in numbers", () => {
    const s = describeReplace([rule({ id: UUID_A }), rule({ id: UUID_B })], [rule({ id: UUID_A })])
    expect(s).toContain("the 2 existing rules are deleted")
    expect(s).toContain("1 rule is written")
  })

  it("names a masking rule about to disappear, and its consequence", () => {
    const existing = [rule({ id: UUID_B, operation: "mask_pii", config: { column: "email" } })]
    const s = describeReplace(existing, [rule({ id: UUID_A })])
    expect(s).toContain("email")
    expect(s).toContain("UNMASKED")
  })

  it("says a masking rule is safe when it is still in the plan", () => {
    const mask = rule({ id: UUID_B, operation: "mask_pii", config: { column: "email" } })
    const s = describeReplace([mask], [mask])
    expect(s).toContain("will be kept")
    expect(s).not.toContain("UNMASKED")
  })
})

describe("loadPipelineTransformPlan", () => {
  it("reads the pipeline's stored plan", async () => {
    mockFetch.mockResolvedValue({
      ok: true,
      status: 200,
      json: async () => ({
        producer_transforms: [
          { id: UUID_A, transform_type: "producer", transform_order: 0, transform_config: { operation: "filter" }, enabled: true },
        ],
      }),
    })
    const out = await loadPipelineTransformPlan("p1")

    expect(mockFetch.mock.calls[0][0]).toBe("/api/v1/transforms/pipeline/p1")
    expect(out.ok && out.data[0].operation).toBe("filter")
  })

  it("explains a 404 as a workspace miss, not as an empty plan", async () => {
    // Returning [] here would render as "this pipeline has no transforms",
    // and the next save would then delete rows the operator never saw.
    mockFetch.mockResolvedValue({ ok: false, status: 404, json: async () => ({}) })
    const out = await loadPipelineTransformPlan("p1")
    expect(out).toEqual({ ok: false, error: "Pipeline not found in this workspace." })
  })
})

describe("savePipelineTransformPlan", () => {
  it("posts the whole plan under `transforms`", async () => {
    mockFetch.mockResolvedValue({ ok: true, status: 200, json: async () => ({ message: "Transforms saved", count: 2 }) })
    const out = await savePipelineTransformPlan("p1", [rule({ id: UUID_A }), rule({ id: UUID_B })])

    expect(mockFetch.mock.calls[0][1].method).toBe("POST")
    const body = JSON.parse(mockFetch.mock.calls[0][1].body)
    expect(body.transforms).toHaveLength(2)
    expect(out).toEqual({ ok: true, data: { count: 2 } })
  })

  it("reports a refusal instead of claiming a save", async () => {
    mockFetch.mockResolvedValue({ ok: false, status: 403, json: async () => ({ error: "forbidden" }) })
    expect(await savePipelineTransformPlan("p1", [rule()])).toEqual({ ok: false, error: "forbidden" })
  })

  it("does not claim a save when the API is unreachable", async () => {
    mockFetch.mockRejectedValue(new Error("network"))
    const out = await savePipelineTransformPlan("p1", [rule()])
    expect(out).toEqual({ ok: false, error: "The API is unreachable — nothing was saved." })
  })
})

/**
 * The builder held its plan in React state with nothing watching it, so a
 * reload, a back button, or picking another pipeline discarded everything built
 * since the last save without a word.
 */
describe("planFingerprint / isPlanDirty", () => {
  it("does not call a freshly loaded plan dirty", () => {
    // Non-zero control: a false positive here puts a discard prompt in front of
    // an operator who changed nothing, and they learn to click through it.
    const loaded = [rule({ id: UUID_A }), rule({ id: UUID_B, operation: "select_columns" })]
    expect(isPlanDirty(planFingerprint(loaded), loaded)).toBe(false)
  })

  it("ignores the ids the server mints on save", () => {
    // Re-reading after a save returns the same plan with different row ids. If
    // that read as a change, the page would claim unsaved work forever.
    const before = [rule({ id: UUID_A })]
    const afterSave = [rule({ id: UUID_B })]
    expect(isPlanDirty(planFingerprint(before), afterSave)).toBe(false)
  })

  it("ignores the order config keys happen to be written in", () => {
    const a = [rule({ config: { column: "amount", operator: ">", value: "100" } })]
    const b = [rule({ config: { value: "100", operator: ">", column: "amount" } })]
    expect(isPlanDirty(planFingerprint(a), b)).toBe(false)
  })

  it("sees an added rule", () => {
    const saved = planFingerprint([rule()])
    expect(isPlanDirty(saved, [rule(), rule({ id: UUID_B, operation: "mask_pii" })])).toBe(true)
  })

  it("sees a removed rule", () => {
    const saved = planFingerprint([rule(), rule({ id: UUID_B })])
    expect(isPlanDirty(saved, [rule()])).toBe(true)
  })

  it("sees an edited config", () => {
    const saved = planFingerprint([rule()])
    expect(isPlanDirty(saved, [rule({ config: { column: "amount", operator: ">", value: "500" } })])).toBe(true)
  })

  it("sees a toggled rule", () => {
    const saved = planFingerprint([rule({ enabled: true })])
    expect(isPlanDirty(saved, [rule({ enabled: false })])).toBe(true)
  })

  it("sees a reorder", () => {
    // The save writes position, so a drag is a real change even though the set
    // of rules is identical.
    const a = rule({ id: UUID_A, operation: "filter" })
    const b = rule({ id: UUID_B, operation: "select_columns" })
    expect(isPlanDirty(planFingerprint([a, b]), [b, a])).toBe(true)
  })

  it("treats an untouched empty scratchpad as clean, and a built one as dirty", () => {
    expect(isPlanDirty(planFingerprint([]), [])).toBe(false)
    expect(isPlanDirty(planFingerprint([]), [rule()])).toBe(true)
  })
})
