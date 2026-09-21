import { describe, expect, it } from "vitest"
import {
  describeRunOrigin,
  describeSkipReason,
  describeUpstreamSet,
} from "@/components/explorer/runProvenance"

describe("describeUpstreamSet", () => {
  const two = [
    { id: "p1", name: "orders_sync" },
    { id: "m1", name: "customer_dim" },
  ]

  it("says any of, or all of, never a bare list", () => {
    expect(describeUpstreamSet(two)).toBe("After any of orders_sync, customer_dim runs")
    expect(describeUpstreamSet(two, "any")).toBe("After any of orders_sync, customer_dim runs")
    expect(describeUpstreamSet(two, "all")).toBe("After all of orders_sync, customer_dim run")
  })

  // With one upstream the two policies are the same trigger, so the sentence is too.
  it("names a single upstream the same way under either policy", () => {
    expect(describeUpstreamSet([two[0]], "all")).toBe("After orders_sync runs")
    expect(describeUpstreamSet([two[0]], "any")).toBe("After orders_sync runs")
  })

  it("falls back to the id for a producer the join found no name for", () => {
    expect(describeUpstreamSet([{ id: "p9", name: " " }])).toBe("After p9 runs")
    expect(describeUpstreamSet(undefined)).toBe("After an upstream runs")
  })
})

describe("describeRunOrigin", () => {
  it("passes a clock or manual run through unchanged", () => {
    expect(describeRunOrigin({ trigger_source: "manual" })).toBe("manual")
    expect(describeRunOrigin({ trigger_source: "scheduled" })).toBe("scheduled")
  })

  // Rows written before migration 104 carry no provenance, and must not claim any.
  it("says only triggered for a triggered row with no provenance", () => {
    expect(describeRunOrigin({ trigger_source: "triggered" })).toBe("triggered")
  })

  it("names the upstream, and the depth and coalescing only when they say something", () => {
    expect(
      describeRunOrigin({
        trigger_source: "triggered",
        upstream_kind: "pipeline",
        upstream_name: "orders_sync",
        trigger_depth: 1,
        coalesced_count: 1,
      })
    ).toBe("after orders_sync")
    expect(
      describeRunOrigin({
        trigger_source: "triggered",
        upstream_kind: "model",
        upstream_name: "orders_daily",
        trigger_depth: 2,
        coalesced_count: 3,
      })
    ).toBe("after orders_daily (depth 2, 3 coalesced)")
    expect(
      describeRunOrigin({
        trigger_source: "triggered",
        upstream_kind: "model",
        upstream_name: "orders_daily",
        coalesced_count: 2,
      })
    ).toBe("after orders_daily (2 coalesced)")
  })

  // No name is a deleted producer or someone else's private model: say what kind it was
  // without inventing, or leaking, which one.
  it("says which kind of upstream when there is no name to show", () => {
    expect(describeRunOrigin({ trigger_source: "triggered", upstream_kind: "pipeline" })).toBe(
      "after a pipeline"
    )
    expect(describeRunOrigin({ trigger_source: "triggered", upstream_kind: "model" })).toBe(
      "after a model"
    )
  })
})

describe("describeSkipReason", () => {
  it("is null for a row that is not a skip", () => {
    expect(describeSkipReason({})).toBeNull()
    expect(describeSkipReason({ skip_reason: "" })).toBeNull()
  })

  it("gives each reason the gateway writes its own sentence", () => {
    const reasons = ["upstream_failed", "upstream_skipped", "chain_depth_exceeded", "waiting_on_upstreams"]
    const sentences = reasons.map((skip_reason) => describeSkipReason({ skip_reason }))
    expect(new Set(sentences).size).toBe(reasons.length)
    for (const s of sentences) {
      expect(s).toMatch(/^not rebuilt/)
      // A known reason is translated, never echoed back as its code.
      expect(reasons.some((r) => s!.includes(r))).toBe(false)
    }
  })

  it("shows an unknown reason rather than dropping it", () => {
    expect(describeSkipReason({ skip_reason: "quota_exhausted" })).toBe(
      "not rebuilt: quota_exhausted"
    )
  })
})
