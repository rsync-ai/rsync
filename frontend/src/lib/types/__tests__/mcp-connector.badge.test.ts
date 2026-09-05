import { describe, it, expect } from "vitest"
import {
  getConnectorStatusBadge,
  getConnectorConfidenceBadge,
  getConnectorConfidenceReasons,
} from "../mcp-connector"

// These tests pin the two-orthogonal-axes contract that fixes the badge-masking
// bug: a runtime-tested connector that also carries a QA confidence rating
// (mysql/postgresql/aws-s3 = "Medium") must still surface "Tested" via the
// Status chip — the Confidence chip must never replace/mask it.

describe("getConnectorStatusBadge (lifecycle axis)", () => {
  it("returns Tested when lifecycle has runtime evidence (preview/beta/ga)", () => {
    for (const lc of ["preview", "beta", "ga"]) {
      expect(getConnectorStatusBadge({ status: "active", lifecycle: lc }).label).toBe("Tested")
    }
  })

  it("returns New when generated but never run (lifecycle draft)", () => {
    expect(getConnectorStatusBadge({ status: "active", lifecycle: "draft" }).label).toBe("New")
    expect(getConnectorStatusBadge({}).label).toBe("New")
  })

  it("returns Draft when preflight failed (status/quality_tier draft), even with a lifecycle", () => {
    expect(getConnectorStatusBadge({ status: "draft", lifecycle: "preview" }).label).toBe("Draft")
    expect(getConnectorStatusBadge({ quality_tier: "draft", lifecycle: "ga" }).label).toBe("Draft")
  })

  it("is NOT influenced by confidence — a Medium connector that ran reads Tested", () => {
    // mysql / postgresql ground truth: confidence_level=medium, lifecycle=preview
    const badge = getConnectorStatusBadge({ status: "active", quality_tier: "bronze", lifecycle: "preview" })
    expect(badge.label).toBe("Tested")
  })
})

describe("getConnectorConfidenceBadge (QA-quality axis, nullable)", () => {
  it("maps explicit confidence_level high/medium/low", () => {
    expect(getConnectorConfidenceBadge({ confidence_level: "high" })?.label).toBe("High")
    expect(getConnectorConfidenceBadge({ confidence_level: "medium" })?.label).toBe("Medium")
    expect(getConnectorConfidenceBadge({ confidence_level: "low" })?.label).toBe("Low")
  })

  it("returns null when confidence is unknown (no chip — Status carries the signal)", () => {
    expect(getConnectorConfidenceBadge({ confidence_level: "unknown" })).toBeNull()
  })

  it("returns null for a draft connector (Status chip already says Draft)", () => {
    expect(getConnectorConfidenceBadge({ status: "draft", confidence_level: "medium" })).toBeNull()
  })

  it("falls back to quality_tier only when confidence_level is absent", () => {
    expect(getConnectorConfidenceBadge({ quality_tier: "gold" })?.label).toBe("High")
    expect(getConnectorConfidenceBadge({ quality_tier: "silver" })?.label).toBe("Medium")
    expect(getConnectorConfidenceBadge({ quality_tier: "bronze" })?.label).toBe("Low")
    expect(getConnectorConfidenceBadge({})).toBeNull()
  })
})

describe("two chips together (regression: the masking bug)", () => {
  // The exact ground-truth rows from GET /api/v1/connectors that the user saw.
  const cases: Array<{
    name: string
    in: { confidence_level?: string; quality_tier?: string; status?: string; lifecycle?: string }
    status: string
    confidence: string | null
  }> = [
    { name: "mysql", in: { confidence_level: "medium", quality_tier: "bronze", status: "active", lifecycle: "preview" }, status: "Tested", confidence: "Medium" },
    { name: "postgresql", in: { confidence_level: "medium", quality_tier: "bronze", status: "active", lifecycle: "preview" }, status: "Tested", confidence: "Medium" },
    { name: "aws-s3", in: { confidence_level: "medium", quality_tier: "bronze", status: "active", lifecycle: "draft" }, status: "New", confidence: "Medium" },
    { name: "azure-blob", in: { confidence_level: "unknown", quality_tier: "bronze", status: "active", lifecycle: "draft" }, status: "New", confidence: null },
    { name: "gcs", in: { confidence_level: "unknown", quality_tier: "bronze", status: "active", lifecycle: "draft" }, status: "New", confidence: null },
    { name: "google-sheets", in: { confidence_level: "unknown", status: "active", lifecycle: "draft" }, status: "New", confidence: null },
    // shopify/gsheets AFTER a successful run: Status flips to Tested even though
    // confidence stays unrated — this is the behavior the user expected.
    { name: "google-sheets (tested)", in: { confidence_level: "unknown", status: "active", lifecycle: "preview" }, status: "Tested", confidence: null },
  ]

  for (const c of cases) {
    it(`${c.name} → Status "${c.status}", Confidence ${c.confidence ?? "none"}`, () => {
      const statusBadge = getConnectorStatusBadge(c.in)
      const confidenceBadge = getConnectorConfidenceBadge(c.in)
      expect(statusBadge.label).toBe(c.status)
      expect(confidenceBadge?.label ?? null).toBe(c.confidence)
    })
  }
})

describe("getConnectorConfidenceReasons describes BOTH axes", () => {
  it("includes a Status line and a QA-confidence line for a tested+Medium connector", () => {
    const reasons = getConnectorConfidenceReasons({ confidence_level: "medium", quality_tier: "bronze", status: "active", lifecycle: "preview" })
    expect(reasons.some((r) => r.startsWith("Status: Tested"))).toBe(true)
    expect(reasons.some((r) => r.startsWith("QA confidence: Medium"))).toBe(true)
  })

  it("reports unrated confidence and New status for a fresh connector", () => {
    const reasons = getConnectorConfidenceReasons({ confidence_level: "unknown", status: "active", lifecycle: "draft" })
    expect(reasons.some((r) => r.startsWith("Status: New"))).toBe(true)
    expect(reasons.some((r) => r.startsWith("QA confidence: unrated"))).toBe(true)
  })
})
