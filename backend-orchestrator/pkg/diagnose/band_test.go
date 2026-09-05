package diagnose

import "testing"

// TestLLMConfidenceCapBelowAutoExecuteBand is the core LLM-safety invariant:
// the cap on an LLM-authored diagnosis's confidence MUST sit strictly below the
// auto-execute band, so an LLM verdict can reach HITL (operator approval) but
// can NEVER cross the threshold that would auto-execute a heal. Both values now
// derive from the single source of truth AutoExecuteBand; this test fails
// closed if a future edit ever lets the cap meet or exceed the band.
func TestLLMConfidenceCapBelowAutoExecuteBand(t *testing.T) {
	if llmConfidenceCap >= AutoExecuteBand {
		t.Fatalf("llmConfidenceCap (%v) must be < AutoExecuteBand (%v) — an LLM diagnosis could auto-execute a heal",
			llmConfidenceCap, AutoExecuteBand)
	}
}

// TestAutoExecuteBandValue pins the band value. The heal decision bands are
// calibrated around 0.85; an accidental change should surface as a test diff,
// not silently shift when a heal auto-executes.
func TestAutoExecuteBandValue(t *testing.T) {
	if AutoExecuteBand != 0.85 {
		t.Fatalf("AutoExecuteBand changed to %v — recalibrate heal bands and update this pin deliberately", AutoExecuteBand)
	}
}
