package handlers

import "testing"

// The confidence chip is a tool-generator QA signal. It must be suppressed for
// non-generated origins (built_in core connectors + hand-curated connectors) so
// the UI only shows it where it is meaningful. Strong QA counts must NOT lift a
// built_in/hand-curated connector above "unknown".
func TestInferConfidenceLevel_suppressedForNonGeneratedOrigins(t *testing.T) {
	strongStructuredQA := map[string]interface{}{
		"tests_passed": float64(6),
		"tests_failed": float64(0),
	}

	cases := []struct {
		name     string
		source   string
		category string
		status   string
		tier     string
		qa       map[string]interface{}
		want     string
	}{
		// Gate: non-generated origins never earn a chip, even with strong QA.
		{"built_in + strong QA", "built_in", "relational_db", "active", "bronze", strongStructuredQA, "unknown"},
		{"hand-curated (hyphen)", "hand-curated", "relational_db", "active", "hand_curated", strongStructuredQA, "unknown"},
		{"hand_curated (underscore)", "hand_curated", "data_warehouse", "active", "hand_curated", strongStructuredQA, "unknown"},
		{"built_in cloud storage + strong QA", "built_in", "cloud_storage", "active", "bronze", strongStructuredQA, "unknown"},

		// Generated connectors are still scored normally.
		{"generated structured + strong QA → medium", "generated", "relational_db", "active", "bronze", strongStructuredQA, "medium"},
		{"generated structured + no QA → unknown", "generated", "relational_db", "active", "bronze", nil, "unknown"},
		{"generated + draft → unknown", "generated", "relational_db", "draft", "bronze", strongStructuredQA, "unknown"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := inferConfidenceLevel(tc.source, tc.category, tc.status, tc.tier, tc.qa)
			if got != tc.want {
				t.Fatalf("inferConfidenceLevel(source=%q, cat=%q, tier=%q) = %q, want %q",
					tc.source, tc.category, tc.tier, got, tc.want)
			}
		})
	}
}
