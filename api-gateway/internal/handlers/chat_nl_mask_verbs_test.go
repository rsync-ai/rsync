package handlers

import (
	"reflect"
	"testing"
)

// mustEqualCols compares two column slices treating nil and empty as equal.
func mustEqualCols(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("maskColumns = %#v, want %#v", got, want)
	}
}

// TestParseMaskingIntentMaskVerbs covers the masking verbs recognized by
// maskVerbRe (chat_nl_pipeline.go:343) that the existing suite never exercised —
// only "mask"/"redact" were tested. Each verb should trigger extraction of the
// named column. It also covers the two PII phrase branches ("personal data" /
// "personally identifiable") in parseMaskingIntent (chat_nl_pipeline.go:374-376)
// that set MaskPII=true — previously only "pii"/"sensitive" were tested.
func TestParseMaskingIntentMaskVerbs(t *testing.T) {
	cases := []struct {
		name     string
		message  string
		wantCols []string
		wantPII  bool
	}{
		// ── every mask verb extracts the named column ───────────────────────
		{"anonymize verb", "anonymize email", []string{"email"}, false},
		{"anonymise verb", "anonymise email", []string{"email"}, false},
		{"obfuscate verb", "obfuscate ssn", []string{"ssn"}, false},
		{"pseudonymize verb", "pseudonymize email", []string{"email"}, false},
		{"pseudonymise verb", "pseudonymise email", []string{"email"}, false},
		{"scrub verb", "scrub email", []string{"email"}, false},

		// ── PII phrase branches → MaskPII=true, no named column ──────────────
		{"personal data → pii", "sync mysql to bq and mask personal data", nil, true},
		{"personally identifiable data → pii", "mask personally identifiable data", nil, true},

		// FIXED (#8): "information"/"info" are now stop-words, so this yields the
		// generic PII flag only, with no spurious "information" column.
		{"personally identifiable information → pii only", "mask personally identifiable information", nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols, pii := parseMaskingIntent(tc.message)
			if pii != tc.wantPII {
				t.Errorf("maskPII = %v, want %v", pii, tc.wantPII)
			}
			mustEqualCols(t, cols, tc.wantCols)
		})
	}
}

// TestParseMaskingIntentClauseCutMarkers verifies the column clause is bounded at
// the first downstream-instruction cut marker (chat_nl_pipeline.go:382-388), so
// the mask clause never over-captures a following schedule/sync/convert/type
// instruction. The existing suite only exercised " and convert"; this covers the
// remaining markers. Every case must extract exactly [email].
func TestParseMaskingIntentClauseCutMarkers(t *testing.T) {
	cases := []struct {
		name    string
		message string
	}{
		{"then marker", "mysql to postgres, mask email then schedule daily"},
		{"and schedule marker", "mask email and schedule daily"},
		{"and sync marker", "mask email and sync every hour"},
		{"and run marker", "mask email and run now"},
		{"every marker", "mask email every hour"},
		{"to type marker", "mask email to type string"},
		{"semicolon marker", "mask email; schedule later"},
		{"period-space marker", "mask email. then run it"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cols, _ := parseMaskingIntent(tc.message)
			mustEqualCols(t, cols, []string{"email"})
		})
	}
}
