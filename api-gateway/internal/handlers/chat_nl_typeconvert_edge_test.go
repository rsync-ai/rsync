package handlers

import "testing"

// TestParseTypeConvertIntentEdge covers edge phrasings of parseTypeConvertIntent
// (chat_nl_pipeline.go:401) beyond the cases already in chat_nl_mask_test.go.
func TestParseTypeConvertIntentEdge(t *testing.T) {
	cases := []struct {
		name    string
		message string
		want    bool
	}{
		// "convert" + "type" (in "types") → true (chat_nl_pipeline.go:410).
		{"convert types", "convert types", true},
		// "recommended types" contains the "recommended type" substring
		// (chat_nl_pipeline.go:405) → true.
		{"apply recommended types", "apply recommended types", true},
		// "convert" + "string columns" → true (chat_nl_pipeline.go:412-413).
		{"convert string columns", "convert string columns", true},
		// No "type"/"string column" trigger with "convert" → false. A per-column
		// cast request like this is NOT recognized as the auto-type-convert intent.
		{"convert email to string is not type-convert", "convert email to string", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := parseTypeConvertIntent(tc.message); got != tc.want {
				t.Errorf("parseTypeConvertIntent(%q) = %v, want %v", tc.message, got, tc.want)
			}
		})
	}
}
