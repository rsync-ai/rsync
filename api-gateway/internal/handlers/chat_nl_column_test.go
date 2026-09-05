package handlers

import (
	"reflect"
	"testing"
)

// TestExtractNLColumnTokens pins the tokenizer behavior of extractNLColumnTokens
// (chat_nl_pipeline.go:421). Inputs are the post-verb CLAUSE (what
// parseMaskingIntent passes in), not the whole message. It splits on space plus
// "," / "&" / "/", drops stop-words (chat_nl_pipeline.go:351-360) and
// non-identifier noise (nlIdentRe, chat_nl_pipeline.go:347), and dedupes.
func TestExtractNLColumnTokens(t *testing.T) {
	cases := []struct {
		name   string
		clause string
		want   []string
	}{
		{"comma and ampersand split", "email, phone & ssn", []string{"email", "phone", "ssn"}},
		{"slash split", "email / phone", []string{"email", "phone"}},
		{"stop-word drop", "the email and phone columns", []string{"email", "phone"}},
		{"numeric non-identifier drop", "123 email", []string{"email"}},
		{"dedupe repeated column", "email and email", []string{"email"}},

		// FIXED (#5): nlIdentRe now allows hyphens, so "user-id" is captured as a
		// named column instead of being silently dropped into blanket-PII masking.
		{"hyphenated name captured", "user-id", []string{"user-id"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractNLColumnTokens(tc.clause)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("extractNLColumnTokens(%q) = %#v, want %#v", tc.clause, got, tc.want)
			}
		})
	}
}

// TestParseMaskingIntentHyphenatedColumn verifies the fix for #5: a hyphenated
// column name is masked specifically, not degraded to blanket generic-PII.
func TestParseMaskingIntentHyphenatedColumn(t *testing.T) {
	// FIXED (#5): masks the specific "user-id" column with no blanket-PII fallback.
	cols, pii := parseMaskingIntent("mask user-id")
	if len(cols) != 1 || cols[0] != "user-id" {
		t.Errorf("maskColumns = %#v, want [user-id]", cols)
	}
	if pii {
		t.Errorf("maskPII = true, want false (specific column, not generic PII)")
	}
}
