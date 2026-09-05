package healer

import (
	"encoding/json"
	"testing"
)

// These guard extractJSONObject, the helper the LLM schema-drift classifier
// (analyzeWithLLM) uses before json.Unmarshal. It asks the model for bare JSON,
// but models routinely wrap it in a ```json … ``` markdown fence. Before this helper that
// raw string was handed to json.Unmarshal, which failed with
// "invalid character '`' looking for beginning of value" and silently dropped
// the healer to its rule-based fallback (observed live in the PR #364 staging
// drift smoke, 2026-07-03). Broker-free and DB-free.

func TestExtractJSONObject(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "bare object unchanged",
			in:   `{"safe_to_auto_migrate":true}`,
			want: `{"safe_to_auto_migrate":true}`,
		},
		{
			name: "json-tagged fence (the prod repro)",
			in:   "```json\n{\"safe_to_auto_migrate\":true}\n```",
			want: `{"safe_to_auto_migrate":true}`,
		},
		{
			name: "untagged fence",
			in:   "```\n{\"a\":1}\n```",
			want: `{"a":1}`,
		},
		{
			name: "fence with prose before and after",
			in:   "Here is the analysis:\n```json\n{\"a\":1}\n```\nHope that helps!",
			want: `{"a":1}`,
		},
		{
			name: "prose surrounding, no fence",
			in:   `Sure! {"a":1} done.`,
			want: `{"a":1}`,
		},
		{
			name: "leading and trailing whitespace",
			in:   "  \n\t {\"a\":1}\n  ",
			want: `{"a":1}`,
		},
		{
			name: "nested object inside fence",
			in:   "```json\n{\"a\":{\"b\":2},\"c\":3}\n```",
			want: `{"a":{"b":2},"c":3}`,
		},
		{
			name: "fence on a single line (no newline after ```json)",
			in:   "```json {\"a\":1} ```",
			want: `{"a":1}`,
		},
		{
			name: "no object at all — returned trimmed so caller still errors",
			in:   "  not json  ",
			want: "not json",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := extractJSONObject(c.in); got != c.want {
				t.Fatalf("extractJSONObject(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// End-to-end guard: a fenced classifier response must now unmarshal cleanly into
// LLMAnalysisResponse — i.e. the healer's LLM path is no longer dead whenever the
// model fences its output.
func TestFencedResponseUnmarshalsIntoAnalysis(t *testing.T) {
	fenced := "```json\n" +
		`{"safe_to_auto_migrate":true,"reasoning":"ADD COLUMN is additive","requires_approval":false,"notify_user":false}` +
		"\n```"

	var analysis LLMAnalysisResponse
	if err := json.Unmarshal([]byte(extractJSONObject(fenced)), &analysis); err != nil {
		t.Fatalf("fenced response failed to unmarshal after extract: %v", err)
	}
	if !analysis.SafeToAutoMigrate {
		t.Errorf("SafeToAutoMigrate = false, want true")
	}
	if analysis.RequiresApproval {
		t.Errorf("RequiresApproval = true, want false")
	}
	if analysis.Reasoning != "ADD COLUMN is additive" {
		t.Errorf("Reasoning = %q, want %q", analysis.Reasoning, "ADD COLUMN is additive")
	}

	// Sanity: the raw fenced string must still fail direct unmarshal (proves the
	// helper is what fixes it, not a change in the input).
	if err := json.Unmarshal([]byte(fenced), &analysis); err == nil {
		t.Error("expected raw fenced string to fail direct json.Unmarshal, but it succeeded")
	}
}
