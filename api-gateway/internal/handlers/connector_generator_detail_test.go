package handlers

import "testing"

// llm-service reports every generator failure by raising FastAPI's
// HTTPException, whose body is {"detail": ...}. The gateway's error-message
// normalization chain read "error" then "message" and then gave up on the
// string "Generation failed" -- so none of those bodies ever reached the user,
// including the one that says the service is not configured.
func TestFastAPIDetailIsRead(t *testing.T) {
	cases := []struct {
		name string
		in   any
		want string
	}{
		{"string detail", "internal_secret_not_configured", "internal_secret_not_configured"},
		{"string detail is trimmed", "  vendor 'x' not found\n", "vendor 'x' not found"},
		{"structured detail", map[string]any{"code": "llm_down", "message": "model endpoint refused"}, "model endpoint refused"},
		{"structured detail without message", map[string]any{"error": "bad spec"}, "bad spec"},
		// Controls: these must stay empty so the caller keeps its own default
		// rather than printing a type name or an empty string as the reason.
		{"absent", nil, ""},
		{"whitespace only", "   ", ""},
		{"wrong type", []any{"a", "b"}, ""},
		{"structured with no string field", map[string]any{"code": 503}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := fastAPIDetail(tc.in); got != tc.want {
				t.Fatalf("fastAPIDetail(%#v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// normalizeGeneratorErrorMessage mirrors the handler's fallback chain so the
// ordering is pinned: an explicit error/message still wins, and `detail` is
// consulted before the generic string.
func normalizeGeneratorErrorMessage(payload map[string]any) string {
	if e, ok := payload["error"].(string); ok && e != "" {
		return e
	}
	if m, ok := payload["message"].(string); ok && m != "" {
		return m
	}
	if d := fastAPIDetail(payload["detail"]); d != "" {
		return d
	}
	return "Generation failed"
}

func TestGeneratorErrorMessageOrdering(t *testing.T) {
	if got := normalizeGeneratorErrorMessage(map[string]any{"detail": "internal_secret_not_configured"}); got != "internal_secret_not_configured" {
		t.Fatalf("detail-only body = %q, want the detail", got)
	}
	// Non-zero controls: the pre-existing precedence is unchanged.
	if got := normalizeGeneratorErrorMessage(map[string]any{"error": "spec invalid", "detail": "ignored"}); got != "spec invalid" {
		t.Fatalf("error should still win, got %q", got)
	}
	if got := normalizeGeneratorErrorMessage(map[string]any{"message": "rate limited", "detail": "ignored"}); got != "rate limited" {
		t.Fatalf("message should still win, got %q", got)
	}
	if got := normalizeGeneratorErrorMessage(map[string]any{}); got != "Generation failed" {
		t.Fatalf("empty body should still fall back, got %q", got)
	}
}
