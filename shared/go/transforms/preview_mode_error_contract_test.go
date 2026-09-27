package transforms

import "testing"

// PreviewTransforms discarded NormalizeAndValidate's error with `_`. That is safe
// only while preview mode converts every rejection into a warning, which is what
// these tests pin down. The handler now surfaces the error anyway; if a future
// preview path starts returning one, this test goes red and names the contract
// that the handler was silently relying on.

func TestPreviewModeTurnsEveryRejectionIntoAWarning(t *testing.T) {
	cases := []struct {
		name string
		raw  []map[string]any
	}{
		{"unknown type", []map[string]any{{"type": "definitely_not_a_transform"}}},
		// mask_pii with neither config.column nor config.columns — validateConfig
		// rejects it. (A `filter` with no condition is NOT a rejection: the
		// condition is an optional no-op, validate.go:428.)
		{"missing required config", []map[string]any{{"type": "mask"}}},
		{"table-scoped with no current table", []map[string]any{
			{"type": "filter", "config": map[string]any{"condition": "id > 1"}, "scope": map[string]any{"table": "customers"}},
		}},
		{"requires_full_dataset", []map[string]any{
			{"type": "aggregate", "config": map[string]any{"group_by": "id"}},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, warnings, err := NormalizeAndValidate(tc.raw, "", NormalizeModePreview)
			if err != nil {
				t.Fatalf("preview mode returned an error (%v); PreviewTransforms must surface it, "+
					"not drop it on the floor", err)
			}
			if len(warnings) == 0 {
				t.Fatal("rejected in silence: no error and no warning, so the caller has nothing to show")
			}
		})
	}
}

func TestPreviewModeDoesNotWarnOnAValidTransform(t *testing.T) {
	// Non-zero control #1: proves the `len(warnings) == 0` assertion above is
	// discriminating, and that preview does not simply warn about everything.
	canonical, warnings, err := NormalizeAndValidate(
		[]map[string]any{{"type": "filter", "config": map[string]any{"condition": "id > 1"}}},
		"orders", NormalizeModePreview)
	if err != nil {
		t.Fatalf("valid transform errored in preview: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("preview warned about a valid transform (%v); the cases above prove nothing", warnings)
	}
	if len(canonical) != 1 {
		t.Fatalf("valid transform was dropped: got %d canonical transforms, want 1", len(canonical))
	}
}

func TestExecutionModeStillRejectsLoudly(t *testing.T) {
	// Non-zero control #2: the rejection cases would pass trivially if these
	// inputs were actually valid, so prove execution mode refuses them.
	for _, raw := range []map[string]any{
		{"type": "definitely_not_a_transform"},
		{"type": "mask"},
	} {
		if _, _, err := NormalizeAndValidate([]map[string]any{raw}, "orders", NormalizeModeExecution); err == nil {
			t.Fatalf("execution mode accepted %v; the preview cases above prove nothing", raw)
		}
	}
}
