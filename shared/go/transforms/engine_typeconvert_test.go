package transforms

import (
	"context"
	"testing"
)

// TestApplyTypeConvert_DefaultKeepsOriginal verifies the default on_error is
// "skip" (keep the original value) rather than the old silent "null" coercion:
// a value that can't be converted must survive, not vanish. Explicit
// on_error=null/skip/error behavior is covered by TestSimpleTransformEngine_TypeConvert.
func TestApplyTypeConvert_DefaultKeepsOriginal(t *testing.T) {
	e := NewSimpleTransformEngine()
	data := []Row{{"n": "42"}, {"n": "N/A"}}
	out, err := e.Apply(context.Background(), data, Transform{
		Type: "type_convert",
		Config: map[string]interface{}{
			"column": "n",
			"to":     "int",
			// on_error intentionally omitted -> default must be non-destructive
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out[0]["n"] != 42 {
		t.Fatalf("expected 42, got %#v", out[0]["n"])
	}
	if out[1]["n"] != "N/A" {
		t.Fatalf("default must keep original on failed conversion, got %#v (silent-null regression)", out[1]["n"])
	}
}
