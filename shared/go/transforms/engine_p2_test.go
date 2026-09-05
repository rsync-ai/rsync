package transforms

import (
	"context"
	"strings"
	"testing"
)

func TestSimpleTransformEngine_JSONFlatten_Object(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	// Already-decoded object value.
	data := []Row{{"id": 1, "metadata": map[string]interface{}{"city": "NYC", "zip": "10001"}}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "json_flatten",
		Config: map[string]interface{}{
			"column": "metadata",
			"prefix": "metadata_",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := out[0]["metadata"]; ok {
		t.Fatalf("expected original column removed")
	}
	if got := out[0]["metadata_city"]; got != "NYC" {
		t.Fatalf("expected metadata_city=NYC, got %#v", got)
	}
	if got := out[0]["metadata_zip"]; got != "10001" {
		t.Fatalf("expected metadata_zip=10001, got %#v", got)
	}
	if got := out[0]["id"]; got != 1 {
		t.Fatalf("expected id preserved, got %#v", got)
	}
}

func TestSimpleTransformEngine_JSONFlatten_FromString(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	// JSON string value (as a DB JSONB column often arrives).
	data := []Row{{"payload": `{"a": "x", "b": {"c": "y"}}`}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "json_flatten",
		Config: map[string]interface{}{
			"column":    "payload",
			"separator": "_",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out[0]["a"]; got != "x" {
		t.Fatalf("expected a=x, got %#v", got)
	}
	// Unlimited depth (default): nested object flattens fully.
	if got := out[0]["b_c"]; got != "y" {
		t.Fatalf("expected b_c=y, got %#v", got)
	}
}

func TestSimpleTransformEngine_JSONFlatten_MaxDepth(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"j": map[string]interface{}{
		"a": map[string]interface{}{"b": map[string]interface{}{"c": float64(1)}},
	}}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "json_flatten",
		Config: map[string]interface{}{
			"column":    "j",
			"max_depth": 2,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// depth 1 = top key "a"; depth 2 = "a_b"; deeper than max_depth stays nested
	// and is scalarized to a JSON string.
	v, ok := out[0]["a_b"]
	if !ok {
		t.Fatalf("expected a_b key present, got row %#v", out[0])
	}
	s, ok := v.(string)
	if !ok || !strings.Contains(s, `"c"`) {
		t.Fatalf("expected a_b to be a JSON string containing c, got %#v", v)
	}
}

func TestSimpleTransformEngine_JSONFlatten_NonJSONUnchanged(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{
		{"col": "not json"},
		{"col": nil},
		{"other": "x"}, // column missing entirely
	}
	out, err := e.Apply(ctx, data, Transform{
		Type:   "json_flatten",
		Config: map[string]interface{}{"column": "col"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := out[0]["col"]; got != "not json" {
		t.Fatalf("expected non-JSON value preserved, got %#v", got)
	}
	if got, ok := out[1]["col"]; !ok || got != nil {
		t.Fatalf("expected nil value preserved, got %#v", got)
	}
	if got := out[2]["other"]; got != "x" {
		t.Fatalf("expected unrelated row preserved, got %#v", got)
	}
	// Row count must be unchanged (wide-format invariant).
	if len(out) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(out))
	}
}

func TestSimpleTransformEngine_ArrayExpand_WideFormat(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"id": 1, "tags": []interface{}{"a", "b", "c"}}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "array_expand",
		Config: map[string]interface{}{
			"column":        "tags",
			"output_prefix": "tag_",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// CRITICAL: one input row -> one output row (wide format), never row expansion.
	if len(out) != 1 {
		t.Fatalf("array_expand must be wide-format (1 row -> 1 row), got %d rows", len(out))
	}
	if _, ok := out[0]["tags"]; ok {
		t.Fatalf("expected original column removed")
	}
	if out[0]["tag_0"] != "a" || out[0]["tag_1"] != "b" || out[0]["tag_2"] != "c" {
		t.Fatalf("expected indexed columns tag_0..2, got %#v", out[0])
	}
}

func TestSimpleTransformEngine_ArrayExpand_MaxElements(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"ids": []interface{}{1, 2, 3, 4, 5}}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "array_expand",
		Config: map[string]interface{}{
			"column":       "ids",
			"max_elements": 2,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := out[0]["ids_2"]; ok {
		t.Fatalf("expected max_elements cap to drop ids_2, got %#v", out[0])
	}
	if out[0]["ids_0"] != 1 || out[0]["ids_1"] != 2 {
		t.Fatalf("expected first 2 elements, got %#v", out[0])
	}
}

func TestSimpleTransformEngine_ArrayExpand_FromStringAndDefaults(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	// JSON array string + default output_prefix (column + "_").
	data := []Row{{"vals": `["x", "y"]`}}
	out, err := e.Apply(ctx, data, Transform{
		Type:   "array_expand",
		Config: map[string]interface{}{"column": "vals"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out[0]["vals_0"] != "x" || out[0]["vals_1"] != "y" {
		t.Fatalf("expected default-prefixed vals_0/vals_1, got %#v", out[0])
	}
}

func TestSimpleTransformEngine_ArrayExpand_NonArrayUnchanged(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"col": "scalar"}, {"missing": 1}}
	out, err := e.Apply(ctx, data, Transform{
		Type:   "array_expand",
		Config: map[string]interface{}{"column": "col"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out[0]["col"] != "scalar" {
		t.Fatalf("expected non-array value preserved, got %#v", out[0]["col"])
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(out))
	}
}

func TestApplyMask_HashFunctions(t *testing.T) {
	t.Setenv("RSYNC_PII_HASH_SALT", "test-salt")

	cases := []struct {
		hashFunc string
		prefix   string
	}{
		{"sha256", "sha256:"},
		{"hmac_sha256", "hmac256:"},
		{"md5", "md5:"},
		{"", "sha256:"},        // default
		{"unknown", "sha256:"}, // fallback
	}
	for _, c := range cases {
		got := hashValue("alice@example.com", c.hashFunc)
		if !strings.HasPrefix(got, c.prefix) {
			t.Fatalf("hashFunc %q: expected prefix %q, got %q", c.hashFunc, c.prefix, got)
		}
		// Determinism: same input -> same output.
		if again := hashValue("alice@example.com", c.hashFunc); again != got {
			t.Fatalf("hashFunc %q: not deterministic (%v != %v)", c.hashFunc, got, again)
		}
	}

	// Different algorithms must produce different digests for the same input.
	sha := hashValue("x", "sha256")
	hm := hashValue("x", "hmac_sha256")
	md := hashValue("x", "md5")
	if sha == hm || sha == md || hm == md {
		t.Fatalf("expected distinct digests across algorithms: sha=%v hmac=%v md5=%v", sha, hm, md)
	}
}

func TestApplyMask_ViaEngine_HashFunctionConfig(t *testing.T) {
	e := NewSimpleTransformEngine()
	ctx := context.Background()

	data := []Row{{"email": "a@b.com", "name": "Alice"}}
	out, err := e.Apply(ctx, data, Transform{
		Type: "mask_pii",
		Config: map[string]interface{}{
			"column":        "email",
			"mask_type":     "hash",
			"hash_function": "md5",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	masked, _ := out[0]["email"].(string)
	if !strings.HasPrefix(masked, "md5:") {
		t.Fatalf("expected md5-prefixed mask, got %#v", out[0]["email"])
	}
	if out[0]["name"] != "Alice" {
		t.Fatalf("expected non-masked column preserved, got %#v", out[0]["name"])
	}
}
