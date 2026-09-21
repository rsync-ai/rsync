package transforms

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func maskCanon(cfg map[string]any) []CanonicalTransform {
	return []CanonicalTransform{{Type: "mask_pii", Enabled: true, Config: cfg}}
}

func packedRows() []Row {
	return []Row{{
		"_id": "65f0aa01",
		"document": map[string]interface{}{
			"email": "alice@example.com",
			"name":  "Al",
			"age":   42,
		},
	}}
}

func TestVerifyNoResidualPlaintext_DetectsPackedNoOp(t *testing.T) {
	canon := maskCanon(map[string]any{"column": "email"})
	in := packedRows()
	snap, err := SnapshotPlaintext(in, canon)
	if err != nil {
		t.Fatal(err)
	}
	// Top-level mask on a packed row is a silent no-op (issue #23).
	out, err := NewSimpleTransformEngine().Apply(context.Background(), in, canon[0].EngineTransform())
	if err != nil {
		t.Fatal(err)
	}
	violations, err := VerifyNoResidualPlaintext(snap, out)
	if !errors.Is(err, ErrResidualPlaintext) {
		t.Fatalf("err = %v, want ErrResidualPlaintext", err)
	}
	if !reflect.DeepEqual(violations, []string{"document.email"}) {
		t.Fatalf("violations = %v, want [document.email]", violations)
	}
}

func TestVerifyNoResidualPlaintext_PassesWhenMasked(t *testing.T) {
	e := NewSimpleTransformEngine()
	for _, cfg := range []map[string]any{
		{"column": "email", "deep": true},
		{"path": "document.email"},
		{"column": "email", "deep": true, "mask_type": "partial"},
	} {
		canon := maskCanon(cfg)
		in := packedRows()
		snap, err := SnapshotPlaintext(in, canon)
		if err != nil {
			t.Fatal(err)
		}
		if snap.Values() == 0 {
			t.Fatalf("control: snapshot for %v collected nothing, the pass below would be vacuous", cfg)
		}
		out, err := e.Apply(context.Background(), in, canon[0].EngineTransform())
		if err != nil {
			t.Fatal(err)
		}
		if v, err := VerifyNoResidualPlaintext(snap, out); err != nil || len(v) != 0 {
			t.Fatalf("masked output flagged for %v: %v %v", cfg, v, err)
		}
	}
}

func TestVerifyNoResidualPlaintext_RenameBeforeMaskLeaks(t *testing.T) {
	canon := []CanonicalTransform{
		{Type: "rename_columns", Enabled: true, Config: map[string]any{"from": "email", "to": "contact"}},
		{Type: "mask_pii", Enabled: true, Config: map[string]any{"column": "email"}},
	}
	in := []Row{{"id": 1, "email": "alice@example.com"}}
	snap, err := SnapshotPlaintext(in, canon)
	if err != nil {
		t.Fatal(err)
	}
	coord := NewTransformCoordinator(NewSimpleTransformEngine(), nil)
	out, err := coord.Apply(context.Background(), in, []Transform{canon[0].EngineTransform(), canon[1].EngineTransform()})
	if err != nil {
		t.Fatal(err)
	}
	violations, err := VerifyNoResidualPlaintext(snap, out)
	if err == nil || !reflect.DeepEqual(violations, []string{"contact"}) {
		t.Fatalf("violations = %v err = %v, want [contact]", violations, err)
	}
}

func TestVerifyNoResidualPlaintext_JSONFlattenMoveLeaks(t *testing.T) {
	canon := maskCanon(map[string]any{"column": "email"})
	in := packedRows()
	snap, err := SnapshotPlaintext(in, canon)
	if err != nil {
		t.Fatal(err)
	}
	out, err := NewSimpleTransformEngine().Apply(context.Background(), in, Transform{Type: "json_flatten", Config: map[string]interface{}{"column": "document"}})
	if err != nil {
		t.Fatal(err)
	}
	violations, _ := VerifyNoResidualPlaintext(snap, out)
	if !reflect.DeepEqual(violations, []string{"email"}) {
		t.Fatalf("violations = %v, want [email]", violations)
	}
}

func TestVerifyNoResidualPlaintext_CaseInsensitive(t *testing.T) {
	canon := maskCanon(map[string]any{"column": "email"})
	in := []Row{{"Profile": map[string]interface{}{"Email": "alice@example.com"}}}
	snap, err := SnapshotPlaintext(in, canon)
	if err != nil {
		t.Fatal(err)
	}
	violations, err := VerifyNoResidualPlaintext(snap, in)
	if err == nil || !reflect.DeepEqual(violations, []string{"Profile.Email"}) {
		t.Fatalf("violations = %v err = %v, want [Profile.Email]", violations, err)
	}
}

func TestVerifyNoResidualPlaintext_Floor(t *testing.T) {
	canon := maskCanon(map[string]any{"columns": []any{"code", "flag", "zip", "card", "ref"}})
	in := []Row{{
		"code": "abc",      // 3 chars: below the string floor
		"flag": true,       // booleans never collected
		"zip":  12345,      // 5 digits: below the number floor
		"card": 4111111111, // 10 digits: collected
		"ref":  "ab12",     // 4 chars: collected
	}}
	snap, err := SnapshotPlaintext(in, canon)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Values() != 2 {
		t.Fatalf("collected %d values, want 2 (card, ref)", snap.Values())
	}
	out := []Row{{
		"other_code": "abc",
		"other_flag": true,
		"other_zip":  12345,
		"card_str":   "4111111111", // number re-emitted as a string still leaks
		"items":      []interface{}{map[string]interface{}{"r": "ab12"}},
	}}
	violations, err := VerifyNoResidualPlaintext(snap, out)
	if err == nil || !reflect.DeepEqual(violations, []string{"card_str", "items[].r"}) {
		t.Fatalf("violations = %v err = %v, want [card_str items[].r]", violations, err)
	}
}

func TestVerifyNoResidualPlaintext_NoValuesInErrors(t *testing.T) {
	values := []string{"alice@example.com", "5551234567", "bob@example.com"}
	canon := maskCanon(map[string]any{"columns": []any{"email", "phone"}})
	in := []Row{
		{"email": values[0], "phone": values[1]},
		{"document": map[string]interface{}{"email": values[2]}},
	}
	snap, err := SnapshotPlaintext(in, canon)
	if err != nil {
		t.Fatal(err)
	}
	violations, err := VerifyNoResidualPlaintext(snap, in)
	if err == nil || len(violations) != 3 {
		t.Fatalf("control: expected 3 violations, got %v (%v)", violations, err)
	}
	text := err.Error() + strings.Join(violations, "|")
	for _, v := range values {
		if strings.Contains(text, v) {
			t.Fatalf("a fixture value leaked into the guard output")
		}
	}
}

func TestVerifyNoResidualPlaintext_InPlaceNestedMutationCannotPassVacuously(t *testing.T) {
	canon := maskCanon(map[string]any{"column": "email", "deep": true})
	in := packedRows()
	snap, err := SnapshotPlaintext(in, canon)
	if err != nil {
		t.Fatal(err)
	}

	// A buggy in-place masker: it leaks a copy of the value to another key, then
	// overwrites the INPUT's shared nested map. If the guard derived its
	// plaintext set from the (now mutated) input, it would find nothing.
	doc := in[0]["document"].(map[string]interface{})
	doc["backup"] = doc["email"]
	doc["email"] = "sha256:0000"
	out := in

	violations, err := VerifyNoResidualPlaintext(snap, out)
	if err == nil || !reflect.DeepEqual(violations, []string{"document.backup"}) {
		t.Fatalf("violations = %v err = %v, want [document.backup]: the snapshot must hold copies taken before the chain", violations, err)
	}

	// And the real deep mask never mutates its input, so a caller that
	// snapshots and then masks sees the original values in its input rows.
	in2 := packedRows()
	if _, err := NewSimpleTransformEngine().Apply(context.Background(), in2, canon[0].EngineTransform()); err != nil {
		t.Fatal(err)
	}
	if in2[0]["document"].(map[string]interface{})["email"] != "alice@example.com" {
		t.Fatalf("deep mask mutated its input row in place")
	}
}

// A path mask covers only the occurrence the user named. The same key name
// elsewhere in the document holds an unrelated value that must not be collected,
// or a correctly applied path mask would halt the pipeline.
func TestVerifyNoResidualPlaintext_PathTargetCollectsOnlyThatPath(t *testing.T) {
	canon := maskCanon(map[string]any{"path": "document.name"})
	newRows := func() []Row {
		return []Row{{
			"_id": "65f0aa01",
			"document": map[string]interface{}{
				"name":  "Alice Smith",
				"items": []interface{}{map[string]interface{}{"name": "Widget Pro"}},
			},
			"name": "Top Level Label",
		}}
	}
	in := newRows()
	snap, err := SnapshotPlaintext(in, canon)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Values() != 1 {
		t.Fatalf("path snapshot collected %d values, want exactly 1 (document.name only)", snap.Values())
	}
	out, stats, err := NewSimpleTransformEngine().ApplyMaskWithStats(context.Background(), in, canon[0].Config)
	if err != nil {
		t.Fatal(err)
	}
	if len(stats.Unmatched()) != 0 {
		t.Fatalf("control: path mask matched nothing: %+v", stats)
	}
	if v, err := VerifyNoResidualPlaintext(snap, out); err != nil || len(v) != 0 {
		t.Fatalf("correctly applied path mask flagged: %v %v", v, err)
	}

	// Non-vacuous: with the mask not applied, the exact path is still caught.
	if v, err := VerifyNoResidualPlaintext(snap, newRows()); !errors.Is(err, ErrResidualPlaintext) || !reflect.DeepEqual(v, []string{"document.name"}) {
		t.Fatalf("unmasked path not detected: %v %v", v, err)
	}

	// A path crossing an array collects every element at that path, and only there.
	canon = maskCanon(map[string]any{"path": "document.items.name"})
	snap, err = SnapshotPlaintext(newRows(), canon)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Values() != 1 {
		t.Fatalf("array path snapshot collected %d values, want 1", snap.Values())
	}
	if v, err := VerifyNoResidualPlaintext(snap, newRows()); !reflect.DeepEqual(v, []string{"document.items[].name"}) || err == nil {
		t.Fatalf("array path leak: got %v %v", v, err)
	}
}

func TestVerifyNoResidualPlaintext_NilSnapshotFailsClosed(t *testing.T) {
	if _, err := VerifyNoResidualPlaintext(nil, []Row{{"email": "alice@example.com"}}); err == nil {
		t.Fatalf("a nil snapshot must be an error")
	}
	if _, err := SnapshotPlaintext(nil, maskCanon(map[string]any{"deep": true})); err == nil {
		t.Fatalf("an unparseable mask config must fail the snapshot")
	}
}

func TestVerifyNoResidualPlaintext_NilAndEmptyIgnored(t *testing.T) {
	canon := maskCanon(map[string]any{"column": "email"})
	in := []Row{{"email": nil}, {"email": ""}, {"email": map[string]interface{}{}}, {}}
	snap, err := SnapshotPlaintext(in, canon)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Values() != 0 {
		t.Fatalf("nil/empty values collected: %d", snap.Values())
	}
	if v, err := VerifyNoResidualPlaintext(snap, []Row{{"x": "", "y": nil}}); err != nil || v != nil {
		t.Fatalf("got %v %v", v, err)
	}

	// No mask transforms at all: nothing to guard.
	snap, err = SnapshotPlaintext([]Row{{"email": "alice@example.com"}}, []CanonicalTransform{{Type: "filter", Enabled: true}})
	if err != nil || snap.Targets() != 0 {
		t.Fatalf("snap=%+v err=%v", snap, err)
	}
	if v, err := VerifyNoResidualPlaintext(snap, []Row{{"email": "alice@example.com"}}); err != nil || v != nil {
		t.Fatalf("got %v %v", v, err)
	}
}

func benchPackedRows(n int) []Row {
	rows := make([]Row, n)
	for i := range rows {
		rows[i] = Row{
			"_id": fmt.Sprintf("id-%06d", i),
			"document": map[string]interface{}{
				"email":   fmt.Sprintf("user%06d@example.com", i),
				"name":    fmt.Sprintf("User %d", i),
				"age":     30 + i%40,
				"phone":   5550000000 + i,
				"profile": map[string]interface{}{"email": fmt.Sprintf("alt%06d@example.com", i), "city": "Paris"},
				"tags":    []interface{}{"a", "b", "c"},
			},
		}
	}
	return rows
}

// BenchmarkMaskGuard_1kPackedRows measures the full guarded path for a 1k-row
// chunk: snapshot, deep mask, verify.
func BenchmarkMaskGuard_1kPackedRows(b *testing.B) {
	e := NewSimpleTransformEngine()
	canon := maskCanon(map[string]any{"columns": []any{"email", "phone"}, "deep": true})
	tr := canon[0].EngineTransform()
	rows := benchPackedRows(1000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap, err := SnapshotPlaintext(rows, canon)
		if err != nil {
			b.Fatal(err)
		}
		out, err := e.Apply(context.Background(), rows, tr)
		if err != nil {
			b.Fatal(err)
		}
		if v, err := VerifyNoResidualPlaintext(snap, out); err != nil || v != nil {
			b.Fatalf("unexpected violations: %v", v)
		}
	}
}

// BenchmarkVerifyNoResidualPlaintext_1kPackedRows isolates the verify walk.
func BenchmarkVerifyNoResidualPlaintext_1kPackedRows(b *testing.B) {
	canon := maskCanon(map[string]any{"columns": []any{"email", "phone"}, "deep": true})
	rows := benchPackedRows(1000)
	snap, err := SnapshotPlaintext(rows, canon)
	if err != nil {
		b.Fatal(err)
	}
	out, err := NewSimpleTransformEngine().Apply(context.Background(), rows, canon[0].EngineTransform())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if v, _ := VerifyNoResidualPlaintext(snap, out); v != nil {
			b.Fatal("unexpected violations")
		}
	}
}
