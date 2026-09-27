package transforms

import (
	"context"
	"strings"
	"testing"
	"time"
)

// KI-NULL-HANDLE-MISSING-COLUMN-DROPS-EVERY-ROW
//
// null_handle with strategy=drop_row (the builder's "skip" action) used to treat
// a column that is ABSENT from the row as a null value. A rule naming a column no
// row carries therefore deleted the entire dataset — silently: HTTP 200,
// transforms_applied: 1, warnings: [], n: 0. Proved on app.rsync.ai on
// 2026-09-24 with three one-rule previews.
//
// The engine's house rule on a missing column is fail-open, stated in
// applyTypeConvert's own comment ("a failed conversion must never silently
// destroy data") and in applyJSONFlatten's header; drop_row was the one branch
// that failed closed AND destructively.

func dropRowOnMissing(t *testing.T, data []Row, col string) []Row {
	t.Helper()
	out, err := NewSimpleTransformEngine().Apply(context.Background(), data, Transform{
		Type:   "null_handle",
		Config: map[string]interface{}{"column": col, "strategy": "drop_row"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return out
}

// The headline: a rule on a column no row carries must not consume the dataset.
func TestNullHandleDropRow_ColumnAbsentFromEveryRowKeepsEveryRow(t *testing.T) {
	data := []Row{
		{"id": 1, "email": "a@b.com"},
		{"id": 2, "email": "c@d.com"},
	}

	out := dropRowOnMissing(t, data, "not_a_column")

	if len(out) != len(data) {
		t.Fatalf("a rule naming a column no row carries dropped %d of %d rows; it must drop none", len(data)-len(out), len(data))
	}
}

// The control that arms the test above: present-and-null MUST still drop, so a
// fix that simply stopped dropping anything would fail here.
func TestNullHandleDropRow_PresentButNullStillDrops(t *testing.T) {
	data := []Row{
		{"id": 1, "email": "a@b.com"},
		{"id": 2, "email": nil},
		{"id": 3, "email": "   "}, // isNullish: whitespace-only counts as null
		{"id": 4},                 // absent, not null — kept
	}

	out := dropRowOnMissing(t, data, "email")

	if len(out) != 2 {
		t.Fatalf("expected the 2 nullish rows dropped and 2 kept, got %d: %v", len(out), out)
	}
	if out[0]["id"] != 1 || out[1]["id"] != 4 {
		t.Fatalf("expected rows 1 and 4 to survive, got %v", out)
	}
}

// The exact repro from prod: the builder happily orders a rename BEFORE a rule
// that still names the old column, and that alone delivered zero rows.
func TestNullHandleDropRow_RenameThenSkipOldNameKeepsRows(t *testing.T) {
	data := []Row{
		{"id": 1, "email": "a@b.com"},
		{"id": 2, "email": "c@d.com"},
	}
	chain := []Transform{
		{Type: "rename_columns", Config: map[string]interface{}{
			"mappings": map[string]interface{}{"email": "email_address"},
		}},
		{Type: "null_handle", Config: map[string]interface{}{
			"column": "email", "strategy": "drop_row",
		}},
	}

	coordinator := NewTransformCoordinator(NewSimpleTransformEngine(), NewDuckDBTransformEngine())
	out, warnings, err := coordinator.ApplyWithWarnings(context.Background(), data, chain)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(out) != 2 {
		t.Fatalf("rename-then-skip(old name) delivered %d of 2 rows; it must deliver all of them", len(out))
	}
	if _, ok := out[0]["email_address"]; !ok {
		t.Fatalf("expected the rename to have landed, got %v", out[0])
	}

	// Fail-open is not enough on its own: the mismatch must be said out loud,
	// or the operator sees a rule that quietly does nothing.
	if len(warnings) != 1 {
		t.Fatalf("expected exactly 1 warning about the missing column, got %d: %v", len(warnings), warnings)
	}
	for _, want := range []string{"transform 1", "null_handle", `"email"`, "not present"} {
		if !strings.Contains(warnings[0], want) {
			t.Fatalf("warning %q does not mention %q", warnings[0], want)
		}
	}
}

// The control that arms the warning: a chain where the column IS present must
// stay silent, so an unconditional warning would fail here.
func TestApplyWithWarnings_ColumnPresentRaisesNoWarning(t *testing.T) {
	data := []Row{{"id": 1, "email": "a@b.com"}, {"id": 2, "email": nil}}
	chain := []Transform{
		{Type: "null_handle", Config: map[string]interface{}{
			"column": "email", "strategy": "drop_row",
		}},
		{Type: "rename_columns", Config: map[string]interface{}{
			"mappings": map[string]interface{}{"email": "email_address"},
		}},
	}

	coordinator := NewTransformCoordinator(NewSimpleTransformEngine(), NewDuckDBTransformEngine())
	out, warnings, err := coordinator.ApplyWithWarnings(context.Background(), data, chain)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warnings when the column is present, got %v", warnings)
	}
	if len(out) != 1 {
		t.Fatalf("expected the null row dropped, got %d rows: %v", len(out), out)
	}
}

// A column present on SOME rows is a normal sparse document, not a mismatch.
func TestApplyWithWarnings_ColumnOnSomeRowsRaisesNoWarning(t *testing.T) {
	data := []Row{{"id": 1}, {"id": 2, "email": "c@d.com"}}
	chain := []Transform{{Type: "null_handle", Config: map[string]interface{}{
		"column": "email", "strategy": "drop_row",
	}}}

	_, warnings, err := NewTransformCoordinator(NewSimpleTransformEngine(), nil).
		ApplyWithWarnings(context.Background(), data, chain)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warning when the column exists on at least one row, got %v", warnings)
	}
}

// An empty input is not evidence that a column is missing; saying so would put a
// spurious warning on every preview of an empty table.
func TestApplyWithWarnings_EmptyInputRaisesNoWarning(t *testing.T) {
	chain := []Transform{{Type: "null_handle", Config: map[string]interface{}{
		"column": "whatever", "strategy": "drop_row",
	}}}

	_, warnings, err := NewTransformCoordinator(NewSimpleTransformEngine(), nil).
		ApplyWithWarnings(context.Background(), []Row{}, chain)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("expected no warning on empty input, got %v", warnings)
	}
}

// The preview endpoint's warnings field is what the operator actually sees, so
// prove the warning survives the PreviewExecutor and is not swallowed there.
func TestPreviewExecutor_SurfacesMissingColumnWarning(t *testing.T) {
	data := []Row{{"id": 1, "email": "a@b.com"}}
	chain := []Transform{{Type: "null_handle", Config: map[string]interface{}{
		"column": "gone", "strategy": "drop_row",
	}}}

	executor := NewPreviewExecutor(NewTransformCoordinator(NewSimpleTransformEngine(), nil))
	out, warnings, err := executor.Preview(data, chain, 3*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected the row kept, got %d", len(out))
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], `"gone"`) {
		t.Fatalf("expected the missing-column warning on the preview result, got %v", warnings)
	}
}

// The batch executor and the CDC sink feed rules in one at a time, so the
// coordinator's step index would read 0 for every rule and contradict the Order
// those callers log beside it. They call the check directly instead, which means
// the message itself must carry no position.
func TestMissingColumnWarnings_MessageCarriesNoPosition(t *testing.T) {
	rule := Transform{Type: "null_handle", Config: map[string]interface{}{
		"column": "gone", "strategy": "drop_row",
	}}

	got := MissingColumnWarnings(rule, []Row{{"id": 1}})
	if len(got) != 1 {
		t.Fatalf("expected 1 warning, got %v", got)
	}
	if strings.Contains(got[0], "transform ") {
		t.Fatalf("the bare message must not number the rule, the caller does: %q", got[0])
	}
	if !strings.HasPrefix(got[0], `column "gone"`) {
		t.Fatalf("expected the message to lead with the column, got %q", got[0])
	}

	// Still positioned when the whole chain goes through the coordinator.
	_, warnings, err := NewTransformCoordinator(NewSimpleTransformEngine(), nil).
		ApplyWithWarnings(context.Background(), []Row{{"id": 1}}, []Transform{rule, rule})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 2 || !strings.HasPrefix(warnings[0], "transform 0 (null_handle): ") ||
		!strings.HasPrefix(warnings[1], "transform 1 (null_handle): ") {
		t.Fatalf("expected each chain warning numbered by its step, got %v", warnings)
	}
}
