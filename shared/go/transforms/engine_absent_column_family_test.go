package transforms

import (
	"context"
	"strings"
	"testing"
)

// The engine has one written rule for a column a rule names but no row carries:
// fail open and warn (see applyNullHandle's comment, and the KI it cites). Three
// transforms did the opposite and deleted the batch instead, each in a way that
// left every row-count invariant reading healthy. These are the guards for all
// three, plus the guard that the warning is no longer switched off for them.

func applyStep(t *testing.T, transform Transform, data []Row) []Row {
	t.Helper()
	out, err := NewSimpleTransformEngine().Apply(context.Background(), data, transform)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return out
}

// T1. The reported shape: in=2, out=0, no warning, no error.
func TestFilterOnAColumnNoRowCarriesKeepsTheBatch(t *testing.T) {
	data := []Row{{"id": 1, "amount": 10}, {"id": 2, "amount": 20}}
	out := applyStep(t, Transform{
		Type:   "filter",
		Config: map[string]interface{}{"condition": "status = 'active'"},
	}, data)

	if len(out) != len(data) {
		t.Fatalf("a filter on a column no row carries deleted the batch: in=%d out=%d", len(data), len(out))
	}
}

// The other half of the rule. A column missing from SOME rows is a sparse
// document, and a non-match there is correct; without this the fix above could
// be "never filter anything", which would be a different silent corruption.
func TestFilterStillFiltersWhenTheColumnIsMerelySparse(t *testing.T) {
	data := []Row{{"id": 1, "status": "active"}, {"id": 2, "status": "closed"}, {"id": 3}}
	out := applyStep(t, Transform{
		Type:   "filter",
		Config: map[string]interface{}{"condition": "status = 'active'"},
	}, data)

	if len(out) != 1 {
		t.Fatalf("expected only the matching row, got %d: %v", len(out), out)
	}
	if out[0]["id"] != 1 {
		t.Fatalf("expected row id=1, got %v", out[0])
	}
}

// T3.
func TestValidateSkipsARequiredColumnNoRowCarries(t *testing.T) {
	data := []Row{{"id": 1, "email": "a@example.com"}, {"id": 2, "email": "b@example.com"}}
	out := applyStep(t, Transform{
		Type:   "validate",
		Config: map[string]interface{}{"required_columns": []interface{}{"customer_email"}},
	}, data)

	if len(out) != len(data) {
		t.Fatalf("validate on an absent required column deleted the batch: in=%d out=%d", len(data), len(out))
	}
}

// The requirement that CAN be evaluated must still be enforced, or the fix above
// silently disables validation instead of scoping it.
func TestValidateStillEnforcesTheRequirementsItCanEvaluate(t *testing.T) {
	data := []Row{{"id": 1, "email": "a@example.com"}, {"id": 2, "email": nil}}
	out := applyStep(t, Transform{
		Type: "validate",
		Config: map[string]interface{}{
			"required_columns": []interface{}{"customer_email", "email"},
		},
	}, data)

	if len(out) != 1 {
		t.Fatalf("expected the null-email row dropped and the other kept, got %d: %v", len(out), out)
	}
	if out[0]["id"] != 1 {
		t.Fatalf("expected row id=1, got %v", out[0])
	}
}

// T8. The quietest one: the row COUNT survives, so nothing downstream notices.
func TestSelectColumnsWithNoMatchingColumnDoesNotEmptyEveryRow(t *testing.T) {
	data := []Row{{"id": 1, "email": "a@example.com"}, {"id": 2, "email": "b@example.com"}}
	out := applyStep(t, Transform{
		Type:   "select_columns",
		Config: map[string]interface{}{"columns": []interface{}{"idd", "e_mail"}},
	}, data)

	if len(out) != len(data) {
		t.Fatalf("row count changed: in=%d out=%d", len(data), len(out))
	}
	for i, row := range out {
		if len(row) == 0 {
			t.Fatalf("row %d came out empty: a typo in every column name emptied the batch while keeping the row count", i)
		}
	}
}

func TestSelectColumnsStillProjectsWhenOneNameMatches(t *testing.T) {
	data := []Row{{"id": 1, "email": "a@example.com", "ssn": "x"}}
	out := applyStep(t, Transform{
		Type:   "select_columns",
		Config: map[string]interface{}{"columns": []interface{}{"id", "e_mail"}},
	}, data)

	if len(out) != 1 || len(out[0]) != 1 {
		t.Fatalf("expected one row projected to one column, got %v", out)
	}
	if _, ok := out[0]["id"]; !ok {
		t.Fatalf("expected the column that exists to survive, got %v", out[0])
	}
}

// T7. The warning existed and was hard-gated to null_handle, so the three
// transforms above ran silent. Table-driven so a type added to columnsReferenced
// without a warning path is visible.
func TestMissingColumnWarningsCoverEveryColumnNamingTransform(t *testing.T) {
	data := []Row{{"id": 1}}
	cases := []struct {
		name string
		rule Transform
	}{
		{"filter", Transform{Type: "filter", Config: map[string]interface{}{"condition": "gone = 1"}}},
		{"validate", Transform{Type: "validate", Config: map[string]interface{}{"required_columns": []interface{}{"gone"}}}},
		{"select_columns", Transform{Type: "select_columns", Config: map[string]interface{}{"columns": []interface{}{"gone"}}}},
		{"exclude_columns", Transform{Type: "exclude_columns", Config: map[string]interface{}{"columns": []interface{}{"gone"}}}},
		{"rename_columns", Transform{Type: "rename_columns", Config: map[string]interface{}{"from": "gone", "to": "here"}}},
		{"null_handle", Transform{Type: "null_handle", Config: map[string]interface{}{"column": "gone", "strategy": "drop_row"}}},
		{"truncate", Transform{Type: "truncate", Config: map[string]interface{}{"column": "gone", "max_length": 5}}},
		{"type_convert", Transform{Type: "type_convert", Config: map[string]interface{}{"column": "gone", "to": "string"}}},
		{"json_flatten", Transform{Type: "json_flatten", Config: map[string]interface{}{"column": "gone"}}},
		{"array_expand", Transform{Type: "array_expand", Config: map[string]interface{}{"column": "gone"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := MissingColumnWarnings(tc.rule, data)
			if len(got) != 1 {
				t.Fatalf("expected 1 warning naming the absent column, got %v", got)
			}
			if !strings.Contains(got[0], `"gone"`) {
				t.Fatalf("expected the warning to name the column, got %q", got[0])
			}
		})
	}
}

// The control. Without it, "warn on everything" passes every assertion above and
// buries the real warnings in noise.
func TestMissingColumnWarningsStaySilentWhenTheColumnIsThere(t *testing.T) {
	data := []Row{{"id": 1, "status": "active"}}
	cases := []Transform{
		{Type: "filter", Config: map[string]interface{}{"condition": "status = 'active'"}},
		{Type: "validate", Config: map[string]interface{}{"required_columns": []interface{}{"id", "status"}}},
		{Type: "select_columns", Config: map[string]interface{}{"columns": []interface{}{"id"}}},
		{Type: "null_handle", Config: map[string]interface{}{"column": "status", "strategy": "drop_row"}},
		// mask_pii is intentionally uncovered: nested/deep targeting means an
		// absent top-level name is not evidence of a mismatch.
		{Type: "mask_pii", Config: map[string]interface{}{"columns": []interface{}{"gone"}}},
	}
	for _, rule := range cases {
		if got := MissingColumnWarnings(rule, data); len(got) != 0 {
			t.Fatalf("%s: expected no warning, got %v", rule.Type, got)
		}
	}
	// And an empty input is not evidence of anything.
	if got := MissingColumnWarnings(cases[0], []Row{}); len(got) != 0 {
		t.Fatalf("expected no warning on empty input, got %v", got)
	}
}

// The end-to-end shape the user sees: a chain that renames a column and then
// filters on the old name. This is T7's report, and the reason the check runs
// against the rows ENTERING each step.
func TestAChainThatRenamesThenFiltersOnTheOldNameWarnsAndKeepsTheRows(t *testing.T) {
	data := []Row{{"id": 1, "status": "active"}, {"id": 2, "status": "closed"}}
	chain := []Transform{
		{Type: "rename_columns", Config: map[string]interface{}{"from": "status", "to": "state"}},
		{Type: "filter", Config: map[string]interface{}{"condition": "status = 'active'"}},
	}

	out, warnings, err := NewTransformCoordinator(NewSimpleTransformEngine(), nil).
		ApplyWithWarnings(context.Background(), data, chain)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("the chain deleted the batch: in=%d out=%d", len(data), len(out))
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], `"status"`) {
		t.Fatalf("expected one warning naming status, got %v", warnings)
	}
	if !strings.HasPrefix(warnings[0], "transform 1 (filter): ") {
		t.Fatalf("expected the warning numbered by its step, got %q", warnings[0])
	}
}
