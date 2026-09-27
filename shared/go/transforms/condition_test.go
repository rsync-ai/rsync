package transforms

import (
	"context"
	"strings"
	"testing"
)

// TestFilterEvaluatesThePlaceholderTheUIShowsTheUser is the whole of T2 in one
// assertion. "amount > 100 AND status = 'active'" is not an example invented
// here: it is the placeholder text of the Filter Condition field itself
// (frontend/src/app/(dashboard)/transforms/page.tsx). The engine's single greedy
// regex read it as `amount > "100 AND status = 'active"`, matched nothing, and
// emptied every batch with no error — the product advertised a syntax whose only
// effect was silent data loss.
func TestFilterEvaluatesThePlaceholderTheUIShowsTheUser(t *testing.T) {
	data := []Row{
		{"id": 1, "amount": 150, "status": "active"},
		{"id": 2, "amount": 50, "status": "active"},
		{"id": 3, "amount": 150, "status": "closed"},
	}
	out, err := NewSimpleTransformEngine().Apply(context.Background(), data, Transform{
		Type:   "filter",
		Config: map[string]interface{}{"condition": "amount > 100 AND status = 'active'"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 1 || out[0]["id"] != 1 {
		t.Fatalf("expected only row id=1, got %v", out)
	}
}

func TestConditionOperators(t *testing.T) {
	row := Row{"amount": 150, "status": "active", "name": "ada"}
	cases := []struct {
		cond string
		want bool
	}{
		{"amount > 100 AND status = 'active'", true},
		{"amount > 200 AND status = 'active'", false},
		{"amount > 200 OR status = 'active'", true},
		{"amount > 200 OR status = 'closed'", false},
		// AND binds tighter than OR, as in SQL.
		{"amount > 200 AND status = 'closed' OR name = 'ada'", true},
		{"name = 'ada' OR amount > 200 AND status = 'closed'", true},
		{"amount > 200 OR name = 'bob' AND status = 'active'", false},
		// Case-insensitive keywords.
		{"amount > 100 and status = 'active'", true},
		{"amount > 200 or status = 'active'", true},
		// A keyword inside a literal is not a separator.
		{"status = 'active'", true},
		{"name != 'bob'", true},
	}
	for _, tc := range cases {
		t.Run(tc.cond, func(t *testing.T) {
			got, err := evaluateCondition(row, tc.cond)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("evaluateCondition(%q) = %v, want %v", tc.cond, got, tc.want)
			}
		})
	}
}

// A keyword that is part of a value or an identifier must not split the
// condition. Without the quote tracking, 'read AND write' becomes two operands
// and the filter quietly means something else.
func TestKeywordsInsideLiteralsAreNotSeparators(t *testing.T) {
	row := Row{"perms": "read AND write", "brand": "x"}

	got, err := evaluateCondition(row, "perms = 'read AND write'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatalf("a quoted AND was treated as a conjunction")
	}

	if cols := conditionColumns("perms = 'read OR write'"); len(cols) != 1 || cols[0] != "perms" {
		t.Fatalf("expected one column perms, got %v", cols)
	}
}

// Parentheses are refused rather than silently regrouped: "a = 1 AND (b = 2 OR
// c = 3)" flattened into DNF by accident would answer a different question and
// look right doing it.
func TestParenthesesAreRefusedNotMisread(t *testing.T) {
	_, err := NewSimpleTransformEngine().Apply(context.Background(), []Row{{"a": 1, "b": 9}}, Transform{
		Type:   "filter",
		Config: map[string]interface{}{"condition": "a = 1 AND (b = 2 OR b = 3)"},
	})
	if err == nil {
		t.Fatalf("expected a parenthesised condition to be refused")
	}
	if !strings.Contains(err.Error(), "parenthesised") {
		t.Fatalf("expected the error to say why, got: %v", err)
	}
}

// An unparseable OPERAND inside a compound condition must still fail loudly —
// the pre-existing guarantee, now that a compound condition parses at all.
func TestAnUnparseableOperandInACompoundConditionStillFailsLoud(t *testing.T) {
	_, err := NewSimpleTransformEngine().Apply(context.Background(), []Row{{"a": 1, "b": 2}}, Transform{
		Type:   "filter",
		Config: map[string]interface{}{"condition": "a = 1 AND b BETWEEN 1 AND 5"},
	})
	if err == nil {
		t.Fatalf("expected an unparseable operand to fail loudly")
	}
}

// T12. A LIKE pattern is not a regex. Interpolating it raw made every
// metacharacter live.
func TestLikePatternsAreNotRegexes(t *testing.T) {
	cases := []struct {
		pattern string
		value   string
		want    bool
	}{
		{"a.b", "a.b", true},
		{"a.b", "axb", false}, // '.' was a wildcard
		{"a%", "abc", true},   // '%' still is one
		{"a_c", "abc", true},  // '_' still is one
		{"a_c", "ac", false},
		{"100%", "100%", true},
		{"c++", "c++", true}, // '+' was a repeat operator
		{"c++", "c", false},
	}
	for _, tc := range cases {
		t.Run(tc.pattern+"/"+tc.value, func(t *testing.T) {
			got, err := evaluateCondition(Row{"v": tc.value}, "v LIKE '"+tc.pattern+"'")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("%q LIKE %q = %v, want %v", tc.value, tc.pattern, got, tc.want)
			}
		})
	}
}

// A pattern that is not a valid regex used to fail the whole filter with
// "invalid LIKE pattern", which took the pipeline down for a literal "(".
func TestALikePatternWithRegexSyntaxNoLongerErrors(t *testing.T) {
	got, err := evaluateCondition(Row{"v": "func("}, `v LIKE 'func('`)
	if err != nil {
		t.Fatalf("a literal '(' in a LIKE pattern must not be a regex syntax error: %v", err)
	}
	if !got {
		t.Fatalf("expected the literal to match itself")
	}
}

// T11. float64 is exact only below 2^53; above it adjacent int64s collapse onto
// the same float, so the filter matched the WRONG row and reported success.
func TestLargeIntegerIdsCompareExactly(t *testing.T) {
	const near = int64(9007199254740993) // 2^53 + 1
	const below = int64(9007199254740992)

	got, err := evaluateCondition(Row{"id": near}, "id = 9007199254740992")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Fatalf("id=%d matched %d: the comparison went through float64", near, below)
	}

	got, err = evaluateCondition(Row{"id": near}, "id = 9007199254740993")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatalf("id=%d did not match itself", near)
	}

	// Ordering across the boundary must stay right too.
	got, err = evaluateCondition(Row{"id": near}, "id > 9007199254740992")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Fatalf("expected %d > %d", near, below)
	}
}

// The control for the integer path: decimals, and numbers arriving as strings
// from a DB driver, must still compare numerically rather than falling back to
// a lexicographic compare.
func TestDecimalAndStringNumbersStillCompareNumerically(t *testing.T) {
	cases := []struct {
		row  Row
		cond string
		want bool
	}{
		{Row{"price": 10.5}, "price > 10", true},
		{Row{"price": "10.50"}, "price > 10", true},
		{Row{"price": "9"}, "price > 10", false},
		// "9" > "10" lexicographically, so this is the one that catches a
		// regression to string comparison.
		{Row{"price": 9}, "price > 10", false},
		{Row{"price": "0100"}, "price = 100", true},
	}
	for _, tc := range cases {
		got, err := evaluateCondition(tc.row, tc.cond)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != tc.want {
			t.Fatalf("%v %q = %v, want %v", tc.row, tc.cond, got, tc.want)
		}
	}
}

// conditionColumns is what lets applyFilter ask, before dropping a row, whether
// the batch carries the columns at all. A compound condition must report every
// one of them, or the absent-column guard only sees the first.
func TestConditionColumnsNamesEveryColumnInACompoundCondition(t *testing.T) {
	got := conditionColumns("amount > 100 AND status = 'active' OR name LIKE 'a%'")
	want := []string{"amount", "status", "name"}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}
