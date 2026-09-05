package transforms

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// TestEvaluateCondition exercises every supported operator, the numeric-vs-string
// comparison rules, and the fail-loud path. Before the fix this predicate
// evaluator had 0% coverage and only handled >, =, LIKE.
func TestEvaluateCondition(t *testing.T) {
	tests := []struct {
		name      string
		condition string
		row       Row
		want      bool
		wantErr   bool
	}{
		// equality
		{"eq string match", "status = 'active'", Row{"status": "active"}, true, false},
		{"eq string no match", "status = 'active'", Row{"status": "closed"}, false, false},
		{"eq numeric", "age = 30", Row{"age": 30}, true, false},
		{"eq int vs float value", "age = 30", Row{"age": 30.0}, true, false},

		// not-equals (both spellings) — previously unsupported (silent drop)
		{"neq bang match", "status != 'active'", Row{"status": "closed"}, true, false},
		{"neq angle match", "status <> 'active'", Row{"status": "closed"}, true, false},
		{"neq numeric", "age != 30", Row{"age": 31}, true, false},

		// ordering — <, <=, >= were previously unsupported or misparsed
		{"gt true", "age > 18", Row{"age": 21}, true, false},
		{"gt false", "age > 18", Row{"age": 10}, false, false},
		{"lt true (was silently dropped)", "age < 18", Row{"age": 10}, true, false},
		{"lte boundary", "age <= 18", Row{"age": 18}, true, false},
		{"gte boundary (was misparsed to >0)", "age >= 100", Row{"age": 100}, true, false},
		{"gte below (misparse guard)", "amount >= 100", Row{"amount": 50}, false, false},

		// decimal-as-string column (common from DB drivers): must compare
		// numerically, not lexicographically ("900" < "1000" numerically, but
		// "900" > "1000" as strings).
		{"gt decimal-as-string true", "amount > 1000", Row{"amount": "1234.56"}, true, false},
		{"gt decimal-as-string false", "amount > 1000", Row{"amount": "900"}, false, false},
		{"lt decimal-as-string", "amount < 1000", Row{"amount": "900"}, true, false},
		{"gt json.Number", "n > 5", Row{"n": json.Number("42")}, true, false},

		// non-numeric ordering falls back to lexicographic
		{"gt string lexicographic", "name > 'm'", Row{"name": "z"}, true, false},

		// LIKE
		{"like prefix", "email LIKE 'a%'", Row{"email": "alice@x.com"}, true, false},
		{"like no match", "email LIKE 'b%'", Row{"email": "alice@x.com"}, false, false},
		{"like lowercase keyword", "email like 'a%'", Row{"email": "alice@x.com"}, true, false},
		{"like underscore", "code LIKE 'A_C'", Row{"code": "ABC"}, true, false},

		// missing column → non-match, not an error
		{"missing column", "ghost > 5", Row{"age": 10}, false, false},
		// empty condition → keep row
		{"empty condition", "", Row{"age": 10}, true, false},

		// fail-loud: unrecognized formats must error, not silently drop
		{"unsupported BETWEEN", "age BETWEEN 1 AND 5", Row{"age": 3}, false, true},
		{"natural-language phrase", "age greater than 30", Row{"age": 40}, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evaluateCondition(tt.row, tt.condition)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("condition %q: expected error, got none (result=%v)", tt.condition, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("condition %q: unexpected error: %v", tt.condition, err)
			}
			if got != tt.want {
				t.Fatalf("condition %q on %v = %v, want %v", tt.condition, tt.row, got, tt.want)
			}
		})
	}
}

// TestApplyFilter_FailsLoudOnUnparseableCondition is the P0 regression guard:
// an unparseable condition used to error on every row, get swallowed, and
// discard the whole dataset while reporting success. It must now error.
func TestApplyFilter_FailsLoudOnUnparseableCondition(t *testing.T) {
	e := NewSimpleTransformEngine()
	data := []Row{{"age": 10}, {"age": 20}}
	out, err := e.Apply(context.Background(), data, Transform{
		Type:   "filter",
		Config: map[string]interface{}{"condition": "age BETWEEN 1 AND 5"},
	})
	if err == nil {
		t.Fatalf("expected filter to fail loudly on an unparseable condition; got nil error and %d rows (silent data-loss regression)", len(out))
	}
	if !strings.Contains(err.Error(), "could not be evaluated") {
		t.Fatalf("expected wrapped fail-loud error, got: %v", err)
	}
}

// TestApplyFilter_LessThanNoLongerDropsAllRows guards the specific bug: "<"
// (and its siblings) used to hit the swallowed-error path and drop everything.
func TestApplyFilter_LessThanNoLongerDropsAllRows(t *testing.T) {
	e := NewSimpleTransformEngine()
	data := []Row{{"age": 10}, {"age": 25}, {"age": 40}}
	out, err := e.Apply(context.Background(), data, Transform{
		Type:   "filter",
		Config: map[string]interface{}{"condition": "age < 30"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows kept (10, 25), got %d: %v", len(out), out)
	}
}

func TestApplyFilter_KeepsMatchingRows(t *testing.T) {
	e := NewSimpleTransformEngine()
	data := []Row{{"age": 10}, {"age": 25}, {"age": 40}}
	out, err := e.Apply(context.Background(), data, Transform{
		Type:   "filter",
		Config: map[string]interface{}{"condition": "age >= 25"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 rows (25, 40), got %d: %v", len(out), out)
	}
}

func TestApplyFilter_NoConditionKeepsAll(t *testing.T) {
	e := NewSimpleTransformEngine()
	data := []Row{{"age": 10}, {"age": 25}}
	out, err := e.Apply(context.Background(), data, Transform{
		Type:   "filter",
		Config: map[string]interface{}{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected all rows kept, got %d", len(out))
	}
}
