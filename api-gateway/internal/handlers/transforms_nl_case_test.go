package handlers

import (
	"strings"
	"testing"
)

// The NL transform extractor used to lowercase the whole request before pulling
// the condition out of it, so a filter literal came back case-folded. The rule
// still looked right in the UI -- `status = 'active'` reads as what the user
// asked for -- but a case-sensitive destination matched nothing, and the
// pipeline reported a clean run over zero rows.

func TestNLFilterKeepsTheLiteralsCase(t *testing.T) {
	defs := parseTransformRequest("filter where status = 'Active'", nil)
	if len(defs) != 1 {
		t.Fatalf("expected one transform, got %d", len(defs))
	}
	cond, _ := defs[0].TransformConfig["condition"].(string)
	if !strings.Contains(cond, "'Active'") {
		t.Fatalf("the literal was case-folded: condition = %q, want it to keep 'Active'", cond)
	}
}

func TestNLFilterMatchesAnUppercaseKeyword(t *testing.T) {
	// The keyword match must stay case-insensitive even though the text is no
	// longer lowercased wholesale.
	defs := parseTransformRequest("Filter WHERE Region = 'EMEA'", nil)
	if len(defs) != 1 {
		t.Fatalf("expected one transform, got %d", len(defs))
	}
	cond, _ := defs[0].TransformConfig["condition"].(string)
	if !strings.Contains(cond, "'EMEA'") || !strings.Contains(cond, "Region") {
		t.Fatalf("uppercase keyword broke extraction: condition = %q", cond)
	}
}

func TestNLMaskKeywordStillMatchesRegardlessOfCase(t *testing.T) {
	// Control: detection must not have become case-sensitive as a side effect.
	lower := parseTransformRequest("mask the email column", []PIIColumnInfo{
		{TableName: "users", ColumnName: "email", PIIType: "email", Confidence: 0.9},
	})
	upper := parseTransformRequest("MASK the email column", []PIIColumnInfo{
		{TableName: "users", ColumnName: "email", PIIType: "email", Confidence: 0.9},
	})
	if len(lower) != len(upper) {
		t.Fatalf("case changed how many rules were detected: %d vs %d", len(lower), len(upper))
	}
	if len(lower) == 0 {
		t.Fatal("no mask rule detected at all; the control proves nothing")
	}
}

func TestIndexFoldReturnsAnIndexIntoTheOriginal(t *testing.T) {
	s := "Only Status = 'Ok'"
	i := indexFold(s, "only ")
	if i != 0 {
		t.Fatalf("indexFold = %d, want 0", i)
	}
	if got := s[i+len("only "):]; got != "Status = 'Ok'" {
		t.Fatalf("slicing the original gave %q", got)
	}
	if indexFold(s, "where ") != -1 {
		t.Fatal("indexFold matched a substring that is not there")
	}
}
