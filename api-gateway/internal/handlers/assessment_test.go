package handlers

import (
	"strings"
	"testing"
)

// Helper: gather a slice of finding codes by severity for easy assertions.
func findingCodes(t AssessmentTable, sev AssessmentSeverity) []string {
	out := []string{}
	for _, f := range t.Findings {
		if f.Severity == sev {
			out = append(out, f.Code)
		}
	}
	return out
}

func TestAssessTable_DeclaredPKSinkSupportsDDL(t *testing.T) {
	// Happy path: shopify-like source with declared PK + sink supports auto-create.
	// Expectation: zero errors, zero warnings, only JSON-collapse info if applicable.
	meta := TableMetadata{
		Name:        "products",
		Schema:      "shopify",
		PrimaryKeys: []string{"id"},
		Columns: []ColumnMetadata{
			{Name: "id", Type: "string"},
			{Name: "title", Type: "string"},
			{Name: "priceRangeV2", Type: "json"},
			{Name: "variants", Type: "json"},
			{Name: "totalInventory", Type: "integer"},
		},
	}
	got := assessTable(meta, true, nil)

	if got.PrimaryKeySource != "declared" {
		t.Errorf("primary_key_source = %q, want declared", got.PrimaryKeySource)
	}
	if got.Mode != "upsert" {
		t.Errorf("mode = %q, want upsert", got.Mode)
	}
	if codes := findingCodes(got, AssessmentError); len(codes) != 0 {
		t.Errorf("unexpected errors: %v", codes)
	}
	if codes := findingCodes(got, AssessmentWarning); len(codes) != 0 {
		t.Errorf("unexpected warnings: %v", codes)
	}
	// JSON collapse for the 2 json columns
	if got.JSONColumnCount != 2 {
		t.Errorf("json_column_count = %d, want 2", got.JSONColumnCount)
	}
	if codes := findingCodes(got, AssessmentInfo); len(codes) != 1 || codes[0] != FindingJSONCollapse {
		t.Errorf("info codes = %v, want [JSON_COLLAPSE]", codes)
	}
}

func TestAssessTable_NoPKSinkSupportsDDL_WarningOnly(t *testing.T) {
	// PK-less source but sink can DDL → synthetic PK warning (not error).
	meta := TableMetadata{
		Name:        "events",
		PrimaryKeys: nil,
		Columns: []ColumnMetadata{
			{Name: "ts", Type: "timestamp"},
			{Name: "payload", Type: "string"},
		},
	}
	got := assessTable(meta, true, nil)

	if got.PrimaryKeySource != "synthetic" {
		t.Errorf("primary_key_source = %q, want synthetic", got.PrimaryKeySource)
	}
	if got.Mode != "upsert_synthetic" {
		t.Errorf("mode = %q, want upsert_synthetic", got.Mode)
	}
	warns := findingCodes(got, AssessmentWarning)
	if len(warns) != 1 || warns[0] != FindingNoPrimaryKey {
		t.Errorf("warning codes = %v, want [NO_PRIMARY_KEY]", warns)
	}
	if errs := findingCodes(got, AssessmentError); len(errs) != 0 {
		t.Errorf("unexpected errors when sink supports DDL: %v", errs)
	}
}

func TestAssessTable_NoPKNoDDL_WarnPlusSinkError(t *testing.T) {
	// PK-less source AND sink can't auto-create. Per the "warn, never block"
	// market-standard policy, NO_PRIMARY_KEY is a WARNING (the run can still
	// start and the sink fails loud rather than silently dropping rows), while
	// SINK_NO_DDL remains an ERROR (the destination genuinely can't be created).
	meta := TableMetadata{
		Name:        "events",
		PrimaryKeys: nil,
		Columns: []ColumnMetadata{
			{Name: "ts", Type: "timestamp"},
		},
	}
	got := assessTable(meta, false, nil)

	warns := findingCodes(got, AssessmentWarning)
	hasPKWarn := false
	for _, c := range warns {
		if c == FindingNoPrimaryKey {
			hasPKWarn = true
		}
	}
	if !hasPKWarn {
		t.Errorf("warning codes = %v, want NO_PRIMARY_KEY as a warning", warns)
	}
	errs := findingCodes(got, AssessmentError)
	hasDDL := false
	for _, c := range errs {
		if c == FindingSinkNoDDL {
			hasDDL = true
		}
		if c == FindingNoPrimaryKey {
			t.Errorf("NO_PRIMARY_KEY must be a warning, not an error (warn-never-block policy)")
		}
	}
	if !hasDDL {
		t.Errorf("error codes = %v, want SINK_NO_DDL", errs)
	}
}

func TestAssessTable_NominatedKeysSuppressesWarning(t *testing.T) {
	// PR-D: a keyless source with user-nominated key columns is treated as
	// keyed — INFO note, no warning, true upsert mode (not synthetic hash).
	meta := TableMetadata{
		Name:        "events",
		Schema:      "public",
		PrimaryKeys: nil,
		Columns: []ColumnMetadata{
			{Name: "tenant_id", Type: "string"},
			{Name: "event_id", Type: "string"},
			{Name: "payload", Type: "string"},
		},
	}
	got := assessTable(meta, true, []string{"tenant_id", "event_id"})

	if got.PrimaryKeySource != "nominated" {
		t.Errorf("primary_key_source = %q, want nominated", got.PrimaryKeySource)
	}
	if got.Mode != "upsert" {
		t.Errorf("mode = %q, want upsert", got.Mode)
	}
	if len(got.NominatedKeys) != 2 || got.NominatedKeys[0] != "tenant_id" || got.NominatedKeys[1] != "event_id" {
		t.Errorf("nominated_keys = %v, want [tenant_id event_id]", got.NominatedKeys)
	}
	if warns := findingCodes(got, AssessmentWarning); len(warns) != 0 {
		t.Errorf("unexpected warnings when keys nominated: %v", warns)
	}
	// The keyless finding is downgraded to INFO and names the nominated columns.
	infos := findingCodes(got, AssessmentInfo)
	hasNoPKInfo := false
	for _, c := range infos {
		if c == FindingNoPrimaryKey {
			hasNoPKInfo = true
		}
	}
	if !hasNoPKInfo {
		t.Errorf("info codes = %v, want a NO_PRIMARY_KEY note", infos)
	}
	// Columns are exposed so the modal can render the picker.
	if len(got.Columns) != 3 {
		t.Errorf("columns = %v, want 3 source columns", got.Columns)
	}
}

func TestAssessTable_JSONCollapseDetailsRecordColumnNames(t *testing.T) {
	meta := TableMetadata{
		Name:        "orders",
		PrimaryKeys: []string{"id"},
		Columns: []ColumnMetadata{
			{Name: "id", Type: "string"},
			{Name: "totalPriceSet", Type: "json"},
			{Name: "lineItems", Type: "json"},
			{Name: "shippingAddress", Type: "json"},
		},
	}
	got := assessTable(meta, true, nil)
	infos := got.Findings // we expect exactly one info
	if len(infos) != 1 || infos[0].Code != FindingJSONCollapse {
		t.Fatalf("findings = %v, want a single JSON_COLLAPSE info", infos)
	}
	cols, _ := infos[0].Details["columns"].([]string)
	if len(cols) != 3 {
		t.Errorf("json_collapse columns = %v, want 3 entries", cols)
	}
}

func TestHasBlockingFindings(t *testing.T) {
	clean := []AssessmentTable{{Findings: []AssessmentFinding{{Severity: AssessmentInfo, Code: "X"}}}}
	if hasBlockingFindings(clean) {
		t.Error("info-only assessment should not block")
	}
	warned := []AssessmentTable{{Findings: []AssessmentFinding{{Severity: AssessmentWarning, Code: "Y"}}}}
	if hasBlockingFindings(warned) {
		t.Error("warnings should not block (frontend handles them via ack)")
	}
	bad := []AssessmentTable{{Findings: []AssessmentFinding{{Severity: AssessmentError, Code: "Z"}}}}
	if !hasBlockingFindings(bad) {
		t.Error("errors must block")
	}
}

func TestSummarise(t *testing.T) {
	clean := []AssessmentTable{{Name: "a"}, {Name: "b"}}
	if got := summarise(clean); got != "All checks passed across 2 tables" {
		t.Errorf("clean summary = %q", got)
	}

	mixed := []AssessmentTable{
		{Name: "a", Findings: []AssessmentFinding{
			{Severity: AssessmentWarning, Code: "W"},
			{Severity: AssessmentInfo, Code: "I"},
		}},
		{Name: "b", Findings: []AssessmentFinding{
			{Severity: AssessmentError, Code: "E"},
		}},
	}
	got := summarise(mixed)
	// Don't assert exact string — pluralisation rules can drift. Just
	// make sure the counts are surfaced.
	for _, want := range []string{"1 error", "1 warning", "1 note", "2 tables"} {
		if !strings.Contains(got, want) {
			t.Errorf("summary %q missing %q", got, want)
		}
	}
}
