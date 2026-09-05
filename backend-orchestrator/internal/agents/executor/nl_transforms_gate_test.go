package executor

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func cfgStr(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

// findTransform returns the first config whose operation+column match.
func findTransform(configs []map[string]any, op, col string) map[string]any {
	for _, c := range configs {
		if cfgStr(c, "operation") == op && cfgStr(c, "column") == col {
			return c
		}
	}
	return nil
}

func TestBuildNLTransforms_ExplicitMaskColumn(t *testing.T) {
	got := buildNLTransforms(&nlTransformIntent{MaskColumns: []string{"email"}}, nil)
	m := findTransform(got, "mask", "email")
	if m == nil {
		t.Fatalf("expected a mask on 'email', got %#v", got)
	}
	if cfgStr(m, "mask_type") != "hash" || cfgStr(m, "hash_function") != "sha256" {
		t.Fatalf("expected sha256 hash mask, got %#v", m)
	}
	if cfgStr(m, "pii_type") != "email" {
		t.Fatalf("expected pii_type=email, got %q", cfgStr(m, "pii_type"))
	}
}

func TestBuildNLTransforms_ExplicitMaskCanonicalizesCasing(t *testing.T) {
	cols := []ColumnMetadata{{Name: "Email", Type: "varchar"}}
	got := buildNLTransforms(&nlTransformIntent{MaskColumns: []string{"EMAIL"}}, cols)
	// Must emit the canonical "Email", not "EMAIL", so the engine's exact-key
	// match actually fires.
	if findTransform(got, "mask", "Email") == nil {
		t.Fatalf("expected mask on canonical 'Email', got %#v", got)
	}
}

func TestBuildNLTransforms_GenericPIIMasksOnlyHighConfidence(t *testing.T) {
	cols := []ColumnMetadata{
		{Name: "email", Type: "varchar"},
		{Name: "phone_number", Type: "varchar"},
		{Name: "full_name", Type: "varchar"},
		{Name: "id", Type: "int"},
	}
	got := buildNLTransforms(&nlTransformIntent{MaskPII: true}, cols)
	if findTransform(got, "mask", "email") == nil {
		t.Fatalf("expected email masked")
	}
	if findTransform(got, "mask", "phone_number") == nil {
		t.Fatalf("expected phone masked")
	}
	if findTransform(got, "mask", "full_name") != nil {
		t.Fatalf("person name must NOT be auto-masked by generic PII")
	}
	if findTransform(got, "mask", "id") != nil {
		t.Fatalf("id must NOT be masked")
	}
}

func TestBuildNLTransforms_TypeConvertHeuristic(t *testing.T) {
	cols := []ColumnMetadata{
		{Name: "price", Type: "varchar"},
		{Name: "quantity", Type: "text"},
		{Name: "is_active", Type: "varchar"},
		{Name: "name", Type: "varchar"},        // no recommendation → stays string
		{Name: "order_id", Type: "varchar"},    // *_id excluded
		{Name: "total_cents", Type: "integer"}, // already numeric → skipped (not string)
	}
	got := buildNLTransforms(&nlTransformIntent{TypeConvert: true}, cols)

	if c := findTransform(got, "type_convert", "price"); c == nil || cfgStr(c, "to") != "float" {
		t.Fatalf("expected price→float, got %#v", got)
	}
	if c := findTransform(got, "type_convert", "quantity"); c == nil || cfgStr(c, "to") != "int" {
		t.Fatalf("expected quantity→int, got %#v", got)
	}
	if c := findTransform(got, "type_convert", "is_active"); c == nil || cfgStr(c, "to") != "bool" {
		t.Fatalf("expected is_active→bool, got %#v", got)
	}
	if findTransform(got, "type_convert", "name") != nil {
		t.Fatalf("plain 'name' must not be converted")
	}
	if findTransform(got, "type_convert", "order_id") != nil {
		t.Fatalf("*_id must not be converted")
	}
	if findTransform(got, "type_convert", "total_cents") != nil {
		t.Fatalf("already-numeric column must not be converted")
	}
	for _, c := range got {
		if cfgStr(c, "operation") == "type_convert" && cfgStr(c, "on_error") != "skip" {
			t.Fatalf("type_convert must set on_error=skip (non-destructive), got %#v", c)
		}
	}
}

func TestBuildNLTransforms_MaskWinsOverTypeConvert(t *testing.T) {
	// A column that is both masked and would otherwise convert must only be masked
	// (the mask output is a string digest).
	cols := []ColumnMetadata{{Name: "account_balance", Type: "varchar"}}
	got := buildNLTransforms(&nlTransformIntent{
		MaskColumns: []string{"account_balance"},
		TypeConvert: true,
	}, cols)
	if findTransform(got, "mask", "account_balance") == nil {
		t.Fatalf("expected mask on account_balance")
	}
	if findTransform(got, "type_convert", "account_balance") != nil {
		t.Fatalf("must not type-convert a masked column")
	}
}

func TestPiiTypeForColumn(t *testing.T) {
	cases := map[string]string{
		"email":          "email",
		"user_email":     "email",
		"phone":          "phone",
		"mobile_number":  "phone",
		"ssn":            "ssn",
		"credit_card_no": "credit_card",
		"passport_num":   "passport",
		"tax_id":         "national_id",
		"first_name":     "",
		"created_at":     "",
	}
	for in, want := range cases {
		if got := piiTypeForColumn(in); got != want {
			t.Errorf("piiTypeForColumn(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestRecommendType(t *testing.T) {
	cases := map[string]string{
		"price":       "float",
		"unit_amount": "float",
		"quantity":    "int",
		"line_count":  "int",
		"is_active":   "bool",
		"has_shipped": "bool",
		"deleted":     "bool",
		"name":        "",
		"description": "",
		"order_id":    "",
		"id":          "",
		// whole-token matching guards (these previously misfired via substring):
		"corporate_name": "",      // "corporate" contains "rate"
		"total_notes":    "",      // has a "total" token but is a text field
		"separate_field": "",      // "separate" contains "rate"
		"order_total":    "float", // genuine numeric token still converts
		"zip_code":       "",      // "code" text signal (leading zeros must survive)
	}
	for in, want := range cases {
		if got := recommendType(in); got != want {
			t.Errorf("recommendType(%q)=%q, want %q", in, got, want)
		}
	}
}

func TestIsStringType(t *testing.T) {
	for _, s := range []string{"varchar", "varchar(255)", "text", "character varying", "nvarchar", "citext", "CHAR(10)"} {
		if !isStringType(s) {
			t.Errorf("isStringType(%q) = false, want true", s)
		}
	}
	for _, s := range []string{"integer", "numeric", "bigint", "boolean", "timestamp", "double precision", ""} {
		if isStringType(s) {
			t.Errorf("isStringType(%q) = true, want false", s)
		}
	}
}

// --- DB-path tests (sqlmock) -------------------------------------------------

const gateUUID = "33333333-3333-3333-3333-333333333333"

func TestPlanNLTransforms_SkipsWhenTransformsAlreadyExist(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT config->'nl_transforms' FROM pipelines").
		WithArgs(gateUUID).
		WillReturnRows(sqlmock.NewRows([]string{"nl_transforms"}).
			AddRow([]byte(`{"mask_columns":["email"]}`)))
	// Already has a transform → gate must NOT discover schema or insert.
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM transform_definitions").
		WithArgs(gateUUID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	a := &Agent{db: db}
	if err := a.planNLTransforms(context.Background(), ExecutorTask{PipelineID: gateUUID}); err != nil {
		t.Fatalf("planNLTransforms: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected DB interaction: %v", err)
	}
}

func TestPlanNLTransforms_NoIntentIsNoOp(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT config->'nl_transforms' FROM pipelines").
		WithArgs(gateUUID).
		WillReturnRows(sqlmock.NewRows([]string{"nl_transforms"}).AddRow([]byte("null")))

	a := &Agent{db: db}
	if err := a.planNLTransforms(context.Background(), ExecutorTask{PipelineID: gateUUID}); err != nil {
		t.Fatalf("planNLTransforms: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected DB interaction (should stop after empty intent): %v", err)
	}
}

func TestPlanNLTransforms_FailClosedOnLoadError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT config->'nl_transforms' FROM pipelines").
		WithArgs(gateUUID).
		WillReturnError(context.DeadlineExceeded)

	a := &Agent{db: db}
	if err := a.planNLTransforms(context.Background(), ExecutorTask{PipelineID: gateUUID}); err == nil {
		t.Fatalf("expected fail-closed error when intent load fails, got nil")
	}
}

// TestPlanNLTransforms_FailsClosedWhenMaskColumnsUnverifiable locks the F6-GAP
// fix (KI-NLCHAT-MASK-SILENT-NOOP). When an explicit mask column is requested but
// the source schema cannot be verified — discovery errors OR returns no columns
// (discoverSourceColumns can return (nil,nil)) — the gate must FAIL CLOSED, never
// fall through and write a literal-name mask the engine silently no-ops on (which
// would land the column UNMASKED with no warning). Here task.Source is nil so
// discovery errors; the gate must abort with a masking error BEFORE persisting any
// transform (no INSERT is mocked, so a fall-through would surface as a persist
// error instead).
func TestPlanNLTransforms_FailsClosedWhenMaskColumnsUnverifiable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT config->'nl_transforms' FROM pipelines").
		WithArgs(gateUUID).
		WillReturnRows(sqlmock.NewRows([]string{"nl_transforms"}).
			AddRow([]byte(`{"mask_columns":["email"]}`)))
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM transform_definitions").
		WithArgs(gateUUID).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	// No INSERT expectation: the gate must fail closed before persisting.

	a := &Agent{db: db}
	err = a.planNLTransforms(context.Background(), ExecutorTask{PipelineID: gateUUID})
	if err == nil {
		t.Fatalf("expected fail-closed error when mask columns cannot be verified, got nil")
	}
	if !strings.Contains(err.Error(), "masking") {
		t.Fatalf("expected a fail-closed masking error (not a persist fall-through), got: %v", err)
	}
}

func TestPlanNLTransforms_NonUUIDSkips(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	a := &Agent{db: db}
	if err := a.planNLTransforms(context.Background(), ExecutorTask{PipelineID: "adhoc-run"}); err != nil {
		t.Fatalf("planNLTransforms: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("no DB query expected for non-UUID pipeline: %v", err)
	}
}

// TestMaskGuardError locks the F6-GAP fail-closed decision (KI-NLCHAT-MASK-SILENT-NOOP):
// masking that cannot be verified against the discovered schema must error, while
// type-conversion-only intents and fully-verifiable masks must not.
func TestMaskGuardError(t *testing.T) {
	cols := []ColumnMetadata{{Name: "id"}, {Name: "Email"}}
	cases := []struct {
		name    string
		intent  *nlTransformIntent
		cols    []ColumnMetadata
		wantErr bool
	}{
		{"no masking requested (type-convert only)", &nlTransformIntent{TypeConvert: true}, nil, false},
		{"nil intent", nil, cols, false},
		{"explicit mask present", &nlTransformIntent{MaskColumns: []string{"email"}}, cols, false},
		{"explicit mask + empty cols fails closed", &nlTransformIntent{MaskColumns: []string{"email"}}, nil, true},
		{"explicit mask + zero-length cols fails closed", &nlTransformIntent{MaskColumns: []string{"email"}}, []ColumnMetadata{}, true},
		{"mask_pii + empty cols fails closed", &nlTransformIntent{MaskPII: true}, nil, true},
		{"explicit mask absent from cols fails closed", &nlTransformIntent{MaskColumns: []string{"ssn"}}, cols, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := maskGuardError(c.intent, c.cols)
			if c.wantErr && err == nil {
				t.Fatalf("expected fail-closed error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Fatalf("expected nil, got: %v", err)
			}
			if c.wantErr && !strings.Contains(err.Error(), "KI-NLCHAT-MASK-SILENT-NOOP") {
				t.Errorf("error should cite the KI marker, got: %v", err)
			}
		})
	}
}

// TestMissingMaskColumns locks KI-NLCHAT-MASK-SILENT-NOOP: an explicitly-named
// mask column that is absent from the discovered source schema must be reported
// as missing (so planNLTransforms fails closed) — case-insensitive, and never a
// false positive for a present column.
func TestMissingMaskColumns(t *testing.T) {
	cols := []ColumnMetadata{{Name: "id"}, {Name: "Email"}, {Name: "full_name"}}
	cases := []struct {
		name        string
		maskCols    []string
		wantMissing []string
	}{
		{"present exact", []string{"full_name"}, nil},
		{"present case-insensitive", []string{"email"}, nil},
		{"absent column reported", []string{"emailx"}, []string{"emailx"}},
		{"mix present + absent", []string{"Email", "ssn", "phone"}, []string{"ssn", "phone"}},
		{"blank + whitespace ignored", []string{"  ", ""}, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := missingMaskColumns(c.maskCols, cols)
			if len(got) != len(c.wantMissing) {
				t.Fatalf("missingMaskColumns(%v) = %v, want %v", c.maskCols, got, c.wantMissing)
			}
			for i := range got {
				if got[i] != c.wantMissing[i] {
					t.Errorf("missing[%d] = %q, want %q", i, got[i], c.wantMissing[i])
				}
			}
		})
	}
}
