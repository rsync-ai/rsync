package handlers

import (
	"path/filepath"
	"regexp"
	"runtime"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
)

// Regression guard for KI-CONN-TEST-DRAFT.
//
// A brand-new connector_type has zero pipeline executions, so the executions
// aggregate in computeLifecycleUncached returns all-zeros and the function
// falls back to connection-test evidence. CreateConnection now persists a
// passing pre-save test as connections.last_test_status='success', so that
// fallback must count it and return "preview" (not "draft"). If it returned
// "draft", the user's first pipeline create/run would be blocked with
// 422 draft_connector_blocked even though they just tested successfully.
func TestComputeLifecycle_SavedSuccessfulTestLiftsDraft(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	prev := db.DB
	db.DB = mockDB
	defer func() {
		db.DB = prev
		_ = mockDB.Close()
	}()

	// 1) Executions aggregate: no successes yet (brand-new connector_type).
	mock.ExpectQuery(regexp.QuoteMeta(`WITH connector_execs AS`)).
		WithArgs("acme").
		WillReturnRows(sqlmock.NewRows(
			[]string{"total_success", "recent_failures", "distinct_success_pipelines"}).
			AddRow(0, 0, 0))

	// 2) Fallback: exactly one saved connection passed its connectivity test.
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT COUNT(*) FROM connections WHERE connector_type = $1 AND last_test_status = 'success'`)).
		WithArgs("acme").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	if got := computeLifecycleUncached("acme", "latest"); got != "preview" {
		t.Fatalf("lifecycle: want preview (a saved successful test must lift draft), got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// Counterpart: with zero executions AND zero saved successful tests, the
// connector must stay "draft" (fail-closed). Proves the fallback only promotes
// on real test evidence, not unconditionally.
func TestComputeLifecycle_NoEvidenceStaysDraft(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	prev := db.DB
	db.DB = mockDB
	defer func() {
		db.DB = prev
		_ = mockDB.Close()
	}()

	mock.ExpectQuery(regexp.QuoteMeta(`WITH connector_execs AS`)).
		WithArgs("acme").
		WillReturnRows(sqlmock.NewRows(
			[]string{"total_success", "recent_failures", "distinct_success_pipelines"}).
			AddRow(0, 0, 0))
	mock.ExpectQuery(regexp.QuoteMeta(
		`SELECT COUNT(*) FROM connections WHERE connector_type = $1 AND last_test_status = 'success'`)).
		WithArgs("acme").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	if got := computeLifecycleUncached("acme", "latest"); got != "draft" {
		t.Fatalf("lifecycle: want draft (no execution or test evidence), got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock expectations: %v", err)
	}
}

// applyVerifiedFloor lifts a durable vendor-verified connector from the
// no-evidence "draft" state to "preview" (→ catalog "Tested" chip) WITHOUT ever
// downgrading real runtime evidence or masking a failure. This guards two
// invariants at once:
//   - a validated connector (production_verified=true) with zero local rows
//     reads "Tested" on a fresh prod DB / after CI clean-checkout, and
//   - an unverified connector (e.g. Redshift/Snowflake, unit-only) still reads
//     "New" — the badge never lies.
func TestApplyVerifiedFloor(t *testing.T) {
	cases := []struct {
		name      string
		lifecycle string
		verified  bool
		want      string
	}{
		{"draft+verified lifts to preview", "draft", true, "preview"},
		{"draft+unverified stays draft", "draft", false, "draft"},
		{"preview+verified unchanged", "preview", true, "preview"},
		{"beta+verified not downgraded", "beta", true, "beta"},
		{"ga+verified not downgraded", "ga", true, "ga"},
		{"preview+unverified unchanged", "preview", false, "preview"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := applyVerifiedFloor(tc.lifecycle, tc.verified); got != tc.want {
				t.Fatalf("applyVerifiedFloor(%q, %v) = %q, want %q", tc.lifecycle, tc.verified, got, tc.want)
			}
		})
	}
}

// connectorProductionVerified must read the durable `production_verified`
// attestation from each connector's real versioned metadata.json. This wires the
// whole chain end-to-end: the flag we ship in metadata → the connector index →
// the lifecycle floor. It guards against the flag landing in the wrong file, a
// key typo, or an id/alias mismatch that would silently leave a validated
// connector reading "New" on prod. Verified connectors (validated against real
// data on staging) must return true; unit-only ones (Snowflake, aws-s3) must
// return false — the badge never lies.
func TestConnectorProductionVerified_ReadsRealMetadata(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// api-gateway/internal/handlers/<this> → repo root is three levels up.
	publicDir := filepath.Join(filepath.Dir(thisFile), "..", "..", "..", "shared", "mcp-connectors", "public")
	t.Setenv("MCP_PUBLIC_CONNECTORS_PATH", publicDir)

	verified := []string{"databricks", "bigquery", "mongodb", "sqlserver", "azure-blob"}
	for _, name := range verified {
		if !connectorProductionVerified(name) {
			t.Errorf("connectorProductionVerified(%q) = false, want true (validated connector must read Tested on a fresh prod DB)", name)
		}
	}

	// Unit-only / not-real-data-validated connectors must NOT be floored — the
	// chip stays honest.
	unverified := []string{"snowflake", "aws-s3", "definitely-not-a-real-connector"}
	for _, name := range unverified {
		if connectorProductionVerified(name) {
			t.Errorf("connectorProductionVerified(%q) = true, want false (no attestation → badge must stay New)", name)
		}
	}
}
