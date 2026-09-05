package assessor

import (
	"context"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// fakeAssessor lets us test Registry + Summarize without touching a DB.
type fakeAssessor struct {
	st     string
	result *Result
}

func (f *fakeAssessor) SourceType() string                                  { return f.st }
func (f *fakeAssessor) Assess(_ context.Context, _ Input) (*Result, error) { return f.result, nil }

func TestRegistry_RegisterAndResolve(t *testing.T) {
	r := NewRegistry()
	pg := &fakeAssessor{st: "postgresql"}
	r.Register(pg)

	got, ok := r.Resolve("postgresql")
	if !ok {
		t.Fatal("expected to resolve postgresql")
	}
	if got != pg {
		t.Errorf("resolved wrong assessor")
	}

	if _, ok := r.Resolve("nonexistent"); ok {
		t.Error("expected unknown type to return false")
	}
}

func TestRegistry_DuplicateRegistrationPanics(t *testing.T) {
	r := NewRegistry()
	r.Register(&fakeAssessor{st: "postgresql"})
	defer func() {
		if recover() == nil {
			t.Error("expected panic on duplicate registration")
		}
	}()
	r.Register(&fakeAssessor{st: "postgresql"})
}

func TestRegistry_EmptySourceTypePanics(t *testing.T) {
	r := NewRegistry()
	defer func() {
		if recover() == nil {
			t.Error("expected panic on empty SourceType")
		}
	}()
	r.Register(&fakeAssessor{st: ""})
}

func TestSummarize_AllPassed(t *testing.T) {
	r := &Result{
		Checks: []Check{
			{Code: "A", Severity: SeverityInfo, Passed: true},
			{Code: "B", Severity: SeverityInfo, Passed: true},
		},
	}
	Summarize(r)
	if r.PassedCount != 2 || r.OverallStatus != "passed" {
		t.Errorf("got passed=%d status=%s", r.PassedCount, r.OverallStatus)
	}
	if r.BlocksStart() {
		t.Error("all-passed result should not block start")
	}
}

func TestSummarize_WarningOnly(t *testing.T) {
	r := &Result{
		Checks: []Check{
			{Code: "A", Severity: SeverityInfo, Passed: true},
			{Code: "B", Severity: SeverityWarning, Passed: false},
		},
	}
	Summarize(r)
	if r.WarningCount != 1 || r.OverallStatus != "warning" {
		t.Errorf("got warning=%d status=%s", r.WarningCount, r.OverallStatus)
	}
	if r.BlocksStart() {
		t.Error("warning-only result should NOT block start (warnings are advisory)")
	}
}

func TestSummarize_FailureBlocksStart(t *testing.T) {
	r := &Result{
		Checks: []Check{
			{Code: "A", Severity: SeverityInfo, Passed: true},
			{Code: "B", Severity: SeverityError, Passed: false},
		},
	}
	Summarize(r)
	if r.FailedCount != 1 || r.OverallStatus != "failed" {
		t.Errorf("got failed=%d status=%s", r.FailedCount, r.OverallStatus)
	}
	if !r.BlocksStart() {
		t.Error("failed-check result MUST block start under STRICT_PREFLIGHT")
	}
}

func TestSummarize_AssessmentCrashIsErrorNotFailure(t *testing.T) {
	// Connection failure is reported as ErrorCount=1, distinct from a
	// check finding. Status is "error" so the UI can show "we couldn't
	// assess" vs. "we assessed and found issues".
	r := &Result{
		Checks: []Check{
			{Code: "POSTGRES_CONNECTION", Severity: SeverityError, Passed: false, Message: "dial tcp: refused"},
		},
		ErrorCount: 1,
	}
	Summarize(r)
	if r.OverallStatus != "error" {
		t.Errorf("expected status=error for assessment crash, got %s", r.OverallStatus)
	}
	if !r.BlocksStart() {
		t.Error("assessment crash MUST block start")
	}
}

func TestCheck_RemediationCarriesCopyableSQL(t *testing.T) {
	// Smoke test: assert the Check struct can carry the full diagnose.Remediation
	// shape — same SQLToRun + DocURL + Steps the StructuredError envelope uses.
	c := Check{
		Code: "MYSQL_BINLOG_FORMAT_NOT_ROW", Severity: SeverityError, Passed: false,
		Message: "binlog_format is STATEMENT",
		Remediation: &diagnose.Remediation{
			SQLToRun: []string{"SET GLOBAL binlog_format = 'ROW';"},
			DocURL:   diagnose.ErrorDocURL("mysql-binlog-format"),
		},
	}
	if c.Remediation == nil || len(c.Remediation.SQLToRun) == 0 {
		t.Fatal("Check did not preserve remediation SQL")
	}
	// Pin the FULL rendered URL, spelled out literally. Remediation.DocURL is
	// serialized into pipeline_run_events and returned to users, so a silent
	// change of host or shape ships a 404 into a live error payload — the exact
	// failure that killed docs.rsync-ai.dev. Comparing against
	// diagnose.ErrorDocURL(...) here would be a tautology that passes no matter
	// what the builder emits, so the expected value is written out by hand.
	const wantURL = "https://github.com/rsync-ai/rsync/blob/main/docs/errors/README.md#mysql-binlog-format"
	if c.Remediation.DocURL != wantURL {
		t.Errorf("DocURL = %q, want %q", c.Remediation.DocURL, wantURL)
	}
}

// TestErrorDocURL_ShapeAndOverride pins the two properties every assessor
// Remediation depends on: the default URL is an ANCHOR into one page (a
// path-shaped .../errors/<slug> would 404 on GitHub), and an operator running a
// docs mirror can repoint it with RSYNC_DOCS_BASE_URL.
func TestErrorDocURL_ShapeAndOverride(t *testing.T) {
	if got, want := diagnose.ErrorDocURL("postgres-wal-level"),
		"https://github.com/rsync-ai/rsync/blob/main/docs/errors/README.md#postgres-wal-level"; got != want {
		t.Errorf("default ErrorDocURL = %q, want %q", got, want)
	}
	// Trailing slash on the override must not produce a doubled separator.
	t.Setenv("RSYNC_DOCS_BASE_URL", "https://docs.example.com/")
	if got, want := diagnose.ErrorDocURL("cdc-missing-pk"),
		"https://docs.example.com/errors/README.md#cdc-missing-pk"; got != want {
		t.Errorf("overridden ErrorDocURL = %q, want %q", got, want)
	}
}

func TestAssessor_PostgresSourceTypeMatches(t *testing.T) {
	if NewPostgresAssessor().SourceType() != "postgresql" {
		t.Error("postgres assessor SourceType should be 'postgresql'")
	}
	if NewMySQLAssessor().SourceType() != "mysql" {
		t.Error("mysql assessor SourceType should be 'mysql'")
	}
}

func TestRegistry_SupportedTypes(t *testing.T) {
	r := NewRegistry()
	r.Register(NewPostgresAssessor())
	r.Register(NewMySQLAssessor())
	types := r.SupportedTypes()
	if len(types) != 2 {
		t.Errorf("want 2 types, got %d: %v", len(types), types)
	}
}

// TestInput_DestinationUsesUpsert pins which destination types are written via
// the sink's ON CONFLICT (pk) upsert path. Getting this wrong either over-blocks
// a valid batch load (false positive) or lets a PK-less table silently drop rows.
func TestInput_DestinationUsesUpsert(t *testing.T) {
	upsert := []string{"postgresql", "postgres", "mysql", "mariadb", "oracle", "sqlserver", "mssql", "PostgreSQL", "  MySQL  "}
	for _, dt := range upsert {
		if !(Input{DestinationType: dt}).DestinationUsesUpsert() {
			t.Errorf("DestinationType %q should use upsert", dt)
		}
	}
	noUpsert := []string{"", "aws-s3", "google-sheets", "shopify", "stripe", "notion", "slack", "unknown"}
	for _, dt := range noUpsert {
		if (Input{DestinationType: dt}).DestinationUsesUpsert() {
			t.Errorf("DestinationType %q should NOT use upsert", dt)
		}
	}
}

// TestInput_RequiresTablePrimaryKeys is the core Fix-1 gate: the per-table PK
// check must run for every CDC pipeline, and for a BATCH pipeline only when the
// destination upserts (relational DB). A batch load to a file/SaaS destination
// must NOT trigger it.
func TestInput_RequiresTablePrimaryKeys(t *testing.T) {
	cases := []struct {
		name     string
		syncMode string
		destType string
		want     bool
	}{
		{"cdc to anywhere", "cdc", "aws-s3", true},
		{"empty syncmode treated as cdc", "", "aws-s3", true},
		{"batch to postgres upserts", "batch", "postgresql", true},
		{"batch to mysql upserts", "batch", "mysql", true},
		{"snapshot to postgres upserts", "snapshot", "postgres", true},
		{"batch to s3 appends", "batch", "aws-s3", false},
		{"batch to sheets appends", "batch", "google-sheets", false},
		{"batch unknown dest does not over-block", "batch", "", false},
	}
	for _, tc := range cases {
		in := Input{SyncMode: tc.syncMode, DestinationType: tc.destType}
		if got := in.RequiresTablePrimaryKeys(); got != tc.want {
			t.Errorf("%s: RequiresTablePrimaryKeys()=%v, want %v", tc.name, got, tc.want)
		}
	}
}

// TestMissingPKReason ensures the failure message is mode-accurate — it must not
// claim "CDC requires one" on a batch run, and must mention the upsert for batch.
func TestMissingPKReason(t *testing.T) {
	if got := missingPKReason(true); !strings.Contains(got, "CDC") {
		t.Errorf("cdc reason should mention CDC, got %q", got)
	}
	batch := missingPKReason(false)
	if strings.Contains(batch, "CDC") {
		t.Errorf("batch reason must not mention CDC, got %q", batch)
	}
	if !strings.Contains(batch, "ON CONFLICT") || !strings.Contains(batch, "silently dropped") {
		t.Errorf("batch reason should explain the upsert drop, got %q", batch)
	}
}
