package handlers

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// U-19: an operator's acknowledgement of an assessment warning was never
// remembered, so every Start / Reload of a PostgreSQL CDC pipeline stopped at
// the pre-migration gate again for the very warnings acknowledged minutes
// earlier. Bug class: *an operator decision the gate asks for is not
// persisted, so the gate re-asks it on every run*. These tests pin the class on
// the gate itself: every warning kind the report can carry (per-table,
// source readiness, destination namespace) is waivable by a remembered ack
// while it is unchanged, and nothing else is.

func ackTestReport() *AssessmentReport {
	return &AssessmentReport{Tables: []AssessmentTable{
		{Name: "orders", Schema: "public", Findings: []AssessmentFinding{
			{Code: FindingNoPrimaryKey, Severity: AssessmentWarning, Message: "Source declares no primary keys for \"orders\"."},
			{Code: FindingJSONCollapse, Severity: AssessmentInfo, Message: "2 columns store nested data"},
		}},
		{Name: "(source readiness)", Findings: []AssessmentFinding{
			{Code: "POSTGRES_UNSELECTED_TABLE_MISSING_PRIMARY_KEY", Severity: AssessmentWarning,
				Message: "public.audit is not in this pipeline and has no primary key.",
				Details: map[string]interface{}{"object": "public.audit"}},
		}},
		{Name: "(destination namespace)", Findings: []AssessmentFinding{
			{Code: FindingDestNamespaceWillCreate, Severity: AssessmentWarning, Message: "Schema analytics will be created."},
		}},
	}}
}

func allWarningKeys(r *AssessmentReport) map[string]bool {
	out := map[string]bool{}
	for _, k := range assessmentWarningKeys(r) {
		out[k.key] = true
	}
	return out
}

func TestAssessmentGate_RememberedAcksClearUnchangedWarnings(t *testing.T) {
	report := ackTestReport()
	acked := allWarningKeys(report)
	if len(acked) != 3 {
		t.Fatalf("expected 3 distinct warning identities (info findings excluded), got %d", len(acked))
	}

	// Control: with nothing remembered the gate must still ask.
	if got := evaluateAssessmentGate(report, false, nil); got != assessmentGateNeedsAck {
		t.Fatalf("no remembered acks: got %v, want NeedsAck", got)
	}
	// The bug: every warning already acknowledged, still re-gated.
	if got := evaluateAssessmentGate(ackTestReport(), false, acked); got != assessmentGateAllow {
		t.Fatalf("all warnings acknowledged and unchanged: got %v, want Allow (U-19: acks not remembered)", got)
	}
}

func TestAssessmentGate_ChangedOrNewWarningStillNeedsAck(t *testing.T) {
	acked := allWarningKeys(ackTestReport())

	changed := ackTestReport()
	changed.Tables[1].Findings[0].Message = "public.audit2 is not in this pipeline and has no primary key."
	changed.Tables[1].Findings[0].Details = map[string]interface{}{"object": "public.audit2"}
	if got := evaluateAssessmentGate(changed, false, acked); got != assessmentGateNeedsAck {
		t.Fatalf("a changed warning must be re-acknowledged: got %v", got)
	}

	added := ackTestReport()
	added.Tables = append(added.Tables, AssessmentTable{Name: "events", Schema: "public", Findings: []AssessmentFinding{
		{Code: FindingNoPrimaryKey, Severity: AssessmentWarning, Message: "Source declares no primary keys for \"events\"."},
	}})
	if got := evaluateAssessmentGate(added, false, acked); got != assessmentGateNeedsAck {
		t.Fatalf("a new warning must be acknowledged: got %v", got)
	}

	// Same code + message on a different table is a different warning.
	moved := ackTestReport()
	moved.Tables[0].Name = "orders_v2"
	if got := evaluateAssessmentGate(moved, false, acked); got != assessmentGateNeedsAck {
		t.Fatalf("the same warning on another table must be acknowledged: got %v", got)
	}
}

func TestAssessmentGate_RememberedAcksNeverWaiveErrors(t *testing.T) {
	report := ackTestReport()
	report.Tables[0].Findings = append(report.Tables[0].Findings, AssessmentFinding{
		Code: FindingSinkNoDDL, Severity: AssessmentError, Message: "Destination connector doesn't support auto-create.",
	})
	// Even an ack set that (wrongly) contains the error's identity must not waive it.
	acked := allWarningKeys(report)
	acked[assessmentWarningKey(report.Tables[0], report.Tables[0].Findings[2])] = true
	if got := evaluateAssessmentGate(report, true, acked); got != assessmentGateBlocked {
		t.Fatalf("errors are never waivable: got %v", got)
	}
	report.Blocking = true
	if got := evaluateAssessmentGate(report, false, acked); got != assessmentGateBlocked {
		t.Fatalf("Blocking report must stay blocked: got %v", got)
	}
}

func TestAssessmentWarningKey_StableAcrossRuns(t *testing.T) {
	a := ackTestReport()
	b := ackTestReport()
	for i := range a.Tables {
		for j := range a.Tables[i].Findings {
			if assessmentWarningKey(a.Tables[i], a.Tables[i].Findings[j]) != assessmentWarningKey(b.Tables[i], b.Tables[i].Findings[j]) {
				t.Fatalf("warning key not deterministic for %s/%d", a.Tables[i].Name, j)
			}
		}
	}
}

var (
	ackSelectRE = regexp.QuoteMeta(`SELECT warning_key FROM pipeline_assessment_acks WHERE pipeline_id = $1::uuid`)
	ackInsertRE = regexp.QuoteMeta(`INSERT INTO pipeline_assessment_acks`)
)

// runAssessmentGate is the handler's whole decision: it must read the
// remembered acks, and remember the warnings the operator acks now, so the
// NEXT run is not gated again.
func TestRunAssessmentGate_PersistsAcksAndHonoursThemNextRun(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	const pid = "11111111-1111-1111-1111-111111111111"
	const uid = "22222222-2222-2222-2222-222222222222"
	report := ackTestReport()
	keys := assessmentWarningKeys(report)

	// Run 1: nothing remembered, operator acks → allowed, 3 acks stored.
	mock.ExpectQuery(ackSelectRE).WithArgs(pid).WillReturnRows(sqlmock.NewRows([]string{"warning_key"}))
	for _, k := range keys {
		mock.ExpectExec(ackInsertRE).
			WithArgs(pid, k.key, k.code, k.table, uid).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}
	if got := runAssessmentGate(context.Background(), database, pid, uid, report, true); got != assessmentGateAllow {
		t.Fatalf("run 1 (acked): got %v, want Allow", got)
	}

	// Run 2: no ack flag, but the remembered acks cover every warning.
	rows := sqlmock.NewRows([]string{"warning_key"})
	for _, k := range keys {
		rows.AddRow(k.key)
	}
	mock.ExpectQuery(ackSelectRE).WithArgs(pid).WillReturnRows(rows)
	if got := runAssessmentGate(context.Background(), database, pid, uid, ackTestReport(), false); got != assessmentGateAllow {
		t.Fatalf("run 2 (remembered): got %v, want Allow", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

func TestRunAssessmentGate_BlockedRunRemembersNothing(t *testing.T) {
	database, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	report := ackTestReport()
	report.Blocking = true
	mock.ExpectQuery(ackSelectRE).WillReturnRows(sqlmock.NewRows([]string{"warning_key"}))
	if got := runAssessmentGate(context.Background(), database, "p", "u", report, true); got != assessmentGateBlocked {
		t.Fatalf("got %v, want Blocked", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("a blocked run must not store acks: %v", err)
	}
}
