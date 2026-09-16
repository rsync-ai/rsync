package handlers

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSinkSupportsAutoCreate_ObjectStoresAreNotBlocked pins the fix for the
// false SINK_NO_DDL blocker: an object store has no tables to pre-create, so
// every MongoDB→GCS/S3/Azure-Blob pipeline was raising a BLOCKING error on
// every table that the user had no way to clear.
//
// It reads the REAL connector metadata off disk, so it fails if either half of
// the fix regresses — the Go reader or the connectors' metadata.json flags.
func TestSinkSupportsAutoCreate_ObjectStoresAreNotBlocked(t *testing.T) {
	root := filepath.Join("..", "..", "..", "shared", "mcp-connectors", "public")
	if _, err := os.Stat(root); err != nil {
		t.Skipf("connector tree not present at %s: %v", root, err)
	}
	t.Setenv("MCP_PUBLIC_CONNECTORS_PATH", root)

	cases := []struct {
		connector string
		want      bool
		why       string
	}{
		// The bug: all three answered false and blocked the run.
		{"gcs", true, "object store — writes an object per batch, nothing to pre-create"},
		{"aws-s3", true, "object store — nothing to pre-create"},
		{"azure-blob", true, "object store — nothing to pre-create"},
		// Regression guards for the cases that already worked.
		{"mongodb", true, "creates the collection on first write"},
		{"postgresql", true, "hardcoded sinksWithAutoCreate entry — issues real DDL"},
		{"mysql", true, "metadata declares supports_ddl + auto_create"},
		// Negative control: without one, a reader that returns true for
		// everything would pass every assertion above.
		{"definitely-not-a-connector", false, "unknown connector must stay fail-closed"},
	}
	for _, tc := range cases {
		if got := sinkSupportsAutoCreate(tc.connector); got != tc.want {
			t.Errorf("sinkSupportsAutoCreate(%q) = %v, want %v (%s)", tc.connector, got, tc.want, tc.why)
		}
	}
}

// TestConnectorSupportsDDL_ObjectStoresStillDeclareNoDDL guards the other side
// of the same change: the object stores must NOT start claiming real DDL. The
// kafka-sink worker gates ensure_table on supports_ddl && auto_create, so
// flipping supports_ddl to true here would make it issue CREATE TABLE calls
// against a bucket.
func TestConnectorSupportsDDL_ObjectStoresStillDeclareNoDDL(t *testing.T) {
	root := filepath.Join("..", "..", "..", "shared", "mcp-connectors", "public")
	if _, err := os.Stat(root); err != nil {
		t.Skipf("connector tree not present at %s: %v", root, err)
	}
	t.Setenv("MCP_PUBLIC_CONNECTORS_PATH", root)

	for _, ct := range []string{"gcs", "aws-s3", "azure-blob", "mongodb"} {
		if connectorSupportsDDL(ct) {
			t.Errorf("connectorSupportsDDL(%q) = true, want false", ct)
		}
	}
	// Positive control: the reader is capable of returning true at all. mysql is
	// the control rather than postgresql because postgresql's metadata.json
	// declares no supports_ddl key — it reaches sinkSupportsAutoCreate through
	// the hardcoded sinksWithAutoCreate map instead.
	if !connectorSupportsDDL("mysql") {
		t.Error("connectorSupportsDDL(\"mysql\") = false, want true — reader is broken, the assertions above are vacuous")
	}
}

// TestEvaluateAssessmentGate pins the severity boundary: ack_warnings waives
// WARNINGS and only warnings. RunPipeline used to skip the whole assessment
// when ack_warnings was set, so the flag silently waived blocking ERRORS too.
func TestEvaluateAssessmentGate(t *testing.T) {
	report := func(sev AssessmentSeverity) *AssessmentReport {
		r := &AssessmentReport{Tables: []AssessmentTable{{
			Name: "users", Findings: []AssessmentFinding{{Code: "X", Severity: sev}},
		}}}
		if sev == AssessmentError {
			r.Blocking = true
		}
		return r
	}

	cases := []struct {
		name string
		rep  *AssessmentReport
		ack  bool
		want assessmentGateOutcome
	}{
		{"nil report allows", nil, false, assessmentGateAllow},
		{"clean report allows", &AssessmentReport{}, false, assessmentGateAllow},
		{"info only allows", report(AssessmentInfo), false, assessmentGateAllow},
		{"warning without ack needs ack", report(AssessmentWarning), false, assessmentGateNeedsAck},
		{"warning with ack allows", report(AssessmentWarning), true, assessmentGateAllow},
		{"error without ack blocks", report(AssessmentError), false, assessmentGateBlocked},
		// The bug, in one line: ack must NOT clear an error.
		{"error WITH ack still blocks", report(AssessmentError), true, assessmentGateBlocked},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := evaluateAssessmentGate(tc.rep, tc.ack); got != tc.want {
				t.Errorf("evaluateAssessmentGate(ack=%v) = %v, want %v", tc.ack, got, tc.want)
			}
		})
	}

	// An ERROR finding blocks even if Blocking was somehow left false.
	r := report(AssessmentError)
	r.Blocking = false
	if got := evaluateAssessmentGate(r, true); got != assessmentGateBlocked {
		t.Errorf("error finding with Blocking=false = %v, want blocked", got)
	}
}
