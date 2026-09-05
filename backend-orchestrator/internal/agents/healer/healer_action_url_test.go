package healer

import (
	"testing"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
)

// TestActionURLForError pins the notification deep-link routing. This is the
// regression guard for the schema-drift alert landing on the wrong page: a
// StructuredError carrying Category=schema_drift must deep-link to the schema
// changes page (where the approve/reject cards render), while every other error
// links to the pipeline overview. The returned path is relative by contract —
// the notifier prefixes the absolute host at delivery time.
func TestActionURLForError(t *testing.T) {
	const pipelineID = "pl-123"

	cases := []struct {
		name string
		se   *diagnose.StructuredError
		want string
	}{
		{
			name: "schema-drift deep-links to the schema changes page",
			se:   &diagnose.StructuredError{Category: diagnose.CategorySchemaDrift},
			want: "/pipelines/pl-123/schema-changes",
		},
		{
			name: "auth error links to the pipeline overview",
			se:   &diagnose.StructuredError{Category: diagnose.CategoryAuthExpired},
			want: "/pipelines/pl-123",
		},
		{
			name: "legacy uncategorized error links to the pipeline overview",
			se:   &diagnose.StructuredError{Code: "LEGACY_UNCLASSIFIED"},
			want: "/pipelines/pl-123",
		},
		{
			name: "nil structured error links to the pipeline overview",
			se:   nil,
			want: "/pipelines/pl-123",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := actionURLForError(pipelineID, tc.se); got != tc.want {
				t.Errorf("actionURLForError(%q, %+v) = %q, want %q", pipelineID, tc.se, got, tc.want)
			}
		})
	}
}

// TestSchemaDriftNotificationDeepLinks proves the end-to-end wiring the fix
// depends on: the drift notify at processAnalysis builds its StructuredError
// via diagnose.FromDiagnosis(CategorySchemaDrift), which stamps the schema_drift
// category + the SCHEMA_DRIFT_DETECTED remediation envelope. That category is
// exactly what routes the deep-link — so a drift alert now lands on the schema
// changes page and carries the richer code/remediation, instead of the bare
// /pipelines/{id} the legacy notifyUser path produced.
func TestSchemaDriftNotificationDeepLinks(t *testing.T) {
	const pipelineID = "pl-abc"

	se := diagnose.FromDiagnosis(
		diagnose.Diagnosis{Category: diagnose.CategorySchemaDrift},
		diagnose.Signal{PipelineID: pipelineID},
	)

	if se.Category != diagnose.CategorySchemaDrift {
		t.Fatalf("FromDiagnosis(schema_drift) category = %q, want %q", se.Category, diagnose.CategorySchemaDrift)
	}
	if se.Code != "SCHEMA_DRIFT_DETECTED" {
		t.Errorf("FromDiagnosis(schema_drift) code = %q, want SCHEMA_DRIFT_DETECTED", se.Code)
	}
	if se.Remediation == nil || len(se.Remediation.Steps) == 0 {
		t.Error("expected a non-empty remediation envelope on the schema-drift error")
	}
	if got, want := actionURLForError(pipelineID, se), "/pipelines/pl-abc/schema-changes"; got != want {
		t.Errorf("drift action_url = %q, want %q", got, want)
	}
}
