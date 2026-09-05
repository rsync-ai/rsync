package healer

// C1: the schema-drift approval card tells the user that a destructive change is
// NOT auto-applied and that approving only records their decision. The healer used
// to be handed those approvals anyway, refuse them inside applyMigration, flip the
// approval row from `approved` to `failed`, and raise a red alert — punishing the
// user for doing exactly what the screen said. isDestructiveDDL is now the single
// definition both the guard and the approved-changes handler consult, and
// handleApprovedChange returns before applyMigration ever sees such a statement.

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/IBM/sarama"
)

func TestIsDestructiveDDL(t *testing.T) {
	// Destructive means "would delete data if run", which an advisory note is not —
	// the lockstep with the gateway lives in TestNotAutoAppliedDDL below, because
	// there are now two independent reasons a statement is never auto-applied.
	tests := []struct {
		ddl  string
		want bool
	}{
		{"ALTER TABLE public.orders ADD COLUMN total numeric", false},
		{"ALTER TABLE public.orders ALTER COLUMN id TYPE bigint", false},
		{"-- drift: table public.shipments appeared in source", false},
		{"ALTER TABLE public.orders DROP COLUMN name", true},
		{"DROP TABLE public.legacy", true},
		{"TRUNCATE TABLE public.orders", true},
		{"ALTER TABLE x; TRUNCATE y", true},
		{"alter table t drop column c", true},
		{"   DROP TABLE t", true},
		// Token check, not substring: a column NAMED drop_* is fine.
		{"ALTER TABLE t ADD COLUMN drop_reason text", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isDestructiveDDL(tt.ddl); got != tt.want {
			t.Errorf("isDestructiveDDL(%q) = %v, want %v", tt.ddl, got, tt.want)
		}
	}
}

// notAutoAppliedDDL is the predicate the gateway mirrors as !autoApplicableDDL
// (handlers/schema_evolution.go, TestAutoApplicableDDL): the gateway tells the user
// what approval will do and gates dispatch on it, this decides what actually
// happens on the other side. They must not disagree about the same statement —
// that disagreement IS the defect. Kept case-for-case with TestAutoApplicableDDL,
// inverted, so a change to one predicate that isn't mirrored shows up as a failing
// case here rather than as a user approving a change nobody applies.
func TestNotAutoAppliedDDL(t *testing.T) {
	tests := []struct {
		ddl  string
		want bool
	}{
		{"ALTER TABLE public.orders ADD COLUMN total numeric", false},
		{"ALTER TABLE public.orders ALTER COLUMN id TYPE bigint", false},
		// Advisory notes: a comment executes cleanly everywhere, so letting one
		// through reported an apply that never happened.
		{"-- drift: table public.shipments appeared in source", true},
		{"-- drift: public.orders.note declared type changed in the source from varchar(50) to varchar(10) (both map to string, so the destination column is unchanged)", true},
		{"   -- drift: anything", true},
		{"ALTER TABLE public.orders DROP COLUMN name", true},
		{"DROP TABLE public.legacy", true},
		{"TRUNCATE TABLE public.orders", true},
		{"ALTER TABLE x; TRUNCATE y", true},
		{"alter table t drop column c", true},
		{"   DROP TABLE t", true},
		{"ALTER TABLE t ADD COLUMN drop_reason text", false},
	}
	for _, tt := range tests {
		if got := notAutoAppliedDDL(tt.ddl); got != tt.want {
			t.Errorf("notAutoAppliedDDL(%q) = %v, want %v", tt.ddl, got, tt.want)
		}
	}
}

// CDC-D1: the cdcstats agent reports source drops that CDC does not mirror by
// publishing them onto HealerTopic with applied=false, so they take this package's
// ordinary classify → file → notify path. That is only safe because these statements
// are never executed — approving one records the decision and leaves the apply to the
// user. The producer normalizes to exactly these two shapes for that reason.
//
// The dependency runs cdcstats → healer, so the assertion has to live here. If
// isDestructiveDDL is ever narrowed such that one of these stops matching, CDC would
// start handing users an Approve button that runs DROP against their destination.
func TestCDCReportedDropsAreDestructive(t *testing.T) {
	// Keep in lockstep with classifySchemaChange in
	// internal/agents/cdcstats/schema_changes.go — these are the only two DDL forms
	// it emits.
	for _, ddl := range []string{
		"DROP TABLE pipeline_test.cdc_drift",
		"ALTER TABLE pipeline_test.cdc_drift DROP COLUMN note",
	} {
		if !isDestructiveDDL(ddl) {
			t.Errorf("cdcstats emits %q; isDestructiveDDL must classify it destructive or the healer will execute it against the destination", ddl)
		}
	}
}

// handleApprovedChange must not touch applyMigration for destructive DDL. The
// Agent here has a nil db and nil kafkaManager: if the destructive branch is ever
// removed, applyMigration is reached and dereferences a.db, so this test fails
// loudly (panic) rather than silently passing.
func TestHandleApprovedChange_DestructiveDDL_NotApplied(t *testing.T) {
	a := &Agent{}
	payload, err := json.Marshal(map[string]interface{}{
		"event_type":  "schema_change_approved",
		"approval_id": "approval-1",
		"pipeline_id": "11111111-1111-1111-1111-111111111111",
		"change_type": "drop_column",
		"table_name":  "orders",
		"ddl":         "ALTER TABLE orders DROP COLUMN legacy_ref",
		"approved_by": "alice@example.com",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := a.handleApprovedChange(context.Background(), &sarama.ConsumerMessage{Value: payload}); err != nil {
		t.Fatalf("handleApprovedChange returned error: %v", err)
	}
	// Reaching here means no apply was attempted, so nothing marked the user's
	// approval `failed` and nothing alarmed them.
}

// B1: the same guarantee for advisory notes. Approving one used to send a SQL
// comment to the destination, which executed it happily and left the row marked
// `applied` having applied nothing — the C1 falsehood from the other side. Same
// nil-db Agent, so a regression that reaches applyMigration panics here.
func TestHandleApprovedChange_AdvisoryDDL_NotApplied(t *testing.T) {
	for _, ddl := range []string{
		"-- drift: table public.shipments appeared in source",
		"-- drift: public.orders.note declared type changed in the source from varchar(50) to varchar(10) (both map to string, so the destination column is unchanged)",
	} {
		a := &Agent{}
		payload, err := json.Marshal(map[string]interface{}{
			"event_type":  "schema_change_approved",
			"approval_id": "approval-advisory",
			"pipeline_id": "11111111-1111-1111-1111-111111111111",
			"change_type": "modify_column",
			"table_name":  "orders",
			"ddl":         ddl,
			"approved_by": "alice@example.com",
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if err := a.handleApprovedChange(context.Background(), &sarama.ConsumerMessage{Value: payload}); err != nil {
			t.Fatalf("handleApprovedChange(%q) returned error: %v", ddl, err)
		}
	}
}

// The auto-apply path can reach applyMigration without passing through
// handleApprovedChange, so the refusal has to hold there independently. It must
// refuse BEFORE touching a.db (nil here) — an advisory note is not a statement.
func TestApplyMigration_RefusesAdvisoryDDL(t *testing.T) {
	a := &Agent{}
	err := a.applyMigration(context.Background(), "11111111-1111-1111-1111-111111111111",
		"-- drift: table public.shipments appeared in source")
	if err == nil {
		t.Fatal("applyMigration accepted an advisory drift note; it would be sent to the destination and report a successful apply that never happened")
	}
}
