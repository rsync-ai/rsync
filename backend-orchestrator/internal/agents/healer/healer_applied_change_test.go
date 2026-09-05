package healer

// CDC-A1: the CDC sink applies a new source column to the destination the moment it
// streams in and used to tell nobody, so the schema changes page stayed empty while the
// destination schema changed underneath the user. The sink now reports it with
// applied=true; the healer must record it as history and must NOT put it through the
// normal analyse → approve path, which can only produce two wrong answers for a change
// that is already live: a pending approval for something that already happened, or a
// re-apply of DDL the destination already has.

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/IBM/sarama"
	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
)

// Every notification body is scrubbed before it is persisted and shown. llmscrub's
// dangling-quote rule is fail-closed — an unpaired ' masks everything after it — so the
// previous copy ("…in the pipeline's Schema Changes tab") reached users as
// "…in the pipeline'[redacted]", losing the only instruction the alert carried. The
// scrubber is right; the copy was wrong. This pins the copy, in both variants, for both
// producers.
//
// It also pins WHERE the copy sends people. The apostrophe fix kept the words "Schema
// Changes tab", and there is no such tab: PipelineDetailTabsClient renders exactly six
// (Overview, Execution History, Steps/DAG, Table statistics, Transforms, Data flow).
// Schema drift lives on its own route, /pipelines/{id}/schema-changes, which the alert
// already deep-links via Category=schema_drift (TestActionURLForError). So an alert that
// survived the scrubber intact still ended with an instruction that could not be
// followed — the user goes to the pipeline, counts six tabs, and finds no way in.
func TestDriftNotificationCopySurvivesScrub(t *testing.T) {
	for _, applied := range []bool{true, false} {
		msg := schemaDriftNotificationText("add_column", "public.orders", applied)
		if got := llmscrub.Scrub(msg); got != msg {
			t.Errorf("applied=%v: notification copy is mangled by llmscrub.\n  before: %s\n   after: %s", applied, msg, got)
		}
		if strings.Contains(msg, "'") {
			t.Errorf("applied=%v: notification copy contains an apostrophe; llmscrub masks the rest of the sentence: %s", applied, msg)
		}
		// Naming the destination is the actionable part and the part the old bug ate.
		if !strings.Contains(msg, "schema changes page") {
			t.Errorf("applied=%v: notification copy no longer names the schema changes page: %s", applied, msg)
		}
		// ...and it must not name a tab. There is none; sending users to hunt for one
		// is the same dead end as saying nothing.
		if strings.Contains(strings.ToLower(msg), "tab") {
			t.Errorf("applied=%v: notification copy points at a tab, but the pipeline detail has no Schema Changes tab: %s", applied, msg)
		}
	}
}

func appliedChangeMessage(t *testing.T, applied bool) *sarama.ConsumerMessage {
	t.Helper()
	payload, err := json.Marshal(map[string]interface{}{
		"event_type":  "SCHEMA_CHANGE_DETECTED",
		"pipeline_id": "11111111-1111-1111-1111-111111111111",
		"schema_change": map[string]interface{}{
			"change_type": "add_column",
			"table":       "public.orders",
			"column_name": "email",
			"column_type": "TEXT",
			"ddl":         "ALTER TABLE public.orders ADD COLUMN email TEXT",
			"applied":     applied,
		},
		"context":       map[string]interface{}{"source": "kafka_mcp_sink", "mode": "cdc"},
		"action_needed": false,
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return &sarama.ConsumerMessage{Value: payload}
}

// The Agent here has a nil db, nil kafkaManager AND nil httpClient. If the
// already-applied short-circuit is ever removed, handleSchemaChangeMessage falls
// through to analyzeWithLLM, which calls a method on the nil *http.Client and panics —
// so this test fails loudly rather than silently letting a live change be re-queued
// for approval.
func TestHandleSchemaChangeMessage_AlreadyApplied_SkipsAnalysis(t *testing.T) {
	a := &Agent{}

	if err := a.handleSchemaChangeMessage(context.Background(), appliedChangeMessage(t, true)); err != nil {
		t.Fatalf("handleSchemaChangeMessage returned error: %v", err)
	}
}

// The short-circuit is keyed on applied, not on the CDC source: an ordinary
// (not-yet-applied) drift event must still take the analyse path. With a nil
// httpClient that path panics, which is exactly the signal we want — it proves the
// branch above is reached only by applied=true and is not swallowing every event.
func TestHandleSchemaChangeMessage_NotApplied_TakesAnalysisPath(t *testing.T) {
	a := &Agent{}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected the not-applied event to reach the analysis path (nil httpClient panic); " +
				"it returned cleanly, so the applied=true short-circuit is swallowing events it should not")
		}
	}()

	_ = a.handleSchemaChangeMessage(context.Background(), appliedChangeMessage(t, false))
}

// recordAlreadyAppliedChange runs with no database and no broker: the sink has already
// committed the column to the destination, so a control-plane outage must degrade to a
// missing history row, never to a panic on the CDC apply path's downstream consumer.
func TestRecordAlreadyAppliedChange_NoDBNoBroker(t *testing.T) {
	a := &Agent{}
	event := &SchemaChangeEvent{
		PipelineID: "11111111-1111-1111-1111-111111111111",
		SchemaChange: SchemaChange{
			ChangeType: "add_column",
			Table:      "public.orders",
			DDL:        "ALTER TABLE public.orders ADD COLUMN email TEXT",
			Applied:    true,
		},
	}
	if err := a.recordAlreadyAppliedChange(context.Background(), event); err != nil {
		t.Fatalf("recordAlreadyAppliedChange returned error: %v", err)
	}
}
