package executor

import (
	"context"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/healer"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// emitSchemaChanges is the executor's producer to healer.HealerTopic. It is gated on
// RSYNC_SCHEMA_DRIFT_ENABLED itself, not only through its caller
// detectAndEmitSchemaDrift: with the flag off kafka.EnsurePlatformTopics does not
// provision rsync.healer.schema-changes, so any produce would auto-create it.
//
// The agent's Kafka manager is the zero value, whose nil tracer makes every produce
// panic: "did not panic" is "did not produce". The flag-on control proves the same
// call reaches the produce, so the flag-off pass is not vacuous.
func TestEmitSchemaChangesProducesNothingWhenTheFlagIsOff(t *testing.T) {
	changes := []healer.SchemaChange{{ChangeType: "add_column", Table: "orders", ColumnName: "note"}}
	emit := func() (p interface{}) {
		defer func() { p = recover() }()
		a := &Agent{kafkaManager: &kafka.Manager{}}
		a.emitSchemaChanges(context.Background(), "pipeline-1", changes)
		return nil
	}

	for _, flag := range []string{"", "false"} {
		t.Run("flag="+flag, func(t *testing.T) {
			t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", flag)
			if p := emit(); p != nil {
				t.Fatalf("emitSchemaChanges produced to %s with RSYNC_SCHEMA_DRIFT_ENABLED=%q (%v)",
					healer.HealerTopic, flag, p)
			}
		})
	}

	t.Run("flag on control", func(t *testing.T) {
		t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "true")
		if p := emit(); p == nil {
			t.Fatal("emitSchemaChanges with the flag on never reached the produce, so the " +
				"flag-off cases prove nothing")
		}
	})
}
