package healer

import (
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// The healer's Kafka surface is the schema-drift path: it consumes HealerTopic and
// ApprovedChangeTopic and produces ResultsTopic. kafka.EnsurePlatformTopics provisions
// those three only when RSYNC_SCHEMA_DRIFT_ENABLED=true, and a consumer-group join or
// a produce auto-creates its topic at broker defaults, so with the flag off the healer
// must neither subscribe nor produce.
//
// Both tests hand the agent a Kafka manager that cannot do anything (nil, or the zero
// value, whose tracer is nil): a subscribe or produce panics. "Did not panic" therefore
// means "did not touch Kafka", and each flag-on control shows the same call DOES reach
// Kafka, so the flag-off pass is not vacuous.

func panics(f func()) (p interface{}) {
	defer func() { p = recover() }()
	f()
	return nil
}

func TestStartSchemaOnlySubscribesNothingWhenTheFlagIsOff(t *testing.T) {
	for _, flag := range []string{"", "false", "TRUE"} {
		t.Run("flag="+flag, func(t *testing.T) {
			t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", flag)
			a := NewAgent(nil, nil, "")
			var err error
			if p := panics(func() { err = a.StartSchemaOnly() }); p != nil {
				t.Fatalf("StartSchemaOnly subscribed with RSYNC_SCHEMA_DRIFT_ENABLED=%q "+
					"(nil Kafka manager used: %v); the group join would auto-create "+
					"rsync.healer.schema-changes and rsync.healer.approved-changes", flag, p)
			}
			if err != nil {
				t.Errorf("StartSchemaOnly with the flag off returned %v, want nil", err)
			}
		})
	}
}

func TestStartSchemaOnlySubscribesWhenTheFlagIsOn(t *testing.T) {
	t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "true")
	a := NewAgent(nil, nil, "")
	if p := panics(func() { _ = a.StartSchemaOnly() }); p == nil {
		t.Fatal("StartSchemaOnly with the flag on never reached the Kafka manager, so " +
			"the flag-off test proves nothing")
	}
}

func TestPublishHealingResultProducesNothingWhenTheFlagIsOff(t *testing.T) {
	result := &HealingResult{PipelineID: "p", ChangeType: "add_column", Status: "applied"}

	t.Run("flag off", func(t *testing.T) {
		t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "")
		a := NewAgent(&kafka.Manager{}, nil, "")
		if p := panics(func() { a.publishHealingResult(result) }); p != nil {
			t.Fatalf("publishHealingResult produced to %s with the flag off (%v); that "+
				"produce auto-creates the topic on a default install", ResultsTopic, p)
		}
	})

	t.Run("flag on control", func(t *testing.T) {
		t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "true")
		a := NewAgent(&kafka.Manager{}, nil, "")
		if p := panics(func() { a.publishHealingResult(result) }); p == nil {
			t.Fatal("publishHealingResult with the flag on never reached the produce, " +
				"so the flag-off case proves nothing")
		}
	})
}
