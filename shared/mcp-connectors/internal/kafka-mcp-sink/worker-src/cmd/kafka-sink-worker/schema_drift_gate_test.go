package main

import (
	"testing"

	"github.com/rsync-ai/shared/kafkaclient"
)

// The sink reports self-applied CDC drift on rsync.healer.schema-changes only when
// the schema-drift loop is on. With it off the orchestrator neither creates that
// topic nor consumes it, so a writer would only make the broker auto-create an
// unread topic. The flag is read exactly like config.SchemaDriftEnabled in the
// orchestrator and the api-gateway: the literal "true" and nothing else.
func TestNewSchemaDriftWriter_GatedOnTheDriftFlag(t *testing.T) {
	cfg := &WorkerConfig{KafkaBootstrapServers: "kafka:29092"}

	for _, v := range []string{"", "false", "TRUE", "1", "yes", " true"} {
		t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", v)
		if w := newSchemaDriftWriter(cfg); w != nil {
			t.Errorf("RSYNC_SCHEMA_DRIFT_ENABLED=%q: got a writer for %q, want nil (loop off)", v, w.Topic)
		}
	}

	t.Setenv("RSYNC_SCHEMA_DRIFT_ENABLED", "true")
	w := newSchemaDriftWriter(cfg)
	if w == nil {
		t.Fatal("RSYNC_SCHEMA_DRIFT_ENABLED=true: got nil, want the schema-change writer")
	}
	defer w.Close()
	if want := kafkaclient.Topic("rsync.healer.schema-changes"); w.Topic != want {
		t.Errorf("writer topic = %q, want %q", w.Topic, want)
	}
}
