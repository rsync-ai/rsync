package healer

import "testing"

// forbiddenDLQTopics are the reactive DLQ topics that the FULL Start() subscribes.
// They MUST NEVER appear in the schema-only drift path: subscribing them would make
// the healer double-react to execution failures that the executor worker already
// handles synchronously via executeWithHealer + suggestRecoveryAction.
var forbiddenDLQTopics = []string{
	"agent.executor.requests.dlq",
	"agent.planner.responses.dlq",
}

// TestSchemaDriftSubscriptions_OnlySchemaTopics is the P0 regression guard: the
// drift-detect → approve path subscribes EXACTLY the two schema topics and NEVER a
// DLQ topic. It is broker-free — schemaDriftSubscriptions() is the single source of
// truth StartSchemaOnly iterates, so asserting it asserts StartSchemaOnly's reach
// without standing up Kafka.
func TestSchemaDriftSubscriptions_OnlySchemaTopics(t *testing.T) {
	a := NewAgent(nil, nil, "") // no broker/db needed: method values only, never invoked here
	subs := a.schemaDriftSubscriptions()

	if len(subs) != 2 {
		t.Fatalf("expected exactly 2 schema-drift subscriptions, got %d", len(subs))
	}

	got := make(map[string]schemaSubscription, len(subs))
	for _, s := range subs {
		if s.Handler == nil {
			t.Errorf("subscription %q has a nil handler", s.Topic)
		}
		if _, dup := got[s.Topic]; dup {
			t.Errorf("duplicate subscription for topic %q", s.Topic)
		}
		got[s.Topic] = s
	}

	// Both schema topics present.
	for _, want := range []string{HealerTopic, ApprovedChangeTopic} {
		if _, ok := got[want]; !ok {
			t.Errorf("missing required schema topic %q", want)
		}
	}

	// No DLQ topic may be subscribed by the schema-only path.
	for _, forbidden := range forbiddenDLQTopics {
		if _, ok := got[forbidden]; ok {
			t.Errorf("schema-only path must NOT subscribe DLQ topic %q (double-processing risk)", forbidden)
		}
	}

	// HealerTopic is the drift ingress — its failure must be fatal (Required=true).
	if s, ok := got[HealerTopic]; ok && !s.Required {
		t.Errorf("HealerTopic (%q) must be a Required subscription", HealerTopic)
	}
	// ApprovedChangeTopic is the apply-on-approve path — best-effort (Required=false).
	if s, ok := got[ApprovedChangeTopic]; ok && s.Required {
		t.Errorf("ApprovedChangeTopic (%q) should be best-effort (Required=false)", ApprovedChangeTopic)
	}
}

// TestSchemaTopicConstants pins the wire values: the api-gateway approve handler and
// the future detector both hardcode these strings; drift here silently breaks the
// producer/consumer contract.
func TestSchemaTopicConstants(t *testing.T) {
	if HealerTopic != "rsync.healer.schema-changes" {
		t.Errorf("HealerTopic = %q, want rsync.healer.schema-changes", HealerTopic)
	}
	if ApprovedChangeTopic != "rsync.healer.approved-changes" {
		t.Errorf("ApprovedChangeTopic = %q, want rsync.healer.approved-changes", ApprovedChangeTopic)
	}
}
