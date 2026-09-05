package kafka

import "testing"

// The planner asks for RF=3 unconditionally (llm-service strategies.py
// DEFAULT_REPLICATION) and pairs it with min.insync.replicas=min(2, rf). These
// tests pin what that request must become on a customer's own Kafka, which is
// the deployment shape the clamp exists for.
func TestClampToClusterLowersReplicationFactorToBrokerCount(t *testing.T) {
	cfg := TopicConfig{Name: "cdc.abc12345", ReplicationFactor: 3}
	cfg.clampToCluster(1)
	if cfg.ReplicationFactor != 1 {
		t.Fatalf("RF=3 on a 1-broker cluster must clamp to 1, got %d", cfg.ReplicationFactor)
	}
}

// The half-fix that leaves a topic permanently unwritable: RF drops to 1 but
// min.insync.replicas stays 2, so every acks=all produce gets NOT_ENOUGH_REPLICAS.
func TestClampToClusterAlsoLowersMinInsyncReplicas(t *testing.T) {
	cfg := TopicConfig{
		Name:              "cdc.abc12345",
		ReplicationFactor: 3,
		Config:            map[string]string{"min.insync.replicas": "2", "cleanup.policy": "delete"},
	}
	cfg.clampToCluster(1)
	if got := cfg.Config["min.insync.replicas"]; got != "1" {
		t.Fatalf("min.insync.replicas must follow RF down to 1, got %q", got)
	}
	if got := cfg.Config["cleanup.policy"]; got != "delete" {
		t.Fatalf("unrelated topic config must survive the clamp, got %q", got)
	}
}

// A cluster that can satisfy the request must be left exactly as asked —
// otherwise the clamp would silently downgrade durability on real deployments.
func TestClampToClusterLeavesSatisfiableRequestsAlone(t *testing.T) {
	cfg := TopicConfig{
		Name:              "agent.control.commands.intent",
		ReplicationFactor: 3,
		Config:            map[string]string{"min.insync.replicas": "2"},
	}
	cfg.clampToCluster(3)
	if cfg.ReplicationFactor != 3 {
		t.Fatalf("RF=3 on a 3-broker cluster must stay 3, got %d", cfg.ReplicationFactor)
	}
	if got := cfg.Config["min.insync.replicas"]; got != "2" {
		t.Fatalf("min.insync.replicas must stay 2, got %q", got)
	}
}

// The clamp only ever lowers. A caller that deliberately asks for RF=1 — a cheap,
// transient topic — must not be silently upgraded to the cluster's broker count:
// that spends disk and inter-broker traffic the caller never asked for, and it
// makes the requested RF unobservable in the resulting topic.
func TestClampToClusterNeverRaisesReplicationFactor(t *testing.T) {
	cfg := TopicConfig{Name: "_rsync-signals.abc12345", ReplicationFactor: 1}
	cfg.clampToCluster(3)
	if cfg.ReplicationFactor != 1 {
		t.Fatalf("RF=1 on a 3-broker cluster must stay 1, got %d", cfg.ReplicationFactor)
	}
}

// An unknown broker count must not be treated as "zero brokers" — clamping RF to
// 0 would turn an unknown into a guaranteed-invalid topic spec.
func TestClampToClusterIsANoOpWhenBrokerCountIsUnknown(t *testing.T) {
	cfg := TopicConfig{Name: "cdc.abc12345", ReplicationFactor: 3}
	cfg.clampToCluster(0)
	if cfg.ReplicationFactor != 3 {
		t.Fatalf("unknown broker count must leave RF untouched, got %d", cfg.ReplicationFactor)
	}
}

// A caller that never set min.insync.replicas must not acquire one.
func TestClampToClusterDoesNotInventMinInsyncReplicas(t *testing.T) {
	cfg := TopicConfig{Name: "cdc.abc12345", ReplicationFactor: 3, Config: map[string]string{}}
	cfg.clampToCluster(1)
	if _, ok := cfg.Config["min.insync.replicas"]; ok {
		t.Fatal("clamp must not add min.insync.replicas that the caller never set")
	}
}

// normalizeTopicConfig is the shared entry point for BOTH creation paths. The
// clamp originally lived only in EnsureTopic, which left the planner-facing
// CreateTopic (POST /api/v1/topology/topics) unprotected — the one path that
// actually sends an over-large replication factor.

func TestNormalizePlannerPayloadOnSingleBrokerCluster(t *testing.T) {
	// Byte-for-byte the payload strategies.py sends: RF=3 unconditionally with
	// min.insync.replicas=min(2,rf). Against a 1-broker cluster this is what
	// used to come back as InvalidReplicationFactor.
	cfg := TopicConfig{
		Name:              "rsync.pipeline.abc",
		Partitions:        3,
		ReplicationFactor: 3,
		Config: map[string]string{
			"cleanup.policy":      "delete",
			"retention.ms":        "604800000",
			"min.insync.replicas": "2",
		},
	}
	normalizeTopicConfig(&cfg, 1)

	if cfg.ReplicationFactor != 1 {
		t.Errorf("replication factor = %d, want 1", cfg.ReplicationFactor)
	}
	if got := cfg.Config["min.insync.replicas"]; got != "1" {
		// RF=1 with misr=2 yields a topic that is created and then permanently
		// unwritable: every acks=all produce returns NOT_ENOUGH_REPLICAS.
		t.Errorf("min.insync.replicas = %q, want \"1\"", got)
	}
	if cfg.Config["retention.ms"] != "604800000" || cfg.Config["cleanup.policy"] != "delete" {
		t.Error("unrelated topic config was modified")
	}
}

func TestNormalizeAppliesDefaults(t *testing.T) {
	cfg := TopicConfig{Name: "t"}
	normalizeTopicConfig(&cfg, 0) // unknown cluster: defaults only, no clamping
	if cfg.Partitions != 3 {
		t.Errorf("partitions = %d, want 3", cfg.Partitions)
	}
	if cfg.ReplicationFactor != 1 {
		t.Errorf("replication factor = %d, want 1", cfg.ReplicationFactor)
	}
}

func TestNormalizeTreatsNegativesAsUnset(t *testing.T) {
	cfg := TopicConfig{Name: "t", Partitions: -5, ReplicationFactor: -2}
	normalizeTopicConfig(&cfg, 3)
	if cfg.Partitions != 3 || cfg.ReplicationFactor != 1 {
		t.Errorf("got partitions=%d rf=%d, want 3 and 1", cfg.Partitions, cfg.ReplicationFactor)
	}
}

func TestNormalizeLeavesSatisfiableRequestIntact(t *testing.T) {
	// A 3-broker cluster can honour the planner's intent as written.
	cfg := TopicConfig{
		Name: "t", Partitions: 6, ReplicationFactor: 3,
		Config: map[string]string{"min.insync.replicas": "2"},
	}
	normalizeTopicConfig(&cfg, 3)
	if cfg.ReplicationFactor != 3 {
		t.Errorf("replication factor = %d, want 3 (unchanged)", cfg.ReplicationFactor)
	}
	if cfg.Config["min.insync.replicas"] != "2" {
		t.Errorf("min.insync.replicas = %q, want \"2\" (unchanged)", cfg.Config["min.insync.replicas"])
	}
	if cfg.Partitions != 6 {
		t.Errorf("partitions = %d, want 6 (unchanged)", cfg.Partitions)
	}
}

// The invariant is min.insync.replicas <= RF, NOT "RF exceeded the broker count".
// Gating the whole clamp on the latter left every route that reaches an unsatisfiable
// misr without tripping the RF trigger completely unprotected — and a topic with
// misr > RF is created successfully, appears in ListTopics, accepts a subscription,
// and then rejects every acks=all produce with NOT_ENOUGH_REPLICAS forever.

// A caller that deliberately asks for a cheap RF=1 topic on a healthy 3-broker
// cluster never trips the RF trigger, so the paired misr=2 used to survive intact.
func TestClampToClusterHoldsInvariantWhenReplicationFactorFits(t *testing.T) {
	cfg := TopicConfig{
		Name:              "cdc.abc12345",
		ReplicationFactor: 1,
		Config:            map[string]string{"min.insync.replicas": "2"},
	}
	cfg.clampToCluster(3)
	if cfg.ReplicationFactor != 1 {
		t.Fatalf("RF must stay 1, got %d", cfg.ReplicationFactor)
	}
	if got := cfg.Config["min.insync.replicas"]; got != "1" {
		t.Fatalf("min.insync.replicas = %q, want \"1\": misr 2 > RF 1 is a topic that "+
			"exists and is permanently unwritable, whether or not the RF was clamped", got)
	}
}

// Same hole one step up: RF=2 fits a 3-broker cluster, so misr=3 rode through.
func TestClampToClusterHoldsInvariantAboveOne(t *testing.T) {
	cfg := TopicConfig{
		Name:              "cdc.abc12345",
		ReplicationFactor: 2,
		Config:            map[string]string{"min.insync.replicas": "3"},
	}
	cfg.clampToCluster(3)
	if got := cfg.Config["min.insync.replicas"]; got != "2" {
		t.Fatalf("min.insync.replicas = %q, want \"2\" (RF)", got)
	}
}

// An unknown broker count disables the RF clamp — it must NOT disable the invariant.
// misr <= RF is checkable without seeing the cluster at all.
func TestClampToClusterHoldsInvariantWhenBrokerCountIsUnknown(t *testing.T) {
	cfg := TopicConfig{
		Name:              "cdc.abc12345",
		ReplicationFactor: 1,
		Config:            map[string]string{"min.insync.replicas": "2"},
	}
	cfg.clampToCluster(0)
	if got := cfg.Config["min.insync.replicas"]; got != "1" {
		t.Fatalf("min.insync.replicas = %q, want \"1\"", got)
	}
}
