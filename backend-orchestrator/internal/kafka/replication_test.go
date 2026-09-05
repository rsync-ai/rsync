package kafka

import (
	"os"
	"testing"
)

// Nothing in this platform read a replication factor from anywhere before this:
// `grep -rn REPLICATION_FACTOR --include='*.go' --include='*.py'` returned zero hits,
// so no deployment could ask for RF=3 even on a cluster that supports it.

func TestReplicationPolicyReadsTheEnvironment(t *testing.T) {
	t.Setenv(EnvReplicationFactor, "3")
	t.Setenv(EnvMinInsyncReplicas, "2")

	// Deliberately not replicationDefaults(): that latches once per process, so a
	// test calling it would prove nothing about which variables were read.
	got := readReplicationPolicyFromEnv()
	if got.replicationFactor != 3 {
		t.Errorf("%s=3 → replicationFactor %d, want 3", EnvReplicationFactor, got.replicationFactor)
	}
	if got.minInsyncReplicas != 2 {
		t.Errorf("%s=2 → minInsyncReplicas %d, want 2", EnvMinInsyncReplicas, got.minInsyncReplicas)
	}
}

func TestReplicationPolicyEnvNamesAreTheDocumentedOnes(t *testing.T) {
	// The variable names are the whole contract with the compose files and the Helm
	// chart, which are another agent's to wire. A rename here is a silent no-op there.
	if EnvReplicationFactor != "KAFKA_REPLICATION_FACTOR" {
		t.Errorf("EnvReplicationFactor = %q", EnvReplicationFactor)
	}
	if EnvMinInsyncReplicas != "KAFKA_MIN_INSYNC_REPLICAS" {
		t.Errorf("EnvMinInsyncReplicas = %q", EnvMinInsyncReplicas)
	}
	if _, set := os.LookupEnv(EnvReplicationFactor); set {
		t.Skip("environment already sets " + EnvReplicationFactor)
	}
}

// A typo in a durability knob must degrade to the built-in default, not fail the boot
// of a deployment that was working the release before.
func TestReplicationPolicyIgnoresMalformedValues(t *testing.T) {
	for _, raw := range []string{"", "   ", "three", "0", "-1", "2.5"} {
		p := parseReplicationPolicy(raw, raw)
		if p.replicationFactor != 0 || p.minInsyncReplicas != 0 {
			t.Errorf("%q → %+v, want the zero policy (fall back to built-in defaults)", raw, p)
		}
		if got := p.replicationFactorOr(1); got != 1 {
			t.Errorf("%q → replicationFactorOr(1) = %d, want 1", raw, got)
		}
	}
}

// forCluster is what the paths that mint their own topics use. With nothing set it has
// to reproduce the derivation they already had, or this becomes a durability change
// nobody asked for.
func TestForClusterPreservesTheHistoricalDerivation(t *testing.T) {
	unset := replicationPolicy{}
	for _, tc := range []struct {
		brokers int
		want    int16
	}{{0, 1}, {1, 1}, {2, 2}, {3, 3}, {9, 3}} {
		if got := unset.forCluster(tc.brokers); got != tc.want {
			t.Errorf("forCluster(%d) = %d, want %d", tc.brokers, got, tc.want)
		}
	}
}

func TestForClusterHonoursAnExplicitRequest(t *testing.T) {
	asked := replicationPolicy{replicationFactor: 3}
	if got := asked.forCluster(9); got != 3 {
		t.Errorf("forCluster(9) = %d, want the requested 3 (not the derived min(3,9))", got)
	}
	// The request is still only a request: applyTo clamps it to what exists, so a
	// KAFKA_REPLICATION_FACTOR that overshoots degrades instead of failing every
	// topic creation with InvalidReplicationFactor.
	cfg := TopicConfig{Name: "cdc.abc12345"}
	replicationPolicy{replicationFactor: 30}.applyTo(&cfg, 2, 1)
	if cfg.ReplicationFactor != 2 {
		t.Errorf("RF=30 on a 2-broker cluster = %d, want 2", cfg.ReplicationFactor)
	}
	if got := cfg.Config[minInsyncReplicasKey]; got != "2" {
		t.Errorf("%s = %q, want \"2\"", minInsyncReplicasKey, got)
	}
}

// An operator's floor is honoured, and then still bounded by the RF that survived the
// clamp — misr > RF is unsatisfiable no matter who asked for it.
func TestApplyToHonoursAndBoundsTheOperatorsFloor(t *testing.T) {
	cfg := TopicConfig{Name: "cdc.abc12345"}
	replicationPolicy{replicationFactor: 3, minInsyncReplicas: 3}.applyTo(&cfg, 3, 1)
	if cfg.ReplicationFactor != 3 || cfg.Config[minInsyncReplicasKey] != "3" {
		t.Errorf("got RF=%d misr=%q, want 3 and \"3\"", cfg.ReplicationFactor, cfg.Config[minInsyncReplicasKey])
	}

	// Same request against a cluster that cannot serve it.
	cfg = TopicConfig{Name: "cdc.abc12345"}
	replicationPolicy{replicationFactor: 3, minInsyncReplicas: 3}.applyTo(&cfg, 1, 1)
	if cfg.ReplicationFactor != 1 {
		t.Errorf("RF = %d, want 1", cfg.ReplicationFactor)
	}
	if got := cfg.Config[minInsyncReplicasKey]; got != "1" {
		t.Errorf("%s = %q, want \"1\": a floor above the RF makes the topic unwritable", minInsyncReplicasKey, got)
	}
}

// pinMinInsyncReplicas is the half that fixes the MSK failure: silence does not mean
// "no floor", it means the broker's default applies, and that default is 2 on MSK.
func TestPinMinInsyncReplicasNeverExceedsTheReplicationFactor(t *testing.T) {
	for _, rf := range []int16{1, 2, 3, 5} {
		cfg := TopicConfig{Name: "cdc.abc12345", ReplicationFactor: rf}
		replicationPolicy{}.pinMinInsyncReplicas(&cfg)
		raw, ok := cfg.Config[minInsyncReplicasKey]
		if !ok {
			t.Fatalf("RF=%d: no %s written — the broker default applies and it is invisible from here", rf, minInsyncReplicasKey)
		}
		want := "2"
		if rf < 2 {
			want = "1"
		}
		if raw != want {
			t.Errorf("RF=%d: %s = %q, want %q", rf, minInsyncReplicasKey, raw, want)
		}
	}
}

// A caller who stated a floor keeps it — pinning is a default, not an override.
func TestPinMinInsyncReplicasDoesNotOverrideTheCaller(t *testing.T) {
	cfg := TopicConfig{
		Name:              "cdc.abc12345",
		ReplicationFactor: 3,
		Config:            map[string]string{minInsyncReplicasKey: "1"},
	}
	replicationPolicy{minInsyncReplicas: 2}.pinMinInsyncReplicas(&cfg)
	if got := cfg.Config[minInsyncReplicasKey]; got != "1" {
		t.Errorf("%s = %q, want the caller's \"1\"", minInsyncReplicasKey, got)
	}
}
