package kafka

import (
	"errors"
	"sort"
	"strconv"
	"testing"

	"github.com/IBM/sarama"
)

// EnsureTopicExists is the path that actually runs: executor.go pre-creates the
// Debezium CDC topics through it on every CDC start, and cdc_incremental.go pre-creates
// the incremental-snapshot signal topic. It used to build its sarama.TopicDetail by
// hand — ReplicationFactor: 1, no ConfigEntries at all — so none of the clamping on the
// EnsureTopic/CreateTopic routes applied to it. These tests read back the detail that
// actually reaches the broker.
//
// They drive ensureAuthoritativeTopic, which is the TopicConfig EnsureTopicExists
// itself builds, so what is asserted is the production request rather than a config
// the test invented.

func misrOf(t *testing.T, d *sarama.TopicDetail) string {
	t.Helper()
	if d == nil {
		t.Fatal("topic was never created")
	}
	v := d.ConfigEntries[minInsyncReplicasKey]
	if v == nil {
		t.Fatalf("no %s in ConfigEntries — the topic inherits the BROKER default, which is "+
			"2 on MSK and every managed cluster; with RF=%d that topic is created, listed, "+
			"subscribable, and permanently unwritable", minInsyncReplicasKey, d.ReplicationFactor)
	}
	return *v
}

// The MSK failure, exactly: a single-broker view, RF=1, and a broker whose default
// min.insync.replicas is 2. The only defence is sending an explicit floor.
func TestEnsureTopicExistsPinsAFloorTheBrokerCannotOverride(t *testing.T) {
	tm, admin := newFakeManager(1)
	if err := ensureAuthoritativeTopic(tm, "dbz.public.orders", 3, nil); err != nil {
		t.Fatalf("ensureAuthoritativeTopic: %v", err)
	}
	d := admin.created["dbz.public.orders"]
	if d == nil {
		t.Fatal("topic was never created")
	}
	if d.ReplicationFactor != 1 {
		t.Errorf("replication factor = %d, want 1", d.ReplicationFactor)
	}
	if got := misrOf(t, d); got != "1" {
		t.Errorf("%s = %q, want \"1\" (anything above RF=1 makes every acks=all produce "+
			"return NOT_ENOUGH_REPLICAS forever)", minInsyncReplicasKey, got)
	}
	if d.NumPartitions != 3 {
		t.Errorf("partitions = %d, want 3", d.NumPartitions)
	}
}

// The floor must hold whatever replication factor the spec ends up with, so the
// invariant cannot be reopened by a future change to the RF default.
func TestEnsureTopicExistsKeepsTheFloorAtOrBelowTheReplicationFactor(t *testing.T) {
	for _, brokers := range []int{0, 1, 2, 3, 9} {
		tm, admin := newFakeManager(brokers)
		if err := ensureAuthoritativeTopic(tm, "dbz.public.orders", 3, nil); err != nil {
			t.Fatalf("brokers=%d: %v", brokers, err)
		}
		d := admin.created["dbz.public.orders"]
		if d == nil {
			t.Fatalf("brokers=%d: topic was never created", brokers)
		}
		rf := int(d.ReplicationFactor)
		if rf < 1 {
			t.Fatalf("brokers=%d: replication factor %d is not a valid topic spec", brokers, rf)
		}
		raw := misrOf(t, d)
		misr, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("brokers=%d: %s = %q is not an integer", brokers, minInsyncReplicasKey, raw)
		}
		if misr < 1 || misr > rf {
			t.Errorf("brokers=%d: %s = %d, want 1..%d (the replication factor)",
				brokers, minInsyncReplicasKey, misr, rf)
		}
	}
}

// The signal topic arrives here already qualified (executor.go mints it via
// kafkaclient.Topic), and the Debezium topics carry Debezium's own topic.prefix
// (connector.py qualifies it with the same KAFKA_TOPIC_PREFIX). Re-qualifying either
// would create a topic under a different name from the one the sink subscribes to —
// a silent zero-rows pipeline, which is the failure this whole change exists to remove.
func TestEnsureTopicExistsDoesNotRewriteTheTopicName(t *testing.T) {
	const name = "rsync.signals.abc12345"
	tm, admin := newFakeManager(1)
	if err := ensureAuthoritativeTopic(tm, name, 1, nil); err != nil {
		t.Fatalf("ensureAuthoritativeTopic: %v", err)
	}
	d := admin.created[name]
	if d == nil {
		t.Fatalf("created %v, want the topic under %q unchanged", keysOf(admin.created), name)
	}
	if d.NumPartitions != 1 {
		t.Errorf("partitions = %d, want the caller's 1", d.NumPartitions)
	}
}

// Same rule when the two sides DISAGREE about the namespace — which is the case that
// actually costs data. Debezium reads KAFKA_TOPIC_PREFIX in its own container; if it
// resolves to something else, its topics arrive here bare. Qualifying them then would
// mint a third name that Debezium never writes to and the sink never reads from, and
// the pipeline would report running while moving nothing. The name the other component
// owns is the name we create.
func TestEnsureTopicExistsDoesNotQualifyAForeignlyNamedTopic(t *testing.T) {
	const name = "cdc-abc12345.public.orders" // no rsync. prefix
	tm, admin := newFakeManager(1)
	if err := ensureAuthoritativeTopic(tm, name, 3, nil); err != nil {
		t.Fatalf("ensureAuthoritativeTopic: %v", err)
	}
	if admin.created[name] == nil {
		t.Errorf("created %v, want exactly %q — a name another component chose must not be rewritten",
			keysOf(admin.created), name)
	}
}

// The CDC topics are KEYED. Kafka maps a key to a partition modulo the partition
// count, so growing an existing topic re-routes keys and silently destroys the
// per-table ordering CDC correctness rests on — with no error anywhere. Routing this
// path through EnsureTopic, which does grow the control topics on purpose, is only
// safe because the CDC path opts out.
func TestEnsureTopicExistsNeverRepartitionsAnExistingTopic(t *testing.T) {
	const name = "rsync.cdc-abc12345.public.orders"
	tm, admin := newFakeManager(3)
	admin.existing[name] = sarama.TopicDetail{NumPartitions: 1, ReplicationFactor: 1}

	if err := ensureAuthoritativeTopic(tm, name, 3, nil); err != nil {
		t.Fatalf("ensureAuthoritativeTopic: %v", err)
	}
	if got, ok := admin.repartitioned[name]; ok {
		t.Errorf("grew %q from 1 to %d partitions — every existing key now hashes to a "+
			"different partition and per-key CDC ordering is broken, silently", name, got)
	}
	if admin.created[name] != nil {
		t.Errorf("re-created %q even though it already exists", name)
	}
}

// Concurrent pre-creation is normal here (the sink start path is retried), so the race
// must stay a success rather than failing a CDC start.
func TestEnsureTopicExistsTreatsAnExistingTopicAsSuccess(t *testing.T) {
	tm, admin := newFakeManager(1)
	admin.createErr = errors.New("kafka server: Topic with this name already exists")
	if err := ensureAuthoritativeTopic(tm, "dbz.public.orders", 3, nil); err != nil {
		t.Fatalf("an already-exists race must not fail the CDC start, got %v", err)
	}
}

func TestEnsureTopicExistsSurfacesRealErrors(t *testing.T) {
	tm, admin := newFakeManager(1)
	admin.createErr = errors.New("kafka server: Replication-factor is invalid")
	err := ensureAuthoritativeTopic(tm, "dbz.public.orders", 3, nil)
	if err == nil {
		t.Fatal("a genuine creation failure must be returned")
	}
	if !errors.Is(err, admin.createErr) {
		t.Errorf("error %v does not wrap the broker's, so the cause is unrecoverable from the log", err)
	}
}

func keysOf(m map[string]*sarama.TopicDetail) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// The Debezium schema-history topic is the reason EnsureTopicExists grew a
// config-carrying sibling. Its geometry is not a preference:
//
//	cleanup.policy=delete  the records are NOT keyed per schema object, so "compact"
//	                       drops DDL that is still needed — and "compact" is the
//	                       plausible-looking wrong answer, since it is what Kafka's
//	                       other internal topics use.
//	retention.ms=-1        forever. The bundled broker's 7-day default silently
//	                       expires the history, and the connector then fails on its
//	                       first RESTART after expiry rather than on first run.
//
// Neither setting is expressible through the old two-argument signature, so before
// this change the topic could only be born with the broker's defaults. A pre-create
// that reaches the broker WITHOUT these entries is indistinguishable from no
// pre-create at all, which is exactly the shape of failure this test exists to catch.
func TestEnsureTopicExistsWithConfigCarriesConfigEntriesToTheBroker(t *testing.T) {
	const name = "rsync.schemahistory.cdc-abc12345"
	tm, admin := newFakeManager(1)

	caller := map[string]string{
		"cleanup.policy": "delete",
		"retention.ms":   "-1",
	}
	if err := ensureAuthoritativeTopic(tm, name, 1, caller); err != nil {
		t.Fatalf("ensureAuthoritativeTopic: %v", err)
	}

	d := admin.created[name]
	if d == nil {
		t.Fatalf("created %v, want %q", keysOf(admin.created), name)
	}
	for k, want := range map[string]string{"cleanup.policy": "delete", "retention.ms": "-1"} {
		got, ok := d.ConfigEntries[k]
		if !ok || got == nil {
			t.Errorf("%s never reached the broker — the topic is born with the broker's "+
				"default, which is the defect the pre-create exists to remove", k)
			continue
		}
		if *got != want {
			t.Errorf("%s = %q, want %q", k, *got, want)
		}
	}

	// The caller's entries must not displace the durability floor: both have to
	// survive together, or fixing retention reopens the NOT_ENOUGH_REPLICAS bug.
	if got := misrOf(t, d); got != "1" {
		t.Errorf("%s = %q, want \"1\" — caller-supplied config entries must be MERGED with "+
			"the replication policy, not substituted for it", minInsyncReplicasKey, got)
	}

	// The policy WRITES min.insync.replicas into the config map it is handed. If that
	// map is the caller's, a caller reusing one across topics (or reading it back)
	// silently acquires a floor it never asked for.
	if _, leaked := caller[minInsyncReplicasKey]; leaked {
		t.Errorf("the caller's map was mutated: %v — ensureAuthoritativeTopic must copy "+
			"before handing it to the replication policy", caller)
	}
	if len(caller) != 2 {
		t.Errorf("the caller's map now has %d entries, want 2 — it was written through", len(caller))
	}
}

// A caller-supplied min.insync.replicas is still bounded by the replication factor.
// The config-carrying route must not become a way around the clamp: misr above RF is a
// topic that is created, listed, subscribable and permanently unwritable.
func TestEnsureTopicExistsWithConfigStillClampsACallerSuppliedFloor(t *testing.T) {
	const name = "rsync.schemahistory.cdc-abc12345"
	tm, admin := newFakeManager(1) // one broker => RF 1
	caller := map[string]string{
		"cleanup.policy":     "delete",
		minInsyncReplicasKey: "5",
	}
	if err := ensureAuthoritativeTopic(tm, name, 1, caller); err != nil {
		t.Fatalf("ensureAuthoritativeTopic: %v", err)
	}
	d := admin.created[name]
	if got := misrOf(t, d); got != "1" {
		t.Errorf("%s = %q, want \"1\" — a caller floor above the replication factor makes "+
			"every acks=all produce return NOT_ENOUGH_REPLICAS forever",
			minInsyncReplicasKey, got)
	}
	if caller[minInsyncReplicasKey] != "5" {
		t.Errorf("the caller's map was clamped in place (%q) — the copy is what should be "+
			"modified", caller[minInsyncReplicasKey])
	}
}

// EnsureTopicExists (two arguments) is still the path executor.go and cdc_incremental.go
// take for the CDC data topics and the incremental-snapshot signal topic. It now
// delegates with a nil config, and nil must behave exactly as it did before: the
// replication floor is still pinned, and nothing else is added.
func TestEnsureTopicExistsStillPassesNilConfig(t *testing.T) {
	const name = "rsync.cdc-abc12345.public.orders"
	tm, admin := newFakeManager(1)
	if err := ensureAuthoritativeTopic(tm, name, 3, nil); err != nil {
		t.Fatalf("a nil config must not fail (or panic): %v", err)
	}
	d := admin.created[name]
	if d == nil {
		t.Fatalf("created %v, want %q", keysOf(admin.created), name)
	}
	if got := misrOf(t, d); got != "1" {
		t.Errorf("%s = %q, want \"1\"", minInsyncReplicasKey, got)
	}
	if len(d.ConfigEntries) != 1 {
		t.Errorf("ConfigEntries = %v, want only %s — the nil path must not acquire "+
			"settings the CDC data topics never asked for",
			entryNames(d.ConfigEntries), minInsyncReplicasKey)
	}
	if d.NumPartitions != 3 {
		t.Errorf("partitions = %d, want the caller's 3", d.NumPartitions)
	}
}

func entryNames(m map[string]*string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
