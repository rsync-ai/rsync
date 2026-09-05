package cdcstats

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/IBM/sarama"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// A6. Both consumer groups this agent opens connected with the client library's
// anonymous default client.id, so on a customer-managed cluster an rsync
// connection was indistinguishable from any other tenant's in the broker's
// request logs, quota buckets and authorization denials.

func TestNewStampsTheServiceClientID(t *testing.T) {
	// A zero Manager is enough: what is under test is what New adds to whatever
	// the manager resolved, not what the manager resolves.
	a := New(nil, &kafka.Manager{})

	got := a.security.ClientID
	if want := kafkaclient.DefaultClientID(kafkaServiceName); got != want {
		t.Fatalf("ClientID = %q, want %q", got, want)
	}
	if got == "" || got == kafkaclient.ClientIDNamespace {
		t.Fatalf("ClientID = %q, which does not name the process", got)
	}
}

func TestSecuredCarriesTheClientIDOntoEveryConsumer(t *testing.T) {
	// The stamp is only worth anything if it reaches the sarama config both
	// consumer groups are built from.
	a := New(nil, &kafka.Manager{})
	// saramaauth.Apply validates before it stamps, so the zero manager's Config
	// needs an address and a protocol — the shape FromEnv would have given it.
	// The client.id is what the assertions look at.
	a.security = a.security.WithBrokers("kafka:29092")
	a.security.SecurityProtocol = kafkaclient.ProtocolPlaintext

	for name, base := range map[string]*sarama.Config{
		"table stats":    newConsumerConfigOldest(),
		"schema changes": newSchemaChangeConsumerConfig(),
	} {
		cfg, err := a.secured(base)
		if err != nil {
			t.Fatalf("%s: secured: %v", name, err)
		}
		if want := kafkaclient.DefaultClientID(kafkaServiceName); cfg.ClientID != want {
			t.Fatalf("%s: ClientID = %q, want %q", name, cfg.ClientID, want)
		}
	}
}

func TestNilKafkaManagerStillYieldsAnAgent(t *testing.T) {
	// Start() refuses to run without a manager, but New must not panic on the
	// path that builds the agent before that check.
	if a := New(nil, nil); a == nil {
		t.Fatal("New returned nil")
	}
}

// === Consumer group namespacing ===
//
// The client.id above says WHO is connecting. These say WHAT the connection asks
// to join, and they are the half that fails silently: an unqualified group id on
// a cluster with PREFIXED group ACLs is denied at JoinGroup, and a denied join
// produces a consumer that never gets a partition assignment rather than an
// error anyone sees.

// groupMinters is every function in this package that names a consumer group.
// The tests below assert a PROPERTY over this set rather than three string
// literals, so a fourth consumer added to this package is covered the moment its
// minter is listed here — and the source scan at the bottom fails if a fourth
// consumer is opened without one.
var groupMinters = map[string]func(string) string{
	"table stats":    tableStatsGroupID,
	"schema changes": schemaChangeGroupID,
}

// testPipelineUUIDs covers the shapes ensureWorker actually passes: a normal
// pipeline UUID, and the degenerate empty one it would pass if a row arrived
// with no id.
var testPipelineUUIDs = []string{
	"abd8a64d-1234-4000-8000-000000000001",
	"0157d295-0000-4000-8000-00000000beef",
	"",
}

func TestEveryGroupIDIsNamespaced(t *testing.T) {
	// Three namespaces: the shipped default, a customer's own, and one written
	// without a trailing separator (TopicPrefix appends one, and a group that
	// missed that would run its prefix into its name).
	for _, prefixEnv := range []struct {
		name string
		set  bool
		val  string
	}{
		{name: "default", set: false},
		{name: "customer", set: true, val: "acme."},
		{name: "no trailing separator", set: true, val: "acme"},
	} {
		t.Run(prefixEnv.name, func(t *testing.T) {
			if prefixEnv.set {
				t.Setenv(kafkaclient.EnvTopicPrefix, prefixEnv.val)
			} else {
				os.Unsetenv(kafkaclient.EnvTopicPrefix)
			}
			want := kafkaclient.TopicPrefix()
			if want == "" {
				t.Fatalf("test setup: prefix resolved empty for %q", prefixEnv.val)
			}

			for minter, mint := range groupMinters {
				for _, pid := range testPipelineUUIDs {
					got := mint(pid)
					if !strings.HasPrefix(got, want) {
						t.Errorf("%s(%q) = %q, which is outside the configured namespace %q",
							minter, pid, got, want)
					}
				}
			}
		})
	}
}

// The empty prefix is the documented migration lever — an existing deployment
// has committed offsets under the bare names, and renaming a group abandons
// them. Setting it empty must mean "do not qualify", never "use the default".
func TestGroupIDsHonorTheEmptyPrefixMigrationLever(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "")

	const pid = "abd8a64d-1234-4000-8000-000000000001"
	for name, want := range map[string]string{
		"table stats":    tableStatsGroupPrefix + pid,
		"schema changes": schemaChangeGroupPrefix + pid,
	} {
		if got := groupMinters[name](pid); got != want {
			t.Errorf("%s group = %q, want the unqualified %q", name, got, want)
		}
	}
}

// Group ids are read back from the broker (cdc_kafka_teardown.go lists groups
// and matches them) and re-qualified on the way, so a second pass must not
// produce rsync.rsync.cdc-table-stats-….
func TestGroupIDQualificationIsIdempotent(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "rsync.")

	for minter, mint := range groupMinters {
		for _, pid := range testPipelineUUIDs {
			got := mint(pid)
			if requalified := kafkaclient.Group(got); requalified != got {
				t.Errorf("%s: re-qualifying %q gave %q", minter, got, requalified)
			}
		}
	}
}

// The two groups want opposite starting offsets and must not share a lag or a
// rebalance (see pipelineWorker.ddlConsumer). Namespacing must not collapse them
// onto one name.
func TestGroupIDsStayDistinctPerPipeline(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "acme.")

	seen := map[string]string{}
	for minter, mint := range groupMinters {
		for _, pid := range testPipelineUUIDs {
			got := mint(pid)
			if prev, dup := seen[got]; dup {
				t.Fatalf("%s and %s collided on %q", prev, minter, got)
			}
			seen[got] = minter
		}
	}
}

// This package's half of the teardown contract: the spelling it is obliged to
// keep. cdc_kafka_teardown.go reaps "cdc-table-stats-<uuid>" and
// "cdc-schema-changes-<uuid>"; shortening the id to SafeID8 or renaming a base
// here strands the group, and a stranded group leaks silently — the delete
// succeeds and the sweep simply matches nothing.
//
// It does NOT verify that the sweep still recognizes these, and it cannot:
// ownsGroup is unexported in internal/handlers, and both sides of any comparison
// written here would come out of this package. That half is
// TestOwnsGroupCoversTheGroupsCDCStatsActuallyMints in internal/handlers, which
// calls the real ownsGroup and reads these prefixes back out of this file's
// source. Verified: dropping ownsGroup's qualified-spelling arm leaves this test
// green and fails that one.
func TestGroupIDSpellingTheTeardownSweepDependsOn(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "rsync.")

	const uuid = "abd8a64d-1234-4000-8000-000000000001"
	for name, base := range map[string]string{
		"table stats":    "cdc-table-stats-" + uuid,
		"schema changes": "cdc-schema-changes-" + uuid,
	} {
		got := groupMinters[name](uuid)
		if want := kafkaclient.Group(base); got != want {
			t.Errorf("%s group = %q, want %q — the teardown sweep keys on that base", name, got, want)
		}
		// The full uuid, not SafeID8. ownsGroup anchors on the whole uuid, so a
		// shortened id is matched by nothing and leaks.
		if !strings.Contains(got, uuid) {
			t.Errorf("%s group = %q does not carry the full pipeline uuid; the sweep keys on it", name, got)
		}
	}
}

// The structural guard: a stranded call site is one that opens a consumer group
// without going through Agent.consumerGroup, and no property test over the
// minters can see it. This one can — it fails the moment sarama.NewConsumerGroup
// appears anywhere in this package but kafka_identity.go.
func TestOnlyKafkaIdentityOpensAConsumerGroup(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(files) == 0 {
		t.Fatal("no sources found; the scan would pass vacuously")
	}

	found := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		n := strings.Count(string(data), "sarama.NewConsumerGroup(")
		if n == 0 {
			continue
		}
		found += n
		if f != "kafka_identity.go" {
			t.Errorf("%s opens a consumer group directly (%d call site(s)); route it through "+
				"Agent.consumerGroup so the id is namespaced", f, n)
		}
	}
	if found == 0 {
		t.Fatal("no sarama.NewConsumerGroup call sites found at all — this guard has stopped guarding anything")
	}
}
