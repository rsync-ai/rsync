package consumer

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// noDockerTransport makes DockerSpawner's availability probe fail, so Spawn takes
// the simulate path without any daemon being contacted.
type noDockerTransport struct{}

func (noDockerTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("docker deliberately unavailable in tests")
}

// Consumer group namespacing.
//
// This agent mints group ids in one process and joins them in another (it hands
// CONSUMER_GROUP_ID to a spawned container), which is why an unqualified id here
// is so quiet: on a cluster with PREFIXED group ACLs the broker denies the
// JoinGroup, the container stays up, the health monitor keeps reporting it
// alive, and no partition is ever assigned. There is no error for anyone to
// read. The tests below assert the PROPERTY — every group id this package builds
// carries the configured namespace — rather than three string literals, so a
// fourth call site is covered by listing its minter, and the structural guards
// at the bottom fail if a fourth call site appears without one.

// testTopics are the shapes decision.Topic / req.Topic actually carry, including
// the already-qualified spelling a topic name reaches this agent with once
// kafkaclient.Topic has been applied upstream.
var testTopics = []string{
	"cdc.abd8a64d",
	"rsync.cdc.abd8a64d",
	"conn.postgres.orders",
	"",
}

// groupMinters is every entry point in this package that produces a consumer
// group id: the raw gate, and the topic-derived minter the two scaling paths
// share.
func groupMinters(c *Config) map[string]func(string) string {
	return map[string]func(string) string{
		"consumerGroupID": consumerGroupID,
		"groupIDForTopic": c.groupIDForTopic,
	}
}

func TestEveryConsumerGroupIDIsNamespaced(t *testing.T) {
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

			cfg := DefaultConfig()
			for minter, mint := range groupMinters(cfg) {
				for _, topic := range testTopics {
					if topic == "" && minter == "consumerGroupID" {
						// Group("") is defined to stay empty — an empty group id is
						// a caller bug, not something to namespace.
						continue
					}
					got := mint(topic)
					if !strings.HasPrefix(got, want) {
						t.Errorf("%s(%q) = %q, which is outside the configured namespace %q",
							minter, topic, got, want)
					}
				}
			}
		})
	}
}

// The DECISION in kafka_identity.go, asserted rather than only argued: a group
// id the operator supplied is qualified like any other, because the whole point
// of one namespace is that ONE PREFIXED ACL covers every group this product
// joins.
func TestOperatorSuppliedGroupPrefixIsStillNamespaced(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "acme.")
	t.Setenv("CONSUMER_GROUP_PREFIX", "ops-ingest")

	got := FromEnv().groupIDForTopic("cdc.abd8a64d")
	if !strings.HasPrefix(got, "acme.") {
		t.Fatalf("group %q is outside the namespace — an operator-set prefix must not opt out of the ACL grant", got)
	}
	if !strings.Contains(got, "ops-ingest") {
		t.Fatalf("group %q lost the operator's prefix", got)
	}
}

// ...and the escape hatch that makes the decision defensible: the same lever
// topics use turns qualification off for groups, so an operator who needs their
// literal string gets it for topics and groups together instead of a group id
// that silently disagrees with the topics it reads.
func TestConsumerGroupsHonorTheEmptyPrefixMigrationLever(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "")
	t.Setenv("CONSUMER_GROUP_PREFIX", "")

	cfg := DefaultConfig()
	if got, want := cfg.groupIDForTopic("cdc.abd8a64d"), "rsync-pipeline-cdc.abd8a64d"; got != want {
		t.Errorf("groupIDForTopic = %q, want the unqualified %q", got, want)
	}
	if got, want := consumerGroupID("caller-supplied-group"), "caller-supplied-group"; got != want {
		t.Errorf("consumerGroupID = %q, want the unqualified %q", got, want)
	}
}

// registry.go RestartConsumer respawns from oldInfo.GroupID, which SpawnConsumer
// already qualified. A second pass must leave it alone: minting
// rsync.rsync.rsync-pipeline-… on restart would abandon the group's committed
// offsets and restart it from auto.offset.reset.
func TestRestartReusingAStoredGroupIDDoesNotRenameIt(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "rsync.")

	cfg := DefaultConfig()
	for _, topic := range testTopics {
		stored := cfg.groupIDForTopic(topic) // what SpawnConsumer wrote into ConsumerInfo
		if respawned := consumerGroupID(stored); respawned != stored {
			t.Errorf("restart of %q rejoined as %q", stored, respawned)
		}
	}
	// Same guarantee for an operator who spells the prefix into their own value.
	if got := consumerGroupID("rsync.acme-ingest"); got != "rsync.acme-ingest" {
		t.Errorf("double-prefixed an already-qualified operator value: %q", got)
	}
}

// The two scaling paths built this concatenation by hand, in two different
// spellings. They are one function now, and this is what keeps a third from
// appearing: any new reference to ConsumerGroupPrefix outside the minter is a
// call site that has to remember to qualify, which is the thing that failed.
func TestConsumerGroupPrefixIsOnlyReferencedByTheMinter(t *testing.T) {
	allowed := map[string]bool{
		"config.go":         true, // declaration, default, CONSUMER_GROUP_PREFIX override
		"kafka_identity.go": true, // the minter
	}

	for file, src := range packageSources(t) {
		if allowed[file] {
			continue
		}
		if strings.Contains(src, "ConsumerGroupPrefix") {
			t.Errorf("%s builds a group id from ConsumerGroupPrefix directly; "+
				"use Config.groupIDForTopic so it is namespaced", file)
		}
	}
}

// The runtime half of the chokepoint proof, on the path that had no other
// qualification anywhere: handlers.SpawnConsumers takes group_id verbatim from
// the HTTP request body and passes it straight to Registry.SpawnConsumer. What
// the broker sees is what lands in ConsumerInfo.GroupID and in the spawned
// container's CONSUMER_GROUP_ID.
//
// The spawner's Docker transport is replaced rather than trusted to be absent.
// This developer machine HAS a Docker socket, so DockerSpawner.isDockerAvailable
// would answer true and the test would try to create a real container; forcing
// the transport to fail drives it down the simulate path deterministically, on
// any machine, without touching a daemon.
func TestSpawnConsumerNamespacesAnAPISuppliedGroupID(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "acme.")

	registry, err := NewRegistry(DefaultConfig(), false, false)
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	registry.spawner.spawner.httpClient = &http.Client{Transport: noDockerTransport{}}

	// Exactly what SpawnRequest.GroupID delivers: a caller-chosen string that
	// nothing else in the request path touches.
	info, err := registry.SpawnConsumer(t.Context(), "caller-supplied-group", "cdc.abd8a64d", "pipeline-1", nil, nil)
	if err != nil {
		t.Fatalf("SpawnConsumer: %v", err)
	}
	if want := "acme.caller-supplied-group"; info.GroupID != want {
		t.Fatalf("ConsumerInfo.GroupID = %q, want %q", info.GroupID, want)
	}
	// The health monitor keys lag and liveness off the same id; if it recorded
	// the bare one, the agent would be watching a group nobody joined.
	if info.Health == nil || info.Health.GroupID != info.GroupID {
		t.Fatalf("health monitor recorded a different group id: %+v", info.Health)
	}
}

// The structural guard for the chokepoint. Every group id that reaches a spawned
// consumer goes through a `.spawner.Spawn` call, and the one in
// Registry.SpawnConsumer is the last place it can be qualified — the HTTP
// SpawnRequest path has no other. If a third spawn site appears, or if
// SpawnConsumer stops qualifying, this fails.
func TestEverySpawnSiteIsQualifiedOrDeliberate(t *testing.T) {
	fset := token.NewFileSet()
	sites := map[string]string{} // "file:func" -> "qualified" | "delegate"

	for file := range packageSources(t) {
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			spawns, qualifies := false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if fun.Name == "consumerGroupID" {
						qualifies = true
					}
				case *ast.SelectorExpr:
					// `<anything>.spawner.Spawn(...)`
					if fun.Sel.Name == "Spawn" {
						if inner, ok := fun.X.(*ast.SelectorExpr); ok && inner.Sel.Name == "spawner" {
							spawns = true
						}
					}
				}
				return true
			})
			if spawns {
				kind := "unqualified"
				if qualifies {
					kind = "qualified"
				}
				sites[file+":"+fn.Name.Name] = kind
			}
		}
	}

	want := map[string]string{
		// The chokepoint: qualifies the id before handing it to a spawner.
		"registry.go:SpawnConsumer": "qualified",
		// A pure delegate — ConsumerSpawner.Spawn forwards what SpawnConsumer
		// already qualified, so it must NOT re-derive anything of its own.
		"spawner.go:Spawn": "unqualified",
	}

	for site, kind := range want {
		got, ok := sites[site]
		if !ok {
			t.Errorf("%s no longer hands a group id to a spawner — this guard has stopped guarding it", site)
			continue
		}
		if got != kind {
			t.Errorf("%s is %s, want %s", site, got, kind)
		}
	}
	for site := range sites {
		if _, ok := want[site]; !ok {
			t.Errorf("%s hands a group id to a spawner and is not covered here; "+
				"route it through consumerGroupID and add it to this list", site)
		}
	}
}

// packageSources returns the non-test .go files of this package, keyed by base
// name. It fails rather than returning nothing, so a broken scan cannot make the
// guards above pass vacuously.
func packageSources(t *testing.T) map[string]string {
	t.Helper()

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	out := make(map[string]string, len(files))
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		out[f] = string(data)
	}
	if len(out) == 0 {
		t.Fatal("no package sources found; the structural guards would pass vacuously")
	}
	return out
}
