package config

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// KAFKA_GROUP_ID is the one place an operator names a consumer group directly,
// so it is the one place the namespace decision is visible to them. These tests
// assert the PROPERTY the decision in kafka_identity.go commits to — every group
// id this package produces carries the configured namespace, whatever the
// operator typed — plus the two escape hatches that make that defensible: the
// empty-prefix lever, and idempotence.
//
// The failure this guards against is silent. On a cluster with PREFIXED group
// ACLs, a group id outside the namespace is refused at JoinGroup; the client
// retries forever, the process stays up, and no partition is ever assigned.

// rawGroupIDs are the shapes KAFKA_GROUP_ID actually arrives in: the default, an
// operator's own value, one with stray whitespace from a compose file, and one
// where the operator wrote the namespace in themselves.
var rawGroupIDs = []string{
	"go-orchestrator-group",
	"ops-ingest",
	"  padded-group  ",
	"rsync.ops-ingest",
}

func TestEveryGroupIDThisPackageBuildsIsNamespaced(t *testing.T) {
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

			for _, raw := range rawGroupIDs {
				got := kafkaGroupID(raw)
				if !strings.HasPrefix(got, want) {
					t.Errorf("kafkaGroupID(%q) = %q, which is outside the configured namespace %q",
						raw, got, want)
				}
			}
		})
	}
}

// The end-to-end shape: what the orchestrator actually runs with. kafka/manager.go
// and sentinel's health monitor both read this one field, so if it is bare here
// it is bare in all of them.
func TestLoadConfigNamespacesTheGroupID(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "acme.")

	for _, tc := range []struct {
		name string
		env  string // "" = leave KAFKA_GROUP_ID unset, i.e. take the default
		want string
	}{
		{name: "default", env: "", want: "acme.go-orchestrator-group"},
		{name: "operator supplied", env: "ops-ingest", want: "acme.ops-ingest"},
		{name: "operator wrote the prefix in", env: "acme.ops-ingest", want: "acme.ops-ingest"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.env == "" {
				os.Unsetenv("KAFKA_GROUP_ID")
			} else {
				t.Setenv("KAFKA_GROUP_ID", tc.env)
			}

			cfg, err := LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if cfg.Kafka.GroupID != tc.want {
				t.Errorf("Kafka.GroupID = %q, want %q", cfg.Kafka.GroupID, tc.want)
			}
		})
	}
}

// The escape hatch the DECISION leans on. An operator who needs their literal
// group id gets it — together with their literal topic names, which is the only
// combination that is actually consistent. This is also the migration lever for
// a deployment that already has committed offsets under the bare name.
func TestGroupIDHonorsTheEmptyPrefixMigrationLever(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "")
	t.Setenv("KAFKA_GROUP_ID", "acme-ingest")

	if got, want := kafkaGroupID("acme-ingest"), "acme-ingest"; got != want {
		t.Errorf("kafkaGroupID = %q, want the unqualified %q", got, want)
	}

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got, want := cfg.Kafka.GroupID, "acme-ingest"; got != want {
		t.Errorf("Kafka.GroupID = %q, want the unqualified %q — the lever must fully disable qualification", got, want)
	}
}

// Idempotence is what makes the decision safe rather than merely defensible: an
// operator who spells the prefix into the variable is not double-prefixed, and a
// process restart re-reading its own value rejoins the same group instead of
// minting rsync.rsync.… and abandoning its committed offsets.
func TestGroupIDQualificationIsIdempotent(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "acme.")

	for _, raw := range rawGroupIDs {
		once := kafkaGroupID(raw)
		if twice := kafkaGroupID(once); twice != once {
			t.Errorf("re-qualifying %q gave %q", once, twice)
		}
	}
}

// KAFKA_GROUP_ID is a group id PREFIX, not a literal: kafka/manager.go joins
// "<GroupID>-<topic>" per topic and uses "<GroupID>" bare for the single-group
// consumer. Both derived spellings have to land inside the namespace, which is
// why the qualification happens on the field rather than at either join site.
func TestBothDerivedSpellingsInheritTheNamespace(t *testing.T) {
	t.Setenv(kafkaclient.EnvTopicPrefix, "acme.")

	base := kafkaGroupID("go-orchestrator-group")
	for _, derived := range []string{
		base, // manager.go:1363, the single-group consumer
		base + "-" + kafkaclient.Topic("agent.events"), // manager.go:890, per-topic
	} {
		if !strings.HasPrefix(derived, "acme.") {
			t.Errorf("derived group %q is outside the namespace", derived)
		}
	}
}

// The structural guard: qualification is only reliable if it is unavoidable. If
// a second reader of KAFKA_GROUP_ID appears — another struct field, a helper,
// a new subsystem — and it does not pass through kafkaGroupID, it is a group id
// that silently opts out of the ACL grant. This fails on that, and fails if the
// scan finds nothing at all rather than passing vacuously.
func TestEveryReadOfKafkaGroupIDIsQualified(t *testing.T) {
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}

	reads, wrapped := map[token.Pos]string{}, map[token.Pos]bool{}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}

		isEnvRead := func(n ast.Node) (token.Pos, bool) {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 1 {
				return 0, false
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || !strings.HasPrefix(sel.Sel.Name, "Get") {
				return 0, false
			}
			lit, ok := call.Args[0].(*ast.BasicLit)
			if !ok || lit.Value != `"KAFKA_GROUP_ID"` {
				return 0, false
			}
			return call.Pos(), true
		}

		ast.Inspect(f, func(n ast.Node) bool {
			if pos, ok := isEnvRead(n); ok {
				reads[pos] = fset.Position(pos).String()
			}
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "kafkaGroupID" {
				for _, arg := range call.Args {
					ast.Inspect(arg, func(inner ast.Node) bool {
						if pos, ok := isEnvRead(inner); ok {
							wrapped[pos] = true
						}
						return true
					})
				}
			}
			return true
		})
	}

	if len(reads) == 0 {
		t.Fatal("no read of KAFKA_GROUP_ID found; this guard would pass vacuously")
	}
	for pos, where := range reads {
		if !wrapped[pos] {
			t.Errorf("%s reads KAFKA_GROUP_ID without kafkaGroupID(); "+
				"that group id would be denied by a PREFIXED group ACL, silently", where)
		}
	}
}
