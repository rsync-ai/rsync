package executor

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/shared/kafkaclient"
)

// The hybrid MySQL handoff seeds a Kafka Connect source offset so the Debezium
// connector resumes at the captured binlog position. Two names have to be exactly
// what Connect and Debezium use, or the seed is silently ignored:
//
//   - the topic is Connect's OFFSET_STORAGE_TOPIC, verbatim. A qualified produce put
//     the record on rsync._rsync-connect-offsets, which nothing reads;
//   - the key's "server" is the connector's topic.prefix, which the Debezium MCP
//     qualifies, so it is rsync.cdc-<id8>, not the bare connector name.

type recordedSeed struct{ topic, key, value string }

type exactTopicRecorder struct{ sent []recordedSeed }

func (r *exactTopicRecorder) ProduceToExactTopic(_ context.Context, topic string, key, value []byte) error {
	r.sent = append(r.sent, recordedSeed{topic, string(key), string(value)})
	return nil
}

// unsetEnv removes a variable for the rest of the test and restores it afterwards.
func unsetEnv(t *testing.T, name string) {
	t.Helper()
	t.Setenv(name, "")
	os.Unsetenv(name)
}

func TestHybridOffsetTopicIsConnectsOwnName(t *testing.T) {
	unsetEnv(t, "KAFKA_CONNECT_OFFSET_TOPIC")
	unsetEnv(t, kafkaclient.EnvTopicPrefix)
	if got := hybridOffsetTopic(); got != "_rsync-connect-offsets" {
		t.Errorf("hybridOffsetTopic() = %q, want _rsync-connect-offsets (OFFSET_STORAGE_TOPIC "+
			"on the kafka-connect container)", got)
	}
	t.Setenv("KAFKA_CONNECT_OFFSET_TOPIC", "connect-offsets")
	if got := hybridOffsetTopic(); got != "connect-offsets" {
		t.Errorf("hybridOffsetTopic() = %q with KAFKA_CONNECT_OFFSET_TOPIC set, want connect-offsets", got)
	}
}

func TestSeedDebeziumMySQLOffsetWritesConnectsExactTopic(t *testing.T) {
	pos := cdc.BinlogPosition{File: "mysql-bin.000003", Pos: 7759}
	cases := []struct {
		name, prefixEnv string
		unsetPrefix     bool
		wantKey         string
	}{
		{"default namespace", "", true, `["cdc-abc12345",{"server":"rsync.cdc-abc12345"}]`},
		{"KAFKA_TOPIC_PREFIX=acme", "acme", false, `["cdc-abc12345",{"server":"acme.cdc-abc12345"}]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			unsetEnv(t, "KAFKA_CONNECT_OFFSET_TOPIC")
			if tc.unsetPrefix {
				unsetEnv(t, kafkaclient.EnvTopicPrefix)
			} else {
				t.Setenv(kafkaclient.EnvTopicPrefix, tc.prefixEnv)
			}
			if kafkaclient.Topic("_rsync-connect-offsets") == "_rsync-connect-offsets" {
				t.Fatal("test is vacuous: the namespace does not qualify the offset topic")
			}

			// The prefix the hybrid path passes: what the Debezium MCP will set for
			// start_sync params carrying connector_name and no topic_prefix.
			prefix := debeziumTopicPrefixFor(map[string]interface{}{"connector_name": "cdc-abc12345"})
			for _, given := range []string{prefix, ""} {
				r := &exactTopicRecorder{}
				if err := seedDebeziumMySQLOffsetTo(context.Background(), r, "cdc-abc12345", given, pos); err != nil {
					t.Fatalf("seed (topicPrefix %q): %v", given, err)
				}
				if len(r.sent) != 1 {
					t.Fatalf("seed (topicPrefix %q) sent %d records, want 1", given, len(r.sent))
				}
				got := r.sent[0]
				if got.topic != "_rsync-connect-offsets" {
					t.Errorf("seed wrote to %q, want _rsync-connect-offsets exactly: Connect reads "+
						"OFFSET_STORAGE_TOPIC verbatim and never sees a qualified copy", got.topic)
				}
				if got.key != tc.wantKey {
					t.Errorf("seed (topicPrefix %q) key = %s, want %s: \"server\" must be the "+
						"connector's qualified topic.prefix", given, got.key, tc.wantKey)
				}
				if got.value != `{"file":"mysql-bin.000003","pos":7759}` {
					t.Errorf("seed value = %s", got.value)
				}
			}
		})
	}
}

// The hybrid path must hand the seed the QUALIFIED topic.prefix. Before, it passed
// connectorName for both arguments, so the key read {"server":"cdc-<id8>"} while the
// connector ran with topic.prefix rsync.cdc-<id8>, and Connect filed the seeded
// offset under a source partition the connector never asks for.
func TestHybridPathSeedsWithTheConnectorsTopicPrefix(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "hybrid_cdc.go", nil, 0)
	if err != nil {
		t.Fatalf("parse hybrid_cdc.go: %v", err)
	}
	var fn *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "executeHybridCDCDataTransfer" {
			fn = fd
		}
	}
	if fn == nil {
		t.Fatal("executeHybridCDCDataTransfer not found in hybrid_cdc.go")
	}

	// Identifiers assigned from debeziumTopicPrefixFor(...) inside the function.
	fromPrefixFor := map[string]bool{}
	isPrefixForCall := func(e ast.Expr) bool {
		c, ok := e.(*ast.CallExpr)
		if !ok {
			return false
		}
		id, ok := c.Fun.(*ast.Ident)
		return ok && id.Name == "debeziumTopicPrefixFor"
	}
	ast.Inspect(fn, func(n ast.Node) bool {
		if as, ok := n.(*ast.AssignStmt); ok && len(as.Lhs) == len(as.Rhs) {
			for i, rhs := range as.Rhs {
				if id, ok := as.Lhs[i].(*ast.Ident); ok && isPrefixForCall(rhs) {
					fromPrefixFor[id.Name] = true
				}
			}
		}
		return true
	})

	seeds := 0
	ast.Inspect(fn, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := c.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "seedDebeziumMySQLOffset" {
			return true
		}
		seeds++
		if len(c.Args) != 4 {
			t.Fatalf("seedDebeziumMySQLOffset called with %d args", len(c.Args))
		}
		prefixArg := c.Args[2]
		id, isIdent := prefixArg.(*ast.Ident)
		if !isPrefixForCall(prefixArg) && !(isIdent && fromPrefixFor[id.Name]) {
			t.Errorf("%s: the topicPrefix passed to seedDebeziumMySQLOffset is not "+
				"debeziumTopicPrefixFor(...); the seed's \"server\" would not match the "+
				"connector's qualified topic.prefix", fset.Position(prefixArg.Pos()))
		}
		return true
	})
	if seeds != 1 {
		t.Fatalf("found %d seedDebeziumMySQLOffset calls in executeHybridCDCDataTransfer, want 1", seeds)
	}
}
