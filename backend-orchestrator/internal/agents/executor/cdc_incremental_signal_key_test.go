package executor

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rsync-ai/shared/kafkaclient"
)

// Debezium's Kafka signal channel accepts an execute-snapshot signal only when its key
// is the connector's topic.prefix, and skips any other key without an error. The
// orchestrator registers the connector as the bare cdc-<id8> while connector.py sets
// topic.prefix to the namespace-qualified rsync.cdc-<id8>, so keying the signal with the
// connector name meant an incremental snapshot's historical backfill never started while
// the pipeline reported running.

func TestIncrementalSnapshotSignalKeyIsTheTopicPrefix(t *testing.T) {
	const conn = "cdc-abc12345"

	t.Run("default namespace", func(t *testing.T) {
		unsetEnv(t, kafkaclient.EnvTopicPrefix)
		if got := incrementalSnapshotSignalKey(nil, conn); got != "rsync.cdc-abc12345" {
			t.Fatalf("key = %q, want rsync.cdc-abc12345 (connector.py's _qualify_topic(connector_name))", got)
		}
	})

	t.Run("KAFKA_TOPIC_PREFIX=acme", func(t *testing.T) {
		t.Setenv(kafkaclient.EnvTopicPrefix, "acme")
		if got := incrementalSnapshotSignalKey(map[string]interface{}{}, conn); got != "acme.cdc-abc12345" {
			t.Fatalf("key = %q, want acme.cdc-abc12345", got)
		}
	})

	t.Run("the live config's topic.prefix wins", func(t *testing.T) {
		unsetEnv(t, kafkaclient.EnvTopicPrefix)
		cfg := map[string]interface{}{"topic.prefix": " tenant7.cdc-abc12345 "}
		if got := incrementalSnapshotSignalKey(cfg, conn); got != "tenant7.cdc-abc12345" {
			t.Fatalf("key = %q, want the connector's own topic.prefix tenant7.cdc-abc12345", got)
		}
	})

	t.Run("a blank topic.prefix falls back to the prediction", func(t *testing.T) {
		unsetEnv(t, kafkaclient.EnvTopicPrefix)
		cfg := map[string]interface{}{"topic.prefix": "  "}
		if got := incrementalSnapshotSignalKey(cfg, conn); got != "rsync.cdc-abc12345" {
			t.Fatalf("key = %q, want rsync.cdc-abc12345", got)
		}
	})
}

func TestResolveSignalKeyReadsTheLiveConnectorConfig(t *testing.T) {
	unsetEnv(t, kafkaclient.EnvTopicPrefix)
	const conn = "cdc-abc12345"

	t.Run("config readable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/connectors/"+conn+"/config" {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write([]byte(`{"name":"cdc-abc12345","topic.prefix":"live.cdc-abc12345"}`))
		}))
		defer srv.Close()
		t.Setenv("KAFKA_CONNECT_URL", srv.URL)
		if got := (&Agent{}).resolveSignalKey(context.Background(), conn); got != "live.cdc-abc12345" {
			t.Fatalf("key = %q, want live.cdc-abc12345 from the connector's config", got)
		}
	})

	t.Run("config unreadable", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		defer srv.Close()
		t.Setenv("KAFKA_CONNECT_URL", srv.URL)
		if got := (&Agent{}).resolveSignalKey(context.Background(), conn); got != "rsync.cdc-abc12345" {
			t.Fatalf("key = %q, want the predicted rsync.cdc-abc12345", got)
		}
	})
}

// triggerIncrementalSnapshot keys the signal with resolveSignalKey, not with the
// connector name it is handed. Running it needs a connected Kafka manager, so this
// reads the source.
func TestTriggerIncrementalSnapshotKeysTheSignalWithTheTopicPrefix(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "cdc_incremental.go", nil, 0)
	if err != nil {
		t.Fatalf("parse cdc_incremental.go: %v", err)
	}
	var fn *ast.FuncDecl
	for _, d := range file.Decls {
		if f, ok := d.(*ast.FuncDecl); ok && f.Name.Name == "triggerIncrementalSnapshot" {
			fn = f
		}
	}
	if fn == nil {
		t.Fatal("triggerIncrementalSnapshot not found in cdc_incremental.go")
	}
	builds := 0
	ast.Inspect(fn, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := c.Fun.(*ast.Ident); !ok || id.Name != "buildIncrementalSnapshotSignal" {
			return true
		}
		builds++
		arg, ok := c.Args[0].(*ast.CallExpr)
		sel, isSel := (*ast.SelectorExpr)(nil), false
		if ok {
			sel, isSel = arg.Fun.(*ast.SelectorExpr)
		}
		if !isSel || sel.Sel.Name != "resolveSignalKey" {
			t.Errorf("%s: the execute-snapshot signal is keyed with something other than "+
				"resolveSignalKey(...); Debezium skips a signal whose key is not its topic.prefix",
				fset.Position(c.Args[0].Pos()))
		}
		return true
	})
	if builds != 1 {
		t.Fatalf("found %d buildIncrementalSnapshotSignal calls, want 1", builds)
	}
}
