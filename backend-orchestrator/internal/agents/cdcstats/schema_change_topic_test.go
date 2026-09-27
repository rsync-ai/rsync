package cdcstats

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// The schema-change consumer reads a connector's bare topic.prefix topic
// (rsync.cdc-<id8>), which only the historized Debezium connectors write. It used to
// be started for every CDC pipeline, and its subscription auto-created that topic at
// the broker's defaults, so each PostgreSQL and MongoDB pipeline carried an empty
// rsync.cdc-<id8>. These tests pin the three fixes: the consumer is wanted only for
// DDL-emitting connectors, the topic is created through the ensure choke point before
// the subscription, and the consumer itself never auto-creates a topic.

func TestSchemaChangeConsumerWantedOnlyForDDLEmittingConnectors(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]interface{}
		want bool
	}{
		{"mysql", map[string]interface{}{"connector.class": "io.debezium.connector.mysql.MySqlConnector"}, true},
		{"mariadb", map[string]interface{}{"connector.class": "io.debezium.connector.mariadb.MariaDbConnector"}, true},
		{"sqlserver", map[string]interface{}{"connector.class": "io.debezium.connector.sqlserver.SqlServerConnector"}, true},
		{"oracle", map[string]interface{}{"connector.class": "io.debezium.connector.oracle.OracleConnector"}, true},
		{"db2", map[string]interface{}{"connector.class": "io.debezium.connector.db2.Db2Connector"}, true},
		{"mysql, include.schema.changes=true", map[string]interface{}{
			"connector.class": "io.debezium.connector.mysql.MySqlConnector", "include.schema.changes": "true"}, true},
		{"mysql, include.schema.changes=false", map[string]interface{}{
			"connector.class": "io.debezium.connector.mysql.MySqlConnector", "include.schema.changes": " FALSE "}, false},
		{"postgresql", map[string]interface{}{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}, false},
		{"mongodb", map[string]interface{}{"connector.class": "io.debezium.connector.mongodb.MongoDbConnector"}, false},
		{"no class", map[string]interface{}{}, false},
		{"nil config", nil, false},
	}
	for _, c := range cases {
		if got := schemaChangeConsumerWanted(c.cfg); got != c.want {
			t.Errorf("%s: schemaChangeConsumerWanted = %v, want %v", c.name, got, c.want)
		}
	}
}

// recordingGroup is a sarama.ConsumerGroup whose Consume records the topics it was
// asked for into a shared call log, then ends the worker so the loop returns.
type recordingGroup struct {
	calls *[]string
	stop  context.CancelFunc
}

func (g *recordingGroup) Consume(_ context.Context, topics []string, _ sarama.ConsumerGroupHandler) error {
	for _, tp := range topics {
		*g.calls = append(*g.calls, "consume "+tp)
	}
	g.stop()
	return nil
}
func (g *recordingGroup) Errors() <-chan error                 { return nil }
func (g *recordingGroup) Close() error                         { return nil }
func (g *recordingGroup) Pause(map[string][]int32)             {}
func (g *recordingGroup) Resume(map[string][]int32)            {}
func (g *recordingGroup) PauseAll()                            {}
func (g *recordingGroup) ResumeAll()                           {}
func (g *recordingGroup) PauseClaims(map[string][]int32) error { return nil }

func runSchemaChangeWorkerOnce(t *testing.T, ensureErr error) []string {
	t.Helper()
	const topic = "rsync.cdc-600b012e"
	var calls []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := &Agent{ensureDDLTopic: func(tp string) error {
		calls = append(calls, "ensure "+tp)
		return ensureErr
	}}
	w := &pipelineWorker{
		pipelineID:  "p",
		topicPrefix: topic,
		ctx:         ctx,
		ddlConsumer: &recordingGroup{calls: &calls, stop: cancel},
	}
	done := make(chan struct{})
	go func() { a.runSchemaChangeWorker(w); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runSchemaChangeWorker did not return after its context was cancelled")
	}
	return calls
}

func TestRunSchemaChangeWorkerEnsuresTheTopicBeforeSubscribing(t *testing.T) {
	calls := runSchemaChangeWorkerOnce(t, nil)
	want := []string{"ensure rsync.cdc-600b012e", "consume rsync.cdc-600b012e"}
	if len(calls) != len(want) || calls[0] != want[0] || calls[1] != want[1] {
		t.Fatalf("calls = %v, want %v: the bare topic.prefix topic must be created through "+
			"the ensure choke point before the consumer subscribes to it", calls, want)
	}
}

// A failed ensure is logged and the worker still subscribes: DDL reporting is
// best-effort, and the topic may exist already (the executor pre-creates it).
func TestRunSchemaChangeWorkerStillSubscribesWhenTheEnsureFails(t *testing.T) {
	calls := runSchemaChangeWorkerOnce(t, errors.New("CreateTopics denied"))
	if len(calls) != 2 || calls[1] != "consume rsync.cdc-600b012e" {
		t.Fatalf("calls = %v, want an ensure then a consume", calls)
	}
}

func TestSchemaChangeConsumerDoesNotAutoCreateTopics(t *testing.T) {
	if sarama.NewConfig().Metadata.AllowAutoTopicCreation != true {
		t.Fatal("test is vacuous: sarama's default no longer allows auto topic creation")
	}
	if newSchemaChangeConsumerConfig().Metadata.AllowAutoTopicCreation {
		t.Error("the schema-change consumer's metadata requests may auto-create the topic " +
			"it subscribes to, at the broker's defaults, for any connector")
	}
}

// ensureWorker opens the schema-change consumer group only in the branch where
// schemaChangeConsumerWanted(cfg) holds. Opening a real group needs a broker, so this
// reads agent.go: every a.consumerGroup(...) call handed schemaChangeGroupID's result
// must sit inside the else of `if !schemaChangeConsumerWanted(cfg)`.
func TestEnsureWorkerOpensTheSchemaChangeGroupOnlyWhenWanted(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "agent.go", nil, 0)
	if err != nil {
		t.Fatalf("parse agent.go: %v", err)
	}
	var fn *ast.FuncDecl
	for _, d := range file.Decls {
		if f, ok := d.(*ast.FuncDecl); ok && f.Name.Name == "ensureWorker" {
			fn = f
		}
	}
	if fn == nil {
		t.Fatal("ensureWorker not found in agent.go")
	}

	var gate *ast.BlockStmt
	ast.Inspect(fn, func(n ast.Node) bool {
		is, ok := n.(*ast.IfStmt)
		if !ok {
			return true
		}
		un, ok := is.Cond.(*ast.UnaryExpr)
		if !ok || un.Op != token.NOT {
			return true
		}
		call, ok := un.X.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "schemaChangeConsumerWanted" {
			if eb, ok := is.Else.(*ast.BlockStmt); ok {
				gate = eb
			}
		}
		return true
	})
	if gate == nil {
		t.Fatal("ensureWorker has no `if !schemaChangeConsumerWanted(cfg) { ... } else { ... }`")
	}

	// Identifiers assigned from schemaChangeGroupID(...).
	ddlIDs := map[string]bool{}
	ast.Inspect(fn, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != len(as.Rhs) {
			return true
		}
		for i, rhs := range as.Rhs {
			c, ok := rhs.(*ast.CallExpr)
			if !ok {
				continue
			}
			if f, ok := c.Fun.(*ast.Ident); ok && f.Name == "schemaChangeGroupID" {
				if id, ok := as.Lhs[i].(*ast.Ident); ok {
					ddlIDs[id.Name] = true
				}
			}
		}
		return true
	})

	opens := 0
	ast.Inspect(fn, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok || len(c.Args) == 0 {
			return true
		}
		sel, ok := c.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "consumerGroup" {
			return true
		}
		isDDL := false
		switch arg := c.Args[0].(type) {
		case *ast.Ident:
			isDDL = ddlIDs[arg.Name]
		case *ast.CallExpr:
			f, ok := arg.Fun.(*ast.Ident)
			isDDL = ok && f.Name == "schemaChangeGroupID"
		}
		if !isDDL {
			return true
		}
		opens++
		if c.Pos() < gate.Lbrace || c.End() > gate.Rbrace {
			t.Errorf("%s: the schema-change consumer group is opened outside the "+
				"schemaChangeConsumerWanted gate; a PostgreSQL or MongoDB pipeline would "+
				"subscribe to an rsync.cdc-<id8> topic nothing writes", fset.Position(c.Pos()))
		}
		return true
	})
	if opens != 1 {
		t.Fatalf("found %d schema-change consumerGroup calls in ensureWorker, want 1; this "+
			"guard no longer sees what it gates", opens)
	}
}
