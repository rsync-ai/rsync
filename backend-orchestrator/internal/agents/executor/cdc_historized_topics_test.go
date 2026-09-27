package executor

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// A CDC pipeline's per-pipeline topics depend on whether its Debezium connector is
// historized (cdc.HistorizedEngine). MySQL/MariaDB, SQL Server, Oracle and Db2 keep a
// schema-history topic and publish source DDL to the bare topic.prefix topic
// (rsync.cdc-<id8>); PostgreSQL-family and MongoDB connectors do neither. The
// executor used to pre-create the history topic for EVERY pipeline, so each PG and
// Mongo pipeline carried an empty rsync.schemahistory.cdc-<id8> no connector wrote,
// and nothing pre-created the DDL topic at all.
//
// These read executeStreamingDataTransfer and pin three things: the history-topic
// pre-create and the schema_history_topic hand-over sit inside an
// `if cdc.HistorizedEngine(sourceConnector)` gate and nowhere outside it; the same
// gate pre-creates the DDL topic through EnsureDDLTopic with the connector's
// predicted topic.prefix; and that pre-create runs before start_sync.

type streamingFn struct {
	fset      *token.FileSet
	fn        *ast.FuncDecl
	inClosure func(token.Pos) bool
}

func parseStreamingFn(t *testing.T) streamingFn {
	t.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "executor.go", nil, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}
	for _, d := range file.Decls {
		if f, ok := d.(*ast.FuncDecl); ok && f.Name.Name == "executeStreamingDataTransfer" {
			return streamingFn{fset: fset, fn: f, inClosure: closureFilter(f)}
		}
	}
	t.Fatal("executeStreamingDataTransfer not found in executor.go")
	return streamingFn{}
}

// historizedGate returns the body of `if cdc.HistorizedEngine(sourceConnector) {...}`.
func (s streamingFn) historizedGate(t *testing.T) *ast.BlockStmt {
	t.Helper()
	var gates []*ast.IfStmt
	ast.Inspect(s.fn, func(n ast.Node) bool {
		is, ok := n.(*ast.IfStmt)
		if !ok || s.inClosure(is.Pos()) {
			return true
		}
		call, ok := is.Cond.(*ast.CallExpr)
		if !ok || len(call.Args) != 1 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "HistorizedEngine" {
			return true
		}
		if pkg, ok := sel.X.(*ast.Ident); !ok || pkg.Name != "cdc" {
			return true
		}
		if arg, ok := call.Args[0].(*ast.Ident); !ok || arg.Name != "sourceConnector" {
			return true
		}
		gates = append(gates, is)
		return true
	})
	if len(gates) != 1 {
		t.Fatalf("found %d `if cdc.HistorizedEngine(sourceConnector)` gates in "+
			"executeStreamingDataTransfer, want exactly 1", len(gates))
	}
	return gates[0].Body
}

func within(n ast.Node, b *ast.BlockStmt) bool {
	return n.Pos() > b.Lbrace && n.End() <= b.Rbrace
}

func TestSchemaHistoryTopicIsForHistorizedEnginesOnly(t *testing.T) {
	s := parseStreamingFn(t)
	gate := s.historizedGate(t)

	var ensures, handovers int
	ast.Inspect(s.fn, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			sel, ok := x.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "EnsureTopicExistsWithConfig" || len(x.Args) == 0 {
				return true
			}
			if id, ok := x.Args[0].(*ast.Ident); ok && id.Name == "shTopic" {
				ensures++
				if !within(x, gate) {
					t.Errorf("%s: the schema-history topic is pre-created outside the "+
						"cdc.HistorizedEngine gate; a PostgreSQL or MongoDB pipeline would get "+
						"a history topic its connector never writes", s.fset.Position(x.Pos()))
				}
			}
		case *ast.AssignStmt:
			for _, lhs := range x.Lhs {
				idx, ok := lhs.(*ast.IndexExpr)
				if !ok {
					continue
				}
				key, ok := idx.Index.(*ast.BasicLit)
				if !ok || key.Kind != token.STRING {
					continue
				}
				if v, _ := strconv.Unquote(key.Value); v == "schema_history_topic" {
					handovers++
					if !within(x, gate) {
						t.Errorf("%s: params[\"schema_history_topic\"] is set outside the "+
							"cdc.HistorizedEngine gate", s.fset.Position(x.Pos()))
					}
				}
			}
		}
		return true
	})
	if ensures != 1 || handovers != 1 {
		t.Fatalf("found %d schema-history pre-creates and %d schema_history_topic hand-overs, "+
			"want 1 of each; this guard no longer sees what it gates", ensures, handovers)
	}
}

func TestDDLTopicIsPreCreatedForHistorizedEnginesBeforeStartSync(t *testing.T) {
	s := parseStreamingFn(t)
	gate := s.historizedGate(t)

	var startSync token.Pos
	ast.Inspect(s.fn, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING || s.inClosure(lit.Pos()) {
			return true
		}
		if v, _ := strconv.Unquote(lit.Value); v == "start_sync" && !startSync.IsValid() {
			startSync = lit.Pos()
		}
		return true
	})
	if !startSync.IsValid() {
		t.Fatal(`no "start_sync" operation in executeStreamingDataTransfer`)
	}

	// Identifiers assigned from debeziumTopicPrefixFor(...) (the if-init form included).
	isPrefixFor := func(e ast.Expr) bool {
		c, ok := e.(*ast.CallExpr)
		if !ok {
			return false
		}
		id, ok := c.Fun.(*ast.Ident)
		return ok && id.Name == "debeziumTopicPrefixFor"
	}
	fromPrefixFor := map[string]bool{}
	ast.Inspect(gate, func(n ast.Node) bool {
		if as, ok := n.(*ast.AssignStmt); ok && len(as.Lhs) == len(as.Rhs) {
			for i, rhs := range as.Rhs {
				if id, ok := as.Lhs[i].(*ast.Ident); ok && isPrefixFor(rhs) {
					fromPrefixFor[id.Name] = true
				}
			}
		}
		return true
	})

	var calls []*ast.CallExpr
	ast.Inspect(s.fn, func(n ast.Node) bool {
		c, ok := n.(*ast.CallExpr)
		if !ok || s.inClosure(c.Pos()) {
			return true
		}
		if sel, ok := c.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "EnsureDDLTopic" {
			calls = append(calls, c)
		}
		return true
	})
	if len(calls) != 1 {
		t.Fatalf("found %d EnsureDDLTopic calls in executeStreamingDataTransfer, want 1: the "+
			"bare topic.prefix DDL topic of a historized connector must be pre-created at 1 "+
			"partition, not left to Debezium's first DDL record or the cdcstats subscription", len(calls))
	}
	c := calls[0]
	if !within(c, gate) {
		t.Errorf("%s: EnsureDDLTopic runs outside the cdc.HistorizedEngine gate; a "+
			"PostgreSQL or MongoDB pipeline would get an rsync.cdc-<id8> topic nothing writes",
			s.fset.Position(c.Pos()))
	}
	if c.Pos() > startSync {
		t.Errorf("%s: EnsureDDLTopic runs after start_sync at %s; the connector may have "+
			"created the topic with the broker's defaults by then",
			s.fset.Position(c.Pos()), s.fset.Position(startSync))
	}
	arg := c.Args[0]
	id, isIdent := arg.(*ast.Ident)
	if !isPrefixFor(arg) && !(isIdent && fromPrefixFor[id.Name]) {
		t.Errorf("%s: EnsureDDLTopic is not handed debeziumTopicPrefixFor(params), the "+
			"topic.prefix the connector will publish its DDL under", s.fset.Position(arg.Pos()))
	}
}
