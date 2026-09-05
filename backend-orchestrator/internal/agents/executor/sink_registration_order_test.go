package executor

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// TestSinkManifestRowIsWrittenOnlyAfterTheWorkerStarts pins the ordering inside
// startKafkaMCPSink: the kafka_sink_worker manifest row must be written on the
// start_sink SUCCESS path, never before the attempt.
//
// Why this is an AST test and not a behavioural one: driving startKafkaMCPSink to
// its failure path needs a stubbed MCP transport, and Agent.mcpClient is a concrete
// *mcp.Client. Introducing an interface there to make this one assertion testable
// would put a refactor in the path of every pipeline start — more risk than the
// defect. The invariant is structural, so assert it structurally.
//
// Why not a string match on the source: the negative assertion in the sentinel's
// sink_presence_test.go keys on exact whitespace ("LEFT JOIN LATERAL (\n\t    SELECT"),
// which means re-indenting the query would make that guard silently vacuous AND
// still green. Positions from the parser cannot drift that way.
//
// The defect being pinned: the row is a claim that a worker exists under this
// consumer group, and the batch sentinel reads it as one — it probes the container
// for that group and raises a CRITICAL "nothing is writing to the destination" when
// it is absent. Written before the worker starts, the claim is false for the whole
// span of topic creation, the bootstrap produce, and five retries with backoff; and
// on total failure the row was simply left behind, since nothing anywhere DELETEs
// from pipeline_dependencies.
func TestSinkManifestRowIsWrittenOnlyAfterTheWorkerStarts(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "executor.go", nil, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}

	var fn *ast.FuncDecl
	for _, d := range file.Decls {
		if f, ok := d.(*ast.FuncDecl); ok && f.Name.Name == "startKafkaMCPSink" {
			fn = f
			break
		}
	}
	if fn == nil {
		t.Fatal("startKafkaMCPSink not found in executor.go — if it moved or was renamed, " +
			"move this guard with it rather than deleting it")
	}

	// The success branch: `if err == nil && resp != nil && resp.Success { ... }`
	// inside the retry loop. Located by its condition rather than by line number.
	var success *ast.IfStmt
	ast.Inspect(fn, func(n ast.Node) bool {
		ifs, ok := n.(*ast.IfStmt)
		if !ok || ifs.Cond == nil {
			return true
		}
		var b strings.Builder
		ast.Inspect(ifs.Cond, func(c ast.Node) bool {
			if sel, ok := c.(*ast.SelectorExpr); ok {
				b.WriteString(sel.Sel.Name)
			}
			return true
		})
		if strings.Contains(b.String(), "Success") {
			success = ifs
		}
		return true
	})
	if success == nil {
		t.Fatal("no `resp.Success` branch found inside startKafkaMCPSink — the retry loop's " +
			"shape changed; re-establish where the sink is known to be running before " +
			"deciding where the manifest row belongs")
	}

	// Closure bodies do not execute where they are written, so a call inside one is
	// gated by wherever the closure is INVOKED. Collect their ranges and exclude them;
	// the invocation is then what gets checked. Getting this wrong is how a guard ends
	// up asserting the opposite of its own name.
	var lits []*ast.FuncLit
	ast.Inspect(fn, func(n ast.Node) bool {
		if fl, ok := n.(*ast.FuncLit); ok {
			lits = append(lits, fl)
		}
		return true
	})
	inClosure := func(p token.Pos) bool {
		for _, fl := range lits {
			if p > fl.Body.Lbrace && p < fl.Body.Rbrace {
				return true
			}
		}
		return false
	}

	// Every call that registers the sink, wherever it sits.
	type site struct {
		name string
		pos  token.Pos
	}
	var sites []site
	ast.Inspect(fn, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch f := call.Fun.(type) {
		case *ast.Ident:
			if f.Name == "registerSinkWorker" {
				sites = append(sites, site{"registerSinkWorker()", call.Pos()})
			}
			// A direct upsertDependency(..., "kafka_sink_worker", ...) — the pre-fix
			// shape. Skipped when it sits inside a closure, since then it is the
			// closure's call sites above that carry the gating.
			if f.Name == "upsertDependency" && !inClosure(call.Pos()) {
				for _, a := range call.Args {
					if lit, ok := a.(*ast.BasicLit); ok && lit.Value == `"kafka_sink_worker"` {
						sites = append(sites, site{`upsertDependency("kafka_sink_worker")`, call.Pos()})
					}
				}
			}
		}
		return true
	})
	if len(sites) == 0 {
		t.Fatal("startKafkaMCPSink no longer registers the sink as a runtime dependency at " +
			"all — the health panel and the batch sink probe both read that row; removing it " +
			"disarms the probe rather than fixing anything")
	}

	for _, s := range sites {
		if s.pos < success.Body.Lbrace || s.pos > success.Body.Rbrace {
			t.Errorf("%s runs at %s, outside the resp.Success branch (%s).\n"+
				"The manifest row asserts that a worker EXISTS under this consumer group. "+
				"Written before start_sink succeeds it is false for the whole span of topic "+
				"creation, the bootstrap produce and five retries with backoff — and a 60s "+
				"batch-sentinel tick landing in that span raises a CRITICAL "+
				"\"nothing is writing to the destination\" against a healthy run. On five "+
				"failures the row is also never cleaned up: nothing in the repo DELETEs from "+
				"pipeline_dependencies.",
				s.name, fset.Position(s.pos), fset.Position(success.Body.Lbrace))
		}
	}
}
