package executor

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"testing"
)

// TestMongoDBTopologyCheckIsWiredBeforeStartSync pins where the standalone-
// MongoDB check runs, in both functions that start a CDC connector.
//
// The check is only worth anything in one position. Debezium accepts a connector for a
// standalone mongod and retries 40573 forever, so once start_sync has returned the run
// is reported "running" and nothing downstream ever fails it. A check that runs after
// start_sync — or after the Postgres/SQL Server/Oracle provisioning and the topic
// pre-creates, which leave resources behind for a run that is about to fail — still
// passes its unit tests and still lets the pipeline show Running while writing nothing.
//
// So, per start site, this asserts that the check:
//   - is a statement directly in the function body, not inside a branch that some
//     paths skip;
//   - is handed the live tester (a.TestConnectionResult) and the SOURCE type and config
//     — every argument is a string or map, so a destination value compiles just as
//     well and silently never matches "mongodb";
//   - fails the run: its if-body returns a response with Status "failed" carrying the
//     check's message;
//   - comes before start_sync, before every other anchor that commits the run, and
//     before any Status: "running".
//
// Positions come from the parser, so re-indentation or a moved comment cannot break
// this; renaming a variable the check is handed can, and the failure says so.
func TestMongoDBTopologyCheckIsWiredBeforeStartSync(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "executor.go", nil, 0)
	if err != nil {
		t.Fatalf("parse executor.go: %v", err)
	}

	sites := []struct {
		fn         string
		sourceType string // expected source-type argument, as written
		config     string // expected config argument — the config start_sync is built from
		// Calls that commit the run or leave resources behind. Each must exist (so the
		// guard is not vacuous) and must come after the check.
		mustFollow []string
	}{
		{
			fn:         "executeStreamingDataTransfer",
			sourceType: "normalizedSource",
			config:     "task.Source.Config",
			mustFollow: []string{"ProvisionResources", "EnsureTopicExistsWithConfig", "executeWithRetry"},
		},
		{
			fn:         "executeStartStreaming",
			sourceType: "dbType",
			config:     "dbConfig",
			mustFollow: []string{"executeWithRetry", "AddStreamingPipeline"},
		},
	}

	for _, site := range sites {
		t.Run(site.fn, func(t *testing.T) {
			var fn *ast.FuncDecl
			for _, d := range file.Decls {
				if f, ok := d.(*ast.FuncDecl); ok && f.Name.Name == site.fn {
					fn = f
					break
				}
			}
			if fn == nil || fn.Body == nil {
				t.Fatalf("%s not found in executor.go — if it moved or was renamed, move this "+
					"guard with it rather than deleting it", site.fn)
			}

			// A call inside a closure runs wherever the closure is invoked, so its written
			// position proves nothing about ordering.
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

			// The check, as a top-level `if msg := checkMongoDBCDCSource(...); ... { return }`.
			var checkIf *ast.IfStmt
			var checkCall *ast.CallExpr
			var checkVar string
			for _, stmt := range fn.Body.List {
				ifs, ok := stmt.(*ast.IfStmt)
				if !ok {
					continue
				}
				assign, ok := ifs.Init.(*ast.AssignStmt)
				if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
					continue
				}
				call, ok := assign.Rhs[0].(*ast.CallExpr)
				if !ok {
					continue
				}
				if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "checkMongoDBCDCSource" {
					checkIf, checkCall = ifs, call
					if lhs, ok := assign.Lhs[0].(*ast.Ident); ok {
						checkVar = lhs.Name
					}
					break
				}
			}
			if checkIf == nil {
				nested := false
				ast.Inspect(fn, func(n ast.Node) bool {
					if call, ok := n.(*ast.CallExpr); ok {
						if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "checkMongoDBCDCSource" {
							nested = true
						}
					}
					return true
				})
				if nested {
					t.Fatalf("%s calls checkMongoDBCDCSource, but not as a top-level "+
						"`if msg := checkMongoDBCDCSource(...); msg != \"\" { return … }` in the "+
						"function body — inside a branch or closure, some paths start CDC without it",
						site.fn)
				}
				t.Fatalf("%s no longer runs checkMongoDBCDCSource before starting CDC. A "+
					"standalone mongod then gets a Debezium connector that retries "+
					"\"$changeStream stage is only supported on replica sets\" forever while "+
					"the run shows Running and writes nothing", site.fn)
			}

			// Arguments.
			if len(checkCall.Args) != 5 {
				t.Fatalf("checkMongoDBCDCSource called with %d args, want 5", len(checkCall.Args))
			}
			if got := exprText(checkCall.Args[1]); got != "a.TestConnectionResult" {
				t.Errorf("%s hands the check %s as its tester, want a.TestConnectionResult — "+
					"anything else never asks the real connector", site.fn, got)
			}
			if got := exprText(checkCall.Args[2]); got != site.sourceType {
				t.Errorf("%s hands the check source type %s, want %s. A destination type "+
					"compiles just as well and never matches mongodb. If the variable was "+
					"renamed, update this guard.", site.fn, got, site.sourceType)
			}
			if got := exprText(checkCall.Args[4]); got != site.config {
				t.Errorf("%s hands the check config %s, want %s — the config start_sync is "+
					"built from. Probing any other config tests a different server than the "+
					"one Debezium connects to. If the variable was renamed, update this guard.",
					site.fn, got, site.config)
			}

			// The if-body must fail the run with the check's message.
			failsRun := false
			if checkVar != "" && len(checkIf.Body.List) > 0 {
				if ret, ok := checkIf.Body.List[len(checkIf.Body.List)-1].(*ast.ReturnStmt); ok && len(ret.Results) == 1 {
					if cl, ok := ret.Results[0].(*ast.CompositeLit); ok {
						var statusFailed, carriesMsg bool
						for _, el := range cl.Elts {
							kv, ok := el.(*ast.KeyValueExpr)
							if !ok {
								continue
							}
							key, _ := kv.Key.(*ast.Ident)
							if key == nil {
								continue
							}
							switch key.Name {
							case "Status":
								if v, err := basicString(kv.Value); err == nil && v == "failed" {
									statusFailed = true
								}
							case "Error":
								if id, ok := kv.Value.(*ast.Ident); ok && id.Name == checkVar {
									carriesMsg = true
								}
							}
						}
						failsRun = statusFailed && carriesMsg
					}
				}
			}
			if !failsRun {
				t.Errorf("in %s the checkMongoDBCDCSource branch does not end in "+
					"`return ExecutorResponse{…, Status: \"failed\", Error: %s}` — a check "+
					"that logs and carries on starts the same connector that never streams, "+
					"and a failure without the message loses the rs.initiate() remediation",
					site.fn, checkVar)
			}

			checkPos := checkCall.Pos()

			// start_sync, located by the operation string.
			var startSyncPos token.Pos
			ast.Inspect(fn, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING || inClosure(lit.Pos()) {
					return true
				}
				if v, err := strconv.Unquote(lit.Value); err == nil && v == "start_sync" && !startSyncPos.IsValid() {
					startSyncPos = lit.Pos()
				}
				return true
			})
			if !startSyncPos.IsValid() {
				t.Fatalf(`no "start_sync" operation found inside %s — this guard can no `+
					"longer see what it orders against; re-point it rather than deleting it", site.fn)
			}
			if checkPos > startSyncPos {
				t.Errorf("checkMongoDBCDCSource runs at %s, AFTER start_sync at %s. By then "+
					"Kafka Connect has accepted the connector and the run is reported running.",
					fset.Position(checkPos), fset.Position(startSyncPos))
			}

			// Every other commit point, first occurrence outside closures.
			for _, name := range site.mustFollow {
				var pos token.Pos
				ast.Inspect(fn, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok || inClosure(call.Pos()) || pos.IsValid() {
						return true
					}
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == name {
						pos = call.Pos()
					}
					return true
				})
				if !pos.IsValid() {
					t.Fatalf("no %s call found in %s — this guard can no longer see that "+
						"commit point; re-point it rather than deleting it", name, site.fn)
				}
				if checkPos > pos {
					t.Errorf("checkMongoDBCDCSource runs at %s, AFTER %s at %s — a run about "+
						"to fail on a standalone source must not have started anything yet",
						fset.Position(checkPos), name, fset.Position(pos))
				}
			}

			// Every Status: "running" in the function.
			running := 0
			ast.Inspect(fn, func(n ast.Node) bool {
				kv, ok := n.(*ast.KeyValueExpr)
				if !ok || inClosure(kv.Pos()) {
					return true
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok || key.Name != "Status" {
					return true
				}
				if v, err := basicString(kv.Value); err == nil && v == "running" {
					running++
					if checkPos > kv.Pos() {
						t.Errorf("%s reports Status \"running\" at %s, before the standalone "+
							"check at %s", site.fn, fset.Position(kv.Pos()), fset.Position(checkPos))
					}
				}
				return true
			})
			if running == 0 {
				t.Fatalf(`no Status: "running" found in %s — this guard can no longer see `+
					"where the run is reported running; re-point it rather than deleting it", site.fn)
			}
		})
	}
}
