package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// The Schema Registry routes proxy ONE shared Confluent registry. A subject name
// carries no workspace, the handler takes no workspace argument, and the registry
// has no tenant concept — so there is no per-workspace answer these routes could
// give, and the reads used to be open to any authenticated user. A member of one
// workspace could list every other tenant's subject names (their topics, hence
// their tables) and read the schemas behind them.
//
// This guard is positional, like the CORS one beside it: the middleware works
// perfectly, the defect is WHERE it is attached. A behavioural test on a
// hand-wired engine cannot see a route someone registers on `api` instead of on
// the gated group — which is exactly how the read routes came to be ungated while
// a green role-gate suite sat next to them asserting the writes.
func TestEverySchemaRouteIsRegisteredOnTheAdminGatedGroup(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parsing main.go: %v", err)
	}

	httpMethods := map[string]bool{
		"GET": true, "POST": true, "PUT": true, "DELETE": true, "PATCH": true, "HEAD": true,
	}

	var groupVar string    // the identifier the /schemas group is assigned to
	var groupGate string   // the middleware it was created with
	var groupPos string    //
	registrations := 0     // every /schemas route registration seen
	offGroup := []string{} // ...that are NOT on the group

	// Pass 1: find `<var> := <router>.Group("/schemas", <middleware>)`.
	ast.Inspect(file, func(n ast.Node) bool {
		assign, ok := n.(*ast.AssignStmt)
		if !ok || len(assign.Lhs) != 1 || len(assign.Rhs) != 1 {
			return true
		}
		call, ok := assign.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Group" || len(call.Args) == 0 {
			return true
		}
		if lit := schemaStringLit(call.Args[0]); lit != "/schemas" {
			return true
		}
		if id, ok := assign.Lhs[0].(*ast.Ident); ok {
			groupVar = id.Name
			groupPos = fset.Position(assign.Pos()).String()
		}
		for _, arg := range call.Args[1:] {
			if c, ok := arg.(*ast.CallExpr); ok {
				if s, ok := c.Fun.(*ast.SelectorExpr); ok {
					groupGate = s.Sel.Name
				}
			}
		}
		return true
	})

	if groupVar == "" {
		t.Fatal(`main.go has no api.Group("/schemas", …): the whole surface is ` +
			`ungated, or it was renamed and this guard can no longer see it`)
	}
	if groupGate != "AdminRoleMiddleware" {
		t.Errorf("the /schemas group at %s is created with %q, want AdminRoleMiddleware: "+
			"the registry is a global resource with no workspace axis to filter on",
			groupPos, groupGate)
	}

	// Pass 2: every /schemas route registration must be on that group.
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !httpMethods[sel.Sel.Name] {
			return true
		}
		path := schemaStringLit(call.Args[0])
		recv, _ := sel.X.(*ast.Ident)
		onGroup := recv != nil && recv.Name == groupVar
		// A route is "a schemas route" either because it is registered on the
		// group (relative path) or because its absolute path says so.
		if !onGroup && !strings.HasPrefix(path, "/schemas") {
			return true
		}
		registrations++
		if !onGroup {
			where := "?"
			if recv != nil {
				where = recv.Name
			}
			offGroup = append(offGroup, where+"."+sel.Sel.Name+"("+strconv.Quote(path)+") at "+
				fset.Position(call.Pos()).String())
		}
		return true
	})

	// Vacuity guard. A walk that matched nothing would report a perfectly gated
	// surface, which is the failure mode this whole file exists to prevent.
	if registrations < 10 {
		t.Fatalf("found only %d /schemas route registrations; main.go has 11, so the "+
			"walk is broken rather than the wiring", registrations)
	}
	for _, r := range offGroup {
		t.Errorf("schema-registry route registered outside the admin-gated group: %s", r)
	}
}

// schemaStringLit is this file's own copy rather than a shared helper: the
// neighbouring cors guard defines one with the same shape, and a test guard that
// depends on another guard's internals breaks in two places at once.
func schemaStringLit(e ast.Expr) string {
	lit, ok := e.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return ""
	}
	s, err := strconv.Unquote(lit.Value)
	if err != nil {
		return ""
	}
	return s
}
