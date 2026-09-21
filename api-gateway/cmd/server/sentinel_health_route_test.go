package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
)

// GET /monitoring/sentinel/health reads sentinel_component_health, a table with no
// workspace column whose component ids name Kafka topics and containers of every
// workspace. It was registered on the plain authenticated group, so any signed-in user
// of any workspace could list them. The route now carries AdminRoleMiddleware, and the
// only UI that reads it is admin/health.
const sentinelHealthPath = "/monitoring/sentinel/health"

const sentinelHealthHandler = "GetSentinelHealth"

func TestSentinelHealthRouteIsAdminOnly(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if problem := sentinelHealthRouteWiring(string(src)); problem != "" {
		t.Fatal(problem)
	}
}

// The reader has to reject each wrong wiring, or the check above proves nothing.
func TestSentinelHealthWiringReaderRejectsWrongWiring(t *testing.T) {
	const ok = `package main
func main() {
	r := gin.Default()
	api := r.Group("/api/v1")
	api.Use(handlers.AuthRequiredMiddleware())
	{
		api.GET("/monitoring/sentinel/health", handlers.AdminRoleMiddleware(), handlers.GetSentinelHealth)
		api.GET("/monitoring/sentinel/issues", handlers.GetSentinelIssues)
	}
	r.Run()
}`
	const onGroup = `package main
func main() {
	r := gin.Default()
	mon := r.Group("/api/v1/monitoring")
	mon.Use(handlers.AdminRoleMiddleware())
	mon.GET("/sentinel/health", handlers.GetSentinelHealth)
}`
	for name, src := range map[string]string{"on the route": ok, "on the route's group": onGroup} {
		if problem := sentinelHealthRouteWiring(src); problem != "" {
			t.Errorf("%s: correct wiring rejected: %s", name, problem)
		}
	}

	const route = `api.GET("/monitoring/sentinel/health", handlers.AdminRoleMiddleware(), handlers.GetSentinelHealth)`
	wrong := map[string]string{
		"no middleware":         strings.Replace(ok, "handlers.AdminRoleMiddleware(), ", "", 1),
		"a weaker middleware":   strings.Replace(ok, "handlers.AdminRoleMiddleware()", "handlers.PowerUserOrAdminMiddleware()", 1),
		"middleware after":      strings.Replace(ok, "handlers.AdminRoleMiddleware(), handlers.GetSentinelHealth", "handlers.GetSentinelHealth, handlers.AdminRoleMiddleware()", 1),
		"not registered":        strings.Replace(ok, route, "", 1),
		"registered twice":      strings.Replace(ok, route, route+"\n\t\tapi.GET(\"/monitoring/sentinel/health\", handlers.GetSentinelHealth)", 1),
		"only inside an if":     strings.Replace(ok, route, "if adminOnly {\n\t\t\t"+route+"\n\t\t}", 1),
		"group Use after route": strings.Replace(onGroup, "mon.Use(handlers.AdminRoleMiddleware())\n\tmon.GET(\"/sentinel/health\", handlers.GetSentinelHealth)", "mon.GET(\"/sentinel/health\", handlers.GetSentinelHealth)\n\tmon.Use(handlers.AdminRoleMiddleware())", 1),
		"another path":          strings.Replace(ok, "/monitoring/sentinel/health", "/monitoring/sentinel/healthz", 1),
	}
	for name, src := range wrong {
		if src == ok || src == onGroup {
			t.Fatalf("%s: the replacement did not apply", name)
		}
		if problem := sentinelHealthRouteWiring(src); problem == "" {
			t.Errorf("%s: wrong wiring accepted", name)
		}
	}
}

// sentinelHealthRouteWiring says why func main in src does not serve GetSentinelHealth
// at /api/v1/monitoring/sentinel/health behind AdminRoleMiddleware, or "" when it does.
// The middleware counts when it is an argument before the handler on the route itself,
// or when the route's group installed it with Use before the route was registered (gin
// copies the group's chain at registration, so a later Use never reaches it).
//
// Only statements that always run are read: main's own statements and plain { } blocks.
func sentinelHealthRouteWiring(src string) string {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", src, 0)
	if err != nil {
		return "parse main.go: " + err.Error()
	}
	var body *ast.BlockStmt
	for _, decl := range file.Decls {
		if fn, isFunc := decl.(*ast.FuncDecl); isFunc && fn.Recv == nil && fn.Name.Name == "main" {
			body = fn.Body
		}
	}
	if body == nil {
		return "main.go has no func main"
	}

	groups := map[string]string{} // variable -> prefix it was created with
	adminGroup := map[string]bool{}
	registrations := 0
	problem := ""
	var walk func([]ast.Stmt)
	walk = func(stmts []ast.Stmt) {
		for _, stmt := range stmts {
			switch s := stmt.(type) {
			case *ast.BlockStmt:
				walk(s.List)
			case *ast.AssignStmt:
				if len(s.Lhs) != 1 || len(s.Rhs) != 1 {
					continue
				}
				id, isIdent := s.Lhs[0].(*ast.Ident)
				call, isCall := s.Rhs[0].(*ast.CallExpr)
				if !isIdent || !isCall || len(call.Args) != 1 {
					continue
				}
				sel, isSel := call.Fun.(*ast.SelectorExpr)
				if !isSel || sel.Sel.Name != "Group" {
					continue
				}
				if lit, ok := stringLit(call.Args[0]); ok {
					parent := ""
					if recv, isRecv := sel.X.(*ast.Ident); isRecv {
						parent = groups[recv.Name]
					}
					groups[id.Name] = parent + lit
					adminGroup[id.Name] = false
				}
			case *ast.ExprStmt:
				call, isCall := s.X.(*ast.CallExpr)
				if !isCall {
					continue
				}
				sel, isSel := call.Fun.(*ast.SelectorExpr)
				if !isSel {
					continue
				}
				recv, isIdent := sel.X.(*ast.Ident)
				if !isIdent {
					continue
				}
				if sel.Sel.Name == "Use" {
					for _, arg := range call.Args {
						if isHandlersCall(arg, "AdminRoleMiddleware") {
							adminGroup[recv.Name] = true
						}
					}
					continue
				}
				last := len(call.Args) - 1
				if last < 1 || !isHandlersRef(call.Args[last], sentinelHealthHandler) {
					continue
				}
				registrations++
				if sel.Sel.Name != "GET" {
					problem = "main.go registers " + sentinelHealthHandler + " with " + sel.Sel.Name + ", want GET"
					return
				}
				path, ok := stringLit(call.Args[0])
				if !ok {
					problem = "main.go registers " + sentinelHealthHandler + " on a path that is not a string literal"
					return
				}
				prefix, known := groups[recv.Name]
				if !known {
					problem = "main.go registers " + sentinelHealthHandler + " on " + recv.Name + ", which is not a route group created in main"
					return
				}
				if prefix+path != "/api/v1"+sentinelHealthPath {
					problem = "main.go serves " + sentinelHealthHandler + " at " + prefix + path + ", want /api/v1" + sentinelHealthPath
					return
				}
				gated := adminGroup[recv.Name]
				for _, arg := range call.Args[1:last] {
					if isHandlersCall(arg, "AdminRoleMiddleware") {
						gated = true
					}
				}
				if !gated {
					problem = "main.go registers " + sentinelHealthHandler + " without handlers.AdminRoleMiddleware() before it, on the route or on group " + recv.Name
					return
				}
			}
		}
	}
	walk(body.List)
	switch {
	case problem != "":
		return problem
	case registrations == 0:
		return "main.go never registers " + sentinelHealthHandler + " unconditionally in func main"
	case registrations > 1:
		return "main.go registers " + sentinelHealthHandler + " " + strconv.Itoa(registrations) + " times"
	}
	return ""
}
