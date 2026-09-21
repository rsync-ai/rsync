package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"

	"api-gateway/internal/db"
	"api-gateway/internal/handlers"

	"github.com/gin-gonic/gin"
)

// The temporal adapter records a model run that never reported a result by posting to
// this path (backend-temporal-adapter/internal/workflows/model_run_failure_activity.go,
// RecordModelRunFailureActivity). The two services are separate modules, so the path is
// a contract that only a test on each side can hold. Renaming it on this side makes
// every such record a 404, which the adapter retries and then gives up on with a log
// line: the failed run vanishes from the history panel again, silently.
const adapterRunFailedPath = "/api/v1/internal/explorer/models/:id/run-failed"

const runFailedHandler = "RecordSavedQueryModelRunFailureInternal"

// TestRunFailedRouteIsRegisteredBehindTheInternalSecret reads the route out of main.go,
// where the router is built inline and cannot be called from a test, then serves that
// exact registration with the real middleware and handler. The syntax check is what
// proves the middleware is installed on the route's group BEFORE the route (gin copies
// the group's chain at registration, so a later Use never reaches it); the served
// requests prove what the adapter's URL gets back.
func TestRunFailedRouteIsRegisteredBehindTheInternalSecret(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	prefix, rel, problem := runFailedRouteWiring(string(src))
	if problem != "" {
		t.Fatal(problem)
	}

	gin.SetMode(gin.TestMode)
	r := gin.New()
	group := r.Group(prefix)
	group.Use(handlers.InternalServiceMiddleware())
	group.POST(rel, handlers.RecordSavedQueryModelRunFailureInternal)

	prevDB := db.DB
	db.DB = nil
	t.Cleanup(func() { db.DB = prevDB })

	const modelID = "dddddddd-0000-4000-8000-000000000001"
	adapterURL := strings.Replace(adapterRunFailedPath, ":id", modelID, 1)
	body := `{"schedule_id":"dddddddd-0000-4000-8000-000000000002","error":"x","started_at":"2026-09-16T10:00:00Z"}`
	cases := []struct {
		name, configured, header, path string
		want                           int
		wantError                      string
	}{
		{"no secret configured refuses", "", "run-failed-route-secret", adapterURL, http.StatusServiceUnavailable, "internal_service_not_configured"},
		{"no header refuses", "run-failed-route-secret", "", adapterURL, http.StatusUnauthorized, "invalid_internal_secret"},
		{"wrong header refuses", "run-failed-route-secret", "wrong", adapterURL, http.StatusUnauthorized, "invalid_internal_secret"},
		// The right secret gets past the middleware to the handler itself, which answers
		// that it has no database — a different 503, told apart by its body.
		{"right header reaches the handler", "run-failed-route-secret", "run-failed-route-secret", adapterURL, http.StatusServiceUnavailable, "database not available"},
		// Control: the same engine answers 404 for a path it does not have, so the
		// refusals above come from a route that matched, not from a missing one.
		{"an unregistered path is 404", "run-failed-route-secret", "", strings.TrimSuffix(adapterURL, "-failed") + "-failure", http.StatusNotFound, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("INTERNAL_SERVICE_SECRET", tc.configured)
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			if tc.header != "" {
				req.Header.Set("X-Internal-Secret", tc.header)
			}
			r.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("POST %s = %d, want %d: %s", tc.path, w.Code, tc.want, w.Body.String())
			}
			if tc.wantError == "" {
				return
			}
			var got struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.Error != tc.wantError {
				t.Fatalf("POST %s body = %s, want error %q", tc.path, w.Body.String(), tc.wantError)
			}
		})
	}
}

// The reader has to reject each wrong wiring, or the check above proves nothing.
func TestRunFailedRouteWiringReaderRejectsWrongWiring(t *testing.T) {
	const ok = `package main
func main() {
	r := gin.Default()
	api := r.Group("/api/v1")
	api.Use(handlers.AuthRequiredMiddleware())
	internal := r.Group("/api/v1/internal")
	internal.Use(handlers.InternalServiceMiddleware())
	{
		internal.POST("/explorer/models/:id/run", handlers.RunSavedQueryModelInternal)
		internal.POST("/explorer/models/:id/run-failed", handlers.RecordSavedQueryModelRunFailureInternal)
	}
	r.Run()
}`
	const route = `internal.POST("/explorer/models/:id/run-failed", handlers.RecordSavedQueryModelRunFailureInternal)`
	const use = "\tinternal.Use(handlers.InternalServiceMiddleware())\n"
	controls := map[string]string{
		"not registered":         strings.Replace(ok, route, "", 1),
		"path renamed":           strings.Replace(ok, ":id/run-failed", ":id/run-failure", 1),
		"another group prefix":   strings.Replace(ok, `r.Group("/api/v1/internal")`, `r.Group("/api/v2/internal")`, 1),
		"middleware removed":     strings.Replace(ok, use, "", 1),
		"middleware after route": strings.Replace(strings.Replace(ok, use, "", 1), "\tr.Run()", use+"\tr.Run()", 1),
		"on the user api group":  strings.Replace(ok, route, strings.Replace(route, "internal.POST", "api.POST", 1), 1),
		"only on one branch":     strings.Replace(ok, route, "if debug { "+route+" }", 1),
		"in a function literal":  strings.Replace(ok, route, "func() { "+route+" }()", 1),
		"registered as GET":      strings.Replace(ok, route, strings.Replace(route, "internal.POST", "internal.GET", 1), 1),
	}
	if prefix, rel, problem := runFailedRouteWiring(ok); problem != "" || prefix+rel != adapterRunFailedPath {
		t.Fatalf("reader rejects correct wiring: %q (%s%s)", problem, prefix, rel)
	}
	for name, src := range controls {
		if src == ok {
			t.Fatalf("control %q did not change the source", name)
		}
		if _, _, problem := runFailedRouteWiring(src); problem == "" {
			t.Errorf("reader accepts wrong wiring %q", name)
		}
	}
}

// runFailedRouteWiring finds, in func main of src, the one unconditional
// X.POST("<path>", handlers.RecordSavedQueryModelRunFailureInternal) and returns the
// group prefix X was created with and the path it registers. problem is non-empty when
// the route is missing, registered more than once, not under adapterRunFailedPath, or
// not preceded by X.Use(handlers.InternalServiceMiddleware()).
//
// Only statements that always run are read: main's own statements and plain { } blocks.
// A registration inside an if, a loop or a function literal is treated as absent.
func runFailedRouteWiring(src string) (prefix, rel, problem string) {
	file, err := parser.ParseFile(token.NewFileSet(), "main.go", src, 0)
	if err != nil {
		return "", "", "parse main.go: " + err.Error()
	}
	var body *ast.BlockStmt
	for _, decl := range file.Decls {
		if fn, isFunc := decl.(*ast.FuncDecl); isFunc && fn.Recv == nil && fn.Name.Name == "main" {
			body = fn.Body
		}
	}
	if body == nil {
		return "", "", "main.go has no func main"
	}

	groups := map[string]string{} // variable -> prefix it was created with
	secured := map[string]bool{}  // variable -> InternalServiceMiddleware installed so far
	var routeGroup, routePath string
	registrations := 0
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
				if sel, isSel := call.Fun.(*ast.SelectorExpr); isSel && sel.Sel.Name == "Group" {
					if lit, ok := stringLit(call.Args[0]); ok {
						groups[id.Name] = lit
						secured[id.Name] = false
					}
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
				switch {
				case sel.Sel.Name == "Use" && len(call.Args) == 1 && isHandlersCall(call.Args[0], "InternalServiceMiddleware"):
					secured[recv.Name] = true
				case len(call.Args) == 2 && isHandlersRef(call.Args[1], runFailedHandler):
					registrations++
					if sel.Sel.Name != http.MethodPost {
						problem = "main.go registers " + runFailedHandler + " with " + sel.Sel.Name + ", want POST"
						return
					}
					path, ok := stringLit(call.Args[0])
					if !ok {
						problem = "main.go registers " + runFailedHandler + " on a path that is not a string literal"
						return
					}
					if _, known := groups[recv.Name]; !known {
						problem = "main.go registers " + runFailedHandler + " on " + recv.Name + ", which is not a route group created in main"
						return
					}
					if !secured[recv.Name] {
						problem = "main.go registers " + runFailedHandler + " on group " + recv.Name + " before (or without) " + recv.Name + ".Use(handlers.InternalServiceMiddleware())"
						return
					}
					routeGroup, routePath = recv.Name, path
				}
			}
		}
	}
	walk(body.List)
	switch {
	case problem != "":
		return "", "", problem
	case registrations == 0:
		return "", "", "main.go never registers " + runFailedHandler + " unconditionally in func main"
	case registrations > 1:
		return "", "", "main.go registers " + runFailedHandler + " " + strconv.Itoa(registrations) + " times"
	}
	prefix = groups[routeGroup]
	if prefix+routePath != adapterRunFailedPath {
		return "", "", "main.go serves " + runFailedHandler + " at " + prefix + routePath + ", but the adapter posts to " + adapterRunFailedPath
	}
	return prefix, routePath, ""
}

func stringLit(e ast.Expr) (string, bool) {
	lit, isLit := e.(*ast.BasicLit)
	if !isLit || lit.Kind != token.STRING {
		return "", false
	}
	v, err := strconv.Unquote(lit.Value)
	return v, err == nil
}

func isHandlersRef(e ast.Expr, name string) bool {
	sel, isSel := e.(*ast.SelectorExpr)
	if !isSel || sel.Sel.Name != name {
		return false
	}
	pkg, isIdent := sel.X.(*ast.Ident)
	return isIdent && pkg.Name == "handlers"
}

func isHandlersCall(e ast.Expr, name string) bool {
	call, isCall := e.(*ast.CallExpr)
	return isCall && len(call.Args) == 0 && isHandlersRef(call.Fun, name)
}
