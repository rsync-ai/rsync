package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// TestCORSMiddlewareIsRegisteredBeforeAnyRoute locks the fix for the gateway's
// public routes answering with no CORS headers at all.
//
// Symptom, from the System Test Suite on a live install: "Backend /health — Failed to
// fetch", while the three /api/v1 probes on the same page reached the same gateway
// fine. /health is the one probe whose entire job is to say whether the gateway is
// reachable, so its failure reads as a dead backend.
//
// Root cause is gin's registration model, not a missing header. gin copies the
// group's CURRENT handler chain into each route when the route is registered
// (routergroup.go: handle -> combineHandlers, which does make+copy — a copy, not a
// reference to a slice that later grows). r.Use(CORS) sat below the public route
// registrations, so /health, /api/health, /version, /ready and /ws were built with a
// chain of tracing+metrics only and could never gain the CORS handler, while every
// /api/v1 route registered after the Use call had it.
//
// Engine.Use additionally calls rebuild404Handlers/rebuild405Handlers, so a
// NONEXISTENT path on this gateway DID return CORS headers. Debugging by poking a
// neighbouring URL therefore gives the exactly inverted signal, which is why this
// guard is on the ordering in source rather than on one endpoint's response.
//
// The check is an AST walk of main.go because the defect is positional: the
// middleware worked perfectly, it was simply installed too late. A behavioural test
// on corsMiddleware alone (below) stays green through the entire bug.
func TestCORSMiddlewareIsRegisteredBeforeAnyRoute(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}

	// Route registrations on the engine: r.GET/POST/... and r.Group.
	routeMethods := map[string]bool{
		"GET": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true,
		"HEAD": true, "OPTIONS": true, "Any": true, "Group": true, "Handle": true,
		"StaticFile": true, "Static": true, "StaticFS": true, "NoRoute": true,
	}

	corsUseOffset := -1
	firstRouteOffset := -1
	var firstRouteDesc string
	routeCount := 0

	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		recv, ok := sel.X.(*ast.Ident)
		if !ok || recv.Name != "r" {
			return true
		}

		off := int(call.Pos())

		if sel.Sel.Name == "Use" && len(call.Args) == 1 {
			if inner, ok := call.Args[0].(*ast.CallExpr); ok {
				if fn, ok := inner.Fun.(*ast.Ident); ok && fn.Name == "corsMiddleware" {
					if corsUseOffset == -1 || off < corsUseOffset {
						corsUseOffset = off
					}
				}
			}
			return true
		}

		if routeMethods[sel.Sel.Name] {
			routeCount++
			if firstRouteOffset == -1 || off < firstRouteOffset {
				firstRouteOffset = off
				firstRouteDesc = "r." + sel.Sel.Name + " at " + fset.Position(call.Pos()).String()
			}
		}
		return true
	})

	// Positive denominator. If the walk stopped matching — the engine variable was
	// renamed, the routes moved to a helper — every assertion below would pass
	// vacuously and this guard would silently stop guarding.
	if routeCount < 5 {
		t.Fatalf("found only %d route registrations on r in main.go; the AST walk stopped "+
			"matching, so this guard is not checking anything", routeCount)
	}
	if corsUseOffset == -1 {
		t.Fatal("main.go never calls r.Use(corsMiddleware()); the gateway serves no CORS " +
			"headers at all, or the middleware was inlined again — inline it and this " +
			"ordering becomes unassertable")
	}

	if corsUseOffset > firstRouteOffset {
		t.Errorf("r.Use(corsMiddleware()) is registered AFTER the first route (%s). "+
			"gin copies the handler chain at registration time, so every route above "+
			"the Use call answers with no Access-Control-* headers — which is how "+
			"/api/health became an opaque 'Failed to fetch' in the browser while "+
			"/api/v1/* worked.", firstRouteDesc)
	}
}

// TestCORSMiddlewareSetsHeaders is the behavioural half: the middleware itself must
// still do its job after being lifted out of the inline closure. It deliberately
// does NOT prove the bug is fixed — it passed throughout — which is the point of
// keeping both tests side by side.
func TestCORSMiddlewareSetsHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("RSYNC_CORS_ORIGINS", "https://app.example.com")

	newEngine := func() *gin.Engine {
		e := gin.New()
		e.Use(corsMiddleware())
		e.GET("/api/health", func(c *gin.Context) { c.String(http.StatusOK, "ok") })
		return e
	}

	t.Run("allowlisted origin is echoed back", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
		req.Header.Set("Origin", "https://app.example.com")
		newEngine().ServeHTTP(w, req)

		if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://app.example.com" {
			t.Errorf("Access-Control-Allow-Origin = %q, want the request origin", got)
		}
		if got := w.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
			t.Errorf("Access-Control-Allow-Credentials = %q, want \"true\" — the session "+
				"is a cookie, so a browser drops the response without it", got)
		}
	})

	t.Run("an origin outside the allowlist gets no allow-origin header", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
		req.Header.Set("Origin", "https://evil.example.com")
		newEngine().ServeHTTP(w, req)

		if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
			t.Errorf("Access-Control-Allow-Origin = %q for a non-allowlisted origin; "+
				"hoisting the middleware must not widen who it answers", got)
		}
	})

	t.Run("preflight is short-circuited", func(t *testing.T) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodOptions, "/api/health", nil)
		req.Header.Set("Origin", "https://app.example.com")
		newEngine().ServeHTTP(w, req)

		if w.Code != http.StatusNoContent {
			t.Errorf("OPTIONS status = %d, want 204", w.Code)
		}
	})
}
