package handlers

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
)

// Regression tests for KI-CDC-STATUS-401-FORCES-LOGOUT (backend half).
//
// Bug class: an api-gateway → orchestrator proxy that carries NO principal when
// INTERNAL_SERVICE_SECRET is unset (the dev default), so every ownership-gated
// orchestrator route answers 401 — and the gateway relays that 401 to the
// browser, whose auth layer reads any 401 as a dead session.
//
// Two invariants, each checked across EVERY CDC control proxy rather than just
// the one the bug was found on:
//  1. the proxied call names the authenticated caller (X-User-ID), so the
//     orchestrator's own workspace-role gate can run for that user;
//  2. an upstream 401 never reaches the browser as a 401.

// cdcProxyRoutes lists every gateway CDC control proxy that reaches an
// ownership-gated orchestrator route (assertPipelineOwner /
// assertPipelineOwnerForHandlers in backend-orchestrator).
var cdcProxyRoutes = []struct {
	name, method, route, upstreamMethod, upstreamPath string
	h                                                 gin.HandlerFunc
}{
	{"status", http.MethodGet, "/cdc/status", http.MethodGet, "/status", GetPipelineCDCStatus},
	{"restart", http.MethodPost, "/cdc/restart", http.MethodPost, "/restart", RestartPipelineCDC},
	{"recover", http.MethodPost, "/cdc/recover", http.MethodPost, "/recover", RecoverPipelineCDC},
	{"backfill", http.MethodPost, "/cdc/backfill", http.MethodPost, "/backfill", BackfillPipelineCDCTables},
	{"backfill-capability", http.MethodGet, "/cdc/backfill", http.MethodGet, "/backfill", GetPipelineCDCBackfillCapability},
	{"pause", http.MethodPost, "/cdc/pause", http.MethodPut, "/pause", PausePipelineCDC},
	{"resume", http.MethodPost, "/cdc/resume", http.MethodPut, "/resume", ResumePipelineCDC},
}

// principalGatedOrchestrator emulates the orchestrator's requirePrincipal +
// assertPipelineOwner pair in a non-production environment with the secret
// unset: a request with neither X-Internal-Secret nor X-User-ID is refused with
// 401 {"error":"authentication required"} (auth_middleware.go requirePrincipal
// step 3 + cdc_authz.go assertResourceWorkspaceRole). It records the X-User-ID
// it was handed.
func principalGatedOrchestrator(t *testing.T, wantMethod, wantPath string) *struct {
	sync.Mutex
	userID string
} {
	t.Helper()
	seen := &struct {
		sync.Mutex
		userID string
	}{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != wantMethod || r.URL.Path != wantPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		seen.Lock()
		seen.userID = r.Header.Get("X-User-ID")
		seen.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("X-Internal-Secret") == "" && r.Header.Get("X-User-ID") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"authentication required"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("ORCHESTRATOR_URL", srv.URL)
	return seen
}

// devAuthRouter runs the REAL AuthRequiredMiddleware (dev fallback: X-User-ID
// header, no session token) before the workspace keys and the handler, so the
// test covers the path the user id takes from authentication to the outbound
// proxy request — not a value a test helper planted in gin's key map.
func devAuthRouter(method, path string, h gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(AuthRequiredMiddleware())
	r.Use(func(c *gin.Context) {
		c.Set(ctxWorkspaceID, wsScopeWS)
		c.Set(ctxWorkspaceRole, "owner")
		c.Next()
	})
	r.Handle(method, path, h)
	return r
}

func TestCDCProxiesCarryTheCallerWhenNoInternalSecret(t *testing.T) {
	for _, rt := range cdcProxyRoutes {
		t.Run(rt.name, func(t *testing.T) {
			t.Setenv("ENVIRONMENT", "development")
			t.Setenv("INTERNAL_SERVICE_SECRET", "")
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			expectPipelineOwnerRole(mock)

			seen := principalGatedOrchestrator(t, rt.upstreamMethod,
				"/api/v1/cdc/pipelines/"+cdcProxyPipelineID+rt.upstreamPath)

			r := devAuthRouter(rt.method, "/api/v1/pipelines/:id"+rt.route, rt.h)
			req := httptest.NewRequest(rt.method, "/api/v1/pipelines/"+cdcProxyPipelineID+rt.route, strings.NewReader(`{}`))
			req.Header.Set("X-User-ID", wsScopeUser)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("%s: the proxied call must carry a principal the orchestrator accepts; got %d %s",
					rt.name, rr.Code, rr.Body.String())
			}
			seen.Lock()
			defer seen.Unlock()
			if seen.userID != wsScopeUser {
				t.Fatalf("%s: orchestrator must be told the authenticated caller; X-User-ID=%q want %q",
					rt.name, seen.userID, wsScopeUser)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

func TestCDCProxiesNeverRelayAnUpstream401(t *testing.T) {
	for _, rt := range cdcProxyRoutes {
		t.Run(rt.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			expectPipelineOwnerRole(mock)

			fakeOrchestrator(t, rt.upstreamMethod, "/api/v1/cdc/pipelines/"+cdcProxyPipelineID+rt.upstreamPath,
				http.StatusUnauthorized, `{"error":"authentication required"}`)

			r := wsScopeRouter(rt.method, "/api/v1/pipelines/:id"+rt.route, rt.h)
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, httptest.NewRequest(rt.method, "/api/v1/pipelines/"+cdcProxyPipelineID+rt.route, strings.NewReader(`{}`)))

			if rr.Code == http.StatusUnauthorized {
				t.Fatalf("%s: an upstream 401 reached the browser as a 401 — the frontend reads that as a dead session", rt.name)
			}
			if rr.Code != http.StatusBadGateway {
				t.Fatalf("%s: want 502 for an upstream auth failure, got %d", rt.name, rr.Code)
			}
			if !strings.Contains(rr.Body.String(), "orchestrator_auth_failed") ||
				!strings.Contains(rr.Body.String(), "INTERNAL_SERVICE_SECRET") {
				t.Fatalf("%s: the 502 must say what is wrong; got %s", rt.name, rr.Body.String())
			}
		})
	}
}

// A 403 is a real authorization answer about this user and never ends a
// session, so it is forwarded; only 401 is remapped.
func TestBrowserStatusForUpstreamRemapsOnly401(t *testing.T) {
	for in, want := range map[int]int{200: 200, 400: 400, 401: 502, 403: 403, 404: 404, 500: 500, 502: 502} {
		if got := browserStatusForUpstream(in); got != want {
			t.Fatalf("browserStatusForUpstream(%d) = %d, want %d", in, got, want)
		}
	}
}

// The caller only reaches the outbound request through the request context, so
// a proxy built from context.Background() would silently drop it. Pin the
// helper pair directly.
func TestSetInternalServiceSecretForwardsTheBoundCaller(t *testing.T) {
	t.Setenv("INTERNAL_SERVICE_SECRET", "")
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodGet, "/", nil)
	bindCallerToRequest(c, wsScopeUser)

	out, _ := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, "http://orchestrator/x", nil)
	setInternalServiceSecret(out)
	if got := out.Header.Get("X-User-ID"); got != wsScopeUser {
		t.Fatalf("X-User-ID = %q, want %q", got, wsScopeUser)
	}
	if out.Header.Get("X-Internal-Secret") != "" {
		t.Fatalf("no secret configured, none must be sent")
	}

	anon, _ := http.NewRequest(http.MethodGet, "http://orchestrator/x", nil)
	setInternalServiceSecret(anon)
	if anon.Header.Get("X-User-ID") != "" {
		t.Fatalf("a request with no bound caller must not invent one")
	}
}

// Source guard for the forwarding half of the class: no handler may write an
// upstream service's status straight onto the browser response. Every such
// site goes through browserStatusForUpstream.
func TestNoHandlerRelaysAnUpstreamStatusVerbatim(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	verbatim := regexp.MustCompile(`c\.(JSON|Data|Status|AbortWithStatusJSON|AbortWithStatus)\(\s*resp\.StatusCode\b`)
	mapped := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			if verbatim.MatchString(line) {
				t.Errorf("%s:%d relays an upstream status verbatim; wrap it in browserStatusForUpstream: %s",
					f, i+1, strings.TrimSpace(line))
			}
		}
		mapped += strings.Count(string(src), "browserStatusForUpstream(")
	}
	// Non-vacuity: the guard must be looking at the directory that holds the
	// forwarding sites (11 call sites + the definition on 2026-09-27).
	if mapped < 10 {
		t.Fatalf("found only %d browserStatusForUpstream call sites; is the guard scanning the handlers directory?", mapped)
	}
}
