package main

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// The orchestrator must not serve connector metadata. Deleted 2026-08-29 —
// `GET /api/v1/mcp/connectors[/:name[/capabilities]]` and their handler.
//
// Why this needs a guard rather than just a deletion: the handler was dead in a
// way that looked harmless (`{"connectors":[],"total":0}` — a count of zero is
// not an error), so the obvious "fix" is to repair the walk. Repairing it is
// worse than leaving it broken. This router's group is a bare
// router.Group("/api/v1") with no auth middleware on it, and the default OSS
// compose publishes ${RSYNC_HP_ORCHESTRATOR:-8081}:8080, so a working listing
// here is an unauthenticated connector catalog with config schemas on every
// self-host box. api-gateway already serves exactly this data behind
// AuthRequired + EmailVerified + CSRF + RateLimit + WorkspaceContext. The
// orchestrator's /api/v1/connections routes were deleted on 2026-05-22 for the
// identical property; this is the second instance, and the guard exists so
// there is not a third.
//
// Denominator discipline: an "assert no route matches" test passes trivially if
// the router it inspects is empty, which is the failure mode this repo has been
// bitten by repeatedly (a count of zero reading as a pass). So the test proves
// it is looking at a populated router, and specifically at the group the
// deleted routes lived in, BEFORE it asserts an absence.
//
// Those denominators are deliberately name-free. An earlier revision required
// `/api/v1/pipeline/shapes` and `/health` to be present by name; that made an
// unrelated rename of either route fail the connector-metadata guard for a
// reason having nothing to do with connector metadata. Nothing about this
// claim depends on which routes the group contains — only that the group is
// non-empty and reachable — so the checks below count routes under the
// `/api/v1/` prefix and probe whichever static GET the group happens to
// expose. Renaming any single route leaves the guard green; emptying the group,
// moving its prefix, or re-registering the deleted routes turns it red.
func TestOrchestratorServesNoConnectorMetadataRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// A real but never-connecting handle: setupRouter starts the healthwatch
	// goroutine, so db must be non-nil-safe. No request below reaches a live DB;
	// port 1 refuses immediately rather than hanging.
	db, err := sql.Open("postgres", "postgres://127.0.0.1:1/rsync_none?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	router := setupRouter(nil, &kafka.TopologyManager{}, nil, nil, nil, db, nil, nil)
	routes := router.Routes()

	// The prefix the deleted routes were registered under: they were
	// `api.GET("/mcp/connectors…")` on `api := router.Group("/api/v1")`.
	const groupPrefix = "/api/v1/"

	var (
		groupPaths    []string // everything under groupPrefix
		topLevelPaths []string // everything outside it
		staticGETs    []string // GETs under groupPrefix with no :param / *wildcard
	)
	for _, r := range routes {
		if strings.HasPrefix(r.Path, groupPrefix) {
			groupPaths = append(groupPaths, r.Method+" "+r.Path)
			if r.Method == http.MethodGet && !strings.ContainsAny(r.Path, ":*") {
				staticGETs = append(staticGETs, r.Path)
			}
			continue
		}
		topLevelPaths = append(topLevelPaths, r.Method+" "+r.Path)
	}

	// --- Positive denominator 1: the router is populated at all. -------------
	const minRoutes = 30 // 37 at the time of writing
	if len(routes) < minRoutes {
		t.Fatalf("setupRouter registered only %d routes (want >= %d) — this test is "+
			"inspecting a broken or empty router, so its absence assertions below "+
			"would pass vacuously", len(routes), minRoutes)
	}

	// --- Positive denominator 2: the group itself is enumerated. -------------
	// Not just "some routes exist": router.Routes() must actually reach into the
	// group the deleted routes were registered on. Counted, not named, so that
	// renaming any one route in the group cannot fail this guard.
	const minGroupRoutes = 15 // 31 at the time of writing
	if len(groupPaths) < minGroupRoutes {
		t.Fatalf("only %d of %d enumerated routes sit under %q (want >= %d) — the group "+
			"the deleted routes lived in is not being enumerated (renamed prefix? group "+
			"no longer registered?), so an absence assertion over it proves nothing",
			len(groupPaths), len(routes), groupPrefix, minGroupRoutes)
	}

	// --- Positive denominator 3: the top-level tree is enumerated too. -------
	if len(topLevelPaths) == 0 {
		t.Fatalf("router.Routes() returned %d routes and every one is under %q — the "+
			"top-level routes (/health, /ready, /version…) are missing from the "+
			"enumeration, so it is not showing the whole router", len(routes), groupPrefix)
	}

	// --- The assertion. -----------------------------------------------------
	var offenders []string
	for _, r := range routes {
		if strings.Contains(strings.ToLower(r.Path), "mcp/connectors") {
			offenders = append(offenders, r.Method+" "+r.Path)
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("the orchestrator registers %d connector-metadata route(s): %s\n"+
			"These were deleted deliberately. This router group has no auth middleware and "+
			"the OSS compose publishes it on 8081, so serving the connector catalog here "+
			"exposes it unauthenticated. Use api-gateway GET /api/v1/connectors instead "+
			"(api-gateway/internal/handlers/tools.go, ListMCPConnectors).",
			len(offenders), strings.Join(offenders, ", "))
	}

	// --- Behavioural half, with a live control. -----------------------------
	// A 404 on a deleted path is only evidence if this router answers something
	// other than 404 somewhere. The control asks that of whatever static GET the
	// group currently exposes — any non-404 (200, 401 from requirePrincipal, 500
	// from a handler that wanted the dead DB) proves the request was routed to a
	// handler, which is the only property the 404s below need.
	if len(staticGETs) == 0 {
		t.Fatalf("no parameter-free GET route under %q to use as a serving control "+
			"(%d group routes enumerated) — without one, the 404s asserted below "+
			"cannot be distinguished from a router that 404s everything",
			groupPrefix, len(groupPaths))
	}
	var servedPath string
	var servedCode int
	var controlTried []string
	for _, path := range staticGETs {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		controlTried = append(controlTried, path)
		if w.Code != http.StatusNotFound {
			servedPath, servedCode = path, w.Code
			break
		}
	}
	if servedPath == "" {
		t.Fatalf("control: all %d parameter-free GET routes under %q returned 404 (%s) — "+
			"this router is not serving anything, so the 404s below are not evidence "+
			"of anything", len(controlTried), groupPrefix, strings.Join(controlTried, ", "))
	}
	t.Logf("control: GET %s → %d (routed, not 404)", servedPath, servedCode)

	for _, path := range []string{
		"/api/v1/mcp/connectors",
		"/api/v1/mcp/connectors?include_internal=true",
		"/api/v1/mcp/connectors/postgresql",
		"/api/v1/mcp/connectors/minio",
		"/api/v1/mcp/connectors/postgresql/capabilities",
	} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusNotFound {
			t.Errorf("GET %s returned %d (want 404): body=%s", path, w.Code, w.Body.String())
		}
	}
}
