package main

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/backend-orchestrator/internal/agents/consumer"
	"github.com/rsync-ai/backend-orchestrator/internal/agents/retention"
)

// TestUngatedGroupsRequireAPrincipal is the sibling of
// TestTopologyRoutesRequireAPrincipal, and exists because that test's own
// comment was wrong: it said "sibling groups were already gated; this one was
// the omission". They were not. Four more groups on the same `api` router were
// reachable anonymously, and three of them answered an unauthenticated curl
// against the live deployment:
//
//	200  GET /orchestrator/api/v1/assess/supported-types
//	200  GET /orchestrator/api/v1/consumers/topics
//	200  GET /orchestrator/api/v1/health/connector-versions
//	401  GET /orchestrator/api/v1/cdc/data-pipelines   <- control: gating is per-group
//
// The 401 control matters: it proves the orchestrator was not simply open at the
// edge, so an anonymous 200 on the rows above is a missing group gate and not a
// misconfigured proxy.
//
// The assertion is status-only. Verified by mutation: dropping requirePrincipal
// from all four groups takes 8 of the 10 subtests RED, with the real answers the
// internet would have got — 200 for supported-types, /consumers/stop and
// /retention/stop, 400 for terminate, 500 where a nil agent panics past the
// missing gate.
//
// Two subtests do NOT discriminate and are here for coverage only: "run
// assessment" and "latest assessment" keep answering 401 with the middleware
// gone, because RunAssessment/GetLatest now call assertPipelineOwnerForHandlers,
// whose unauthenticated 401 body is byte-identical to the middleware's. The
// group gate is what the other eight pin; "supported types" is the sharpest of
// them, since it is a pure registry read with nothing behind it at all.
func TestUngatedGroupsRequireAPrincipal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	// Production-like so the X-User-ID dev fallback is off, and an empty
	// internal secret so the X-Internal-Secret branch cannot match an absent
	// header. Without both, this test would pass on a router with no gate at all.
	t.Setenv("ENVIRONMENT", "production")
	t.Setenv("INTERNAL_SERVICE_SECRET", "")

	db, err := sql.Open("postgres", "postgres://127.0.0.1:1/rsync_none?sslmode=disable")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Non-nil registry/agent so the two conditionally-mounted groups are
	// actually registered; anonymous requests are refused before either is used.
	router := setupRouter(nil, nil, nil, &consumer.Registry{}, &retention.Agent{}, db, nil, nil)

	const pipelineID = "abd8a64d-0000-0000-0000-000000000000"
	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		// Pre-flight assessment. The POST is the severe one: an anonymous caller
		// supplying another tenant's source_connection_id had rsync.ai decrypt those
		// credentials and dial the victim's database from inside its own network.
		{"run assessment", http.MethodPost, "/api/v1/pipelines/" + pipelineID + "/assess",
			`{"source_connection_id":"00000000-0000-0000-0000-000000000001"}`},
		{"latest assessment", http.MethodGet, "/api/v1/pipelines/" + pipelineID + "/assess/latest", ""},
		{"supported types", http.MethodGet, "/api/v1/assess/supported-types", ""},

		// Consumer control plane — ON BY DEFAULT, so every deployment had this.
		{"consumer status", http.MethodGet, "/api/v1/consumers/status", ""},
		{"stop consumer agent", http.MethodPost, "/api/v1/consumers/stop", ""},
		{"terminate consumers", http.MethodPost, "/api/v1/consumers/consumers/terminate", `{}`},

		// Retention control plane (latent: the agent defaults off).
		{"retention status", http.MethodGet, "/api/v1/retention/status", ""},
		{"stop retention agent", http.MethodPost, "/api/v1/retention/stop", ""},

		// Connector health rollup: which connectors this host runs, and which fail.
		{"connector versions", http.MethodGet, "/api/v1/health/connector-versions", ""},
		{"refresh connector versions", http.MethodPost, "/api/v1/health/connector-versions/refresh", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			if w.Code != http.StatusUnauthorized {
				t.Fatalf("anonymous %s %s: got %d, want 401 — this group is not gated by requirePrincipal",
					tc.method, tc.path, w.Code)
			}
		})
	}
}
