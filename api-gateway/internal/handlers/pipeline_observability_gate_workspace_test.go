package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// Seven observability endpoints gated on `pipelines.created_by = <caller>` rather
// than on the caller's ACTIVE workspace. That check is workspace-blind in BOTH
// directions:
//
//   - the creator kept full read access after switching workspaces, so with
//     `demo` active these returned 200 for a `personal` pipeline. Confirmed on
//     prod against pipeline 20912e3b-…: /events answered 103,642 bytes of
//     execution ids, trace ids, stage ids and DATA_PLANE_METRICS; /checkpoints
//     leaked connection_id, source_table "pipeline_test.demo_customers" and the
//     row/byte cursors; /trends leaked execution ids, run counts and durations.
//   - a teammate who shares the pipeline's workspace but did not create it was
//     denied, or silently served an empty list.
//
// The corrected gate binds (pipeline, user, ACTIVE workspace) and answers 404 —
// never 403, which would confirm a foreign pipeline exists.
//
// Shared fixtures (gateRoleQuery, activeWS, gateUserID, gatePipeID, newGateRouter)
// live in pipeline_read_gate_workspace_test.go.

// newRawGateRouter is newGateRouter for the POST route, with an admin role so the
// RBAC check downstream of the gate would PASS — the 404 then proves tenancy is
// enforced ahead of RBAC rather than the request failing for an unrelated reason.
func newRawGateRouter(handler gin.HandlerFunc) *gin.Engine {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", gateUserID)
		c.Set("user_role", "admin")
		c.Set(ctxWorkspaceID, activeWS)
		c.Next()
	})
	r.POST("/api/v1/pipelines/:id/events/raw", handler)
	return r
}

func TestPipelineObservabilityEndpoints_ForeignActiveWorkspace_Return404(t *testing.T) {
	gin.SetMode(gin.TestMode)

	base := "/api/v1/pipelines/" + gatePipeID

	cases := []struct {
		name    string
		method  string
		route   string
		url     string
		body    string
		handler gin.HandlerFunc
	}{
		{
			name:    "checkpoints",
			method:  http.MethodGet,
			route:   "/api/v1/pipelines/:id/checkpoints",
			url:     base + "/checkpoints",
			handler: GetPipelineCheckpoints,
		},
		{
			name:    "run events",
			method:  http.MethodGet,
			route:   "/api/v1/pipelines/:id/events",
			url:     base + "/events",
			handler: GetPipelineEvents,
		},
		{
			name:    "trends",
			method:  http.MethodGet,
			route:   "/api/v1/pipelines/:id/trends",
			url:     base + "/trends",
			handler: GetPipelineTrends,
		},
		{
			// No execution_a/execution_b on purpose: the gate must run BEFORE
			// parameter validation, or a 400 would tell the caller "this pipeline
			// exists, your params are wrong" for a pipeline they cannot see.
			name:    "compare without params",
			method:  http.MethodGet,
			route:   "/api/v1/pipelines/:id/compare",
			url:     base + "/compare",
			handler: ComparePipelineRuns,
		},
		{
			// The upgrade must never happen: the gate runs before it, so this
			// plain (non-WebSocket) request gets a JSON 404 instead of a socket.
			name:    "event stream subscribe",
			method:  http.MethodGet,
			route:   "/api/v1/pipelines/:id/events/stream",
			url:     base + "/events/stream",
			handler: SubscribePipelineEvents,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock: %v", err)
			}
			defer sqlDB.Close()
			db.DB = sqlDB

			// The pipeline exists and the caller even created it — but its
			// workspace is not the ACTIVE one, so the three-way join finds
			// nothing. The old created_by predicate matched here and served data.
			mock.ExpectQuery(gateRoleQuery).
				WithArgs(gatePipeID, gateUserID, activeWS).
				WillReturnRows(sqlmock.NewRows([]string{"role"}))

			rr := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.url, strings.NewReader(tc.body))
			newGateRouter(tc.route, tc.handler).ServeHTTP(rr, req)

			if rr.Code != http.StatusNotFound {
				t.Fatalf("expected 404 for a pipeline outside the active workspace, got %d: %s",
					rr.Code, rr.Body.String())
			}
			// No data query may follow the failed gate, and the gate must have run
			// at all — an unexercised expectation means the handler returned early
			// and the 404 above proved nothing.
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// Raw events carry unredacted payloads, so this one is asserted separately with a
// role that clears RBAC: an admin acting in the wrong workspace must still 404.
func TestGetPipelineEventsRaw_ForeignActiveWorkspace_404BeforeRBAC(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer sqlDB.Close()
	db.DB = sqlDB

	mock.ExpectQuery(gateRoleQuery).
		WithArgs(gatePipeID, gateUserID, activeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+gatePipeID+"/events/raw",
		strings.NewReader(`{"justification":"debugging prod issue"}`))
	req.Header.Set("Content-Type", "application/json")
	newRawGateRouter(GetPipelineEventsRaw).ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for an admin acting outside the pipeline's workspace, got %d: %s",
			rr.Code, rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// The other half of the defect: a workspace VIEWER who did not create the
// pipeline must be served. Under the old created_by predicate the gate passed but
// the query returned nothing, so a teammate saw an empty event list for a run
// they were looking at. The data query must therefore carry no creator filter.
func TestGetPipelineEvents_TeammateInOwningWorkspace_ServedWithoutCreatorFilter(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer sqlDB.Close()
	db.DB = sqlDB

	mock.ExpectQuery(gateRoleQuery).
		WithArgs(gatePipeID, gateUserID, activeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
	// WithArgs(pipelineID) alone: a surviving `AND p.created_by = $2` would append
	// a second argument and fail this expectation.
	mock.ExpectQuery(`FROM pipeline_run_events e`).
		WithArgs(gatePipeID).
		WillReturnRows(sqlmock.NewRows([]string{
			"pipeline_id", "execution_id", "event_id", "seq", "event_type",
			"stage_id", "stage_group", "severity", "trace_id", "occurred_at",
			"received_at", "payload",
		}))

	rr := httptest.NewRecorder()
	newGateRouter("/api/v1/pipelines/:id/events", GetPipelineEvents).
		ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+gatePipeID+"/events", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for a viewer in the owning active workspace, got %d: %s",
			rr.Code, rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestGetPipelineCheckpoints_TeammateInOwningWorkspace_Served(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer sqlDB.Close()
	db.DB = sqlDB

	mock.ExpectQuery(gateRoleQuery).
		WithArgs(gatePipeID, gateUserID, activeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
	mock.ExpectQuery(`FROM pipeline_checkpoints`).
		WithArgs(gatePipeID).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "pipeline_id", "connection_id", "source_table", "position",
			"created_at", "updated_at",
		}))

	rr := httptest.NewRecorder()
	newGateRouter("/api/v1/pipelines/:id/checkpoints", GetPipelineCheckpoints).
		ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+gatePipeID+"/checkpoints", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for a viewer in the owning active workspace, got %d: %s",
			rr.Code, rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
