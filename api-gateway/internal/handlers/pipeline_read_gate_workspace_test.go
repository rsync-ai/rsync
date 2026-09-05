package handlers

import (
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"api-gateway/internal/config"
	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

// Three pipeline READ endpoints were the last users of canAccessPipeline, which
// gated on workspace MEMBERSHIP alone — "does the caller belong to the workspace
// that owns this pipeline" — and never on the caller's ACTIVE workspace.
//
// Found on prod: with `demo` active, GET /pipelines/{personal-id} correctly
// returned 404 while these three returned 200, leaking schema_name
// "rsync_public_20912e3b", table_name "demo_customers" and read_rows 2000 out of
// the non-active workspace. Not a cross-user breach (membership was still
// required), but it broke the isolation model the parent route enforces, and it
// is what let a stranded detail route render real data instead of an error.
//
// These pin the corrected gate: the role query binds the ACTIVE workspace, so a
// pipeline outside it resolves no row and the handler answers 404 — never 403,
// which would confirm the resource exists.

const (
	gateRoleQuery = `SELECT wm\.role\s+FROM pipelines r`
	// The workspace the caller has ACTIVE — not the one owning the pipeline.
	activeWS   = "99999999-9999-9999-9999-999999999999"
	gateUserID = "00000000-0000-0000-0000-000000000000"
	gatePipeID = "11111111-1111-1111-1111-111111111111"
)

// newGateRouter wires the auth + workspace-context values the gate reads, then
// registers `handler` at `route`.
func newGateRouter(route string, handler gin.HandlerFunc) *gin.Engine {
	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", gateUserID)
		c.Set(ctxWorkspaceID, activeWS)
		c.Next()
	})
	r.GET(route, handler)
	return r
}

func TestPipelineReadEndpoints_ForeignActiveWorkspace_Return404(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// GetPipelineMonitoringOverview returns 404 when the feature is off, which
	// would make its case pass for the wrong reason. Enable it, then assert the
	// gate query actually ran (below) so a vacuous pass is impossible.
	os.Setenv("FEATURE_MONITORING_OVERVIEW", "true")
	defer os.Unsetenv("FEATURE_MONITORING_OVERVIEW")
	config.LoadFeatures()
	if f := config.GetFeatures(); f == nil || !f.MonitoringOverview {
		t.Fatalf("monitoring overview feature must be enabled for this test to be meaningful")
	}

	cases := []struct {
		name    string
		route   string
		url     string
		handler gin.HandlerFunc
	}{
		{
			name:    "table stats",
			route:   "/api/v1/pipelines/:id/table-stats",
			url:     "/api/v1/pipelines/" + gatePipeID + "/table-stats",
			handler: GetPipelineTableStats,
		},
		{
			name:    "monitoring overview",
			route:   "/api/v1/pipelines/:id/monitoring/overview",
			url:     "/api/v1/pipelines/" + gatePipeID + "/monitoring/overview",
			handler: GetPipelineMonitoringOverview,
		},
		{
			name:    "schedule list",
			route:   "/api/v1/pipelines/:id/schedules",
			url:     "/api/v1/pipelines/" + gatePipeID + "/schedules",
			handler: ListPipelineSchedules,
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

			// The pipeline exists and the caller is a member of its owning
			// workspace — but that workspace is not the ACTIVE one, so the
			// three-way join finds nothing. This empty result IS the fix: the old
			// membership-only query matched here and returned the row.
			mock.ExpectQuery(gateRoleQuery).
				WithArgs(gatePipeID, gateUserID, activeWS).
				WillReturnRows(sqlmock.NewRows([]string{"role"}))

			rr := httptest.NewRecorder()
			newGateRouter(tc.route, tc.handler).
				ServeHTTP(rr, httptest.NewRequest(http.MethodGet, tc.url, nil))

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

func TestListPipelineSchedules_ActiveWorkspaceOwnsPipeline_Allowed(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer sqlDB.Close()
	db.DB = sqlDB

	// A VIEWER in the active workspace that owns the pipeline: reads are allowed,
	// so the tightened gate must not have turned a read endpoint into a
	// member-only one.
	mock.ExpectQuery(gateRoleQuery).
		WithArgs(gatePipeID, gateUserID, activeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("viewer"))
	mock.ExpectQuery(`FROM pipeline_schedules`).
		WithArgs(gatePipeID).
		WillReturnRows(sqlmock.NewRows([]string{
			"schedule_id", "pipeline_id", "schedule_type", "schedule_spec",
			"temporal_schedule_id", "status", "created_by", "created_at",
			"updated_at", "paused_at", "paused_reason",
		}))

	rr := httptest.NewRecorder()
	newGateRouter("/api/v1/pipelines/:id/schedules", ListPipelineSchedules).
		ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/pipelines/"+gatePipeID+"/schedules", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 for a viewer in the owning active workspace, got %d: %s",
			rr.Code, rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
