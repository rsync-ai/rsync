package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// GET /pipelines/:id/cdc/backfill is the read-only "would a re-snapshot be
// accepted?" check the Re-snapshot card makes on load. Everyone who can see the
// card must be able to ask it, so a VIEWER passes and the orchestrator's answer
// is forwarded as-is. The POST on the same path stays Member-only — the control
// below — so opening the read did not open the write.

func TestGetPipelineCDCBackfillCapability_ViewerAllowedAndForwarded(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("viewer"))

	var gotMethod, gotPath string
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"supported":false,"signal_channel":"","error":"cdc_backfill_not_supported"}`))
	}))
	defer orch.Close()
	t.Setenv("ORCHESTRATOR_URL", orch.URL)

	r := wsScopeRouterAsRole(http.MethodGet, "/p/:id/cdc/backfill", GetPipelineCDCBackfillCapability, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/p/"+wsScopePipeline+"/cdc/backfill", nil))

	if w.Code != http.StatusOK {
		t.Fatalf("a viewer must be able to ask; got %d: %s", w.Code, w.Body.String())
	}
	if gotMethod != http.MethodGet || gotPath != "/api/v1/cdc/pipelines/"+wsScopePipeline+"/backfill" {
		t.Fatalf("proxied %s %s; want GET /api/v1/cdc/pipelines/%s/backfill", gotMethod, gotPath, wsScopePipeline)
	}
	if !strings.Contains(w.Body.String(), `"supported":false`) {
		t.Fatalf("orchestrator answer was not forwarded: %s", w.Body.String())
	}
}

func TestBackfillPipelineCDCTables_ViewerStillDenied(t *testing.T) {
	defer expectViewerRoleGate(t)()
	r := wsScopeRouterAsRole(http.MethodPost, "/p/:id/cdc/backfill", BackfillPipelineCDCTables, "viewer")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/p/"+wsScopePipeline+"/cdc/backfill", strings.NewReader(`{"tables":["public.users"]}`)))
	assertViewerDenied(t, w, "CDC backfill")
}
