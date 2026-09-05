package handlers

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Regression tests for KI-CDC-CONTROL-502-BODY-NOT-IN-TOAST.
//
// The user-visible symptom was a CDC Pause/Resume toast that read exactly
// "Request failed" — the orchestrator's real reason ("kafka connect refused the
// pause of cdc-a9d7f773 (HTTP 404)") never reached the operator. The frontend
// half of that bug is fixed in frontend/src/lib/utils/error-handling.ts; this
// file holds the api-gateway half of the invariant:
//
//	A >= 400 answer from the orchestrator must reach the browser with its
//	status code intact AND with a non-empty body that names something
//	actionable — even when the orchestrator's own body is unreadable/empty.
//
// Before the fix, `body, _ := io.ReadAll(resp.Body)` discarded the read error
// and forwarded a zero-length application/json body, which is precisely the
// input that made the old frontend helper collapse to the bare literal.
const (
	cdcProxyPipelineID = "d13898e0-094a-422c-ab62-8e752235957f"
	// The exact shape backend-orchestrator/cmd/orchestrator/main.go:1882-1886
	// emits when Kafka Connect refuses the control action.
	cdcProxyOrchestratorReason = "kafka connect refused the pause of cdc-a9d7f773 (HTTP 404)"
)

// expectPipelineOwnerRole satisfies requirePipelineWorkspaceRole's single query
// (built in workspace_context.go:300-305) with an "owner" row.
func expectPipelineOwnerRole(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(cdcProxyPipelineID, wsScopeUser, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("owner"))
}

// fakeOrchestrator answers `wantPath` with (status, body) and 404s anything else,
// so a proxy that calls the wrong upstream path fails loudly rather than quietly.
func fakeOrchestrator(t *testing.T, wantMethod, wantPath string, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != wantMethod || r.URL.Path != wantPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if body != "" {
			_, _ = w.Write([]byte(body))
		}
	}))
	t.Cleanup(srv.Close)
	os.Setenv("ORCHESTRATOR_URL", srv.URL)
	t.Cleanup(func() { os.Unsetenv("ORCHESTRATOR_URL") })
	return srv
}

func TestPausePipelineCDC_502CarriesTheOrchestratorReason(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	expectPipelineOwnerRole(mock)

	fakeOrchestrator(t, http.MethodPut, "/api/v1/cdc/pipelines/"+cdcProxyPipelineID+"/pause",
		http.StatusBadGateway,
		`{"success":false,"pipeline_id":"`+cdcProxyPipelineID+`","connector_name":"cdc-a9d7f773","error":"`+cdcProxyOrchestratorReason+`","result":{"status_code":404}}`)

	r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines/:id/cdc/pause", PausePipelineCDC)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+cdcProxyPipelineID+"/cdc/pause", nil))

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status must be forwarded verbatim: want 502, got %d (%s)", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), cdcProxyOrchestratorReason) {
		t.Fatalf("the orchestrator's reason must survive the proxy; got %q", rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// THE DISCRIMINATING CASE (RED before the fix): when the upstream 502 body is
// empty, the proxy used to forward zero bytes, leaving the browser nothing to
// show. The toast literal "Request failed" is manufactured exactly here.
func TestPausePipelineCDC_BlankUpstream502StillCarriesAMessage(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	expectPipelineOwnerRole(mock)

	fakeOrchestrator(t, http.MethodPut, "/api/v1/cdc/pipelines/"+cdcProxyPipelineID+"/pause",
		http.StatusBadGateway, "")

	r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines/:id/cdc/pause", PausePipelineCDC)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+cdcProxyPipelineID+"/cdc/pause", nil))

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status must be forwarded verbatim: want 502, got %d", rr.Code)
	}
	got := strings.TrimSpace(rr.Body.String())
	if got == "" {
		t.Fatalf("a blank upstream >=400 body must be replaced by an actionable one, got an empty body")
	}
	if !strings.Contains(got, "Pause") {
		t.Fatalf("the synthesized message must name the action; got %q", got)
	}
	if !strings.Contains(got, "502") {
		t.Fatalf("the synthesized message must name the HTTP status; got %q", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestResumePipelineCDC_502CarriesTheOrchestratorReason(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	expectPipelineOwnerRole(mock)

	const resumeReason = "kafka connect refused the resume of cdc-a9d7f773 (HTTP 404)"
	fakeOrchestrator(t, http.MethodPut, "/api/v1/cdc/pipelines/"+cdcProxyPipelineID+"/resume",
		http.StatusBadGateway,
		`{"success":false,"pipeline_id":"`+cdcProxyPipelineID+`","connector_name":"cdc-a9d7f773","error":"`+resumeReason+`","result":{"status_code":404}}`)

	r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines/:id/cdc/resume", ResumePipelineCDC)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+cdcProxyPipelineID+"/cdc/resume", nil))

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("status must be forwarded verbatim: want 502, got %d (%s)", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), resumeReason) {
		t.Fatalf("the orchestrator's reason must survive the proxy; got %q", rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// A 2xx body is untouched by the new blank-body guard — only >=400 with a blank
// body is synthesized. This is the control that keeps the guard narrow.
func TestPausePipelineCDC_SuccessBodyIsForwardedUnchanged(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	expectPipelineOwnerRole(mock)

	const okBody = `{"success":true,"pipeline_id":"` + cdcProxyPipelineID + `"}`
	fakeOrchestrator(t, http.MethodPut, "/api/v1/cdc/pipelines/"+cdcProxyPipelineID+"/pause",
		http.StatusOK, okBody)

	r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines/:id/cdc/pause", PausePipelineCDC)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+cdcProxyPipelineID+"/cdc/pause", nil))

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d (%s)", rr.Code, rr.Body.String())
	}
	if rr.Body.String() != okBody {
		t.Fatalf("a successful body must be forwarded byte-for-byte; want %q got %q", okBody, rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
