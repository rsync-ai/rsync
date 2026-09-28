package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// The chat's table-picker Cancel (#16) POSTs /pipelines/:id/stop. When the
// pipeline was already stopped elsewhere (another tab, the pipeline page) the stop
// is refused, and the chat treats the setup as cancelled only when the refusal
// names the pipeline's status as "stopped" under the current_status key
// (AgenticChatInterfaceV2.tsx handleCancelTableSetup). Any other status, such as
// "paused" where the run is still parked, must stay a refusal. These pin that
// response contract.
//
// Harness helpers (wsScopeMockDB, wsScopeRouter, wsScopeUser, wsScopeWS,
// wsScopePipeline, gateRoleRows) come from workspace_scoping_test.go and
// workspace_mutation_scoping_test.go in this package.
func TestStopPipeline_RefusalNamesCurrentStatus(t *testing.T) {
	for _, status := range []string{"stopped", "paused"} {
		status := status
		t.Run(status, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			mock.MatchExpectationsInOrder(false)

			mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
				WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
				WillReturnRows(gateRoleRows("member"))
			mock.ExpectQuery(`SELECT p\.status, .* FROM pipelines p WHERE p\.id = \$1 AND p\.workspace_id = \$2`).
				WithArgs(wsScopePipeline, wsScopeWS).
				WillReturnRows(sqlmock.NewRows([]string{"status", "is_cdc"}).AddRow(status, false))
			// No UPDATE is expected: a refused stop writes nothing, and sqlmock
			// fails any statement it was not told about.

			r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines/:id/stop", StopPipeline)
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+wsScopePipeline+"/stop", nil)
			r.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("expected 400 for a %s pipeline, got %d: %s", status, w.Code, w.Body.String())
			}
			var body map[string]interface{}
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatalf("response is not JSON: %v (%s)", err, w.Body.String())
			}
			if got, ok := body["current_status"].(string); !ok || got != status {
				t.Fatalf("expected current_status %q in the refusal, got body %v", status, body)
			}
			if got, _ := body["error"].(string); got != "Pipeline is not running" {
				t.Fatalf("expected the plain refusal reason, got body %v", body)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

// A CDC pipeline streams outside any execution, so Stop goes through the
// orchestrator, which parks the connector and stops the sink. These pin that the
// gateway does not flip the row when the orchestrator could not stop the
// connector, and that a paused CDC pipeline can be stopped.
func stopCDCHarness(t *testing.T, status string, orchestratorStatus int) (sqlmock.Sqlmock, *httptest.ResponseRecorder, *[]string, func()) {
	t.Helper()
	var calls []string
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		w.WriteHeader(orchestratorStatus)
		_, _ = w.Write([]byte(`{"success":` + map[bool]string{true: "true", false: "false"}[orchestratorStatus < 300] + `,"warnings":["w1"]}`))
	}))
	t.Setenv("ORCHESTRATOR_URL", orch.URL)

	mock, cleanup := wsScopeMockDB(t)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	mock.ExpectQuery(`SELECT p\.status, .* FROM pipelines p WHERE p\.id = \$1 AND p\.workspace_id = \$2`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"status", "is_cdc"}).AddRow(status, true))
	return mock, nil, &calls, func() { cleanup(); orch.Close() }
}

func serveStop(t *testing.T) *httptest.ResponseRecorder {
	t.Helper()
	r := wsScopeRouter(http.MethodPost, "/api/v1/pipelines/:id/stop", StopPipeline)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+wsScopePipeline+"/stop", nil))
	return w
}

func TestStopPipeline_CDCStopsConnectorAndClosesEveryExecution(t *testing.T) {
	for _, status := range []string{"running", "paused"} {
		t.Run(status, func(t *testing.T) {
			mock, _, calls, cleanup := stopCDCHarness(t, status, http.StatusOK)
			defer cleanup()
			mock.ExpectExec(`UPDATE pipelines SET status = 'stopped'`).
				WithArgs(wsScopePipeline, wsScopeWS).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(`FROM pipeline_progress`).WithArgs(wsScopePipeline).
				WillReturnRows(sqlmock.NewRows([]string{"execution_id"}))
			mock.ExpectQuery(`FROM executions`).WithArgs(wsScopePipeline).
				WillReturnRows(sqlmock.NewRows([]string{"id"}))
			// The stream row (id = pipeline id) is closed along with any other.
			mock.ExpectExec(`UPDATE executions[\s\S]*WHERE pipeline_id = \$1 AND status IN \('running','pending'\)`).
				WithArgs(wsScopePipeline).WillReturnResult(sqlmock.NewResult(0, 2))

			w := serveStop(t)
			if w.Code != http.StatusOK {
				t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
			}
			if len(*calls) != 1 || !strings.HasSuffix((*calls)[0], "/api/v1/cdc/pipelines/"+wsScopePipeline+"/stop") {
				t.Fatalf("orchestrator calls = %v", *calls)
			}
			if !strings.Contains(w.Body.String(), "w1") {
				t.Fatalf("sink warnings not passed on: %s", w.Body.String())
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}
}

func TestStopPipeline_CDCConnectorRefusedKeepsStatus(t *testing.T) {
	mock, _, _, cleanup := stopCDCHarness(t, "running", http.StatusBadGateway)
	defer cleanup()
	// No UPDATE is expected: sqlmock fails any statement it was not told about.
	w := serveStop(t)
	if w.Code != http.StatusBadGateway {
		t.Fatalf("code=%d body=%s; want the orchestrator's 502", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// A batch Stop cancelled the Temporal run and flipped the row but never told
// kafka-mcp-sink, whose "sink-<id8>-batch" worker kept writing the run's
// backlog into the destination for ~8.5 minutes on prod. Stop must ask the
// orchestrator to stop that worker — and, being best effort, must still stop
// the pipeline when the orchestrator cannot.
func stopBatchHarness(t *testing.T, orchestratorStatus int) (sqlmock.Sqlmock, *[]string, func()) {
	t.Helper()
	var calls []string
	orch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		w.WriteHeader(orchestratorStatus)
		_, _ = w.Write([]byte(`{"success":true,"warnings":["sink-w1"]}`))
	}))
	t.Setenv("ORCHESTRATOR_URL", orch.URL)

	mock, cleanup := wsScopeMockDB(t)
	mock.MatchExpectationsInOrder(false)
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(wsScopePipeline, wsScopeUser, wsScopeWS).
		WillReturnRows(gateRoleRows("member"))
	mock.ExpectQuery(`SELECT p\.status, .* FROM pipelines p WHERE p\.id = \$1 AND p\.workspace_id = \$2`).
		WithArgs(wsScopePipeline, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"status", "is_cdc"}).AddRow("running", false))
	mock.ExpectExec(`UPDATE pipelines SET status = 'stopped'`).
		WithArgs(wsScopePipeline, wsScopeWS).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`FROM pipeline_progress`).WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{"execution_id"}))
	mock.ExpectQuery(`FROM executions`).WithArgs(wsScopePipeline).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	return mock, &calls, func() { cleanup(); orch.Close() }
}

func TestStopPipeline_BatchStopsTheSinkWorker(t *testing.T) {
	mock, calls, cleanup := stopBatchHarness(t, http.StatusOK)
	defer cleanup()

	w := serveStop(t)
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", w.Code, w.Body.String())
	}
	want := "PUT /api/v1/pipelines/" + wsScopePipeline + "/sink/stop"
	if len(*calls) != 1 || (*calls)[0] != want {
		t.Fatalf("orchestrator calls = %v; want exactly [%s] — the batch sink worker keeps draining after Stop", *calls, want)
	}
	if !strings.Contains(w.Body.String(), "sink-w1") {
		t.Fatalf("sink warnings not passed on: %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

func TestStopPipeline_BatchStopSucceedsWhenTheSinkStopFails(t *testing.T) {
	mock, calls, cleanup := stopBatchHarness(t, http.StatusInternalServerError)
	defer cleanup()

	w := serveStop(t)
	if w.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s; a failed sink stop must not fail the user's Stop", w.Code, w.Body.String())
	}
	if len(*calls) != 1 {
		t.Fatalf("orchestrator calls = %v; want the sink stop to be attempted", *calls)
	}
	if !strings.Contains(w.Body.String(), "sink worker") {
		t.Fatalf("a failed sink stop is not reported as a warning: %s", w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
