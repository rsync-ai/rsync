package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
			mock.ExpectQuery(`SELECT status FROM pipelines WHERE id = \$1 AND workspace_id = \$2`).
				WithArgs(wsScopePipeline, wsScopeWS).
				WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow(status))
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
