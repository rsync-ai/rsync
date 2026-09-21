package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// Verbatim shape of backend-orchestrator mcp.ConnectorDeployingMessage("MongoDB").
const deployingTestErr = "The MongoDB connector is still being set up for first use — this can take a minute or two. Please try again shortly."

// assertDeployingSaveResponse checks the retryable "still being set up" answer to a
// save whose pre-save test reached a connector that is not deployed yet: not the 422
// "credentials failed" refusal.
func assertDeployingSaveResponse(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 (retryable), got %d: %s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("Retry-After"); got == "" {
		t.Fatalf("expected a Retry-After header")
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if resp["error"] != "connector_deploying" || resp["status"] != "connector_deploying" {
		t.Fatalf("expected error/status=connector_deploying, got error=%v status=%v", resp["error"], resp["status"])
	}
	if resp["retryable"] != true {
		t.Fatalf("expected retryable=true, got %v", resp["retryable"])
	}
	if te, _ := resp["test_error"].(string); !strings.Contains(te, connectorDeployingMarker) {
		t.Fatalf("test_error must carry the deploying message, got %q", te)
	}
}

func TestCreateConnection_DeployingMarkerIsRetryable(t *testing.T) {
	editSecretsEnv(t)
	calls, stop := fakeTestConnectionOrchestrator(t, false, deployingTestErr)
	defer stop()

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	// Declared so it can be observed, and required to stay unfulfilled: nothing is
	// saved while the connector is still deploying.
	mock.ExpectExec(`INSERT INTO connections`).WillReturnResult(sqlmock.NewResult(0, 1))

	body, _ := json.Marshal(map[string]any{
		"name":            "mongo-src",
		"connection_type": "source",
		"connector_type":  "mongodb",
		"config":          map[string]any{"connection_string": "mongodb+srv://reader:s3cret@cluster0.example.net/shop"},
	})
	r := wsScopeRouter(http.MethodPost, "/api/v1/connections", CreateConnection)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/connections", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)

	assertDeployingSaveResponse(t, w)
	if n := atomic.LoadInt32(calls); n != 1 {
		t.Fatalf("expected exactly one connectivity test, got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err == nil || !strings.Contains(err.Error(), "INSERT INTO connections") {
		t.Fatalf("the INSERT must stay the unfulfilled expectation (nothing saved while deploying), got: %v", err)
	}
}

func TestUpdateConnection_DeployingMarkerIsRetryable(t *testing.T) {
	editSecretsEnv(t)
	calls, stop := fakeTestConnectionOrchestrator(t, false, deployingTestErr)
	defer stop()

	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.MatchExpectationsInOrder(false)
	expectEditPreamble(t, mock)
	// Required to stay unfulfilled: the edit is not saved and the stored verdict is
	// not rewritten while the connector is still deploying.
	mock.ExpectExec(`UPDATE connections SET`).WillReturnResult(sqlmock.NewResult(0, 1))

	w := putConnection(t, map[string]interface{}{
		"config": map[string]interface{}{
			"connection_string": "mongodb+srv://reader:s3cret@cluster1.example.net/shop",
			"database":          "shop",
		},
	})

	assertDeployingSaveResponse(t, w)
	if n := atomic.LoadInt32(calls); n != 1 {
		t.Fatalf("expected exactly one connectivity test, got %d", n)
	}
	if err := mock.ExpectationsWereMet(); err == nil || !strings.Contains(err.Error(), "UPDATE connections SET") {
		t.Fatalf("the UPDATE must stay the unfulfilled expectation (nothing saved while deploying), got: %v", err)
	}
}
