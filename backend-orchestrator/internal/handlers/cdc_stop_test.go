package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

const stopTestPipelineID = "637a0df2-1111-4222-8333-944455556666"

type recordingSinks struct {
	mu     sync.Mutex
	groups []string
}

func (r *recordingSinks) ExecuteWithContext(_ context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
	cfg, _ := req.Params["config"].(map[string]interface{})
	g, _ := cfg["consumer_group"].(string)
	r.mu.Lock()
	r.groups = append(r.groups, g)
	r.mu.Unlock()
	return &mcp.ExecuteResponse{Success: true}, nil
}

// fakeConnect answers each "METHOD path" with a status; anything else is a 404.
// It records the calls in order.
func fakeConnect(t *testing.T, answers map[string]int) *[]string {
	t.Helper()
	var mu sync.Mutex
	calls := &[]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Method + " " + r.URL.Path
		mu.Lock()
		*calls = append(*calls, key)
		mu.Unlock()
		code, ok := answers[key]
		if !ok {
			code = http.StatusNotFound
		}
		w.WriteHeader(code)
		if r.Method == http.MethodGet && code == http.StatusOK {
			_, _ = w.Write([]byte(`{"name":"cdc-637a0df2","connector":{"state":"RUNNING"},"tasks":[{"id":0,"state":"RUNNING"}]}`))
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("KAFKA_CONNECT_URL", srv.URL)
	return calls
}

func runStop(t *testing.T, answers map[string]int, expectStatusFlip bool) (int, map[string]interface{}, *[]string, *recordingSinks) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	calls := fakeConnect(t, answers)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("FROM cdc_resources").WithArgs(stopTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("cdc-637a0df2"))
	if expectStatusFlip {
		mock.ExpectQuery("SELECT COUNT").WithArgs(stopTestPipelineID, "637a0df2").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectExec("UPDATE pipelines SET status = 'stopped'").WithArgs(stopTestPipelineID).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}

	sinks := &recordingSinks{}
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set("auth_internal", true) })
	r.PUT("/cdc/pipelines/:pipeline_id/stop", stopCDCPipeline(db, sinks, nil))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/cdc/pipelines/"+stopTestPipelineID+"/stop", nil))
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	// sqlmock refuses any statement not expected above, so a failed stop that
	// still flipped the status would fail here.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db: %v (body %v)", err, body)
	}
	return w.Code, body, calls, sinks
}

func TestStopCDCPipelineParksConnectorAndStopsSink(t *testing.T) {
	code, body, calls, sinks := runStop(t, map[string]int{"PUT /connectors/cdc-637a0df2/stop": http.StatusNoContent}, true)
	if code != http.StatusOK || body["success"] != true {
		t.Fatalf("code=%d body=%v", code, body)
	}
	if len(*calls) != 1 || (*calls)[0] != "PUT /connectors/cdc-637a0df2/stop" {
		t.Fatalf("connect calls = %v; want only the stop (no DELETE: offsets must survive)", *calls)
	}
	if len(sinks.groups) == 0 {
		t.Fatal("no sink worker stopped")
	}
	for _, g := range sinks.groups {
		if !testNamesFor(stopTestPipelineID).ownsSinkGroup(g) {
			t.Errorf("stopped a group this pipeline does not own: %s", g)
		}
	}
}

func TestStopCDCPipelineFallsBackToPauseOnOldConnect(t *testing.T) {
	code, body, calls, _ := runStop(t, map[string]int{
		"GET /connectors/cdc-637a0df2/status": http.StatusOK,
		"PUT /connectors/cdc-637a0df2/pause":  http.StatusAccepted,
	}, true)
	if code != http.StatusOK {
		t.Fatalf("code=%d body=%v", code, body)
	}
	if res, _ := body["result"].(map[string]interface{}); res["connector"] != "paused" {
		t.Fatalf("result = %v; want paused (calls %v)", body["result"], *calls)
	}
}

func TestStopCDCPipelineWithNoConnectorStillStops(t *testing.T) {
	code, body, _, _ := runStop(t, map[string]int{}, true)
	if code != http.StatusOK {
		t.Fatalf("code=%d body=%v", code, body)
	}
	if res, _ := body["result"].(map[string]interface{}); res["connector"] != "absent" {
		t.Fatalf("result = %v; want absent", body["result"])
	}
}

func TestStopCDCPipelineRefusedKeepsStatus(t *testing.T) {
	code, body, _, sinks := runStop(t, map[string]int{"PUT /connectors/cdc-637a0df2/stop": http.StatusInternalServerError}, false)
	if code != http.StatusBadGateway || body["success"] != false {
		t.Fatalf("code=%d body=%v; want 502", code, body)
	}
	if len(sinks.groups) != 0 {
		t.Fatalf("sink stopped although the connector still streams: %v", sinks.groups)
	}
}

func TestStopCDCPipelineConnectUnreachable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("KAFKA_CONNECT_URL", "http://127.0.0.1:1")
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectQuery("FROM cdc_resources").WithArgs(stopTestPipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"resource_name"}).AddRow("cdc-637a0df2"))
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set("auth_internal", true) })
	r.PUT("/cdc/pipelines/:pipeline_id/stop", stopCDCPipeline(db, &recordingSinks{}, nil))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/cdc/pipelines/"+stopTestPipelineID+"/stop", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("code=%d body=%s; want 503", w.Code, w.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func testNamesFor(pipelineID string) pipelineKafkaNames {
	return pipelineKafkaNames{id8: pipelineID[:8], uuid: pipelineID}
}
