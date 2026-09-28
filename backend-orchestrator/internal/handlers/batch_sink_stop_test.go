package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func runBatchSinkStop(t *testing.T, othersWithSameID8 int) (int, map[string]interface{}, *recordingSinks) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT COUNT").WithArgs(stopTestPipelineID, "637a0df2").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(othersWithSameID8))

	sinks := &recordingSinks{}
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Set("auth_internal", true) })
	r.PUT("/pipelines/:id/sink/stop", stopBatchSinkWorkers(db, sinks, nil))

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/pipelines/"+stopTestPipelineID+"/sink/stop", nil))
	var body map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db: %v (body %v)", err, body)
	}
	return w.Code, body, sinks
}

// A batch Stop reached Temporal and the pipelines row but never kafka-mcp-sink,
// so the "sink-<id8>-batch" worker kept writing the run's backlog into the
// destination for minutes. The stop must reach every spelling of that group —
// and only that group: a batch Stop must not stop a CDC or streaming worker.
func TestBatchStopStopsOnlyTheBatchSinkWorker(t *testing.T) {
	code, body, sinks := runBatchSinkStop(t, 0)
	if code != http.StatusOK || body["success"] != true {
		t.Fatalf("code=%d body=%v", code, body)
	}
	if len(sinks.groups) == 0 {
		t.Fatal("batch stop sent no stop_sink: the batch sink worker keeps draining after Stop")
	}
	var bare, qualified bool
	for _, g := range sinks.groups {
		if !strings.HasSuffix(g, "-batch") {
			t.Errorf("batch stop stopped non-batch group %s", g)
		}
		if !testNamesFor(stopTestPipelineID).ownsSinkGroup(g) {
			t.Errorf("batch stop stopped a group this pipeline does not own: %s", g)
		}
		if g == "sink-637a0df2-batch" {
			bare = true
		} else if strings.HasSuffix(g, "sink-637a0df2-batch") {
			qualified = true
		}
	}
	if !bare || !qualified {
		t.Errorf("stopped %v; want both the bare and the namespace-qualified batch group", sinks.groups)
	}
}

// main.go mounts this route on /api/v1 beside POST /pipelines/:id/run and the
// assessment routes. gin panics at startup when two routes name one path
// segment differently, so the route must use ":id" like its neighbours.
func TestBatchSinkStopRouteCoexistsWithThePipelineRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("registering the batch sink-stop route panicked: %v", r)
		}
	}()
	api := gin.New().Group("/api/v1")
	api.POST("/pipelines/:id/run", func(*gin.Context) {})
	NewAssessmentHandler(nil, nil, nil).RegisterRoutes(api)
	api.PUT("/pipelines/:id/sink/stop", StopBatchSinkWorkers(nil, nil, nil))
}

func TestBatchStopSkipsWorkersWhenTheID8IsShared(t *testing.T) {
	code, body, sinks := runBatchSinkStop(t, 1)
	if code != http.StatusOK {
		t.Fatalf("code=%d body=%v", code, body)
	}
	if len(sinks.groups) != 0 {
		t.Fatalf("stopped %v although another pipeline shares the id8 — those could be its workers", sinks.groups)
	}
	if w, _ := body["warnings"].([]interface{}); len(w) != 1 {
		t.Fatalf("warnings = %v; want the shared-id8 warning", body["warnings"])
	}
}
