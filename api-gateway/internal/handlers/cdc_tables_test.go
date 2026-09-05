package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"api-gateway/internal/db"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
)

func TestUpdatePipelineCDCTables_Success(t *testing.T) {
	gin.SetMode(gin.TestMode)

	pipelineID := "11111111-1111-1111-1111-111111111111"
	connectorName := "cdc-tenant-" + pipelineID
	userID := "00000000-0000-0000-0000-000000000000"

	// Fake Kafka Connect API
	connectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/connectors":
			_ = json.NewEncoder(w).Encode([]string{connectorName})
			return
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}))
	defer connectSrv.Close()

	os.Setenv("KAFKA_CONNECT_URL", connectSrv.URL)
	defer os.Unsetenv("KAFKA_CONNECT_URL")

	// Fake Orchestrator /cdc/tables endpoint (the authoritative side-effect now).
	var sawUpdate bool
	var updateBody map[string]any
	orchestratorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/cdc/tables":
			sawUpdate = true
			_ = json.NewDecoder(r.Body).Decode(&updateBody)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
			return
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/cdc/pipelines/"+pipelineID+"/sink/restart":
			// Best-effort call; doesn't affect success.
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
			return
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}))
	defer orchestratorSrv.Close()
	os.Setenv("ORCHESTRATOR_URL", orchestratorSrv.URL)
	defer os.Unsetenv("ORCHESTRATOR_URL")

	// Mock DB (best-effort UPDATE pipelines)
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer sqlDB.Close()
	db.DB = sqlDB
	// Authorization gate is now requireResourceRole (workspace-role), not the old
	// membership-only canAccessPipeline. It binds (resourceID, userID, activeWS)
	// and returns the caller's role; "owner" satisfies the >= member requirement.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(pipelineID, userID, "99999999-9999-9999-9999-999999999999").
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("owner"))
	mock.ExpectQuery("SELECT COALESCE\\(config->'selected_tables'").
		WithArgs(pipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"selected_tables"}).AddRow(`["db.users"]`))
	mock.ExpectExec("UPDATE pipelines").
		WithArgs(sqlmock.AnyArg(), pipelineID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Router
	r := gin.New()
	r.Use(func(c *gin.Context) {
		// Simulate auth + workspace-context middleware (gate reads the active ws).
		c.Set("user_id", userID)
		c.Set(ctxWorkspaceID, "99999999-9999-9999-9999-999999999999")
		c.Next()
	})
	r.POST("/api/v1/pipelines/:id/cdc/tables", UpdatePipelineCDCTables)

	body := []byte(`{"tables":["db.users","db.orders"]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+pipelineID+"/cdc/tables", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if !sawUpdate {
		t.Fatalf("expected orchestrator /cdc/tables to be called")
	}
	if updateBody["pipeline_id"] != pipelineID {
		t.Fatalf("expected pipeline_id %q, got %#v", pipelineID, updateBody["pipeline_id"])
	}
	if tables, ok := updateBody["tables"].([]any); !ok || len(tables) != 2 || tables[0] != "db.users" || tables[1] != "db.orders" {
		t.Fatalf("unexpected update tables payload: %#v", updateBody["tables"])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// TestUpdatePipelineCDCTables_NoConnectorPersistsSelection pins
// KI-CDC-EDIT-TABLES-NO-CONNECTOR. A CDC pipeline that never provisioned (blocked
// on day one by a table with no PRIMARY KEY) has no Debezium connector, so "Edit
// Tables → uncheck the offending table → Save" used to 404 `connector_not_found`
// *before* the UPDATE that persists the corrected list — the one corrective action
// available to that pipeline was discarded every time, leaving no self-service
// recovery path. The selection must now be saved and reported as pending.
func TestUpdatePipelineCDCTables_NoConnectorPersistsSelection(t *testing.T) {
	gin.SetMode(gin.TestMode)

	pipelineID := "33333333-3333-3333-3333-333333333333"
	userID := "00000000-0000-0000-0000-000000000000"

	// Kafka Connect is healthy and simply has no connector for this pipeline —
	// the exact state of a pipeline that never finished CDC provisioning.
	connectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/connectors" {
			_ = json.NewEncoder(w).Encode([]string{"cdc-some-other-pipeline"})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer connectSrv.Close()
	os.Setenv("KAFKA_CONNECT_URL", connectSrv.URL)
	defer os.Unsetenv("KAFKA_CONNECT_URL")

	// There is no connector to push to, so the orchestrator must not be called:
	// its own handler would 404 on the same missing connector.
	orchestratorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("orchestrator must not be called when no connector exists: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer orchestratorSrv.Close()
	os.Setenv("ORCHESTRATOR_URL", orchestratorSrv.URL)
	defer os.Unsetenv("ORCHESTRATOR_URL")

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer sqlDB.Close()
	db.DB = sqlDB
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(pipelineID, userID, "99999999-9999-9999-9999-999999999999").
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("owner"))
	mock.ExpectQuery("SELECT COALESCE\\(config->'selected_tables'").
		WithArgs(pipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"selected_tables"}).AddRow(`["db.users","db.nopk"]`))
	// The whole point of the fix: this UPDATE has to run.
	mock.ExpectExec("UPDATE pipelines").
		WithArgs(sqlmock.AnyArg(), pipelineID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", userID)
		c.Set(ctxWorkspaceID, "99999999-9999-9999-9999-999999999999")
		c.Next()
	})
	r.POST("/api/v1/pipelines/:id/cdc/tables", UpdatePipelineCDCTables)

	// The user drops the offending no-PK table.
	body := []byte(`{"tables":["db.users"]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+pipelineID+"/cdc/tables", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 (selection saved), got %d: %s", rr.Code, rr.Body.String())
	}
	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp["success"] != true {
		t.Fatalf("expected success=true, got %#v", resp["success"])
	}
	if resp["pending_provision"] != true {
		t.Fatalf("expected pending_provision=true so the UI can say the list is not live yet, got %#v", resp["pending_provision"])
	}
	if tables, ok := resp["tables"].([]any); !ok || len(tables) != 1 || tables[0] != "db.users" {
		t.Fatalf("response did not echo the saved selection: %#v", resp["tables"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the corrected table list was never persisted: %v", err)
	}
}

// TestUpdatePipelineCDCTables_ConnectUnreachableDoesNotPersist is the other half
// of the same fix. "No connector exists" and "Kafka Connect is down" must not
// collapse into one branch: when Connect cannot answer we do not know whether a
// live connector exists, and persisting a list we never pushed would leave the
// running connector silently out of sync with what the UI now shows as saved.
func TestUpdatePipelineCDCTables_ConnectUnreachableDoesNotPersist(t *testing.T) {
	gin.SetMode(gin.TestMode)

	pipelineID := "44444444-4444-4444-4444-444444444444"
	userID := "00000000-0000-0000-0000-000000000000"

	connectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"rebalancing"}`))
	}))
	defer connectSrv.Close()
	os.Setenv("KAFKA_CONNECT_URL", connectSrv.URL)
	defer os.Unsetenv("KAFKA_CONNECT_URL")

	orchestratorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("orchestrator must not be called when Kafka Connect is unreachable: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer orchestratorSrv.Close()
	os.Setenv("ORCHESTRATOR_URL", orchestratorSrv.URL)
	defer os.Unsetenv("ORCHESTRATOR_URL")

	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer sqlDB.Close()
	db.DB = sqlDB
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(pipelineID, userID, "99999999-9999-9999-9999-999999999999").
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("owner"))
	mock.ExpectQuery("SELECT COALESCE\\(config->'selected_tables'").
		WithArgs(pipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"selected_tables"}).AddRow(`["db.users"]`))
	// Deliberately NO ExpectExec: an UPDATE here would fail the test, because
	// sqlmock rejects calls it was not told to expect.

	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set("user_id", userID)
		c.Set(ctxWorkspaceID, "99999999-9999-9999-9999-999999999999")
		c.Next()
	})
	r.POST("/api/v1/pipelines/:id/cdc/tables", UpdatePipelineCDCTables)

	body := []byte(`{"tables":["db.users","db.orders"]}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+pipelineID+"/cdc/tables", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 when Kafka Connect cannot answer, got %d: %s", rr.Code, rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// TestFindDebeziumConnectorNameClassifiesFailures pins the distinction the two
// handler branches above depend on: only a clean "Connect answered, no match"
// may surface as errCDCConnectorNotProvisioned.
func TestFindDebeziumConnectorNameClassifiesFailures(t *testing.T) {
	pipelineID := "55555555-5555-5555-5555-555555555555"

	t.Run("connect answers with no match", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode([]string{"cdc-unrelated", "jdbc-sink"})
		}))
		defer srv.Close()
		os.Setenv("KAFKA_CONNECT_URL", srv.URL)
		defer os.Unsetenv("KAFKA_CONNECT_URL")

		_, err := findDebeziumConnectorName(pipelineID)
		if !errors.Is(err, errCDCConnectorNotProvisioned) {
			t.Fatalf("want errCDCConnectorNotProvisioned, got %v", err)
		}
	})

	t.Run("connect returns an error status", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer srv.Close()
		os.Setenv("KAFKA_CONNECT_URL", srv.URL)
		defer os.Unsetenv("KAFKA_CONNECT_URL")

		_, err := findDebeziumConnectorName(pipelineID)
		if err == nil {
			t.Fatal("expected an error")
		}
		if errors.Is(err, errCDCConnectorNotProvisioned) {
			t.Fatal("an unhealthy Kafka Connect must not be reported as 'no connector provisioned'")
		}
	})

	t.Run("connect is unreachable", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		srv.Close() // nothing is listening on this address any more
		os.Setenv("KAFKA_CONNECT_URL", srv.URL)
		defer os.Unsetenv("KAFKA_CONNECT_URL")

		_, err := findDebeziumConnectorName(pipelineID)
		if err == nil {
			t.Fatal("expected an error")
		}
		if errors.Is(err, errCDCConnectorNotProvisioned) {
			t.Fatal("an unreachable Kafka Connect must not be reported as 'no connector provisioned'")
		}
	})

	t.Run("connect lists the pipeline's connector", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode([]string{"cdc-tenant-" + pipelineID})
		}))
		defer srv.Close()
		os.Setenv("KAFKA_CONNECT_URL", srv.URL)
		defer os.Unsetenv("KAFKA_CONNECT_URL")

		name, err := findDebeziumConnectorName(pipelineID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if name != "cdc-tenant-"+pipelineID {
			t.Fatalf("got connector %q", name)
		}
	})
}

func TestUpdatePipelineCDCTables_BackfillNewlyAdded_TriggersOrchestrator(t *testing.T) {
	gin.SetMode(gin.TestMode)

	pipelineID := "22222222-2222-2222-2222-222222222222"
	connectorName := "cdc-tenant-" + pipelineID
	userID := "00000000-0000-0000-0000-000000000000"

	// Fake Kafka Connect API
	connectSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/connectors":
			_ = json.NewEncoder(w).Encode([]string{connectorName})
			return
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}))
	defer connectSrv.Close()
	os.Setenv("KAFKA_CONNECT_URL", connectSrv.URL)
	defer os.Unsetenv("KAFKA_CONNECT_URL")

	// Fake Orchestrator backfill endpoint
	var sawUpdate bool
	var sawBackfill bool
	var backfillBody map[string]any
	var updateBody map[string]any
	orchestratorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/api/v1/cdc/tables":
			sawUpdate = true
			_ = json.NewDecoder(r.Body).Decode(&updateBody)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
			return
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/cdc/pipelines/"+pipelineID+"/backfill":
			sawBackfill = true
			_ = json.NewDecoder(r.Body).Decode(&backfillBody)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
			return
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/cdc/pipelines/"+pipelineID+"/sink/restart":
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
			return
		default:
			w.WriteHeader(http.StatusNotFound)
			return
		}
	}))
	defer orchestratorSrv.Close()
	os.Setenv("ORCHESTRATOR_URL", orchestratorSrv.URL)
	defer os.Unsetenv("ORCHESTRATOR_URL")

	// Mock DB
	sqlDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer sqlDB.Close()
	db.DB = sqlDB
	// Authorization gate is now requireResourceRole (workspace-role), not the old
	// membership-only canAccessPipeline. It binds (resourceID, userID, activeWS)
	// and returns the caller's role; "owner" satisfies the >= member requirement.
	mock.ExpectQuery(`SELECT wm\.role\s+FROM pipelines r`).
		WithArgs(pipelineID, userID, "99999999-9999-9999-9999-999999999999").
		WillReturnRows(sqlmock.NewRows([]string{"role"}).AddRow("owner"))
	mock.ExpectQuery("SELECT COALESCE\\(config->'selected_tables'").
		WithArgs(pipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"selected_tables"}).AddRow(`["db.users"]`))
	mock.ExpectExec("UPDATE pipelines").
		WithArgs(sqlmock.AnyArg(), pipelineID).
		WillReturnResult(sqlmock.NewResult(0, 1))

	// Router
	r := gin.New()
	r.Use(func(c *gin.Context) {
		// Simulate auth + workspace-context middleware (gate reads the active ws).
		c.Set("user_id", userID)
		c.Set(ctxWorkspaceID, "99999999-9999-9999-9999-999999999999")
		c.Next()
	})
	r.POST("/api/v1/pipelines/:id/cdc/tables", UpdatePipelineCDCTables)

	body := []byte(`{"tables":["db.users","db.orders"],"backfill_newly_added":true}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/pipelines/"+pipelineID+"/cdc/tables", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if !sawUpdate {
		t.Fatalf("expected orchestrator /cdc/tables to be called")
	}
	if !sawBackfill {
		t.Fatalf("expected orchestrator backfill to be called")
	}
	// Newly added table should be db.orders only.
	if tables, ok := backfillBody["tables"].([]any); !ok || len(tables) != 1 || tables[0] != "db.orders" {
		t.Fatalf("unexpected backfill tables: %#v", backfillBody["tables"])
	}
	if mode, _ := backfillBody["mode"].(string); mode != "incremental" {
		t.Fatalf("expected mode incremental, got %#v", backfillBody["mode"])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}
