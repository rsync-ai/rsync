package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/shared/crypto"
)

const docFindConnID = "33333333-3333-3333-3333-333333333333"

// docFindConnection sets up the encrypted config and expects the workspace-scoped
// connection load with the given connector type.
func docFindConnection(t *testing.T, mock sqlmock.Sqlmock, connectorType string) {
	t.Helper()
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
	enc, err := crypto.EncryptString(`{"connection_string":"mongodb://h/shop","database":"shop"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	mock.ExpectQuery(`SELECT connector_type, config\s+FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(docFindConnID, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type", "config"}).AddRow(connectorType, enc))
}

// docFindOrchestrator records the request body and answers with (status, body).
func docFindOrchestrator(t *testing.T, wantPath string, status int, body string) (*atomic.Int32, *atomic.Value) {
	t.Helper()
	calls := &atomic.Int32{}
	got := &atomic.Value{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != wantPath {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		calls.Add(1)
		b, _ := io.ReadAll(r.Body)
		got.Store(string(b))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	t.Setenv("ORCHESTRATOR_URL", srv.URL)
	return calls, got
}

func serveDocFind(t *testing.T, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := wsScopeRouter(http.MethodPost, "/api/v1/explorer/documents/find", FindExplorerDocuments)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/explorer/documents/find", strings.NewReader(body)))
	return rr
}

func TestFindExplorerDocuments_ForwardsSpecAndRedacts(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	docFindConnection(t, mock, "mongodb")
	calls, sent := docFindOrchestrator(t, "/api/v1/agent/explorer-find", http.StatusOK, `{
		"success": true, "collection": "customers", "returned": 1, "has_more": true,
		"paging_mode": "skip", "next_skip": 1, "columns": ["_id","email","profile","items","big"],
		"documents": [{"_id":{"$oid":"65f000000000000000000001"},"email":"alice@example.com",
			"profile":{"name":"Al","ssn":"123-45-6789"},"items":[{"password":"hunter22","qty":2}],
			"big":9007199254740993}]}`)

	rr := serveDocFind(t, `{"connection_id":"`+docFindConnID+`","collection":"customers",
		"filter":{"n":{"$gte":9007199254740993}},"sort":{"tier":1,"n":-1}}`)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("orchestrator calls = %d, want 1", calls.Load())
	}
	req := sent.Load().(string)
	for _, want := range []string{`"connector_type":"mongodb"`, `"collection":"customers"`, `"limit":50`,
		`"sort":[["tier",1],["n",-1]]`, `9007199254740993`, `"connection_string"`} {
		if !strings.Contains(req, want) {
			t.Errorf("orchestrator request missing %s: %s", want, req)
		}
	}

	out := rr.Body.String()
	for _, leaked := range []string{"alice@example.com", "123-45-6789", "hunter22"} {
		if strings.Contains(out, leaked) {
			t.Errorf("response leaks %q: %s", leaked, out)
		}
	}
	for _, kept := range []string{`"name":"Al"`, `"qty":2`, `9007199254740993`, `"$oid":"65f000000000000000000001"`, `"next_skip":1`} {
		if !strings.Contains(out, kept) {
			t.Errorf("response lost %s: %s", kept, out)
		}
	}
	if strings.Contains(out, `"success"`) {
		t.Errorf("internal success flag forwarded: %s", out)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}
}

// A refused filter never reaches the database or the orchestrator.
func TestFindExplorerDocuments_RejectsBeforeLoading(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	calls, _ := docFindOrchestrator(t, "/api/v1/agent/explorer-find", http.StatusOK, `{}`)

	rr := serveDocFind(t, `{"connection_id":"`+docFindConnID+`","collection":"c","filter":{"$or":[{"$where":"sleep(1)"}]}}`)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rr.Code, rr.Body.String())
	}
	var body map[string]string
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if body["error_code"] != "operator_not_allowed" || body["path"] != "filter.$or[0].$where" {
		t.Errorf("got %+v", body)
	}
	if calls.Load() != 0 {
		t.Errorf("orchestrator called %d times", calls.Load())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("db expectations: %v", err)
	}

	if rr := serveDocFind(t, `{"collection":"c"}`); rr.Code != http.StatusBadRequest {
		t.Errorf("missing connection_id: want 400, got %d", rr.Code)
	}
}

func TestFindExplorerDocuments_ConnectionGates(t *testing.T) {
	t.Run("not found in workspace", func(t *testing.T) {
		mock, cleanup := wsScopeMockDB(t)
		defer cleanup()
		mock.ExpectQuery(`SELECT connector_type, config\s+FROM connections`).
			WithArgs(docFindConnID, wsScopeWS).
			WillReturnRows(sqlmock.NewRows([]string{"connector_type", "config"}))
		rr := serveDocFind(t, `{"connection_id":"`+docFindConnID+`","collection":"c"}`)
		if rr.Code != http.StatusNotFound {
			t.Fatalf("want 404, got %d", rr.Code)
		}
	})
	t.Run("sql connection", func(t *testing.T) {
		mock, cleanup := wsScopeMockDB(t)
		defer cleanup()
		docFindConnection(t, mock, "postgresql")
		calls, _ := docFindOrchestrator(t, "/api/v1/agent/explorer-find", http.StatusOK, `{}`)
		rr := serveDocFind(t, `{"connection_id":"`+docFindConnID+`","collection":"c"}`)
		if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "not_document_connection") {
			t.Fatalf("want 400 not_document_connection, got %d: %s", rr.Code, rr.Body.String())
		}
		if calls.Load() != 0 {
			t.Errorf("orchestrator called for a SQL connection")
		}
	})
}

func TestFindExplorerDocuments_UpstreamStatusMapping(t *testing.T) {
	cases := []struct {
		name       string
		status     int
		body       string
		wantStatus int
		wantCode   string
	}{
		{"refusal keeps 400 and path", http.StatusBadRequest,
			`{"error":"explorer_find failed","error_code":"invalid_cursor","path":"cursor","details":"cursor is not valid"}`,
			http.StatusBadRequest, "invalid_cursor"},
		{"timeout keeps 504", http.StatusGatewayTimeout,
			`{"error":"explorer_find failed","error_code":"query_timeout","details":"operation exceeded time limit"}`,
			http.StatusGatewayTimeout, "query_timeout"},
		{"unreachable is 502", http.StatusBadGateway,
			`{"error":"explorer_find failed","error_code":"connection_failed","details":"no reachable servers"}`,
			http.StatusBadGateway, "connection_failed"},
		{"other 5xx becomes 502", http.StatusInternalServerError, ``, http.StatusBadGateway, "query_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			docFindConnection(t, mock, "mongodb")
			docFindOrchestrator(t, "/api/v1/agent/explorer-find", tc.status, tc.body)
			rr := serveDocFind(t, `{"connection_id":"`+docFindConnID+`","collection":"c"}`)
			if rr.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rr.Code, tc.wantStatus, rr.Body.String())
			}
			var body map[string]string
			_ = json.Unmarshal(rr.Body.Bytes(), &body)
			if body["error_code"] != tc.wantCode || body["error"] == "" {
				t.Errorf("body = %+v, want error_code %s and a message", body, tc.wantCode)
			}
		})
	}
}

// A document connection must never reach the SQL executors (D8).
func TestExecuteExplorerQuery_RejectsDocumentConnection(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	docFindConnection(t, mock, "mongodb")
	calls, _ := docFindOrchestrator(t, "/api/v1/agent/explorer-query", http.StatusOK, `{"rows":[]}`)

	r := wsScopeRouter(http.MethodPost, "/api/v1/explorer/query", ExecuteExplorerQuery)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/explorer/query",
		strings.NewReader(`{"connection_id":"`+docFindConnID+`","sql":"SELECT 1"}`)))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d: %s", rr.Code, rr.Body.String())
	}
	if calls.Load() != 0 {
		t.Errorf("a document connection reached the delegated SQL executor")
	}
}

func TestBuildDocumentSchemaIndex_MapsCollections(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
	enc, err := crypto.EncryptString(`{"connection_string":"mongodb://h/shop"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	_, sent := docFindOrchestrator(t, "/api/v1/agent/discover-schema", http.StatusOK, `{"tables":[
		{"name":"orders","schema":"shop","primary_keys":["_id"],
		 "columns":[{"name":"_id","type":"objectId","nullable":false},{"name":"total","type":"double","nullable":true}]},
		{"name":"customers","schema":"shop","columns":[]}]}`)

	idx, err := buildSchemaIndex(t.Context(), docFindConnID, "mongodb", enc, false)
	if err != nil {
		t.Fatalf("buildSchemaIndex: %v", err)
	}
	if idx.TableCount != 2 || idx.Tables[0].Name != "orders" || idx.Tables[0].Schema != "shop" {
		t.Fatalf("unexpected index: %+v", idx)
	}
	cols := idx.Tables[0].Columns
	if len(cols) != 2 || !cols[0].IsPrimaryKey || cols[1].IsPrimaryKey || !cols[1].IsNullable {
		t.Errorf("columns: %+v", cols)
	}
	if idx.SchemaHash == "" || idx.SchemaHash == "unsupported" {
		t.Errorf("schema hash: %q", idx.SchemaHash)
	}
	req := sent.Load().(string)
	if !strings.Contains(req, `"include_row_counts":false`) || !strings.Contains(req, `"connector_type":"mongodb"`) {
		t.Errorf("discover request: %s", req)
	}
}
