package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"api-gateway/internal/cache"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/rsync-ai/shared/crypto"
)

const docFindConnID = "33333333-3333-3333-3333-333333333333"

// reasonCapRunes is how much of a connector's reason the gateway promises to keep.
// It is spelled out here rather than read from the handler so a changed cap fails.
const reasonCapRunes = 600

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

	rr := serveDocFind(t, `{"connection_id":"`+docFindConnID+`","database":"crm","collection":"customers",
		"filter":{"n":{"$gte":9007199254740993}},"sort":{"tier":1,"n":-1}}`)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("orchestrator calls = %d, want 1", calls.Load())
	}
	req := sent.Load().(string)
	for _, want := range []string{`"connector_type":"mongodb"`, `"database":"crm"`, `"collection":"customers"`, `"limit":50`,
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

// The MongoDB connector does not use success:false when it cannot list collections.
// It answers 200 with overall_status "failed", no tables and the reason in
// warnings_messages, and the orchestrator passes that through as a success. Only
// "failed" is an error: "partial", "success" and a missing status still build an
// index, and a reachable database with no collections is still an empty index.
func TestBuildDocumentSchemaIndex_OverallStatus(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
	enc, err := crypto.EncryptString(`{"connection_string":"mongodb://h/shop"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	cases := []struct {
		name       string
		body       string
		wantErr    []string // substrings of the error; nil means no error
		notInErr   []string
		wantTables int
	}{
		{
			name: "failed is an error carrying the connector's reason",
			body: `{"overall_status":"failed","tables":[],"warnings_messages":[
				"bad auth : authentication failed, full error: {'ok': 0.0, 'errmsg': 'bad auth : authentication failed', 'code': 8000}"]}`,
			wantErr:  []string{"could not list collections", "bad auth : authentication failed"},
			notInErr: []string{"full error", "8000"},
		},
		{
			name:    "failed with every reason joined",
			body:    `{"overall_status":"failed","tables":[],"warnings_messages":["first reason","second reason"]}`,
			wantErr: []string{"first reason; second reason"},
		},
		{
			name: "failed on an unreachable host keeps the timeout and drops the topology dump",
			body: `{"overall_status":"failed","tables":[],"warnings_messages":[
				"cluster0-shard-00-00.example.net:27017: timed out, Timeout: 30s, Topology Description: <TopologyDescription id: 65f0, topology_type: ReplicaSetNoPrimary, servers: [<ServerDescription ('cluster0-shard-00-00.example.net', 27017) server_type: Unknown, rtt: None, error=NetworkTimeout('timed out')>]>"]}`,
			wantErr:  []string{"cluster0-shard-00-00.example.net:27017: timed out"},
			notInErr: []string{"Topology", "ServerDescription"},
		},
		{
			name:    "the connector's missing-database message stays readable",
			body:    `{"overall_status":"failed","tables":[],"warnings_messages":["Missing 'database' in config"]}`,
			wantErr: []string{"Missing database in config"},
		},
		{
			name:    "failed with the reason under error instead of warnings",
			body:    `{"overall_status":"failed","tables":[],"error":"not authorized on shop to execute command"}`,
			wantErr: []string{"not authorized on shop to execute command"},
		},
		{
			name:    "failed without a reason still says what failed",
			body:    `{"overall_status":"failed","tables":[],"warnings_messages":[]}`,
			wantErr: []string{"could not list collections", "no reason"},
		},
		{
			// The connector can fail after it already listed some collections; the
			// status decides, not the table list.
			name: "failed is an error even when some collections came back",
			body: `{"overall_status":"failed","warnings_messages":["connection lost while sampling"],
				"tables":[{"name":"orders","schema":"shop","columns":[]}]}`,
			wantErr: []string{"could not list collections: connection lost while sampling"},
		},
		{
			name: "driver noise is cut from every reason, not only the first",
			body: `{"overall_status":"failed","tables":[],"warnings_messages":[
				"orders: sample failed: not authorized",
				"bad auth : authentication failed, full error: {'ok': 0.0, 'errmsg': 'bad auth', 'code': 8000}",
				"cluster0.example.net:27017: timed out, Topology Description: <TopologyDescription id: 65f0, servers: [<ServerDescription>]>"]}`,
			wantErr:  []string{"orders: sample failed: not authorized; bad auth : authentication failed; cluster0.example.net:27017: timed out"},
			notInErr: []string{"full error", "8000", "Topology", "ServerDescription"},
		},
		{
			name:     "the warnings win over the error key",
			body:     `{"overall_status":"failed","tables":[],"warnings_messages":["from the warnings"],"error":"from the error key"}`,
			wantErr:  []string{"could not list collections: from the warnings"},
			notInErr: []string{"from the error key"},
		},
		{
			name:     "blank warnings are no reason",
			body:     `{"overall_status":"failed","tables":[],"warnings_messages":["", "  "]}`,
			wantErr:  []string{"could not list collections: the connector gave no reason"},
			notInErr: []string{";"},
		},
		{
			name:     "blank warnings fall back to the error key",
			body:     `{"overall_status":"failed","tables":[],"warnings_messages":[" "],"error":"not authorized on shop"}`,
			wantErr:  []string{"could not list collections: not authorized on shop"},
			notInErr: []string{";", "no reason"},
		},
		{
			name:     "a warning that is only driver noise is left out",
			body:     `{"overall_status":"failed","tables":[],"warnings_messages":[", full error: {'ok': 0.0}", "bad auth : authentication failed"]}`,
			wantErr:  []string{"could not list collections: bad auth : authentication failed"},
			notInErr: []string{";", "full error"},
		},
		{
			name:     "non-string warnings are skipped",
			body:     `{"overall_status":"failed","tables":[],"warnings_messages":[42, null, {"code": 8000}, "bad auth : authentication failed"]}`,
			wantErr:  []string{"could not list collections: bad auth : authentication failed"},
			notInErr: []string{";", "8000"},
		},
		{
			name:       "success with collections is an index",
			body:       `{"overall_status":"success","warnings_messages":[],"tables":[{"name":"orders","schema":"shop","columns":[]}]}`,
			wantTables: 1,
		},
		{
			name: "partial is not an error",
			body: `{"overall_status":"partial","warnings_messages":["orders: sample failed: not authorized"],
				"tables":[{"name":"orders","schema":"shop","columns":[]},{"name":"customers","schema":"shop","columns":[]}]}`,
			wantTables: 2,
		},
		{
			name:       "missing status is not an error",
			body:       `{"tables":[{"name":"orders","schema":"shop","columns":[]}]}`,
			wantTables: 1,
		},
		{
			name:       "a reachable database with no collections is an empty index",
			body:       `{"overall_status":"success","warnings_messages":[],"tables":[]}`,
			wantTables: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls, _ := docFindOrchestrator(t, "/api/v1/agent/discover-schema", http.StatusOK, tc.body)
			idx, err := buildSchemaIndex(t.Context(), docFindConnID, "mongodb", enc, false)
			if calls.Load() != 1 {
				t.Fatalf("orchestrator calls = %d, want 1", calls.Load())
			}
			if tc.wantErr != nil {
				if err == nil {
					t.Fatalf("want an error, got index %+v", idx)
				}
				for _, want := range tc.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("error %q does not contain %q", err.Error(), want)
					}
				}
				for _, bad := range tc.notInErr {
					if strings.Contains(err.Error(), bad) {
						t.Errorf("error %q still contains %q", err.Error(), bad)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("want an index, got error: %v", err)
			}
			if idx.TableCount != tc.wantTables || len(idx.Tables) != tc.wantTables {
				t.Fatalf("TableCount=%d len(Tables)=%d, want %d", idx.TableCount, len(idx.Tables), tc.wantTables)
			}
		})
	}
}

// A non-200 from the orchestrator is already an error; its body is connector text too
// and must not carry a credentialed URI back out.
func TestBuildDocumentSchemaIndex_UpstreamErrorBodyIsScrubbed(t *testing.T) {
	const credURL = "connector call failed for mongodb://svc_user:Hunter2Secret@db.internal:27017/shop"
	// ASCII filler that puts the password across byte 512 of the body.
	asciiPad := `{"error":"schema discovery failed","details":"` + strings.Repeat("no answer ", 42)
	// Multi-byte filler: under 600 runes, so the whole reason is kept, but well over
	// 1024 bytes, so a byte-sized read limit near the kept length would cut the URL.
	widePad := `{"error":"schema discovery failed","details":"` + strings.Repeat("応答がありません ", 50) + "cause: "

	cases := []struct {
		name     string
		status   int
		body     string
		wantErr  []string
		notInErr []string
	}{
		{
			// The orchestrator's discover-schema handler answers 503 {"error","details"}.
			name:    "short JSON body",
			status:  http.StatusServiceUnavailable,
			body:    `{"error":"schema discovery failed","details":"` + credURL + `"}`,
			wantErr: []string{"status 503", "schema discovery failed: connector call failed for mongodb://[redacted]@db.internal:27017/shop"},
		},
		{
			name:    "credential past byte 512 of the body",
			status:  http.StatusServiceUnavailable,
			body:    asciiPad + credURL + `"}`,
			wantErr: []string{"status 503", "schema discovery failed", "mongodb://[redacted]@db.internal:27017/shop"},
		},
		{
			name:    "credential past byte 1024 of a multi-byte body",
			status:  http.StatusServiceUnavailable,
			body:    widePad + credURL + `"}`,
			wantErr: []string{"status 503", "応答がありません", "mongodb://[redacted]@db.internal:27017/shop"},
		},
		{
			// A proxy in front of the orchestrator answers in plain text.
			name:     "plain-text body is kept",
			status:   http.StatusBadGateway,
			body:     "upstream connect error or disconnect/reset before headers",
			wantErr:  []string{"status 502: upstream connect error or disconnect/reset before headers"},
			notInErr: []string{"no reason"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Check the fixtures really sit where their names say.
			switch tc.name {
			case "credential past byte 512 of the body":
				at := strings.Index(tc.body, "Hunter2")
				if at >= 512 || at+len("Hunter2Secret@") <= 512 {
					t.Fatalf("fixture: password spans bytes %d..%d, want it across byte 512", at, at+len("Hunter2Secret@"))
				}
			case "credential past byte 1024 of a multi-byte body":
				if at := strings.Index(tc.body, "svc_user"); at <= 1024 {
					t.Fatalf("fixture: credential starts at byte %d, want past 1024", at)
				}
				if n := utf8.RuneCountInString(tc.body); n >= reasonCapRunes {
					t.Fatalf("fixture: body is %d runes, want under %d so nothing is truncated", n, reasonCapRunes)
				}
			}

			t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
			enc, err := crypto.EncryptString(`{"connection_string":"mongodb://h/shop"}`)
			if err != nil {
				t.Fatalf("encrypt: %v", err)
			}
			calls, _ := docFindOrchestrator(t, "/api/v1/agent/discover-schema", tc.status, tc.body)

			_, err = buildSchemaIndex(t.Context(), docFindConnID, "mongodb", enc, false)
			if n := calls.Load(); n != 1 {
				t.Fatalf("orchestrator calls = %d, want 1", n)
			}
			if err == nil {
				t.Fatalf("want an error for a %d from the orchestrator", tc.status)
			}
			for _, want := range tc.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err.Error(), want)
				}
			}
			for _, bad := range append([]string{"Hunter2", "svc_user:"}, tc.notInErr...) {
				if strings.Contains(err.Error(), bad) {
					t.Errorf("error %q contains %q", err.Error(), bad)
				}
			}
		})
	}
}

// A failed listing's reason is capped, and the cap is applied after scrubbing, so a
// credential that crosses the cap is masked rather than cut open.
func TestBuildDocumentSchemaIndex_LongReasonIsBoundedAndScrubbed(t *testing.T) {
	const credURL = "mongodb://svc_user:Hunter2Secret@db.internal:27017/shop"
	longWarning := strings.Repeat("no answer ", 1000)
	crossingPad := strings.Repeat("no answer ", 57) + "cause: "
	if at := utf8.RuneCountInString(crossingPad); at >= reasonCapRunes || at+len("mongodb://svc_user:Hunter2") <= reasonCapRunes {
		t.Fatalf("fixture: credential starts at rune %d, want it across rune %d", at, reasonCapRunes)
	}

	cases := []struct {
		name    string
		warning string
		wantIn  []string
	}{
		{name: "a long reason is cut to the cap", warning: longWarning},
		{name: "a credential across the cap is masked", warning: crossingPad + credURL, wantIn: []string{"mongodb://[redacted]@"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
			enc, err := crypto.EncryptString(`{"connection_string":"mongodb://h/shop"}`)
			if err != nil {
				t.Fatalf("encrypt: %v", err)
			}
			w, _ := json.Marshal([]string{tc.warning})
			calls, _ := docFindOrchestrator(t, "/api/v1/agent/discover-schema", http.StatusOK,
				`{"overall_status":"failed","tables":[],"warnings_messages":`+string(w)+`}`)

			_, err = buildSchemaIndex(t.Context(), docFindConnID, "mongodb", enc, false)
			if n := calls.Load(); n != 1 {
				t.Fatalf("orchestrator calls = %d, want 1", n)
			}
			if err == nil {
				t.Fatal("want an error for overall_status failed")
			}
			const prefix = "could not list collections: "
			msg := err.Error()
			at := strings.Index(msg, prefix)
			if at < 0 {
				t.Fatalf("error %q does not contain %q", msg, prefix)
			}
			reason := msg[at+len(prefix):]
			// The cap plus the ellipsis that marks the cut.
			if n := utf8.RuneCountInString(reason); n != reasonCapRunes+1 {
				t.Errorf("reason is %d runes, want %d: %q", n, reasonCapRunes+1, reason)
			}
			if !strings.HasSuffix(reason, "…") {
				t.Errorf("a cut reason should end in an ellipsis: %q", reason)
			}
			for _, want := range tc.wantIn {
				if !strings.Contains(reason, want) {
					t.Errorf("reason %q does not contain %q", reason, want)
				}
			}
			for _, bad := range []string{"Hunter2", "svc_user:"} {
				if strings.Contains(msg, bad) {
					t.Errorf("error %q contains %q", msg, bad)
				}
			}
		})
	}
}

// The path the Data Explorer takes: GET schema-index for a MongoDB connection whose
// cluster refuses the login must answer with an error, not a 200 with no tables, and
// the connector's text must not carry the credentials back to the browser.
func TestGetSchemaIndex_DocumentDiscoveryFailureIsAnError(t *testing.T) {
	prevCache := explorerCache
	explorerCache = nil
	t.Cleanup(func() { explorerCache = prevCache })

	route := "/api/v1/explorer/connections/:id/schema-index"
	url := "/api/v1/explorer/connections/" + docFindConnID + "/schema-index"

	// The page's first load, its Retry (refresh=true) and the refresh endpoint all
	// build the index the same way; none may turn a failed listing into a 200.
	requests := []struct {
		name    string
		method  string
		route   string
		url     string
		handler func(*gin.Context)
	}{
		{"load", http.MethodGet, route, url, GetSchemaIndex},
		{"retry", http.MethodGet, route, url + "?refresh=true", GetSchemaIndex},
		{"refresh endpoint", http.MethodPost, route + "/refresh", url + "/refresh", RefreshSchemaIndex},
	}
	for _, req := range requests {
		t.Run("failed discovery: "+req.name, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			docFindConnection(t, mock, "mongodb")
			calls, _ := docFindOrchestrator(t, "/api/v1/agent/discover-schema", http.StatusOK, `{"overall_status":"failed","tables":[],
				"warnings_messages":["bad auth : authentication failed while connecting to mongodb+srv://app_user:S3cretPass99@cluster0.example.net/shop, full error: {'ok': 0.0, 'errmsg': 'bad auth : authentication failed'}"]}`)

			rr := httptest.NewRecorder()
			wsScopeRouter(req.method, req.route, req.handler).ServeHTTP(rr, httptest.NewRequest(req.method, req.url, nil))

			if n := calls.Load(); n != 1 {
				t.Fatalf("orchestrator calls = %d, want 1", n)
			}
			if rr.Code < 400 {
				t.Fatalf("status = %d, want an error status: %s", rr.Code, rr.Body.String())
			}
			var body map[string]interface{}
			if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v: %s", err, rr.Body.String())
			}
			msg, _ := body["error"].(string)
			if !strings.Contains(msg, "bad auth : authentication failed") {
				t.Errorf("error message lost the connector's reason: %q", msg)
			}
			if _, hasTables := body["tables"]; hasTables {
				t.Errorf("an error response carries a table list: %s", rr.Body.String())
			}
			for _, leaked := range []string{"S3cretPass99", "app_user:"} {
				if strings.Contains(rr.Body.String(), leaked) {
					t.Errorf("response leaks %q: %s", leaked, rr.Body.String())
				}
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("db expectations: %v", err)
			}
		})
	}

	t.Run("successful discovery is still a 200 with the collections", func(t *testing.T) {
		mock, cleanup := wsScopeMockDB(t)
		defer cleanup()
		docFindConnection(t, mock, "mongodb")
		docFindOrchestrator(t, "/api/v1/agent/discover-schema", http.StatusOK,
			`{"overall_status":"success","warnings_messages":[],"tables":[{"name":"orders","schema":"shop","columns":[]}]}`)

		rr := httptest.NewRecorder()
		wsScopeRouter(http.MethodGet, route, GetSchemaIndex).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, url, nil))

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rr.Code, rr.Body.String())
		}
		var body struct {
			TableCount int `json:"table_count"`
			Tables     []struct {
				Name string `json:"name"`
			} `json:"tables"`
		}
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.TableCount != 1 || len(body.Tables) != 1 || body.Tables[0].Name != "orders" {
			t.Fatalf("unexpected body: %s", rr.Body.String())
		}
	})
}

// memRedis answers the schema-index cache's GET, SET and DEL from a map, so the
// cache path runs without a Redis server. It never dials: any other command, or a
// connection attempt, is an error.
type memRedis struct {
	mu   sync.Mutex
	data map[string]string
	sets []string
}

func (m *memRedis) DialHook(redis.DialHook) redis.DialHook {
	return func(context.Context, string, string) (net.Conn, error) {
		return nil, errors.New("memRedis: no network in tests")
	}
}

func (m *memRedis) ProcessPipelineHook(redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(context.Context, []redis.Cmder) error { return errors.New("memRedis: no pipelines") }
}

func (m *memRedis) ProcessHook(redis.ProcessHook) redis.ProcessHook {
	return func(_ context.Context, cmd redis.Cmder) error {
		m.mu.Lock()
		defer m.mu.Unlock()
		args := cmd.Args()
		key := ""
		if len(args) > 1 {
			key = fmt.Sprint(args[1])
		}
		switch c := cmd.(type) {
		case *redis.StringCmd: // GET
			v, ok := m.data[key]
			if !ok {
				c.SetErr(redis.Nil)
				return redis.Nil
			}
			c.SetVal(v)
		case *redis.StatusCmd: // SET key value [ex n]
			var v string
			switch raw := args[2].(type) {
			case []byte:
				v = string(raw)
			default:
				v = fmt.Sprint(raw)
			}
			m.data[key] = v
			m.sets = append(m.sets, key)
			c.SetVal("OK")
		case *redis.IntCmd: // DEL key
			delete(m.data, key)
			c.SetVal(1)
		default:
			err := fmt.Errorf("memRedis: unexpected command %v", args)
			cmd.SetErr(err)
			return err
		}
		return nil
	}
}

func (m *memRedis) setKeys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.sets...)
}

// A failed listing must never be written to the schema-index cache: a cached empty
// index would show "0 databases" again on the next load without asking the cluster.
// And the page's Retry (refresh=true) must go past an empty index already cached.
func TestGetSchemaIndex_DocumentFailureIsNeverCached(t *testing.T) {
	mem := &memRedis{data: map[string]string{}}
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", MaxRetries: -1})
	client.AddHook(mem)
	t.Cleanup(func() { _ = client.Close() })
	prevCache := explorerCache
	explorerCache = cache.NewExplorerCache(client, 10*time.Minute)
	t.Cleanup(func() { explorerCache = prevCache })

	route := "/api/v1/explorer/connections/:id/schema-index"
	url := "/api/v1/explorer/connections/" + docFindConnID + "/schema-index"
	key := "explorer:schema_index:" + docFindConnID
	const failed = `{"overall_status":"failed","tables":[],"warnings_messages":["bad auth : authentication failed"]}`
	const listed = `{"overall_status":"success","warnings_messages":[],"tables":[{"name":"orders","schema":"shop","columns":[]}]}`

	get := func(t *testing.T, orchestratorBody, target string) (*httptest.ResponseRecorder, int32) {
		t.Helper()
		mock, cleanup := wsScopeMockDB(t)
		defer cleanup()
		docFindConnection(t, mock, "mongodb")
		calls, _ := docFindOrchestrator(t, "/api/v1/agent/discover-schema", http.StatusOK, orchestratorBody)
		rr := httptest.NewRecorder()
		wsScopeRouter(http.MethodGet, route, GetSchemaIndex).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, target, nil))
		return rr, calls.Load()
	}

	t.Run("a failed listing is not stored", func(t *testing.T) {
		rr, calls := get(t, failed, url)
		if calls != 1 || rr.Code < 400 {
			t.Fatalf("calls = %d, status = %d, want 1 call and an error: %s", calls, rr.Code, rr.Body.String())
		}
		if keys := mem.setKeys(); len(keys) != 0 {
			t.Fatalf("a failed listing was cached under %v", keys)
		}
	})

	t.Run("a successful listing is stored", func(t *testing.T) {
		rr, calls := get(t, listed, url)
		if calls != 1 || rr.Code != http.StatusOK {
			t.Fatalf("calls = %d, status = %d, want 1 call and 200: %s", calls, rr.Code, rr.Body.String())
		}
		if keys := mem.setKeys(); len(keys) != 1 || keys[0] != key {
			t.Fatalf("cached keys = %v, want [%s]", keys, key)
		}
	})

	t.Run("retry goes past an empty index already cached", func(t *testing.T) {
		mem.mu.Lock()
		mem.data[key] = `{"connection_id":"` + docFindConnID + `","schema_hash":"stale","table_count":0,"tables":[]}`
		mem.sets = nil
		mem.mu.Unlock()

		// Control: a plain load is answered from that cached entry, so the fake is
		// really in the path.
		if rr, calls := get(t, failed, url); calls != 0 || rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"cached":true`) {
			t.Fatalf("control: calls = %d, status = %d, want the cached entry: %s", calls, rr.Code, rr.Body.String())
		}

		rr, calls := get(t, failed, url+"?refresh=true")
		if calls != 1 {
			t.Fatalf("orchestrator calls = %d, want 1: retry answered from the cache: %s", calls, rr.Body.String())
		}
		if rr.Code < 400 || strings.Contains(rr.Body.String(), `"table_count"`) {
			t.Fatalf("status = %d, want an error without a table count: %s", rr.Code, rr.Body.String())
		}
		if keys := mem.setKeys(); len(keys) != 0 {
			t.Fatalf("a failed retry was cached under %v", keys)
		}
	})
}
