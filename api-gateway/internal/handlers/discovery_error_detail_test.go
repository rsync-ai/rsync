package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/shared/crypto"
)

// discoveryFailedBody is what the orchestrator's /agent/discover-schema returns
// when the connector itself reports overall_status "failed".
const discoveryFailedBody = `{"error":"schema discovery failed","details":"The DNS query name does not exist: _mongodb._tcp.cluster0.example.net.","overall_status":"failed"}`

const discoveryFailedDetail = "The DNS query name does not exist: _mongodb._tcp.cluster0.example.net."

func TestOrchestratorErrorDetail(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"details wins", 422, discoveryFailedBody, discoveryFailedDetail},
		{"error when no details", 503, `{"error":"schema discovery failed"}`, "schema discovery failed"},
		{"message when no details", 500, `{"error":"x_failed","message":"readable"}`, "readable"},
		{"non-JSON body is used as-is", 502, "  Bad Gateway \n", "Bad Gateway"},
		{"empty body falls back to status", 504, "", "HTTP 504"},
		{"empty JSON falls back to status", 500, `{}`, "HTTP 500"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := orchestratorErrorDetail(tc.status, []byte(tc.body)); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}

	t.Run("long message is bounded", func(t *testing.T) {
		got := orchestratorErrorDetail(422, []byte(`{"details":"`+strings.Repeat("x", 4000)+`"}`))
		if n := len([]rune(got)); n > orchestratorErrorDetailMaxLen+1 {
			t.Fatalf("got %d runes", n)
		}
	})
}

// The table dialog reads this endpoint. A failed discovery must come back as
// an error carrying the connector's message, not as raw JSON in `details`.
func TestGetConnectionMetadata_DiscoveryFailureShowsConnectorMessage(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
	enc, err := crypto.EncryptString(`{"connection_string":"mongodb+srv://cluster0.example.net/shop","database":"shop"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	calls, _ := docFindOrchestrator(t, "/api/v1/agent/discover-schema", http.StatusUnprocessableEntity, discoveryFailedBody)

	mock.ExpectQuery(`SELECT 1 FROM connections WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(docFindConnID, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"?column?"}).AddRow(1))
	mock.ExpectQuery(`SELECT connector_type, type, config\s+FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(docFindConnID, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type", "type", "config"}).AddRow("mongodb", "source", enc))

	r := wsScopeRouter(http.MethodGet, "/api/v1/connections/:id/metadata", GetConnectionMetadata)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/connections/"+docFindConnID+"/metadata", nil))

	if rr.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body=%s", rr.Code, rr.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("orchestrator calls = %d, want 1", calls.Load())
	}
	var body map[string]interface{}
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["details"] != discoveryFailedDetail {
		t.Fatalf("details = %q, want the connector's message", body["details"])
	}
	if _, hasTables := body["tables"]; hasTables {
		t.Fatalf("a failed discovery must not return a tables list: %s", rr.Body.String())
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestBuildDocumentSchemaIndex_DiscoveryFailureShowsConnectorMessage(t *testing.T) {
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
	enc, err := crypto.EncryptString(`{"connection_string":"mongodb://h/shop"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	docFindOrchestrator(t, "/api/v1/agent/discover-schema", http.StatusUnprocessableEntity, discoveryFailedBody)

	_, err = buildSchemaIndex(t.Context(), docFindConnID, "mongodb", enc, false)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), discoveryFailedDetail) || strings.Contains(err.Error(), `{"error"`) {
		t.Fatalf("error = %q, want the connector's message without raw JSON", err)
	}
}

func TestFetchSourceTables_DiscoveryFailureShowsConnectorMessage(t *testing.T) {
	docFindOrchestrator(t, "/api/v1/agent/discover-schema", http.StatusUnprocessableEntity, discoveryFailedBody)

	tables, err := fetchSourceTables(t.Context(), docFindConnID, "mongodb", map[string]interface{}{"database": "shop"}, wsScopeUser)
	if err == nil {
		t.Fatalf("expected an error, got tables %+v", tables)
	}
	if !strings.Contains(err.Error(), discoveryFailedDetail) {
		t.Fatalf("error = %q, want the connector's message", err)
	}
}
