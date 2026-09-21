package handlers

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/shared/crypto"
)

const namespacesPath = "/api/v1/connections/:id/namespaces"

// namespacesConnection expects the workspace-scoped load of a connection pinned
// to v1.0.0 and the audit row the listing writes.
func namespacesConnection(t *testing.T, mock sqlmock.Sqlmock, connectorType string) {
	t.Helper()
	t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
	enc, err := crypto.EncryptString(`{"host":"db","database":"shop","schema":"shop"}`)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	mock.ExpectQuery(`SELECT connector_type, config, connector_version\s+FROM connections\s+WHERE id = \$1 AND workspace_id = \$2`).
		WithArgs(docFindConnID, wsScopeWS).
		WillReturnRows(sqlmock.NewRows([]string{"connector_type", "config", "connector_version"}).AddRow(connectorType, enc, "v1.0.0"))
	mock.ExpectExec(`INSERT INTO connection_access_logs`).
		WithArgs(docFindConnID, wsScopeUser, "list_namespaces", true, "").
		WillReturnResult(sqlmock.NewResult(1, 1))
}

func serveNamespaces(id string) *httptest.ResponseRecorder {
	r := wsScopeRouter(http.MethodGet, namespacesPath, ListConnectionNamespaces)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/connections/"+id+"/namespaces", nil))
	return rr
}

func TestListConnectionNamespaces_ListsThroughTheConnector(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	namespacesConnection(t, mock, "postgresql")
	calls, got := docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusOK,
		`{"namespaces":["public","shop"],"current":"shop"}`)

	rr := serveNamespaces(docFindConnID)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	var out struct {
		ConnectionID  string   `json:"connection_id"`
		ConnectorType string   `json:"connector_type"`
		NamespaceKind string   `json:"namespace_kind"`
		Namespaces    []string `json:"namespaces"`
		Current       string   `json:"current"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.ConnectionID != docFindConnID || out.ConnectorType != "postgresql" || out.NamespaceKind != "schema" ||
		strings.Join(out.Namespaces, ",") != "public,shop" || out.Current != "shop" {
		t.Errorf("response = %+v", out)
	}
	if calls.Load() != 1 {
		t.Fatalf("orchestrator called %d times, want 1", calls.Load())
	}
	var fwd struct {
		ConnectorType string                 `json:"connector_type"`
		ConnectionID  string                 `json:"connection_id"`
		Config        map[string]interface{} `json:"config"`
	}
	if err := json.Unmarshal([]byte(got.Load().(string)), &fwd); err != nil {
		t.Fatalf("forwarded body: %v", err)
	}
	if fwd.ConnectorType != "postgresql" || fwd.ConnectionID != docFindConnID {
		t.Errorf("forwarded = %+v", fwd)
	}
	// The decrypted config goes to the connector, pinned to the connection's version.
	if fwd.Config["host"] != "db" || fwd.Config["connector_version"] != "v1.0.0" {
		t.Errorf("forwarded config = %v", fwd.Config)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestListConnectionNamespaces_NamespaceKindComesFromTheConnectorModel(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	namespacesConnection(t, mock, "mongodb")
	docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusOK, `{"namespaces":["app"],"current":""}`)

	rr := serveNamespaces(docFindConnID)

	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"namespace_kind":"database"`) ||
		!strings.Contains(rr.Body.String(), `"lists_namespaces":true`) {
		t.Errorf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
}

// A 502 body is replaced by the edge proxy's own page, so the connector's
// reason would never reach the user; the gateway answers 424 instead.
func TestListConnectionNamespaces_ConnectorFailureIsA424WithItsReason(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	namespacesConnection(t, mock, "postgresql")
	docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusBadGateway,
		`{"error":"list_namespaces failed","details":"Listing namespaces failed: permission denied for schema secret"}`)

	rr := serveNamespaces(docFindConnID)

	if rr.Code != http.StatusFailedDependency {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "permission denied for schema secret") {
		t.Errorf("the connector's reason is lost: %s", rr.Body.String())
	}
}

// GCS, S3, Azure Blob, MinIO and SaaS connectors have no list_namespaces tool.
// Asking one was a 502 (and behind Cloudflare an HTML error page); the answer is
// a plain "nothing to list", given without calling the connector.
func TestListConnectionNamespaces_AConnectorWithNothingToListIsA200(t *testing.T) {
	for _, connectorType := range []string{"gcs", "aws-s3", "salesforce"} {
		t.Run(connectorType, func(t *testing.T) {
			mock, cleanup := wsScopeMockDB(t)
			defer cleanup()
			t.Setenv("ENCRYPTION_KEY", "unit-test-encryption-key-0123456789ab")
			mock.ExpectQuery(`SELECT connector_type, config, connector_version`).
				WithArgs(docFindConnID, wsScopeWS).
				WillReturnRows(sqlmock.NewRows([]string{"connector_type", "config", "connector_version"}).AddRow(connectorType, "not-decrypted", "v1.0.0"))
			calls, _ := docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusBadGateway, `{"error":"tool not found"}`)

			rr := serveNamespaces(docFindConnID)

			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
			}
			var out struct {
				ListsNamespaces *bool    `json:"lists_namespaces"`
				NamespaceKind   string   `json:"namespace_kind"`
				Namespaces      []string `json:"namespaces"`
			}
			if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if out.ListsNamespaces == nil || *out.ListsNamespaces || out.NamespaceKind != "" || out.Namespaces == nil || len(out.Namespaces) != 0 {
				t.Errorf("response = %s", rr.Body.String())
			}
			if calls.Load() != 0 {
				t.Error("the connector was asked for namespaces it cannot list")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestListConnectionNamespaces_CurrentIsAlwaysListed(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	namespacesConnection(t, mock, "mongodb")
	docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusOK, `{"namespaces":["admin_app","shop"],"current":"demo"}`)

	rr := serveNamespaces(docFindConnID)

	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"namespaces":["admin_app","shop","demo"]`) {
		t.Errorf("the connection's own database must be offered; status = %d, body = %s", rr.Code, rr.Body.String())
	}
}

func TestListConnectionNamespaces_AnotherWorkspacesConnectionIsNotFound(t *testing.T) {
	mock, cleanup := wsScopeMockDB(t)
	defer cleanup()
	mock.ExpectQuery(`SELECT connector_type, config, connector_version`).
		WithArgs(docFindConnID, wsScopeWS).
		WillReturnError(sql.ErrNoRows)
	calls, _ := docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusOK, `{"namespaces":[]}`)

	rr := serveNamespaces(docFindConnID)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rr.Code)
	}
	if calls.Load() != 0 {
		t.Error("the orchestrator was called for a connection the workspace does not own")
	}
}

func TestListConnectionNamespaces_RejectsANonUUID(t *testing.T) {
	_, cleanup := wsScopeMockDB(t)
	defer cleanup()
	if rr := serveNamespaces("not-a-uuid"); rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

const previewNamespacesPath = "/api/v1/connections/namespaces"

func servePreviewNamespaces(role, body string) *httptest.ResponseRecorder {
	r := wsScopeRouterAsRole(http.MethodPost, previewNamespacesPath, PreviewConnectionNamespaces, role)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, previewNamespacesPath, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(rr, req)
	return rr
}

// The Scope step previews a connection before it is saved: the config the form
// holds goes to the connector, pinned to the version the form picked.
func TestPreviewConnectionNamespaces_ListsFromTheUnsavedConfig(t *testing.T) {
	calls, got := docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusOK,
		`{"namespaces":["crm","shop"],"current":""}`)

	rr := servePreviewNamespaces("member",
		`{"connector_type":"mysql","connector_version":"v1.0.0","config":{"host":"db","username":"u"}}`)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	for _, want := range []string{`"lists_namespaces":true`, `"namespace_kind":"database"`, `"namespaces":["crm","shop"]`} {
		if !strings.Contains(rr.Body.String(), want) {
			t.Errorf("body %s lacks %s", rr.Body.String(), want)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("orchestrator called %d times, want 1", calls.Load())
	}
	var fwd struct {
		ConnectorType string                 `json:"connector_type"`
		Config        map[string]interface{} `json:"config"`
	}
	if err := json.Unmarshal([]byte(got.Load().(string)), &fwd); err != nil {
		t.Fatalf("forwarded body: %v", err)
	}
	if fwd.ConnectorType != "mysql" || fwd.Config["host"] != "db" || fwd.Config["connector_version"] != "v1.0.0" {
		t.Errorf("forwarded = %+v", fwd)
	}
}

func TestPreviewConnectionNamespaces_NothingToListNeverCallsTheConnector(t *testing.T) {
	calls, _ := docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusBadGateway, `{"error":"tool not found"}`)

	rr := servePreviewNamespaces("member", `{"connector_type":"gcs","config":{"bucket":"b"}}`)

	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"lists_namespaces":false`) {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if calls.Load() != 0 {
		t.Error("the connector was asked for namespaces it cannot list")
	}
}

// The preview answers failures and the connection's own database as the saved
// listing does: a 424 carrying the reason (a 502 body is replaced at the edge),
// and the database the form names offered even when the login cannot list it.
func TestPreviewConnectionNamespaces_FailureIsA424AndCurrentIsListed(t *testing.T) {
	docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusBadGateway,
		`{"error":"Access denied for user 'u'@'%'"}`)
	rr := servePreviewNamespaces("member", `{"connector_type":"mysql","config":{"host":"db"}}`)
	if rr.Code != http.StatusFailedDependency || !strings.Contains(rr.Body.String(), "Access denied") {
		t.Errorf("failure: status = %d, body = %s", rr.Code, rr.Body.String())
	}

	docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusOK, `{"namespaces":["crm"],"current":"shop"}`)
	rr = servePreviewNamespaces("member", `{"connector_type":"mysql","config":{"host":"db","database":"shop"}}`)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"namespaces":["crm","shop"]`) {
		t.Errorf("current: status = %d, body = %s", rr.Code, rr.Body.String())
	}
}

// It exercises credentials the caller typed, like POST /connections/test: a
// viewer may not, and a request without a connector type is refused unsent.
func TestPreviewConnectionNamespaces_Refusals(t *testing.T) {
	calls, _ := docFindOrchestrator(t, "/api/v1/agent/list-namespaces", http.StatusOK, `{"namespaces":["x"]}`)

	if rr := servePreviewNamespaces("viewer", `{"connector_type":"mysql","config":{"host":"db"}}`); rr.Code != http.StatusForbidden {
		t.Errorf("viewer: status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if rr := servePreviewNamespaces("member", `{"config":{"host":"db"}}`); rr.Code != http.StatusBadRequest {
		t.Errorf("no connector_type: status = %d, body = %s", rr.Code, rr.Body.String())
	}
	if calls.Load() != 0 {
		t.Errorf("orchestrator called %d times on a refused request", calls.Load())
	}
}
