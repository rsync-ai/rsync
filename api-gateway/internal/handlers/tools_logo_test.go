package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
)

func writeFile(t *testing.T, path string, contents []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

func TestConnectorLogo_AcceptsCanonicalAndAliases(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tmpDir := t.TempDir()
	t.Setenv("MCP_CONNECTORS_PATH", tmpDir)

	// Create a Freshdesk connector with a JPG logo (historically common for generated connectors).
	writeFile(t, filepath.Join(tmpDir, "freshdesk", "metadata.json"), []byte(`{
  "name": "Freshdesk API",
  "version": "1.0.0",
  "description": "Connector for interacting with Freshdesk API",
  "connector_type": "freshdesk",
  "category": "api_saas",
  "runtime": "python",
  "entrypoint": "connector.py",
  "supports_source": true,
  "supports_destination": false,
  "supports_cdc": false,
  "auth_type": "basic",
  "required_config": ["api_key"],
  "optional_config": [],
  "config_schema": {"type":"object","properties":{},"required":[]},
  "capabilities": {"max_batch_size": 10000, "supported_formats": ["json"], "supported_compressions": ["none"], "supports_cdc": false},
  "operations": []
}`))
	writeFile(t, filepath.Join(tmpDir, "freshdesk", "logo.jpg"), []byte("jpeg-bytes"))

	r := gin.New()
	r.GET("/api/v1/connectors", ListMCPConnectors)
	r.GET("/api/v1/connectors/:name/logo", GetMCPConnectorLogo)

	// 1) List: should include a logo_url and canonical name should be derived from connector_type.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/connectors", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", w.Code, w.Body.String())
	}

	var listResp struct {
		Connectors []MCPConnector `json:"connectors"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResp.Connectors) != 1 {
		t.Fatalf("expected 1 connector, got %d", len(listResp.Connectors))
	}
	if listResp.Connectors[0].Name != "freshdesk" {
		t.Fatalf("expected canonical name 'freshdesk', got '%s'", listResp.Connectors[0].Name)
	}
	if listResp.Connectors[0].LogoURL != "/api/v1/connectors/freshdesk/logo" {
		t.Fatalf("expected logo_url '/api/v1/connectors/freshdesk/logo', got '%s'", listResp.Connectors[0].LogoURL)
	}

	// 2) Logo endpoint: accept alias derived from display name (freshdesk-api).
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/connectors/freshdesk-api/logo", nil)
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("logo status=%d body=%s", w2.Code, w2.Body.String())
	}
	if ct := w2.Header().Get("Content-Type"); ct != "image/jpeg" {
		t.Fatalf("expected Content-Type image/jpeg, got '%s'", ct)
	}
}

func TestConnectors_VersionsOnlyLayout_IsDiscoverable(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tmpDir := t.TempDir()
	t.Setenv("MCP_CONNECTORS_PATH", tmpDir)

	// Simulate the "versions-only" layout:
	// <root>/public/<category>/<id>/latest.json
	// <root>/public/<category>/<id>/versions/<v>/metadata.json
	publicRoot := filepath.Join(tmpDir, "public")
	connRel := filepath.Join("api", "acme-crm")
	version := "v1.0.0"

	writeFile(t, filepath.Join(publicRoot, connRel, "latest.json"), []byte(`{
  "current_version": "v1.0.0"
}`))

	writeFile(t, filepath.Join(publicRoot, connRel, "versions", version, "metadata.json"), []byte(`{
  "id": "acme-crm",
  "name": "Acme CRM",
  "display_name": "Acme CRM",
  "version": "1.0.0",
  "description": "Test connector",
  "connector_type": "acme-crm",
  "category": "api_saas",
  "runtime": "python",
  "entrypoint": "connector.py",
  "supports_source": true,
  "supports_destination": true,
  "supports_cdc": false,
  "config_schema": {"type":"object","properties":{}}
}`))

	// Assets live in the version directory in this layout.
	writeFile(t, filepath.Join(publicRoot, connRel, "versions", version, "Dockerfile"), []byte("FROM scratch\n"))
	writeFile(t, filepath.Join(publicRoot, connRel, "versions", version, "logo.svg"), []byte(`<svg xmlns="http://www.w3.org/2000/svg"></svg>`))

	r := gin.New()
	r.GET("/api/v1/connectors", ListMCPConnectors)
	r.GET("/api/v1/connectors/:name", GetMCPConnector)
	r.GET("/api/v1/connectors/:name/logo", GetMCPConnectorLogo)

	// List should include the connector even though root metadata.json is absent.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/connectors", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var listResp struct {
		Connectors []MCPConnector `json:"connectors"`
		Total      int            `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if listResp.Total != 1 || len(listResp.Connectors) != 1 {
		t.Fatalf("expected 1 connector, got total=%d len=%d", listResp.Total, len(listResp.Connectors))
	}
	if listResp.Connectors[0].Name != "acme-crm" {
		t.Fatalf("expected name 'acme-crm', got '%s'", listResp.Connectors[0].Name)
	}
	if listResp.Connectors[0].LogoURL != "/api/v1/connectors/acme-crm/logo" {
		t.Fatalf("expected logo_url '/api/v1/connectors/acme-crm/logo', got '%s'", listResp.Connectors[0].LogoURL)
	}
	if !listResp.Connectors[0].HasDockerfile {
		t.Fatalf("expected has_dockerfile=true for versions-only layout")
	}

	// Detail endpoint should also work.
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/connectors/acme-crm", nil)
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w2.Code, w2.Body.String())
	}

	// Logo endpoint should serve the versioned logo.
	req3 := httptest.NewRequest(http.MethodGet, "/api/v1/connectors/acme-crm/logo", nil)
	w3 := httptest.NewRecorder()
	r.ServeHTTP(w3, req3)
	if w3.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w3.Code, w3.Body.String())
	}
	ct := w3.Header().Get("Content-Type")
	if ct != "image/svg+xml" {
		t.Fatalf("expected content-type image/svg+xml, got %q", ct)
	}
}

func TestConnectorLogo_CanonicalKebabIdToUnderscoreDir(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tmpDir := t.TempDir()
	t.Setenv("MCP_CONNECTORS_PATH", tmpDir)

	// Create AWS S3 connector folder in underscore form, but connector_type is kebab-case.
	writeFile(t, filepath.Join(tmpDir, "aws_s3", "metadata.json"), []byte(`{
  "name": "AWS S3",
  "version": "1.0.0",
  "description": "S3 connector",
  "connector_type": "aws-s3",
  "category": "cloud_storage",
  "runtime": "python",
  "entrypoint": "connector.py",
  "supports_source": true,
  "supports_destination": true,
  "supports_cdc": false,
  "auth_type": "basic",
  "required_config": ["access_key_id","secret_access_key","bucket"],
  "optional_config": [],
  "config_schema": {"type":"object","properties":{},"required":[]},
  "capabilities": {"max_batch_size": 10000, "supported_formats": ["json"], "supported_compressions": ["none"], "supports_cdc": false},
  "operations": []
}`))
	writeFile(t, filepath.Join(tmpDir, "aws_s3", "logo.png"), []byte("png-bytes"))

	r := gin.New()
	r.GET("/api/v1/connectors", ListMCPConnectors)
	r.GET("/api/v1/connectors/:name/logo", GetMCPConnectorLogo)

	// List should return canonical connector id "aws-s3" and logo_url with canonical.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/connectors", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("list status=%d body=%s", w.Code, w.Body.String())
	}
	var listResp struct {
		Connectors []MCPConnector `json:"connectors"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("decode list: %v", err)
	}
	if len(listResp.Connectors) != 1 {
		t.Fatalf("expected 1 connector, got %d", len(listResp.Connectors))
	}
	if listResp.Connectors[0].Name != "aws-s3" {
		t.Fatalf("expected canonical name 'aws-s3', got '%s'", listResp.Connectors[0].Name)
	}
	if listResp.Connectors[0].LogoURL != "/api/v1/connectors/aws-s3/logo" {
		t.Fatalf("expected logo_url '/api/v1/connectors/aws-s3/logo', got '%s'", listResp.Connectors[0].LogoURL)
	}

	// Logo endpoint should resolve canonical id to underscore directory.
	req2 := httptest.NewRequest(http.MethodGet, "/api/v1/connectors/aws-s3/logo", nil)
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("logo status=%d body=%s", w2.Code, w2.Body.String())
	}
	if ct := w2.Header().Get("Content-Type"); ct != "image/png" {
		t.Fatalf("expected Content-Type image/png, got '%s'", ct)
	}
}

// TestConnectorLogo_NotInstalled_FallsBackToGenericSVG covers KI-UI-5: a connector
// with NO on-disk directory (e.g. snowflake, mongodb, bigquery, or an unknown name)
// must serve the generic database icon with 200, not a 404 broken-image placeholder.
func TestConnectorLogo_NotInstalled_FallsBackToGenericSVG(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Empty connectors dir — no connector is installed at all.
	tmpDir := t.TempDir()
	t.Setenv("MCP_CONNECTORS_PATH", tmpDir)

	r := gin.New()
	r.GET("/api/v1/connectors/:name/logo", GetMCPConnectorLogo)

	for _, name := range []string{"snowflake", "mongodb", "bigquery", "unknown"} {
		req := httptest.NewRequest(http.MethodGet, "/api/v1/connectors/"+name+"/logo", nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("[%s] expected 200 (generic fallback), got %d body=%s", name, w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); ct != "image/svg+xml" {
			t.Fatalf("[%s] expected Content-Type image/svg+xml, got %q", name, ct)
		}
		if w.Body.String() != genericConnectorSVG {
			t.Fatalf("[%s] expected generic connector SVG body, got %q", name, w.Body.String())
		}
	}
}


