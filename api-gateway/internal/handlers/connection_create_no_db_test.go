package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// CreateConnection used to answer an unavailable database with 201 Created and a
// freshly minted uuid. The caller stored an id that exists nowhere, and every
// pipeline, discovery run and retry built on it failed later against a
// connection the server had never written -- long after the outage left the logs.
func TestCreateConnectionRefusesWhenTheDatabaseIsUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// No DB is initialised in this package's tests, so db.GetDB() is nil -- the
	// exact condition the mock response used to serve.
	body, _ := json.Marshal(map[string]any{
		"name":            "outage-probe",
		"connection_type": "source",
		"connector_type":  "postgresql",
		"config":          map[string]any{"host": "db.example.com"},
	})

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("user_id", "11111111-1111-1111-1111-111111111111")
	// Mirror what WorkspaceContext middleware would have set, so the request
	// reaches the database check rather than stopping at the role gate.
	c.Set("workspace_id", "22222222-2222-2222-2222-222222222222")
	c.Set("workspace_role", "owner")
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/connections", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	CreateConnection(c)

	if w.Code == http.StatusCreated {
		t.Fatalf("fabricated a created connection during an outage: %d %s", w.Code, w.Body.String())
	}
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body = %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response is not JSON: %v", err)
	}
	if id, ok := resp["id"]; ok {
		t.Fatalf("the refusal still handed back an id: %v", id)
	}
}
