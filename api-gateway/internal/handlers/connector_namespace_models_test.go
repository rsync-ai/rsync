package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/backend-orchestrator/pkg/namespacemodel"
)

func TestGetConnectorNamespaceModels(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	// Registered beside /connectors/:name exactly as cmd/server/main.go does:
	// the static path must win over the parameter.
	r.GET("/api/v1/connectors/namespace-models", GetConnectorNamespaceModels)
	r.GET("/api/v1/connectors/:name", func(c *gin.Context) { c.String(http.StatusTeapot, c.Param("name")) })

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/connectors/namespace-models", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("status %d, body %s", w.Code, w.Body.String())
	}
	var body struct {
		Models          map[string]namespacemodel.Model `json:"models"`
		Defaults        namespacemodel.Model            `json:"defaults"`
		ListsNamespaces []string                        `json:"lists_namespaces"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Models) < 14 {
		t.Fatalf("got %d models; every database and storage connector declares one", len(body.Models))
	}
	want := map[string]namespacemodel.Model{
		"mysql":      {TableNamespace: "database", DestinationNamespace: "database", DestinationDefault: "default"},
		"postgres":   {TableNamespace: "schema", DestinationNamespace: "schema", DestinationDefault: "public"},
		"awss3":      {TableNamespace: "schema", DestinationNamespace: "path"},
		"bigquery":   {TableNamespace: "dataset", DestinationNamespace: "dataset"},
		"mongodb":    {TableNamespace: "database", DestinationNamespace: "database"},
		"postgresql": {TableNamespace: "schema", DestinationNamespace: "schema", DestinationDefault: "public"},
	}
	for k, m := range want {
		if body.Models[k] != m {
			t.Errorf("models[%q] = %+v, want %+v", k, body.Models[k], m)
		}
	}
	if body.Defaults != (namespacemodel.Model{TableNamespace: "schema", DestinationNamespace: "schema"}) {
		t.Errorf("defaults = %+v", body.Defaults)
	}
	// Databases list their namespaces; an object store has none to list.
	lists := map[string]bool{}
	for _, k := range body.ListsNamespaces {
		lists[k] = true
	}
	for k, want := range map[string]bool{"mysql": true, "mongodb": true, "postgresql": true, "bigquery": true, "awss3": false, "gcs": false} {
		if lists[k] != want {
			t.Errorf("lists_namespaces has %q = %v, want %v", k, lists[k], want)
		}
	}

	// Control: a real connector name still reaches the :name route.
	w = httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/connectors/mysql", nil))
	if w.Code != http.StatusTeapot || w.Body.String() != "mysql" {
		t.Fatalf("/connectors/mysql = %d %q, want the :name route", w.Code, w.Body.String())
	}
}
