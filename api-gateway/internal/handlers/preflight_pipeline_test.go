package handlers

import (
	"database/sql"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// =====================================================================
// CapabilityAgent — pure filesystem inspection
// =====================================================================

// makeConnectorDir creates a minimal connector layout (metadata.json
// only) under root. Returns root. Used by every CapabilityAgent test
// so each test starts from a known-clean tree.
func makeConnectorDir(t *testing.T, structure map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for relPath, contents := range structure {
		full := filepath.Join(root, relPath)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(full, []byte(contents), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	return root
}

func TestCapabilityAgent_FlatLayoutFound(t *testing.T) {
	root := makeConnectorDir(t, map[string]string{
		"public/stripe/metadata.json": `{"id":"stripe"}`,
	})
	agent := &CapabilityAgent{Root: root}
	if hitl := agent.Check("stripe", "source"); hitl != nil {
		t.Fatalf("expected pass, got HITL: %+v", hitl)
	}
}

func TestCapabilityAgent_CategoryNestedLayoutFound(t *testing.T) {
	root := makeConnectorDir(t, map[string]string{
		"public/database/postgresql/metadata.json": `{"id":"postgresql"}`,
	})
	agent := &CapabilityAgent{Root: root}
	if hitl := agent.Check("postgresql", "destination"); hitl != nil {
		t.Fatalf("expected pass, got HITL: %+v", hitl)
	}
}

func TestCapabilityAgent_InternalLayoutFound(t *testing.T) {
	root := makeConnectorDir(t, map[string]string{
		"internal/debezium/metadata.json": `{"id":"debezium"}`,
	})
	agent := &CapabilityAgent{Root: root}
	if hitl := agent.Check("debezium", "source"); hitl != nil {
		t.Fatalf("expected pass, got HITL: %+v", hitl)
	}
}

func TestCapabilityAgent_NotFoundReturnsHITL(t *testing.T) {
	root := makeConnectorDir(t, map[string]string{
		"public/stripe/metadata.json": `{"id":"stripe"}`,
	})
	agent := &CapabilityAgent{Root: root}
	hitl := agent.Check("zendesk", "source")
	if hitl == nil {
		t.Fatal("expected HITL response for missing connector")
	}
	if hitl.ActionNeeded != "generate_connector" {
		t.Errorf("action_needed: want generate_connector, got %q", hitl.ActionNeeded)
	}
	if hitl.ConnectorType != "zendesk" {
		t.Errorf("connector_type: want zendesk, got %q", hitl.ConnectorType)
	}
	if hitl.Role != "source" {
		t.Errorf("role: want source, got %q", hitl.Role)
	}
	if !strings.Contains(hitl.Message, "zendesk") {
		t.Errorf("message should name the missing connector: %q", hitl.Message)
	}
}

func TestCapabilityAgent_EmptyConnectorTypeReturnsHITL(t *testing.T) {
	root := makeConnectorDir(t, map[string]string{})
	agent := &CapabilityAgent{Root: root}
	hitl := agent.Check("", "destination")
	if hitl == nil {
		t.Fatal("expected HITL response for empty connector_type")
	}
	if hitl.ActionNeeded != "specify_connector" {
		t.Errorf("action_needed: want specify_connector, got %q", hitl.ActionNeeded)
	}
}

func TestCapabilityAgent_DirectoryWithoutMetadataFails(t *testing.T) {
	root := makeConnectorDir(t, map[string]string{
		"public/halfbaked/README.md": `# WIP — no metadata.json yet`,
	})
	agent := &CapabilityAgent{Root: root}
	if hitl := agent.Check("halfbaked", "source"); hitl == nil {
		t.Error("expected HITL response — directory without metadata.json shouldn't count as installed")
	}
}

// =====================================================================
// ConnectionAgent — DB-backed via sqlmock
// =====================================================================

func newMockDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func TestConnectionAgent_ZeroConnectionsReturnsHITLCreate(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, connector_type`)).
		WithArgs("user-1", "shopify-admin-graphql", "source").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type"}))

	agent := NewConnectionAgent(db)
	resolved, hitl, err := agent.Check("user-1", "shopify-admin-graphql", "source")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resolved != nil {
		t.Fatal("expected no auto-resolve")
	}
	if hitl == nil {
		t.Fatal("expected HITL response")
	}
	if hitl.ActionNeeded != "create_connection" {
		t.Errorf("action_needed: want create_connection, got %q", hitl.ActionNeeded)
	}
	if hitl.ConnectorType != "shopify-admin-graphql" {
		t.Errorf("connector_type: want shopify-admin-graphql, got %q", hitl.ConnectorType)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("sqlmock: %v", err)
	}
}

func TestConnectionAgent_ExactlyOneAutoResolves(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, connector_type`)).
		WithArgs("user-1", "postgresql", "destination").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type"}).
			AddRow("conn-1", "Local Postgres", "postgresql"))

	agent := NewConnectionAgent(db)
	resolved, hitl, err := agent.Check("user-1", "postgresql", "destination")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if hitl != nil {
		t.Fatalf("expected auto-resolve, got HITL: %+v", hitl)
	}
	if resolved == nil {
		t.Fatal("expected resolved connection")
	}
	if resolved.ID != "conn-1" {
		t.Errorf("id: want conn-1, got %q", resolved.ID)
	}
	if resolved.Name != "Local Postgres" {
		t.Errorf("name: want Local Postgres, got %q", resolved.Name)
	}
}

func TestConnectionAgent_MultipleReturnsPicker(t *testing.T) {
	db, mock := newMockDB(t)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, connector_type`)).
		WithArgs("user-1", "stripe", "source").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type"}).
			AddRow("conn-prod", "stripe-prod", "stripe").
			AddRow("conn-stage", "stripe-staging", "stripe").
			AddRow("conn-test", "stripe-test", "stripe"))

	agent := NewConnectionAgent(db)
	resolved, hitl, err := agent.Check("user-1", "stripe", "source")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if resolved != nil {
		t.Fatal("expected HITL picker, got auto-resolve")
	}
	if hitl.ActionNeeded != "pick_connection" {
		t.Errorf("action_needed: want pick_connection, got %q", hitl.ActionNeeded)
	}
	options, ok := hitl.Details["options"].([]map[string]string)
	if !ok {
		t.Fatalf("details.options missing or wrong type: %v", hitl.Details["options"])
	}
	if len(options) != 3 {
		t.Errorf("options: want 3, got %d", len(options))
	}
	// Order of options should mirror DESC created_at — i.e. preserve the
	// DB row order.
	if options[0]["id"] != "conn-prod" {
		t.Errorf("first option: want conn-prod, got %q", options[0]["id"])
	}
}

func TestConnectionAgent_EmptyConnectorTypeIsError(t *testing.T) {
	db, _ := newMockDB(t)
	agent := NewConnectionAgent(db)
	_, _, err := agent.Check("user-1", "", "source")
	if err == nil {
		t.Error("expected error for empty connector_type")
	}
}

func TestConnectionAgent_NilDBIsError(t *testing.T) {
	agent := NewConnectionAgent(nil)
	_, _, err := agent.Check("user-1", "stripe", "source")
	if err == nil {
		t.Error("expected error for nil DB")
	}
}

// =====================================================================
// runPipelinePreflight — orchestration
// =====================================================================

func TestRunPipelinePreflight_AllProvidedConnectionIDs_Pass(t *testing.T) {
	root := makeConnectorDir(t, map[string]string{
		"public/stripe/metadata.json":     `{"id":"stripe"}`,
		"public/postgresql/metadata.json": `{"id":"postgresql"}`,
	})
	t.Setenv("TOOLS_DIR", root)

	db, mock := newMockDB(t)
	// Capability lookups for both existing connection IDs.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT connector_type FROM connections WHERE id = $1`)).
		WithArgs("conn-source", "user-1").
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow("stripe"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT connector_type FROM connections WHERE id = $1`)).
		WithArgs("conn-dest", "user-1").
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow("postgresql"))

	req := &CreatePipelineRequest{Name: "x", Request: "r"}
	src, dst := "conn-source", "conn-dest"
	hitl, err := runPipelinePreflight(db, "user-1", req, &src, &dst)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if hitl != nil {
		t.Fatalf("expected pass, got HITL: %+v", hitl)
	}
}

func TestRunPipelinePreflight_ConnectorTypeWithoutConnection_AutoResolves(t *testing.T) {
	root := makeConnectorDir(t, map[string]string{
		"public/shopify-admin-graphql/metadata.json": `{"id":"shopify-admin-graphql"}`,
		"public/postgresql/metadata.json":            `{"id":"postgresql"}`,
	})
	t.Setenv("TOOLS_DIR", root)

	db, mock := newMockDB(t)
	// ConnectionAgent finds exactly one Shopify source.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, connector_type`)).
		WithArgs("user-1", "shopify-admin-graphql", "source").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type"}).
			AddRow("shopify-conn", "Shopify Dev Store", "shopify-admin-graphql"))
	// Then CapabilityAgent looks up its connector_type.
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT connector_type FROM connections WHERE id = $1`)).
		WithArgs("shopify-conn", "user-1").
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow("shopify-admin-graphql"))
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT connector_type FROM connections WHERE id = $1`)).
		WithArgs("dest-conn", "user-1").
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow("postgresql"))

	req := &CreatePipelineRequest{
		Name:                "shopify-test",
		Request:             "sync products",
		SourceConnectorType: "shopify-admin-graphql",
	}
	src, dst := "", "dest-conn"
	hitl, err := runPipelinePreflight(db, "user-1", req, &src, &dst)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if hitl != nil {
		t.Fatalf("expected pass after auto-resolve, got HITL: %+v", hitl)
	}
	if src != "shopify-conn" {
		t.Errorf("source ID should be set to auto-resolved value, got %q", src)
	}
}

func TestRunPipelinePreflight_ConnectorTypeNoConnections_ReturnsHITL(t *testing.T) {
	root := makeConnectorDir(t, map[string]string{
		"public/zendesk/metadata.json": `{"id":"zendesk"}`,
	})
	t.Setenv("TOOLS_DIR", root)

	db, mock := newMockDB(t)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, name, connector_type`)).
		WithArgs("user-1", "zendesk", "source").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type"}))

	req := &CreatePipelineRequest{
		Name:                "x",
		Request:             "r",
		SourceConnectorType: "zendesk",
	}
	src, dst := "", "dest-conn"
	hitl, err := runPipelinePreflight(db, "user-1", req, &src, &dst)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if hitl == nil {
		t.Fatal("expected HITL_CREATE response")
	}
	if hitl.Agent != "ConnectionAgent" {
		t.Errorf("agent: want ConnectionAgent, got %q", hitl.Agent)
	}
	if hitl.ActionNeeded != "create_connection" {
		t.Errorf("action_needed: want create_connection, got %q", hitl.ActionNeeded)
	}
}

func TestRunPipelinePreflight_ConnectorMissingOnDisk_ReturnsHITLGenerate(t *testing.T) {
	// Connection exists in DB but the connector code is missing from disk
	// (someone deleted public/zendesk/ after creating a Zendesk connection).
	root := makeConnectorDir(t, map[string]string{
		"public/postgresql/metadata.json": `{"id":"postgresql"}`,
		// note: no zendesk dir
	})
	t.Setenv("TOOLS_DIR", root)

	db, mock := newMockDB(t)
	mock.ExpectQuery(regexp.QuoteMeta(`SELECT connector_type FROM connections WHERE id = $1`)).
		WithArgs("zendesk-conn", "user-1").
		WillReturnRows(sqlmock.NewRows([]string{"connector_type"}).AddRow("zendesk"))

	req := &CreatePipelineRequest{Name: "x", Request: "r"}
	src, dst := "zendesk-conn", ""
	hitl, err := runPipelinePreflight(db, "user-1", req, &src, &dst)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if hitl == nil {
		t.Fatal("expected HITL_GENERATE response")
	}
	if hitl.Agent != "CapabilityAgent" {
		t.Errorf("agent: want CapabilityAgent, got %q", hitl.Agent)
	}
	if hitl.ActionNeeded != "generate_connector" {
		t.Errorf("action_needed: want generate_connector, got %q", hitl.ActionNeeded)
	}
}

func TestRunPipelinePreflight_NoSourceConfig_PassesWithoutCheck(t *testing.T) {
	// Empty source — runs (e.g. destination-only pipelines or planner-mode
	// flows that resolve later). Pre-flight shouldn't block. The
	// downstream insert / planner handles "source not specified" errors.
	root := makeConnectorDir(t, map[string]string{})
	t.Setenv("TOOLS_DIR", root)

	db, _ := newMockDB(t)
	req := &CreatePipelineRequest{Name: "x", Request: "r"}
	src, dst := "", ""
	hitl, err := runPipelinePreflight(db, "user-1", req, &src, &dst)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if hitl != nil {
		t.Errorf("empty source/dest shouldn't trigger HITL here, got: %+v", hitl)
	}
}
