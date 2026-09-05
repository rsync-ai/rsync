package intent

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rsync-ai/backend-orchestrator/internal/registry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock connector registry for testing
type mockRegistry struct {
	// connectors: input-key -> canonical_type. Lets a test mock the
	// alias-resolution behaviour of the real registry by mapping
	// "shopify" -> "shopify-admin-graphql".
	connectors map[string]string
}

func (m *mockRegistry) GetCapabilities(connectorType string) *registry.ConnectorCapabilities {
	if canonical, ok := m.connectors[connectorType]; ok {
		return &registry.ConnectorCapabilities{
			ConnectorType: canonical,
		}
	}
	return nil
}

// newMockRegistry registers each input string as both the input key
// AND its canonical type — the common case. Use newMockRegistryWithAliases
// when testing the alias-resolution boundary.
func newMockRegistry(connectorTypes ...string) *mockRegistry {
	m := &mockRegistry{
		connectors: make(map[string]string),
	}
	for _, ct := range connectorTypes {
		m.connectors[ct] = ct
	}
	return m
}

// newMockRegistryWithAliases lets a test pin a specific alias→canonical
// mapping (e.g. "shopify" → "shopify-admin-graphql") to mirror what the
// real registry does via vendor-prefix matching.
func newMockRegistryWithAliases(aliasToCanonical map[string]string) *mockRegistry {
	return &mockRegistry{connectors: aliasToCanonical}
}

// TestWorkspaceResolver_ExactNameMatch tests exact connection name resolution
func TestWorkspaceResolver_ExactNameMatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockReg := newMockRegistry("mysql", "postgresql")
	resolver := NewWorkspaceContextResolver(db, mockReg)

	// Setup: expect query for exact name match
	rows := sqlmock.NewRows([]string{"id", "name", "connector_type", "environment", "alias"}).
		AddRow("conn-123", "production", "mysql", sql.NullString{String: "production", Valid: true}, sql.NullString{})

	mock.ExpectQuery(`SELECT id, name, connector_type, environment, alias FROM connections WHERE workspace_id = (.+) AND name = (.+)`).
		WithArgs("workspace-1", "production").
		WillReturnRows(rows)

	// Execute
	rawIntent := RawIntent{
		Source:      "production",
		Destination: "postgresql",
	}
	resolved, err := resolver.Resolve(context.Background(), rawIntent, "workspace-1")

	// Assert
	require.NoError(t, err)
	assert.Equal(t, "mysql", resolved.SourceConnectorType)
	assert.NotNil(t, resolved.SourceConnectionID)
	assert.Equal(t, "conn-123", *resolved.SourceConnectionID)
	assert.Equal(t, "postgresql", resolved.DestinationConnectorType)
	assert.Nil(t, resolved.DestinationConnectionID)

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestWorkspaceResolver_AmbiguousEnvironment tests ambiguous environment resolution
func TestWorkspaceResolver_AmbiguousEnvironment(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockReg := newMockRegistry("mysql", "postgresql")
	resolver := NewWorkspaceContextResolver(db, mockReg)

	// Setup: no exact name match
	mock.ExpectQuery(`SELECT id, name, connector_type, environment, alias FROM connections WHERE workspace_id = (.+) AND name = (.+)`).
		WithArgs("workspace-1", "production").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type", "environment", "alias"}))

	// Setup: no alias match
	mock.ExpectQuery(`SELECT id, name, connector_type, environment, alias FROM connections WHERE workspace_id = (.+) AND alias = (.+)`).
		WithArgs("workspace-1", "production").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type", "environment", "alias"}))

	// Setup: multiple environment matches (ambiguous)
	envRows := sqlmock.NewRows([]string{"id", "name", "connector_type", "environment", "alias"}).
		AddRow("conn-123", "prod-mysql", "mysql", sql.NullString{String: "production", Valid: true}, sql.NullString{}).
		AddRow("conn-456", "prod-postgres", "postgresql", sql.NullString{String: "production", Valid: true}, sql.NullString{})

	mock.ExpectQuery(`SELECT id, name, connector_type, environment, alias FROM connections WHERE workspace_id = (.+) AND environment = (.+)`).
		WithArgs("workspace-1", "production").
		WillReturnRows(envRows)

	// Execute
	rawIntent := RawIntent{
		Source:      "production",
		Destination: "staging",
	}
	_, err = resolver.Resolve(context.Background(), rawIntent, "workspace-1")

	// Assert: should return ambiguous error
	require.Error(t, err)
	resErr, ok := err.(*ResolutionError)
	require.True(t, ok, "Error should be *ResolutionError")
	assert.Equal(t, "ambiguous", resErr.Type)
	assert.Equal(t, "production", resErr.Reference)
	assert.Equal(t, "source", resErr.Context)
	assert.Len(t, resErr.Matches, 2)

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestWorkspaceResolver_NotFound tests workspace reference not found
func TestWorkspaceResolver_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockReg := newMockRegistry("mysql", "postgresql")
	resolver := NewWorkspaceContextResolver(db, mockReg)

	// Setup: no matches in any strategy
	mock.ExpectQuery(`SELECT id, name, connector_type, environment, alias FROM connections WHERE workspace_id = (.+) AND name = (.+)`).
		WithArgs("workspace-1", "production").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type", "environment", "alias"}))

	mock.ExpectQuery(`SELECT id, name, connector_type, environment, alias FROM connections WHERE workspace_id = (.+) AND alias = (.+)`).
		WithArgs("workspace-1", "production").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type", "environment", "alias"}))

	mock.ExpectQuery(`SELECT id, name, connector_type, environment, alias FROM connections WHERE workspace_id = (.+) AND environment = (.+)`).
		WithArgs("workspace-1", "production").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type", "environment", "alias"}))

	// Substring match on name/description (uses $2 twice but only one argument is passed)
	mock.ExpectQuery(`SELECT id, name, connector_type, environment, alias FROM connections WHERE workspace_id = (.+) AND \(name ILIKE (.+) OR description ILIKE (.+)\) LIMIT 10`).
		WithArgs("workspace-1", "production").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type", "environment", "alias"}))

	// Tag match (optional) - return no rows
	mock.ExpectQuery(`SELECT id, name, connector_type, environment, alias FROM connections WHERE workspace_id = (.+) AND (.+) = ANY\(tags\)`).
		WithArgs("workspace-1", "production").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type", "environment", "alias"}))

	// Expect suggestions query
	suggestRows := sqlmock.NewRows([]string{"id", "name", "connector_type", "environment"}).
		AddRow("conn-789", "my-mysql", "mysql", sql.NullString{})

	mock.ExpectQuery(`SELECT id, name, connector_type, environment FROM connections WHERE workspace_id = (.+) ORDER BY created_at DESC LIMIT 10`).
		WithArgs("workspace-1").
		WillReturnRows(suggestRows)

	// Execute
	rawIntent := RawIntent{
		Source:      "production",
		Destination: "staging",
	}
	_, err = resolver.Resolve(context.Background(), rawIntent, "workspace-1")

	// Assert: should return not_found error with suggestions
	require.Error(t, err)
	resErr, ok := err.(*ResolutionError)
	require.True(t, ok, "Error should be *ResolutionError")
	assert.Equal(t, "not_found", resErr.Type)
	assert.Equal(t, "production", resErr.Reference)
	assert.Equal(t, "source", resErr.Context)
	assert.Len(t, resErr.Suggestions, 1)
	assert.Equal(t, "my-mysql", resErr.Suggestions[0].Name)

	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestWorkspaceResolver_ConnectorTypePassthrough tests that known connector types pass through
func TestWorkspaceResolver_ConnectorTypePassthrough(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockReg := newMockRegistry("mysql", "postgresql", "snowflake")
	resolver := NewWorkspaceContextResolver(db, mockReg)

	// Execute: both are known connector types
	rawIntent := RawIntent{
		Source:      "postgresql",
		Destination: "snowflake",
	}
	resolved, err := resolver.Resolve(context.Background(), rawIntent, "workspace-1")

	// Assert: should resolve as connector types without DB lookup
	require.NoError(t, err)
	assert.Equal(t, "postgresql", resolved.SourceConnectorType)
	assert.Nil(t, resolved.SourceConnectionID) // No specific connection
	assert.Equal(t, "snowflake", resolved.DestinationConnectorType)
	assert.Nil(t, resolved.DestinationConnectionID)
}


// TestResolveReference_AliasToCanonical is the regression guard for the
// chat journey bug we shipped in PR-G: when the chat NL produces a
// generic vendor name like "shopify" but the catalog only has
// "shopify-admin-graphql", the resolver must return the CANONICAL
// connector_type — not the input alias — so downstream lookups
// (connection auto-select, capability resolver) hit the right rows in
// the connections table.
//
// Before the fix, isConnectorType returned a bool and resolveReference
// echoed back the input reference. Connections were queried with
// WHERE connector_type='shopify' and got zero rows; the pipeline died
// at the resolver with `No connection found for 'shopify'` even when
// the user had a perfectly valid shopify-admin-graphql connection.
func TestResolveReference_AliasToCanonical(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mockReg := newMockRegistryWithAliases(map[string]string{
		"shopify":               "shopify-admin-graphql",
		"shopify-admin-graphql": "shopify-admin-graphql",
		"postgresql":            "postgresql",
	})
	resolver := NewWorkspaceContextResolver(db, mockReg)

	rawIntent := RawIntent{
		Source:      "shopify",    // generic alias
		Destination: "postgresql", // canonical
	}
	resolved, err := resolver.Resolve(context.Background(), rawIntent, "workspace-1")

	require.NoError(t, err)
	// Source must be the CANONICAL connector_type, not the alias.
	assert.Equal(t, "shopify-admin-graphql", resolved.SourceConnectorType,
		"chat-driven 'shopify' alias must resolve to canonical 'shopify-admin-graphql'")
	assert.Nil(t, resolved.SourceConnectionID, "no specific connection ID for connector-type-only references")
	// Destination exact match passes through unchanged.
	assert.Equal(t, "postgresql", resolved.DestinationConnectorType)
}
