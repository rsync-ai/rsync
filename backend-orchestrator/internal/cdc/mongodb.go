package cdc

import (
	"context"
	"database/sql"

	log "github.com/sirupsen/logrus"
)

// MongoDBManager handles CDC "provisioning" for MongoDB sources.
//
// MongoDB CDC is fundamentally different from the relational engines. Debezium
// captures via the change-streams API, whose stream "position" is a resume
// token persisted as the Kafka Connect offset. There is NO server-side object
// to create — no publication, no replication slot, no capture instance — so
// ProvisionResources and CleanupResources are deliberate no-ops.
//
// Two source prerequisites are NOT enforced here, and Debezium does not enforce
// them loudly either. Probed on Debezium 3.1 (#19): against a standalone mongod
// ("$changeStream stage is only supported on replica sets") and with a user that
// lacks change-stream privileges (Unauthorized), the connector is accepted, the
// task reports RUNNING, and Debezium retries without limit while writing nothing;
// the errors appear only in the Kafka Connect worker log. The privilege case is
// avoided by construction: the debezium MCP connector scopes the change stream to
// the captured database (capture.scope=database), so `read` on that database is
// enough. The standalone case is caught before start_sync by the executor, which
// asks the mongodb MCP connector's test_connection for the topology
// (executor/mongodb_topology_check.go); this provider stays dependency-free.
//
// Destination mapping is the "packed" shape (see the sink's mongo-document
// decode branch): every collection lands as _id (TEXT PK) + one JSONB/JSON
// `document` column plus CDC metadata. _id is therefore ALWAYS the primary key,
// so no collection is ever "missing a PK".
type MongoDBManager struct {
	db *sql.DB
}

// NewMongoDBManager creates a new MongoDB CDC manager. db is the orchestrator's
// own metadata handle (used by relational providers to read connection configs
// and record cdc_resources); MongoDB provisions nothing, so it is retained only
// to satisfy the ProviderFactory signature.
func NewMongoDBManager(db *sql.DB) *MongoDBManager {
	return &MongoDBManager{db: db}
}

// init registers MongoDB as a CDC source provider so the handler dispatches
// through the shared registry (internal/cdc/provider.go) instead of a switch.
func init() {
	RegisterProvider(func(db *sql.DB) CDCSourceProvider {
		return NewMongoDBManager(db)
	}, "mongodb")
}

// Family returns the canonical source-family key for MongoDB.
func (m *MongoDBManager) Family() string { return "mongodb" }

// PrimaryKeyNamespace qualifies an unqualified collection with the MongoDB
// database name (collections live under <database>.<collection>). MongoDB has
// no "schema" level, so the schema candidate is ignored.
func (m *MongoDBManager) PrimaryKeyNamespace(defaultDB, defaultSchema string) string {
	return defaultDB
}

// ProvisionResources is a no-op for MongoDB: change streams require no
// server-side objects (contrast PostgreSQL's slot+publication or SQL Server's
// capture instances). It returns an empty resource set so the cleanup path has
// nothing to tear down.
func (m *MongoDBManager) ProvisionResources(ctx context.Context, config CDCResourceConfig, tables []string) ([]CDCResource, error) {
	log.WithFields(log.Fields{
		"pipeline_id":      config.PipelineID,
		"connection_id":    config.ConnectionID,
		"database":         config.Database,
		"collection_count": len(tables),
	}).Info("MongoDB CDC: change streams need no server-side resources; provisioning is a no-op")
	return []CDCResource{}, nil
}

// CleanupResources is a no-op: MongoDB provisions nothing, so there is nothing
// to remove. The Debezium connector itself (and its Kafka Connect offsets, i.e.
// the resume token) is torn down by the connector-delete path in the CDC
// handler, common to every family.
func (m *MongoDBManager) CleanupResources(ctx context.Context, pipelineID string) error {
	return nil
}

// ValidatePrerequisites returns no blocking errors: it has no connection to the
// source (see the type comment). It does not catch a standalone mongod — Debezium
// retries that forever with the task RUNNING rather than failing at start — so the
// executor checks topology before start_sync instead, through the mongodb
// connector's test_connection (executor/mongodb_topology_check.go).
func (m *MongoDBManager) ValidatePrerequisites(ctx context.Context, connectionID string) ([]ValidationError, error) {
	return []ValidationError{}, nil
}

// ValidateTablesHavePrimaryKeys never blocks a MongoDB collection: _id is a
// mandatory, always-present field and is the primary key of the packed
// destination table, so no collection can lack a PK.
func (m *MongoDBManager) ValidateTablesHavePrimaryKeys(ctx context.Context, connectionID string, defaultNamespace string, tables []string) ([]string, error) {
	return []string{}, nil
}
