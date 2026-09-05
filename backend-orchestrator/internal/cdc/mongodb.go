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
// The one true source prerequisite (the deployment is a replica set or sharded
// cluster; a standalone mongod cannot emit change streams) is enforced by
// Debezium at connector start and surfaced by the healer: "not a replica set"
// routes to ActionEscalate with an rs.initiate() remediation (see
// pkg/diagnose). We keep this provider dependency-free (no mongo driver in the
// orchestrator) rather than open a second connection solely to re-check that.
// [fast-follow] a driver-based hello() pre-check could move the failure earlier
// than connector start, at the cost of the mongo-go-driver dependency.
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

// ValidatePrerequisites defers the replica-set / privilege check to Debezium
// connector start, where a non-replica-set source fails loudly and the healer
// converts it to an escalation with an rs.initiate() remediation. Kept
// dependency-free by design; returns no blocking errors.
func (m *MongoDBManager) ValidatePrerequisites(ctx context.Context, connectionID string) ([]ValidationError, error) {
	return []ValidationError{}, nil
}

// ValidateTablesHavePrimaryKeys never blocks a MongoDB collection: _id is a
// mandatory, always-present field and is the primary key of the packed
// destination table, so no collection can lack a PK.
func (m *MongoDBManager) ValidateTablesHavePrimaryKeys(ctx context.Context, connectionID string, defaultNamespace string, tables []string) ([]string, error) {
	return []string{}, nil
}
