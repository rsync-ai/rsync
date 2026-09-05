package cdc

import (
	"context"
	"database/sql"
	"sort"
	"strings"
)

// CDCSourceProvider is the common contract each CDC source-database family
// implements. Registering a provider (see RegisterProvider) is all that is
// required to make a new source database provisionable end-to-end: the CDC
// handler dispatches provisioning, cleanup and PK validation through this
// interface instead of a hard-coded switch, so adding a database is "register
// one provider" rather than editing every dispatch site.
//
// Ordering invariants that are specific to a family — e.g. PostgreSQL's
// "publication BEFORE replication slot" rule (see CLAUDE.md) — are the
// responsibility of that family's ProvisionResources implementation. They are
// intentionally NOT expressed here so each provider owns its own correct
// ordering; the interface only fixes the call shape, never the sequence.
type CDCSourceProvider interface {
	// ProvisionResources creates the source-side CDC objects (replication
	// slots, publications, capture instances, supplemental logging, …) and
	// records them in cdc_resources. Each provider enforces its own required
	// ordering internally.
	ProvisionResources(ctx context.Context, config CDCResourceConfig, tables []string) ([]CDCResource, error)

	// CleanupResources tears down previously provisioned resources. It MUST be
	// idempotent and best-effort: when nothing exists for the pipeline it
	// no-ops. The cleanup handler may call it for EVERY registered family
	// (cdc_resources is frequently empty for e.g. MySQL pipelines), so a
	// provider must never error just because it owns nothing for the pipeline.
	CleanupResources(ctx context.Context, pipelineID string) error

	// ValidatePrerequisites checks that the source connection is CDC-capable
	// (PostgreSQL wal_level, MySQL binlog_format/row_image, SQL Server Agent
	// running + CDC enabled, Oracle supplemental logging, …).
	ValidatePrerequisites(ctx context.Context, connectionID string) ([]ValidationError, error)

	// ValidateTablesHavePrimaryKeys returns the subset of tables lacking a
	// primary key (relational destinations require one for upsert/delete
	// semantics). defaultNamespace qualifies unqualified table names and is the
	// schema (PostgreSQL) or database (MySQL); PrimaryKeyNamespace selects which.
	ValidateTablesHavePrimaryKeys(ctx context.Context, connectionID string, defaultNamespace string, tables []string) ([]string, error)

	// PrimaryKeyNamespace selects which default namespace (database vs schema)
	// ValidateTablesHavePrimaryKeys should use for this family, given the two
	// candidates inferred from a Debezium connector config.
	PrimaryKeyNamespace(defaultDB, defaultSchema string) string

	// Family returns the canonical source-family key (e.g. "postgresql").
	Family() string
}

// ProviderFactory constructs a provider bound to the orchestrator DB handle.
type ProviderFactory func(db *sql.DB) CDCSourceProvider

// providerRegistry maps a normalized database-type key to its provider factory.
// Populated by each provider file's init(); read via NewProvider.
var providerRegistry = map[string]ProviderFactory{}

// RegisterProvider registers a CDC source provider under one or more database
// type keys. Called from each provider file's init(). Keys are normalized (see
// NormalizeDBType) so aliases like "postgres"/"mssql" resolve to the same
// provider as their canonical name.
func RegisterProvider(factory ProviderFactory, dbTypes ...string) {
	for _, t := range dbTypes {
		providerRegistry[NormalizeDBType(t)] = factory
	}
}

// NewProvider returns a provider for the given database type, or (nil, false)
// if no provider is registered for it. This is the single dispatch point that
// replaced the old two-case switch in the CDC handler.
func NewProvider(dbType string, db *sql.DB) (CDCSourceProvider, bool) {
	factory, ok := providerRegistry[NormalizeDBType(dbType)]
	if !ok {
		return nil, false
	}
	return factory(db), true
}

// IsSupportedDBType reports whether a CDC provider is registered for dbType.
func IsSupportedDBType(dbType string) bool {
	_, ok := providerRegistry[NormalizeDBType(dbType)]
	return ok
}

// RegisteredDBTypes returns the sorted set of database types that have a
// registered CDC provider. Cleanup uses this to run teardown for every known
// family regardless of what cdc_resources holds (each CleanupResources is
// idempotent/best-effort).
func RegisteredDBTypes() []string {
	out := make([]string, 0, len(providerRegistry))
	for k := range providerRegistry {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// NormalizeDBType folds database-type aliases to their canonical key. Kept in
// lockstep with the aliases used elsewhere (handler pipelineDestination check,
// planner CONNECTOR_CLASSES): postgres→postgresql, mssql→sqlserver.
func NormalizeDBType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	switch t {
	case "postgres":
		return "postgresql"
	case "mssql":
		return "sqlserver"
	case "oracledb":
		return "oracle"
	}
	return t
}
