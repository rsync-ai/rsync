package executor

import (
	"context"
	"database/sql"
	"strings"

	"github.com/rsync-ai/backend-orchestrator/pkg/namespacemodel"
)

// A server-level source is a MySQL/MongoDB/ClickHouse connection that names no
// database: its tables span every database in the connection's Scope, so one
// pipeline can carry shop.users and crm.users. Destination mapping for it is
// MIRROR — each source database lands in a same-named database (MongoDB, MySQL,
// ClickHouse) or schema (PostgreSQL) at the destination — unless the user typed
// a destination namespace, which sends everything to that one place.

// configuredDatabase is the database a connection names, read from the same
// keys the connectors read.
func configuredDatabase(cfg map[string]string) string {
	for _, k := range []string{"database", "db_name", "db"} {
		if v := strings.TrimSpace(cfg[k]); v != "" {
			return v
		}
	}
	return ""
}

// serverLevelSource reports whether a connection's tables live in databases
// (namespace_model table_namespace "database") and it names none.
func serverLevelSource(connectorType string, cfg map[string]string) bool {
	return tableNamespaceIsDatabase(connectorType) && configuredDatabase(cfg) == ""
}

// deliberateNamespace reports whether ns is a destination namespace the user
// chose, as opposed to one pipeline creation filled in (api-gateway
// seedDestinationNamespace): empty or "default", an engine's default schema
// (public, dbo), the destination's namespace_model destination_default, or the
// source connector type's own name (a mongodb source seeds "mongodb").
func deliberateNamespace(ns, sourceType, destType string) bool {
	ns = strings.TrimSpace(ns)
	if !isRealNamespace(ns) || isEngineDefaultNamespace(ns) {
		return false
	}
	if d := namespacemodel.For(destType).DestinationDefault; d != "" && strings.EqualFold(ns, d) {
		return false
	}
	return !strings.EqualFold(ns, connectorTypeSlug(sourceType))
}

// connectorTypeSlug is the name seedDestinationNamespace derives from a source
// connector type: lower case, with '-', '.' and ' ' as '_'.
func connectorTypeSlug(connectorType string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(connectorType)) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_':
			b.WriteRune(r)
		case r == '-' || r == '.' || r == ' ':
			b.WriteRune('_')
		}
	}
	return b.String()
}

// SchemaModeOverride reads a pipeline's explicit layout choice,
// pipelines.config.destination_schema_mode (written by the table-selection
// picker): "preserve" (also for "mirror"), "flatten", or "" when it is unset,
// unknown or unreadable.
func SchemaModeOverride(ctx context.Context, db *sql.DB, pipelineID string) string {
	if db == nil || strings.TrimSpace(pipelineID) == "" {
		return ""
	}
	var mode sql.NullString
	_ = db.QueryRowContext(ctx,
		`SELECT NULLIF(TRIM(LOWER(COALESCE(config->>'destination_schema_mode',''))),'') FROM pipelines WHERE id = $1`,
		pipelineID,
	).Scan(&mode)
	switch mode.String {
	case "preserve", "mirror":
		return "preserve"
	case "flatten":
		return "flatten"
	}
	return ""
}

// MirrorSourceNamespaces reports whether a pipeline mirrors its source
// databases at the destination. Only a server-level source mirrors; the
// pipeline's explicit schema mode (SchemaModeOverride) decides first, else it
// mirrors unless the user typed a destination namespace. The CDC sink's
// mirror_source_namespace flag and the batch path's preserve layout both follow
// it, so a batch run and its CDC sink write the same places.
func MirrorSourceNamespaces(sourceType string, sourceCfg map[string]string, destType, destinationNamespace, schemaMode string) bool {
	if !serverLevelSource(sourceType, sourceCfg) {
		return false
	}
	switch schemaMode {
	case "preserve":
		return true
	case "flatten":
		return false
	}
	return !deliberateNamespace(destinationNamespace, sourceType, destType)
}

// mirrorSourceNamespacesFor is MirrorSourceNamespaces for a task.
func (a *Agent) mirrorSourceNamespacesFor(ctx context.Context, task ExecutorTask, destinationNamespace string) bool {
	if task.Source == nil || task.Destination == nil {
		return false
	}
	return MirrorSourceNamespaces(task.Source.Type, task.Source.Config, task.Destination.Type, destinationNamespace,
		SchemaModeOverride(ctx, a.db, task.PipelineID))
}
