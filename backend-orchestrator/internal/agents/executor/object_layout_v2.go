package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/storage"
)

// Object-storage layout v2 (gcs, aws-s3, azure-blob): which pipelines write it, and the
// per-table object_layout block the sink builds v2 keys from
// (kafka-sink-worker object_layout_v2_write.go).
//
// pipelines.storage_layout_version (migrations 106 and 108):
//
//	0  undecided: the pipeline never started a sink. Decided once, at the first
//	   sink start (startKafkaMCPSink), and stored as 1 or 2.
//	1  layout v1, <conn prefix>/<pipeline id slug>/... Every pipeline that wrote
//	   files before layout v2 existed stays here.
//	2  layout v2, <conn prefix>/<pipeline prefix>/<db>/[<schema>/]<table>/...
//
// A pipeline moves 1 → 2 only on a batch reload, which cleans and rewrites every
// table. Nothing moves 2 → 1: a v2 pipeline that stops being eligible fails
// instead of writing v1 keys next to its v2 folders.
//
// The pipeline prefix is the pipeline's destination namespace (the "Path prefix"
// the user types for a v2 destination). A pipeline without a valid one stays on
// v1, as do sources outside the families below.

type objectLayoutMode int

const (
	// objectLayoutDecide runs at the sink start: 0 → 1 or 2, 1 stays, 2 is checked.
	objectLayoutDecide objectLayoutMode = iota
	// objectLayoutReload runs before a batch reload starts its sink: 1 → 2 when
	// eligible. 0 is left for the sink start to decide.
	objectLayoutReload
	// objectLayoutRead never writes: 0 and 1 read as v1, 2 is checked.
	objectLayoutRead
	// objectLayoutRestart is the CDC sink restart handler: 0 → 1 (a restart
	// writes files, so the layout is fixed from here), never → 2.
	objectLayoutRestart
)

// ObjectLayout is a pipeline's resolved object-storage layout. Version 1 carries
// no other field.
type ObjectLayout struct {
	Version        int
	PipelinePrefix string
	SourceFamily   string
	SourceDatabase string
}

// ObjectLayoutInput is what the layout decision reads besides the pipelines row.
type ObjectLayoutInput struct {
	PipelineID   string
	SourceType   string
	SourceConfig map[string]string
	DestType     string
	// Namespace is the resolved destination namespace, used as the pipeline prefix.
	Namespace string
	// DestConnID is used when pipelines.destination_connection_id is NULL.
	DestConnID string
}

// objectLayoutV2DestSupported matches the sink's gate (objectLayoutV2Destination): v2
// is built for gcs, aws-s3 and azure-blob, the stores that stamp rsync_* object
// metadata and implement get_cdc_offsets. minio and every other type stay v1. Pinned
// by v2_destinations in shared/object_layout_golden.json.
func objectLayoutV2DestSupported(destType string) bool {
	switch strings.ReplaceAll(strings.ToLower(strings.TrimSpace(destType)), "_", "-") {
	case "gcs", "aws-s3", "azure-blob":
		return true
	}
	return false
}

// objectLayoutV2SourceFamily maps a source connector type to a layout v2 source
// family. "" = not a known database family; such a pipeline stays on v1.
func objectLayoutV2SourceFamily(srcType string) string {
	if isPostgresFamily(srcType) {
		return "postgresql"
	}
	if isMongoSourceFamily(srcType) {
		return "mongodb"
	}
	switch normalizeDBType(srcType) {
	case "mysql", "aurora_mysql":
		return "mysql"
	case "sqlserver", "mssql", "sql_server":
		return "sqlserver"
	case "oracle":
		return "oracle"
	}
	return ""
}

func firstConfigValue(cfg map[string]string, keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(cfg[k]); v != "" {
			return v
		}
	}
	return ""
}

// objectLayoutV2SourceDatabase is the source database the <db> folder is named
// after. The keys mirror the source connectors' own lookups.
func objectLayoutV2SourceDatabase(family string, cfg map[string]string) string {
	switch family {
	case "mongodb":
		return firstConfigValue(cfg, "database", "db_name", "db")
	case "oracle":
		return firstConfigValue(cfg, "database", "service_name", "sid")
	}
	return firstConfigValue(cfg, "database")
}

// objectLayoutV2Eligibility returns the v2 layout a pipeline would get, or the
// reason it stays on v1.
func objectLayoutV2Eligibility(in ObjectLayoutInput) (ObjectLayout, string) {
	if !objectLayoutV2DestSupported(in.DestType) {
		return ObjectLayout{Version: 1}, "destination_not_v2"
	}
	family := objectLayoutV2SourceFamily(in.SourceType)
	if family == "" {
		return ObjectLayout{Version: 1}, "source_family_unsupported"
	}
	prefix := strings.TrimSpace(in.Namespace)
	if _, err := storage.LayoutV2PipelineRootPrefix("", prefix); err != nil {
		return ObjectLayout{Version: 1}, "pipeline_prefix_invalid"
	}
	srcDB := objectLayoutV2SourceDatabase(family, in.SourceConfig)
	if srcDB == "" {
		return ObjectLayout{Version: 1}, "source_database_required"
	}
	return ObjectLayout{Version: 2, PipelinePrefix: prefix, SourceFamily: family, SourceDatabase: srcDB}, ""
}

// resolveObjectLayout reads, and in the decide/reload/restart modes records, the
// pipeline's layout version. A non-v2 destination always reads v1 and touches
// nothing. A database error fails closed: the caller must not start a sink whose
// key layout it could not establish.
func resolveObjectLayout(ctx context.Context, db *sql.DB, in ObjectLayoutInput, mode objectLayoutMode) (ObjectLayout, error) {
	v1 := ObjectLayout{Version: 1}
	if db == nil || strings.TrimSpace(in.PipelineID) == "" || !objectLayoutV2DestSupported(in.DestType) {
		return v1, nil
	}
	var current int
	if err := db.QueryRowContext(ctx,
		`SELECT storage_layout_version FROM pipelines WHERE id = $1::uuid`, in.PipelineID,
	).Scan(&current); err != nil {
		return v1, fmt.Errorf("object layout: read storage_layout_version: %w", err)
	}
	want, reason := objectLayoutV2Eligibility(in)
	switch current {
	case 2:
		if want.Version != 2 {
			return v1, fmt.Errorf("object layout: pipeline %s writes layout v2 but is no longer eligible (%s); "+
				"it will not fall back to v1 keys next to its v2 folders", in.PipelineID, reason)
		}
		return want, nil
	case 1:
		if mode != objectLayoutReload || want.Version != 2 {
			return v1, nil
		}
	case 0:
		switch mode {
		case objectLayoutRead, objectLayoutReload:
			return v1, nil
		case objectLayoutRestart:
			want, reason = v1, "sink_restart"
		}
	default:
		return v1, fmt.Errorf("object layout: pipeline %s has unknown storage_layout_version %d", in.PipelineID, current)
	}
	return recordObjectLayout(ctx, db, in, current, want, reason)
}

// recordObjectLayout stores want.Version for a pipeline still at `from`. A v2
// decision is refused (recorded as v1) when another v2 pipeline on the same
// destination connection already writes under the same prefix: both would clean
// and number the same table folders. The check and the write run under a
// transaction-scoped advisory lock on the destination connection, so two
// pipelines deciding at once cannot both win.
func recordObjectLayout(ctx context.Context, db *sql.DB, in ObjectLayoutInput, from int, want ObjectLayout, reason string) (ObjectLayout, error) {
	v1 := ObjectLayout{Version: 1}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return v1, fmt.Errorf("object layout: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var destConnID string
	if err := tx.QueryRowContext(ctx,
		`SELECT COALESCE(destination_connection_id::text, '') FROM pipelines WHERE id = $1::uuid`, in.PipelineID,
	).Scan(&destConnID); err != nil {
		return v1, fmt.Errorf("object layout: read destination_connection_id: %w", err)
	}
	if destConnID == "" {
		destConnID = strings.TrimSpace(in.DestConnID)
	}
	if want.Version == 2 && destConnID == "" {
		want, reason = v1, "destination_connection_unknown"
	}
	if destConnID != "" {
		if _, err := tx.ExecContext(ctx,
			`SELECT pg_advisory_xact_lock(hashtext('object_layout_v2:' || $1))`, destConnID,
		); err != nil {
			return v1, fmt.Errorf("object layout: lock: %w", err)
		}
	}
	if want.Version == 2 {
		var taken bool
		if err := tx.QueryRowContext(ctx, `SELECT EXISTS (
				SELECT 1 FROM pipelines
				WHERE id <> $1::uuid
				  AND destination_connection_id::text = $2
				  AND storage_layout_version = 2
				  AND NULLIF(TRIM(COALESCE(config->>'destination_namespace', '')), '') = $3)`,
			in.PipelineID, destConnID, want.PipelinePrefix,
		).Scan(&taken); err != nil {
			return v1, fmt.Errorf("object layout: check prefix owner: %w", err)
		}
		if taken {
			want, reason = v1, "pipeline_prefix_in_use"
		}
	}
	res, err := tx.ExecContext(ctx,
		`UPDATE pipelines SET storage_layout_version = $2 WHERE id = $1::uuid AND storage_layout_version = $3`,
		in.PipelineID, want.Version, from)
	if err != nil {
		return v1, fmt.Errorf("object layout: record version: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		// Another run recorded a version first; its value wins.
		_ = tx.Rollback()
		return resolveObjectLayout(ctx, db, in, objectLayoutRead)
	}
	if err := tx.Commit(); err != nil {
		return v1, fmt.Errorf("object layout: commit: %w", err)
	}
	fields := log.Fields{
		"pipeline_id":     in.PipelineID,
		"from_version":    from,
		"layout_version":  want.Version,
		"pipeline_prefix": want.PipelinePrefix,
	}
	if want.Version == 2 {
		log.WithFields(fields).Info("object layout: pipeline writes layout v2")
	} else {
		fields["reason"] = reason
		log.WithFields(fields).Warn("object layout: pipeline stays on layout v1")
	}
	return want, nil
}

// ObjectLayoutAppliesTo reports whether a destination type has a layout decision
// at all (gcs, aws-s3, azure-blob). Every other destination is v1 without a database
// read.
func ObjectLayoutAppliesTo(destType string) bool {
	return objectLayoutV2DestSupported(destType)
}

// ResolveObjectLayoutForRestart is the CDC sink restart handler's entry point
// (handlers/cdc_sink.go): it never moves a pipeline to v2.
func ResolveObjectLayoutForRestart(ctx context.Context, db *sql.DB, in ObjectLayoutInput) (ObjectLayout, error) {
	return resolveObjectLayout(ctx, db, in, objectLayoutRestart)
}

// objectLayoutInputFor builds the decision input from an executor task.
func objectLayoutInputFor(task ExecutorTask, namespace string) ObjectLayoutInput {
	in := ObjectLayoutInput{PipelineID: task.PipelineID, Namespace: namespace, DestConnID: hybridDestConnID(task)}
	if task.Source != nil {
		in.SourceType, in.SourceConfig = task.Source.Type, task.Source.Config
	}
	if task.Destination != nil {
		in.DestType = task.Destination.Type
	}
	return in
}

// hybridDestConnID extracts the destination connection id from params/payload.
func hybridDestConnID(task ExecutorTask) string {
	for _, m := range []map[string]interface{}{task.Params, task.Payload} {
		if v, ok := m["destination_connection_id"].(string); ok {
			if v = strings.TrimSpace(v); v != "" && v != "auto" {
				return v
			}
		}
	}
	return ""
}

// SinkConfigFields are the start_sink config keys the sink reads the layout from
// (WorkerConfig storage_layout_version / source_family / source_database).
func (l ObjectLayout) SinkConfigFields() map[string]interface{} {
	if l.Version != 2 {
		return map[string]interface{}{"storage_layout_version": 1, "source_family": "", "source_database": ""}
	}
	return map[string]interface{}{
		"storage_layout_version": 2,
		"source_family":          l.SourceFamily,
		"source_database":        l.SourceDatabase,
	}
}

// objectLayoutV2DefaultSchema is the schema a bare source table lives in.
func objectLayoutV2DefaultSchema(family string, cfg map[string]string) string {
	switch family {
	case "postgresql":
		return "public"
	case "sqlserver":
		return "dbo"
	case "oracle":
		return strings.ToUpper(firstConfigValue(cfg, "username", "user"))
	}
	return ""
}

// objectLayoutV2TableFor splits a selected source table name into the layout v2
// database / schema / table for its family.
//
//	postgresql, sqlserver, oracle  [<db>.]<schema>.<table>, or a bare table in the
//	                               family's default schema; <db> is the source database.
//	mongodb                        <db>.<collection> or <collection>. A collection name
//	                               may itself hold dots (fs.files), so only the source
//	                               database's own prefix is stripped.
//	mysql                          <db>.<table> or <table>.
func objectLayoutV2TableFor(l ObjectLayout, cfg map[string]string, tableName string) storage.LayoutV2Table {
	t := storage.LayoutV2Table{
		PipelinePrefix: l.PipelinePrefix,
		SourceFamily:   l.SourceFamily,
		Database:       l.SourceDatabase,
	}
	name := strings.TrimSpace(tableName)
	switch l.SourceFamily {
	case "postgresql", "sqlserver", "oracle":
		t.Schema, t.Table = storage.ExtractSchemaAndTable(name)
		if strings.TrimSpace(t.Schema) == "" {
			t.Schema = objectLayoutV2DefaultSchema(l.SourceFamily, cfg)
		}
	case "mongodb":
		t.Table = name
		if l.SourceDatabase != "" && strings.HasPrefix(name, l.SourceDatabase+".") {
			t.Table = strings.TrimPrefix(name, l.SourceDatabase+".")
		}
	case "mysql":
		t.Table = name
		if i := strings.Index(name, "."); i > 0 && i+1 < len(name) {
			t.Database, t.Table = name[:i], name[i+1:]
		}
	}
	return t
}

// objectLayoutV2Message is the object_layout block of one table's batch and EOF
// messages (sink parseObjectLayoutV2Msg). It returns nil for a v1 pipeline, and an
// error when the table cannot be named in layout v2: the table must fail rather
// than write under a v1 key.
func objectLayoutV2Message(l ObjectLayout, cfg map[string]string, tableName, dt string) (map[string]interface{}, error) {
	if l.Version != 2 {
		return nil, nil
	}
	t := objectLayoutV2TableFor(l, cfg, tableName)
	// A load key checks the table prefix and dt together, the two things the sink
	// would otherwise reject after the rows are already on the topic.
	if _, err := storage.LayoutV2LoadKey(t, dt, 1); err != nil {
		var le *storage.LayoutV2Error
		if errors.As(err, &le) {
			return nil, fmt.Errorf("object layout v2: table %q: %s", tableName, le.Code)
		}
		return nil, fmt.Errorf("object layout v2: table %q: %w", tableName, err)
	}
	return map[string]interface{}{
		"version":         2,
		"pipeline_prefix": t.PipelinePrefix,
		"source_family":   t.SourceFamily,
		"database":        t.Database,
		"schema":          t.Schema,
		"table":           t.Table,
		"dt":              dt,
	}, nil
}
