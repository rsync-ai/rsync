package handlers

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"api-gateway/internal/db"
	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// The captured inserts and snapshot rows of a pipeline_run_table_stats row, as
// readers show them. A CDC row counted before migration 114 still holds its
// snapshot reads inside `inserts`; migration 115 recorded how many in
// legacy_snapshot_reads without lowering `inserts` (the stats consumer seeds from
// it and would write the old value back). Every read of the captured inserts goes
// through capturedInsertsSQL, so those reads show as snapshot rows, not inserts.
// NULL legacy_snapshot_reads (every other row) leaves both columns as stored;
// migration 115 sets snapshot_rows wherever it sets legacy_snapshot_reads.
const (
	capturedInsertsSQL  = "(inserts - COALESCE(legacy_snapshot_reads, 0))"
	capturedSnapshotSQL = "(snapshot_rows + COALESCE(legacy_snapshot_reads, 0))"
)

// TableStat represents a single table's statistics
type TableStat struct {
	// schema_name can be NULL in DB (e.g. if the source doesn't have schemas).
	SchemaName    string `json:"schema_name,omitempty"`
	TableName     string `json:"table_name"`
	QualifiedName string `json:"qualified_name"`
	Mode          string `json:"mode"` // "batch" | "cdc"
	Status        string `json:"status"`

	// Where the rows LANDED. schema_name/qualified_name above are the SOURCE-side
	// name for CDC — the sink derives them from the Debezium envelope — so a
	// MySQL->Postgres pipeline reports the MySQL database as its schema and a reader
	// asking "where did my data go?" gets the wrong half of the pipeline. These two
	// are added alongside rather than replacing them, because qualified_name is the
	// key two independent producers upsert the stats row on (migration 089).
	//
	// nil means the emitter could not name one: an object-storage destination (no
	// schema to name), a pipeline with no configured destination namespace, or a sink
	// predating the change. Deliberately distinguishable from an empty string.
	DestinationSchema        *string `json:"destination_schema,omitempty"`
	DestinationQualifiedName *string `json:"destination_qualified_name,omitempty"`

	// The execution id the orchestrator minted for this run. Differs from the run's
	// execution_id only on the CDC lane, where execution_id is forced to pipeline_id so
	// the captured-side and applied-side counters share one row — which leaves the id
	// every sink log line carries appearing nowhere in the stats. This is that id, and
	// it is what makes a CDC log line joinable to the numbers it produced.
	//
	// nil on batch (execution_id already IS that id) and on rows written before
	// migration 090.
	OrchestrationExecutionID *string `json:"orchestration_execution_id,omitempty"`

	// Batch fields
	ReadRows     *int64 `json:"read_rows,omitempty"`
	InsertedRows *int64 `json:"inserted_rows,omitempty"`

	// CDC fields
	Inserts     *int64     `json:"inserts,omitempty"`
	Updates     *int64     `json:"updates,omitempty"`
	Deletes     *int64     `json:"deletes,omitempty"`
	TotalEvents *int64     `json:"total_events,omitempty"`
	LastEventTs *time.Time `json:"last_event_ts,omitempty"`

	// CDC applied fields (destination-truth)
	AppliedInserts     *int64     `json:"applied_inserts,omitempty"`
	AppliedUpdates     *int64     `json:"applied_updates,omitempty"`
	AppliedDeletes     *int64     `json:"applied_deletes,omitempty"`
	AppliedTotalEvents *int64     `json:"applied_total_events,omitempty"`
	LastAppliedTs      *time.Time `json:"last_applied_ts,omitempty"`

	// CDC initial-load rows (Debezium op "r"), counted apart from inserts (migration
	// 114): captured by the stats consumer, applied by the sink. nil means the producer
	// does not count them yet, which is not a measured zero.
	SnapshotRows        *int64 `json:"snapshot_rows,omitempty"`
	AppliedSnapshotRows *int64 `json:"applied_snapshot_rows,omitempty"`
	// LoadMode is "streaming_only" for a CDC table that was added without loading its
	// existing rows (pipelines.config->'cdc_streaming_only_tables'); absent otherwise.
	LoadMode string `json:"load_mode,omitempty"`

	// DLQRows counts records the destination will never receive — parked in the sink's
	// dead-letter queue after exhausting retries, offsets committed, worker continued.
	// Not omitempty: a zero here is the meaningful statement "nothing was lost", and
	// omitting it makes an old sink (which never reports it) indistinguishable from a
	// clean run. Every other counter is "what landed"; this is the only one that says
	// what did not.
	DLQRows int64 `json:"dlq_rows"`

	// Timestamps
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// TableStatsSummary provides aggregate metrics across all tables
type TableStatsSummary struct {
	Mode            string `json:"mode"` // "batch" | "cdc" | "mixed"
	TotalTables     int    `json:"total_tables"`
	TablesCompleted int    `json:"tables_completed"`
	TablesFailed    int    `json:"tables_failed"`
	TablesRunning   int    `json:"tables_running"`
	TablesDegraded  int    `json:"tables_degraded"`
	// TablesWaitingForData counts selected CDC tables with nothing captured or
	// applied yet. They are not in TablesRunning.
	TablesWaitingForData int `json:"tables_waiting_for_data"`
	// TablesRemoved counts CDC stats rows whose table is no longer selected (status
	// "removed"). They are NOT in TotalTables, but their counters stay in every total
	// below — the rows they moved did move.
	TablesRemoved int `json:"tables_removed"`

	// Batch aggregates
	TotalReadRows     *int64 `json:"total_read_rows,omitempty"`
	TotalInsertedRows *int64 `json:"total_inserted_rows,omitempty"`

	// CDC aggregates
	TotalInserts   *int64 `json:"total_inserts,omitempty"`
	TotalUpdates   *int64 `json:"total_updates,omitempty"`
	TotalDeletes   *int64 `json:"total_deletes,omitempty"`
	TotalCDCEvents *int64 `json:"total_cdc_events,omitempty"`

	// CDC applied aggregates
	TotalAppliedInserts   *int64 `json:"total_applied_inserts,omitempty"`
	TotalAppliedUpdates   *int64 `json:"total_applied_updates,omitempty"`
	TotalAppliedDeletes   *int64 `json:"total_applied_deletes,omitempty"`
	TotalAppliedCDCEvents *int64 `json:"total_applied_cdc_events,omitempty"`

	// CDC initial-load aggregates; nil when no table reports the count.
	TotalSnapshotRows        *int64 `json:"total_snapshot_rows,omitempty"`
	TotalAppliedSnapshotRows *int64 `json:"total_applied_snapshot_rows,omitempty"`

	// TotalDLQRows is the pipeline-wide count of records parked in the DLQ, and
	// TablesWithDLQ how many tables shed at least one. Both modes.
	TotalDLQRows  int64 `json:"total_dlq_rows"`
	TablesWithDLQ int   `json:"tables_with_dlq"`
}

// GetPipelineTableStats returns DMS-like per-table statistics for a pipeline execution.
// GET /api/v1/pipelines/:id/table-stats?execution_id=&mode=&q=&limit=&offset=&sort=&export=csv
func GetPipelineTableStats(c *gin.Context) {
	database := db.GetDB()
	if database == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database not connected"})
		return
	}

	pipelineID := c.Param("id")
	if _, err := uuid.Parse(pipelineID); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Pipeline not found"})
		return
	}

	// RBAC: read gate scoped to the caller's ACTIVE workspace. Per-table row counts
	// and schema/table names are tenant data — membership in the owning workspace is
	// not enough while a different workspace is active.
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	// Parse filters
	executionID := strings.TrimSpace(c.Query("execution_id"))
	modeFilter := strings.TrimSpace(c.Query("mode")) // "batch", "cdc", or empty (all)
	search := strings.TrimSpace(c.Query("q"))
	sortBy := strings.TrimSpace(c.DefaultQuery("sort", "qualified_name")) // qualified_name, inserted_rows, status, updated_at
	exportFormat := strings.TrimSpace(c.Query("export"))

	// CDC table stats are stored under a stable execution key (execution_id == pipeline_id)
	// so UIs can always query the latest streaming counters, even when the current run has a
	// separate Temporal execution/workflow ID.
	if modeFilter == "cdc" {
		executionID = pipelineID
	}

	// A caller that names a CDC run by its own id — the Execution Details page does
	// — matched no row, because the run's stats live under the pipeline key above,
	// and the page said "No table statistics" beside a pipeline showing 83,230 rows
	// (UI #40). When the run has no rows of its own, read the stream key instead.
	if modeFilter == "" && executionID != "" && executionID != pipelineID &&
		cdcRunStatsAreUnderPipelineKey(database, pipelineID, executionID) {
		modeFilter = "cdc"
		executionID = pipelineID
	}

	limit := 50
	if s := c.Query("limit"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v > 0 && v <= 1000 {
			limit = v
		}
	}

	offset := 0
	if s := c.Query("offset"); s != "" {
		if v, err := strconv.Atoi(s); err == nil && v >= 0 {
			offset = v
		}
	}

	// Special-case CDC mode: always include selected tables even if they have 0 events yet.
	// Without this, newly added tables won't show up until the first CDC event is observed
	// (because the stats row is created lazily by the CDC stats projector).
	if modeFilter == "cdc" {
		selected := getPipelineSelectedTables(database, pipelineID)
		if len(selected) > 0 {
			streamingOnly := getPipelineCDCStreamingOnlyTables(database, pipelineID)
			cdcTables, cdcSummary, cdcTotal, err := buildCDCTableStatsResponse(database, pipelineID, executionID, selected, streamingOnly, search, sortBy, limit, offset, exportFormat)
			if err != nil {
				log.WithError(err).Error("Failed to build CDC table stats response")
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch table stats"})
				return
			}

			// CSV export
			if exportFormat == "csv" {
				exportTableStatsCSV(c, cdcTables, cdcSummary)
				return
			}

			c.JSON(http.StatusOK, gin.H{
				"pipeline_id":  pipelineID,
				"execution_id": executionID,
				"summary":      cdcSummary,
				"tables":       cdcTables,
				"total":        cdcTotal,
				"limit":        limit,
				"offset":       offset,
			})
			return
		}
	}

	// Build query
	args := []interface{}{pipelineID}
	argIdx := 2

	query := `
		SELECT
			schema_name, table_name, qualified_name, mode, status,
			read_rows, inserted_rows,
			` + capturedInsertsSQL + ` AS inserts, updates, deletes,
			(COALESCE(` + capturedInsertsSQL + `, 0) + COALESCE(updates, 0) + COALESCE(deletes, 0)) AS total_events,
			last_event_ts,
			applied_inserts, applied_updates, applied_deletes,
			(COALESCE(applied_inserts, 0) + COALESCE(applied_updates, 0) + COALESCE(applied_deletes, 0)) AS applied_total_events,
			last_applied_ts,
			COALESCE(dlq_rows, 0) AS dlq_rows,
			destination_schema, destination_qualified_name, orchestration_execution_id,
			started_at, completed_at, updated_at
		FROM pipeline_run_table_stats
		WHERE pipeline_id = $1
	`

	if executionID != "" {
		query += fmt.Sprintf(" AND execution_id = $%d::uuid", argIdx)
		args = append(args, executionID)
		argIdx++
	}

	if modeFilter != "" && (modeFilter == "batch" || modeFilter == "cdc") {
		query += fmt.Sprintf(" AND mode = $%d", argIdx)
		args = append(args, modeFilter)
		argIdx++
	}

	if search != "" {
		query += fmt.Sprintf(" AND (qualified_name ILIKE $%d OR table_name ILIKE $%d)", argIdx, argIdx)
		args = append(args, "%"+search+"%")
		argIdx++
	}

	// Determine sort order
	var orderClause string
	switch sortBy {
	case "inserted_rows":
		orderClause = "ORDER BY inserted_rows DESC NULLS LAST"
	case "inserts":
		orderClause = "ORDER BY " + capturedInsertsSQL + " DESC NULLS LAST"
	case "status":
		orderClause = "ORDER BY CASE status WHEN 'failed' THEN 1 WHEN 'degraded' THEN 2 WHEN 'running' THEN 3 ELSE 4 END, qualified_name"
	case "updated_at":
		orderClause = "ORDER BY updated_at DESC"
	default:
		orderClause = "ORDER BY qualified_name"
	}

	// Count total (for pagination)
	// NOTE: do not attempt fragile string-replacements on the SELECT query.
	// Build a proper COUNT(*) query with the same filters.
	countArgs := make([]interface{}, 0, len(args))
	countArgs = append(countArgs, args...)
	countQuery := `
		SELECT COUNT(*)
		FROM pipeline_run_table_stats
		WHERE pipeline_id = $1
	`
	if executionID != "" {
		// execution_id is always the second arg when provided (because pipeline_id is $1).
		countQuery += " AND execution_id = $2::uuid"
	}
	// If modeFilter/search are present, append them using the same placeholder indices as the main query.
	// This works because countArgs is identical to args (before LIMIT/OFFSET are appended).
	if modeFilter != "" && (modeFilter == "batch" || modeFilter == "cdc") {
		// Find the placeholder index for modeFilter in the original args.
		// It is always the last arg added before `search` (if any).
		// We simply rebuild the same filter fragment by scanning args length.
		// Safer approach: re-append the filter using argIdx arithmetic, but argIdx has moved.
		// Here we reuse the already-built WHERE fragments by checking which filters were applied:
		//
		// - If executionID is set, modeFilter is $3; else $2
		modeIdx := 2
		if executionID != "" {
			modeIdx = 3
		}
		countQuery += fmt.Sprintf(" AND mode = $%d", modeIdx)
	}
	if search != "" {
		// - If executionID is set and modeFilter is set, search is $4; else it shifts accordingly.
		searchIdx := 2
		if executionID != "" {
			searchIdx++
		}
		if modeFilter != "" && (modeFilter == "batch" || modeFilter == "cdc") {
			searchIdx++
		}
		countQuery += fmt.Sprintf(" AND (qualified_name ILIKE $%d OR table_name ILIKE $%d)", searchIdx, searchIdx)
	}

	var total int
	if err := database.QueryRow(countQuery, countArgs...).Scan(&total); err != nil && err != sql.ErrNoRows {
		log.WithError(err).Error("Failed to count table stats")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to count table stats"})
		return
	}

	// Fetch paginated results
	query += fmt.Sprintf(" %s LIMIT $%d OFFSET $%d", orderClause, argIdx, argIdx+1)
	args = append(args, limit, offset)

	rows, err := database.Query(query, args...)
	if err != nil {
		log.WithError(err).Error("Failed to query table stats")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to fetch table stats"})
		return
	}
	defer rows.Close()

	tables := make([]TableStat, 0)
	for rows.Next() {
		var stat TableStat
		var schemaName sql.NullString
		var tableName, qualifiedName, mode, status string
		var readRows, insertedRows sql.NullInt64
		var inserts, updates, deletes, totalEvents sql.NullInt64
		var appliedInserts, appliedUpdates, appliedDeletes, appliedTotalEvents sql.NullInt64
		var lastEventTs, lastAppliedTs, startedAt, completedAt sql.NullTime
		var dlqRows int64
		var destSchema, destQualifiedName, orchExecutionID sql.NullString
		var updatedAt time.Time

		if err := rows.Scan(
			&schemaName, &tableName, &qualifiedName, &mode, &status,
			&readRows, &insertedRows,
			&inserts, &updates, &deletes, &totalEvents, &lastEventTs,
			&appliedInserts, &appliedUpdates, &appliedDeletes, &appliedTotalEvents, &lastAppliedTs,
			&dlqRows,
			&destSchema, &destQualifiedName, &orchExecutionID,
			&startedAt, &completedAt, &updatedAt,
		); err != nil {
			log.WithError(err).Warn("Failed to scan table stat row")
			continue
		}

		if schemaName.Valid {
			stat.SchemaName = schemaName.String
		}
		if destSchema.Valid {
			v := destSchema.String
			stat.DestinationSchema = &v
		}
		if destQualifiedName.Valid {
			v := destQualifiedName.String
			stat.DestinationQualifiedName = &v
		}
		if orchExecutionID.Valid {
			v := orchExecutionID.String
			stat.OrchestrationExecutionID = &v
		}
		stat.TableName = tableName
		stat.QualifiedName = qualifiedName
		stat.Mode = mode
		// Normalize: older runs may have persisted status='degraded' even when read==written.
		// Treat as completed for UI if batch counts match — but NEVER when rows were shed
		// to the DLQ. A DLQ'd row is never counted as read, so read==written is exactly the
		// reconciliation that loss slips past; promoting on it would re-hide the loss.
		if mode == "batch" && status == "degraded" && dlqRows == 0 &&
			readRows.Valid && insertedRows.Valid && readRows.Int64 == insertedRows.Int64 {
			stat.Status = "completed"
		} else {
			stat.Status = status
		}
		stat.DLQRows = dlqRows
		stat.UpdatedAt = updatedAt

		if readRows.Valid {
			v := readRows.Int64
			stat.ReadRows = &v
		}
		if insertedRows.Valid {
			v := insertedRows.Int64
			stat.InsertedRows = &v
		}
		if inserts.Valid {
			v := inserts.Int64
			stat.Inserts = &v
		}
		if updates.Valid {
			v := updates.Int64
			stat.Updates = &v
		}
		if deletes.Valid {
			v := deletes.Int64
			stat.Deletes = &v
		}
		if totalEvents.Valid {
			v := totalEvents.Int64
			stat.TotalEvents = &v
		}
		if lastEventTs.Valid {
			stat.LastEventTs = &lastEventTs.Time
		}
		if appliedInserts.Valid {
			v := appliedInserts.Int64
			stat.AppliedInserts = &v
		}
		if appliedUpdates.Valid {
			v := appliedUpdates.Int64
			stat.AppliedUpdates = &v
		}
		if appliedDeletes.Valid {
			v := appliedDeletes.Int64
			stat.AppliedDeletes = &v
		}
		if appliedTotalEvents.Valid {
			v := appliedTotalEvents.Int64
			stat.AppliedTotalEvents = &v
		}
		if lastAppliedTs.Valid {
			stat.LastAppliedTs = &lastAppliedTs.Time
		}
		if startedAt.Valid {
			stat.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			stat.CompletedAt = &completedAt.Time
		}

		tables = append(tables, stat)
	}

	// Compute summary
	summary := computeTableStatsSummary(database, pipelineID, executionID)

	// CSV export
	if exportFormat == "csv" {
		exportTableStatsCSV(c, tables, summary)
		return
	}

	resp := gin.H{
		"pipeline_id":  pipelineID,
		"execution_id": executionID,
		"summary":      summary,
		"tables":       tables,
		"total":        total,
		"limit":        limit,
		"offset":       offset,
	}
	// A batch run that found nothing new (a Resume with no source change) lists every
	// table at 0 read / 0 written. Name the last run that did move rows, so the page
	// can show what the pipeline moved instead of a grid of zeros.
	if executionID != "" && summary.Mode == "batch" {
		if run := lastBatchRunThatMovedRows(database, pipelineID); run != nil {
			resp["last_data_run"] = run
		}
	}
	c.JSON(http.StatusOK, resp)
}

// LastDataRun is the most recent batch execution of a pipeline whose table stats
// read or wrote at least one row.
type LastDataRun struct {
	ExecutionID string     `json:"execution_id"`
	Tables      int        `json:"tables"`
	RowsRead    int64      `json:"rows_read"`
	RowsWritten int64      `json:"rows_written"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
}

// lastBatchRunThatMovedRows returns nil when no batch run of the pipeline moved a
// row, or when the lookup fails (the field is advisory; the page works without it).
func lastBatchRunThatMovedRows(database *sql.DB, pipelineID string) *LastDataRun {
	var run LastDataRun
	var finished sql.NullTime
	err := database.QueryRow(`
		SELECT execution_id::text,
		       COUNT(*),
		       SUM(COALESCE(read_rows, 0)),
		       SUM(COALESCE(inserted_rows, 0)),
		       MAX(COALESCE(completed_at, updated_at))
		  FROM pipeline_run_table_stats
		 WHERE pipeline_id = $1::uuid
		   AND mode = 'batch'
		   AND execution_id IS NOT NULL
		 GROUP BY execution_id
		HAVING SUM(COALESCE(read_rows, 0)) > 0 OR SUM(COALESCE(inserted_rows, 0)) > 0
		 ORDER BY MAX(COALESCE(completed_at, updated_at)) DESC
		 LIMIT 1
	`, pipelineID).Scan(&run.ExecutionID, &run.Tables, &run.RowsRead, &run.RowsWritten, &finished)
	if err != nil {
		if err != sql.ErrNoRows {
			log.WithError(err).Warn("Failed to look up the last batch run that moved rows")
		}
		return nil
	}
	if finished.Valid {
		run.FinishedAt = &finished.Time
	}
	return &run
}

// cdcRunStatsAreUnderPipelineKey reports whether executionID is a run of a CDC
// pipeline with no table-stats rows of its own, i.e. one whose counters were written
// under execution_id = pipeline_id. A batch run, a run of another pipeline, or a
// CDC run that did write rows under its own id (a snapshot) keeps its own rows.
// Fails closed: any error leaves the caller's execution id as given.
func cdcRunStatsAreUnderPipelineKey(database *sql.DB, pipelineID, executionID string) bool {
	if _, err := uuid.Parse(executionID); err != nil {
		return false
	}
	var underPipelineKey bool
	err := database.QueryRow(`
		SELECT EXISTS (SELECT 1 FROM pipelines p WHERE p.id = $1::uuid AND `+pipelineRowIsCDCSQL+`)
		   AND EXISTS (SELECT 1 FROM executions e WHERE e.id = $2::uuid AND e.pipeline_id = $1::uuid)
		   AND NOT EXISTS (
		     SELECT 1 FROM pipeline_run_table_stats s
		     WHERE s.pipeline_id = $1::uuid AND s.execution_id = $2::uuid
		   )
	`, pipelineID, executionID).Scan(&underPipelineKey)
	if err != nil {
		log.WithError(err).Warn("table stats: could not resolve a CDC run to its pipeline key")
		return false
	}
	return underPipelineKey
}

func getPipelineSelectedTables(database *sql.DB, pipelineID string) []string {
	if database == nil || strings.TrimSpace(pipelineID) == "" {
		return nil
	}
	var raw string
	if err := database.QueryRow(
		`SELECT COALESCE(config->'selected_tables','[]'::jsonb)::text FROM pipelines WHERE id = $1::uuid`,
		pipelineID,
	).Scan(&raw); err != nil || strings.TrimSpace(raw) == "" {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		return nil
	}
	seen := make(map[string]bool, len(arr))
	out := make([]string, 0, len(arr))
	for _, v := range arr {
		s := strings.TrimSpace(v)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

// getPipelineCDCStreamingOnlyTables returns pipelines.config->'cdc_streaming_only_tables'
// as a set: the CDC tables added without loading their existing rows (maintained by
// UpdatePipelineCDCTables and BackfillPipelineCDCTables). Nil on any error.
func getPipelineCDCStreamingOnlyTables(database *sql.DB, pipelineID string) map[string]bool {
	if database == nil || strings.TrimSpace(pipelineID) == "" {
		return nil
	}
	var raw string
	if err := database.QueryRow(
		`SELECT COALESCE(config->'`+cdcStreamingOnlyTablesKey+`','[]'::jsonb)::text FROM pipelines WHERE id = $1::uuid`,
		pipelineID,
	).Scan(&raw); err != nil {
		return nil
	}
	var arr []string
	if err := json.Unmarshal([]byte(raw), &arr); err != nil {
		return nil
	}
	out := make(map[string]bool, len(arr))
	for _, v := range arr {
		if s := strings.TrimSpace(v); s != "" {
			out[s] = true
		}
	}
	return out
}

func buildCDCTableStatsResponse(
	database *sql.DB,
	pipelineID string,
	executionID string,
	selectedTables []string,
	streamingOnly map[string]bool,
	search string,
	sortBy string,
	limit int,
	offset int,
	exportFormat string,
) ([]TableStat, TableStatsSummary, int, error) {
	// Fetch existing CDC stats rows (may be empty for newly added tables).
	existing := make(map[string]TableStat, 64)
	// read_rows/inserted_rows are batch-only in this response (BUG #8): for CDC the sink
	// fills them with change-event totals that duplicate the counters below.
	rows, err := database.Query(`
		SELECT
			schema_name, table_name, qualified_name, mode, status,
			`+capturedSnapshotSQL+` AS snapshot_rows, applied_snapshot_rows,
			`+capturedInsertsSQL+` AS inserts, updates, deletes,
			(COALESCE(`+capturedInsertsSQL+`, 0) + COALESCE(updates, 0) + COALESCE(deletes, 0)) AS total_events,
			last_event_ts,
			applied_inserts, applied_updates, applied_deletes,
			(COALESCE(applied_inserts, 0) + COALESCE(applied_updates, 0) + COALESCE(applied_deletes, 0)) AS applied_total_events,
			last_applied_ts,
			COALESCE(dlq_rows, 0) AS dlq_rows,
			destination_schema, destination_qualified_name, orchestration_execution_id,
			started_at, completed_at, updated_at
		FROM pipeline_run_table_stats
		WHERE pipeline_id = $1
		  AND execution_id = $2::uuid
		  AND mode = 'cdc'
	`, pipelineID, executionID)
	if err != nil {
		return nil, TableStatsSummary{}, 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var stat TableStat
		var schemaName sql.NullString
		var tableName, qualifiedName, mode, status string
		var snapshotRows, appliedSnapshotRows sql.NullInt64
		var inserts, updates, deletes, totalEvents sql.NullInt64
		var appliedInserts, appliedUpdates, appliedDeletes, appliedTotalEvents sql.NullInt64
		var lastEventTs, lastAppliedTs, startedAt, completedAt sql.NullTime
		var dlqRows int64
		var destSchema, destQualifiedName, orchExecutionID sql.NullString
		var updatedAt time.Time

		if err := rows.Scan(
			&schemaName, &tableName, &qualifiedName, &mode, &status,
			&snapshotRows, &appliedSnapshotRows,
			&inserts, &updates, &deletes, &totalEvents, &lastEventTs,
			&appliedInserts, &appliedUpdates, &appliedDeletes, &appliedTotalEvents, &lastAppliedTs,
			&dlqRows,
			&destSchema, &destQualifiedName, &orchExecutionID,
			&startedAt, &completedAt, &updatedAt,
		); err != nil {
			continue
		}
		if snapshotRows.Valid {
			v := snapshotRows.Int64
			stat.SnapshotRows = &v
		}
		if appliedSnapshotRows.Valid {
			v := appliedSnapshotRows.Int64
			stat.AppliedSnapshotRows = &v
		}

		if schemaName.Valid {
			stat.SchemaName = schemaName.String
		}
		if destSchema.Valid {
			v := destSchema.String
			stat.DestinationSchema = &v
		}
		if destQualifiedName.Valid {
			v := destQualifiedName.String
			stat.DestinationQualifiedName = &v
		}
		if orchExecutionID.Valid {
			v := orchExecutionID.String
			stat.OrchestrationExecutionID = &v
		}
		stat.TableName = tableName
		stat.QualifiedName = qualifiedName
		stat.Mode = mode
		stat.Status = status
		stat.DLQRows = dlqRows
		stat.UpdatedAt = updatedAt

		if inserts.Valid {
			v := inserts.Int64
			stat.Inserts = &v
		}
		if updates.Valid {
			v := updates.Int64
			stat.Updates = &v
		}
		if deletes.Valid {
			v := deletes.Int64
			stat.Deletes = &v
		}
		if totalEvents.Valid {
			v := totalEvents.Int64
			stat.TotalEvents = &v
		}
		if lastEventTs.Valid {
			stat.LastEventTs = &lastEventTs.Time
		}
		if appliedInserts.Valid {
			v := appliedInserts.Int64
			stat.AppliedInserts = &v
		}
		if appliedUpdates.Valid {
			v := appliedUpdates.Int64
			stat.AppliedUpdates = &v
		}
		if appliedDeletes.Valid {
			v := appliedDeletes.Int64
			stat.AppliedDeletes = &v
		}
		if appliedTotalEvents.Valid {
			v := appliedTotalEvents.Int64
			stat.AppliedTotalEvents = &v
		}
		if lastAppliedTs.Valid {
			stat.LastAppliedTs = &lastAppliedTs.Time
		}
		if startedAt.Valid {
			stat.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			stat.CompletedAt = &completedAt.Time
		}

		if strings.TrimSpace(stat.QualifiedName) != "" {
			existing[stat.QualifiedName] = stat
		}
	}

	// Build merged list in the order of selectedTables (stable), overlaying existing stats.
	all := make([]TableStat, 0, len(selectedTables))
	now := time.Now().UTC()
	used := make(map[string]bool, len(existing))
	for _, qn := range selectedTables {
		qn = strings.TrimSpace(qn)
		if qn == "" {
			continue
		}
		loadMode := ""
		if streamingOnly[qn] {
			loadMode = cdcLoadModeStreamingOnly
		}
		if key, ok := matchStatsRow(existing, used, qn); ok {
			used[key] = true
			st := existing[key]
			st.QualifiedName = qn
			st.LoadMode = loadMode
			all = append(all, st)
			continue
		}

		// A selected table the stats projector has no row for: nothing has been
		// captured or applied. Its counters stay nil — nothing was measured, which
		// is not a measured zero — and it is not "running", which is what a table
		// that has never moved a row used to read as.
		schemaName, tableName := splitQualified(qn)
		stat := TableStat{
			SchemaName:    schemaName,
			TableName:     tableName,
			QualifiedName: qn,
			Mode:          "cdc",
			Status:        tableStatusWaitingForData,
			LoadMode:      loadMode,
			UpdatedAt:     now,
		}
		all = append(all, stat)
	}

	// BUG #19/#20: a stats row whose table is no longer selected used to vanish, and its
	// counts with it, so the totals fell when a table was removed. Keep it as "removed"
	// (any row the merge above did not use), sorted so the order is stable.
	removed := make([]string, 0)
	for qn := range existing {
		if !used[qn] {
			removed = append(removed, qn)
		}
	}
	sort.Strings(removed)
	for _, qn := range removed {
		st := existing[qn]
		st.Status = tableStatusRemoved
		all = append(all, st)
	}

	// Summary should reflect all selected (and removed) tables, not search/pagination.
	summary := computeCDCSummary(all)

	// Apply search filter for the returned list.
	filtered := all
	if strings.TrimSpace(search) != "" {
		s := strings.ToLower(strings.TrimSpace(search))
		out := make([]TableStat, 0, len(all))
		for _, t := range all {
			if strings.Contains(strings.ToLower(t.QualifiedName), s) || strings.Contains(strings.ToLower(t.TableName), s) {
				out = append(out, t)
			}
		}
		filtered = out
	}

	// Sorting
	switch sortBy {
	case "inserts":
		sort.SliceStable(filtered, func(i, j int) bool {
			return derefInt64(filtered[i].Inserts) > derefInt64(filtered[j].Inserts)
		})
	case "status":
		sort.SliceStable(filtered, func(i, j int) bool {
			ai := statusRank(filtered[i].Status)
			aj := statusRank(filtered[j].Status)
			if ai != aj {
				return ai < aj
			}
			return filtered[i].QualifiedName < filtered[j].QualifiedName
		})
	case "updated_at":
		sort.SliceStable(filtered, func(i, j int) bool {
			return filtered[i].UpdatedAt.After(filtered[j].UpdatedAt)
		})
	default:
		sort.SliceStable(filtered, func(i, j int) bool {
			return filtered[i].QualifiedName < filtered[j].QualifiedName
		})
	}

	total := len(filtered)

	// CSV export wants all rows (client sets limit=10000), but ensure we don't paginate if export=csv.
	if exportFormat == "csv" {
		return filtered, summary, total, nil
	}

	// Pagination
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 50
	}
	if offset >= total {
		return []TableStat{}, summary, total, nil
	}
	end := offset + limit
	if end > total {
		end = total
	}
	return filtered[offset:end], summary, total, nil
}

// matchStatsRow finds the stats row of a selected table: the row of that exact
// name, else the one unused row whose name equals it ignoring case or is its
// dot-suffix either way ("dbo.users" vs "inventory.dbo.users"). With two such
// rows it matches neither: a guess could show one table's counts as another's.
func matchStatsRow(existing map[string]TableStat, used map[string]bool, qn string) (string, bool) {
	if _, ok := existing[qn]; ok && !used[qn] {
		return qn, true
	}
	want := strings.ToLower(qn)
	found, n := "", 0
	for key := range existing {
		if used[key] {
			continue
		}
		k := strings.ToLower(strings.TrimSpace(key))
		if k == want || strings.HasSuffix(k, "."+want) || strings.HasSuffix(want, "."+k) {
			found, n = key, n+1
		}
	}
	return found, n == 1
}

func splitQualified(qn string) (schemaName string, tableName string) {
	parts := strings.Split(strings.TrimSpace(qn), ".")
	parts = filterNonEmpty(parts)
	if len(parts) == 0 {
		return "", ""
	}
	if len(parts) == 1 {
		return "", parts[0]
	}
	// everything before last is "schema"/namespace (db or schema)
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1]
}

func filterNonEmpty(parts []string) []string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func derefInt64(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

// addOptionalInt64 sums counters where nil means "not counted": the total stays nil
// until at least one addend is non-nil.
func addOptionalInt64(total, v *int64) *int64 {
	if v == nil {
		return total
	}
	sum := *v
	if total != nil {
		sum += *total
	}
	return &sum
}

// tableStatusWaitingForData is the status of a selected CDC table with no stats
// row yet (see buildCDCTableStatsResponse).
const tableStatusWaitingForData = "waiting_for_data"

// tableStatusRemoved is the status of a CDC stats row whose table is no longer
// selected (see buildCDCTableStatsResponse).
const tableStatusRemoved = "removed"

// cdcLoadModeStreamingOnly is TableStat.LoadMode for a table listed in
// pipelines.config->'cdc_streaming_only_tables'.
const cdcLoadModeStreamingOnly = "streaming_only"

func statusRank(status string) int {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "failed":
		return 1
	case "degraded":
		return 2
	case "running":
		return 3
	case tableStatusWaitingForData:
		return 4
	case "completed":
		return 5
	case tableStatusRemoved:
		return 6
	default:
		return 7
	}
}

func computeCDCSummary(all []TableStat) TableStatsSummary {
	var summary TableStatsSummary
	summary.Mode = "cdc"

	var totalInserts, totalUpdates, totalDeletes, totalCDCEvents int64
	var totalAppliedInserts, totalAppliedUpdates, totalAppliedDeletes, totalAppliedCDCEvents int64
	var totalSnapshot, totalAppliedSnapshot *int64
	// Did any table actually report a captured / an applied counter? Nothing else
	// distinguishes "the stream has moved nothing" from "nobody has measured this
	// yet", and the two want different pixels.
	var anyCaptured, anyApplied bool

	for _, t := range all {
		// A removed table leaves the table counts but keeps its counters in the totals.
		isRemoved := strings.EqualFold(strings.TrimSpace(t.Status), tableStatusRemoved)
		if isRemoved {
			summary.TablesRemoved++
		} else {
			summary.TotalTables++
		}
		switch strings.ToLower(strings.TrimSpace(t.Status)) {
		case "completed":
			summary.TablesCompleted++
		case "failed":
			summary.TablesFailed++
		case "running":
			summary.TablesRunning++
		case "degraded":
			summary.TablesDegraded++
		case tableStatusWaitingForData:
			summary.TablesWaitingForData++
		}

		// A selected table with no stats row is synthesized with nil counters
		// (buildCDCTableStatsResponse). derefInt64 turns those into 0, which is
		// correct for the SUM but must not decide whether the total is reported:
		// see the anyCaptured/anyApplied note below.
		if t.Inserts != nil || t.Updates != nil || t.Deletes != nil || t.TotalEvents != nil {
			anyCaptured = true
		}
		if t.AppliedInserts != nil || t.AppliedUpdates != nil || t.AppliedDeletes != nil || t.AppliedTotalEvents != nil {
			anyApplied = true
		}

		totalInserts += derefInt64(t.Inserts)
		totalUpdates += derefInt64(t.Updates)
		totalDeletes += derefInt64(t.Deletes)
		totalCDCEvents += derefInt64(t.TotalEvents)
		totalAppliedInserts += derefInt64(t.AppliedInserts)
		totalAppliedUpdates += derefInt64(t.AppliedUpdates)
		totalAppliedDeletes += derefInt64(t.AppliedDeletes)
		totalAppliedCDCEvents += derefInt64(t.AppliedTotalEvents)
		totalSnapshot = addOptionalInt64(totalSnapshot, t.SnapshotRows)
		totalAppliedSnapshot = addOptionalInt64(totalAppliedSnapshot, t.AppliedSnapshotRows)
		summary.TotalDLQRows += t.DLQRows
		if t.DLQRows > 0 && !isRemoved {
			summary.TablesWithDLQ++
		}
	}
	summary.TotalSnapshotRows = totalSnapshot
	summary.TotalAppliedSnapshotRows = totalAppliedSnapshot

	// Report a total only when something measured it. These fields are
	// `*int64 … omitempty`, and this function used to assign all eight
	// unconditionally — so a pipeline whose stats agent had never written a row
	// published eight MEASURED ZEROS, and the UI rendered a wall of "0"
	// (fmtNum prints "–" for a missing value, "0" for a zero). A user reads that
	// as "my pipeline moved nothing", which is a much stronger claim than the
	// data supports. computeTableStatsSummary, the sibling path for a pipeline
	// with no selected_tables, has always guarded these behind `if hasCDC`;
	// this is the same discipline for the selected-tables path.
	//
	// The two sides are tracked apart on purpose: with ENABLE_CDC_TABLE_STATS off
	// the sink still reports Applied while Captured is genuinely unmeasured, and
	// collapsing them into one flag would hide the half that does exist.
	if anyCaptured {
		summary.TotalInserts = &totalInserts
		summary.TotalUpdates = &totalUpdates
		summary.TotalDeletes = &totalDeletes
		summary.TotalCDCEvents = &totalCDCEvents
	}
	if anyApplied {
		summary.TotalAppliedInserts = &totalAppliedInserts
		summary.TotalAppliedUpdates = &totalAppliedUpdates
		summary.TotalAppliedDeletes = &totalAppliedDeletes
		summary.TotalAppliedCDCEvents = &totalAppliedCDCEvents
	}
	return summary
}

func computeTableStatsSummary(database *sql.DB, pipelineID, executionID string) TableStatsSummary {
	args := []interface{}{pipelineID}
	argIdx := 2
	where := "WHERE pipeline_id = $1"
	if executionID != "" {
		where += fmt.Sprintf(" AND execution_id = $%d::uuid", argIdx)
		args = append(args, executionID)
		argIdx++
	}

	var summary TableStatsSummary

	// Count by status and mode (with normalization: batch degraded with read==written counts as completed)
	query := fmt.Sprintf(`
		SELECT 
			mode,
			CASE
				WHEN mode = 'batch'
				     AND status = 'degraded'
				     AND COALESCE(dlq_rows, 0) = 0
				     AND COALESCE(read_rows, 0) = COALESCE(inserted_rows, 0)
				THEN 'completed'
				ELSE status
			END AS status,
			COUNT(*) as cnt,
			SUM(COALESCE(read_rows, 0)) as sum_read_rows,
			SUM(COALESCE(inserted_rows, 0)) as sum_inserted_rows,
			SUM(COALESCE(`+capturedInsertsSQL+`, 0)) as sum_inserts,
			SUM(COALESCE(updates, 0)) as sum_updates,
			SUM(COALESCE(deletes, 0)) as sum_deletes,
			SUM(COALESCE(`+capturedInsertsSQL+`, 0) + COALESCE(updates, 0) + COALESCE(deletes, 0)) as sum_total_events,
			SUM(COALESCE(applied_inserts, 0)) as sum_applied_inserts,
			SUM(COALESCE(applied_updates, 0)) as sum_applied_updates,
			SUM(COALESCE(applied_deletes, 0)) as sum_applied_deletes,
			SUM(COALESCE(applied_inserts, 0) + COALESCE(applied_updates, 0) + COALESCE(applied_deletes, 0)) as sum_applied_total_events,
			SUM(COALESCE(dlq_rows, 0)) as sum_dlq_rows,
			COUNT(*) FILTER (WHERE COALESCE(dlq_rows, 0) > 0) as cnt_tables_with_dlq
		FROM pipeline_run_table_stats
		%s
		GROUP BY mode, 2
	`, where)

	rows, err := database.Query(query, args...)
	if err != nil {
		log.WithError(err).Warn("Failed to compute table stats summary")
		return summary
	}
	defer rows.Close()

	hasBatch := false
	hasCDC := false
	var totalReadRows, totalInsertedRows int64
	var totalInserts, totalUpdates, totalDeletes, totalCDCEvents int64
	var totalAppliedInserts, totalAppliedUpdates, totalAppliedDeletes, totalAppliedCDCEvents int64

	for rows.Next() {
		var mode, status string
		var cnt int
		var sumReadRows, sumInsertedRows sql.NullInt64
		var sumInserts, sumUpdates, sumDeletes, sumTotalEvents sql.NullInt64
		var sumAppliedInserts, sumAppliedUpdates, sumAppliedDeletes, sumAppliedTotalEvents sql.NullInt64
		var sumDLQRows sql.NullInt64
		var cntTablesWithDLQ int

		if err := rows.Scan(&mode, &status, &cnt, &sumReadRows, &sumInsertedRows,
			&sumInserts, &sumUpdates, &sumDeletes, &sumTotalEvents,
			&sumAppliedInserts, &sumAppliedUpdates, &sumAppliedDeletes, &sumAppliedTotalEvents,
			&sumDLQRows, &cntTablesWithDLQ,
		); err != nil {
			continue
		}

		summary.TotalDLQRows += sumDLQRows.Int64
		summary.TablesWithDLQ += cntTablesWithDLQ
		summary.TotalTables += cnt
		switch status {
		case "completed":
			summary.TablesCompleted += cnt
		case "failed":
			summary.TablesFailed += cnt
		case "running":
			summary.TablesRunning += cnt
		case "degraded":
			summary.TablesDegraded += cnt
		}

		if mode == "batch" {
			hasBatch = true
			if sumReadRows.Valid {
				totalReadRows += sumReadRows.Int64
			}
			if sumInsertedRows.Valid {
				totalInsertedRows += sumInsertedRows.Int64
			}
		} else if mode == "cdc" {
			hasCDC = true
			if sumInserts.Valid {
				totalInserts += sumInserts.Int64
			}
			if sumUpdates.Valid {
				totalUpdates += sumUpdates.Int64
			}
			if sumDeletes.Valid {
				totalDeletes += sumDeletes.Int64
			}
			if sumTotalEvents.Valid {
				totalCDCEvents += sumTotalEvents.Int64
			}
			if sumAppliedInserts.Valid {
				totalAppliedInserts += sumAppliedInserts.Int64
			}
			if sumAppliedUpdates.Valid {
				totalAppliedUpdates += sumAppliedUpdates.Int64
			}
			if sumAppliedDeletes.Valid {
				totalAppliedDeletes += sumAppliedDeletes.Int64
			}
			if sumAppliedTotalEvents.Valid {
				totalAppliedCDCEvents += sumAppliedTotalEvents.Int64
			}
		}
	}

	if hasBatch && !hasCDC {
		summary.Mode = "batch"
	} else if hasCDC && !hasBatch {
		summary.Mode = "cdc"
	} else if hasBatch && hasCDC {
		summary.Mode = "mixed"
	}

	if hasBatch {
		summary.TotalReadRows = &totalReadRows
		summary.TotalInsertedRows = &totalInsertedRows
	}
	if hasCDC {
		summary.TotalInserts = &totalInserts
		summary.TotalUpdates = &totalUpdates
		summary.TotalDeletes = &totalDeletes
		summary.TotalCDCEvents = &totalCDCEvents
		summary.TotalAppliedInserts = &totalAppliedInserts
		summary.TotalAppliedUpdates = &totalAppliedUpdates
		summary.TotalAppliedDeletes = &totalAppliedDeletes
		summary.TotalAppliedCDCEvents = &totalAppliedCDCEvents
	}

	return summary
}

func exportTableStatsCSV(c *gin.Context, tables []TableStat, summary TableStatsSummary) {
	c.Header("Content-Type", "text/csv")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=table-stats-%s.csv", time.Now().Format("2006-01-02")))

	writer := csv.NewWriter(c.Writer)
	defer writer.Flush()

	// CSV header (generic across batch/cdc)
	header := []string{"schema", "table", "qualified_name", "mode", "status"}
	if summary.Mode == "batch" || summary.Mode == "mixed" {
		header = append(header, "read_rows", "inserted_rows")
	}
	if summary.Mode == "cdc" || summary.Mode == "mixed" {
		header = append(header,
			"inserts", "updates", "deletes", "total_events", "last_event_ts",
			"applied_inserts", "applied_updates", "applied_deletes", "applied_total_events", "last_applied_ts",
		)
	}
	// dlq_rows is emitted for every mode: it is the only column that reports rows the
	// destination never received, so it must not be conditional on the mode filter.
	header = append(header, "dlq_rows", "started_at", "completed_at", "updated_at")
	writer.Write(header)

	for _, t := range tables {
		row := []string{
			t.SchemaName,
			t.TableName,
			t.QualifiedName,
			t.Mode,
			t.Status,
		}

		if summary.Mode == "batch" || summary.Mode == "mixed" {
			row = append(row,
				int64Str(t.ReadRows),
				int64Str(t.InsertedRows),
			)
		}

		if summary.Mode == "cdc" || summary.Mode == "mixed" {
			row = append(row,
				int64Str(t.Inserts),
				int64Str(t.Updates),
				int64Str(t.Deletes),
				int64Str(t.TotalEvents),
				timeStr(t.LastEventTs),
				int64Str(t.AppliedInserts),
				int64Str(t.AppliedUpdates),
				int64Str(t.AppliedDeletes),
				int64Str(t.AppliedTotalEvents),
				timeStr(t.LastAppliedTs),
			)
		}

		row = append(row,
			strconv.FormatInt(t.DLQRows, 10),
			timeStr(t.StartedAt),
			timeStr(t.CompletedAt),
			t.UpdatedAt.Format(time.RFC3339),
		)

		writer.Write(row)
	}
}

func int64Str(v *int64) string {
	if v == nil {
		return ""
	}
	return strconv.FormatInt(*v, 10)
}

func timeStr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(time.RFC3339)
}
