package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

// APIPollingWorker handles pipelines with sync_mode = "api_polling".
//
// These pipelines periodically call export() on an API/SaaS source connector
// with a cursor (timestamp or page token) to fetch only new/updated records,
// then write them to the destination via import_data().
//
// Cursor tracking:
//   - Before each run: read cursor_value from pipeline_api_cursors.
//   - After a successful run: write the next_cursor returned by the connector.
//   - First run (no cursor): compute from polling_initial_lookback or use nil.
type APIPollingWorker struct {
	kafkaManager    *kafka.Manager
	db              *sql.DB
	toolsDir        string
	executorAgent   *executor.Agent
	progressEmitter *ProgressEmitter
	tracer          trace.Tracer
	workerID        string
}

func NewAPIPollingWorker(kafkaManager *kafka.Manager, db *sql.DB, toolsDir string, agent *executor.Agent) *APIPollingWorker {
	return &APIPollingWorker{
		kafkaManager:    kafkaManager,
		db:              db,
		toolsDir:        toolsDir,
		executorAgent:   agent,
		progressEmitter: NewProgressEmitter(kafkaManager, db),
		tracer:          otel.Tracer("api-polling-worker"),
		workerID:        fmt.Sprintf("api-poll-%s", uuid.New().String()[:8]),
	}
}

func (w *APIPollingWorker) GetWorkerType() string {
	return "api_polling"
}

// Execute runs one polling cycle for the given pipeline task.
func (w *APIPollingWorker) Execute(ctx context.Context, task Task) TaskResult {
	ctx, span := w.tracer.Start(ctx, "api_polling.execute",
		trace.WithAttributes(
			attribute.String("task_id", task.TaskID),
			attribute.String("pipeline_id", task.PipelineID),
		),
	)
	defer span.End()

	logger := log.WithFields(log.Fields{
		"worker":      w.workerID,
		"task_id":     task.TaskID,
		"pipeline_id": task.PipelineID,
	})
	logger.Info("🔄 API Polling Worker: starting run")

	pipelineID := task.PipelineID
	if pipelineID == "" {
		return TaskResult{
			TaskID:  task.TaskID,
			Status:  "failure",
			Error:   "pipeline_id is required",
		}
	}

	// ── 1. Load pipeline config ───────────────────────────────────────────────
	cfg, err := w.loadPipelineConfig(ctx, pipelineID)
	if err != nil {
		return w.fail(task, fmt.Sprintf("failed to load pipeline config: %v", err))
	}

	// ── 2. Determine resources to poll ───────────────────────────────────────
	resources := cfg.Resources
	if len(resources) == 0 && cfg.Table != "" {
		resources = []string{cfg.Table}
	}
	if len(resources) == 0 {
		resources = []string{"default"}
	}

	// ── 3. Run export→import for each resource ────────────────────────────────
	totalRows := 0
	var allErrors []string

	for _, resource := range resources {
		rows, err := w.pollResource(ctx, pipelineID, resource, cfg, task, logger)
		if err != nil {
			logger.WithError(err).Errorf("resource %q polling failed", resource)
			allErrors = append(allErrors, fmt.Sprintf("%s: %v", resource, err))
		} else {
			totalRows += rows
		}
	}

	if len(allErrors) > 0 && len(allErrors) == len(resources) {
		return w.fail(task, strings.Join(allErrors, "; "))
	}

	output := map[string]interface{}{
		"total_rows_synced": totalRows,
		"resources_polled":  resources,
		"run_at":            time.Now().UTC().Format(time.RFC3339),
	}
	if len(allErrors) > 0 {
		output["warnings"] = allErrors
	}

	return TaskResult{
		TaskID:      task.TaskID,
		WorkflowID:  task.WorkflowID,
		StepID:      task.StepID,
		Status:      "success",
		Output:      output,
		CompletedAt: time.Now(),
		TraceID:     task.TraceID,
	}
}

// pollResource runs one export→import cycle for a single API resource.
func (w *APIPollingWorker) pollResource(
	ctx context.Context,
	pipelineID, resource string,
	cfg *pollingConfig,
	task Task,
	logger *log.Entry,
) (int, error) {
	// ── Read cursor ───────────────────────────────────────────────────────────
	cursor, err := w.readCursor(ctx, pipelineID, resource)
	if err != nil {
		logger.WithError(err).Warn("cursor read error, proceeding without cursor")
		cursor = ""
	}

	// Compute initial lookback on first run
	if cursor == "" && cfg.InitialLookback > 0 {
		since := time.Now().UTC().Add(-cfg.InitialLookback)
		cursor = since.Format(time.RFC3339)
		logger.WithField("since", cursor).Info("First run: using initial lookback cursor")
	}

	logger.WithFields(log.Fields{
		"resource": resource,
		"cursor":   cursor,
	}).Info("Polling resource")

	// ── Export from source ────────────────────────────────────────────────────
	exportParams := map[string]interface{}{
		"table":    resource,
		"resource": resource,
		"limit":    cfg.BatchSize,
	}
	if cursor != "" {
		exportParams["cursor"] = cursor
		exportParams["since"] = cursor
		if cfg.CursorField != "" {
			exportParams["cursor_field"] = cfg.CursorField
			exportParams["incremental_field"] = cfg.CursorField
		}
	}

	exportTask := executor.ExecutorTask{
		TaskID:     task.TaskID + "-export-" + resource,
		PipelineID: pipelineID,
		Operation:  "export",
		Source: &executor.ConnectorConfig{
			Type:   cfg.SourceConnector,
			Config: cfg.SourceConfig,
		},
		Params: exportParams,
	}

	exportResp := w.executorAgent.ExecuteTask(ctx, exportTask)
	if exportResp.Status == "failed" || exportResp.Error != "" {
		return 0, fmt.Errorf("export failed: %s", exportResp.Error)
	}

	records, _ := exportResp.Result["records"].([]interface{})
	nextCursor, _ := exportResp.Result["next_cursor"].(string)
	rowCount := len(records)

	logger.WithFields(log.Fields{
		"resource":    resource,
		"row_count":   rowCount,
		"next_cursor": nextCursor,
	}).Info("Export complete")

	// ── Emit progress ─────────────────────────────────────────────────────────
	if w.progressEmitter != nil {
		_ = w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:  "PROGRESS",
			PipelineID: pipelineID,
			Status:     "processing",
			Message:    fmt.Sprintf("Fetched %d records from %s", rowCount, resource),
		})
	}

	// ── Short-circuit if nothing to import ────────────────────────────────────
	if rowCount == 0 {
		if nextCursor != "" {
			_ = w.writeCursor(ctx, pipelineID, resource, nextCursor, 0)
		}
		logger.WithField("resource", resource).Info("No new records to import")
		return 0, nil
	}

	// ── Import to destination ─────────────────────────────────────────────────
	importParams := map[string]interface{}{
		"table": cfg.DestTable(resource),
		"data":  records,
		"mode":  cfg.ImportMode,
	}

	importTask := executor.ExecutorTask{
		TaskID:     task.TaskID + "-import-" + resource,
		PipelineID: pipelineID,
		Operation:  "import",
		Destination: &executor.ConnectorConfig{
			Type:   cfg.DestConnector,
			Config: cfg.DestConfig,
		},
		Params: importParams,
	}

	importResp := w.executorAgent.ExecuteTask(ctx, importTask)
	if importResp.Status == "failed" || importResp.Error != "" {
		return 0, fmt.Errorf("import failed: %s", importResp.Error)
	}

	// ── Persist cursor ────────────────────────────────────────────────────────
	cursorToSave := nextCursor
	if cursorToSave == "" {
		cursorToSave = time.Now().UTC().Format(time.RFC3339)
	}
	if err := w.writeCursor(ctx, pipelineID, resource, cursorToSave, rowCount); err != nil {
		logger.WithError(err).Warn("Failed to persist cursor; next run may re-fetch some records")
	}

	return rowCount, nil
}

// ── Cursor persistence ────────────────────────────────────────────────────────

func (w *APIPollingWorker) readCursor(ctx context.Context, pipelineID, resource string) (string, error) {
	if w.db == nil {
		return "", nil
	}
	var cursor sql.NullString
	err := w.db.QueryRowContext(ctx,
		`SELECT cursor_value FROM pipeline_api_cursors
		  WHERE pipeline_id = $1 AND resource_name = $2`,
		pipelineID, resource,
	).Scan(&cursor)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if !cursor.Valid {
		return "", nil
	}
	return cursor.String, nil
}

func (w *APIPollingWorker) writeCursor(ctx context.Context, pipelineID, resource, cursor string, rowCount int) error {
	if w.db == nil {
		return nil
	}
	_, err := w.db.ExecContext(ctx,
		`INSERT INTO pipeline_api_cursors
		    (pipeline_id, resource_name, cursor_value, last_synced_at, last_row_count, updated_at)
		 VALUES ($1, $2, $3, NOW(), $4, NOW())
		 ON CONFLICT (pipeline_id, resource_name) DO UPDATE SET
		    cursor_value   = EXCLUDED.cursor_value,
		    last_synced_at = NOW(),
		    last_row_count = EXCLUDED.last_row_count,
		    updated_at     = NOW()`,
		pipelineID, resource, cursor, rowCount,
	)
	return err
}

// ── Pipeline config ───────────────────────────────────────────────────────────

type pollingConfig struct {
	SourceConnector string
	SourceConfig    map[string]string
	DestConnector   string
	DestConfig      map[string]string
	Resources       []string
	Table           string
	CursorField     string
	BatchSize       int
	InitialLookback time.Duration
	ImportMode      string
	DestTablePrefix string
}

func (c *pollingConfig) DestTable(resource string) string {
	if c.DestTablePrefix != "" {
		return c.DestTablePrefix + "_" + resource
	}
	return resource
}

func (w *APIPollingWorker) loadPipelineConfig(ctx context.Context, pipelineID string) (*pollingConfig, error) {
	if w.db == nil {
		return nil, fmt.Errorf("database not available")
	}

	var (
		sourceConnector     string
		destConnector       string
		sourceConfigRaw     sql.NullString
		destConfigRaw       sql.NullString
		pipelineConfigRaw   sql.NullString
		cursorField         sql.NullString
		tableName           sql.NullString
		initialLookbackText sql.NullString
	)

	err := w.db.QueryRowContext(ctx, `
		SELECT
		    COALESCE(src_conn.connector_type, '')  AS source_connector,
		    COALESCE(dst_conn.connector_type, '')  AS dest_connector,
		    src_conn.config                         AS source_config,
		    dst_conn.config                         AS dest_config,
		    p.config                                AS pipeline_config,
		    p.polling_cursor_field,
		    p.table_name,
		    p.polling_initial_lookback::text
		FROM pipelines p
		LEFT JOIN connections src_conn ON src_conn.id = p.source_connection_id
		LEFT JOIN connections dst_conn ON dst_conn.id = p.destination_connection_id
		WHERE p.id = $1
	`, pipelineID).Scan(
		&sourceConnector,
		&destConnector,
		&sourceConfigRaw,
		&destConfigRaw,
		&pipelineConfigRaw,
		&cursorField,
		&tableName,
		&initialLookbackText,
	)
	if err != nil {
		return nil, fmt.Errorf("query pipeline: %w", err)
	}

	cfg := &pollingConfig{
		SourceConnector: sourceConnector,
		DestConnector:   destConnector,
		BatchSize:       10000,
		ImportMode:      "upsert",
	}
	if tableName.Valid {
		cfg.Table = tableName.String
	}
	if cursorField.Valid {
		cfg.CursorField = cursorField.String
	}
	if initialLookbackText.Valid && initialLookbackText.String != "" {
		cfg.InitialLookback = parsePostgresInterval(initialLookbackText.String)
	}

	// Unmarshal connection configs (stored as JSONB maps)
	if sourceConfigRaw.Valid {
		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(sourceConfigRaw.String), &raw); err == nil {
			cfg.SourceConfig = flattenToStringMap(raw)
		}
	}
	if destConfigRaw.Valid {
		var raw map[string]interface{}
		if err := json.Unmarshal([]byte(destConfigRaw.String), &raw); err == nil {
			cfg.DestConfig = flattenToStringMap(raw)
		}
	}

	// Parse pipeline-level config overrides
	if pipelineConfigRaw.Valid {
		var pCfg map[string]interface{}
		if err := json.Unmarshal([]byte(pipelineConfigRaw.String), &pCfg); err == nil {
			if tables, ok := pCfg["tables"].([]interface{}); ok {
				for _, t := range tables {
					if s, ok := t.(string); ok {
						cfg.Resources = append(cfg.Resources, s)
					}
				}
			}
			if bs, ok := pCfg["batch_size"].(float64); ok && bs > 0 {
				cfg.BatchSize = int(bs)
			}
			if mode, ok := pCfg["import_mode"].(string); ok && mode != "" {
				cfg.ImportMode = mode
			}
			if prefix, ok := pCfg["dest_table_prefix"].(string); ok {
				cfg.DestTablePrefix = prefix
			}
		}
	}

	return cfg, nil
}

func (w *APIPollingWorker) fail(task Task, msg string) TaskResult {
	return TaskResult{
		TaskID:      task.TaskID,
		WorkflowID:  task.WorkflowID,
		StepID:      task.StepID,
		Status:      "failure",
		Error:       msg,
		CompletedAt: time.Now(),
		TraceID:     task.TraceID,
	}
}

// flattenToStringMap converts map[string]interface{} to map[string]string.
func flattenToStringMap(m map[string]interface{}) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		switch val := v.(type) {
		case string:
			out[k] = val
		default:
			out[k] = fmt.Sprintf("%v", val)
		}
	}
	return out
}

// parsePostgresInterval converts a PostgreSQL interval string to time.Duration.
func parsePostgresInterval(s string) time.Duration {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return 0
	}
	var total time.Duration
	if idx := strings.Index(s, "day"); idx > 0 {
		var days int
		if _, err := fmt.Sscanf(strings.TrimSpace(s[:idx]), "%d", &days); err == nil {
			total += time.Duration(days) * 24 * time.Hour
		}
	}
	if idx := strings.Index(s, "hour"); idx > 0 {
		var hours int
		if _, err := fmt.Sscanf(strings.TrimSpace(s[:idx]), "%d", &hours); err == nil {
			total += time.Duration(hours) * time.Hour
		}
	}
	if idx := strings.Index(s, "minute"); idx > 0 {
		var mins int
		if _, err := fmt.Sscanf(strings.TrimSpace(s[:idx]), "%d", &mins); err == nil {
			total += time.Duration(mins) * time.Minute
		}
	}
	if total == 0 {
		return 7 * 24 * time.Hour // default: 7 days lookback
	}
	return total
}
