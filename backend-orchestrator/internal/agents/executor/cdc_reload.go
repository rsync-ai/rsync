package executor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
	"github.com/rsync-ai/backend-orchestrator/internal/storage"
)

// CDC Reload re-reads every table the pipeline captures, for every Debezium
// source, through the same queue as Re-snapshot (cdc_snapshot_requests).
//
// Reload used to do nothing on a CDC pipeline: the run started the connector
// again with the offsets it already had, Debezium logged its initial snapshot as
// SKIPPED, and the run read "completed". Stop now keeps the connector, its
// offsets and the slot (handlers.StopCDCPipeline), so Start resumes it, and a
// Reload is that Start plus a blocking snapshot of every captured table: the
// dispatcher sends the execute-snapshot signal once the connector runs with the
// tables, streaming pauses while they are read, then resumes from the kept
// position, so no change made while stopped is lost.
//
// What the destination keeps: an object-storage layout v2 folder is emptied per
// table before it is rewritten (the same cleans_folder rule as Re-snapshot). A
// database destination is upserted: every source row is written again, but a
// row that exists only in the destination stays. Layout v1 folders get a second
// copy of each table.
//
// Blocking is the one mode every Kafka-signal engine accepts: MongoDB has no
// read-only incremental snapshot, object storage takes blocking only, and a
// blocking snapshot marks its end, so the request is seen to finish.
const cdcReloadSnapshotMode = "blocking"

// errCDCReloadNoSignalChannel: the connector cannot be asked to re-read tables.
var errCDCReloadNoSignalChannel = errors.New("the CDC connector has no Kafka signal channel, so it cannot be asked to re-read its tables; " +
	"PostgreSQL and MongoDB connectors get one when they are created, so recreating the pipeline adds it")

// taskRunMode is the run's run_mode: the run request's (task params, then
// payload), else the pipeline's default_run_mode, else resume.
func taskRunMode(ctx context.Context, db *sql.DB, task ExecutorTask) storage.RunMode {
	for _, m := range []map[string]interface{}{task.Params, task.Payload} {
		if v, ok := m["run_mode"].(string); ok && strings.TrimSpace(v) != "" {
			return storage.ParseRunMode(v)
		}
	}
	if db != nil && task.PipelineID != "" {
		var dbRunMode string
		_ = db.QueryRowContext(ctx, "SELECT COALESCE(default_run_mode,'') FROM pipelines WHERE id = $1", task.PipelineID).Scan(&dbRunMode)
		if strings.TrimSpace(dbRunMode) != "" {
			return storage.ParseRunMode(dbRunMode)
		}
	}
	return storage.ParseRunMode("")
}

// cdcReloadNeeded: this CDC start is a Reload of a Debezium pipeline whose
// re-read is not already running. An incremental snapshot the same start
// triggered is that full read, so a second one would load every table twice.
// mode is read only when the rest holds (it costs a query).
func cdcReloadNeeded(incrementalTriggered bool, cdcProvider string, mode func() storage.RunMode) bool {
	return !incrementalTriggered && cdcProvider == "debezium" && mode() == storage.RunModeReload
}

// cdcReloadTables are the data-collections a Reload re-reads: the connector's
// include list, unescaped, which is exactly what the dispatcher checks the
// running tasks against. startCollections (start_sync's data_collections, the
// include list it submitted) covers a config read that returned none. nil when
// neither names plain tables: a pattern names no table.
func cdcReloadTables(cfg map[string]interface{}, startCollections []string) []string {
	if t := cdcsnapshot.PlainIncludeTables(cdcsnapshot.ParseIncludeList(cfg)); len(t) > 0 {
		return t
	}
	return cdcsnapshot.PlainIncludeTables(startCollections)
}

// hasKafkaSignalChannel: the connector reads execute-snapshot signals from Kafka
// (the channel the dispatcher sends on).
func hasKafkaSignalChannel(cfg map[string]interface{}) bool {
	return strings.TrimSpace(cdcsnapshot.SignalTopic(cfg)) != "" &&
		strings.Contains(strings.ToLower(fmt.Sprint(cfg["signal.enabled.channels"])), "kafka")
}

// queueCDCReload queues the Reload's re-snapshot. It fails rather than let a
// Reload read "completed" while nothing is re-read.
func (a *Agent) queueCDCReload(ctx context.Context, task ExecutorTask, connectorName string, startCollections []string) (cdcsnapshot.Request, error) {
	if a.db == nil {
		return cdcsnapshot.Request{}, errors.New("no database to queue the re-snapshot in")
	}
	base := strings.TrimRight(os.Getenv("KAFKA_CONNECT_URL"), "/")
	if base == "" {
		base = "http://kafka-connect:8083"
	}
	cfg, err := cdcsnapshot.NewConnectClient(base).Config(ctx, connectorName)
	if err != nil {
		return cdcsnapshot.Request{}, fmt.Errorf("could not read the CDC connector config: %w", err)
	}
	if !hasKafkaSignalChannel(cfg) {
		return cdcsnapshot.Request{}, errCDCReloadNoSignalChannel
	}
	tables := cdcReloadTables(cfg, startCollections)
	if len(tables) == 0 {
		return cdcsnapshot.Request{}, errors.New("the CDC connector's include list names no plain table to re-read")
	}
	destType := ""
	if task.Destination != nil {
		destType = task.Destination.Type
	}
	r, err := cdcsnapshot.NewStore(a.db).Insert(ctx, cdcsnapshot.Request{
		PipelineID:    task.PipelineID,
		ConnectorName: connectorName,
		Mode:          cdcReloadSnapshotMode,
		Tables:        tables,
		Source:        cdcsnapshot.SourceResnapshot,
		CleansFolder:  PipelineCleansFolderOnResnapshot(ctx, a.db, task.PipelineID, destType),
		NotBefore:     time.Now(),
	})
	if errors.Is(err, cdcsnapshot.ErrUnavailable) {
		return cdcsnapshot.Request{}, errors.New("the snapshot request queue is not available (migration 113 not applied)")
	}
	if err != nil {
		return cdcsnapshot.Request{}, fmt.Errorf("could not queue the re-snapshot: %w", err)
	}
	log.WithFields(log.Fields{
		"pipeline_id":   task.PipelineID,
		"connector":     connectorName,
		"request_id":    r.ID,
		"tables":        len(tables),
		"cleans_folder": r.CleansFolder,
	}).Info("🔁 CDC Reload: re-snapshot of every captured table queued; the dispatcher sends it once the connector runs")
	return r, nil
}
