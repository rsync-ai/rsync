package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/rsync-ai/shared/kafkaclient"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/backend-orchestrator/internal/utils"
)

// hybridNormalizeDBType lower-cases, normalizes separators, and maps common aliases
// so source-family detection matches executeStreamingDataTransfer's local normalizer.
func hybridNormalizeDBType(s string) string {
	v := strings.ToLower(strings.TrimSpace(s))
	v = strings.ReplaceAll(v, "-", "_")
	switch v {
	case "postgres":
		return "postgresql"
	case "mariadb":
		return "mysql"
	default:
		return v
	}
}

// hybridIsPostgresFamily reports whether a source type uses PostgreSQL logical
// replication (where the slot is the durable position anchor — no offset seeding).
func hybridIsPostgresFamily(t string) bool {
	switch hybridNormalizeDBType(t) {
	case "postgresql", "cockroachdb", "cockroach_db", "aurora_postgresql", "alloydb", "neon", "supabase":
		return true
	}
	return false
}

// hybridSourceConnID extracts the source connection id from params/payload.
func hybridSourceConnID(task ExecutorTask) string {
	if task.Params != nil {
		if v, ok := task.Params["source_connection_id"].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	if task.Payload != nil {
		if v, ok := task.Payload["source_connection_id"].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// hybridDestAppendOnly reports whether the destination is configured for append-only
// (history) CDC writes. The position-anchored handoff replays the batch-window changes
// via CDC; for upsert destinations that overlap is idempotent, but for append-only it
// would duplicate history rows — so the hybrid path refuses append-only (deferred).
func hybridDestAppendOnly(task ExecutorTask) bool {
	if task.Destination == nil || task.Destination.Config == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(task.Destination.Config["cdc_write_mode"])) {
	case "append", "append_only", "append-only", "history", "insert":
		return true
	}
	return false
}

// hybridBackfillMarkerType is the cdc_resources resource_type recorded once the
// historical batch load has completed, so CDC pipeline restarts skip the (expensive)
// batch and resume streaming from the committed offset instead of re-loading history.
const hybridBackfillMarkerType = "hybrid_backfill_done"

// hybridBackfillDone reports whether the one-time historical batch load already
// completed for this pipeline (durable marker in cdc_resources).
func (a *Agent) hybridBackfillDone(ctx context.Context, pipelineID string) bool {
	if a.db == nil {
		return false
	}
	resources, err := cdc.GetCDCResources(ctx, a.db, pipelineID)
	if err != nil {
		return false
	}
	for _, r := range resources {
		if r.ResourceType == hybridBackfillMarkerType && r.Status == "active" {
			return true
		}
	}
	return false
}

// recordHybridBackfillDone persists the one-time backfill-complete marker.
func (a *Agent) recordHybridBackfillDone(ctx context.Context, task ExecutorTask, connID string, rows int) {
	if a.db == nil {
		return
	}
	pid := task.PipelineID
	marker := cdc.CDCResource{
		PipelineID:   &pid,
		ConnectionID: connID,
		ResourceType: hybridBackfillMarkerType,
		ResourceName: fmt.Sprintf("hybrid_backfill_%s", utils.SafeID8(task.PipelineID)),
		Status:       "active",
		DatabaseType: hybridNormalizeDBType(task.Source.Type),
		Metadata: map[string]interface{}{
			"rows_loaded":  rows,
			"completed_at": time.Now().UTC().Format(time.RFC3339),
		},
		CreatedAt: time.Now(),
	}
	if err := cdc.RecordResource(ctx, a.db, marker); err != nil {
		log.WithError(err).Warn("⚠️  Could not record hybrid backfill marker; a CDC restart may re-run the historical load")
	}
}

// hybridOffsetTopic returns the Kafka Connect offset-storage topic the Debezium
// worker reads. It matches OFFSET_STORAGE_TOPIC on the kafka-connect container
// (docker-compose.yml). The orchestrator does not run Connect, so the value is
// configured here with a default; override via KAFKA_CONNECT_OFFSET_TOPIC.
func hybridOffsetTopic() string {
	if v := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_OFFSET_TOPIC")); v != "" {
		return v
	}
	return "_rsync-connect-offsets"
}

// buildDebeziumMySQLOffsetRecord produces the exact key/value bytes for a Debezium
// MySQL source offset, as stored in the Kafka Connect connect-offsets topic
// (JsonConverter, schemas off):
//
//	key:   ["<connector_name>",{"server":"<topic_prefix>"}]
//	value: {"file":"<binlog_file>","pos":<pos>[,"gtids":"<gtid_set>"]}
//
// Kept pure (no I/O) so the wire format — which must match Debezium byte-for-byte or
// the seeded offset is ignored — is unit-testable.
func buildDebeziumMySQLOffsetRecord(connectorName, topicPrefix string, pos cdc.BinlogPosition) (key []byte, value []byte, err error) {
	if strings.TrimSpace(connectorName) == "" || pos.IsZero() {
		return nil, nil, fmt.Errorf("invalid offset record args (connector=%q, binlog_file=%q)", connectorName, pos.File)
	}
	if strings.TrimSpace(topicPrefix) == "" {
		topicPrefix = connectorName
	}
	// key = [connectorName, {"server": topicPrefix}] — the Kafka Connect source partition.
	keyObj := []interface{}{connectorName, map[string]string{"server": topicPrefix}}
	valObj := map[string]interface{}{"file": pos.File, "pos": pos.Pos}
	if strings.TrimSpace(pos.GTID) != "" {
		valObj["gtids"] = pos.GTID
	}
	key, err = json.Marshal(keyObj)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal offset key: %w", err)
	}
	value, err = json.Marshal(valObj)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal offset value: %w", err)
	}
	return key, value, nil
}

// seedDebeziumMySQLOffset writes a Kafka Connect source offset to the connect-offsets
// topic so a subsequently-created Debezium MySQL connector (snapshot.mode=recovery)
// RESUMES streaming from the captured binlog position P instead of re-snapshotting.
// This is the MySQL half of the position-anchored batch+CDC handoff (PostgreSQL needs
// no seeding — its replication slot is the durable anchor).
//
// Offset record format (Debezium MySQL via Kafka Connect JsonConverter, schemas off):
//
//	key:   ["<connector_name>",{"server":"<topic_prefix>"}]
//	value: {"file":"<binlog_file>","pos":<pos>[,"gtids":"<gtid_set>"]}
//
// connectorName is the Kafka Connect connector name (also the registration name).
// topicPrefix is the Debezium topic.prefix (equals connector_name in our executor
// unless overridden). BOTH must match what the Debezium MCP _build_config will set,
// or Connect's offset store won't associate the seeded offset with the connector.
//
// Timing: the executor seeds the offset BEFORE the (multi-minute) batch load and only
// creates the connector AFTER the batch completes, so Connect's offset backing store
// has long since consumed the seeded record — avoiding any read-before-seed race.
func (a *Agent) seedDebeziumMySQLOffset(ctx context.Context, connectorName, topicPrefix string, pos cdc.BinlogPosition) error {
	if a.kafkaManager == nil {
		return fmt.Errorf("kafka manager unavailable for offset seeding")
	}
	connectorName = strings.TrimSpace(connectorName)
	if connectorName == "" || pos.IsZero() {
		return fmt.Errorf("invalid offset seed args (connector=%q, binlog_file=%q)", connectorName, pos.File)
	}
	if strings.TrimSpace(topicPrefix) == "" {
		topicPrefix = connectorName
	}

	keyBytes, valBytes, err := buildDebeziumMySQLOffsetRecord(connectorName, topicPrefix, pos)
	if err != nil {
		return err
	}

	offsetTopic := hybridOffsetTopic()
	if err := a.kafkaManager.ProduceWithContext(ctx, offsetTopic, keyBytes, valBytes); err != nil {
		return fmt.Errorf("produce seed offset to %s: %w", offsetTopic, err)
	}

	log.WithFields(log.Fields{
		"connector":    connectorName,
		"topic_prefix": topicPrefix,
		"binlog_file":  pos.File,
		"binlog_pos":   pos.Pos,
		"offset_topic": offsetTopic,
		"key":          string(keyBytes),
	}).Info("🌱 Seeded Debezium MySQL offset for position-anchored CDC handoff")
	return nil
}

// executeHybridCDCDataTransfer runs the position-anchored hybrid handoff for a CDC
// pipeline whose cdc_initial_load == "batch":
//
//	Phase 1  Capture position P + (PG) pin WAL — BEFORE any data is read.
//	           PG    : provision publication + replication slot now; the slot retains
//	                   WAL from its consistent_point = P.
//	           MySQL : capture binlog coordinates P via SHOW MASTER STATUS.
//	Phase 2  Batch historical load (executeBatchDataTransfer — its own sink, upsert by PK).
//	Phase 3  Start CDC streaming FROM P (executeStreamingDataTransfer):
//	           MySQL : seed the connect-offsets topic at P; Debezium snapshot.mode=recovery.
//	           PG    : Debezium snapshot.mode=no_data resumes from the slot's confirmed_flush_lsn.
//
// Debezium replays every change since P, so changes that occur DURING the batch window
// are streamed (and converge with the batch via idempotent PK upserts) — no gap, no loss.
// Restarts skip Phase 1-2 via a durable backfill marker and resume streaming.
func (a *Agent) executeHybridCDCDataTransfer(ctx context.Context, task ExecutorTask, kafkaTopic string, traceID string) ExecutorResponse {
	fail := func(msg string) ExecutorResponse {
		return ExecutorResponse{TaskID: task.TaskID, PipelineID: task.PipelineID, Status: "failed", Error: msg}
	}
	if task.Source == nil {
		return fail("hybrid CDC: missing source config")
	}
	sourceType := task.Source.Type
	connectorName := fmt.Sprintf("cdc-%s", utils.SafeID8(task.PipelineID))

	// Guardrail: the overlap window only converges cleanly for upsert/current-state
	// destinations. Append-only history mode would duplicate the replayed batch-window
	// rows — refuse and tell the user to use the default Debezium snapshot.
	if hybridDestAppendOnly(task) {
		return fail("hybrid batch historical load (cdc_initial_load=batch) is not supported for append-only CDC destinations (cdc_write_mode=append); set cdc_initial_load=debezium to use the Debezium snapshot instead")
	}

	// Rerun guard: if the one-time historical load already completed, skip straight to
	// streaming so a CDC restart doesn't re-load history. Use schema_recovery (not
	// streaming_only): it maps to Debezium 'recovery' for MySQL (REBUILD schema history
	// from the live DB if the history topic isn't ready yet, then resume from the
	// committed offset) and 'no_data' for PostgreSQL (resume from the slot's
	// confirmed_flush_lsn). streaming_only→'never' would FAIL on MySQL with
	// "The db history topic is missing" whenever the history topic hasn't been built
	// (e.g. the initial-run double-invocation, where this resume path immediately
	// follows the first streaming start). recovery is idempotent and safe to repeat.
	if a.hybridBackfillDone(ctx, task.PipelineID) {
		log.WithField("pipeline_id", task.PipelineID).Info("♻️  Hybrid CDC: backfill already complete — resuming streaming (schema_recovery)")
		if task.Params == nil {
			task.Params = map[string]interface{}{}
		}
		task.Params["cdc_mode"] = "schema_recovery"
		return a.executeStreamingDataTransfer(ctx, task, kafkaTopic, traceID)
	}

	log.WithFields(log.Fields{
		"pipeline_id": task.PipelineID,
		"source":      sourceType,
	}).Info("🚀 Hybrid CDC: batch historical load + position-anchored Debezium handoff")

	sourceConnID := hybridSourceConnID(task)
	if sourceConnID == "" || sourceConnID == "auto" {
		return fail("hybrid CDC requires source_connection_id")
	}

	// ── Phase 1: capture P (and pin WAL for PG) BEFORE the batch reads any data. ──
	var mysqlPos cdc.BinlogPosition
	isPG := hybridIsPostgresFamily(sourceType)
	if isPG {
		sourceDBName := ""
		if task.Source.Config != nil {
			sourceDBName = strings.TrimSpace(task.Source.Config["database"])
			if sourceDBName == "" {
				sourceDBName = strings.TrimSpace(task.Source.Config["db_name"])
			}
		}
		tables := hybridTablesFromTask(task)
		cfg := cdc.CDCResourceConfig{
			PipelineID:   task.PipelineID,
			ConnectionID: sourceConnID,
			DatabaseType: "postgresql",
			Database:     sourceDBName,
		}
		// Creating the slot now pins WAL at the consistent_point = P. The later
		// executeStreamingDataTransfer call re-provisions idempotently (slot exists).
		pgMgr := cdc.NewPostgreSQLManager(a.db)
		if _, err := pgMgr.ProvisionResources(ctx, cfg, tables); err != nil {
			return fail(fmt.Sprintf("hybrid CDC: failed to provision PostgreSQL slot before batch (needed to pin WAL at P): %v", err))
		}
		// The slot pins WAL from P for the whole batch window. On a very large/slow
		// historical load this accumulates WAL on the source until CDC starts consuming
		// and advances confirmed_flush_lsn — monitor pg_replication_slots / disk on big tables.
		log.WithField("pipeline_id", task.PipelineID).Info("📌 Hybrid CDC: PostgreSQL slot provisioned — WAL pinned at consistent_point (P); source WAL grows until CDC begins consuming")
	} else if hybridNormalizeDBType(sourceType) == "mysql" {
		mysqlMgr := cdc.NewMySQLManager(a.db)
		// Ensure server_id exists (idempotent) and capture binlog coords at P.
		if _, err := mysqlMgr.ProvisionResources(ctx, cdc.CDCResourceConfig{
			PipelineID:   task.PipelineID,
			ConnectionID: sourceConnID,
			DatabaseType: "mysql",
			Database:     strings.TrimSpace(task.Source.Config["database"]),
		}, hybridTablesFromTask(task)); err != nil {
			return fail(fmt.Sprintf("hybrid CDC: failed to provision MySQL CDC resources: %v", err))
		}
		pos, err := mysqlMgr.CaptureBinlogPosition(ctx, sourceConnID)
		if err != nil || pos.IsZero() {
			return fail(fmt.Sprintf("hybrid CDC: failed to capture MySQL binlog position P before batch: %v", err))
		}
		mysqlPos = pos
		// Guardrail: the captured binlog position P must survive the entire batch load.
		// If retention is shorter than the batch takes, MySQL purges the binlog at P → CDC
		// cannot resume from P → SILENT data loss (the run would otherwise report success).
		// We can't know batch duration up front, so fail loud BEFORE any data moves when
		// retention is finite and below a conservative floor (default 1 day == the assessor's
		// recommended minimum), rather than warning and proceeding into a doomed handoff.
		// Opt out with CDC_HYBRID_MIN_BINLOG_RETENTION_SEC=0, or lower it for known-fast
		// batches. secs==0 means unlimited/unreadable retention and is deliberately allowed.
		if minRetention := hybridMinBinlogRetentionSec(); minRetention > 0 {
			if secs, rerr := mysqlMgr.CheckBinlogRetention(ctx, sourceConnID); rerr == nil && secs > 0 && secs < minRetention {
				return fail(fmt.Sprintf(
					"hybrid CDC: MySQL binlog retention is %ds, below the safe floor of %ds — a long historical batch load could outlive it, purge the binlog at position P, and silently lose CDC changes. Increase binlog_expire_logs_seconds on the source (>= %d), set CDC_HYBRID_MIN_BINLOG_RETENTION_SEC lower if you accept the risk, or use cdc_initial_load=debezium.",
					secs, minRetention, minRetention))
			}
		}
		log.WithFields(log.Fields{
			"pipeline_id": task.PipelineID,
			"binlog_file": pos.File,
			"binlog_pos":  pos.Pos,
		}).Info("📌 Hybrid CDC: captured MySQL binlog position (P) before batch")
	} else {
		return fail(fmt.Sprintf("hybrid CDC (cdc_initial_load=batch) is only supported for PostgreSQL-family and MySQL sources, got %q", sourceType))
	}

	// ── Phase 2: batch historical load. Reuse the batch data plane (its own sink, upsert). ──
	batchTopic := kafkaclient.Topic(fmt.Sprintf("pipeline.%s.data", utils.SafeID8(task.PipelineID)))
	log.WithFields(log.Fields{"pipeline_id": task.PipelineID, "batch_topic": batchTopic}).Info("📦 Hybrid CDC: starting batch historical load")
	batchResp := a.executeBatchDataTransfer(ctx, task, batchTopic, traceID)
	if batchResp.Status != "success" {
		return fail(fmt.Sprintf("hybrid CDC: batch historical load failed (CDC not started, no data loss): %s", batchResp.Error))
	}
	// Only persist the one-time backfill-complete marker when the batch actually landed
	// rows. executeBatchDataTransfer runs its own silent-drop check, but the ack-ledger
	// fail-soft branch can still return success-with-0-landed (ackRows==0). Recording the
	// marker on a 0-row batch would lock the skip in forever (hybridBackfillDone short-
	// circuits all future runs straight to streaming), so a silent drop or a not-yet-
	// populated source would never get re-backfilled. Streaming still starts from P this
	// run; if the source was genuinely empty nothing is lost (CDC captures everything from
	// P), and a later re-run simply re-attempts the cheap, idempotent (upsert-by-PK) batch
	// instead of trusting an unverified empty backfill (EXEC-M3).
	if batchResp.RowsProcessed > 0 {
		a.recordHybridBackfillDone(ctx, task, sourceConnID, batchResp.RowsProcessed)
	} else {
		log.WithFields(log.Fields{
			"pipeline_id": task.PipelineID,
		}).Warn("⚠️ Hybrid CDC: batch landed 0 rows — NOT recording backfill-done marker (a re-run will re-attempt the historical load); streaming still starts from P")
	}
	log.WithFields(log.Fields{
		"pipeline_id": task.PipelineID,
		"rows_loaded": batchResp.RowsProcessed,
	}).Info("✅ Hybrid CDC: batch historical load complete — starting CDC from P")

	// ── Phase 3: start CDC streaming from P. ──
	if task.Params == nil {
		task.Params = map[string]interface{}{}
	}
	if !isPG {
		// MySQL: seed the offset so Debezium resumes from P instead of re-snapshotting.
		if err := a.seedDebeziumMySQLOffset(ctx, connectorName, connectorName, mysqlPos); err != nil {
			return fail(fmt.Sprintf("hybrid CDC: failed to seed Debezium offset at P: %v", err))
		}
	}
	// schema_recovery → connector maps to Debezium 'recovery' (MySQL: rebuild schema +
	// resume from seeded offset) / 'no_data' (PG: resume from slot's confirmed_flush_lsn).
	task.Params["cdc_mode"] = "schema_recovery"
	streamResp := a.executeStreamingDataTransfer(ctx, task, kafkaTopic, traceID)
	if streamResp.Result == nil {
		streamResp.Result = map[string]interface{}{}
	}
	streamResp.Result["hybrid_initial_load"] = "batch"
	streamResp.Result["hybrid_backfill_rows"] = batchResp.RowsProcessed
	return streamResp
}

// hybridTablesFromTask returns the resolved table list (tables → selected_tables).
func hybridTablesFromTask(task ExecutorTask) []string {
	out := []string{}
	if task.Params == nil {
		return out
	}
	pick := func(v interface{}) {
		switch tv := v.(type) {
		case []string:
			for _, s := range tv {
				if strings.TrimSpace(s) != "" {
					out = append(out, strings.TrimSpace(s))
				}
			}
		case []interface{}:
			for _, it := range tv {
				s := strings.TrimSpace(fmt.Sprint(it))
				if s != "" {
					out = append(out, s)
				}
			}
		}
	}
	if v, ok := task.Params["tables"]; ok && v != nil {
		pick(v)
	}
	if len(out) == 0 {
		if v, ok := task.Params["selected_tables"]; ok && v != nil {
			pick(v)
		}
	}
	return out
}
