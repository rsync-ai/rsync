package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// Snapshot strategy for a CDC pipeline's historical (initial) load.
const (
	// snapshotStrategyBlocking = Debezium's built-in blocking initial snapshot
	// (single long transaction; on PG it pins the xmin horizon / WAL for the whole
	// snapshot → bloat on large tables). This is the safe default and current behavior.
	snapshotStrategyBlocking = "blocking"
	// snapshotStrategyIncremental = a chunked, lock-free, resumable snapshot that runs
	// CONCURRENTLY with streaming (no long transaction, no xmin/WAL pin), triggered
	// out-of-band over the Kafka signal channel. Best for large tables / fast-moving data.
	snapshotStrategyIncremental = "incremental"
)

// incrementalSnapshotRowThresholdDefault is the total estimated source row count at/above
// which a CDC pipeline backfills history with an incremental snapshot instead of the
// blocking one. Tunable via CDC_INCREMENTAL_SNAPSHOT_MIN_ROWS.
const incrementalSnapshotRowThresholdDefault int64 = 1_000_000

func incrementalSnapshotRowThreshold() int64 {
	if raw := strings.TrimSpace(os.Getenv("CDC_INCREMENTAL_SNAPSHOT_MIN_ROWS")); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return incrementalSnapshotRowThresholdDefault
}

// hybridMinBinlogRetentionSecDefault is the conservative floor (seconds) below which the
// MySQL hybrid batch handoff refuses to run. In hybrid mode we capture a binlog position P,
// run a (potentially multi-minute) historical batch, then resume CDC from P. If the source
// purges the binlog at P before the batch finishes, CDC cannot resume and changes are
// silently lost. We can't know batch duration up front, so we fail loud when retention is
// finite and below this floor. Default 86400 (1 day) matches the assessor's
// MYSQL_BINLOG_EXPIRE_TOO_SHORT recommended minimum (assessor/mysql.go). Tunable via
// CDC_HYBRID_MIN_BINLOG_RETENTION_SEC; set to 0 to disable the guard entirely.
const hybridMinBinlogRetentionSecDefault = 86400

func hybridMinBinlogRetentionSec() int {
	if raw := strings.TrimSpace(os.Getenv("CDC_HYBRID_MIN_BINLOG_RETENTION_SEC")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			return n
		}
	}
	return hybridMinBinlogRetentionSecDefault
}

// computeCDCSnapshotStrategy decides how a CDC pipeline loads its historical data.
//
// Incremental is chosen only when ALL of the following hold; otherwise blocking — the
// safe default that preserves today's behavior:
//   - the source is PostgreSQL-family (phase 1; MySQL read-only incremental needs GTID
//     handling and is wired separately)
//   - every selected table has a primary key (incremental snapshots chunk by PK)
//   - the estimated total row count is at/above the threshold (small loads stay simple —
//     a blocking snapshot of a few thousand rows is instant and has no bloat concern)
//
// It is deliberately conservative: callers pass allTablesHavePK=false / totalRowEstimate=0
// whenever they could not measure the source, which yields blocking.
func computeCDCSnapshotStrategy(sourceType string, totalRowEstimate, thresholdRows int64, allTablesHavePK bool) string {
	if !allTablesHavePK || totalRowEstimate < thresholdRows {
		return snapshotStrategyBlocking
	}
	if hybridIsPostgresFamily(sourceType) {
		return snapshotStrategyIncremental
	}
	return snapshotStrategyBlocking
}

// cdcAutoBatchInitialLoadEnabled reports whether the orchestrator may auto-select the
// resumable hybrid batch initial load for a large CDC pipeline that would otherwise use
// Debezium's non-resumable blocking snapshot. Defaults ON; set CDC_AUTO_BATCH_INITIAL_LOAD
// to "false"/"0"/"off"/"no" to disable (kill switch → fall back to today's behavior).
func cdcAutoBatchInitialLoadEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CDC_AUTO_BATCH_INITIAL_LOAD"))) {
	case "false", "0", "off", "no":
		return false
	default:
		return true
	}
}

// shouldBatchForResumableSnapshot decides, from a MEASURED source estimate, whether a CDC
// pipeline with no explicit cdc_initial_load should use the resumable hybrid batch initial
// load instead of Debezium's non-resumable blocking snapshot. Pure (no I/O) so it is unit-
// testable; the caller supplies the measured source facts and the size threshold.
//
// It returns true only for the exact data-loss scenario: a LARGE load (>= thresholdRows)
// that the Debezium path would run as a blocking snapshot because a selected table lacks a
// primary key. Incremental snapshots chunk by PK, so one no-PK table forces the whole
// pipeline to the blocking, non-resumable snapshot — which restart-loops to the Sentinel cap
// and reaps the replication slot, losing WAL (data loss). The batch path pins WAL at a
// consistent point, back-fills history resumably (no-PK-safe, checkpointed), then streams
// from that point with no gap. Cases deliberately left on today's path:
//   - all-PK large loads → the Debezium path already gives them the concurrent, resumable
//     incremental snapshot (better than batch-then-stream, no WAL pin)
//   - small loads → a blocking snapshot is instant; no resumability concern
//   - unmeasurable loads → conservative; keep today's behavior
func shouldBatchForResumableSnapshot(est cdcSizeEstimate, thresholdRows int64) bool {
	if !est.measured || est.totalRows < thresholdRows {
		return false
	}
	return !est.allHavePK
}

// shouldAutoUseBatchInitialLoad gathers the source facts and reports whether a CDC pipeline
// with NO explicit cdc_initial_load choice should auto-default to the resumable hybrid batch
// historical load. Phase 1 scope: PostgreSQL-family sources with a non-append-only
// destination (the hybrid batch preconditions in executeHybridCDCDataTransfer). MySQL is
// deferred — its binlog-retention window during a long batch load needs separate handling.
// Best-effort: any measurement failure keeps today's path (returns false).
func (a *Agent) shouldAutoUseBatchInitialLoad(ctx context.Context, task ExecutorTask, selected []string) bool {
	if !cdcAutoBatchInitialLoadEnabled() || task.Source == nil || len(selected) == 0 {
		return false
	}
	// Only route to batch when the hybrid path can actually run it: PG-family source and a
	// non-append-only destination (append-only would duplicate the replayed overlap window).
	if !hybridIsPostgresFamily(task.Source.Type) || hybridDestAppendOnly(task) {
		return false
	}
	est := a.estimateCDCSourceSize(ctx, task, selected)
	return shouldBatchForResumableSnapshot(est, incrementalSnapshotRowThreshold())
}

// buildIncrementalSnapshotSignal builds the Kafka signal-channel message that triggers a
// Debezium incremental snapshot. Per the Debezium signalling contract the message KEY must
// be the connector's topic.prefix (== the connector name here) and the VALUE is the
// execute-snapshot signal JSON. dataCollections are the fully-qualified table identifiers
// as Debezium emits them (e.g. "public.orders").
func buildIncrementalSnapshotSignal(topicPrefix string, dataCollections []string) (key, value []byte, err error) {
	prefix := strings.TrimSpace(topicPrefix)
	if prefix == "" {
		return nil, nil, fmt.Errorf("incremental snapshot signal: empty topic prefix")
	}
	cols := make([]string, 0, len(dataCollections))
	seen := map[string]struct{}{}
	for _, c := range dataCollections {
		s := strings.TrimSpace(c)
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		cols = append(cols, s)
	}
	if len(cols) == 0 {
		return nil, nil, fmt.Errorf("incremental snapshot signal: no data collections")
	}
	payload := map[string]interface{}{
		"type": "execute-snapshot",
		"data": map[string]interface{}{
			"type":             "INCREMENTAL",
			"data-collections": cols,
		},
	}
	value, err = json.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("incremental snapshot signal: marshal: %w", err)
	}
	return []byte(prefix), value, nil
}

// stringSliceFromResult coerces an MCP result value (which JSON-decodes to []interface{})
// into a trimmed []string, dropping empties.
func stringSliceFromResult(v interface{}) []string {
	out := []string{}
	switch tv := v.(type) {
	case []string:
		for _, s := range tv {
			if s = strings.TrimSpace(s); s != "" {
				out = append(out, s)
			}
		}
	case []interface{}:
		for _, it := range tv {
			if s := strings.TrimSpace(fmt.Sprint(it)); s != "" {
				out = append(out, s)
			}
		}
	}
	return out
}

// cdcSizeEstimate carries the measured source facts plus the specific reasons the
// incremental (resumable) snapshot may be disallowed, so the caller can LOG why a pipeline
// fell back to the blocking snapshot instead of deciding it silently.
type cdcSizeEstimate struct {
	totalRows  int64    // summed estimated row count over matched selected tables
	allHavePK  bool     // true only when every matched selected table has a primary key
	noPKTables []string // selected tables that were matched but lack a primary key
	unmatched  []string // selected tables not found in source discovery
	matched    int      // number of selected tables matched in discovery
	measured   bool     // discovery succeeded, >=1 table matched, and none were unmatched
}

// estimateCDCSourceSize discovers the source schema and aggregates, over the SELECTED
// tables only, the total estimated row count and whether every selected table has a
// primary key. It also records the specific tables that lack a PK or could not be matched
// so the caller can explain a blocking-snapshot fallback. measured=false whenever discovery
// fails, returns nothing, or a selected table can't be matched — the caller treats "cannot
// measure" as a reason to use the blocking snapshot (unchanged behavior).
func (a *Agent) estimateCDCSourceSize(ctx context.Context, task ExecutorTask, selected []string) cdcSizeEstimate {
	est := cdcSizeEstimate{allHavePK: true}
	if task.Source == nil || task.Source.Config == nil || len(selected) == 0 {
		return est
	}
	srcCfg := make(map[string]interface{}, len(task.Source.Config))
	for k, v := range task.Source.Config {
		srcCfg[k] = v
	}
	discovered, err := a.DiscoverSchema(ctx, task.Source.Type, srcCfg)
	if err != nil || len(discovered) == 0 {
		return est
	}

	type meta struct {
		rows  int64
		hasPK bool
	}
	byName := map[string]meta{}
	for _, tbl := range discovered {
		m := meta{rows: int64(tbl.RowCount), hasPK: len(tbl.PrimaryKeys) > 0}
		name := strings.TrimSpace(tbl.Name)
		if schema := strings.TrimSpace(tbl.Schema); schema != "" && name != "" {
			byName[strings.ToLower(schema+"."+name)] = m
		}
		if name != "" {
			byName[strings.ToLower(name)] = m
		}
	}

	for _, sel := range selected {
		s := strings.ToLower(strings.TrimSpace(sel))
		if s == "" {
			continue
		}
		m, found := byName[s]
		if !found {
			if i := strings.LastIndex(s, "."); i >= 0 && i+1 < len(s) {
				m, found = byName[s[i+1:]]
			}
		}
		if !found {
			// A selected table we can't measure → don't risk incremental; record it.
			est.unmatched = append(est.unmatched, sel)
			est.allHavePK = false
			continue
		}
		est.matched++
		est.totalRows += m.rows
		if !m.hasPK {
			est.noPKTables = append(est.noPKTables, sel)
			est.allHavePK = false
		}
	}
	// measured mirrors the previous ok=true contract: discovery worked, at least one table
	// matched, and every selected table was found (an unmatched table stays conservative).
	est.measured = est.matched > 0 && len(est.unmatched) == 0
	return est
}

// cdcBlockingReason explains, for logs, why a CDC pipeline fell back to the blocking
// (non-resumable) snapshot instead of the incremental one.
func cdcBlockingReason(est cdcSizeEstimate, thresholdRows int64) string {
	switch {
	case !est.measured && len(est.unmatched) > 0:
		return fmt.Sprintf("%d selected table(s) not found in source discovery", len(est.unmatched))
	case !est.measured:
		return "source could not be measured (schema discovery failed or returned nothing)"
	case len(est.noPKTables) > 0:
		return fmt.Sprintf("%d selected table(s) have no primary key (incremental snapshots chunk by PK)", len(est.noPKTables))
	case est.totalRows < thresholdRows:
		return fmt.Sprintf("estimated %d rows below incremental threshold %d", est.totalRows, thresholdRows)
	default:
		return "source not PostgreSQL-family"
	}
}

// triggerIncrementalSnapshot waits (bounded) for the Debezium connector task to reach
// RUNNING, then sends the execute-snapshot signal over the Kafka signal channel. The wait
// matters: the Kafka signal channel consumer reads only signals that arrive AFTER it is
// live, so signalling before the task is RUNNING would be silently dropped and history
// would never backfill.
//
// The incremental strategy starts Debezium with snapshot.mode=no_data, so THIS signal is
// the ONLY historical-backfill path — there is no blocking-snapshot fallback. A silent
// failure here means the user asked for cdc_mode=initial, got a "running" pipeline, and
// never receives the historical rows. So every failure is returned to the caller (which
// fails the run loudly) rather than swallowed. Returns nil only once the signal is produced.
func (a *Agent) triggerIncrementalSnapshot(ctx context.Context, task ExecutorTask, connectorName, signalTopic string, dataCollections []string) error {
	if a.kafkaManager == nil {
		return fmt.Errorf("CDC incremental snapshot: no Kafka manager; history will NOT backfill (connector started with snapshot.mode=no_data)")
	}
	key, value, err := buildIncrementalSnapshotSignal(connectorName, dataCollections)
	if err != nil {
		return fmt.Errorf("CDC incremental snapshot: could not build signal (history will NOT backfill): %w", err)
	}

	if !a.waitConnectorRunning(ctx, connectorName, 45*time.Second) {
		// The 45s wait IS the delivery-correctness contract: a signal produced before the
		// task is RUNNING is silently dropped. Not-RUNNING therefore means unreliable
		// delivery, so escalate rather than "send anyway" and hope.
		return fmt.Errorf("CDC incremental snapshot: connector %q task not RUNNING within 45s; the execute-snapshot signal would be dropped and history would NOT backfill", connectorName)
	}

	if err := a.kafkaManager.EnsureTopicExists(signalTopic, 1); err != nil {
		// Non-fatal: the produce below is the authoritative delivery check (broker
		// auto-create may still succeed).
		log.WithError(err).WithField("topic", signalTopic).Warn("⚠️  CDC incremental snapshot: could not ensure signal topic exists (attempting produce anyway)")
	}
	if err := a.kafkaManager.ProduceWithContext(ctx, signalTopic, key, value); err != nil {
		return fmt.Errorf("CDC incremental snapshot: failed to send execute-snapshot signal (history will NOT backfill): %w", err)
	}
	log.WithFields(log.Fields{
		"pipeline_id":  task.PipelineID,
		"signal_topic": signalTopic,
		"tables":       len(dataCollections),
	}).Info("📸 CDC incremental snapshot triggered via Kafka signal (backfill runs concurrently with streaming)")
	return nil
}

// waitConnectorRunning polls the Kafka Connect REST status for the connector until both
// the connector and at least one task report RUNNING, or the timeout elapses. Returns
// true only when RUNNING was observed.
func (a *Agent) waitConnectorRunning(ctx context.Context, connectorName string, timeout time.Duration) bool {
	base := strings.TrimRight(os.Getenv("KAFKA_CONNECT_URL"), "/")
	if base == "" {
		base = "http://kafka-connect:8083"
	}
	statusURL := fmt.Sprintf("%s/connectors/%s/status", base, connectorName)
	deadline := time.Now().Add(timeout)

	for attempt := 0; ; attempt++ {
		if running := probeConnectorRunning(ctx, statusURL); running {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
	}
}

// probeConnectorRunning does one status GET and reports whether the connector and at least
// one task are RUNNING.
func probeConnectorRunning(ctx context.Context, statusURL string) bool {
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, statusURL, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return false
	}
	var status struct {
		Connector struct {
			State string `json:"state"`
		} `json:"connector"`
		Tasks []struct {
			State string `json:"state"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(body, &status); err != nil {
		return false
	}
	if !strings.EqualFold(status.Connector.State, "RUNNING") {
		return false
	}
	for _, t := range status.Tasks {
		if strings.EqualFold(t.State, "RUNNING") {
			return true
		}
	}
	return false
}
