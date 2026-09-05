package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rsync-ai/shared/kafkaclient"
	"io"
	"math/big"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	_ "github.com/rsync-ai/shared/pgdriver"
	"github.com/rsync-ai/shared/transforms"
	"github.com/segmentio/kafka-go"
)

var (
	gPipelineID  atomic.Value // string
	gExecutionID atomic.Value // string
)

func loadStr(v *atomic.Value) string {
	if s, ok := v.Load().(string); ok {
		return s
	}
	return ""
}

// logSafeFields are the structured field keys that carry pipeline METADATA and
// are therefore exempt from scrubbing: identifiers, names, counts and topic
// coordinates. Everything else is scrubbed.
//
// The list is an ALLOWLIST on purpose — unknown keys are scrubbed. A field
// carrying free-form text is the leak path (destination drivers embed offending
// row values in their error strings), and new fields are far more often error
// text than new coordinates. A structural field left off this list gets its
// digits masked, which is ugly in SigNoz; a value field wrongly exempted ships
// customer data. Only the first failure mode is acceptable.
//
// Counts (rows_written, raw_count, …) MUST be listed: reScrubLongDigits masks
// any run of 7+ digits, so a million-row write would otherwise log as
// "[num-redacted]".
var logSafeFields = map[string]bool{
	// Correlation / identity
	"timestamp": true, "level": true, "service": true,
	"pipeline_id": true, "execution_id": true, "trace_id": true,
	// Kafka coordinates
	"topic": true, "dlq_topic": true, "partition": true,
	"offset": true, "partitions": true,
	// Schema metadata — table and column names are explicitly permitted
	"table": true, "column": true, "columns": true,
	// Destination call metadata
	"tool": true, "host": true,
	// Counts and retry bookkeeping
	"attempt": true, "max_attempts": true, "sleep_ms": true,
	"raw_count": true, "rows": true, "rows_fetched": true,
	"rows_written": true, "imported": true,
	// CDC apply-path metadata (KI-CDC-DELETE-PATH-UNLOGGED). Every key here is a
	// NAME, a COORDINATE, a COUNT or a HASH — never a value:
	//   op / debezium_op — the DMS code (I/U/D) and the raw Debezium op (c/r/u/d)
	//   dest_table       — a destination table NAME (same class as "table")
	//   key_fields       — primary-key COLUMN NAMES, comma-joined
	//   pk_fingerprint   — a truncated SHA-256 of the key, never the key itself
	//   rows_deleted / rows_flushed — counts; must be listed or reScrubLongDigits
	//                      masks any 7+ digit run
	//   flush_reason     — a fixed enum (pre_delete_flush / interval_flush /
	//                      shutdown_flush / size_flush). NOT the key "reason",
	//                      which already carries raw tErr.Error() text at
	//                      main.go:3594/:3698/:4025 and MUST stay scrubbed.
	//   lsn / tx_id      — log position and transaction id. tx_id must be listed
	//                      because SinkMessage.TxID is a STRING holding an
	//                      xmin/GTID, which reScrubLongDigits would otherwise mask.
	// If anyone later reuses one of these keys to carry a VALUE it ships in the
	// clear; TestCDCDeleteLogNeverLeaksPKValue is the guard against that.
	"op": true, "debezium_op": true, "dest_table": true,
	"key_fields": true, "pk_fingerprint": true,
	"rows_deleted": true, "rows_flushed": true,
	"flush_reason": true, "lsn": true, "tx_id": true,
}

// scrubLogValue masks a structured field value unless its key is a known
// metadata field. Only strings are scrubbed — numeric and boolean values cannot
// carry row text.
func scrubLogValue(key string, v any) any {
	if logSafeFields[key] {
		return v
	}
	if s, ok := v.(string); ok {
		return scrubLog(s)
	}
	return v
}

// logEvent writes one structured JSON log line to stderr. fields are optional
// key/value pairs (even count). It never changes control flow.
//
// This is the single scrubbing chokepoint for sink logs. It used to scrub
// nothing: logf and logMsgEvent scrubbed their message text on the way in, but
// every logEvent caller that passed an error through a FIELD ("error",
// err.Error()) shipped the raw driver error — row values and all — straight to
// SigNoz, violating the metadata-only privacy rule. Scrubbing here covers the
// message and every field on every path, including direct logEvent callers.
func logEvent(level, msg string, fields ...any) {
	rec := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"level":     level,
		"message":   scrubLog(strings.TrimRight(msg, "\n")),
		"service":   "kafka-mcp-sink",
	}
	if p := loadStr(&gPipelineID); p != "" {
		rec["pipeline_id"] = p
	}
	if e := loadStr(&gExecutionID); e != "" {
		rec["execution_id"] = e
	}
	for i := 0; i+1 < len(fields); i += 2 {
		if k, ok := fields[i].(string); ok {
			rec[k] = scrubLogValue(k, fields[i+1])
		}
	}
	b, err := json.Marshal(rec)
	if err != nil {
		fmt.Fprintln(os.Stderr, scrubLog(msg)) // fallback, never panic
		return
	}
	fmt.Fprintln(os.Stderr, string(b))
}

// logf is a printf-style convenience that preserves existing format strings.
func logf(level, format string, a ...any) {
	// Scrubbing happens in logEvent, which covers the message and every field on
	// every path — including the direct logEvent callers this wrapper misses.
	logEvent(level, fmt.Sprintf(format, a...))
}

// logMsgEvent logs a structured line with per-message correlation fields
// (trace_id, table, topic, partition, offset) injected automatically. sm may be
// nil (e.g. before parse or in low-level read paths); when nil or empty, trace_id
// and table are pulled from the Kafka message headers so the line is still
// correlatable. The field is named exactly "trace_id" so the otel-collector
// promotes it to SigNoz's typed trace field (enabling trace↔log correlation).
func logMsgEvent(level string, sm *SinkMessage, msg kafka.Message, text string, fields ...any) {
	traceID, table := "", ""
	if sm != nil {
		traceID, table = sm.TraceID, sm.Table
	}
	if traceID == "" {
		traceID = strings.TrimSpace(string(headerValue(msg.Headers, "trace_id")))
	}
	if table == "" {
		table = strings.TrimSpace(string(headerValue(msg.Headers, "table")))
	}
	base := []any{
		"trace_id", traceID,
		"table", table,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	}
	// Scrubbing (message + fields) happens in logEvent.
	logEvent(level, text, append(base, fields...)...)
}

// pkFingerprint returns a short, stable, non-reversible token for a CDC primary
// key. Empty string when there is no key (or it cannot be encoded).
//
// Why a fingerprint and not the key itself: the PK is customer ROW DATA — an
// email primary key is PII — and logSafeFields exists precisely to stop values
// reaching SigNoz. A fingerprint still answers the only forensic question a
// wrong-row delete poses: take the row that vanished, hash its key, and grep the
// logs for the match. Logging the value would answer the same question by
// shipping the data, which the metadata-only privacy rule forbids.
//
// json.Marshal sorts map keys, so the encoding is canonical: the same key
// produces the same fingerprint across workers, restarts and partitions.
//
// Honest caveat: over a small key space (say integer ids 1..1e6) the fingerprint
// is brute-forceable. It is a CORRELATION TOKEN, not anonymisation — it keeps
// values out of the log by construction, but it is not a privacy guarantee.
func pkFingerprint(pk map[string]interface{}) string {
	if len(pk) == 0 {
		return ""
	}
	b, err := json.Marshal(pk)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])[:12]
}

type WorkerConfig struct {
	PipelineID  string `json:"pipeline_id"`
	ExecutionID string `json:"execution_id"`
	Topic       string `json:"topic"`
	// Topics allows a single worker to consume multiple Kafka topics under one consumer group.
	// Back-compat: if empty, Topic is used.
	Topics                []string `json:"topics"`
	ConsumerGroup         string   `json:"consumer_group"`
	KafkaBootstrapServers string   `json:"kafka_bootstrap_servers"`
	// DLQBootstrapServers overrides the brokers used for DLQ publishing only.
	// Useful for chaos testing (simulate DLQ down while Kafka read path is up).
	DLQBootstrapServers  string                 `json:"dlq_bootstrap_servers,omitempty"`
	DestinationConnector string                 `json:"destination_connector"`
	DestinationVersion   string                 `json:"destination_version"`
	DestinationConfig    map[string]interface{} `json:"destination_config"`
	MetricsPort          int                    `json:"metrics_port"`
	// SinkMode controls message parsing: "batch" (default), "cdc", or "auto" (detect from message structure)
	SinkMode string `json:"sink_mode"`
	// StartOffset controls where a NEW consumer group begins reading:
	// - "earliest" (default): from the beginning of the topic
	// - "latest": from the end of the topic (streaming-only semantics)
	// NOTE: This only applies when the consumer group has no committed offsets.
	StartOffset string `json:"start_offset"`
	// DestinationNamespace is the user-selected destination namespace (schema/db)
	// for relational sinks, injected by the orchestrator into the sink's start
	// config. CDC needs it here because Debezium controls the per-message payload,
	// so (unlike batch's per-message db_or_schema header) parseCDCMessage reads the
	// namespace from this config. Empty => unchanged (connector falls back to
	// config["database"]).
	DestinationNamespace string `json:"destination_namespace"`
	// KafkaSinkWorker config controls CDC batching, metadata, and dedup behaviors in the sink worker.
	// It is optional and defaults are applied when omitted.
	KafkaSinkWorker *KafkaSinkWorkerConfig `json:"kafka_sink_worker,omitempty"`
}

type KafkaSinkWorkerConfig struct {
	CDCBatching   *CDCBatchingConfig   `json:"cdc_batching,omitempty"`
	Metadata      *WorkerMetadata      `json:"metadata,omitempty"`
	Deduplication *DeduplicationConfig `json:"deduplication,omitempty"`
	DLQ           *DLQConfig           `json:"dlq,omitempty"`
}

type WorkerMetadata struct {
	IncludeKafkaMetadata *bool `json:"include_kafka_metadata,omitempty"`
}

type CDCBatchingConfig struct {
	Enabled               *bool `json:"enabled,omitempty"`
	MaxEvents             int   `json:"max_events,omitempty"`
	MaxBytes              int   `json:"max_bytes,omitempty"`
	FlushIntervalSeconds  int   `json:"flush_interval_seconds,omitempty"`
	MaxRetries            int   `json:"max_retries,omitempty"`
	BackoffMS             int   `json:"backoff_ms,omitempty"`
	AbsoluteMaxEventBytes int   `json:"absolute_max_event_bytes,omitempty"`
}

type DeduplicationConfig struct {
	RedisEnabled     *bool  `json:"redis_enabled,omitempty"`
	RedisKeyTTLHours int    `json:"redis_key_ttl_hours,omitempty"`
	OnRedisFailure   string `json:"on_redis_failure,omitempty"` // warn_continue (default) or fail_pipeline
}

type DLQConfig struct {
	Enabled *bool `json:"enabled,omitempty"`
}

type Metrics struct {
	startedAt time.Time
	processed uint64
	skipped   uint64
	failed    uint64

	// CDC-specific counters
	cdcInserts uint64
	cdcUpdates uint64
	cdcDeletes uint64
	cdcReads   uint64 // snapshot reads

	// Reliability counters
	dlqPublishFailures uint64
	dlqRouted          uint64 // messages parked in DLQ after exhausting retries (incl. batched CDC)

	// dlqByTable is dlqRouted split per qualified table name (value: int64), so a
	// discarded row can be *reported* and not merely counted in aggregate. A DLQ'd
	// row is a row the destination will never receive: it is never counted as
	// captured, so the captured-vs-applied reconciliation that exists to catch loss
	// balances perfectly while the source holds one more row than the destination.
	// This is the counter that closes that blind spot — it rides the TABLE_STATS
	// event into pipeline_run_table_stats and out to the pipeline UI.
	dlqByTable sync.Map

	// Observability: last known Kafka position + lag (best-effort from kafka-go reader stats).
	lastKafkaOffset       int64
	lastKafkaLag          int64
	lastKafkaPartition    int64
	lastProcessedAtUnixMs int64
	lastCommittedAtUnixMs int64
	lastCommittedOffset   int64

	// Observability: CDC source time + lag
	lastSourceTSUnixMs int64
	lastSourceLagMs    int64

	mu        sync.Mutex
	lastError string
	lastTopic string
}

func (m *Metrics) setErr(err error) {
	if err == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastError = err.Error()
}

func (m *Metrics) setLastTopic(topic string) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastTopic = strings.TrimSpace(topic)
}

func updateReaderStats(m *Metrics, reader *kafka.Reader) {
	if m == nil || reader == nil {
		return
	}
	st := reader.Stats()
	atomic.StoreInt64(&m.lastKafkaOffset, st.Offset)
	atomic.StoreInt64(&m.lastKafkaLag, st.Lag)
	if p, err := strconv.ParseInt(strings.TrimSpace(st.Partition), 10, 64); err == nil {
		atomic.StoreInt64(&m.lastKafkaPartition, p)
	}
	if strings.TrimSpace(st.Topic) != "" {
		m.setLastTopic(st.Topic)
	}
}

type SinkMessage struct {
	PipelineID  string
	ExecutionID string

	// OrchestrationExecutionID is the execution id the ORCHESTRATOR minted for this
	// run, carried for LABELLING only — no write path keys off it.
	//
	// It exists because ExecutionID above is forced to PipelineID on the CDC lane (see
	// parseCDCMessage) so the captured-side and applied-side counters upsert into one
	// stats row. That convention is deliberate and load-bearing — two hard FKs depend
	// on it (migrations 043, 045) — but its cost is that the id every sink log line
	// carries appears in no stats row, so nothing joins a CDC log back to the numbers
	// it produced without knowing the convention exists.
	//
	// Empty on the batch lane, where ExecutionID already IS the orchestration id and
	// there is nothing to say.
	OrchestrationExecutionID string

	Table       string
	StorageType string
	TraceID     string
	Ignore      bool
	Dataset     string
	DBOrSchema  string
	Dt          string
	RunMode     string

	// DestNamespace is the destination database/schema this message's rows land in,
	// for LABELLING stats only — the write paths keep using DBOrSchema.
	//
	// It exists because DBOrSchema is overloaded by design and the two meanings must
	// not be conflated (executor.go:4095-4118): for a relational destination it IS the
	// destination namespace, but for object storage it is a PATH SEGMENT in the bronze
	// key layout that the executor deliberately fills with the SOURCE schema. Reporting
	// that value as "the destination schema" would be the very mislabelling this field
	// exists to end, so it is empty rather than confidently wrong whenever this sink
	// cannot name a real destination namespace. See destinationNamespaceForStats.
	DestNamespace string

	BatchOffset int64
	RowCount    int64

	// inline
	Data []map[string]interface{}

	// minio
	ClaimCheckURL string

	// eof marker
	EOF           bool
	TotalReadRows int64

	// CDC-specific fields (Debezium envelope)
	IsCDC  bool
	CDCOp  string                 // c=create, u=update, d=delete, r=read (snapshot)
	Before map[string]interface{} // previous row state (for u/d)
	After  map[string]interface{} // new row state (for c/u/r)
	// IsSnapshot is true when this CDC event originates from a Debezium snapshot.
	// (Best-effort derived from payload.source.snapshot or op=="r".)
	IsSnapshot bool
	// PK is the primary key object parsed from the Kafka message key (Debezium key).
	// Example: {"event_id":1}
	PK       map[string]interface{}
	TxID     string // transaction ID for idempotency
	LSN      int64  // log sequence number
	SourceTS int64  // source timestamp (ms)
	// IngestionTS is the worker processing timestamp (ms).
	// This is always populated for CDC messages and is used as a fallback for SourceTS.
	IngestionTS int64
	// KeyFields is a best-effort list of primary key fields for this CDC message.
	// Debezium provides the record key as JSON (e.g. {"event_id":1}); using it avoids
	// ambiguous inference when a row has multiple *_id columns.
	KeyFields []string

	// ColumnTypes maps column name -> destination DDL type (best-effort) derived from
	// Kafka Connect schema metadata (Debezium with JsonConverter schemas enabled).
	// Used to avoid blanket TEXT columns in relational sinks.
	ColumnTypes map[string]string

	// ColumnTypesAreDDL is true when ColumnTypes holds FINAL destination DDL
	// types (DOUBLE PRECISION, JSONB, TIMESTAMPTZ, NUMERIC(38,4), …) already
	// resolved by deriveColumnTypesForDestination — i.e. the CDC path. When
	// false (batch/executor path), ColumnTypes are CANONICAL source names and
	// the destination connector must map them itself. The destination's
	// ensure_table uses this to decide passthrough vs. canonical mapping, so a
	// resolved "DOUBLE PRECISION" is not re-mapped down to TEXT.
	ColumnTypesAreDDL bool

	// ── Blob (raw-bytes passthrough) fields — universal-blob-passthrough plan §3.
	// Set ONLY when StorageType=="blob" (IsBlob). A blob message carries no row
	// payload: the object's bytes live byte-identical in the claim-check store at
	// ClaimCheckURL (an s3://… data_ref) and the destination object-storage
	// connector fetches them itself and writes them raw — bytes never transit the
	// sink. The capability gate already proved the destination accepts "blob".
	IsBlob      bool
	ObjectKey   string // source-relative object path; drives the destination key
	ContentType string // MIME carried verbatim from the source object
	Sha256      string // hex digest of the source bytes (integrity check at write)
	Size        int64  // source object size in bytes
	// StagingConfig are the claim-check MinIO creds (endpoint/access/secret/region/
	// bucket) the destination connector uses to fetch ClaimCheckURL. Forwarded from
	// the executor (which staged the bytes) so the sink holds no creds of its own.
	StagingConfig map[string]interface{}
}

type consumerTransformsCacheEntry struct {
	loadedAt time.Time
	raw      []map[string]interface{}
}

var consumerTransformsCache sync.Map // map[pipeline_id]consumerTransformsCacheEntry

const consumerTransformsCacheTTL = 30 * time.Second

func looksLikeUUID(s string) bool {
	s = strings.TrimSpace(s)
	if len(s) != 36 {
		return false
	}
	for _, pos := range []int{8, 13, 18, 23} {
		if s[pos] != '-' {
			return false
		}
	}
	return true
}

func loadConsumerTransformsRaw(ctx context.Context, pgDB *sql.DB, pipelineID string) ([]map[string]interface{}, error) {
	pipelineID = strings.TrimSpace(pipelineID)
	if pgDB == nil || pipelineID == "" {
		return nil, nil
	}
	// E2E/adhoc CDC runs sometimes use non-uuid pipeline_id; skip DB-backed transforms in that case.
	if !looksLikeUUID(pipelineID) {
		return nil, nil
	}

	if v, ok := consumerTransformsCache.Load(pipelineID); ok {
		if entry, ok := v.(consumerTransformsCacheEntry); ok {
			if time.Since(entry.loadedAt) < consumerTransformsCacheTTL {
				return entry.raw, nil
			}
		}
	}

	rows, err := pgDB.QueryContext(ctx, `
		SELECT id::text, transform_order, transform_config, enabled
		FROM transform_definitions
		WHERE pipeline_id = $1 AND transform_type = 'consumer'
		ORDER BY transform_order ASC
	`, pipelineID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]map[string]interface{}, 0)
	for rows.Next() {
		var id string
		var order int
		var cfgJSON []byte
		var enabled bool
		if err := rows.Scan(&id, &order, &cfgJSON, &enabled); err != nil {
			continue
		}
		cfg := map[string]interface{}{}
		if len(cfgJSON) > 0 {
			_ = json.Unmarshal(cfgJSON, &cfg)
		}
		out = append(out, map[string]interface{}{
			"id":               strings.TrimSpace(id),
			"transform_order":  order,
			"enabled":          enabled,
			"transform_config": cfg,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	consumerTransformsCache.Store(pipelineID, consumerTransformsCacheEntry{loadedAt: time.Now().UTC(), raw: out})
	return out, nil
}

// ensureExecutionRowForCDCAudit pre-creates the synthetic executions row that the
// CDC convention execution_id == pipeline_id (parseCDCMessage below) requires before
// any child row can reference it. TWO hard FKs depend on it:
// transform_execution_logs.execution_id (migration 045) and
// pipeline_batch_acks.fk_batch_acks_execution (migration 043). Best-effort by design:
// it only logs on failure, because both callers are audit-only paths.
func ensureExecutionRowForCDCAudit(ctx context.Context, pgDB *sql.DB, pipelineID, executionID string) {
	if pgDB == nil {
		return
	}
	pipelineID = strings.TrimSpace(pipelineID)
	executionID = strings.TrimSpace(executionID)
	if pipelineID == "" || executionID == "" {
		return
	}
	if !looksLikeUUID(pipelineID) || !looksLikeUUID(executionID) {
		return
	}
	// Best-effort: ensures the transform_execution_logs AND pipeline_batch_acks FKs to
	// executions don't fail for CDC (execution_id == pipeline_id).
	if _, err := pgDB.ExecContext(ctx, `
		INSERT INTO executions (id, pipeline_id, status, start_time, trigger_source)
		VALUES ($1, $2, 'running', NOW(), 'cdc')
		ON CONFLICT (id) DO NOTHING
	`, executionID, pipelineID); err != nil {
		logf("warning", "postgres warning: failed to ensure executions row (pipeline_id=%s execution_id=%s): %v", pipelineID, executionID, err)
	}
}

func upsertTransformExecutionLogPG(
	ctx context.Context,
	pgDB *sql.DB,
	pipelineID string,
	executionID string,
	tableName string,
	t transforms.CanonicalTransform,
	inputRows int,
	outputRows int,
	duration time.Duration,
	stepErr error,
) {
	if pgDB == nil {
		return
	}
	pipelineID = strings.TrimSpace(pipelineID)
	executionID = strings.TrimSpace(executionID)
	tableName = strings.TrimSpace(tableName)
	if pipelineID == "" || executionID == "" || tableName == "" {
		return
	}
	if !looksLikeUUID(pipelineID) || !looksLikeUUID(executionID) {
		return
	}

	status := "success"
	var errMsg interface{} = nil
	if stepErr != nil {
		status = "failed"
		errMsg = stepErr.Error()
	}
	snap, mErr := json.Marshal(t)
	if mErr != nil || len(snap) == 0 {
		snap = []byte(`{}`)
	}
	durMs := duration.Milliseconds()
	if durMs < 0 {
		durMs = 0
	}

	if _, err := pgDB.ExecContext(ctx, `
		INSERT INTO transform_execution_logs (
			pipeline_id, execution_id, table_name,
			transform_id, transform_order, transform_type,
			status, error_message,
			input_rows, output_rows, duration_ms,
			config_snapshot, updated_at
		) VALUES (
			$1, $2, $3,
			$4, $5, $6,
			$7, $8,
			$9, $10, $11,
			$12, now()
		)
		ON CONFLICT (pipeline_id, execution_id, table_name, transform_order)
		DO UPDATE SET
			transform_id = EXCLUDED.transform_id,
			transform_type = EXCLUDED.transform_type,
			status = CASE WHEN EXCLUDED.status = 'failed' THEN 'failed' ELSE transform_execution_logs.status END,
			error_message = COALESCE(EXCLUDED.error_message, transform_execution_logs.error_message),
			input_rows = transform_execution_logs.input_rows + EXCLUDED.input_rows,
			output_rows = transform_execution_logs.output_rows + EXCLUDED.output_rows,
			duration_ms = transform_execution_logs.duration_ms + EXCLUDED.duration_ms,
			config_snapshot = EXCLUDED.config_snapshot,
			updated_at = now()
	`, pipelineID, executionID, tableName, strings.TrimSpace(t.ID), t.Order, t.Type, status, errMsg, inputRows, outputRows, durMs, string(snap)); err != nil {
		logf("warning", "postgres warning: failed to upsert transform_execution_logs (pipeline_id=%s execution_id=%s table=%s order=%d): %v", pipelineID, executionID, tableName, t.Order, err)
	}
}

// applyConsumerTransforms applies consumer-hop transforms (transform_type='consumer')
// before the destination write. It runs on the CDC path ONLY.
//
// Batch transforms are applied exactly once upstream, by the executor's producer hop
// (backend-orchestrator executor.go applyTransformsToData, which loads
// transform_type='producer'). The NL-transforms gate persists each transform config as
// BOTH a producer AND a consumer row with identical config, so running the consumer hop
// here on a batch message would apply every transform a SECOND time — mask_pii would
// become sha256(sha256(x)), json_flatten would double-flatten, etc. Because row-reducing
// ops are idempotent, row counts still reconcile (input==output), so the double-apply is
// invisible to count checks and only shows up in the values. The !sm.IsCDC guard below
// enforces single-application on batch; CDC applies transforms only here (the producer
// hop does not run for CDC), so CDC is unchanged. This also keeps the hybrid
// cdc_initial_load=batch handoff correct: the historical load runs as batch (producer
// hop) and the streaming phase runs as CDC (this hop), each applying transforms once.
func applyConsumerTransforms(ctx context.Context, pgDB *sql.DB, cfg *WorkerConfig, sm *SinkMessage) error {
	if sm == nil {
		return nil
	}
	if !sm.IsCDC {
		// Batch path: transforms were already applied once by the executor's producer
		// hop. Applying the (identical) consumer transforms again here would double-apply.
		// See the doc-comment above. sm.IsCDC is set true only in the Debezium CDC parse
		// path; batch messages leave it false.
		return nil
	}
	if !looksLikeUUID(sm.PipelineID) || !looksLikeUUID(sm.ExecutionID) {
		// No control-plane DB-backed transforms for ad-hoc/non-pipeline runs.
		return nil
	}
	op := strings.ToLower(strings.TrimSpace(sm.CDCOp))
	if op == "d" {
		// CDC delete event carries no row body to transform.
		return nil
	}
	if len(sm.Data) == 0 {
		return nil
	}

	raw, err := loadConsumerTransformsRaw(ctx, pgDB, sm.PipelineID)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return nil
	}

	ensureExecutionRowForCDCAudit(ctx, pgDB, sm.PipelineID, sm.ExecutionID)

	canonical, _, err := transforms.NormalizeAndValidate(raw, sm.Table, transforms.NormalizeModeCDC)
	if err != nil {
		// A transform-validation failure here fail-closes the whole batch to the DLQ —
		// a silent drop for the table. The persisted consumer transform list is
		// authoritative and well-formed, so a failure almost always means we validated a
		// stale or partially-read cache entry. Drop the cache, re-read fresh from the DB
		// once, and re-validate before giving up: a transient empty read must not silently
		// drop a table. We deliberately do NOT skip the offending transform — it may be a
		// real mask_pii step, and skipping it would emit unmasked PII to the destination.
		consumerTransformsCache.Delete(strings.TrimSpace(sm.PipelineID))
		if raw2, rErr := loadConsumerTransformsRaw(ctx, pgDB, sm.PipelineID); rErr == nil && len(raw2) > 0 {
			if c2, _, e2 := transforms.NormalizeAndValidate(raw2, sm.Table, transforms.NormalizeModeCDC); e2 == nil {
				canonical = c2
				err = nil
			} else {
				err = e2
				raw = raw2
			}
		}
		if err != nil {
			// Previously opaque: surface the offending element so a genuine malformed
			// transform (vs. a transient read) can be distinguished from the DLQ line alone.
			firstOp := ""
			if len(raw) > 0 {
				if tc, ok := raw[0]["transform_config"].(map[string]interface{}); ok {
					firstOp, _ = tc["operation"].(string)
				}
			}
			logEvent("error", "consumer transform validation failed after fresh DB reload; batch -> DLQ",
				"pipeline_id", strings.TrimSpace(sm.PipelineID),
				"execution_id", strings.TrimSpace(sm.ExecutionID),
				"table", sm.Table,
				"reason", err.Error(),
				"raw_count", len(raw),
				"first_transform_op", firstOp,
			)
			return err
		}
	}
	if len(canonical) == 0 {
		return nil
	}

	rows := make([]transforms.Row, 0, len(sm.Data))
	for _, r := range sm.Data {
		rows = append(rows, transforms.Row(r))
	}

	tier1 := transforms.NewSimpleTransformEngine()
	tier2 := transforms.NewDuckDBTransformEngine()
	coordinator := transforms.NewTransformCoordinator(tier1, tier2)

	out := rows
	for _, t := range canonical {
		inRows := len(out)
		start := time.Now()
		stepOut, stepErr := coordinator.Apply(ctx, out, []transforms.Transform{t.EngineTransform()})
		dur := time.Since(start)

		outRows := 0
		if stepOut != nil {
			outRows = len(stepOut)
		}
		upsertTransformExecutionLogPG(ctx, pgDB, sm.PipelineID, sm.ExecutionID, sm.Table, t, inRows, outRows, dur, stepErr)
		if stepErr != nil {
			return stepErr
		}
		out = stepOut
	}

	sm.Data = make([]map[string]interface{}, 0, len(out))
	for _, r := range out {
		sm.Data = append(sm.Data, map[string]interface{}(r))
	}
	sm.RowCount = int64(len(sm.Data))

	// Reconcile destination column types for value-mutating transforms. ensure_table
	// derives each column's DDL type from the PRE-transform source schema (sm.ColumnTypes),
	// but mask_pii rewrites a value to a string hash and type_convert changes the type.
	// Without this, masking a non-string column (boolean verifiedEmail, numeric salary)
	// makes ensure_table CREATE a BOOLEAN/NUMERIC column the string value cannot be
	// inserted into — a deterministic dest-write failure and silent table drop.
	reconcileTransformedColumnTypes(sm, canonical)

	// Keep CDC envelope convenience fields in sync. For batch messages op is empty,
	// so this is skipped (batch has no After/Before envelope).
	if op == "c" || op == "u" || op == "r" {
		if len(sm.Data) > 0 {
			sm.After = sm.Data[0]
		} else {
			sm.After = nil
		}
	}
	return nil
}

// reconcileTransformedColumnTypes updates sm.ColumnTypes so the destination DDL matches
// the POST-transform value type for value-type-mutating consumer transforms. Only
// mask_pii (-> string) and type_convert (-> its target) change a value's type; all other
// ops preserve it. json_flatten's new columns are absent from sm.ColumnTypes and fall
// through to the connector's TEXT default, so they need no entry here.
//
// The override is written in the SAME vocabulary the message already carries: final DDL
// tokens on the CDC path (sm.ColumnTypesAreDDL == true, e.g. "TEXT"/"BIGINT") and
// canonical source tokens on the batch/executor path (e.g. "string"/"integer"). The
// chosen DDL tokens (TEXT, BIGINT, DOUBLE PRECISION, BOOLEAN) are valid in both
// PostgreSQL and MySQL and are re-normalized connector-side.
func reconcileTransformedColumnTypes(sm *SinkMessage, canonical []transforms.CanonicalTransform) {
	if sm == nil || len(canonical) == 0 {
		return
	}
	set := func(col, ddlType, canonicalType string) {
		c := unqualifiedColumn(col)
		if c == "" {
			return
		}
		if sm.ColumnTypes == nil {
			sm.ColumnTypes = make(map[string]string, 4)
		}
		if sm.ColumnTypesAreDDL {
			sm.ColumnTypes[c] = ddlType
		} else {
			sm.ColumnTypes[c] = canonicalType
		}
	}
	for _, t := range canonical {
		switch t.Type {
		case "mask_pii":
			// hash/redact/partial all yield a string value regardless of input type.
			for _, col := range maskedColumns(t.Config) {
				set(col, "TEXT", "string")
			}
		case "type_convert":
			to := strings.ToLower(strings.TrimSpace(toString(t.Config["to"])))
			ddlType, canonicalType, ok := convertTargetType(to)
			if !ok {
				continue
			}
			set(toString(t.Config["column"]), ddlType, canonicalType)
		}
	}
}

// unqualifiedColumn strips a leading "table." qualifier, mirroring the transform engine's
// lastIdent so the key matches sm.ColumnTypes (keyed by bare column name).
func unqualifiedColumn(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.LastIndex(s, "."); i >= 0 && i < len(s)-1 {
		return s[i+1:]
	}
	return s
}

// maskedColumns returns the column(s) a mask_pii transform targets (config.column or
// config.columns).
func maskedColumns(cfg map[string]any) []string {
	if cfg == nil {
		return nil
	}
	if c := strings.TrimSpace(toString(cfg["column"])); c != "" {
		return []string{c}
	}
	return toStringSlice(cfg["columns"])
}

// convertTargetType maps a type_convert "to" target to (ddlType, canonicalType, ok).
func convertTargetType(to string) (string, string, bool) {
	switch to {
	case "string":
		return "TEXT", "string", true
	case "int", "integer":
		return "BIGINT", "integer", true
	case "float", "double":
		return "DOUBLE PRECISION", "double", true
	case "bool", "boolean":
		return "BOOLEAN", "boolean", true
	}
	return "", "", false
}

type fatalError struct {
	err error
}

func (e fatalError) Error() string {
	if e.err == nil {
		return "fatal error"
	}
	return e.err.Error()
}

func (e fatalError) Unwrap() error { return e.err }

func isFatal(err error) bool {
	if err == nil {
		return false
	}
	var fe fatalError
	return errors.As(err, &fe)
}

// poisonError marks a message that is DETERMINISTICALLY un-processable — retrying
// can never succeed (e.g. a keyless CDC delete routed to an upsert destination that
// needs a PK, or a change event with no row payload at all). It is deliberately
// NOT a fatalError: a fatalError halts the worker (os.Exit) for a genuinely
// unrecoverable condition (DLQ down, destructive schema change), but a poison
// message must instead be dead-lettered + committed so the worker advances and keeps
// delivering every OTHER table. Treating poison as fatal is a crash-loop trap: the
// offset never commits, the supervisor respawns into the same message forever, and
// the whole pipeline delivers nothing. See KI-SINK-KEYLESS-DELETE-CRASHLOOP.
type poisonError struct {
	err error
}

func (e poisonError) Error() string {
	if e.err == nil {
		return "poison message"
	}
	return e.err.Error()
}

func (e poisonError) Unwrap() error { return e.err }

func isPoison(err error) bool {
	if err == nil {
		return false
	}
	var pe poisonError
	return errors.As(err, &pe)
}

type manifest struct {
	PipelineID   string   `json:"pipeline_id"`
	ExecutionID  string   `json:"execution_id"`
	Dt           string   `json:"dt"`
	UploadedKeys []string `json:"uploaded_keys"`
	RowCounts    []int64  `json:"row_counts"`
	TotalRows    int64    `json:"total_rows"`
	CreatedAt    string   `json:"created_at"`
}

type tableWriteState struct {
	dataset    string
	dbOrSchema string
	dt         string
	runMode    string
	keys       []string
	rowCounts  []int64
	// Ensure reload cleanup is only attempted once per execution+table.
	reloadCleaned bool
}

type cdcBatchingParams struct {
	enabled               bool
	maxEvents             int
	maxBytes              int
	flushInterval         time.Duration
	maxRetries            int
	backoff               time.Duration
	absoluteMaxEventBytes int
}

func resolveCDCBatchingParams(cfg *WorkerConfig) cdcBatchingParams {
	// Plan defaults.
	p := cdcBatchingParams{
		enabled: true,
		// Bigger batches amortize the fixed per-flush cost (ensure_table pre-call,
		// MCP round-trip, commit) over more rows once the batched ack-ledger fix
		// removes the per-message bottleneck. Kept conservative for the sink's
		// 256MB container limit; maxBytes raised in lockstep so ~2.7KB CDC events
		// hit the row cap (2000) before the byte cap. Larger batches lose no data —
		// uncommitted rows are redelivered from Kafka and idempotently re-applied.
		maxEvents:             2000,
		maxBytes:              24 * 1024 * 1024, // 24MB
		flushInterval:         30 * time.Second,
		maxRetries:            3,
		backoff:               1000 * time.Millisecond,
		absoluteMaxEventBytes: 100 * 1024 * 1024, // 100MB
	}
	if cfg == nil || cfg.KafkaSinkWorker == nil || cfg.KafkaSinkWorker.CDCBatching == nil {
		return p
	}
	bc := cfg.KafkaSinkWorker.CDCBatching
	if bc.Enabled != nil {
		p.enabled = *bc.Enabled
	}
	if bc.MaxEvents > 0 {
		p.maxEvents = bc.MaxEvents
	}
	if bc.MaxBytes > 0 {
		p.maxBytes = bc.MaxBytes
	}
	if bc.FlushIntervalSeconds > 0 {
		p.flushInterval = time.Duration(bc.FlushIntervalSeconds) * time.Second
	}
	if bc.MaxRetries > 0 {
		p.maxRetries = bc.MaxRetries
	}
	if bc.BackoffMS > 0 {
		p.backoff = time.Duration(bc.BackoffMS) * time.Millisecond
	}
	if bc.AbsoluteMaxEventBytes > 0 {
		p.absoluteMaxEventBytes = bc.AbsoluteMaxEventBytes
	}
	return p
}

type cdcObjectBatch struct {
	topic       string
	partition   int
	table       string
	format      string
	compression string

	bucket    string
	container string
	prefix    string

	// partSegs is the Hive "col=val/" prefix and timeSeg the "dt=…" time-bucket
	// segment for this batch, resolved from the destination partition_by /
	// partition_time_granularity config. Both empty → legacy dt-only layout. They are
	// part of the batch key, so every event in a batch shares the same partition path.
	partSegs string
	timeSeg  string

	firstOffset  int64
	lastOffset   int64
	firstEventTS int64

	bytes        int
	events       []map[string]interface{}
	messages     []kafka.Message
	sms          []*SinkMessage
	createdAt    time.Time
	lastAppendAt time.Time
}

// highWaterTracker holds the durable per-(topic,partition) Kafka offsets that
// have already been written to the destination. It replaces the Redis dedup
// cache for CDC correctness: the destination's _rsync_cdc_offsets table (written
// in the same transaction as the data for Tier-A relational sinks) is the sole
// source of truth. On startup the tracker is SEEDED from <dest>_get_cdc_offsets;
// during the run it is ADVANCED after each successful destination write + Kafka
// commit. A redelivered message whose offset <= the high-water mark for its
// (topic,partition) is a duplicate and is skipped — providing idempotency on
// recovery when the Kafka commit lagged the durable destination offset.
type highWaterTracker struct {
	mu sync.Mutex
	hw map[string]int64 // key: "topic|partition" -> last durably-written offset
}

func newHighWaterTracker() *highWaterTracker {
	return &highWaterTracker{hw: map[string]int64{}}
}

func hwKey(topic string, partition int) string {
	return fmt.Sprintf("%s|%d", topic, partition)
}

// seen reports whether offset has already been durably written for (topic,partition).
func (t *highWaterTracker) seen(topic string, partition int, offset int64) bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	h, ok := t.hw[hwKey(topic, partition)]
	return ok && offset <= h
}

// advance raises the high-water mark for (topic,partition) to max(current, offset).
func (t *highWaterTracker) advance(topic string, partition int, offset int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	k := hwKey(topic, partition)
	if cur, ok := t.hw[k]; !ok || offset > cur {
		t.hw[k] = offset
	}
}

// seed merges durable destination offsets into the tracker at startup.
func (t *highWaterTracker) seed(offsets map[string]int64) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for k, v := range offsets {
		if cur, ok := t.hw[k]; !ok || v > cur {
			t.hw[k] = v
		}
	}
}

// callGetCDCOffsets fetches durable per-partition high-water offsets from the
// destination's get_cdc_offsets tool and returns them keyed "topic|partition".
// Returns an empty map (not an error) when the destination has no offset table
// yet, or when the destination does not implement get_cdc_offsets (Tier C object
// stores rely on deterministic keys for idempotency instead).
func callGetCDCOffsets(ctx context.Context, httpClient *http.Client, cfg *WorkerConfig, destType string) (map[string]int64, error) {
	out := map[string]int64{}
	args := map[string]interface{}{
		"config":      cfg.DestinationConfig,
		"pipeline_id": cfg.PipelineID,
	}
	res, err := callDestinationTool(ctx, httpClient, cfg, destType, "get_cdc_offsets", args)
	if err != nil {
		return out, err
	}
	rawOffsets, ok := res["offsets"].([]interface{})
	if !ok {
		return out, nil
	}
	for _, item := range rawOffsets {
		m, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		topic, _ := m["topic"].(string)
		if strings.TrimSpace(topic) == "" {
			continue
		}
		partition := int(toInt64(m["partition"]))
		offset := toInt64(m["offset"])
		k := hwKey(topic, partition)
		if cur, ok := out[k]; !ok || offset > cur {
			out[k] = offset
		}
	}
	return out, nil
}

type cdcObjectBatcher struct {
	cfg      *WorkerConfig
	destType string
	destCfg  map[string]interface{}
	params   cdcBatchingParams
	ackTTL   time.Duration

	reader       *kafka.Reader
	hw           *highWaterTracker
	pgDB         *sql.DB
	httpClient   *http.Client
	eventsWriter *kafka.Writer
	dlqWriter    *kafka.Writer
	metrics      *Metrics

	cdcInserts *sync.Map
	cdcUpdates *sync.Map
	cdcDeletes *sync.Map
	cdcBytes   *sync.Map

	batches map[string]*cdcObjectBatch
}

// cdcDBBatch accumulates CDC upsert rows for a single (topic, partition, table) key.
// On flush, all buffered rows are written in a single MCP upsert_data call instead
// of one call per row, giving ~1000x throughput improvement for snapshot loads.
type cdcDBBatch struct {
	table       string
	targetTable string // normalized destination table name
	topic       string
	partition   int

	rows        []map[string]interface{}
	messages    []kafka.Message
	sms         []*SinkMessage
	keyFields   []string          // PK/key columns for upsert ON CONFLICT
	columnTypes map[string]string // Debezium column type hints for DDL

	firstOffset  int64
	lastOffset   int64
	createdAt    time.Time
	lastAppendAt time.Time
}

// cdcDBBatcher buffers CDC insert/update events and flushes them to relational
// database destinations in bulk (N rows per MCP call instead of 1 per call).
// Deletes are passed through immediately to preserve ordering.
// Only used for non-object-storage, non-warehouse destinations (PostgreSQL, MySQL, etc.).
type cdcDBBatcher struct {
	cfg      *WorkerConfig
	destType string
	destCfg  map[string]interface{}
	params   cdcBatchingParams
	ackTTL   time.Duration

	reader       *kafka.Reader
	hw           *highWaterTracker
	pgDB         *sql.DB
	httpClient   *http.Client
	ddl          *DDLSupport
	eventsWriter *kafka.Writer
	dlqWriter    *kafka.Writer
	metrics      *Metrics

	cdcInserts *sync.Map
	cdcUpdates *sync.Map
	cdcDeletes *sync.Map
	cdcBytes   *sync.Map

	batches map[string]*cdcDBBatch // key: "topic|partition|table"
}

func newCDCObjectBatcher(cfg *WorkerConfig, destType string, reader *kafka.Reader, hw *highWaterTracker, pgDB *sql.DB, httpClient *http.Client,
	eventsWriter *kafka.Writer, dlqWriter *kafka.Writer, metrics *Metrics, ackTTL time.Duration,
	cdcInserts, cdcUpdates, cdcDeletes, cdcBytes *sync.Map) *cdcObjectBatcher {

	if cfg == nil || reader == nil || httpClient == nil || metrics == nil {
		return nil
	}
	if !isObjectStorageConnector(destType) {
		return nil
	}
	p := resolveCDCBatchingParams(cfg)
	if !p.enabled {
		return nil
	}
	// Group C file rolling: let the destination's max_file_rows / max_file_mb cap each
	// bronze object's size, overriding the global CDC batching defaults. The object
	// batcher already rolls on events|bytes|interval (whichever first), so this is a
	// pure parameter override — the AWS DMS CdcMinFileSize/MaxFileSize analog (interval
	// stays governed by cdc_batching.flush_interval_seconds / the 30 s default).
	if maxRows, maxBytes := objectFileRollLimits(cfg.DestinationConfig); maxRows > 0 || maxBytes > 0 {
		if maxRows > 0 {
			p.maxEvents = maxRows
		}
		if maxBytes > 0 {
			p.maxBytes = maxBytes
		}
	}
	return &cdcObjectBatcher{
		cfg:          cfg,
		destType:     canonicalConnectorType(destType),
		destCfg:      cfg.DestinationConfig,
		params:       p,
		ackTTL:       ackTTL,
		reader:       reader,
		hw:           hw,
		pgDB:         pgDB,
		httpClient:   httpClient,
		eventsWriter: eventsWriter,
		dlqWriter:    dlqWriter,
		metrics:      metrics,
		cdcInserts:   cdcInserts,
		cdcUpdates:   cdcUpdates,
		cdcDeletes:   cdcDeletes,
		cdcBytes:     cdcBytes,
		batches:      map[string]*cdcObjectBatch{},
	}
}

// newCDCDBBatcher creates a batcher for relational DB CDC sinks (PostgreSQL, MySQL, …).
// Returns nil if batching is disabled, or if the destination is object storage / warehouse
// (those have their own batching paths).
func newCDCDBBatcher(cfg *WorkerConfig, destType string, reader *kafka.Reader, hw *highWaterTracker, pgDB *sql.DB,
	httpClient *http.Client, ddl *DDLSupport, eventsWriter *kafka.Writer, dlqWriter *kafka.Writer,
	metrics *Metrics, ackTTL time.Duration, cdcInserts, cdcUpdates, cdcDeletes, cdcBytes *sync.Map) *cdcDBBatcher {

	if cfg == nil || reader == nil || httpClient == nil || metrics == nil {
		return nil
	}
	// Object storage uses cdcObjectBatcher; warehouses use the merge path in processCDCEvent.
	if isObjectStorageConnector(destType) || isDataWarehouseConnector(destType) {
		return nil
	}
	p := resolveCDCBatchingParams(cfg)
	if !p.enabled {
		return nil
	}
	// Use a shorter default flush interval for relational sinks (5 s instead of 30 s)
	// so streaming-mode deltas land quickly while snapshot-mode still batches to 1000 rows.
	if cfg.KafkaSinkWorker == nil || cfg.KafkaSinkWorker.CDCBatching == nil ||
		cfg.KafkaSinkWorker.CDCBatching.FlushIntervalSeconds == 0 {
		p.flushInterval = 5 * time.Second
	}
	return &cdcDBBatcher{
		cfg:          cfg,
		destType:     canonicalConnectorType(destType),
		destCfg:      cfg.DestinationConfig,
		params:       p,
		ackTTL:       ackTTL,
		reader:       reader,
		hw:           hw,
		pgDB:         pgDB,
		httpClient:   httpClient,
		ddl:          ddl,
		eventsWriter: eventsWriter,
		dlqWriter:    dlqWriter,
		metrics:      metrics,
		cdcInserts:   cdcInserts,
		cdcUpdates:   cdcUpdates,
		cdcDeletes:   cdcDeletes,
		cdcBytes:     cdcBytes,
		batches:      map[string]*cdcDBBatch{},
	}
}

func (b *cdcDBBatcher) batchKey(msg kafka.Message, sm *SinkMessage) string {
	return fmt.Sprintf("%s|%d|%s", msg.Topic, msg.Partition, sm.Table)
}

// add buffers one CDC upsert event (op=c/r/u). Deletes are NOT added here; call
// processCDCEvent directly for deletes (after flushing pending upserts for the table).
func (b *cdcDBBatcher) add(ctx context.Context, msg kafka.Message, sm *SinkMessage) {
	if b == nil || sm == nil {
		return
	}

	// Dedup hot-path: if this offset is at/below the durable high-water mark for
	// its (topic,partition), it was already written to the destination — skip + commit.
	if b.hw.seen(msg.Topic, msg.Partition, msg.Offset) {
		atomic.AddUint64(&b.metrics.skipped, 1)
		logMsgEvent("debug", sm, msg, "cdc dedup: offset at/below high-water, skipping")
		_ = b.reader.CommitMessages(ctx, msg)
		return
	}

	// Extract the row to write.  For upserts use sm.Data[0] / sm.After; fall back to PK.
	var row map[string]interface{}
	if len(sm.Data) > 0 {
		row = sm.Data[0]
	} else if sm.After != nil {
		row = sm.After
	}
	if row == nil {
		// Nothing to write; ack and move on.
		atomic.AddUint64(&b.metrics.skipped, 1)
		logMsgEvent("debug", sm, msg, "cdc event has no writable row (no Data/After), skipping")
		_ = b.reader.CommitMessages(ctx, msg)
		return
	}

	// Determine key fields for ON CONFLICT (upsert).
	keyFields := []string{}
	for _, k := range []string{"key_fields", "primary_keys", "primary_key_fields"} {
		if b.destCfg != nil {
			if v, ok := b.destCfg[k]; ok {
				if kf := toStringSlice(v); len(kf) > 0 {
					keyFields = kf
					break
				}
			}
		}
	}
	if len(keyFields) == 0 && sm != nil && len(sm.KeyFields) > 0 {
		keyFields = append([]string{}, sm.KeyFields...)
	}
	if len(keyFields) == 0 {
		keyFields = inferKeyFieldsForRow(row)
	}

	targetTable := normalizeTargetTable(b.destType, b.destCfg, sm.Table)
	// Per-pipeline destination namespace (CDC): send a BARE table so the connector
	// applies the namespace (forwarded separately on the write/ensure calls). A
	// qualified table would make the connector ignore the namespace and leak the
	// source schema. No-op when no real namespace is set.
	if isRealNamespace(b.cfg.DestinationNamespace) {
		targetTable = bareTableForNamespace(targetTable)
	}
	key := b.batchKey(msg, sm)
	now := time.Now().UTC()

	batch := b.batches[key]
	if batch == nil {
		batch = &cdcDBBatch{
			table:        sm.Table,
			targetTable:  targetTable,
			topic:        msg.Topic,
			partition:    msg.Partition,
			keyFields:    keyFields,
			columnTypes:  sm.ColumnTypes,
			firstOffset:  msg.Offset,
			lastOffset:   msg.Offset,
			createdAt:    now,
			lastAppendAt: now,
		}
		b.batches[key] = batch
	}

	// Flush on threshold BEFORE appending (same pattern as cdcObjectBatcher).
	if len(batch.rows) > 0 && len(batch.rows)+1 > b.params.maxEvents {
		b.flushBatch(ctx, key, batch, "threshold_flush")
		// Recreate after flush.
		batch = nil
		if b2 := b.batches[key]; b2 != nil {
			batch = b2
		}
		if batch == nil {
			batch = &cdcDBBatch{
				table:        sm.Table,
				targetTable:  targetTable,
				topic:        msg.Topic,
				partition:    msg.Partition,
				keyFields:    keyFields,
				columnTypes:  sm.ColumnTypes,
				firstOffset:  msg.Offset,
				lastOffset:   msg.Offset,
				createdAt:    now,
				lastAppendAt: now,
			}
			b.batches[key] = batch
		}
	}

	batch.rows = append(batch.rows, row)
	batch.messages = append(batch.messages, msg)
	batch.sms = append(batch.sms, sm)
	batch.lastOffset = msg.Offset
	batch.lastAppendAt = now
	// Merge key fields if later messages carry richer info.
	if len(batch.keyFields) == 0 && len(keyFields) > 0 {
		batch.keyFields = keyFields
	}

	// Flush immediately when threshold exactly reached.
	if len(batch.rows) >= b.params.maxEvents {
		b.flushBatch(ctx, key, batch, "threshold_reached_flush")
	}
}

// flushDue flushes all batches that have exceeded the flush interval.
func (b *cdcDBBatcher) flushDue(ctx context.Context, now time.Time) {
	if b == nil {
		return
	}
	for key, batch := range b.batches {
		if batch == nil || len(batch.rows) == 0 {
			continue
		}
		if now.Sub(batch.createdAt) >= b.params.flushInterval {
			b.flushBatch(ctx, key, batch, "interval_flush")
		}
	}
}

// flushTable flushes any pending upsert batch for the given (topic, partition, table)
// and returns how many buffered rows it flushed (0 when nothing was buffered).
// Called before processing a delete to preserve write ordering.
//
// The count is returned so the caller can log it: "did this delete race its own
// table's buffered upserts?" is the write-ordering question a wrong-row delete
// raises, and it is unanswerable without knowing whether anything was pending.
func (b *cdcDBBatcher) flushTable(ctx context.Context, topic string, partition int, table string) int {
	if b == nil {
		return 0
	}
	key := fmt.Sprintf("%s|%d|%s", topic, partition, table)
	if batch, ok := b.batches[key]; ok && batch != nil && len(batch.rows) > 0 {
		// Read the count BEFORE flushing: flushBatch → commitFlushedBatch removes
		// the batch from b.batches, so len(batch.rows) is not readable afterwards.
		n := len(batch.rows)
		b.flushBatch(ctx, key, batch, "pre_delete_flush")
		return n
	}
	return 0
}

// flushAll flushes every pending batch (e.g. on context cancellation or worker shutdown).
func (b *cdcDBBatcher) flushAll(ctx context.Context) {
	if b == nil {
		return
	}
	for key, batch := range b.batches {
		if batch != nil && len(batch.rows) > 0 {
			b.flushBatch(ctx, key, batch, "shutdown_flush")
		}
	}
}

// flushBatch writes all buffered rows for one (topic|partition|table) key to the
// destination in a single upsert_data call, then persists per-message acks and
// commits the Kafka offsets. Fail-closed: exits on unrecoverable errors.
// commitFlushedBatch is everything a landed CDC batch owes: the audit ledger,
// the per-table counters, the Kafka offset commit + high-water advance, and the
// TABLE_STATS emit. Split out of flushBatch so the extended infrastructure-fault
// retry can reuse it when a destination comes back mid-hold, rather than keeping
// a second copy of the commit sequence in step with this one
// (KI-CDC-SINK-INFRA-FAULT-DLQ-COMMITS).
func (b *cdcDBBatcher) commitFlushedBatch(ctx context.Context, key string, batch *cdcDBBatch) {
	// Per-message bookkeeping. The durable offset already landed in
	// _rsync_cdc_offsets within the upsert transaction above; the Postgres ledger
	// write here is a best-effort audit trail only (non-fatal on error).
	// Best-effort audit ledger for the ENTIRE batch in one (chunked) INSERT
	// instead of one remote round-trip per message — the high-volume CDC
	// throughput fix. Non-fatal: exactly-once is enforced by _rsync_cdc_offsets
	// committed in the upsert transaction above, not by this ledger.
	if b.pgDB != nil {
		if ackErr := persistCDCAcksBatch(ctx, b.pgDB, batch.sms, batch.messages, batch.targetTable); ackErr != nil {
			logf("warning", "cdc ack-ledger batch write failed (non-fatal, audit only): %v", ackErr)
		}
	}

	// Update per-table CDC counters (in-memory).
	for _, sm := range batch.sms {
		switch strings.ToLower(strings.TrimSpace(sm.CDCOp)) {
		case "c":
			incrementCounter(b.cdcInserts, sm.Table, 1)
			atomic.AddUint64(&b.metrics.cdcInserts, 1)
		case "r":
			incrementCounter(b.cdcInserts, sm.Table, 1)
			atomic.AddUint64(&b.metrics.cdcReads, 1)
		case "u":
			incrementCounter(b.cdcUpdates, sm.Table, 1)
			atomic.AddUint64(&b.metrics.cdcUpdates, 1)
		}
		incrementCounter(b.cdcBytes, sm.Table, cdcRowBytes(sm))
	}

	// Commit Kafka offsets for the entire batch in one call, then advance the
	// in-memory high-water mark so any redelivery of these offsets is skipped.
	_ = b.reader.CommitMessages(ctx, batch.messages...)
	last := batch.messages[len(batch.messages)-1]
	b.hw.advance(batch.topic, batch.partition, last.Offset)
	atomic.StoreInt64(&b.metrics.lastCommittedOffset, last.Offset)
	atomic.StoreInt64(&b.metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
	atomic.StoreInt64(&b.metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())

	// Emit TABLE_STATS with running totals.
	lastSM := batch.sms[len(batch.sms)-1]
	inserts := loadCounter(b.cdcInserts, lastSM.Table)
	updates := loadCounter(b.cdcUpdates, lastSM.Table)
	deletes := loadCounter(b.cdcDeletes, lastSM.Table)
	_ = emitCDCTableStats(ctx, b.eventsWriter, lastSM, inserts, updates, deletes, loadCounter(b.cdcBytes, lastSM.Table), loadCounter(&b.metrics.dlqByTable, lastSM.Table))

	atomic.AddUint64(&b.metrics.processed, uint64(len(batch.rows)))
	delete(b.batches, key)
}

func (b *cdcDBBatcher) flushBatch(ctx context.Context, key string, batch *cdcDBBatch, reason string) {
	if b == nil || batch == nil || len(batch.rows) == 0 {
		return
	}

	// Schema evolution is handled by ensureDestinationTable's additive reconciliation
	// (CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS) — the destination's live
	// schema IS the source of truth, so no separate Redis-cached gate is needed.
	// Net-additive changes apply automatically; incompatible changes surface as a write
	// error and escalate via the fail-closed os.Exit path below.
	// cdcDBBatcher is the CDC apply path exclusively, so batch.columnTypes are
	// always resolved destination DDL types → pass through verbatim (types_are_ddl).
	// cdcDBBatcher buffers insert/update (full-image) events only — deletes bypass it —
	// so enabling the fail-closed drop gate here is safe (no PK-only delete images).
	// Skip the gate in append-only history mode, where tombstone rows are intentional.
	//
	// Document-DB destinations (MongoDB) have no DDL and no ensure_table tool —
	// collections auto-create on first write — so skip the relational reconcile entirely.
	if !isDocumentDBConnector(b.destType) {
		if err := ensureDestinationTable(ctx, b.httpClient, b.cfg, b.ddl, b.destType, batch.targetTable,
			strings.TrimSpace(b.cfg.DestinationNamespace), batch.rows, batch.keyFields, batch.columnTypes, b.cfg.PipelineID, true, !cdcAppendMode(b.cfg), true); err != nil {
			if isFatal(err) {
				// Destructive source schema change: halt fail-closed so the offset is
				// not advanced past a row the destination can no longer faithfully hold.
				logf("error", "fatal: destructive schema change in CDC batch for %s: %v", batch.targetTable, err)
				os.Exit(1)
			}
			// Non-fatal: log and proceed; destination write may still succeed.
			// Surface it on the metrics endpoint too. Proceeding is correct — the
			// upsert below is itself fail-closed (retry → setErr → per-row
			// isolation → DLQ → os.Exit) — but a log line alone is invisible to
			// the health scrape, so a destination whose reconcile fails on every
			// flush while writes keep succeeding looks perfectly healthy. This is
			// a visibility fix ONLY; it deliberately does not change halt
			// semantics.
			logf("warning", "warn: ensureDestinationTable failed (will retry on next flush): %v", err)
			b.metrics.setErr(fmt.Errorf("ensureDestinationTable %s: %w", batch.targetTable, err))
		}
	}

	// Build the upsert_data MCP call with ALL buffered rows.
	// kafka_offset is written into the destination's _rsync_cdc_offsets table in the
	// SAME transaction as the data (Tier-A relational sinks) → exactly-once: the durable
	// offset advances iff the data commits. batch.lastOffset is the highest offset in
	// this batch for (topic,partition).
	args := map[string]interface{}{
		"config":    b.destCfg,
		"table":     batch.targetTable,
		"data":      batch.rows,
		"operation": "upsert",
		"kafka_offset": map[string]interface{}{
			"pipeline_id": b.cfg.PipelineID,
			"topic":       batch.topic,
			"partition":   batch.partition,
			"offset":      batch.lastOffset,
		},
	}
	// Forward the destination namespace on the write (batch.targetTable is bare);
	// no-op when unset. Mirrors the batch path's writeToDestination.
	addNamespaceParam(args, b.cfg.DestinationNamespace)
	if len(batch.keyFields) > 0 {
		args["key_fields"] = batch.keyFields
		args["primary_key_fields"] = batch.keyFields
	}
	// Forward the Debezium-derived destination column types on the WRITE call too,
	// not just the ensure_table pre-call. If ensureDestinationTable is skipped (its
	// in-memory "already ensured" cache) or loses the race, the connector's upsert
	// auto-creates the table — and without these it would default every column to
	// TEXT. With them, the on-the-fly create is correctly typed. See
	// CAPABILITIES.md KI-CDC-TYPE-RACE.
	if len(batch.columnTypes) > 0 {
		args["column_types"] = batch.columnTypes
	}

	// Keyless table + not append-mode → synthetic-PK path. ensureDestinationTable
	// created _rsync_row_hash (NOT NULL) + a unique index for exactly this case; the
	// WRITE call must compute the hash via upsert_data(synthetic_pk=true), or the
	// connector plain-inserts a NULL hash and the NOT NULL constraint rejects every
	// row — a silent drop of a keyless/GIPK table. Append-mode keeps plain INSERT.
	//
	// A document DB never synthesizes a PK: MongoDB auto-assigns _id, so a keyless
	// source is a plain insert (import_data). Excluded here so keyless → import_data below.
	useSyntheticPK := len(batch.keyFields) == 0 && !cdcAppendMode(b.cfg) && !isDocumentDBConnector(b.destType)
	if useSyntheticPK {
		args["synthetic_pk"] = true
	}
	toolName := fmt.Sprintf("%s_upsert_data", b.destType)
	if len(batch.keyFields) == 0 && !useSyntheticPK {
		toolName = fmt.Sprintf("%s_import_data", b.destType)
	}

	var lastErr error
	for attempt := 0; attempt <= b.params.maxRetries; attempt++ {
		result, err := callDestinationTool(ctx, b.httpClient, b.cfg, b.destType, strings.TrimPrefix(toolName, b.destType+"_"), args)
		if err != nil {
			lastErr = err
			if attempt < b.params.maxRetries {
				sleep := b.params.backoff * time.Duration(1<<attempt)
				sleep += time.Duration(rand.Intn(250)) * time.Millisecond
				time.Sleep(sleep)
			}
			continue
		}

		// Require the destination to report an actual write-count. A success
		// response WITHOUT a count field is the silent-data-loss signature: the
		// connector returned ok but may not have persisted. Do NOT fabricate
		// len(batch.rows) (the old behavior) — that committed Kafka offsets for
		// rows that never landed. Treat a missing count as a retryable failure;
		// after maxRetries flushBatch fails closed (no offset commit) below. A
		// legitimate zero (count field present and == 0) is trusted as-is.
		writtenRows, ok := extractDestRowCount(result)
		if !ok {
			lastErr = fmt.Errorf("dest %s returned success without a write-count field (possible non-landing write); table=%s rows=%d",
				toolName, batch.targetTable, len(batch.rows))
			if attempt < b.params.maxRetries {
				sleep := b.params.backoff * time.Duration(1<<attempt)
				sleep += time.Duration(rand.Intn(250)) * time.Millisecond
				time.Sleep(sleep)
			}
			continue
		}

		// KI-CDC-DELETE-PATH-UNLOGGED: this is what finally emits `pre_delete_flush`.
		// The reason string was threaded all the way down here and then discarded —
		// it reached a logger only on the FAILURE branches below, so on the happy
		// path nothing ever recorded WHY a batch flushed. writtenRows was discarded
		// too, under a comment claiming it was "consumed for logging purposes" while
		// nothing logged it.
		//
		// Emitted BEFORE commitFlushedBatch so the line exists even if the commit
		// path exits; it never alters control flow.
		logEvent("info", "cdc batch flushed",
			"flush_reason", reason,
			"table", batch.targetTable,
			"tool", toolName,
			"rows", len(batch.rows),
			"rows_written", writtenRows,
			"topic", batch.topic,
			"partition", batch.partition,
			"offset", batch.lastOffset)
		b.commitFlushedBatch(ctx, key, batch)
		return
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("cdc db batch flush failed after %d retries", b.params.maxRetries)
	}
	atomic.AddUint64(&b.metrics.failed, 1)
	b.metrics.setErr(lastErr)

	// KI-CDC-SINK-INFRA-FAULT-DLQ-COMMITS — an infrastructure fault is not a poison
	// row. The destination never saw these rows (the connector is unreachable, the
	// database behind it is restarting, DNS is down), so dead-lettering them and
	// committing their offsets deletes them from the stream: Kafka never redelivers
	// a committed offset, and the DLQ topic has no reachable consumer. The retry
	// loop above spends only ~7s at the defaults, so a routine destination restart
	// used to be enough to lose data.
	//
	// Hold the offsets instead. Spend a much larger budget re-attempting the same
	// write, and if the destination is still gone at the end of it, fail closed
	// exactly as cdcObjectBatcher.flushBatch does — no DLQ, no commit, exit, and let
	// the supervisor respawn into the uncommitted batch. This gate runs BEFORE
	// per-row isolation on purpose: during an outage every row looks individually
	// poisonous, so per-row isolation would convert one infrastructure error into a
	// whole-batch DLQ.
	if isDestInfraFault(lastErr) {
		var landed bool
		lastErr, landed = holdForInfraFault(ctx, "cdc db batch", batch.targetTable, lastErr, func() error {
			result, err := callDestinationTool(ctx, b.httpClient, b.cfg, b.destType, strings.TrimPrefix(toolName, b.destType+"_"), args)
			if err != nil {
				return err
			}
			if _, ok := extractDestRowCount(result); !ok {
				return fmt.Errorf("dest %s returned success without a write-count field (possible non-landing write); table=%s rows=%d",
					toolName, batch.targetTable, len(batch.rows))
			}
			return nil
		})
		if landed {
			b.commitFlushedBatch(ctx, key, batch)
			return
		}
		if ctx.Err() != nil {
			// Graceful shutdown, not an outage. Leave the offsets uncommitted so the
			// batch is redelivered to the next worker.
			return
		}
		if isDestInfraFault(lastErr) {
			sinkFailClosed("fatal: cdc db batch destination unreachable for the whole %s infrastructure-fault budget — failing closed: offsets NOT committed and NO rows dead-lettered, so Kafka redelivers this batch to the respawned worker (reason=%s, rows=%d, table=%s): %v",
				infraRetryBudget(), reason, len(batch.rows), batch.targetTable, lastErr)
			return
		}
		// The destination came back and now names a row-level fault. Fall through to
		// per-row isolation / DLQ with that error, which is what it is for.
	}

	// S1 — per-row isolation. A whole-batch write fails atomically if even ONE
	// row has a value the destination rejects (out-of-range temporal, unsigned
	// BIGINT overflow, over-precision NUMERIC, etc.). Condemning the entire batch
	// (DLQ-all or crash-loop) turns one poison row into total data loss for the
	// table. Before doing that, re-attempt the rows individually so only the
	// genuinely-bad row(s) are condemned and every good row still lands. Only
	// worth it for multi-row batches; single-row batches fall straight through.
	if len(batch.rows) > 1 {
		b.flushBatchPerRow(ctx, key, batch, reason, lastErr)
		return
	}

	// Persistent destination failure after exhausting retries. Mirror the single-event
	// path (processCDCEvent): if a DLQ is configured, route every message in the batch to
	// the DLQ for per-message traceability, then commit offsets so the worker makes
	// forward progress instead of crash-looping (which surfaces no per-row bad record and
	// trips the healer's restart escalation). Without a DLQ — or if ANY DLQ publish fails
	// — stay fail-closed: do NOT commit, crash so the orchestrator/healer surfaces it.
	if b.dlqWriter != nil {
		if classifyDestFault(lastErr) == faultUnclassified {
			logUnclassifiedCondemn("cdc db batch", batch.targetTable, lastErr)
		}
		for i, m := range batch.messages {
			rowErr := lastErr
			if i < len(batch.sms) && batch.sms[i] != nil {
				rowErr = fmt.Errorf("cdc db batch flush failed (table=%s op=%s): %w",
					batch.targetTable, batch.sms[i].CDCOp, lastErr)
			}
			if dlqErr := sendToDLQ(ctx, b.dlqWriter, m, rowErr, b.metrics, dlqTableOf(batch, i)); dlqErr != nil {
				// Fail-closed: a DLQ outage must not silently drop records.
				sinkFailClosed("fatal cdc db batch DLQ publish error (reason=%s, table=%s): %v (original: %v)",
					reason, batch.targetTable, dlqErr, lastErr)
				return
			}
		}
		// All bad records safely parked in the DLQ — commit offsets and advance the
		// high-water mark so the failed batch is not redelivered.
		_ = b.reader.CommitMessages(ctx, batch.messages...)
		last := batch.messages[len(batch.messages)-1]
		b.hw.advance(batch.topic, batch.partition, last.Offset)
		atomic.StoreInt64(&b.metrics.lastCommittedOffset, last.Offset)
		atomic.StoreInt64(&b.metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
		// dlqRouted (aggregate + per-table) is bumped inside sendToDLQ, once per message.
		// Report the loss on the way out: these rows are gone from the destination, and
		// without this the table's next TABLE_STATS would look identical to a clean flush.
		if len(batch.sms) > 0 {
			lastSM := batch.sms[len(batch.sms)-1]
			_ = emitCDCTableStats(ctx, b.eventsWriter, lastSM,
				loadCounter(b.cdcInserts, lastSM.Table), loadCounter(b.cdcUpdates, lastSM.Table),
				loadCounter(b.cdcDeletes, lastSM.Table), loadCounter(b.cdcBytes, lastSM.Table),
				loadCounter(&b.metrics.dlqByTable, lastSM.Table))
		}
		logf("warning", "warn: cdc db batch routed %d message(s) to DLQ after %d failed retries (reason=%s, table=%s): %v",
			len(batch.messages), b.params.maxRetries, reason, batch.targetTable, lastErr)
		delete(b.batches, key)
		return
	}

	// Fail-closed: no DLQ configured. Do NOT commit offsets. Crash so the orchestrator surfaces the failure.
	sinkFailClosed("fatal cdc db batch flush error (reason=%s, rows=%d, table=%s): %v",
		reason, len(batch.rows), batch.targetTable, lastErr)
}

// flushBatchPerRow is the per-row isolation recovery path (S1). It is called
// after a whole-batch upsert has exhausted its retries, for multi-row batches
// only. It writes each row in its own single-row upsert call (each carrying its
// own kafka_offset for exactly-once), so a single poison row no longer discards
// the good rows. Genuinely-bad rows are routed to the DLQ (or, with no DLQ,
// trigger a fail-closed crash exactly as the whole-batch path would). On
// completion it commits the whole batch's Kafka offsets and clears the batch.
func (b *cdcDBBatcher) flushBatchPerRow(ctx context.Context, key string, batch *cdcDBBatch, reason string, batchErr error) {
	// Keyless + not append-mode → synthetic-PK path: upsert_data computes + upserts on
	// _rsync_row_hash (see flushBatch). Only genuine append-mode keyless rows take the
	// plain-INSERT import_data path. Keeps the per-row recovery path consistent with the
	// whole-batch path so a retried keyless row doesn't NULL-violate _rsync_row_hash.
	// Document DBs never synthesize a PK (Mongo auto-assigns _id), so keyless → import_data.
	useSyntheticPK := len(batch.keyFields) == 0 && !cdcAppendMode(b.cfg) && !isDocumentDBConnector(b.destType)
	toolBase := "upsert_data"
	op := "upsert"
	if len(batch.keyFields) == 0 && !useSyntheticPK {
		toolBase = "import_data"
		op = "insert"
	}

	good, bad := 0, 0
	for i := range batch.rows {
		m := batch.messages[i]
		args := map[string]interface{}{
			"config":    b.destCfg,
			"table":     batch.targetTable,
			"data":      []map[string]interface{}{batch.rows[i]},
			"operation": op,
			"kafka_offset": map[string]interface{}{
				"pipeline_id": b.cfg.PipelineID,
				"topic":       m.Topic,
				"partition":   m.Partition,
				"offset":      m.Offset,
			},
		}
		if len(batch.keyFields) > 0 {
			args["key_fields"] = batch.keyFields
			args["primary_key_fields"] = batch.keyFields
		}
		// synthetic-PK path (decided once before the loop): tell upsert_data to compute
		// and upsert on _rsync_row_hash, matching toolBase/op above.
		if useSyntheticPK {
			args["synthetic_pk"] = true
		}
		if len(batch.columnTypes) > 0 {
			args["column_types"] = batch.columnTypes
		}
		// Forward the destination namespace on the per-row write (batch.targetTable is
		// bare); no-op when unset.
		addNamespaceParam(args, b.cfg.DestinationNamespace)

		var rowErr error
		landed := false
		for attempt := 0; attempt <= b.params.maxRetries; attempt++ {
			result, err := callDestinationTool(ctx, b.httpClient, b.cfg, b.destType, toolBase, args)
			if err != nil {
				rowErr = err
			} else if _, ok := extractDestRowCount(result); !ok {
				rowErr = fmt.Errorf("dest returned success without write-count (row offset=%d table=%s)", m.Offset, batch.targetTable)
			} else {
				landed = true
				break
			}
			if attempt < b.params.maxRetries {
				time.Sleep(b.params.backoff*time.Duration(1<<attempt) + time.Duration(rand.Intn(250))*time.Millisecond)
			}
		}

		if landed {
			good++
			if b.pgDB != nil && i < len(batch.sms) && batch.sms[i] != nil {
				_ = persistCDCAckToPostgres(ctx, b.pgDB, batch.sms[i], 1, batch.targetTable, m.Topic, m.Partition, m.Offset)
			}
			if i < len(batch.sms) && batch.sms[i] != nil {
				switch strings.ToLower(strings.TrimSpace(batch.sms[i].CDCOp)) {
				case "c":
					incrementCounter(b.cdcInserts, batch.sms[i].Table, 1)
					atomic.AddUint64(&b.metrics.cdcInserts, 1)
				case "r":
					incrementCounter(b.cdcInserts, batch.sms[i].Table, 1)
					atomic.AddUint64(&b.metrics.cdcReads, 1)
				case "u":
					incrementCounter(b.cdcUpdates, batch.sms[i].Table, 1)
					atomic.AddUint64(&b.metrics.cdcUpdates, 1)
				}
				incrementCounter(b.cdcBytes, batch.sms[i].Table, cdcRowBytes(batch.sms[i]))
			}
			atomic.AddUint64(&b.metrics.processed, 1)
			continue
		}

		// KI-CDC-SINK-INFRA-FAULT-DLQ-COMMITS — the destination went down DURING
		// per-row recovery. Reaching this function means the whole-batch error was
		// NOT an infrastructure fault (flushBatch's gate runs first), so a row
		// failing this way now means the destination stopped answering mid-recovery,
		// not that this row is poison. Fail closed rather than dead-letter it: the
		// batch's offsets are still uncommitted, and every row this loop already
		// wrote carried its own kafka_offset, so redelivery re-applies them
		// idempotently through _rsync_cdc_offsets.
		if isDestInfraFault(rowErr) {
			if ctx.Err() != nil {
				return // graceful shutdown; offsets stay uncommitted
			}
			sinkFailClosed("fatal: cdc per-row isolation hit a destination infrastructure fault at row offset=%d — failing closed: offsets NOT committed and this row NOT dead-lettered, so Kafka redelivers the batch (reason=%s, table=%s, landed_so_far=%d): %v",
				m.Offset, reason, batch.targetTable, good, rowErr)
			return
		}

		// This row genuinely cannot be written. Park it in the DLQ; with no DLQ
		// we MUST fail-closed (a bad row must never be silently dropped).
		bad++
		condemned := fmt.Errorf("per-row isolation: row offset=%d table=%s op=%s failed: %w",
			m.Offset, batch.targetTable, sinkOpOf(batch, i), rowErr)
		if classifyDestFault(rowErr) == faultUnclassified {
			logUnclassifiedCondemn("cdc per-row isolation", batch.targetTable, rowErr)
		}
		if b.dlqWriter == nil {
			sinkFailClosed("fatal cdc per-row flush, no DLQ (reason=%s, table=%s): %v", reason, batch.targetTable, condemned)
			return
		}
		if dlqErr := sendToDLQ(ctx, b.dlqWriter, m, condemned, b.metrics, dlqTableOf(batch, i)); dlqErr != nil {
			sinkFailClosed("fatal cdc per-row DLQ publish error (reason=%s, table=%s): %v (original: %v)", reason, batch.targetTable, dlqErr, condemned)
			return
		}
		// dlqRouted (aggregate + per-table) is bumped inside sendToDLQ.
	}

	// Every row is now either written or parked in the DLQ → safe to commit the
	// whole batch's offsets and advance the high-water mark.
	_ = b.reader.CommitMessages(ctx, batch.messages...)
	last := batch.messages[len(batch.messages)-1]
	b.hw.advance(batch.topic, batch.partition, last.Offset)
	atomic.StoreInt64(&b.metrics.lastCommittedOffset, last.Offset)
	atomic.StoreInt64(&b.metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
	atomic.StoreInt64(&b.metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
	if len(batch.sms) > 0 {
		lastSM := batch.sms[len(batch.sms)-1]
		_ = emitCDCTableStats(ctx, b.eventsWriter, lastSM,
			loadCounter(b.cdcInserts, lastSM.Table), loadCounter(b.cdcUpdates, lastSM.Table), loadCounter(b.cdcDeletes, lastSM.Table), loadCounter(b.cdcBytes, lastSM.Table), loadCounter(&b.metrics.dlqByTable, lastSM.Table))
	}
	logf("warning", "warn: cdc per-row isolation recovered batch (reason=%s, table=%s): %d row(s) written, %d row(s) DLQ'd (batch error: %v)",
		reason, batch.targetTable, good, bad, batchErr)
	delete(b.batches, key)
}

// sinkOpOf returns the CDC op for row i, or "?" if unavailable.
func sinkOpOf(batch *cdcDBBatch, i int) string {
	if i < len(batch.sms) && batch.sms[i] != nil {
		return batch.sms[i].CDCOp
	}
	return "?"
}

// dlqTableOf returns the source table row i belongs to, for attributing a DLQ
// routing to the table that actually lost the row. Falls back to the batch's
// target table (same table, destination-side name) rather than "" — an
// unattributed loss is a loss no surface can report.
func dlqTableOf(batch *cdcDBBatch, i int) string {
	if i < len(batch.sms) && batch.sms[i] != nil && strings.TrimSpace(batch.sms[i].Table) != "" {
		return batch.sms[i].Table
	}
	return batch.targetTable
}

func (b *cdcObjectBatcher) batchKey(msg kafka.Message, sm *SinkMessage, format, compression, partSegs, timeSeg string) string {
	return fmt.Sprintf("%s|%d|%s|%s|%s|%s|%s", msg.Topic, msg.Partition, sm.Table, strings.ToLower(format), strings.ToLower(compression), partSegs, timeSeg)
}

func (b *cdcObjectBatcher) add(ctx context.Context, msg kafka.Message, sm *SinkMessage) {
	if b == nil || sm == nil {
		return
	}

	// Dedup hot-path: skip offsets at/below the durable high-water mark. For object
	// stores the high-water tracker is empty across restarts (no get_cdc_offsets); the
	// deterministic object keys provide idempotency, so a redelivery overwrites the
	// same key. This in-run check only avoids re-processing within a single run.
	if b.hw.seen(msg.Topic, msg.Partition, msg.Offset) {
		atomic.AddUint64(&b.metrics.skipped, 1)
		logMsgEvent("debug", sm, msg, "cdc object-store dedup: offset at/below high-water, skipping")
		_ = b.reader.CommitMessages(ctx, msg)
		return
	}

	format := firstStr(b.destCfg, "file_format", "format")
	if format == "" {
		format = "jsonl"
	}
	compression := firstStr(b.destCfg, "compression")
	if compression == "" {
		compression = "none"
	}

	operation := "upsert"
	if strings.EqualFold(strings.TrimSpace(sm.CDCOp), "d") {
		operation = "delete"
	}

	evt, evtBytes, err := buildBronzeCDCEvent(b.cfg, msg, sm, operation, format)
	if err != nil {
		atomic.AddUint64(&b.metrics.failed, 1)
		b.metrics.setErr(err)
		if dlqErr := sendToDLQ(ctx, b.dlqWriter, msg, err, b.metrics, sm.Table); dlqErr != nil {
			logf("error", "fatal dlq error: %v", dlqErr)
			os.Exit(1)
		}
		_ = b.reader.CommitMessages(ctx, msg)
		return
	}

	if evtBytes > b.params.absoluteMaxEventBytes {
		err := fmt.Errorf("CDC event too large (%d bytes) exceeds absolute_max_event_bytes=%d", evtBytes, b.params.absoluteMaxEventBytes)
		atomic.AddUint64(&b.metrics.failed, 1)
		b.metrics.setErr(err)
		if dlqErr := sendToDLQ(ctx, b.dlqWriter, msg, err, b.metrics, sm.Table); dlqErr != nil {
			logf("error", "fatal dlq error: %v", dlqErr)
			os.Exit(1)
		}
		_ = b.reader.CommitMessages(ctx, msg)
		return
	}

	// Resolve the destination's partition layout (Group C) once per event so events
	// with different partition-column values / time buckets land in distinct batches
	// (and therefore distinct objects).
	partSegs, timeSeg := cdcPartitionContext(b.destCfg, sm)
	key := b.batchKey(msg, sm, format, compression, partSegs, timeSeg)
	now := time.Now().UTC()

	// If this event would exceed the soft batch max_bytes, flush existing batch first, then write this event as a single-event batch.
	if evtBytes > b.params.maxBytes {
		if existing := b.batches[key]; existing != nil && len(existing.events) > 0 {
			b.flushBatch(ctx, key, existing, "oversized_event_flush_current")
		}
		tmp := &cdcObjectBatch{
			topic:        msg.Topic,
			partition:    msg.Partition,
			table:        sm.Table,
			format:       format,
			compression:  compression,
			bucket:       firstStr(b.destCfg, "bucket", "bucket_name"),
			container:    firstStr(b.destCfg, "container"),
			prefix:       firstStr(b.destCfg, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path"),
			firstOffset:  msg.Offset,
			lastOffset:   msg.Offset,
			firstEventTS: firstNonZero(sm.SourceTS, sm.IngestionTS),
			partSegs:     partSegs,
			timeSeg:      timeSeg,
			bytes:        evtBytes,
			events:       []map[string]interface{}{evt},
			messages:     []kafka.Message{msg},
			sms:          []*SinkMessage{sm},
			createdAt:    now,
			lastAppendAt: now,
		}
		b.flushBatch(ctx, key, tmp, "oversized_event_single_batch")
		return
	}

	batch := b.batches[key]
	if batch == nil {
		batch = &cdcObjectBatch{
			topic:        msg.Topic,
			partition:    msg.Partition,
			table:        sm.Table,
			format:       format,
			compression:  compression,
			bucket:       firstStr(b.destCfg, "bucket", "bucket_name"),
			container:    firstStr(b.destCfg, "container"),
			prefix:       firstStr(b.destCfg, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path"),
			firstOffset:  msg.Offset,
			lastOffset:   msg.Offset,
			firstEventTS: firstNonZero(sm.SourceTS, sm.IngestionTS),
			partSegs:     partSegs,
			timeSeg:      timeSeg,
			createdAt:    now,
			lastAppendAt: now,
		}
		if strings.TrimSpace(batch.prefix) == "" {
			batch.prefix = "test-aws-s3"
		}
		b.batches[key] = batch
	}

	// Flush on threshold BEFORE appending if we'd exceed max_events/max_bytes.
	if len(batch.events) > 0 && (len(batch.events)+1 > b.params.maxEvents || batch.bytes+evtBytes > b.params.maxBytes) {
		b.flushBatch(ctx, key, batch, "threshold_flush")
		// Recreate batch after flush (flushBatch deletes/clears on success or DLQ).
		batch = nil
		if b2 := b.batches[key]; b2 != nil {
			batch = b2
		}
		if batch == nil {
			batch = &cdcObjectBatch{
				topic:        msg.Topic,
				partition:    msg.Partition,
				table:        sm.Table,
				format:       format,
				compression:  compression,
				bucket:       firstStr(b.destCfg, "bucket", "bucket_name"),
				container:    firstStr(b.destCfg, "container"),
				prefix:       firstStr(b.destCfg, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path"),
				firstOffset:  msg.Offset,
				lastOffset:   msg.Offset,
				firstEventTS: firstNonZero(sm.SourceTS, sm.IngestionTS),
				partSegs:     partSegs,
				timeSeg:      timeSeg,
				createdAt:    now,
				lastAppendAt: now,
			}
			if strings.TrimSpace(batch.prefix) == "" {
				batch.prefix = "test-aws-s3"
			}
			b.batches[key] = batch
		}
	}

	// Append.
	batch.events = append(batch.events, evt)
	batch.messages = append(batch.messages, msg)
	batch.sms = append(batch.sms, sm)
	batch.bytes += evtBytes
	batch.lastOffset = msg.Offset
	batch.lastAppendAt = now

	// Flush immediately if we hit thresholds exactly.
	if len(batch.events) >= b.params.maxEvents || batch.bytes >= b.params.maxBytes {
		b.flushBatch(ctx, key, batch, "threshold_reached_flush")
	}
}

func (b *cdcObjectBatcher) flushDue(ctx context.Context, now time.Time) {
	if b == nil {
		return
	}
	for key, batch := range b.batches {
		if batch == nil || len(batch.events) == 0 {
			continue
		}
		if now.Sub(batch.createdAt) >= b.params.flushInterval {
			b.flushBatch(ctx, key, batch, "interval_flush")
		}
	}
}

func (b *cdcObjectBatcher) flushBatch(ctx context.Context, key string, batch *cdcObjectBatch, reason string) {
	if b == nil || batch == nil || len(batch.events) == 0 {
		return
	}

	timeSeg := batch.timeSeg
	if strings.TrimSpace(timeSeg) == "" {
		timeSeg = timePartitionSegment(firstNonZero(batch.firstEventTS, time.Now().UTC().UnixMilli()), "")
	}
	var batchSM *SinkMessage
	if len(batch.sms) > 0 {
		batchSM = batch.sms[0]
	}
	dbOrSchema, tbl := cdcObjectPath(batchSM)
	destKey := cdcObjectKey(batch.prefix, dbOrSchema, tbl, timeSeg, batch.partSegs, batch.firstEventTS, batch.partition, batch.firstOffset, batch.lastOffset, batch.format, batch.compression)

	args := map[string]interface{}{
		"config":      b.destCfg,
		"key":         destKey,
		"data":        batch.events,
		"format":      batch.format,
		"file_format": batch.format,
		"compression": batch.compression,
	}
	if canonicalConnectorType(b.destType) == "azure-blob" {
		if strings.TrimSpace(batch.container) != "" {
			args["container"] = batch.container
		} else if strings.TrimSpace(batch.bucket) != "" {
			args["container"] = batch.bucket
		}
	} else if strings.TrimSpace(batch.bucket) != "" {
		args["bucket"] = batch.bucket
	}

	var lastErr error
	for attempt := 0; attempt <= b.params.maxRetries; attempt++ {
		_, err := callDestinationTool(ctx, b.httpClient, b.cfg, b.destType, "import_data", args)
		if err != nil {
			lastErr = err
		} else {
			// Destination write succeeded. Object stores use deterministic keys for
			// idempotency, so there is no per-destination offset table; the Postgres
			// ledger write below is a best-effort audit only (non-fatal on error).
			ackOK := true
			// Best-effort audit ledger for the ENTIRE batch in one (chunked) INSERT
			// instead of one remote round-trip per message — the high-volume CDC
			// throughput fix. Object stores use deterministic keys for idempotency
			// (no offset table), so this ledger stays best-effort / non-fatal.
			if b.pgDB != nil {
				if ackErr := persistCDCAcksBatch(ctx, b.pgDB, batch.sms, batch.messages, destKey); ackErr != nil {
					logf("warning", "cdc ack-ledger batch write failed (non-fatal, audit only): %v", ackErr)
				}
			}

			// Update per-table CDC counters (in-memory).
			for _, sm := range batch.sms {
				switch strings.ToLower(strings.TrimSpace(sm.CDCOp)) {
				case "c":
					incrementCounter(b.cdcInserts, sm.Table, 1)
					atomic.AddUint64(&b.metrics.cdcInserts, 1)
				case "r":
					incrementCounter(b.cdcInserts, sm.Table, 1)
					atomic.AddUint64(&b.metrics.cdcReads, 1)
				case "u":
					incrementCounter(b.cdcUpdates, sm.Table, 1)
					atomic.AddUint64(&b.metrics.cdcUpdates, 1)
				case "d":
					incrementCounter(b.cdcDeletes, sm.Table, 1)
					atomic.AddUint64(&b.metrics.cdcDeletes, 1)
				}
				incrementCounter(b.cdcBytes, sm.Table, cdcRowBytes(sm))
			}

			if ackOK {
				// Commit Kafka offsets only after flush succeeds, then advance the
				// in-memory high-water mark.
				_ = b.reader.CommitMessages(ctx, batch.messages...)
				last := batch.messages[len(batch.messages)-1]
				b.hw.advance(last.Topic, last.Partition, last.Offset)
				atomic.StoreInt64(&b.metrics.lastCommittedOffset, last.Offset)
				atomic.StoreInt64(&b.metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
				atomic.StoreInt64(&b.metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())

				// Emit running CDC table stats (use the last message's table as representative).
				lastSM := batch.sms[len(batch.sms)-1]
				inserts := loadCounter(b.cdcInserts, lastSM.Table)
				updates := loadCounter(b.cdcUpdates, lastSM.Table)
				deletes := loadCounter(b.cdcDeletes, lastSM.Table)
				_ = emitCDCTableStats(ctx, b.eventsWriter, lastSM, inserts, updates, deletes, loadCounter(b.cdcBytes, lastSM.Table), loadCounter(&b.metrics.dlqByTable, lastSM.Table))

				atomic.AddUint64(&b.metrics.processed, uint64(len(batch.events)))
				delete(b.batches, key)
				return
			}
		}

		// Retry with exponential backoff + jitter.
		if attempt < b.params.maxRetries {
			sleep := b.params.backoff * time.Duration(1<<attempt)
			sleep = sleep + time.Duration(rand.Intn(250))*time.Millisecond
			time.Sleep(sleep)
			continue
		}
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("flush failed")
	}
	atomic.AddUint64(&b.metrics.failed, 1)
	b.metrics.setErr(lastErr)

	// Fail-closed: do NOT commit offsets when flush fails. Exiting forces operator action and
	// prevents silent data loss (dropping to DLQ + committing offsets would lose the stream).
	logf("error", "fatal cdc batch flush error: %v", lastErr)
	os.Exit(1)
}

func firstNonZero(a, b int64) int64 {
	if a > 0 {
		return a
	}
	return b
}

func writeStateKey(sm *SinkMessage) string {
	if sm == nil {
		return ""
	}
	return fmt.Sprintf("%s|%s", strings.TrimSpace(sm.ExecutionID), strings.TrimSpace(sm.Table))
}

func slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	lastDash := false
	for _, r := range s {
		isAlnum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlnum {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		// treat everything else as separator
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	// clamp to a reasonable length (matches pipelines.dataset constraints)
	if len(out) > 63 {
		out = strings.Trim(out[:63], "-")
	}
	return out
}

func sanitizePathPart(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// Replace separators and dangerous chars
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	s = strings.ReplaceAll(s, "..", "_")
	return s
}

func canonicalConnectorType(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, "_", "-")
	return s
}

// sourceFamily returns the Debezium source-database family for a CDC event,
// read from the value Debezium stamps into every envelope: payload.source.connector
// ("postgresql", "mysql", "sqlserver", "mongodb", "oracle"). This is an explicit,
// authoritative signal — NOT a substring guess on the connector name (contrast
// isObjectStorageConnector, whose "*s3*" heuristic is deliberately avoided here).
// It drives per-family decode dispatch in parseCDCMessage (e.g. MongoDB emits
// before/after as JSON strings, not nested structs).
func sourceFamily(payload map[string]interface{}) string {
	source, ok := payload["source"].(map[string]interface{})
	if !ok {
		return ""
	}
	return strings.ToLower(strings.TrimSpace(toString(source["connector"])))
}

// isObjectStorageConnector returns true for connectors that receive CDC data as
// bronze-layer files (JSONL/Parquet) rather than row-level MCP tool calls.
//
// To add a new object-storage destination:
//  1. Add its canonical name here.
//  2. Implement import_data(key, records, format) in the connector's Python file.
//  3. Follow the bronze key structure: cdc/{topic}/{table}/dt={date}/partition={p}/batch={start}-{end}.{ext}
//  4. Write a manifest file alongside each batch for idempotent re-import.
func isObjectStorageConnector(connector string) bool {
	c := canonicalConnectorType(connector)
	if c == "" {
		return false
	}
	if c == "minio" || c == "aws-s3" || c == "s3" || c == "gcs" || c == "azure-blob" {
		return true
	}
	// Back-compat: treat any "*s3*" as object storage.
	return strings.Contains(c, "s3")
}

// isDataWarehouseConnector returns true for connectors that receive CDC data as
// row-level merge operations with soft-delete semantics.
//
// Warehouse CDC contract every connector must honour:
//   - Destination table carries _rsync_deleted BOOL and _rsync_synced_at TIMESTAMP.
//   - DELETE events set _rsync_deleted=true (never physically remove rows).
//   - merge() must return {rows_merged, rows_inserted, rows_updated, rows_deleted}.
//
// To add a new warehouse destination:
//  1. Add its canonical name to the switch below.
//  2. Implement _ensure_cdc_columns() in the connector's Python file (adds the two sentinel columns).
//  3. Implement merge(table, rows, key_fields, cdc_metadata) using a native MERGE/UPSERT statement.
//  4. Handle the _rsync_deleted flag: matched+deleted → UPDATE SET _rsync_deleted=true;
//     matched+alive → UPDATE business columns; not matched+alive → INSERT.
//  5. Add a test fixture under shared/mcp-connectors/tests/warehouse/ that verifies
//     insert, update, and delete paths all produce the correct _rsync_deleted value.
func isDataWarehouseConnector(connector string) bool {
	switch canonicalConnectorType(connector) {
	// Fully implemented: BigQuery (MERGE INTO staging), Redshift (S3 COPY + 2-tier merge),
	// Snowflake (MERGE INTO with WHEN MATCHED/NOT MATCHED branches).
	case "bigquery", "redshift", "snowflake":
		return true
	// Registered but merge() not yet implemented — uses Delta Lake MERGE INTO (see gap plan).
	case "databricks":
		return true
	// Future candidates — add merge() implementation before enabling:
	// case "clickhouse":  // ReplacingMergeTree + OPTIMIZE TABLE or ALTER TABLE UPDATE
	// case "duckdb":      // DuckDB supports standard MERGE INTO since v0.9
	default:
		return false
	}
}

// isDocumentDBConnector returns true for schemaless document-store destinations
// (MongoDB) that write whole documents via import/upsert/delete keyed on _id (or a
// relational source's key_fields) instead of typed relational columns.
//
// A document DB is NEITHER object storage NOR a warehouse. Without this classifier
// every such "else" destination is treated as a relational SQL target, which forces
// an ensure_table DDL preflight and a synthetic-PK (_rsync_row_hash) fallback that a
// document store neither implements nor needs: collections auto-create on first write
// and _id is always present. So branch this wherever the sink would otherwise assume a
// relational destination — skip ensure_table/DDL and never synthesize a PK.
//
// To add a document-store destination: add its canonical name here and implement
// import_data/upsert_data/delete_data (keyed on the store's natural id) in its connector.
func isDocumentDBConnector(connector string) bool {
	switch canonicalConnectorType(connector) {
	// mongodb is the canonical id the sink receives; the rest are the aliases
	// declared in the connector's metadata.json (canonicalized: "_"->"-"),
	// matched defensively so any upstream that skips id-resolution still routes right.
	case "mongodb", "mongo", "mongodb-atlas", "atlas":
		return true
	// Future candidates — implement import/upsert/delete before enabling:
	// case "couchdb", "elasticsearch":
	default:
		return false
	}
}

func includeKafkaMetadata(cfg *WorkerConfig) bool {
	// Plan default: include kafka_partition/kafka_offset in bronze envelope (recommended).
	if cfg == nil || cfg.KafkaSinkWorker == nil || cfg.KafkaSinkWorker.Metadata == nil || cfg.KafkaSinkWorker.Metadata.IncludeKafkaMetadata == nil {
		return true
	}
	return *cfg.KafkaSinkWorker.Metadata.IncludeKafkaMetadata
}

func bronzeOpFromDebezium(op string) string {
	switch strings.ToLower(strings.TrimSpace(op)) {
	case "c", "r":
		return "I"
	case "u":
		return "U"
	case "d":
		return "D"
	default:
		return strings.ToUpper(strings.TrimSpace(op))
	}
}

// cdcAppendMode reports whether the destination should receive CDC changes as an
// append-only history — every c/r/u/d becomes an INSERT carrying _rsync_cdc_* identity
// columns — instead of the default upsert/merge current-state materialization.
//
// Controlled by destination_config.cdc_write_mode. Default ("" / "upsert" / "merge")
// preserves the existing upsert behavior, so this is opt-in per destination and never
// changes the in-place update path (PG ON CONFLICT, MySQL ON DUPLICATE KEY, warehouse MERGE).
func cdcAppendMode(cfg *WorkerConfig) bool {
	if cfg == nil || cfg.DestinationConfig == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(toString(cfg.DestinationConfig["cdc_write_mode"]))) {
	case "append", "append_only", "append-only", "history", "insert":
		return true
	default:
		return false
	}
}

// cdcSeq builds a DMS-style composite ordering token: zero-padded source commit time
// (epoch ms, 16 digits) followed by the zero-padded log position (19 digits). Fixed width
// so a lexicographic compare equals a numeric compare; MAX(_rsync_cdc_seq) per business key
// selects the latest change. Returned as a STRING because the 35-digit value exceeds int64.
//
// Why ts-led and not LSN/GTID-led: the commit timestamp is monotonic across PG failover,
// MySQL binlog rotation, and GTID resets, whereas the log position is not universally
// comparable across those events. The log position only breaks intra-millisecond ties (a
// binlog/WAL segment cannot rotate within 1ms), so (ts_ms, position) is a safe total order.
func cdcSeq(sm *SinkMessage) string {
	if sm == nil {
		return ""
	}
	return fmt.Sprintf("%016d%019d", sm.SourceTS, sm.LSN)
}

// cdcIdentityColumns returns the append-only identity/ordering columns injected into each
// destination row. Mirrors the established industry pattern (AWS DMS AR_H_*, Airbyte
// _ab_cdc_*, Fivetran history columns):
//
//	_rsync_cdc_op       I/U/D operation (snapshot reads "r" map to I)
//	_rsync_cdc_event_ts source commit time (epoch ms) — the event_timestamp
//	_rsync_cdc_lsn      log position: Postgres LSN / MySQL binlog offset
//	_rsync_cdc_gtid     transaction identity: MySQL GTID / Postgres txId
//	_rsync_cdc_seq      composite ordering token (see cdcSeq)
//
// _rsync_cdc_lsn is always populated; _rsync_cdc_gtid is the global transaction id (GTID on
// MySQL, txId on Postgres). Together they identify a unique change; _rsync_cdc_seq orders them.
func cdcIdentityColumns(sm *SinkMessage) map[string]interface{} {
	return map[string]interface{}{
		"_rsync_cdc_op":       bronzeOpFromDebezium(sm.CDCOp),
		"_rsync_cdc_event_ts": sm.SourceTS,
		"_rsync_cdc_lsn":      sm.LSN,
		"_rsync_cdc_gtid":     sm.TxID,
		"_rsync_cdc_seq":      cdcSeq(sm),
	}
}

// withCDCIdentity returns a copy of row with the identity columns merged in. The caller's
// map is never mutated. The _rsync_ prefix avoids collisions with source columns; if a
// source somehow carries a reserved name, the identity value wins (correctness over source).
func withCDCIdentity(row map[string]interface{}, sm *SinkMessage) map[string]interface{} {
	out := copyMap(row)
	if out == nil {
		out = map[string]interface{}{}
	}
	for k, v := range cdcIdentityColumns(sm) {
		out[k] = v
	}
	return out
}

// cdcIdentityColumnTypes returns dialect-neutral SQL types for the injected
// _rsync_cdc_* columns so ensure_table creates them well-typed instead of
// defaulting to TEXT. BIGINT/VARCHAR/TEXT are valid on both Postgres and MySQL
// (validated by the connector's _normalize_ddl_type). _rsync_cdc_seq stays a
// fixed-width VARCHAR so lexicographic MAX() equals numeric max (the DMS
// AR_H_CHANGE_SEQ shape) — never a numeric type, which would overflow at 35 digits.
func cdcIdentityColumnTypes() map[string]string {
	return map[string]string{
		"_rsync_cdc_op":       "VARCHAR(8)",
		"_rsync_cdc_event_ts": "BIGINT",
		"_rsync_cdc_lsn":      "BIGINT",
		"_rsync_cdc_gtid":     "VARCHAR(255)",
		"_rsync_cdc_seq":      "VARCHAR(40)",
	}
}

// mergeCDCIdentityColumnTypes returns a copy of base with the identity-column types
// added. Existing entries win (a source column that happens to share a name keeps its
// Debezium-derived type); base is never mutated.
func mergeCDCIdentityColumnTypes(base map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range cdcIdentityColumnTypes() {
		if _, exists := out[k]; !exists {
			out[k] = v
		}
	}
	return out
}

func isColumnarFormat(format string) bool {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "parquet", "orc", "avro", "arrow":
		return true
	default:
		return false
	}
}

func normalizeTableForPath(table string) string {
	t := strings.TrimSpace(table)
	if t == "" {
		return ""
	}
	// Underscore normalization (plan decision): "public.users" -> "public_users"
	t = strings.ReplaceAll(t, ".", "_")
	t = strings.ReplaceAll(t, "/", "_")
	t = strings.ReplaceAll(t, "\\", "_")
	t = strings.ReplaceAll(t, " ", "_")
	return sanitizePathPart(t)
}

func utcDatePartition(tsMs int64) string {
	if tsMs <= 0 {
		tsMs = time.Now().UTC().UnixMilli()
	}
	return time.UnixMilli(tsMs).UTC().Format("2006-01-02")
}

// hiveDefaultPartition is the sentinel Hive/Spark use for a null or empty partition
// value, so a missing partition column still produces a well-formed, queryable key.
const hiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__"

// parsePartitionColumns splits a comma-separated partition_by list into trimmed,
// non-empty column names, preserving order. Returns nil when s is blank.
func parsePartitionColumns(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var cols []string
	for _, c := range strings.Split(s, ",") {
		if c = strings.TrimSpace(c); c != "" {
			cols = append(cols, c)
		}
	}
	return cols
}

// hivePartitionSegments builds Hive-style "col=val/" path segments (each with a
// trailing slash) from a row map for the given columns, in order. A missing/nil/empty
// value uses hiveDefaultPartition so the key is always well-formed. Column names and
// values are sanitized for path safety. Returns "" when cols is empty.
func hivePartitionSegments(row map[string]interface{}, cols []string) string {
	if len(cols) == 0 {
		return ""
	}
	var b strings.Builder
	for _, col := range cols {
		name := sanitizePathPart(col)
		if name == "" {
			continue
		}
		val := hiveDefaultPartition
		if row != nil {
			if v, ok := row[col]; ok && v != nil {
				if s := sanitizePathPart(fmt.Sprintf("%v", v)); s != "" {
					val = s
				}
			}
		}
		b.WriteString(name)
		b.WriteString("=")
		b.WriteString(val)
		b.WriteString("/")
	}
	return b.String()
}

// timePartitionSegment builds the DMS-style time-bucket path segment for a destination's
// partition_time_granularity — a plain date folder (no "dt=" key), matching an AWS DMS S3
// target with date-based folder partitioning. Finer granularity appends a sub-folder:
//
//	"", "none", "day" -> "2006-01-02"      (e.g. 2026-06-30)
//	"hour"            -> "2006-01-02/15"
//	"month"           -> "2006-01"
//
// The returned segment has no trailing slash (cdcObjectKey adds the separator).
func timePartitionSegment(tsMs int64, granularity string) string {
	if tsMs <= 0 {
		tsMs = time.Now().UTC().UnixMilli()
	}
	t := time.UnixMilli(tsMs).UTC()
	switch strings.ToLower(strings.TrimSpace(granularity)) {
	case "hour":
		return fmt.Sprintf("%s/%s", t.Format("2006-01-02"), t.Format("15"))
	case "month":
		return t.Format("2006-01")
	default: // "", "none", "day"
		return utcDatePartition(tsMs)
	}
}

// objectTimestamp formats an event timestamp as the DMS-style file stem
// "YYYYMMDD-HHMMSSmmm" (e.g. 20260630-065737467), used as the leaf name of a bronze
// object so the layout reads like an AWS DMS S3 target.
func objectTimestamp(tsMs int64) string {
	if tsMs <= 0 {
		tsMs = time.Now().UTC().UnixMilli()
	}
	t := time.UnixMilli(tsMs).UTC()
	return t.Format("20060102-150405") + fmt.Sprintf("%03d", t.Nanosecond()/1_000_000)
}

// cdcPartitionContext resolves the Hive partition segments and time-bucket segment for
// one CDC bronze event, honoring the destination's partition_by /
// partition_time_granularity config (Group C of docs/connectors/cloud-storage-config.md).
// The after-image supplies partition-column values (deletes have none → default
// partition); the event timestamp drives the time bucket. Both default to the legacy
// dt-only layout when no partition config is set, so keys stay byte-identical to the
// pre-Phase-4 bronze layout.
// warnedMissingPartitionCol dedups the "partition_by names a missing column" warning to
// once per (pipeline, column) so a misconfiguration logs a single actionable line instead
// of one per event.
var warnedMissingPartitionCol sync.Map // key "pipelineID|col" -> struct{}

func warnMissingPartitionColOnce(pipelineID, col, where string) {
	if _, loaded := warnedMissingPartitionCol.LoadOrStore(pipelineID+"|"+col, struct{}{}); loaded {
		return
	}
	logf("warn", "partition_by column %q not found in %s schema (pipeline=%q); skipping it for object partitioning — every row would otherwise land in __HIVE_DEFAULT_PARTITION__. Check the destination partition_by config.", col, where, pipelineID)
}

// filterPresentPartitionColumns drops configured partition columns that are absent from
// the row's schema (a misconfiguration such as partition_by naming a non-existent column,
// which would otherwise bucket every row into __HIVE_DEFAULT_PARTITION__). A column that
// is present but null is kept (a legitimate null → default partition). Absent columns are
// reported once per (pipeline,column) via warnFn. When row is empty (e.g. a delete with no
// after-image) no judgement is made and cols pass through unchanged.
func filterPresentPartitionColumns(row map[string]interface{}, cols []string, warnFn func(col string)) []string {
	if len(cols) == 0 || len(row) == 0 {
		return cols
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if _, ok := row[c]; ok {
			out = append(out, c)
		} else if warnFn != nil {
			warnFn(c)
		}
	}
	return out
}

func cdcPartitionContext(destCfg map[string]interface{}, sm *SinkMessage) (partSegs, timeSeg string) {
	cols := parsePartitionColumns(firstStr(destCfg, "partition_by"))
	var row map[string]interface{}
	var ts int64
	if sm != nil {
		row = sm.After
		if row == nil && len(sm.Data) > 0 {
			row = sm.Data[0]
		}
		ts = firstNonZero(sm.SourceTS, sm.IngestionTS)
	}
	// Guard against a partition_by column that doesn't exist in the source schema.
	if len(row) > 0 {
		pid := ""
		if sm != nil {
			pid = sm.PipelineID
		}
		cols = filterPresentPartitionColumns(row, cols, func(col string) {
			warnMissingPartitionColOnce(pid, col, "CDC after-image")
		})
	}
	partSegs = hivePartitionSegments(row, cols)
	// Group D: cdc_partition_by_op prepends an op=<I|U|D>/ segment so each change type
	// lands under its own prefix (outermost partition → cheap op pruning in Athena/
	// Spark). Composes with partition_by (op first, then the column tuple). If
	// cdc_include_op is also true the in-file op duplicates this partition column —
	// set cdc_include_op=false to avoid a duplicate-column clash when registering the
	// bronze path as a Hive/Athena table.
	if cfgBool(destCfg, "cdc_partition_by_op", false) && sm != nil {
		op := bronzeOpFromDebezium(sm.CDCOp)
		if op == "" {
			op = hiveDefaultPartition
		}
		partSegs = "op=" + sanitizePathPart(op) + "/" + partSegs
	}
	timeSeg = timePartitionSegment(ts, firstStr(destCfg, "partition_time_granularity"))
	return partSegs, timeSeg
}

// cdcObjectPath derives the {db_or_schema}/{table} path segments for a CDC bronze object
// from the sink message. sm.Table is "<schema>.<table>"; its schema is used as the
// db_or_schema fallback when SinkMessage.DBOrSchema is empty. The pipeline namespace is
// the user-set path_prefix (DMS bucketFolder model) — deliberately NOT a pipeline-id
// segment — so keys read as <prefix>/<schema>/<table>/…
func cdcObjectPath(sm *SinkMessage) (dbOrSchema, table string) {
	if sm == nil {
		return "default", ""
	}
	tbl := sm.Table
	schemaFromTable := ""
	if idx := strings.LastIndex(tbl, "."); idx >= 0 && idx+1 < len(tbl) {
		schemaFromTable = tbl[:idx]
		tbl = tbl[idx+1:]
	}
	dbOrSchema = sanitizePathPart(sm.DBOrSchema)
	if dbOrSchema == "" {
		dbOrSchema = sanitizePathPart(schemaFromTable)
	}
	if dbOrSchema == "" {
		dbOrSchema = "default"
	}
	table = sanitizePathPart(tbl)
	return dbOrSchema, table
}

// cdcObjectKey builds the deterministic bronze object key for a CDC batch, reading like an
// AWS DMS S3 target with date-based folder partitioning:
//
//	<prefix>/<db_or_schema>/<table>/<partSegs><YYYY-MM-DD>[/HH]/<YYYYMMDD-HHMMSSmmm>[-p<n>]-<offset>.<ext>
//
// dateSeg is the plain date folder (from timePartitionSegment); partSegs is the optional
// Hive "col=val/" prefix (only when partition_by is set). The leaf leads with the event
// timestamp (tsMs) for DMS readability, then a trailing Kafka offset (and "-p<n>" for a
// multi-partition topic) that guarantees uniqueness across batches AND makes a redelivered
// message overwrite the same object (idempotent retries) — a bare timestamp could collide
// when a bulk change lands many rows in the same millisecond.
func cdcObjectKey(prefix, dbOrSchema, table, dateSeg, partSegs string, tsMs int64, partition int, firstOffset, lastOffset int64, format, compression string) string {
	if strings.TrimSpace(dateSeg) == "" {
		dateSeg = timePartitionSegment(tsMs, "")
	}
	ext := format
	if strings.EqualFold(strings.TrimSpace(compression), "gzip") {
		ext = fmt.Sprintf("%s.gz", format)
	}
	name := objectTimestamp(tsMs)
	if partition > 0 {
		name = fmt.Sprintf("%s-p%d", name, partition)
	}
	name = fmt.Sprintf("%s-%d", name, firstOffset)
	return tablePrefix(prefix, "", dbOrSchema, table) + partSegs + fmt.Sprintf("%s/%s.%s", dateSeg, name, ext)
}

func buildBronzeCDCEvent(cfg *WorkerConfig, msg kafka.Message, sm *SinkMessage, operation, format string) (map[string]interface{}, int, error) {
	if sm == nil {
		return nil, 0, fmt.Errorf("nil sink message")
	}
	bronzeOp := bronzeOpFromDebezium(sm.CDCOp)
	var after map[string]interface{}
	if bronzeOp != "D" {
		after = sm.After
		if after == nil && len(sm.Data) > 0 {
			after = sm.Data[0]
		}
	}
	pkObj := pkObjectForCDC(sm, operation)
	if bronzeOp == "D" && len(pkObj) == 0 {
		return nil, 0, fmt.Errorf("CDC delete missing pk (Kafka message key empty or unparsable)")
	}

	evt := map[string]interface{}{
		"table":           sm.Table,
		"lsn":             sm.LSN,
		"source_ts_ms":    sm.SourceTS,
		"ingestion_ts_ms": sm.IngestionTS,
		"is_snapshot":     sm.IsSnapshot,
	}
	// Group D: cdc_include_op (default true) controls whether the op (I/U/D) is emitted
	// in the bronze envelope. Disable it for an append-only after-image log — e.g. when
	// also partitioning by op, where the change type already lives in the object path.
	if cfgBool(cfg.DestinationConfig, "cdc_include_op", true) {
		evt["op"] = bronzeOp
	}
	if includeKafkaMetadata(cfg) {
		evt["kafka_partition"] = msg.Partition
		evt["kafka_offset"] = msg.Offset
	}

	// Physical encoding strategy:
	// - json/jsonl: pk+after are objects
	// - columnar: pk_json/after_json strings (stable schema)
	if isColumnarFormat(format) {
		if b, err := json.Marshal(pkObj); err == nil {
			evt["pk_json"] = string(b)
		} else {
			evt["pk_json"] = ""
		}
		if bronzeOp != "D" && after != nil {
			if b, err := json.Marshal(after); err == nil {
				evt["after_json"] = string(b)
			} else {
				evt["after_json"] = ""
			}
		} else {
			evt["after_json"] = nil
		}
	} else {
		evt["pk"] = pkObj
		if bronzeOp == "D" {
			evt["after"] = nil
		} else {
			evt["after"] = after
		}
	}

	b, _ := json.Marshal(evt)
	return evt, len(b), nil
}

func pkObjectForCDC(sm *SinkMessage, operation string) map[string]interface{} {
	if sm == nil {
		return nil
	}
	if len(sm.PK) > 0 {
		return sm.PK
	}
	// Fallback: if Debezium key wasn't parsable/available, attempt to build PK object from row payload.
	var row map[string]interface{}
	if operation == "delete" {
		row = sm.Before
	} else {
		row = sm.After
	}
	if row == nil && len(sm.Data) > 0 {
		row = sm.Data[0]
	}
	if row == nil {
		return nil
	}
	keys := []string{}
	if len(sm.KeyFields) > 0 {
		keys = append(keys, sm.KeyFields...)
	} else {
		keys = inferKeyFieldsForRow(row)
	}
	if len(keys) == 0 {
		return nil
	}
	out := map[string]interface{}{}
	for _, k := range keys {
		if v, ok := row[k]; ok {
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	if m == nil {
		return nil
	}
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func toStringSlice(v interface{}) []string {
	out := []string{}
	if v == nil {
		return out
	}
	switch t := v.(type) {
	case string:
		s := strings.TrimSpace(t)
		if s != "" {
			out = append(out, s)
		}
	case []string:
		for _, s := range t {
			s = strings.TrimSpace(s)
			if s != "" {
				out = append(out, s)
			}
		}
	case []interface{}:
		for _, it := range t {
			s := strings.TrimSpace(fmt.Sprint(it))
			if s != "" {
				out = append(out, s)
			}
		}
	default:
		s := strings.TrimSpace(fmt.Sprint(v))
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func fileExt(format, compression string) string {
	ext := strings.ToLower(strings.TrimSpace(format))
	if ext == "" {
		ext = "json"
	}
	switch ext {
	case "ndjson", "json_lines", "jsonlines":
		ext = "jsonl"
	}
	comp := strings.ToLower(strings.TrimSpace(compression))
	if comp == "" || comp == "none" {
		return ext
	}
	switch comp {
	case "gzip", "gz":
		return fmt.Sprintf("%s.gz", ext)
	case "zstd", "zst":
		return fmt.Sprintf("%s.zst", ext)
	case "snappy":
		return fmt.Sprintf("%s.snappy", ext)
	case "lz4":
		return fmt.Sprintf("%s.lz4", ext)
	default:
		return fmt.Sprintf("%s.%s", ext, comp)
	}
}

func tablePrefix(prefix, dataset, dbOrSchema, table string) string {
	parts := []string{strings.Trim(prefix, "/")}
	if dataset != "" {
		parts = append(parts, dataset)
	}
	if dbOrSchema != "" {
		parts = append(parts, dbOrSchema)
	}
	if table != "" {
		parts = append(parts, table)
	}
	return strings.Join(parts, "/") + "/"
}

func partitionPrefix(prefix, dataset, dbOrSchema, table, dt string) string {
	return tablePrefix(prefix, dataset, dbOrSchema, table) + fmt.Sprintf("dt=%s/", dt)
}

func manifestKey(prefix, dataset, dbOrSchema, table, dt string) string {
	return partitionPrefix(prefix, dataset, dbOrSchema, table, dt) + "_MANIFEST.json"
}

func successKey(prefix, dataset, dbOrSchema, table, dt string) string {
	return partitionPrefix(prefix, dataset, dbOrSchema, table, dt) + "_SUCCESS"
}

// partKey builds the batch part-file object key. partSegs is the (possibly empty)
// Hive "col=val/" prefix inserted between the table and the dt bucket — with
// partSegs="" the key is byte-identical to the pre-partitioning layout. (The
// manifest/_SUCCESS markers deliberately keep the partSegs-free partitionPrefix so
// they sit at the dataset root, per Hive/Spark convention.)
// partKey builds a batch part-file object key. partSuffix is "" for the legacy
// single-file-per-message layout (byte-identical pre-Phase-4e key) and a zero-padded
// chunk index (e.g. "0001") when file rolling (max_file_rows/max_file_mb) splits a
// message into multiple part-files within the same (table, partition, dt).
func partKey(prefix, dataset, dbOrSchema, table, partSegs, dt string, offset int64, partSuffix, ext string) string {
	name := fmt.Sprintf("part-%06d", offset)
	if partSuffix != "" {
		name = fmt.Sprintf("part-%06d-%s", offset, partSuffix)
	}
	return tablePrefix(prefix, dataset, dbOrSchema, table) + partSegs + fmt.Sprintf("dt=%s/%s.%s", dt, name, ext)
}

// objectPartitionGroup is one Hive partition's rows for the batch object-storage
// write path (partition_by splits a single batch message into one part-file per
// distinct partition-column tuple).
type objectPartitionGroup struct {
	partSegs string
	rows     []map[string]interface{}
}

// splitRowsForObjectPartition groups batch rows by their Hive partition tuple when
// the destination sets partition_by; otherwise returns a single group with empty
// partSegs (the legacy single-part-file behavior). Insertion order is preserved
// (first-seen partition value) so retries reproduce identical object keys.
func splitRowsForObjectPartition(destCfg map[string]interface{}, rows []map[string]interface{}) []objectPartitionGroup {
	cols := parsePartitionColumns(firstStr(destCfg, "partition_by"))
	// Drop partition columns absent from the row schema (misconfig guard — see
	// filterPresentPartitionColumns). Use the first row as the representative schema.
	if len(cols) > 0 && len(rows) > 0 {
		cols = filterPresentPartitionColumns(rows[0], cols, func(col string) {
			warnMissingPartitionColOnce("", col, "batch row")
		})
	}
	if len(cols) == 0 {
		return []objectPartitionGroup{{partSegs: "", rows: rows}}
	}
	order := make([]string, 0)
	bySeg := make(map[string][]map[string]interface{})
	for _, r := range rows {
		seg := hivePartitionSegments(r, cols)
		if _, ok := bySeg[seg]; !ok {
			order = append(order, seg)
		}
		bySeg[seg] = append(bySeg[seg], r)
	}
	out := make([]objectPartitionGroup, 0, len(order))
	for _, seg := range order {
		out = append(out, objectPartitionGroup{partSegs: seg, rows: bySeg[seg]})
	}
	return out
}

// objectWriteUnit is one physical object to write in the batch path: a partition
// group's rows after file-rolling (max_file_rows/max_file_mb) chunking. partSuffix
// disambiguates multiple part-files within the same (table, partition, dt) when
// rolling is on; it is "" when rolling is off so the object key stays byte-identical
// to the pre-Phase-4e single-file layout.
type objectWriteUnit struct {
	rows       []map[string]interface{}
	partSegs   string
	partSuffix string
}

// objectFileRollLimits resolves the destination's file-rolling caps (Group C):
// max_file_rows (rows per object) and max_file_mb (≈ MB per object, pre-compression).
// Returns (0, 0) when neither is set → rolling off (one object per logical unit,
// byte-identical to the pre-Phase-4e behavior). Mirrors AWS DMS S3 targets, which roll
// a file on a row OR size threshold, whichever is hit first; the byte budget is
// pre-compression because the connector applies parquet/gzip downstream.
func objectFileRollLimits(destCfg map[string]interface{}) (maxRows, maxBytes int) {
	if destCfg == nil {
		return 0, 0
	}
	if n := toInt64(destCfg["max_file_rows"]); n > 0 {
		maxRows = int(n)
	}
	if n := toInt64(destCfg["max_file_mb"]); n > 0 {
		maxBytes = int(n) * 1024 * 1024
	}
	return maxRows, maxBytes
}

// chunkRowsForFileRolling splits rows into sub-slices ("part files") so each chunk
// holds at most maxRows rows AND ~maxBytes bytes (whichever cap is hit first), mirroring
// AWS DMS's roll-on-size-or-count behavior. Byte size is estimated pre-compression via
// per-row json.Marshal length (the true on-disk size is post-encode in the connector,
// exactly like DMS's pre-compression size target). A single row larger than maxBytes
// still occupies its own chunk — rows are never split. With both caps <= 0 (or no rows)
// it returns a single chunk → the legacy one-object-per-unit behavior.
func chunkRowsForFileRolling(rows []map[string]interface{}, maxRows, maxBytes int) [][]map[string]interface{} {
	if (maxRows <= 0 && maxBytes <= 0) || len(rows) == 0 {
		return [][]map[string]interface{}{rows}
	}
	var chunks [][]map[string]interface{}
	var cur []map[string]interface{}
	curBytes := 0
	for _, r := range rows {
		rb := 0
		if maxBytes > 0 {
			if b, err := json.Marshal(r); err == nil {
				rb = len(b)
			}
		}
		if len(cur) > 0 {
			overRows := maxRows > 0 && len(cur)+1 > maxRows
			overBytes := maxBytes > 0 && curBytes+rb > maxBytes
			if overRows || overBytes {
				chunks = append(chunks, cur)
				cur = nil
				curBytes = 0
			}
		}
		cur = append(cur, r)
		curBytes += rb
	}
	if len(cur) > 0 {
		chunks = append(chunks, cur)
	}
	return chunks
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		logf("error", "config error: %v", err)
		os.Exit(2)
	}

	// Fail closed on bad Kafka security config: dialing plaintext at a cluster the
	// operator asked us to authenticate to surfaces as a broker outage, not a
	// misconfiguration, and costs an on-call cycle to tell apart.
	if err := initKafkaSecurity(cfg.KafkaBootstrapServers); err != nil {
		logf("error", "kafka security config error: %v", err)
		os.Exit(2)
	}

	gPipelineID.Store(strings.TrimSpace(cfg.PipelineID))
	gExecutionID.Store(strings.TrimSpace(cfg.ExecutionID))

	metrics := &Metrics{startedAt: time.Now()}
	// Graceful shutdown: cancel ctx on SIGINT/SIGTERM so in-flight retry loops
	// (ctxAwareSleep) actually unblock — as their docs promise — and the consume
	// loop gets a chance to drain buffered CDC batches before exit instead of
	// being hard-killed mid-write. Previously main used a plain WithCancel with no
	// signal handler, so SIGTERM terminated the process abruptly (no drain).
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go serveMetrics(cfg.MetricsPort, metrics)

	destType := canonicalConnectorType(cfg.DestinationConnector)
	dedup, derr := resolveDedupConfig(cfg, destType)
	if derr != nil {
		metrics.setErr(derr)
		logf("error", "config error: %v", derr)
		os.Exit(2)
	}

	// In-memory high-water tracker replaces the Redis dedup cache. It is seeded
	// from the destination's durable _rsync_cdc_offsets table below (after the
	// HTTP client is available) and advanced after each successful write+commit.
	_ = dedup // retained: dedup.ttl still flows into the batchers as an ack-audit TTL
	tracker := newHighWaterTracker()

	// Connect to Postgres for the durable ACK ledger.
	//
	// The ledger is the single source of truth for exactly-once delivery:
	//   - the dedup SELECT before every write reads from it
	//   - the ack INSERT after every successful write writes to it
	//   - the postflight silent-drop invariant reads from it
	//
	// Running the sink without the ledger is therefore at-least-once, not
	// exactly-once, and can silently double-write any row whose Kafka offset
	// commit was lost. We refuse to start by default. Operators who run the
	// sink against destinations where duplicates are acceptable (e.g. append-
	// only object storage with monotonic keys) can opt out explicitly with
	// RSYNC_SINK_ALLOW_NO_LEDGER=true.
	allowNoLedger := strings.EqualFold(strings.TrimSpace(os.Getenv("RSYNC_SINK_ALLOW_NO_LEDGER")), "true")

	var pgDB *sql.DB
	pgURL := strings.TrimSpace(os.Getenv("POSTGRES_URL"))
	if pgURL == "" {
		pgURL = strings.TrimSpace(os.Getenv("DATABASE_URL"))
	}
	if pgURL != "" {
		pgDB, err = sql.Open("postgres", pgURL)
		if err != nil {
			logf("error", "postgres open error: %v", err)
			pgDB = nil
		} else {
			pgDB.SetMaxOpenConns(10)
			pgDB.SetMaxIdleConns(5)
			pgDB.SetConnMaxLifetime(5 * time.Minute)
			// Bound the readiness ping: sql.Open does not connect, so this is the
			// first real TCP connect to pipeline_db. Without a deadline a firewalled /
			// partitioned host (packets DROPPED, not refused) blocks for the OS default
			// (~75s+) while the worker sits in "starting" limbo and the Python readiness
			// probe times out. A ping failure already fails closed below (fatal unless
			// RSYNC_SINK_ALLOW_NO_LEDGER), so bounding it just makes that verdict fast.
			pingCtx, cancelPing := context.WithTimeout(ctx, 5*time.Second)
			pingErr := pgDB.PingContext(pingCtx)
			cancelPing()
			if pingErr != nil {
				logf("error", "postgres ping error: %v", pingErr)
				pgDB.Close()
				pgDB = nil
			} else {
				defer pgDB.Close()
				logEvent("info", "connected to postgres for ACK ledger")
			}
		}
	}

	if pgDB == nil && !allowNoLedger {
		logEvent("error",
			"FATAL: ACK ledger (POSTGRES_URL / DATABASE_URL) is unavailable and "+
				"RSYNC_SINK_ALLOW_NO_LEDGER is not set. Refusing to start because "+
				"running without the ledger drops exactly-once and can silently "+
				"produce duplicate destination writes on Kafka redelivery. Provide "+
				"a working Postgres URL, or set RSYNC_SINK_ALLOW_NO_LEDGER=true to "+
				"opt into at-least-once semantics.")
		os.Exit(1)
	}

	// Resolve topic subscriptions (supports multi-topic CDC runs).
	groupTopics := []string{}
	if len(cfg.Topics) > 0 {
		for _, t := range cfg.Topics {
			tt := strings.TrimSpace(t)
			if tt != "" {
				groupTopics = append(groupTopics, tt)
			}
		}
	}
	if len(groupTopics) == 0 {
		if strings.TrimSpace(cfg.Topic) != "" {
			groupTopics = []string{strings.TrimSpace(cfg.Topic)}
		}
	}

	// Resolve every subscribed topic before joining the consumer group, so a member
	// that joins while a Debezium topic is still lazily uncreated does not have to
	// wait out a full stall-watchdog cycle to start consuming. This is a startup
	// precondition, NOT the fix for the empty-assignment wedge: measured against a
	// live broker it left the wedge rate unchanged (1/6 without, 4/18 with), because
	// the wedge also happens when the topic is demonstrably present at join time.
	// The remedy for that is watchConsumerStall below.
	waitForTopicPartitions(cfg.KafkaBootstrapServers, groupTopics, topicResolveTimeout())

	// Split, not wrapped: []string{cfg.KafkaBootstrapServers} made a multi-broker
	// CSV one unresolvable hostname. Dialer carries TLS/SASL.
	readerDialer, rderr := kafkaDialer(cfg.KafkaBootstrapServers)
	if rderr != nil {
		logf("error", "kafka dialer: %v", rderr)
		os.Exit(2)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     kafkaSecurity.Brokers,
		Dialer:      readerDialer,
		GroupID:     cfg.ConsumerGroup,
		GroupTopics: groupTopics,
		MinBytes:    1,
		// Must EXCEED the broker's message.max.bytes (KAFKA_MESSAGE_MAX_BYTES=15 MB, raised
		// for large-column CDC rows) so a max-size change event can be fetched; a smaller
		// cap would strand it. Kept in lockstep with KAFKA_MESSAGE_MAX_BYTES /
		// CONNECT_PRODUCER_MAX_REQUEST_SIZE.
		MaxBytes: 16 * 1024 * 1024,
		// Prefetch a larger in-memory queue so the single consume goroutine is not
		// starved waiting for the next fetch once the per-flush cost drops (the
		// batched ack-ledger fix), and cap the fetch wait (kafka-go default 10s) so a
		// low-volume CDC stream still returns and flushes promptly. Neither affects
		// exactly-once: messages are only committed after the durable destination write.
		QueueCapacity: 2000,
		MaxWait:       500 * time.Millisecond,
		// CDC data-loss fix (KI-CDC-1): Debezium creates per-table topics
		// (cdc-<pid>.<db>.<table>) LAZILY — only when the first change event is
		// emitted. The sink, however, joins the consumer group immediately after
		// the connector is registered, BEFORE the topic exists. kafka-go's
		// makeAssignments tolerates the missing topic ("it's not a failure if the
		// topic doesn't exist yet... a topic watcher can trigger a rebalance when
		// the topic comes into being") and assigns 0 partitions, leaving the group
		// Stable forever — so every streamed row is silently dropped. Enabling the
		// partition watcher makes the reader rejoin until the topic appears, then
		// assigns its partitions. The batch path sidesteps this by bootstrapping
		// its topic before sink start; CDC can't (Debezium owns the topic).
		WatchPartitionChanges:  true,
		PartitionWatchInterval: 5 * time.Second,
		CommitInterval:         0, // commit ONLY after destination ack
		StartOffset:            startOffset(cfg),
	})
	defer reader.Close()

	eventsWriter := &kafka.Writer{
		Addr:                   brokerAddr(cfg.KafkaBootstrapServers),
		Transport:              kafkaTransport(),
		Topic:                  kafkaclient.Topic("pipeline.domain.events"),
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
	}
	defer eventsWriter.Close()

	// NOTE: Do not pin DLQ to a single topic; route per-message to "<source_topic>.dlq".
	dlqBrokers := cfg.KafkaBootstrapServers
	if strings.TrimSpace(cfg.DLQBootstrapServers) != "" {
		dlqBrokers = strings.TrimSpace(cfg.DLQBootstrapServers)
	}
	dlqWriter := &kafka.Writer{
		Addr:                   brokerAddr(dlqBrokers),
		Transport:              kafkaTransport(),
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
		// Fail-closed SLA: when the DLQ broker is unreachable the worker must halt
		// quickly (orchestration surfaces the failure). kafka-go's defaults
		// (WriteTimeout 10s, MaxAttempts 10) let a single WriteMessages block ~10s;
		// sendToDLQ then retries it 3x, so a down DLQ took ~40s to fail — past the
		// 30s halt SLA, leaving the pipeline looking alive. Bound it: each publish
		// caps at 3s and does not internally retry, so the sendToDLQ retry budget
		// exhausts in ~12s and the caller can os.Exit fail-closed well within SLA.
		// Async stays false (the default) so WriteMessages returns the real error
		// synchronously rather than buffering it; we set it explicitly to document
		// that the fail-closed contract depends on synchronous writes.
		WriteTimeout: 3 * time.Second,
		MaxAttempts:  1,
		Async:        false,
	}
	defer dlqWriter.Close()

	// Dedicated transport: the sink streams many batches to the SAME destination
	// MCP host, but http.DefaultTransport caps idle keep-alives at 2 per host
	// (MaxIdleConnsPerHost), so under sustained load most batch writes pay a fresh
	// TCP+TLS handshake. Raise the per-host idle pool so successive writes reuse
	// warm connections.
	httpClient := &http.Client{
		Timeout: destHTTPTimeout(),
		Transport: &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			MaxIdleConns:          100,
			MaxIdleConnsPerHost:   32,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
	}

	// Destination DDL support (best-effort; used to auto-create missing dest tables for CDC/batch).
	ddl := resolveDDLSupport(ctx, httpClient, cfg, destType)

	// Report CDC's self-applied additive drift on the healer's schema-change topic —
	// the same topic the executor's batch detector writes to — so a column added
	// mid-stream lands in the pipeline's Schema changes tab instead of nowhere.
	// Separate from eventsWriter, which is pinned to pipeline.domain.events.
	driftWriter := &kafka.Writer{
		Addr:                   brokerAddr(cfg.KafkaBootstrapServers),
		Transport:              kafkaTransport(),
		Topic:                  kafkaclient.Topic("rsync.healer.schema-changes"),
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireAll,
		// Reporting is best-effort and sits on the CDC apply path: fail fast rather
		// than hold a committed batch waiting on the broker (see reportAppliedSchemaDrift).
		WriteTimeout: 3 * time.Second,
		MaxAttempts:  1,
	}
	defer driftWriter.Close()
	ddl.Drift = driftWriter

	// Seed the high-water tracker from the destination's durable offset table so a
	// restart skips any offsets already written (exactly-once on recovery). Tier-C
	// object stores don't implement get_cdc_offsets — an error/empty result leaves
	// the tracker empty, which is correct (deterministic keys handle idempotency).
	if seeded, serr := callGetCDCOffsets(ctx, httpClient, cfg, destType); serr == nil && len(seeded) > 0 {
		tracker.seed(seeded)
		logEvent("info", "seeded high-water tracker from destination offsets", "partitions", len(seeded))
	} else if serr != nil {
		logf("info", "high-water seed skipped (get_cdc_offsets unavailable: %v)", serr)
	}

	// Track per-table max written/read so EOF can finalize without a destination scan.
	var tableMaxWritten sync.Map // map[string]int64
	var tableMaxRead sync.Map    // map[string]int64
	var tableMaxBytes sync.Map   // map[string]int64 — cumulative committed bytes per (execution, table)

	// CDC counters per table
	var tableCDCInserts sync.Map // map[string]int64
	var tableCDCUpdates sync.Map // map[string]int64
	var tableCDCDeletes sync.Map // map[string]int64
	var tableCDCBytes sync.Map   // map[string]int64 — cumulative committed bytes per table

	// Track written object keys per table so we can emit _MANIFEST.json + _SUCCESS at EOF.
	writeStates := map[string]*tableWriteState{}

	// Fail-closed: track per-(partition,offset) failure counts so transient
	// destination errors (DNS unreachable, MCP container restarting, 5xx)
	// trigger a backoff-and-retry without committing the Kafka offset.
	// Without this the worker was silently committing on every failure and
	// pipelines reported `completed` with empty destination tables.
	// After maxAttempts, the message is treated as poison: DLQ + commit so the
	// worker doesn't loop forever on a schema mismatch or similar permanent
	// error.
	type batchAttemptKey struct {
		partition int
		offset    int64
	}
	batchAttempts := map[batchAttemptKey]int{}
	const maxBatchAttempts = 5

	// CDC micro-batching for object storage destinations (append-only bronze).
	// When enabled, offsets are committed ONLY after a flush succeeds.
	cdcBatcher := newCDCObjectBatcher(cfg, destType, reader, tracker, pgDB, httpClient, eventsWriter, dlqWriter, metrics, dedup.ttl,
		&tableCDCInserts, &tableCDCUpdates, &tableCDCDeletes, &tableCDCBytes)

	// CDC bulk-batching for relational DB destinations (PostgreSQL, MySQL, …).
	// Accumulates up to 1000 rows per (topic|partition|table), flushes every 5 s or on threshold.
	// Returns nil for object-storage / warehouse / non-CDC destinations so the caller is safe to nil-check.
	dbBatcher := newCDCDBBatcher(cfg, destType, reader, tracker, pgDB, httpClient, ddl, eventsWriter, dlqWriter, metrics, dedup.ttl,
		&tableCDCInserts, &tableCDCUpdates, &tableCDCDeletes, &tableCDCBytes)

	// Stall watchdog: a kafka-go member that is handed an empty partition assignment
	// at join stays alive, healthy-looking and Stable forever while consuming nothing
	// (see watchConsumerStall). Nothing else in this process — or in start_sink's
	// readiness probe — can see that, so watch for it explicitly.
	activity := newConsumerActivity(time.Now())
	go watchConsumerStall(ctx, cfg.KafkaBootstrapServers, cfg.ConsumerGroup, groupTopics,
		startOffset(cfg) == kafka.FirstOffset, activity, stallWatchdogTimeout(),
		func(detail string) {
			gStallRestart.Store(true)
			logEvent("error",
				"consumer stalled with records still waiting on the broker; restarting worker to force a fresh consumer-group join",
				"group", cfg.ConsumerGroup,
				"topics", strings.Join(groupTopics, ","),
				"detail", detail)
			cancel()
		})

	for {
		// Use a short poll timeout so we can flush due CDC batches even when the topic is quiet.
		activity.pollTick()
		fetchCtx, cancelFetch := context.WithTimeout(ctx, 1*time.Second)
		msg, err := reader.FetchMessage(fetchCtx)
		cancelFetch()
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				if cdcBatcher != nil {
					cdcBatcher.flushDue(ctx, time.Now().UTC())
				}
				if dbBatcher != nil {
					dbBatcher.flushDue(ctx, time.Now().UTC())
				}
				continue
			}
			if errors.Is(err, context.Canceled) {
				// Shutdown signalled (SIGTERM/SIGINT cancelled ctx). Flush any
				// buffered CDC batches before exit so a clean stop doesn't force the
				// next run to re-process them. The main ctx is already cancelled, so
				// run the drain on a fresh bounded context (DB writes + offset commits
				// on a cancelled ctx fail fast). Offsets for any batch we can't drain
				// in time stay uncommitted and are safely redelivered on restart, so a
				// cut-short drain loses nothing.
				drainCtx, drainCancel := context.WithTimeout(context.Background(), 15*time.Second)
				if dbBatcher != nil {
					dbBatcher.flushAll(drainCtx)
				}
				if cdcBatcher != nil {
					cdcBatcher.flushDue(drainCtx, time.Now().UTC().Add(time.Hour))
				}
				drainCancel()
				if gStallRestart.Load() {
					// Not a clean stop: leave a non-zero code so the supervisor
					// respawns this worker with a fresh consumer-group join.
					os.Exit(1)
				}
				return
			}
			metrics.setErr(err)
			logEvent("error", "kafka read error, backing off 1s before retry", "error", err.Error())
			time.Sleep(1 * time.Second)
			continue
		}

		activity.messageTick()
		updateReaderStats(metrics, reader)
		if strings.TrimSpace(msg.Topic) != "" {
			metrics.setLastTopic(msg.Topic)
		}

		sm, parseErr := parseSinkMessage(cfg, msg)
		if parseErr != nil {
			atomic.AddUint64(&metrics.failed, 1)
			metrics.setErr(parseErr)
			// No table to attribute the loss to — the message could not be parsed, so
			// sm is nil. Falls back to the topic-level aggregate in dlqRouted.
			if dlqErr := sendToDLQ(ctx, dlqWriter, msg, parseErr, metrics, ""); dlqErr != nil {
				logf("error", "fatal dlq error: %v", dlqErr)
				os.Exit(1)
			}
			_ = reader.CommitMessages(ctx, msg)
			atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
			atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
			continue
		}

		if sm.Ignore {
			logMsgEvent("debug", sm, msg, "message marked ignore (tombstone/bootstrap), committing and skipping")
			_ = reader.CommitMessages(ctx, msg)
			atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
			atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
			atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
			continue
		}

		if sm.EOF || sm.StorageType == "eof" {
			execTableKey := writeStateKey(sm)

			// For object storage destinations: write _MANIFEST.json and _SUCCESS markers for the partition.
			destType := canonicalConnectorType(cfg.DestinationConnector)
			looksLikeObjectStorage := isObjectStorageConnector(destType)
			if looksLikeObjectStorage {
				st := ensureWriteState(writeStates, sm)

				keys := st.keys
				counts := st.rowCounts
				// Fallback to durable ledger if we don't have in-memory keys (e.g. restart before EOF).
				if (len(keys) == 0 || len(keys) != len(counts)) && pgDB != nil {
					if k2, c2, err := loadKeysAndCountsFromLedger(ctx, pgDB, sm); err == nil && len(k2) > 0 && len(k2) == len(c2) {
						keys, counts = k2, c2
					}
				}

				if len(keys) > 0 && len(keys) == len(counts) {
					destCfg := cfg.DestinationConfig
					bucket := firstStr(destCfg, "bucket", "bucket_name")
					container := firstStr(destCfg, "container")
					prefix := firstStr(destCfg, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path")
					if prefix == "" {
						prefix = "test-aws-s3"
					}

					tablePart := sm.Table
					if idx := strings.LastIndex(tablePart, "."); idx >= 0 && idx+1 < len(tablePart) {
						tablePart = tablePart[idx+1:]
					}
					tablePart = sanitizePathPart(tablePart)

					m := manifest{
						PipelineID:   sm.PipelineID,
						ExecutionID:  sm.ExecutionID,
						Dt:           st.dt,
						UploadedKeys: keys,
						RowCounts:    counts,
						TotalRows:    sumInt64(counts),
						CreatedAt:    time.Now().UTC().Format(time.RFC3339),
					}
					mb, _ := json.Marshal(m)

					// Write manifest
					manifestParams := map[string]interface{}{
						"config":       destCfg,
						"key":          manifestKey(prefix, st.dataset, st.dbOrSchema, tablePart, st.dt),
						"data":         string(mb),
						"raw":          true,
						"content_type": "application/json",
					}
					if destType == "azure-blob" {
						if container != "" {
							manifestParams["container"] = container
						} else if bucket != "" {
							manifestParams["container"] = bucket
						}
					} else if bucket != "" {
						manifestParams["bucket"] = bucket
					}
					// Write the manifest with an IN-PLACE bounded retry. A continue-based
					// retry never re-fetches this EOF on a GroupID reader (FetchMessage
					// advances and never redelivers an uncommitted offset in-session), so
					// the DLQ-on-exhaustion branch below was unreachable and batchAttempts
					// never advanced past 1. The manifest is METADATA — the table rows
					// already landed — so on exhaustion we DLQ + commit to let the group
					// advance rather than stall forever.
					var err error
					for attempt := 1; attempt <= maxBatchAttempts; attempt++ {
						if _, err = callDestinationTool(ctx, httpClient, cfg, destType, "import_data", manifestParams); err == nil {
							break
						}
						atomic.AddUint64(&metrics.failed, 1)
						metrics.setErr(err)
						if attempt < maxBatchAttempts {
							logf("warning", "EOF manifest write failed (attempt %d/%d, retrying): %v", attempt, maxBatchAttempts, err)
							if !ctxAwareSleep(ctx, time.Duration(attempt)*2*time.Second) {
								return
							}
						}
					}
					if err != nil {
						logf("info", "EOF manifest write failed %d times, DLQ + commit to advance (data already landed): %v", maxBatchAttempts, err)
						if dlqErr := sendToDLQ(ctx, dlqWriter, msg, err, metrics, sm.Table); dlqErr != nil {
							logf("error", "fatal dlq error: %v", dlqErr)
							os.Exit(1)
						}
						_ = reader.CommitMessages(ctx, msg)
						atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
						atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
						atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
						continue
					}

					// Write success marker (empty)
					successParams := map[string]interface{}{
						"config":       destCfg,
						"key":          successKey(prefix, st.dataset, st.dbOrSchema, tablePart, st.dt),
						"data":         "",
						"raw":          true,
						"content_type": "text/plain",
					}
					if destType == "azure-blob" {
						if container != "" {
							successParams["container"] = container
						} else if bucket != "" {
							successParams["container"] = bucket
						}
					} else if bucket != "" {
						successParams["bucket"] = bucket
					}
					// Write the _SUCCESS marker with the same IN-PLACE bounded retry (see
					// the manifest branch above). The marker is metadata; on exhaustion we
					// DLQ + commit rather than stall the consumer group forever.
					for attempt := 1; attempt <= maxBatchAttempts; attempt++ {
						if _, err = callDestinationTool(ctx, httpClient, cfg, destType, "import_data", successParams); err == nil {
							break
						}
						atomic.AddUint64(&metrics.failed, 1)
						metrics.setErr(err)
						if attempt < maxBatchAttempts {
							logf("warning", "EOF success-marker write failed (attempt %d/%d, retrying): %v", attempt, maxBatchAttempts, err)
							if !ctxAwareSleep(ctx, time.Duration(attempt)*2*time.Second) {
								return
							}
						}
					}
					if err != nil {
						logf("info", "EOF success-marker write failed %d times, DLQ + commit to advance (data already landed): %v", maxBatchAttempts, err)
						if dlqErr := sendToDLQ(ctx, dlqWriter, msg, err, metrics, sm.Table); dlqErr != nil {
							logf("error", "fatal dlq error: %v", dlqErr)
							os.Exit(1)
						}
						_ = reader.CommitMessages(ctx, msg)
						atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
						atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
						atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
						continue
					}
				}
			}

			// Finalize status using max-so-far.
			written := loadMax(&tableMaxWritten, execTableKey)
			read := sm.TotalReadRows
			if read < loadMax(&tableMaxRead, execTableKey) {
				read = loadMax(&tableMaxRead, execTableKey)
			}
			finalStatus := "completed"
			if written != read {
				finalStatus = "degraded"
			}
			bytesFinal := loadMax(&tableMaxBytes, execTableKey)
			_ = emitTableStats(ctx, eventsWriter, sm, "batch", finalStatus, read, written, bytesFinal)
			atomic.AddUint64(&metrics.processed, 1)
			_ = reader.CommitMessages(ctx, msg)
			atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
			atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
			atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
			continue
		}

		// Handle CDC events differently from batch
		if sm.IsCDC {
			// For object storage destinations, prefer micro-batching and commit offsets on flush.
			if cdcBatcher != nil {
				// Apply consumer-hop transforms (masking / type-convert) BEFORE batching.
				// The object-storage CDC lane previously bypassed applyConsumerTransforms
				// entirely (only the relational processCDCEvent path called it), so a
				// pipeline configured to mask PII wrote PLAINTEXT to the bronze objects —
				// a silent data-privacy leak. applyConsumerTransforms mutates sm.Data and
				// re-syncs the CDC envelope (sm.After) for c/u/r ops (delete tombstones are
				// a no-op, matching the relational path). Fail-closed: never batch
				// untransformed data (mirror the structured/relational transform gates).
				if pgDB != nil {
					if tErr := applyConsumerTransforms(ctx, pgDB, cfg, sm); tErr != nil {
						atomic.AddUint64(&metrics.failed, 1)
						metrics.setErr(tErr)
						if dlqWriter != nil {
							if dlqErr := sendToDLQ(ctx, dlqWriter, msg, tErr, metrics, sm.Table); dlqErr != nil {
								logf("error", "fatal dlq error: %v", dlqErr)
								os.Exit(1)
							}
							_ = reader.CommitMessages(ctx, msg)
							continue
						}
						logMsgEvent("error", sm, msg, "cdc object-store consumer transform failed (no DLQ, will retry)", "reason", tErr.Error())
						continue
					}
				}
				cdcBatcher.add(ctx, msg, sm)
				// Opportunistic interval flush even under constant load.
				cdcBatcher.flushDue(ctx, time.Now().UTC())
				atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
				if sm.SourceTS > 0 {
					atomic.StoreInt64(&metrics.lastSourceTSUnixMs, sm.SourceTS)
					atomic.StoreInt64(&metrics.lastSourceLagMs, time.Now().UTC().UnixMilli()-sm.SourceTS)
				}
				continue
			}

			// For relational DB destinations, batch inserts/updates; process deletes immediately
			// (a delete must flush any buffered rows for the same table first to preserve ordering).
			if dbBatcher != nil {
				// Append-only history mode routes EVERY op (c/r/u/d) through the single-event
				// processCDCEvent → writeCDCToDestination path — the only append-aware writer
				// (it forces <dest>_import_data, injects _rsync_cdc_* via withCDCIdentity, sends
				// NO key_fields, and types the identity columns via mergeCDCIdentityColumnTypes).
				// The cdcDBBatcher is upsert-only: it would write via <dest>_upsert_data keyed on
				// the business PK, never populate identity values, and create a unique index that
				// breaks the delete-tombstone insert. Default (upsert) mode keeps batching c/r/u
				// unchanged; only deletes take the single-event path as before.
				if sm.CDCOp == "d" || cdcAppendMode(cfg) {
					// Flush any buffered rows for this table first to preserve ordering.
					// (In append mode the batcher never accumulates, so this is a no-op then.)
					flushed := dbBatcher.flushTable(ctx, msg.Topic, msg.Partition, sm.Table)
					// KI-CDC-DELETE-PATH-UNLOGGED: record the ordering barrier itself.
					// Gated on CDCOp == "d" because this branch is also entered for every
					// op in append mode (see the comment above), where the batcher never
					// accumulates and the flush is a no-op.
					//
					// Emitted even when nothing was flushed: rows_flushed=0 is itself the
					// answer when investigating whether a delete raced its own table's
					// buffered upserts.
					if sm.CDCOp == "d" {
						logMsgEvent("info", sm, msg, "cdc pre-delete flush",
							"flush_reason", "pre_delete_flush",
							"rows_flushed", flushed)
					}
					commit, err := processCDCEvent(ctx, tracker, pgDB, httpClient, cfg, ddl, eventsWriter, dlqWriter, msg, sm, metrics,
						&tableCDCInserts, &tableCDCUpdates, &tableCDCDeletes, &tableCDCBytes, dedup.ttl)
					if err != nil {
						metrics.setErr(err)
						if isFatal(err) {
							logf("error", "fatal cdc error (single-event): %v", err)
							os.Exit(1)
						}
					}
					// IN-PLACE bounded retry: re-process the SAME event on a transient failure;
					// a continue-based retry never re-attempts it (FetchMessage advances forward,
					// never redelivering an uncommitted offset in-session).
					for attempt := 2; !commit && err != nil && !isPoison(err) && attempt <= maxBatchAttempts; attempt++ {
						backoff := time.Duration(attempt-1) * 2 * time.Second
						logf("warning", "cdc single-event failed (attempt %d/%d, retrying in %s): %v",
							attempt-1, maxBatchAttempts, backoff, err)
						if !ctxAwareSleep(ctx, backoff) {
							return
						}
						commit, err = processCDCEvent(ctx, tracker, pgDB, httpClient, cfg, ddl, eventsWriter, dlqWriter, msg, sm, metrics,
							&tableCDCInserts, &tableCDCUpdates, &tableCDCDeletes, &tableCDCBytes, dedup.ttl)
						if err != nil {
							metrics.setErr(err)
							if isFatal(err) {
								logf("error", "fatal cdc error (single-event): %v", err)
								os.Exit(1)
							}
						}
					}
					if commit {
						delete(batchAttempts, batchAttemptKey{partition: msg.Partition, offset: msg.Offset})
						_ = reader.CommitMessages(ctx, msg)
						atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
						atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
					} else if err != nil {
						if isPoison(err) {
							logf("info", "cdc single-event is a poison message (not retryable), DLQ + commit to advance: %v", err)
						} else {
							logf("info", "cdc single-event failed %d times, DLQ + commit: %v", maxBatchAttempts, err)
						}
						if dlqErr := sendToDLQ(ctx, dlqWriter, msg, err, metrics, sm.Table); dlqErr != nil {
							logf("error", "fatal dlq error: %v", dlqErr)
							os.Exit(1)
						}
						delete(batchAttempts, batchAttemptKey{partition: msg.Partition, offset: msg.Offset})
						_ = reader.CommitMessages(ctx, msg)
						atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
						atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
					}
				} else {
					// Insert / update / snapshot read — accumulate in the batch (upsert mode only).
					// Apply consumer-hop transforms (masking / type-convert) BEFORE batching.
					// The relational CDC upsert-batcher path previously bypassed
					// applyConsumerTransforms entirely (only the delete/append processCDCEvent
					// path and the object-storage lane called it), so a pipeline configured to
					// mask PII wrote PLAINTEXT to the relational destination for every c/u/r
					// (insert/update/snapshot-read) event — a silent data-privacy leak, invisible
					// to the #617 transform-monitoring rollup because no transform_execution_logs
					// row was ever written. applyConsumerTransforms mutates sm.Data and re-syncs
					// the CDC envelope (sm.After) for c/u/r ops; the sm.IsCDC guard passes here
					// (this is inside the CDC branch), so the transform actually applies. Fail-
					// closed: never batch untransformed data — mirror the object-storage lane above.
					if pgDB != nil {
						if tErr := applyConsumerTransforms(ctx, pgDB, cfg, sm); tErr != nil {
							atomic.AddUint64(&metrics.failed, 1)
							metrics.setErr(tErr)
							if dlqWriter != nil {
								if dlqErr := sendToDLQ(ctx, dlqWriter, msg, tErr, metrics, sm.Table); dlqErr != nil {
									logf("error", "fatal dlq error: %v", dlqErr)
									os.Exit(1)
								}
								_ = reader.CommitMessages(ctx, msg)
								continue
							}
							logMsgEvent("error", sm, msg, "cdc relational upsert consumer transform failed (no DLQ, will retry)", "reason", tErr.Error())
							continue
						}
					}
					dbBatcher.add(ctx, msg, sm)
					// Opportunistic interval flush even under constant load.
					dbBatcher.flushDue(ctx, time.Now().UTC())
				}
				atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
				if sm.SourceTS > 0 {
					atomic.StoreInt64(&metrics.lastSourceTSUnixMs, sm.SourceTS)
					atomic.StoreInt64(&metrics.lastSourceLagMs, time.Now().UTC().UnixMilli()-sm.SourceTS)
				}
				continue
			}

			commit, err := processCDCEvent(ctx, tracker, pgDB, httpClient, cfg, ddl, eventsWriter, dlqWriter, msg, sm, metrics,
				&tableCDCInserts, &tableCDCUpdates, &tableCDCDeletes, &tableCDCBytes, dedup.ttl)
			if err != nil {
				metrics.setErr(err)
				if isFatal(err) {
					// Fail-closed: do not commit offset; crash so orchestration can surface the failure.
					logf("error", "fatal cdc error: %v", err)
					os.Exit(1)
				}
			}
			// IN-PLACE bounded retry: re-process the SAME event on a transient
			// failure. A `continue`-based retry never re-attempts it — FetchMessage
			// on a GroupID reader advances to the next message and never redelivers
			// an uncommitted offset in-session, so the poison branch below was
			// unreachable and a later commit buried the failed offset.
			for attempt := 2; !commit && err != nil && !isPoison(err) && attempt <= maxBatchAttempts; attempt++ {
				backoff := time.Duration(attempt-1) * 2 * time.Second
				logf("warning", "cdc event failed (attempt %d/%d, retrying in %s): %v",
					attempt-1, maxBatchAttempts, backoff, err)
				if !ctxAwareSleep(ctx, backoff) {
					return
				}
				commit, err = processCDCEvent(ctx, tracker, pgDB, httpClient, cfg, ddl, eventsWriter, dlqWriter, msg, sm, metrics,
					&tableCDCInserts, &tableCDCUpdates, &tableCDCDeletes, &tableCDCBytes, dedup.ttl)
				if err != nil {
					metrics.setErr(err)
					if isFatal(err) {
						logf("error", "fatal cdc error: %v", err)
						os.Exit(1)
					}
				}
			}
			// KI-CDC-SINK-INFRA-FAULT-DLQ-COMMITS — the unbatched CDC path condemns on
			// the same blind spot as cdcDBBatcher.flushBatch, just with a different
			// budget (maxBatchAttempts at 2/4/6/8s ≈ 20s). A destination outage that
			// outlasts it would dead-letter a live change event and commit its offset.
			// Hold the offset and keep probing instead; only fail closed if the
			// destination is still gone at the end of the extended budget.
			if !commit && err != nil && isDestInfraFault(err) {
				var landed bool
				err, landed = holdForInfraFault(ctx, "cdc single event", sm.Table, err, func() error {
					c, e := processCDCEvent(ctx, tracker, pgDB, httpClient, cfg, ddl, eventsWriter, dlqWriter, msg, sm, metrics,
						&tableCDCInserts, &tableCDCUpdates, &tableCDCDeletes, &tableCDCBytes, dedup.ttl)
					if e != nil {
						metrics.setErr(e)
						return e
					}
					if !c {
						return fmt.Errorf("cdc event still undelivered after retry (table=%s offset=%d)", sm.Table, msg.Offset)
					}
					return nil
				})
				if landed {
					commit, err = true, nil
				} else if ctx.Err() != nil {
					return // graceful shutdown; offset stays uncommitted
				} else if isDestInfraFault(err) {
					sinkFailClosed("fatal: cdc single event destination unreachable for the whole %s infrastructure-fault budget — failing closed: offset NOT committed and the event NOT dead-lettered, so Kafka redelivers it (table=%s, offset=%d): %v",
						infraRetryBudget(), sm.Table, msg.Offset, err)
					return
				}
			}
			if commit {
				delete(batchAttempts, batchAttemptKey{partition: msg.Partition, offset: msg.Offset})
				_ = reader.CommitMessages(ctx, msg)
				atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
				atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
			} else if err != nil {
				// Exhausted maxBatchAttempts → poison message: DLQ + commit so we advance.
				logf("info", "cdc event not delivered (poison message or exhausted retries), DLQ + commit to advance: %v", err)
				if classifyDestFault(err) == faultUnclassified {
					logUnclassifiedCondemn("cdc single event", sm.Table, err)
				}
				if dlqErr := sendToDLQ(ctx, dlqWriter, msg, err, metrics, sm.Table); dlqErr != nil {
					logf("error", "fatal dlq error: %v", dlqErr)
					os.Exit(1)
				}
				delete(batchAttempts, batchAttemptKey{partition: msg.Partition, offset: msg.Offset})
				_ = reader.CommitMessages(ctx, msg)
				atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
				atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
			}
			atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
			if sm.SourceTS > 0 {
				atomic.StoreInt64(&metrics.lastSourceTSUnixMs, sm.SourceTS)
				atomic.StoreInt64(&metrics.lastSourceLagMs, time.Now().UTC().UnixMilli()-sm.SourceTS)
			}
			continue
		}

		// Standard batch processing.
		// Idempotency check uses the Postgres ack ledger keyed on the Kafka
		// coordinates (topic+partition+offset) which are guaranteed unique.
		// Redis was previously the gate, keyed on `batch_offset` which the
		// orchestrator emits as 0 for every batch under cursor paging — every
		// batch after the first was silently skipped. Postgres is the durable
		// truth signal the postflight invariant already reads, so making it
		// the dedup source removes a layer instead of adding one.
		if pgDB != nil {
			var existing int
			skipDup := false
			for {
				derr := pgDB.QueryRowContext(ctx, `
				SELECT 1 FROM pipeline_batch_acks
				WHERE pipeline_id = $1
				  AND execution_id = $2
				  AND table_name = $3
				  AND kafka_topic = $4
				  AND kafka_partition = $5
				  AND kafka_offset = $6
				LIMIT 1
			`, sm.PipelineID, sm.ExecutionID, sm.Table, msg.Topic, msg.Partition, msg.Offset).Scan(&existing)
				if derr == nil {
					skipDup = true
					break
				}
				if errors.Is(derr, sql.ErrNoRows) {
					break
				}
				// Transient ledger error (Postgres down/reset/timeout). Retry the SAME
				// SELECT in place; a continue would advance FetchMessage past this message.
				// The partition stalls until the ledger answers — observable + recoverable.
				atomic.AddUint64(&metrics.failed, 1)
				metrics.setErr(derr)
				ak := batchAttemptKey{partition: msg.Partition, offset: msg.Offset}
				batchAttempts[ak] = batchAttempts[ak] + 1
				backoff := ledgerRetryBackoff(batchAttempts[ak])
				logf("warning", "ledger dedup SELECT failed (attempt %d, retrying in place in %s): %v",
					batchAttempts[ak], backoff, derr)
				if !ctxAwareSleep(ctx, backoff) {
					return
				}
			}
			if skipDup {
				// Ack already exists -> redelivery after a lost offset commit; skip + advance.
				atomic.AddUint64(&metrics.skipped, 1)
				_ = reader.CommitMessages(ctx, msg)
				atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
				atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
				atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
				continue
			}
		}

		rows := sm.Data

		// ── Blob (raw-bytes passthrough) lane — universal-blob-passthrough plan §3.
		// A blob message points to bytes staged byte-identical in the claim-check
		// store; the destination object-storage connector fetches them by data_ref
		// and writes them raw. No row fetch, no consumer transform, no schema — a
		// blob is opaque bytes, not records — so this short-circuits the structured
		// machinery entirely. One object copied == one ledger "row" (the executor
		// produced row_count=1 for this message), keeping reconciliation exact.
		if sm.IsBlob {
			var destKey string
			var werr error
			poison := false
			for attempt := 1; attempt <= maxBatchAttempts; attempt++ {
				destKey, werr = writeBlobToDestination(ctx, httpClient, cfg, sm)
				if werr == nil {
					break
				}
				atomic.AddUint64(&metrics.failed, 1)
				metrics.setErr(werr)
				if attempt < maxBatchAttempts {
					backoff := time.Duration(attempt) * 2 * time.Second
					logf("warning", "blob write failed (attempt %d/%d, retrying in %s): %v",
						attempt, maxBatchAttempts, backoff, werr)
					if !ctxAwareSleep(ctx, backoff) {
						return
					}
					continue
				}
				poison = true
			}
			if poison {
				// Exhausted retries → negative-ack (so the executor's landed-row
				// reconciliation sees the REAL reason, not an empty error) + DLQ +
				// commit to advance. Mirrors the structured poison path.
				logf("info", "blob write failed %d times, DLQ + commit: %v", maxBatchAttempts, werr)
				if pgDB != nil {
					if nackErr := persistNegativeBatchAckToPostgresWithRetry(ctx,
						pgDB, sm, 1, werr.Error(),
						msg.Topic, msg.Partition, msg.Offset, metrics); nackErr != nil {
						return
					}
				}
				if dlqErr := sendToDLQ(ctx, dlqWriter, msg, werr, metrics, sm.Table); dlqErr != nil {
					logf("error", "fatal dlq error: %v", dlqErr)
					os.Exit(1)
				}
				delete(batchAttempts, batchAttemptKey{partition: msg.Partition, offset: msg.Offset})
				_ = reader.CommitMessages(ctx, msg)
				atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
				atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
				atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
				continue
			}
			delete(batchAttempts, batchAttemptKey{partition: msg.Partition, offset: msg.Offset})
			// Durable ack BEFORE commit (same ordering as the structured path): a
			// committed offset with no ack would re-write on redelivery (silent dup).
			if pgDB != nil {
				if ackErr := persistBatchAckToPostgresWithRetry(ctx,
					pgDB, sm, 1, 1, destKey,
					msg.Topic, msg.Partition, msg.Offset, metrics); ackErr != nil {
					return
				}
			}
			// Staged blobs are reclaimed by the MinIO bucket lifecycle (staging/
			// prefix expiry), NOT a per-write delete: blobs are content-addressed, so
			// two distinct source objects with identical bytes share one staged
			// object and a per-write GC could race a sibling message's fetch.
			// Lifecycle is the backstop (same posture deleteFromMinIO documents).
			execTableKey := writeStateKey(sm)
			readSoFar := addAndLoad(&tableMaxRead, execTableKey, 1)
			writtenSoFar := addAndLoad(&tableMaxWritten, execTableKey, 1)
			bytesSoFar := addAndLoad(&tableMaxBytes, execTableKey, committedBatchBytes(sm, 1))
			_ = emitTableStats(ctx, eventsWriter, sm, "batch", "running", readSoFar, writtenSoFar, bytesSoFar)
			atomic.AddUint64(&metrics.processed, 1)
			_ = reader.CommitMessages(ctx, msg)
			atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
			atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
			atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
			continue
		}

		if sm.StorageType == "minio" && sm.ClaimCheckURL != "" {
			// Pass the header row_count so a successful-but-empty read (read-after-write
			// race / minio alias split-brain) is retried and ultimately DLQ'd, never
			// committed as a valid empty batch.
			rows, err = fetchFromMinIO(ctx, httpClient, sm.ClaimCheckURL, sm.RowCount)
			if err != nil {
				atomic.AddUint64(&metrics.failed, 1)
				metrics.setErr(err)
				// Record a NEGATIVE batch ack BEFORE the DLQ+commit. The rows were
				// never fetched from MinIO, so committing the offset here otherwise
				// silently drops the whole (>256KB claim-check) batch. Without the
				// negative ack the executor's landed-row reconciliation sees
				// (landed=0, ackRows=0) — the AMBIGUOUS benign-undercount branch —
				// and reports the run `completed`; with it, it sees (landed=0,
				// ackRows>0) and fails the run closed with the REAL MinIO reason.
				// rows_read = sm.RowCount (the expected count; nothing landed).
				// Mirrors the dest-write / transform / blob poison paths above.
				if pgDB != nil {
					nackRows := sm.RowCount
					if nackRows <= 0 {
						nackRows = 1
					}
					if nackErr := persistNegativeBatchAckToPostgresWithRetry(ctx,
						pgDB, sm, nackRows, err.Error(),
						msg.Topic, msg.Partition, msg.Offset, metrics); nackErr != nil {
						// ctx-cancel (shutdown) — leave the offset uncommitted for redelivery.
						return
					}
				}
				if dlqErr := sendToDLQ(ctx, dlqWriter, msg, err, metrics, sm.Table); dlqErr != nil {
					logf("error", "fatal dlq error: %v", dlqErr)
					os.Exit(1)
				}
				_ = reader.CommitMessages(ctx, msg)
				continue
			}
			// Claim-check (MinIO) messages never populate sm.Data in parseSinkMessage
			// (it returns early for storage_type=minio). The consumer-transform hop below
			// reads and writes sm.Data in place, and line `rows = sm.Data` re-reads it
			// afterward. Without this assignment sm.Data stays nil, applyConsumerTransforms
			// bails on the empty slice, and the re-read WIPES every fetched row — silently
			// dropping the entire batch for any payload >256KB (the claim-check threshold).
			// Restore the invariant: sm.Data is the canonical row set for this message.
			sm.Data = rows
			// DIAG: surface how many rows the claim-check actually yielded. A 0 here on a
			// message whose header says row_count>0 pinpoints a MinIO read/format problem
			// rather than a destination-write problem.
			logMsgEvent("info", sm, msg, "claim-check fetched from MinIO",
				"rows_fetched", fmt.Sprintf("%d", len(rows)),
				"row_count_header", fmt.Sprintf("%d", sm.RowCount),
				"claim_check_url", sm.ClaimCheckURL)
		}

		rowsRead := sm.RowCount
		// Defensive: do not trust producer-provided row_count if it exceeds the decoded payload.
		// This can happen if a producer slices beyond len(rows) (up to cap) and includes a trailing
		// nil map, which becomes JSON `null` and inflates row_count.
		if rowsRead <= 0 || rowsRead > int64(len(rows)) {
			rowsRead = int64(len(rows))
		}

		// Consumer-hop transforms are NOT applied on the batch path. This call site is
		// reached only for batch messages (sm.IsCDC == false, after the CDC branches above
		// continue), and applyConsumerTransforms early-returns for !sm.IsCDC. Batch
		// transforms are applied exactly once upstream by the executor's producer hop;
		// re-applying the identical consumer rows here would double-apply every transform
		// (mask_pii -> sha256(sha256(x)), json_flatten twice) — see the
		// applyConsumerTransforms doc-comment. The call is kept as a guarded no-op so the
		// single-application guard lives in exactly one place; on batch sm.Data is left
		// unchanged (already producer-transformed) and the error branch below never fires.
		if pgDB != nil && len(rows) > 0 {
			if tErr := applyConsumerTransforms(ctx, pgDB, cfg, sm); tErr != nil {
				atomic.AddUint64(&metrics.failed, 1)
				metrics.setErr(tErr)
				// Fail-closed: a transform error must not let untransformed data through.
				if dlqWriter != nil {
					// KI-NLCHAT-TYPECONVERT-FALSE-SUCCESS: write a NEGATIVE batch ack
					// BEFORE the DLQ+commit so the executor's landed-row reconciliation
					// sees ackRows>0 with the REAL transform reason (AckEvidencedDrop)
					// instead of the ambiguous zero-ack path — a 0-landed run must never
					// be reported `completed`. Mirrors the dest-write poison path above.
					if pgDB != nil {
						if nackErr := persistNegativeBatchAckToPostgresWithRetry(ctx,
							pgDB, sm, int64(len(rows)), tErr.Error(),
							msg.Topic, msg.Partition, msg.Offset, metrics); nackErr != nil {
							logMsgEvent("error", sm, msg, "consumer transform failed: negative-ack persist failed", "reason", nackErr.Error())
						}
					}
					if dlqErr := sendToDLQ(ctx, dlqWriter, msg, tErr, metrics, sm.Table); dlqErr != nil {
						logf("error", "fatal dlq error: %v", dlqErr)
						os.Exit(1)
					}
					_ = reader.CommitMessages(ctx, msg)
					continue
				}
				// No DLQ configured: leave the offset uncommitted so kafka-go redelivers
				// (the executor's minioFilesCreated>0 reconciliation guard fails the run
				// closed in this case — KI-NLCHAT-TYPECONVERT-FALSE-SUCCESS gap A1).
				logMsgEvent("error", sm, msg, "consumer transform failed (no DLQ, will retry)", "reason", tErr.Error())
				continue
			}
			rows = sm.Data // applyConsumerTransforms updates sm.Data in-place
		}

		// run_mode=reload: best-effort destination cleanup (once per execution+table, before first write)
		destType := canonicalConnectorType(cfg.DestinationConnector)
		looksLikeObjectStorage := isObjectStorageConnector(destType)
		st := ensureWriteState(writeStates, sm)
		// Only attempt cleanup at the beginning of a table's batch stream.
		if st.runMode == "reload" && !st.reloadCleaned && sm.BatchOffset == 0 {
			destCfg := cfg.DestinationConfig

			if looksLikeObjectStorage {
				bucket := firstStr(destCfg, "bucket", "bucket_name")
				container := firstStr(destCfg, "container")
				prefix := firstStr(destCfg, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path")
				if prefix == "" {
					prefix = "test-aws-s3"
				}
				tablePart := sm.Table
				if idx := strings.LastIndex(tablePart, "."); idx >= 0 && idx+1 < len(tablePart) {
					tablePart = tablePart[idx+1:]
				}
				tablePart = sanitizePathPart(tablePart)

				delParams := map[string]interface{}{
					"config": destCfg,
					"prefix": tablePrefix(prefix, st.dataset, st.dbOrSchema, tablePart),
				}
				if destType == "azure-blob" {
					if container != "" {
						delParams["container"] = container
					} else if bucket != "" {
						delParams["container"] = bucket
					}
				} else if bucket != "" {
					delParams["bucket"] = bucket
				}
				_, err := callDestinationTool(ctx, httpClient, cfg, destType, "delete_prefix", delParams)
				if err != nil {
					atomic.AddUint64(&metrics.failed, 1)
					metrics.setErr(err)
					// Cleanup failures are environmental; fail-closed to avoid committing past this message.
					logf("error", "fatal reload cleanup error: %v", err)
					os.Exit(1)
				}
				st.reloadCleaned = true
			} else {
				// Database destinations: full-refresh = DROP the destination table so the
				// following batch's ensure_table rebuilds it from scratch. This mirrors the
				// executor's reload path and DMS/Fivetran/Airbyte full-load semantics
				// (DROP_AND_CREATE), avoiding TRUNCATE's stale-column / type-drift problem
				// (see the connector drop_table docstring). The connector owns the operation
				// via the structured drop_table tool — we never send raw SQL over the wire.
				//
				// Only drop when DDL rebuild is available; otherwise ensure_table can't
				// recreate the schema, so skip cleanup (best-effort, matches prior behavior).
				// safeTruncateSQL is reused purely as a safe-identifier gate on the target.
				target := normalizeTargetTable(destType, destCfg, sm.Table)
				if _, ok := safeTruncateSQL(target); ok && ddl != nil && ddl.supported(ctx, httpClient, cfg, destType) {
					dropArgs := map[string]interface{}{
						"config":      destCfg,
						"table":       target,
						"pipeline_id": cfg.PipelineID,
					}
					// Drop must hit the SAME database the sink writes to. target is
					// bare for single-namespace dests, so forward the namespace; else
					// the connector drops config["database"] while data lives elsewhere.
					addNamespaceParam(dropArgs, sm.DBOrSchema)
					_, err := callDestinationTool(ctx, httpClient, cfg, destType, "drop_table", dropArgs)
					if err != nil {
						atomic.AddUint64(&metrics.failed, 1)
						metrics.setErr(err)
						logf("error", "fatal reload cleanup error: %v", err)
						os.Exit(1)
					}
					// CRITICAL: invalidate the process-lived ensured-schema cache for
					// this table. ddl.Ensured persists across executions; after the
					// physical DROP above it would still report the (now-gone) table
					// as fully ensured, so ensureDestinationTable's "already ensured all
					// columns" fast-path (skip) would fire and ensure_table would NOT
					// recreate the table. Every subsequent INSERT then fails with
					// `relation "<schema.table>" does not exist` until the batch is
					// DLQ'd — silent full-table data loss on reload. Deleting the key
					// forces a real ensure_table (CREATE) on this batch's write.
					//
					// The cache key MUST match the one ensureDestinationTable keys on.
					// writeToDestination resolves its target via resolveDestTableForWrite,
					// which BARES the table when a real destination namespace is set
					// (key "<destType>:<table>"), whereas `target` here is the qualified
					// normalizeTargetTable form ("<destType>:<schema>.<table>"). Deleting
					// only the qualified key left the bare key live, so the ensure fast-path
					// still skipped CREATE after the physical DROP → every INSERT failed
					// `relation "<schema.table>" does not exist` (silent reload data loss).
					// Invalidate BOTH forms so the recreate fires regardless of namespace.
					ddl.Ensured.Delete(ensuredCacheKey(destType, sm.DBOrSchema, target))
					ddl.Ensured.Delete(ensuredCacheKey(destType, sm.DBOrSchema, resolveDestTableForWrite(destType, destCfg, sm.Table, sm.DBOrSchema)))
					st.reloadCleaned = true
				} else {
					// Can't safely identify the target or DDL rebuild is disabled: proceed
					// without cleanup (reload still resets checkpoints upstream).
					st.reloadCleaned = true
				}
			}
		}

		// Destination write with IN-PLACE bounded retry.
		//
		// We must retry the SAME message synchronously here, NOT `continue` the
		// consumer loop. reader.FetchMessage on a GroupID reader advances to the
		// NEXT message and never redelivers an uncommitted offset within a
		// session, so the old `continue`-based retry never re-attempted this
		// write: its per-offset attempt counter was stuck at 1, the poison
		// branch (attempts >= maxBatchAttempts) below was UNREACHABLE, and a
		// later offset commit (e.g. the EOF marker) buried this offset. The
		// effect was a silent full-batch drop on any persistent dest-write error
		// (classic case: destination type drift like `name bigint` vs text) —
		// rows read > 0, landed = 0, and NO ack at all on file, so the executor's
		// reconciliation hit the ambiguous (landed=0, ackRows=0) branch and
		// reported an empty-reason "unverified completion".
		var writtenRows int64
		var destKey string
		var destKeys []string
		var destCounts []int64
		poison := false
		// Object-storage destinations split a batch into one part-file per Hive
		// partition tuple when partition_by is set, then further into size/row-capped
		// chunks when max_file_rows/max_file_mb are set (Group C file rolling). Each
		// resulting object is one objectWriteUnit. Relational/warehouse + the
		// unpartitioned, no-rolling object case → a single unit (partSegs="" +
		// partSuffix="" → byte-identical legacy key). Computed once; the retry below
		// re-writes all units (object writes are idempotent by key).
		var writeUnits []objectWriteUnit
		if looksLikeObjectStorage {
			maxRows, maxBytes := objectFileRollLimits(cfg.DestinationConfig)
			rolling := maxRows > 0 || maxBytes > 0
			for _, g := range splitRowsForObjectPartition(cfg.DestinationConfig, rows) {
				for ci, ch := range chunkRowsForFileRolling(g.rows, maxRows, maxBytes) {
					sfx := ""
					if rolling {
						sfx = fmt.Sprintf("%04d", ci)
					}
					writeUnits = append(writeUnits, objectWriteUnit{rows: ch, partSegs: g.partSegs, partSuffix: sfx})
				}
			}
		} else {
			writeUnits = []objectWriteUnit{{rows: rows, partSegs: "", partSuffix: ""}}
		}
		for attempt := 1; attempt <= maxBatchAttempts; attempt++ {
			writtenRows = 0
			destKeys = destKeys[:0]
			destCounts = destCounts[:0]
			err = nil
			for _, u := range writeUnits {
				var wr int64
				var k string
				wr, k, err = writeToDestination(ctx, httpClient, cfg, ddl, sm, u.rows, u.partSegs, u.partSuffix)
				if err != nil {
					break
				}
				writtenRows += wr
				destKey = k
				destKeys = append(destKeys, k)
				destCounts = append(destCounts, wr)
			}
			if err == nil {
				break
			}
			atomic.AddUint64(&metrics.failed, 1)
			metrics.setErr(err)
			if attempt < maxBatchAttempts {
				// Fail-closed: do NOT commit the offset; back off and re-attempt
				// the SAME write. ctx-cancel (shutdown) returns early, leaving the
				// offset uncommitted for redelivery on the next start.
				backoff := time.Duration(attempt) * 2 * time.Second
				logf("warning", "dest write failed (attempt %d/%d, retrying in %s): %v",
					attempt, maxBatchAttempts, backoff, err)
				if !ctxAwareSleep(ctx, backoff) {
					return
				}
				continue
			}
			poison = true // exhausted all attempts → treat as a poison message
		}
		if poison {
			// Poison message — escape via negative ack + DLQ + commit so we advance.
			logf("info", "dest write failed %d times, DLQ + commit: %v", maxBatchAttempts, err)
			// Record a NEGATIVE ack (rows_written=0 + last_error) BEFORE we advance
			// the offset. Without it the offset commit below leaves the executor's
			// landed-row reconciliation blind: it sees (landed=0, ackRows=0) — the
			// AMBIGUOUS branch — and reports unverified_completion with an EMPTY
			// error ("Data transfer failed: "). With the negative ack the executor
			// sees (landed=0, ackRows>0) and surfaces the REAL reason. Durable +
			// retried-forever like the positive ack; only ctx-cancel (shutdown)
			// returns early, leaving the offset uncommitted for redelivery.
			if pgDB != nil {
				if nackErr := persistNegativeBatchAckToPostgresWithRetry(ctx,
					pgDB, sm, rowsRead, err.Error(),
					msg.Topic, msg.Partition, msg.Offset, metrics); nackErr != nil {
					return
				}
			}
			if dlqErr := sendToDLQ(ctx, dlqWriter, msg, err, metrics, sm.Table); dlqErr != nil {
				logf("error", "fatal dlq error: %v", dlqErr)
				os.Exit(1)
			}
			delete(batchAttempts, batchAttemptKey{partition: msg.Partition, offset: msg.Offset})
			_ = reader.CommitMessages(ctx, msg)
			atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
			atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
			atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
			continue
		}
		// Successful write — clear retry counter for this offset.
		delete(batchAttempts, batchAttemptKey{partition: msg.Partition, offset: msg.Offset})
		if writtenRows < 0 {
			writtenRows = 0
		}

		// Track keys for manifest emission (object storage only). With partition_by a
		// single batch produced multiple part-files (one per partition), so record
		// every key + its row count — the EOF manifest lists them all.
		if looksLikeObjectStorage {
			st := ensureWriteState(writeStates, sm)
			st.keys = append(st.keys, destKeys...)
			st.rowCounts = append(st.rowCounts, destCounts...)
		}

		// Write to Postgres (durable ledger) — fail-closed.
		//
		// The destination write above already succeeded. If we commit the
		// Kafka offset without landing the ack, the postflight invariant
		// goes blind AND a future redelivery after a process restart will
		// not find an ack in the dedup SELECT → re-write to destination →
		// silent duplicate.
		//
		// The previous policy (5 retries → DLQ + commit) is therefore
		// unsafe: it silently advanced the offset on a row we had no
		// ledger entry for. We instead retry forever with capped backoff
		// until either the ack lands or the worker is shut down. The
		// partition stalls during a sustained Postgres outage — an
		// observable, recoverable failure that is strictly better than
		// silent duplicates.
		if pgDB != nil {
			ackErr := persistBatchAckToPostgresWithRetry(ctx,
				pgDB, sm, writtenRows, rowsRead, destKey,
				msg.Topic, msg.Partition, msg.Offset, metrics)
			if ackErr != nil {
				// Only reachable when ctx is cancelled — graceful shutdown.
				return
			}
		}

		// Cumulative running totals per (execution, table) — the orchestrator
		// emits sm.BatchOffset=0 for every batch when cursor paging is used,
		// so the old `sm.BatchOffset + rowsRead` formula plateaued at the
		// first batch's size. We instead ADD each batch's read/written to a
		// running total in worker memory; the projector still uses GREATEST()
		// in the stats projection so monotonicity is preserved.
		// Claim-check cleanup: once rows are durably written to the destination
		// (ack-ledgered above), the MinIO staging object is no longer needed.
		// Delete it to prevent unbounded MinIO growth: every batch payload
		// >256KB stages to MinIO and was previously NEVER reclaimed. Best-effort;
		// a delete failure must never stall the pipeline (bucket lifecycle is the backstop).
		if sm.StorageType == "minio" && sm.ClaimCheckURL != "" {
			if delErr := deleteFromMinIO(ctx, httpClient, sm.ClaimCheckURL); delErr != nil {
				logf("warning", "claim-check cleanup: failed to delete %s (non-fatal): %v", sm.ClaimCheckURL, delErr)
			}
		}

		execTableKey := writeStateKey(sm)
		readSoFar := addAndLoad(&tableMaxRead, execTableKey, rowsRead)
		writtenSoFar := addAndLoad(&tableMaxWritten, execTableKey, writtenRows)
		bytesSoFar := addAndLoad(&tableMaxBytes, execTableKey, committedBatchBytes(sm, writtenRows))

		// Emit destination-truth stats (monotonic). Projector uses GREATEST().
		_ = emitTableStats(ctx, eventsWriter, sm, "batch", "running", readSoFar, writtenSoFar, bytesSoFar)

		atomic.AddUint64(&metrics.processed, 1)
		_ = reader.CommitMessages(ctx, msg)
		atomic.StoreInt64(&metrics.lastCommittedOffset, msg.Offset)
		atomic.StoreInt64(&metrics.lastCommittedAtUnixMs, time.Now().UTC().UnixMilli())
		atomic.StoreInt64(&metrics.lastProcessedAtUnixMs, time.Now().UTC().UnixMilli())
	}
}

func startOffset(cfg *WorkerConfig) int64 {
	if cfg == nil {
		return kafka.FirstOffset
	}
	switch strings.ToLower(strings.TrimSpace(cfg.StartOffset)) {
	case "latest", "newest":
		return kafka.LastOffset
	default:
		return kafka.FirstOffset
	}
}

type effectiveDedupConfig struct {
	enabled   bool
	onFailure string // fail_pipeline or warn_continue
	ttl       time.Duration
}

// resolveDedupConfig now only resolves the ack-audit TTL. Deduplication itself is
// no longer Redis-backed: exactly-once for Tier-A relational sinks comes from the
// destination's _rsync_cdc_offsets table (written in the same transaction as the
// data) and the in-memory high-water tracker seeded from it. The legacy
// redis_enabled / on_redis_failure knobs are still parsed for backward-compatible
// config but no longer gate startup. ttl flows into the best-effort Postgres ack
// ledger as an informational retention hint only.
func resolveDedupConfig(cfg *WorkerConfig, destType string) (effectiveDedupConfig, error) {
	destType = canonicalConnectorType(destType)

	enabled := true
	onFailure := "fail_pipeline"
	ttlHours := 7 * 24

	if cfg != nil && cfg.KafkaSinkWorker != nil && cfg.KafkaSinkWorker.Deduplication != nil {
		d := cfg.KafkaSinkWorker.Deduplication
		if d.RedisEnabled != nil {
			enabled = *d.RedisEnabled
		}
		if strings.TrimSpace(d.OnRedisFailure) != "" {
			onFailure = strings.ToLower(strings.TrimSpace(d.OnRedisFailure))
		}
		if d.RedisKeyTTLHours > 0 {
			ttlHours = d.RedisKeyTTLHours
		}
	}

	if onFailure != "fail_pipeline" && onFailure != "warn_continue" {
		return effectiveDedupConfig{}, fmt.Errorf("invalid kafka_sink_worker.deduplication.on_redis_failure=%q (expected fail_pipeline or warn_continue)", onFailure)
	}

	return effectiveDedupConfig{
		enabled:   enabled,
		onFailure: onFailure,
		ttl:       time.Duration(ttlHours) * time.Hour,
	}, nil
}

// processCDCEvent handles a single CDC (Debezium) event with idempotency.
// Returns whether the Kafka offset should be committed.
func processCDCEvent(ctx context.Context, hw *highWaterTracker, pgDB *sql.DB, httpClient *http.Client, cfg *WorkerConfig, ddl *DDLSupport,
	eventsWriter *kafka.Writer, dlqWriter *kafka.Writer, msg kafka.Message, sm *SinkMessage, metrics *Metrics,
	cdcInserts, cdcUpdates, cdcDeletes, cdcBytes *sync.Map, ackTTL time.Duration) (commit bool, err error) {

	// CDC idempotency is Kafka-position based (topic+partition+offset): for Tier-A
	// relational sinks the offset is written into _rsync_cdc_offsets in the same
	// transaction as the delete, so any offset at/below the durable high-water mark
	// has already been applied — skip it.
	if hw.seen(msg.Topic, msg.Partition, msg.Offset) {
		atomic.AddUint64(&metrics.skipped, 1)
		return true, nil
	}

	// Apply consumer transforms (CDC path) before writing to destination.
	if pgDB != nil {
		if tErr := applyConsumerTransforms(ctx, pgDB, cfg, sm); tErr != nil {
			atomic.AddUint64(&metrics.failed, 1)
			// Fail-closed: return (false, err) so the CALLER's in-place bounded retry
			// re-attempts this event; the caller DLQs + commits only after
			// maxBatchAttempts is exhausted. Do NOT DLQ here on the first failure —
			// returning commit=true made the caller's retry loop (guarded on !commit)
			// dead code, dead-lettering a recoverable event on attempt 1.
			return false, tErr
		}
	}

	// Apply CDC operation to destination
	var writtenRows int64
	var destKey string

	switch sm.CDCOp {
	case "c", "r": // create, read (snapshot) - insert
		if len(sm.Data) > 0 {
			// Snapshot reads ("r") can re-send existing rows; treat as upsert to be idempotent.
			// We also upsert for "c" to avoid duplicate-key failures on retries.
			writtenRows, destKey, err = writeCDCToDestination(ctx, httpClient, cfg, ddl, msg, sm, "upsert")
			if err == nil {
				incrementCounter(cdcInserts, sm.Table, 1)
				if sm.CDCOp == "r" {
					atomic.AddUint64(&metrics.cdcReads, 1)
				} else {
					atomic.AddUint64(&metrics.cdcInserts, 1)
				}
			}
		}
	case "u": // update - upsert
		if len(sm.Data) > 0 {
			writtenRows, destKey, err = writeCDCToDestination(ctx, httpClient, cfg, ddl, msg, sm, "upsert")
			if err == nil {
				incrementCounter(cdcUpdates, sm.Table, 1)
				atomic.AddUint64(&metrics.cdcUpdates, 1)
			}
		}
	case "d": // delete
		// Upsert mode performs a deterministic keyed delete, which requires the PK in the
		// Kafka record key. Append-only mode instead records the delete as a tombstone row
		// (op=D) from the Before image, so the PK is not mandatory there.
		// Do NOT DLQ individual deletes without PK (trap: DLQ storm and never catch up).
		if !cdcAppendMode(cfg) && (sm.PK == nil || len(sm.KeyFields) == 0) {
			// Poison, not fatal: a keyless delete can never be applied to an
			// upsert dest no matter how many times we retry. DLQ + advance so the
			// pipeline keeps delivering other tables instead of crash-looping.
			// (Fix: add a PK to the source table, or run the pipeline in append
			// mode, which records deletes as tombstones without a PK.)
			return false, poisonError{err: fmt.Errorf("CDC delete missing primary key in message key (required for deterministic deletes)")}
		}
		writtenRows, destKey, err = writeCDCToDestination(ctx, httpClient, cfg, ddl, msg, sm, "delete")
		if err == nil {
			incrementCounter(cdcDeletes, sm.Table, 1)
			atomic.AddUint64(&metrics.cdcDeletes, 1)
		}
	default:
		err = fmt.Errorf("unknown CDC operation: %s", sm.CDCOp)
	}

	if err != nil {
		atomic.AddUint64(&metrics.failed, 1)
		// Fail-closed: return (false, err) so the CALLER's in-place bounded retry
		// re-attempts the write; the caller DLQs + commits only after
		// maxBatchAttempts. A transient destination blip (connection reset, brief
		// MCP unavailability, deadlock) must NOT dead-letter a good event on attempt 1.
		// (Previously this DLQ'd and returned commit=true on the first failure, making
		// the caller's !commit-guarded retry loop unreachable — a silent data-quality
		// regression for every CDC event in append-only mode and every delete.)
		return false, err
	}

	// Advance the in-memory high-water mark now that the destination has the row.
	// For Tier-A relational sinks the durable offset (_rsync_cdc_offsets) was written
	// inside writeCDCToDestination's transaction; the tracker mirrors that so redelivered
	// offsets are skipped without a round-trip. For Tier-B/C the tracker is best-effort.
	hw.advance(msg.Topic, msg.Partition, msg.Offset)

	// Write to Postgres ledger (durable audit trail, best-effort — never fatal).
	if pgDB != nil {
		_ = persistCDCAckToPostgres(ctx, pgDB, sm, writtenRows, destKey, msg.Topic, msg.Partition, msg.Offset)
	}

	// Emit CDC TABLE_STATS (running mode for streaming)
	incrementCounter(cdcBytes, sm.Table, cdcRowBytes(sm))
	inserts := loadCounter(cdcInserts, sm.Table)
	updates := loadCounter(cdcUpdates, sm.Table)
	deletes := loadCounter(cdcDeletes, sm.Table)
	_ = emitCDCTableStats(ctx, eventsWriter, sm, inserts, updates, deletes, loadCounter(cdcBytes, sm.Table), loadCounter(&metrics.dlqByTable, sm.Table))

	atomic.AddUint64(&metrics.processed, 1)
	return true, nil
}

// cdcAckKeyFor generates a crash-safe idempotency key for CDC events.
// For bronze/object storage (and generally), we key by Kafka position: topic + partition + offset.
func cdcAckKeyFor(pipelineID, topic string, partition int, offset int64) string {
	safeTopic := strings.ReplaceAll(strings.TrimSpace(topic), " ", "_")
	safeTopic = strings.ReplaceAll(safeTopic, "/", "_")
	safeTopic = strings.ReplaceAll(safeTopic, "\\", "_")
	safeTopic = strings.ReplaceAll(safeTopic, ":", "_")
	if safeTopic == "" {
		safeTopic = "unknown_topic"
	}
	return fmt.Sprintf("cdc_ack:%s:%s:p%d:o%d", pipelineID, safeTopic, partition, offset)
}

// isSuspiciousTableOverride rejects destination table overrides that are English
// articles / filler words. A NL parser captured "the" from "...into the <conn>"
// and it was forced onto destCfg["table"], silently funnelling every pipeline
// using that destination into a junk table "the". Defense-in-depth for the
// upstream fix in the orchestrator's inferTablesFromUserRequest.
func isSuspiciousTableOverride(s string) bool {
	switch strings.ToLower(strings.TrimSpace(strings.Trim(s, `"`))) {
	case "the", "a", "an", "to", "into", "from", "as", "table", "tables",
		"source", "destination", "dest", "sink", "target", "connection":
		return true
	}
	return false
}

func normalizeTargetTable(destType string, destCfg map[string]interface{}, fallback string) string {
	target := strings.TrimSpace(fallback)
	if destCfg != nil {
		if v, ok := destCfg["table"].(string); ok {
			vt := strings.TrimSpace(v)
			if vt != "" && !isSuspiciousTableOverride(vt) {
				target = vt
			} else if vt != "" {
				// Reject article/filler overrides (e.g. "the") that an NL parser
				// captured and forced onto destCfg["table"]; honoring them
				// silently funnels every pipeline's rows into one junk table.
				logf("info",
					"normalizeTargetTable: ignoring suspicious table override %q; routing to source table %q",
					vt, fallback)
			}
		}
	}
	// MySQL Debezium events identify tables as "<db>.<table>" (where <db> is the MySQL database).
	// For Postgres destinations, that "<db>" is typically NOT a schema. Normalize to public.<table>
	// when the prefix matches the destination database name.
	canonicalDest := canonicalConnectorType(destType)
	if canonicalDest == "postgresql" || canonicalDest == "postgres" {
		dbName := firstStr(destCfg, "database", "db_name", "db")
		parts := strings.Split(strings.TrimSpace(target), ".")
		if dbName != "" && len(parts) == 2 && strings.EqualFold(parts[0], dbName) {
			target = "public." + parts[1]
		}
	}
	// MySQL has no separate schema namespace — every database IS the schema.
	// When the source is Postgres-shaped ("<schema>.<table>", typically
	// "public.<table>"), MySQL parses "public" as a database name and the
	// write fails with "Access denied for user … to database 'public'".
	// Strip any leading schema part so the MySQL connector falls back to
	// its configured database. Same logic applies to other single-namespace
	// destinations (SQLite, ClickHouse default, etc.).
	if isSingleNamespaceDest(canonicalDest) {
		t := strings.TrimSpace(target)
		// Unquote optional double quotes around each part for the prefix
		// comparison: `"public"."foo"` should normalize the same as
		// `public.foo`.
		if idx := strings.Index(t, "."); idx > 0 && idx < len(t)-1 {
			target = t[idx+1:]
		}
	}
	return target
}

// isSingleNamespaceDest reports whether a destination DB type uses a single
// namespace (database == schema). Cross-DB pipelines from Postgres-shaped
// sources prefix table names with a schema ("public.<table>") that these
// destinations would otherwise misinterpret as a database name.
//
// MongoDB qualifies here: the database comes from the connection config and the
// collection is the (bare) table name — there is no schema layer — so a leading
// "<schema>." / "<db>." prefix from the source must be stripped to the collection.
func isSingleNamespaceDest(canonicalDest string) bool {
	switch canonicalDest {
	case "mysql", "mariadb", "sqlite", "clickhouse", "mongodb":
		return true
	}
	return false
}

// isRealNamespace reports whether s is a usable per-pipeline destination
// namespace. Mirrors the executor's isRealNamespace: the planner stores the
// literal "default" as a generic placeholder — it is NOT a real database/schema
// name and MUST NOT be forwarded, or the connector would skip it and fall back
// to the shared connection default (e.g. MySQL "pipeline_test").
func isRealNamespace(s string) bool {
	s = strings.TrimSpace(s)
	return s != "" && !strings.EqualFold(s, "default")
}

// destinationNamespaceForStats reports the destination database/schema that TABLE_STATS
// should be labelled with, or "" when this sink cannot honestly name one.
//
// Stats used to carry only the SOURCE-derived name: CDC overwrites sm.Table with
// "<source schema>.<source table>" (parseCDCMessage, "Override table from source"), and
// the stats builders split that for metadata.table.schema. So a MySQL→Postgres pipeline
// whose rows landed in rsync_verify_cdc reported schema "pipeline_test" — the source
// database — and anyone reading the stats to answer "where did my data go?" was handed
// the wrong half of the pipeline.
//
// The destination name is a SEPARATE field rather than a correction of the existing one,
// because metadata.table.qualified_name is the cross-producer correlation key: the
// orchestrator's cdcstats agent independently upserts the same
// (pipeline_id, execution_id, qualified_name) row (migration 034's unique index) with the
// captured-side counters, and it names the SOURCE (cdcstats/parser.go reads
// payload.source.schema). Rewriting qualified_name here would stop the two producers
// colliding and split every CDC table into two half-filled rows — captured in one,
// applied in the other. Add, never overwrite.
//
// Object storage gets "" on purpose: there DBOrSchema is a bronze-layout path segment the
// executor fills with the source schema, so returning it would restore the exact
// mislabelling above under a more confident name.
func destinationNamespaceForStats(cfg *WorkerConfig, dbOrSchema string) string {
	if cfg == nil || isObjectStorageConnector(cfg.DestinationConnector) {
		return ""
	}
	ns := strings.TrimSpace(dbOrSchema)
	if !isRealNamespace(ns) {
		return ""
	}
	return ns
}

// addNamespaceParam forwards the authoritative destination namespace to a
// connector tool call. The sink normalizes the table to a BARE name for
// single-namespace destinations (the "<ns>." prefix is stripped); the namespace
// then travels separately here so the connector resolves the correct database
// instead of config["database"]. Both keys are set for connector-side aliasing.
func addNamespaceParam(args map[string]interface{}, ns string) {
	if !isRealNamespace(ns) {
		return
	}
	ns = strings.TrimSpace(ns)
	args["namespace"] = ns
	args["db_or_schema"] = ns
}

// bareTableForNamespace strips a leading "<schema>." / "<db>." qualifier from a
// table identifier so the connector applies the separately-forwarded destination
// namespace. The relational connectors only honor the namespace param when the
// table has NO dot — a qualified name would leak the source schema. Quote-aware:
// `"rsync_public"."t"` -> `t`. Only called by the CDC write paths when a real
// destination namespace is set; the batch path already sends bare tables.
func bareTableForNamespace(t string) string {
	t = strings.TrimSpace(t)
	if idx := strings.LastIndex(t, "."); idx >= 0 && idx < len(t)-1 {
		t = t[idx+1:]
	}
	return strings.Trim(strings.TrimSpace(t), "\"`")
}

// resolveDestTableForWrite is the canonical relational table-resolution rule
// shared by the write paths: normalize the source-derived table for the dest
// type, then strip any schema/db qualifier when a REAL per-pipeline namespace is
// set (the namespace is forwarded separately via addNamespaceParam, and the
// connectors only honor it when the table has NO dot). The CDC paths apply this
// inline (963-970, 3390-3392); the non-CDC batch path (writeToDestination) MUST
// too — otherwise a PG dest receives a qualified table like "rsync_public.orders"
// ALONGSIDE a namespace param, ignores the namespace, and silent-drops into the
// leaked source schema (read>0, landed=0). When no real namespace is set,
// normalizeTargetTable's conservative behavior is preserved so a legitimately
// schema-qualified PG dest table is kept intact.
func resolveDestTableForWrite(destType string, destCfg map[string]interface{}, table, namespace string) string {
	t := normalizeTargetTable(destType, destCfg, table)
	if isRealNamespace(namespace) {
		t = bareTableForNamespace(t)
	}
	return t
}

// ensuredCacheKey builds the process-lived ddl.Ensured cache key. It MUST include
// the destination namespace/schema. With per-source-schema preservation (the
// executor mirrors a multi-schema source by setting each table's namespace to its
// source schema), two source schemas' same-named tables — e.g. sales.orders and
// procurement.orders — both bare-ify to "orders" via resolveDestTableForWrite and
// would otherwise collide on one cache entry: the second table's ensure_table is
// skipped by the "already ensured" fast-path, so its schema is never created and
// every INSERT fails / rows land in the wrong schema (silent cross-schema
// clobber). Keying on (destType, namespace, table) keeps them distinct.
//
// namespace is normalized identically to addNamespaceParam (empty/"default" -> "")
// so the historical single-namespace behavior is unchanged (the key just gains a
// stable, empty namespace segment). This is the SINGLE source of truth for the
// key — the ensure Load/Store path AND the reload-mode invalidation must both use
// it, or a cache-key mismatch reopens the reload data-loss bug (see cbb8a251).
func ensuredCacheKey(destType, namespace, targetTable string) string {
	ns := strings.TrimSpace(namespace)
	if !isRealNamespace(ns) {
		ns = ""
	}
	return canonicalConnectorType(destType) + ":" + ns + ":" + strings.TrimSpace(targetTable)
}

func safeIdentPart(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return false
	}
	return true
}

func safeTruncateSQL(table string) (string, bool) {
	t := strings.TrimSpace(table)
	if t == "" {
		return "", false
	}
	schema := "public"
	name := t
	if strings.Contains(t, ".") {
		parts := strings.SplitN(t, ".", 2)
		if len(parts) == 2 {
			schema = strings.TrimSpace(strings.Trim(parts[0], `"`))
			name = strings.TrimSpace(strings.Trim(parts[1], `"`))
		}
	}
	if !safeIdentPart(schema) || !safeIdentPart(name) {
		return "", false
	}
	// Quote identifiers defensively.
	return fmt.Sprintf(`TRUNCATE TABLE "%s"."%s"`, schema, name), true
}

// writeCDCToDestination writes a CDC event to the destination with the specified operation
func writeCDCToDestination(ctx context.Context, httpClient *http.Client, cfg *WorkerConfig, ddl *DDLSupport, msg kafka.Message, sm *SinkMessage, operation string) (int64, string, error) {
	destCfg := cfg.DestinationConfig
	destType := canonicalConnectorType(cfg.DestinationConnector)
	looksLikeObjectStorage := isObjectStorageConnector(destType)
	isWarehouse := isDataWarehouseConnector(destType)
	// Append-only history mode: every change becomes an INSERT carrying _rsync_cdc_*
	// identity columns. Object storage is already append-only (bronze), so this only
	// changes the relational and warehouse paths below.
	appendMode := cdcAppendMode(cfg)

	// Allow destination override for the target table/collection.
	// This is important for:
	// - E2E tests (write into a single known destination table)
	// - Prod use-cases where destination naming differs from source db.table
	targetTable := normalizeTargetTable(destType, destCfg, sm.Table)

	// Per-pipeline destination namespace (CDC): route relational writes to
	// <namespace>.<bare-table> exactly like the batch path. Bare the table here and
	// forward the namespace separately below; the connector only honors the namespace
	// param when the table has no dot, so a qualified name would leak the source
	// schema. Object stores key by the source table id, so they keep the full name.
	destNamespace := strings.TrimSpace(sm.DBOrSchema)
	if isRealNamespace(destNamespace) && !looksLikeObjectStorage {
		targetTable = bareTableForNamespace(targetTable)
	}

	// Determine the destination tool name (MCP JSON-RPC "tools/call").
	// Many connectors only expose tools via JSON-RPC and do not support legacy direct method calls.
	toolName := ""
	if looksLikeObjectStorage {
		// Object stores don't support row-level upsert/delete semantics; write the CDC event as an object.
		toolName = fmt.Sprintf("%s_import_data", destType)
	} else if appendMode {
		// Append-only history: every c/r/u/d is an INSERT (no merge, no keyed delete).
		// The _rsync_cdc_* columns carry op/identity/ordering for downstream dedup.
		// Warehouses use their batch-load tool; relational sinks use import_data — both
		// perform a plain INSERT (append), matching the full-load path's tool choice.
		if isWarehouse {
			toolName = fmt.Sprintf("%s_load", destType)
		} else {
			toolName = fmt.Sprintf("%s_import_data", destType)
		}
	} else if isWarehouse {
		// Warehouses: apply CDC micro-batches via merge (connector decides best strategy).
		toolName = fmt.Sprintf("%s_merge", destType)
	} else {
		switch operation {
		case "insert":
			toolName = fmt.Sprintf("%s_import_data", destType)
		case "upsert":
			toolName = fmt.Sprintf("%s_upsert_data", destType)
		case "delete":
			toolName = fmt.Sprintf("%s_delete_data", destType)
		default:
			toolName = fmt.Sprintf("%s_import_data", destType)
		}
	}

	params := map[string]interface{}{
		"config": destCfg,
		// `table` is the DESTINATION target; `source_table` is the original Debezium table id.
		"table":        targetTable,
		"source_table": sm.Table,
		"operation":    operation,
	}

	destKey := ""
	keyFieldsForDDL := []string{}

	// Data payload:
	// - DB destinations: apply row-level changes (after/before)
	// - Object storage: write the event envelope as a single-row object/file
	if looksLikeObjectStorage {
		// Respect destination config format, defaulting to jsonl for CDC bronze.
		bucket := firstStr(destCfg, "bucket", "bucket_name")
		container := firstStr(destCfg, "container")
		prefix := firstStr(destCfg, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path")
		if prefix == "" {
			prefix = "test-aws-s3"
		}
		format := firstStr(destCfg, "file_format", "format")
		if format == "" {
			format = "jsonl"
		}
		compression := firstStr(destCfg, "compression")
		if compression == "" {
			compression = "none"
		}

		// Minimal bronze envelope:
		// op/table/pk/after/lsn/source_ts_ms/ingestion_ts_ms (+ optional kafka metadata)
		evt, _, err := buildBronzeCDCEvent(cfg, msg, sm, operation, format)
		if err != nil {
			return 0, "", err
		}
		params["data"] = []map[string]interface{}{evt}

		// Deterministic CDC object key (idempotent retries). Honors partition_by /
		// partition_time_granularity (Group C). DMS-style layout:
		// <prefix>/<db_or_schema>/<table>/<col=val/…><YYYY-MM-DD>/<YYYYMMDD-HHMMSSmmm>-<offset>.<ext>
		partSegs, dateSeg := cdcPartitionContext(destCfg, sm)
		dbOrSchema, tbl := cdcObjectPath(sm)
		destKey = cdcObjectKey(prefix, dbOrSchema, tbl, dateSeg, partSegs, firstNonZero(sm.SourceTS, sm.IngestionTS), msg.Partition, msg.Offset, msg.Offset, format, compression)
		params["key"] = destKey
		if canonicalConnectorType(destType) == "azure-blob" {
			if container != "" {
				params["container"] = container
			} else if bucket != "" {
				// Back-compat: allow bucket field to serve as container.
				params["container"] = bucket
			}
		} else if bucket != "" {
			params["bucket"] = bucket
		}
		params["format"] = format
		params["file_format"] = format
		params["compression"] = compression
	} else if appendMode {
		// Append-only history (relational + warehouse): emit the changed row plus the
		// _rsync_cdc_* identity columns as a plain INSERT. No merge keys and no soft-delete
		// flag — every c/r/u/d is preserved as a distinct row; downstream selects the latest
		// per business key via MAX(_rsync_cdc_seq). Deletes append the Before image (full row
		// when present, else just the PK) tagged op=D.
		var rows []map[string]interface{}
		if operation == "delete" {
			var r map[string]interface{}
			if sm.Before != nil {
				r = sm.Before
			} else if sm.PK != nil {
				r = sm.PK
			}
			if r != nil {
				rows = []map[string]interface{}{withCDCIdentity(r, sm)}
			}
		} else if len(sm.Data) > 0 {
			rows = make([]map[string]interface{}, 0, len(sm.Data))
			for _, r := range sm.Data {
				rows = append(rows, withCDCIdentity(r, sm))
			}
		} else if sm.After != nil {
			rows = []map[string]interface{}{withCDCIdentity(sm.After, sm)}
		}
		if len(rows) == 0 {
			// Poison, not fatal: a change event carrying no row payload (no After,
			// no Data, no Before/PK for a delete) can never be written. DLQ + advance.
			return 0, "", poisonError{err: fmt.Errorf("append-only CDC missing row payload for op=%q table=%q", sm.CDCOp, sm.Table)}
		}
		params["data"] = rows
		// No key_fields: import_data / <warehouse>_load perform a plain INSERT. The append
		// table intentionally carries no unique constraint on the business key (it stores
		// every version). cdc_metadata kept for audit/logging parity with the upsert path.
		params["cdc_metadata"] = map[string]interface{}{
			"op":          bronzeOpFromDebezium(sm.CDCOp),
			"debezium_op": sm.CDCOp,
			"tx_id":       sm.TxID,
			"lsn":         sm.LSN,
			"source_ts":   sm.SourceTS,
		}
	} else if isWarehouse {
		// Build a single-row CDC micro-batch for merge.
		var row map[string]interface{}
		if operation == "delete" {
			row = sm.Before
		} else {
			row = sm.After
		}
		if row == nil && len(sm.Data) > 0 {
			row = sm.Data[0]
		}
		if row == nil {
			return 0, "", fmt.Errorf("missing CDC row payload for operation=%s", operation)
		}

		// Soft delete convention for warehouse current tables.
		// BigQuery merge uses `_rsync_deleted`; Redshift/Snowflake may treat it as a regular column.
		if operation == "delete" {
			row = copyMap(row)
			row["_rsync_deleted"] = true
		}

		// Best-effort PK inference (prefer explicit destination config; else fallback to "id" if present).
		pks := toStringSlice(destCfg["primary_keys"])
		if len(pks) == 0 {
			pks = toStringSlice(destCfg["key_fields"])
		}
		if len(pks) == 0 {
			pks = toStringSlice(destCfg["primary_key_fields"])
		}
		if len(pks) == 0 {
			if _, ok := row["id"]; ok {
				pks = []string{"id"}
			}
		}
		if len(pks) == 0 {
			return 0, "", fmt.Errorf("missing primary_keys for warehouse merge (set destination_config.primary_keys)")
		}
		keyFieldsForDDL = append([]string{}, pks...)

		params["data"] = []map[string]interface{}{row}
		params["primary_keys"] = pks
		params["key_fields"] = pks

		// Add CDC metadata for audit/logging
		params["cdc_metadata"] = map[string]interface{}{
			"op":          bronzeOpFromDebezium(sm.CDCOp),
			"debezium_op": sm.CDCOp,
			"tx_id":       sm.TxID,
			"lsn":         sm.LSN,
			"source_ts":   sm.SourceTS,
		}
	} else {
		// For inserts/upserts, use After; for deletes, use Before (as key)
		if operation == "delete" {
			// Deterministic deletes must use PK from the Kafka record key.
			if sm != nil && sm.PK != nil && len(sm.PK) > 0 {
				params["key_data"] = sm.PK
				params["data"] = []map[string]interface{}{sm.PK}
			} else if sm.Before != nil {
				// Fallback for non-strict paths (should be avoided for correctness).
				params["key_data"] = sm.Before
				params["data"] = []map[string]interface{}{sm.Before}
			} else {
				// Poison, not fatal: no key payload and no before image → the delete
				// cannot be targeted at any dest row. DLQ + advance, don't crash-loop.
				return 0, "", poisonError{err: fmt.Errorf("CDC delete missing PK (no key payload and no before image)")}
			}
		} else if len(sm.Data) > 0 {
			params["data"] = sm.Data
		}

		// Provide deterministic key_fields for DB upserts/deletes so destinations don't default to "id".
		// Without this, Postgres upserts may pick columns[0] (JSON key order), which can cause:
		//   "there is no unique or exclusion constraint matching the ON CONFLICT specification"
		// when the table has a unique index on the actual PK (e.g. order_id).
		explicit := []string{}
		for _, k := range []string{"key_fields", "primary_keys", "primary_key_fields", "pk_fields", "primary_key"} {
			if destCfg != nil {
				if v, ok := destCfg[k]; ok {
					explicit = toStringSlice(v)
					if len(explicit) > 0 {
						break
					}
				}
			}
		}
		// Best signal: Debezium Kafka message key contains the primary key fields.
		if len(explicit) == 0 && sm != nil && len(sm.KeyFields) > 0 {
			explicit = append([]string{}, sm.KeyFields...)
		}
		if len(explicit) == 0 {
			var row map[string]interface{}
			if operation == "delete" && sm.Before != nil {
				row = sm.Before
			} else if len(sm.Data) > 0 {
				row = sm.Data[0]
			} else if sm.After != nil {
				row = sm.After
			}
			explicit = inferKeyFieldsForRow(row)
		}
		if len(explicit) > 0 {
			params["key_fields"] = explicit
			params["primary_key_fields"] = explicit
			keyFieldsForDDL = append([]string{}, explicit...)
		}

		// Add CDC metadata for audit/logging
		params["cdc_metadata"] = map[string]interface{}{
			// DMS-style operation codes: I/U/D (snapshot reads "r" map to I)
			"op":          bronzeOpFromDebezium(sm.CDCOp),
			"debezium_op": sm.CDCOp,
			"tx_id":       sm.TxID,
			"lsn":         sm.LSN,
			"source_ts":   sm.SourceTS,
		}
	}

	// Best-effort: auto-create / reconcile destination table (DB destinations only)
	// before applying CDC events. ensureDestinationTable performs net-additive
	// reconciliation (CREATE TABLE IF NOT EXISTS + ADD COLUMN IF NOT EXISTS) against
	// the live destination schema — that IS the schema-evolution gate. Additive changes
	// are applied; destructive changes (DROP/RENAME/incompatible TYPE) are surfaced by
	// the connector as errors and escalate (no offset commit). No external schema cache.
	//
	// Document DB (MongoDB): skip ensure_table entirely — collections auto-create on
	// first write and _id is always present, so there is nothing to reconcile.
	if isDocumentDBConnector(destType) {
		// The per-pipeline namespace has no Mongo schema analog (the database comes from
		// the connection config), so it is not forwarded; the collection is the bare table.
	} else if !looksLikeObjectStorage && !isWarehouse {
		rowsForDDL := []map[string]interface{}{}
		if d, ok := params["data"].([]map[string]interface{}); ok && len(d) > 0 {
			rowsForDDL = d
		} else if operation == "delete" && sm.Before != nil {
			rowsForDDL = []map[string]interface{}{sm.Before}
		} else if len(sm.Data) > 0 {
			rowsForDDL = sm.Data
		}
		colTypesForDDL := sm.ColumnTypes
		if appendMode {
			// Type the injected _rsync_cdc_* columns explicitly (BIGINT/VARCHAR);
			// otherwise ensure_table defaults them to TEXT.
			colTypesForDDL = mergeCDCIdentityColumnTypes(sm.ColumnTypes)
		}
		// CDC apply path: colTypesForDDL are resolved destination DDL types
		// (deriveColumnTypesForDestination + mergeCDCIdentityColumnTypes).
		// Forward the per-pipeline destination namespace so CREATE SCHEMA/TABLE land
		// in <namespace>.<table> (mirrors the batch path). destNamespace is "" when the
		// pipeline has no real namespace, which keeps ensure and write in agreement
		// (both resolve config["database"]).
		// Fail-closed drop gate only for full-image upserts (c/r/u). Delete images can
		// be PK-only (non-FULL replica identity) and append-mode tombstones are
		// intentional, so both are excluded to avoid false halts. A returned
		// fatalError propagates up to the consumer loop's isFatal os.Exit.
		failOnMissing := operation != "delete" && !appendMode
		if err := ensureDestinationTable(ctx, httpClient, cfg, ddl, destType, targetTable, destNamespace, rowsForDDL, keyFieldsForDDL, colTypesForDDL, sm.PipelineID, true, failOnMissing, true); err != nil {
			return 0, "", err
		}
		// Forward the destination namespace on the relational write call too (the
		// table is bare). Gate matches ensureDestinationTable: skip object storage
		// (keyed paths) and warehouses (namespace via their own db/schema config).
		// No-op when unset.
		addNamespaceParam(params, destNamespace)
	} else if isWarehouse {
		// Warehouse CDC merge (databricks/snowflake): the target table is already
		// bare (bareTableForNamespace above), so forward the per-pipeline destination
		// namespace exactly like the relational path — the connector's
		// merge()/_config_with_namespace then routes the MERGE into <namespace>.<table>
		// instead of leaking into the connection's default schema (the same silent
		// mis-route the batch write path fixes). ensureDestinationTable is intentionally
		// skipped here — a warehouse self-provisions the target + soft-delete sentinels
		// inside merge(). No-op when the pipeline has no real namespace.
		addNamespaceParam(params, destNamespace)
	}

	// Tier-A/B durable offset: hand the connector the Kafka position so it can persist
	// it into _rsync_cdc_offsets. For relational (Tier-A) sinks this write happens in the
	// SAME transaction as the row change → exactly-once. Warehouses (Tier-B) persist it
	// best-effort after merge. Object stores (Tier-C) use deterministic keys and ignore it.
	if !looksLikeObjectStorage {
		params["kafka_offset"] = map[string]interface{}{
			"pipeline_id": cfg.PipelineID,
			"topic":       msg.Topic,
			"partition":   msg.Partition,
			"offset":      msg.Offset,
		}
	}

	// MCP JSON-RPC tool invocation
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name":      toolName,
			"arguments": params,
		},
	}
	body, _ := json.Marshal(reqBody)

	hosts := destinationHostCandidates(destType, cfg.DestinationVersion)
	var lastErr error
	for _, host := range hosts {
		u := fmt.Sprintf("http://%s:8000/mcp", host)
		req, _ := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var out map[string]interface{}
		if err := json.Unmarshal(raw, &out); err != nil {
			lastErr = fmt.Errorf("dest response not json: %w", err)
			continue
		}

		// Destination MCPs may respond in either shape:
		// - Direct result: {"success": true, ...}
		// - JSON-RPC: {"jsonrpc":"2.0","id":...,"result":{"success": true, ...}}
		res := out
		if nested, ok := out["result"].(map[string]interface{}); ok && nested != nil {
			res = nested
		}

		success, _ := res["success"].(bool)
		if !success {
			// Some connectors may not implement upsert/delete; fall back to import_data when possible.
			// (Skip for object storage since we already use import_data there.)
			if !looksLikeObjectStorage && operation != "insert" {
				errMsg := strings.ToLower(fmt.Sprint(res["error"]))
				if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "method not found") {
					params["upsert"] = (operation == "upsert")
					params["is_delete"] = (operation == "delete")
					reqBody["params"].(map[string]interface{})["name"] = fmt.Sprintf("%s_import_data", destType)
					body, _ = json.Marshal(reqBody)
					req, _ = http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					resp, err = httpClient.Do(req)
					if err != nil {
						lastErr = err
						continue
					}
					raw, _ = io.ReadAll(resp.Body)
					resp.Body.Close()
					if err := json.Unmarshal(raw, &out); err != nil {
						lastErr = fmt.Errorf("dest fallback response not json: %w", err)
						continue
					}
					res = out
					if nested, ok := out["result"].(map[string]interface{}); ok && nested != nil {
						res = nested
					}
					success, _ = res["success"].(bool)
				}
			}
			if !success {
				lastErr = fmt.Errorf("dest error: %v", res["error"])
				continue
			}
		}

		// Extract rows (best-effort). Uses the same canonical write-count
		// field set as the strict batch path so streaming consumers see the
		// same accept criteria — adding a new field name in one place
		// covers both paths.
		// Called ONCE and reused below — a connector whose result map is consumed
		// on read would otherwise be double-counted.
		n, haveCount := extractDestRowCount(res)
		if !haveCount {
			// Default: treat as 1 logical event applied/written
			n = 1
		}

		// KI-CDC-DELETE-PATH-UNLOGGED: a delete used to apply and log NOTHING.
		//
		// The mechanism: this function performs its own MCP HTTP round-trip (the
		// loop above) instead of going through callDestinationTool, so it inherited
		// none of that function's "destination tool call ok" logging. That is why
		// the live logs showed upsert_data lines and no delete_data line at all,
		// while the applied_deletes counter still moved.
		//
		// Gated on operation == "delete" deliberately: in append-only mode EVERY
		// c/r/u/d flows through here, so an unconditional info line would be one
		// log per event. rows_deleted is passed as an int64 (not a string) so it
		// survives even if the key is later dropped from logSafeFields.
		// logMsgEvent injects trace_id/table/topic/partition/offset, which is what
		// makes the line correlatable back to the Debezium record in Kafka.
		if operation == "delete" {
			logMsgEvent("info", sm, msg, "cdc delete applied",
				"tool", toolName,
				"host", host,
				"dest_table", targetTable,
				"key_fields", strings.Join(keyFieldsForDDL, ","),
				"pk_fingerprint", pkFingerprint(sm.PK),
				"rows_deleted", n,
				"count_reported", haveCount,
				"op", "D",
				"debezium_op", sm.CDCOp,
				"lsn", sm.LSN,
				"tx_id", sm.TxID)
		}

		return n, destKey, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("destination call failed")
	}
	return 0, destKey, lastErr
}

// blobDestKey joins the destination prefix with the source-relative object key so
// the destination mirrors the source layout (prefix="archive",
// objectKey="docs/report.pdf" → "archive/docs/report.pdf"). Empty prefix → the
// object key verbatim. (universal-blob-passthrough plan §3.)
func blobDestKey(prefix, objectKey string) string {
	p := strings.Trim(strings.TrimSpace(prefix), "/")
	k := strings.TrimLeft(strings.TrimSpace(objectKey), "/")
	if p == "" {
		return k
	}
	return p + "/" + k
}

// writeBlobToDestination copies one staged blob to an object-storage destination —
// universal-blob-passthrough plan §3. It hands the destination connector a
// claim-check pointer (data_ref) plus the staging creds and lets the connector
// fetch the bytes itself and write them raw (raw:true), so the bytes never transit
// the sink. content_type is carried verbatim; sha256 lets the connector verify
// integrity after the fetch. Fails loud (never silent) on a non-object-storage
// destination or any connector error.
func writeBlobToDestination(ctx context.Context, httpClient *http.Client, cfg *WorkerConfig, sm *SinkMessage) (string, error) {
	destType := canonicalConnectorType(cfg.DestinationConnector)
	if !isObjectStorageConnector(destType) {
		// Defense in depth: the capability gate rejects blob→non-object-storage up
		// front; this guards a misconfigured sink so a blob is never dropped onto a
		// structured destination silently.
		return "", fmt.Errorf("blob passthrough unsupported for destination %q (object storage only)", destType)
	}
	if strings.TrimSpace(sm.ObjectKey) == "" {
		return "", fmt.Errorf("blob message missing object_key")
	}
	if strings.TrimSpace(sm.ClaimCheckURL) == "" {
		return "", fmt.Errorf("blob message missing claim_check_url (data_ref)")
	}

	destCfg := cfg.DestinationConfig
	bucket := firstStr(destCfg, "bucket", "bucket_name")
	container := firstStr(destCfg, "container")
	prefix := firstStr(destCfg, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path")
	destKey := blobDestKey(prefix, sm.ObjectKey)

	args := map[string]interface{}{
		"config":         destCfg,
		"blob":           true,
		"raw":            true,
		"data_ref":       sm.ClaimCheckURL,
		"staging_config": sm.StagingConfig,
		"key":            destKey,
		"content_type":   sm.ContentType,
		"sha256":         sm.Sha256,
		"size":           sm.Size,
	}
	if bucket != "" {
		args["bucket"] = bucket
	}
	if container != "" {
		args["container"] = container
	}

	reqBody := map[string]interface{}{
		"jsonrpc": "2.0", "id": 1, "method": "tools/call",
		"params": map[string]interface{}{
			"name":      fmt.Sprintf("%s_import_data", destType),
			"arguments": args,
		},
	}
	body, _ := json.Marshal(reqBody)

	hosts := destinationHostCandidates(destType, cfg.DestinationVersion)
	var lastErr error
	for _, host := range hosts {
		u := fmt.Sprintf("http://%s:8000/mcp", host)
		req, _ := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		var out map[string]interface{}
		if err := json.Unmarshal(raw, &out); err != nil {
			lastErr = fmt.Errorf("blob dest response not json: %w", err)
			continue
		}
		// Connectors answer either {"success":...} or JSON-RPC {"result":{...}}.
		res := out
		if nested, ok := out["result"].(map[string]interface{}); ok && nested != nil {
			res = nested
		}
		if success, _ := res["success"].(bool); !success {
			lastErr = fmt.Errorf("blob dest error: %v", res["error"])
			continue
		}
		return destKey, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("blob destination call failed")
	}
	return "", lastErr
}

// incrementCounter atomically increments a counter in a sync.Map
func incrementCounter(m *sync.Map, key string, delta int64) {
	for {
		val, _ := m.LoadOrStore(key, int64(0))
		old := val.(int64)
		if m.CompareAndSwap(key, old, old+delta) {
			break
		}
	}
}

// loadCounter loads a counter value from sync.Map
func loadCounter(m *sync.Map, key string) int64 {
	if val, ok := m.Load(key); ok {
		return val.(int64)
	}
	return 0
}

// emitCDCTableStats emits TABLE_STATS for CDC mode (streaming). dlqRows is the
// running count of this table's records parked in the DLQ — rows the destination
// will never receive. It is carried on every emission (not only when non-zero) so
// the projector can clear a stale value, and it drives status=degraded: a table
// still streaming while shedding rows is not "running", and reporting it as
// running is what let a discarded row pass for a healthy one.
func emitCDCTableStats(ctx context.Context, w *kafka.Writer, sm *SinkMessage, inserts, updates, deletes, bytesCommitted, dlqRows int64) error {
	b, _ := json.Marshal(buildCDCTableStatsEvent(sm, inserts, updates, deletes, bytesCommitted, dlqRows))

	return w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(sm.PipelineID),
		Value: b,
		Headers: []kafka.Header{
			{Key: "trace_id", Value: []byte(sm.TraceID)},
		},
	})
}

// tableIdentityForStats builds the metadata.table object both TABLE_STATS emitters
// send, and returns the raw table name for the human-readable message line.
//
// schema / name / qualified_name describe the table as this sink received it, which
// for CDC is the SOURCE-side name — and that has to stay true. qualified_name is the
// key the projector upserts on, and the orchestrator's cdcstats agent independently
// writes the captured-side counters into the same row under the same source-derived
// name; rewriting it here would split every CDC table in two. See
// destinationNamespaceForStats for the full argument.
//
// destination_schema / destination_qualified_name answer what the source-side name
// cannot — where the rows actually landed. They are OMITTED rather than blanked when
// the sink cannot name a destination namespace, so "this sink does not know" stays
// distinguishable from "the namespace is empty", and an older sink that never emits
// them reads the same way.
func tableIdentityForStats(sm *SinkMessage) (map[string]interface{}, string) {
	tableName := sm.Table
	schemaName := ""
	shortName := tableName
	parts := strings.Split(tableName, ".")
	for len(parts) >= 2 && parts[0] != "" && parts[0] == parts[1] {
		parts = append(parts[:1], parts[2:]...)
	}
	qualifiedName := strings.Join(parts, ".")
	if len(parts) >= 2 {
		schemaName = parts[0]
		shortName = parts[len(parts)-1]
	} else {
		shortName = qualifiedName
	}

	table := map[string]interface{}{
		"schema":         schemaName,
		"name":           shortName,
		"qualified_name": qualifiedName,
	}
	if ns := strings.TrimSpace(sm.DestNamespace); ns != "" {
		table["destination_schema"] = ns
		table["destination_qualified_name"] = ns + "." + shortName
	}
	return table, tableName
}

// buildCDCTableStatsEvent builds the TABLE_STATS payload. Split out from the write so
// the counts/status contract the api-gateway projector reads is unit-testable without
// a broker.
func buildCDCTableStatsEvent(sm *SinkMessage, inserts, updates, deletes, bytesCommitted, dlqRows int64) map[string]interface{} {
	table, tableName := tableIdentityForStats(sm)

	totalEvents := inserts + updates + deletes

	// CDC is "running" until stopped — except while it is dropping rows on the floor.
	tableStatus := "running"
	if dlqRows > 0 {
		tableStatus = "degraded"
	}

	meta := map[string]interface{}{
		"source": "kafka_mcp_sink",
		"mode":   "cdc",
		"status": tableStatus,
		"table":  table,
		"counts": map[string]interface{}{
			"inserts":      inserts,
			"updates":      updates,
			"deletes":      deletes,
			"total_events": totalEvents,
			// Rows the destination will never receive. Deliberately NOT folded into
			// read_rows/inserted_rows: those are "what landed", and adding a lost row
			// to either would restore the very reconciliation that hid the loss.
			"dlq_rows": dlqRows,
			// For compatibility with batch stats
			"read_rows":       totalEvents,
			"inserted_rows":   inserts + updates, // rows added/modified
			"bytes_committed": bytesCommitted,
		},
		"cdc_position": map[string]interface{}{
			"tx_id":     sm.TxID,
			"lsn":       sm.LSN,
			"source_ts": sm.SourceTS,
		},
	}
	// The id the sink's own logs carry. Added ONLY when it says something execution_id
	// does not: on this lane execution_id has been rewritten to pipeline_id, so without
	// it the stats row and the logs share no id at all. OMITTED rather than blanked when
	// the two agree or the sink was never told one — so "nothing to add" and "a sink
	// older than this change" read identically, and neither can overwrite a stored value.
	if oid := strings.TrimSpace(sm.OrchestrationExecutionID); oid != "" && oid != sm.ExecutionID {
		meta["orchestration_execution_id"] = oid
	}

	event := map[string]interface{}{
		"schema_version": 2,
		"event_type":     "TABLE_STATS",
		"pipeline_id":    sm.PipelineID,
		"execution_id":   sm.ExecutionID,
		"trace_id":       sm.TraceID,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"stage":          "executor",
		"stage_group":    "executing",
		"status":         "processing",
		"message":        fmt.Sprintf("CDC statistics update: %s", tableName),
		"metadata":       meta,
	}
	return event
}

func loadConfig() (*WorkerConfig, error) {
	raw := strings.TrimSpace(os.Getenv("CONFIG"))
	if raw == "" {
		return nil, fmt.Errorf("missing CONFIG env")
	}
	var cfg WorkerConfig
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return nil, fmt.Errorf("invalid CONFIG json: %w", err)
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		// Back-compat: allow multi-topic workers that specify config.topics only.
		if len(cfg.Topics) == 0 {
			return nil, fmt.Errorf("missing config.topic")
		}
		for _, t := range cfg.Topics {
			if strings.TrimSpace(t) != "" {
				cfg.Topic = strings.TrimSpace(t)
				break
			}
		}
		if strings.TrimSpace(cfg.Topic) == "" {
			return nil, fmt.Errorf("missing config.topic")
		}
	}
	if cfg.ConsumerGroup == "" {
		return nil, fmt.Errorf("missing config.consumer_group")
	}
	if cfg.KafkaBootstrapServers == "" {
		cfg.KafkaBootstrapServers = "kafka:29092"
	}
	if cfg.DestinationConnector == "" {
		return nil, fmt.Errorf("missing config.destination_connector")
	}
	if cfg.DestinationConfig == nil {
		cfg.DestinationConfig = map[string]interface{}{}
	}
	if cfg.MetricsPort == 0 {
		cfg.MetricsPort = pickFreePort()
	}
	return &cfg, nil
}

func pickFreePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// isFlatDebeziumEnvelope reports whether payload is a schemas-disabled ("flat")
// Debezium change event — the message itself IS the Debezium payload, so there is
// no payload["payload"] wrapper to look inside.
//
// The discriminator is deliberately STRICTER than the schema-enabled branch's
// key-presence check, because "auto" mode also runs on BATCH topics (the
// hybrid-CDC batch-backfill sink is started with sink_mode="auto" on
// pipeline.<id>.data) and a Kafka topic is a trust boundary the worker doesn't
// own: `source` must be the Debezium source STRUCT (a JSON object) and `op` must
// be a Debezium operation code. No batch producer emits that pair at the top
// level (the executor's sendChunkedToKafka sends pipeline_id/table/data and keeps
// row values nested under "data"), so a batch envelope can never be mis-routed to
// parseCDCMessage. before/after/ts_ms are intentionally NOT required: truncate
// ("t") and logical-message ("m") events carry none of them.
func isFlatDebeziumEnvelope(payload map[string]interface{}) bool {
	if payload == nil {
		return false
	}
	if _, ok := payload["source"].(map[string]interface{}); !ok {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(toString(payload["op"]))) {
	case "c", "u", "d", "r", "t", "m":
		return true
	}
	return false
}

func parseSinkMessage(cfg *WorkerConfig, msg kafka.Message) (*SinkMessage, error) {
	// Debezium (and some other producers) can emit tombstone messages with a null/empty value.
	// Treat those as ignorable (commit offset and move on) rather than DLQ/failure.
	raw := bytes.TrimSpace(msg.Value)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		traceID := ""
		if cfg != nil && strings.TrimSpace(cfg.PipelineID) != "" {
			traceID = strings.TrimSpace(cfg.PipelineID)
		}
		return &SinkMessage{
			PipelineID:  strings.TrimSpace(cfg.PipelineID),
			ExecutionID: strings.TrimSpace(cfg.ExecutionID),
			TraceID:     traceID,
			Ignore:      true,
			BatchOffset: msg.Offset,
		}, nil
	}

	// Decode with UseNumber so integers survive as json.Number rather than being
	// forced through float64. A BIGINT near ±2^63 (e.g. 9223372036854775807) has no
	// exact float64 representation — encoding/json's default for a JSON number into
	// interface{} — so it rounds (to 9223372036854775808), overflows on INSERT
	// ("bigint out of range") and the row is silently DLQ'd. normalizeJSONNumbers
	// then re-materializes each json.Number as int64 (exact integers) or float64,
	// restoring the concrete Go types every downstream consumer already expects
	// while preserving full int64 precision. Source-agnostic (CDC + batch).
	var payload map[string]interface{}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&payload); err != nil {
		return nil, fmt.Errorf("invalid json payload: %w", err)
	}
	normalizeJSONNumbers(payload)

	// Debezium (Kafka Connect JsonConverter with schemas enabled) message shape:
	//   {"schema": {...}, "payload": {"op":"c|u|d|r","before":{...},"after":{...},"source":{...},"ts_ms":...}}
	//
	// Pre-live decision: CDC mode expects schema-enabled envelopes.
	var connectSchema map[string]interface{}
	var innerPayload map[string]interface{}
	if pv, ok := payload["payload"].(map[string]interface{}); ok && pv != nil {
		innerPayload = pv
	}
	if sv, ok := payload["schema"].(map[string]interface{}); ok && sv != nil {
		connectSchema = sv
	}
	sinkMode := strings.TrimSpace(cfg.SinkMode)
	if sinkMode == "" {
		sinkMode = "auto"
	}

	// A blob (raw-bytes passthrough) message is NEVER a Debezium CDC envelope — it
	// has no op/source. Detect it up front and keep isCDC false REGARDLESS of
	// sink_mode, so storage_type=="blob" (or is_blob) can never be mis-routed to
	// parseCDCMessage and DLQ'd; the blob branch below is its only correct handler.
	// (universal-blob-passthrough plan §3.)
	isBlobPayload := strings.TrimSpace(toString(payload["storage_type"])) == "blob"
	if !isBlobPayload {
		if b, ok := payload["is_blob"].(bool); ok && b {
			isBlobPayload = true
		}
	}

	isCDC := false
	if isBlobPayload {
		isCDC = false
	} else if sinkMode == "cdc" {
		isCDC = true
	} else if sinkMode == "auto" {
		// Auto-detect CDC by presence of Debezium payload fields (schema-enabled envelope).
		if innerPayload != nil {
			_, hasOp := innerPayload["op"]
			_, hasSource := innerPayload["source"]
			if hasOp && hasSource {
				isCDC = true
			}
		} else if isFlatDebeziumEnvelope(payload) {
			// KI-SINK-CDC-AUTODETECT: schemas-disabled ("flat") Debezium — the message
			// itself IS the payload, so innerPayload is nil and the checks above can never
			// fire. Without this the event fell through to batch parsing, which requires
			// payload["table"] (a Debezium envelope carries the table at source.table),
			// so every change event was DLQ'd "missing table" and the run landed 0 rows.
			// The isCDC block below already handles this shape once isCDC is true.
			isCDC = true
		}
	}

	if isCDC {
		// Accept BOTH CDC shapes:
		// 1) Kafka Connect JsonConverter with schemas enabled:
		//    {"schema": {...}, "payload": {...}}
		// 2) Schema-less Debezium JSON (schemas disabled), where the message itself is the Debezium payload:
		//    {"op":"c|u|d|r","before":...,"after":...,"source":...,"ts_ms":...}
		if innerPayload == nil {
			_, hasOp := payload["op"]
			_, hasSource := payload["source"]
			if hasOp && hasSource {
				return parseCDCMessage(cfg, msg, payload, nil)
			}
			return nil, fmt.Errorf("CDC message missing required payload fields (op/source)")
		}
		// Schema is optional: without it we still apply CDC (type coercion will be best-effort).
		return parseCDCMessage(cfg, msg, innerPayload, connectSchema)
	}

	// Standard batch message parsing
	storageType := strings.TrimSpace(toString(payload["storage_type"]))
	if storageType == "" {
		storageType = strings.TrimSpace(toString(headerValue(msg.Headers, "storage_type")))
	}
	if storageType == "" {
		storageType = "inline"
	}

	pipelineID := strings.TrimSpace(toString(payload["pipeline_id"]))
	if pipelineID == "" {
		pipelineID = strings.TrimSpace(toString(headerValue(msg.Headers, "pipeline_id")))
	}
	if pipelineID == "" {
		pipelineID = strings.TrimSpace(cfg.PipelineID)
	}

	executionID := strings.TrimSpace(toString(payload["execution_id"]))
	if executionID == "" {
		executionID = strings.TrimSpace(toString(headerValue(msg.Headers, "execution_id")))
	}
	if executionID == "" {
		executionID = strings.TrimSpace(cfg.ExecutionID)
	}

	traceID := strings.TrimSpace(toString(payload["trace_id"]))
	if traceID == "" {
		traceID = strings.TrimSpace(toString(headerValue(msg.Headers, "trace_id")))
	}
	if traceID == "" {
		traceID = pipelineID
	}

	if storageType == "bootstrap" {
		return &SinkMessage{
			PipelineID:  pipelineID,
			ExecutionID: executionID,
			StorageType: storageType,
			TraceID:     traceID,
			Ignore:      true,
		}, nil
	}

	table := strings.TrimSpace(toString(payload["table"]))
	if table == "" {
		table = strings.TrimSpace(toString(headerValue(msg.Headers, "table")))
	}
	if table == "" {
		return nil, fmt.Errorf("missing table")
	}

	sm := &SinkMessage{
		PipelineID:  pipelineID,
		ExecutionID: executionID,
		Table:       table,
		StorageType: storageType,
		TraceID:     traceID,
	}

	// Blob (raw-bytes passthrough) message — universal-blob-passthrough plan §3.
	// Discriminated by storage_type=="blob" (or is_blob). It carries a pointer to
	// bytes staged byte-identical in the claim-check store, NOT a row payload, so
	// it returns here BEFORE the PK/column-type machinery (none of which applies to
	// opaque bytes). Fail loud (never silent-drop) if the pointer fields are
	// missing or the data_ref is unsafe.
	isBlobMsg := storageType == "blob"
	if !isBlobMsg {
		if b, ok := payload["is_blob"].(bool); ok && b {
			isBlobMsg = true
		}
	}
	if isBlobMsg {
		sm.IsBlob = true
		sm.StorageType = "blob"
		sm.ObjectKey = strings.TrimSpace(toString(payload["object_key"]))
		sm.ContentType = strings.TrimSpace(toString(payload["content_type"]))
		sm.ClaimCheckURL = strings.TrimSpace(toString(payload["claim_check_url"]))
		sm.Sha256 = strings.TrimSpace(toString(payload["sha256"]))
		// payload["size"] arrives as json.Number (UseNumber, above); toInt64 handles
		// json.Number/float64/string uniformly and returns 0 when absent.
		sm.Size = toInt64(payload["size"])
		if sc, ok := payload["staging_config"].(map[string]interface{}); ok {
			sm.StagingConfig = sc
		}
		if sm.ObjectKey == "" {
			return nil, fmt.Errorf("blob message missing object_key")
		}
		if sm.ClaimCheckURL == "" {
			return nil, fmt.Errorf("blob message missing claim_check_url")
		}
		if vErr := validateClaimCheckURL(sm.ClaimCheckURL); vErr != nil {
			return nil, fmt.Errorf("invalid claim_check_url: %w", vErr)
		}
		return sm, nil
	}

	// Optional: explicit primary key fields provided by the producer (executor).
	// When present, use these to:
	// - create the correct unique index on destination (avoid heuristics like "first *_id")
	// - apply idempotent upserts in resume mode
	keys := []string{}
	for _, k := range []string{"primary_keys", "key_fields", "primary_key_fields", "pk_fields", "primary_key"} {
		if v, ok := payload[k]; ok {
			keys = toStringSlice(v)
			if len(keys) > 0 {
				break
			}
		}
	}
	if len(keys) == 0 {
		for _, k := range []string{"primary_keys", "key_fields", "primary_key_fields", "pk_fields", "primary_key"} {
			if hv := strings.TrimSpace(toString(headerValue(msg.Headers, k))); hv != "" {
				// allow comma-separated headers
				parts := strings.Split(hv, ",")
				for _, p := range parts {
					pp := strings.TrimSpace(p)
					if pp != "" {
						keys = append(keys, pp)
					}
				}
				if len(keys) > 0 {
					break
				}
			}
		}
	}
	if len(keys) > 0 {
		uniq := map[string]struct{}{}
		out := make([]string, 0, len(keys))
		for _, k := range keys {
			kk := strings.TrimSpace(k)
			if kk == "" {
				continue
			}
			if _, ok := uniq[kk]; ok {
				continue
			}
			uniq[kk] = struct{}{}
			out = append(out, kk)
		}
		if len(out) > 0 {
			sort.Strings(out)
			sm.KeyFields = out
		}
	}

	// Optional metadata for deterministic object storage layouts
	sm.Dataset = strings.TrimSpace(toString(payload["dataset"]))
	if sm.Dataset == "" {
		sm.Dataset = strings.TrimSpace(toString(headerValue(msg.Headers, "dataset")))
	}
	sm.DBOrSchema = strings.TrimSpace(toString(payload["db_or_schema"]))
	if sm.DBOrSchema == "" {
		sm.DBOrSchema = strings.TrimSpace(toString(headerValue(msg.Headers, "db_or_schema")))
	}
	sm.DestNamespace = destinationNamespaceForStats(cfg, sm.DBOrSchema)
	sm.Dt = strings.TrimSpace(toString(payload["dt"]))
	if sm.Dt == "" {
		sm.Dt = strings.TrimSpace(toString(headerValue(msg.Headers, "dt")))
	}
	sm.RunMode = strings.TrimSpace(toString(payload["run_mode"]))
	if sm.RunMode == "" {
		sm.RunMode = strings.TrimSpace(toString(headerValue(msg.Headers, "run_mode")))
	}

	if eof, ok := payload["eof"].(bool); ok && eof {
		sm.EOF = true
		sm.TotalReadRows = toInt64(payload["total_read_rows"])
		return sm, nil
	}
	if storageType == "eof" {
		sm.EOF = true
		sm.TotalReadRows = toInt64(payload["total_read_rows"])
		return sm, nil
	}

	sm.BatchOffset = toInt64(payload["batch_offset"])
	if sm.BatchOffset == 0 {
		// allow offsets coming from headers
		if v := strings.TrimSpace(toString(headerValue(msg.Headers, "batch_offset"))); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				sm.BatchOffset = n
			}
		}
	}
	sm.RowCount = toInt64(payload["row_count"])

	if storageType == "minio" {
		sm.ClaimCheckURL = strings.TrimSpace(toString(payload["claim_check_url"]))
		if sm.ClaimCheckURL == "" {
			return nil, fmt.Errorf("minio message missing claim_check_url")
		}
		// Security: claim_check_url arrives over Kafka (a trust boundary) and is
		// forwarded verbatim into minio-mcp read AND delete calls. Validate scheme +
		// bucket against the staging allow-list so a crafted pointer can't drive the
		// sink to read or delete arbitrary buckets reachable by the minio credentials.
		// A rejected message fails closed (DLQ + commit) via the parseSinkMessage caller.
		if vErr := validateClaimCheckURL(sm.ClaimCheckURL); vErr != nil {
			return nil, fmt.Errorf("invalid claim_check_url: %w", vErr)
		}
		// Read column_types for minio claim-check messages so ensureDestinationTable
		// gets typed columns even when rows are fetched later from object storage.
		if ct, ok := payload["column_types"].(map[string]interface{}); ok && len(ct) > 0 {
			sm.ColumnTypes = make(map[string]string, len(ct))
			for col, typ := range ct {
				if s, ok := typ.(string); ok && strings.TrimSpace(s) != "" {
					sm.ColumnTypes[col] = strings.TrimSpace(s)
				}
			}
		}
		return sm, nil
	}

	if data, ok := payload["data"].([]interface{}); ok && len(data) > 0 {
		sm.Data = make([]map[string]interface{}, 0, len(data))
		for _, it := range data {
			if m, ok := it.(map[string]interface{}); ok {
				sm.Data = append(sm.Data, m)
			}
		}
	}

	// Read column_types sent by the executor so ensureDestinationTable can create
	// typed columns (integer, varchar, timestamp) instead of falling back to TEXT.
	if ct, ok := payload["column_types"].(map[string]interface{}); ok && len(ct) > 0 {
		sm.ColumnTypes = make(map[string]string, len(ct))
		for col, typ := range ct {
			if s, ok := typ.(string); ok && strings.TrimSpace(s) != "" {
				sm.ColumnTypes[col] = strings.TrimSpace(s)
			}
		}
	}

	return sm, nil
}

// parseCDCMessage parses a Debezium CDC event envelope.
// With Kafka Connect JsonConverter schemas enabled, the outer envelope is:
//
//	{"schema": {...}, "payload": {...}}
//
// and parseSinkMessage passes the inner payload + the connect schema here.
// debeziumUnavailablePlaceholder is the string Debezium substitutes
// into UPDATE row images for TOAST-able columns whose value didn't
// change in the update — when the source table has REPLICA IDENTITY
// DEFAULT (i.e. only PK columns in `before`). If we write this
// sentinel verbatim to the destination, we OVERWRITE the destination's
// good TOAST value with the 25-byte string. T2-1 source-side fix is
// to set REPLICA IDENTITY FULL on every selected table at provisioning
// time (cdc/postgresql.go); this sink-side filter is the
// belt-and-suspenders backstop for tables the source-side ALTER
// couldn't run on (extension-owned, RLS-locked, etc.).
const debeziumUnavailablePlaceholder = "__debezium_unavailable_value"

// filterDebeziumUnavailable drops keys whose value equals the
// Debezium unchanged-TOAST sentinel. Removing the key (not setting
// it to NULL) makes the destination's upsert path SKIP that column
// in its SET clause — the existing destination value is preserved.
//
// The destination Postgres MCP at
// shared/mcp-connectors/public/postgresql/versions/v1.0.14/connector.py:2890+
// builds the UPDATE SET clause from data[0].keys(), so a missing key
// is the correct way to say "leave this column alone".
func filterDebeziumUnavailable(row map[string]interface{}) map[string]interface{} {
	if row == nil {
		return nil
	}
	out := make(map[string]interface{}, len(row))
	for k, v := range row {
		if s, ok := v.(string); ok && s == debeziumUnavailablePlaceholder {
			// Drop the key entirely — sink upsert preserves the
			// destination value when the key isn't present.
			continue
		}
		out[k] = v
	}
	return out
}

func parseCDCMessage(cfg *WorkerConfig, msg kafka.Message, payload map[string]interface{}, connectSchema map[string]interface{}) (*SinkMessage, error) {
	pipelineID := strings.TrimSpace(cfg.PipelineID)
	executionID := strings.TrimSpace(cfg.ExecutionID)
	// CDC stats are keyed by execution_id == pipeline_id so the UI can always query a stable
	// streaming counter bucket. (Batch executions still use a distinct execution_id.)
	// Keep the orchestration id before the CDC convention overwrites it. This is the
	// only place it is still in hand, and it is the id every sink log line carries.
	orchestrationExecutionID := executionID
	if strings.EqualFold(strings.TrimSpace(cfg.SinkMode), "cdc") || executionID == "" {
		executionID = pipelineID
	}
	traceID := pipelineID
	if traceID == "" {
		traceID = fmt.Sprintf("cdc-%d", msg.Offset)
	}

	// Extract table from topic name or source metadata
	// Debezium topics are typically: <prefix>.<db>.<table> or <prefix>.<schema>.<table>
	table := extractTableFromTopicOrSource(msg.Topic, payload)
	if table == "" {
		return nil, fmt.Errorf("cannot determine table from CDC event")
	}

	op := strings.TrimSpace(toString(payload["op"]))
	if op == "" {
		return nil, fmt.Errorf("CDC event missing 'op' field")
	}

	sm := &SinkMessage{
		PipelineID:               pipelineID,
		ExecutionID:              executionID,
		OrchestrationExecutionID: orchestrationExecutionID,
		Table:                    table,
		StorageType:              "cdc",
		TraceID:                  traceID,
		IsCDC:                    true,
		CDCOp:                    op,
	}
	// Snapshot reads use op == "r" (Debezium); still prefer payload.source.snapshot when present.
	if strings.EqualFold(strings.TrimSpace(op), "r") {
		sm.IsSnapshot = true
	}

	// Worker ingestion timestamp (ms); also used as SourceTS fallback.
	sm.IngestionTS = time.Now().UTC().UnixMilli()

	// Derive key fields from Kafka message key (Debezium emits PK as JSON object).
	// Example key: {"event_id":1}
	if len(msg.Key) > 0 {
		var keyEnvelope map[string]interface{}
		if err := json.Unmarshal(msg.Key, &keyEnvelope); err == nil && len(keyEnvelope) > 0 {
			// When Kafka Connect JsonConverter schemas are enabled, the key is wrapped:
			//   {"schema": {...}, "payload": {...}}
			// Unwrap to get actual PK fields.
			keyObj := keyEnvelope
			if pv, ok := keyEnvelope["payload"]; ok {
				if m, ok := pv.(map[string]interface{}); ok && m != nil {
					keyObj = m
				}
			}
			if len(keyObj) == 0 {
				// Tombstone key payload can be null/empty; skip.
				goto keyDone
			}
			// Store the full PK object for bronze + delete identification.
			sm.PK = keyObj
			keys := make([]string, 0, len(keyObj))
			for k := range keyObj {
				kk := strings.TrimSpace(k)
				if kk != "" {
					keys = append(keys, kk)
				}
			}
			if len(keys) > 0 {
				sort.Strings(keys)
				sm.KeyFields = keys
			}
		}
	}
keyDone:

	// Source-family dispatch. MongoDB is a document store: Debezium emits
	// before/after as JSON *strings* (not nested structs) and the destination
	// mapping is "packed" (_id + one JSON document column), so it takes a wholly
	// separate decode path from the relational engines (PostgreSQL/MySQL/SQL
	// Server/Oracle share the flat-struct path below).
	if sourceFamily(payload) == "mongodb" {
		// Fully populates sm.Data/After/Before/PK/KeyFields/ColumnTypes and fails
		// loud on a parse error (never a silent RowCount=0), so the relational
		// extraction + DDL derivation below is skipped for Mongo.
		if err := decodeMongoDocument(cfg, sm, payload, op); err != nil {
			return nil, err
		}
	} else {
		// Extract before/after depending on operation
		if before, ok := payload["before"].(map[string]interface{}); ok {
			sm.Before = filterDebeziumUnavailable(before)
		}
		if after, ok := payload["after"].(map[string]interface{}); ok {
			sm.After = filterDebeziumUnavailable(after)
		}

		// For creates and snapshot reads, row data is in "after"
		// For deletes, row data is in "before"
		// For updates, we have both
		switch op {
		case "c", "r": // create, read (snapshot)
			if sm.After != nil {
				sm.Data = []map[string]interface{}{sm.After}
				sm.RowCount = 1
			}
		case "u": // update
			if sm.After != nil {
				sm.Data = []map[string]interface{}{sm.After}
				sm.RowCount = 1
			}
		case "d": // delete
			if sm.Before != nil {
				sm.Data = []map[string]interface{}{sm.Before}
				sm.RowCount = 1
			}
		}

		// Best-effort: derive destination DDL types + coerce values using Kafka Connect schema metadata.
		// This is required for type fidelity in relational sinks:
		// - DDL types: NUMERIC/TIMESTAMP/JSONB instead of TEXT
		// - Values: decode Decimal bytes (base64) and convert Debezium time logical types
		rowField := "after"
		rowData := sm.After
		if strings.EqualFold(strings.TrimSpace(op), "d") {
			rowField = "before"
			rowData = sm.Before
		}
		rowSchema := findConnectSchemaField(connectSchema, rowField)
		rowFields := connectStructFields(rowSchema)
		sm.ColumnTypes = deriveColumnTypesForDestination(cfg, connectSchema, op)
		// These are FINAL destination DDL types (mapConnectFieldToDDLType), so the
		// destination connector must pass them through verbatim rather than re-map.
		sm.ColumnTypesAreDDL = true
		coerceCDCRowValues(cfg, rowData, rowFields)
	}

	// Extract source metadata for idempotency
	if source, ok := payload["source"].(map[string]interface{}); ok {
		// Snapshot signal (best-effort)
		if isDebeziumSnapshot(source) {
			sm.IsSnapshot = true
		}
		// Transaction ID varies by DB type
		if txID := toString(source["txId"]); txID != "" {
			sm.TxID = txID
		} else if gtid := toString(source["gtid"]); gtid != "" {
			sm.TxID = gtid // MySQL GTID
		} else if xmin := toString(source["xmin"]); xmin != "" {
			sm.TxID = xmin // Postgres xmin
		}

		// LSN/position for ordering. SQL Server does NOT emit a numeric lsn/pos —
		// it emits hex-formatted commit_lsn/change_lsn (binary(10) as
		// "0000002a:00000948:0003"), so without the SQL Server branch below
		// sm.LSN would be 0 for every SQL Server change event and the ordering /
		// idempotency key would be lost. Prefer commit_lsn (commit order).
		if lsn := toInt64(source["lsn"]); lsn > 0 {
			sm.LSN = lsn
		} else if pos := toInt64(source["pos"]); pos > 0 {
			sm.LSN = pos // MySQL binlog position
		} else if clsn := toString(source["commit_lsn"]); clsn != "" {
			sm.LSN = parseSQLServerLSN(clsn) // SQL Server hex commit LSN
			if sm.LSN == 0 {
				logf("warning", "warn: SQL Server commit_lsn %q parsed to LSN 0 (unparseable or all-zeros) — ordering/idempotency key degraded", clsn)
			}
		} else if chlsn := toString(source["change_lsn"]); chlsn != "" {
			sm.LSN = parseSQLServerLSN(chlsn) // SQL Server hex change LSN
			if sm.LSN == 0 {
				logf("warning", "warn: SQL Server change_lsn %q parsed to LSN 0 (unparseable or all-zeros) — ordering/idempotency key degraded", chlsn)
			}
		}

		// Override table from source if available (more reliable)
		if srcTable := toString(source["table"]); srcTable != "" {
			if srcSchema := toString(source["schema"]); srcSchema != "" {
				sm.Table = srcSchema + "." + srcTable
			} else if srcDB := toString(source["db"]); srcDB != "" {
				sm.Table = srcDB + "." + srcTable
			}
		}

		// Prefer source.ts_ms when available (Debezium source metadata).
		if ts := toInt64(source["ts_ms"]); ts > 0 {
			sm.SourceTS = ts
		}
	}

	// Source timestamp fallback:
	// 1) payload.source.ts_ms (set above)
	// 2) payload.ts_ms
	// 3) ingestion timestamp (always set)
	if sm.SourceTS <= 0 {
		if tsMs := toInt64(payload["ts_ms"]); tsMs > 0 {
			sm.SourceTS = tsMs
		}
	}
	if sm.SourceTS <= 0 {
		sm.SourceTS = sm.IngestionTS
	}

	// Use Kafka offset + partition as batch offset for idempotency
	sm.BatchOffset = msg.Offset

	// Destination namespace for CDC. Debezium controls the message payload, so the
	// user-selected namespace can't ride per-message the way batch's db_or_schema
	// header does; the orchestrator injects it into the sink's start config instead
	// (WorkerConfig.DestinationNamespace). Carry it onto every CDC SinkMessage so the
	// relational write paths route to <namespace>.<bare-table> (mirrors batch). Empty
	// => unchanged: addNamespaceParam no-ops and the connector falls back to config.
	sm.DBOrSchema = strings.TrimSpace(cfg.DestinationNamespace)
	sm.DestNamespace = destinationNamespaceForStats(cfg, sm.DBOrSchema)

	return sm, nil
}

func coerceCDCRowValues(cfg *WorkerConfig, row map[string]interface{}, fieldSchemas []map[string]interface{}) {
	if cfg == nil || row == nil || len(fieldSchemas) == 0 {
		return
	}
	destType := canonicalConnectorType(cfg.DestinationConnector)
	if destType == "" {
		return
	}

	// Defensive: keep only fields declared in the Connect schema for the row struct.
	// This prevents leaking outer envelope keys (e.g. "schema"/"payload") into destination rows/DDL.
	expected := map[string]struct{}{}
	for _, fs := range fieldSchemas {
		if fs == nil {
			continue
		}
		col := strings.TrimSpace(toString(fs["field"]))
		if col != "" {
			expected[col] = struct{}{}
		}
	}
	for k := range row {
		if _, ok := expected[k]; !ok {
			delete(row, k)
		}
	}

	for _, fs := range fieldSchemas {
		if fs == nil {
			continue
		}
		col := strings.TrimSpace(toString(fs["field"]))
		if col == "" {
			continue
		}
		val, ok := row[col]
		if !ok || val == nil {
			continue
		}
		t := strings.ToLower(strings.TrimSpace(toString(fs["type"])))
		name := strings.ToLower(strings.TrimSpace(toString(fs["name"])))

		// Debezium io.debezium.data.VariableScaleDecimal is a STRUCT carrying the
		// per-value scale + unscaled two's-complement magnitude:
		// {"scale":N,"value":"<base64>"}. Debezium emits it for an unscaled Oracle
		// NUMBER (the typical integer PK) and an unbounded Postgres NUMERIC. Unlike
		// the fixed-scale Decimal below (t=="bytes", scale in the schema params) the
		// scale is PER VALUE, so it must be read from the value struct itself — the
		// schema params carry none (see mapConnectFieldToDDLType.precision(), which
		// returns 0,false for VariableScaleDecimal). Without this decode the raw JSON
		// object reaches the relational sink verbatim -> "invalid input syntax for
		// type numeric: {\"scale\": 0, \"value\": \"Aw==\"}" -> 3 retries -> .dlq ->
		// offset committed -> pipeline reports Streaming/lag-0/healthy while dropping
		// 100% of rows (silent total data loss). Decode to the same numeric literal
		// string the fixed-scale branch produces, which the relational sink accepts.
		if t == "struct" && strings.Contains(name, "variablescaledecimal") {
			if vsd, ok := val.(map[string]interface{}); ok {
				if b64, ok := vsd["value"].(string); ok && strings.TrimSpace(b64) != "" {
					sc := int(toInt64(vsd["scale"]))
					if dec, err := decodeDecimalBase64(strings.TrimSpace(b64), sc); err == nil {
						row[col] = dec
					}
				}
			}
			continue
		}

		// Decimal (Kafka Connect) is encoded as base64 string when using JsonConverter.
		if t == "bytes" && strings.Contains(name, "decimal") {
			sc := 0
			if p, ok := fs["parameters"].(map[string]interface{}); ok && p != nil {
				if v, ok := p["scale"]; ok {
					if n, err := strconv.Atoi(strings.TrimSpace(toString(v))); err == nil {
						sc = n
					}
				}
			}
			if s, ok := val.(string); ok && strings.TrimSpace(s) != "" {
				if dec, err := decodeDecimalBase64(strings.TrimSpace(s), sc); err == nil {
					row[col] = dec
				}
			}
			continue
		}

		// Debezium time logical types.
		if strings.Contains(name, "io.debezium.time.") {
			// Zoned (timezone-aware) temporals — io.debezium.time.ZonedTime and
			// ZonedTimestamp — are encoded as ISO-8601 STRINGS (e.g. "07:23:00Z",
			// "2026-06-09T12:34:56.000000Z"), not integers. Their name contains the
			// substring ".time"/"timestamp", so without this guard they fall into
			// the integer branches below, where toInt64("07:23:00Z")==0 silently
			// rewrote ZonedTime to "00:00:00" (data loss).
			//
			// PostgreSQL accepts ISO-8601 verbatim, but MySQL DATETIME/TIME reject
			// the 'T' separator and trailing 'Z'/offset ("Incorrect datetime value:
			// '2026-06-09T12:34:56.000000Z'" → whole batch DLQ'd → ZERO rows for any
			// PG→MySQL table with a timestamptz/timetz column). Reformat per dest.
			if strings.Contains(name, "zoned") {
				if s, ok := val.(string); ok && strings.TrimSpace(s) != "" {
					s = strings.TrimSpace(s)
					if strings.Contains(name, "zonedtimestamp") {
						row[col] = coerceZonedTimestampForDest(destType, s)
					} else {
						row[col] = coerceZonedTimeForDest(destType, s)
					}
				}
				continue
			}
			// Timestamp: int64 ms/µs/ns since epoch. Epoch 0 (n==0 →
			// 1970-01-01 00:00:00.000) and any pre-1970 instant (n<0) are VALID —
			// the field's presence (non-null) was already checked at the top of the
			// loop, so convert unconditionally. The old `if n > 0` guard left epoch-0
			// and earlier as a RAW int64, which INSERTed into a `timestamp` column →
			// "is of type timestamp without time zone but expression is of type
			// integer" → whole batch DLQ'd → ZERO rows land.
			if strings.Contains(name, "timestamp") {
				n := toInt64(val)
				var tm time.Time
				switch {
				case strings.Contains(name, "microtimestamp"):
					tm = time.Unix(0, n*1000).UTC()
				case strings.Contains(name, "nanotimestamp"):
					tm = time.Unix(0, n).UTC()
				default:
					// io.debezium.time.Timestamp is milliseconds
					tm = time.UnixMilli(n).UTC()
				}
				row[col] = formatTimestampForDest(destType, tm)
				continue
			}
			// Date: int32 days since epoch.
			if strings.HasSuffix(name, ".date") {
				days := toInt64(val)
				tm := time.Unix(0, 0).UTC().Add(time.Duration(days) * 24 * time.Hour)
				row[col] = tm.Format("2006-01-02")
				continue
			}
			// Time since midnight. The Debezium logical type determines the
			// unit — they are NOT interchangeable:
			//   io.debezium.time.Time      -> int32 MILLISECONDS  (time.precision.mode=connect)
			//   io.debezium.time.MicroTime -> int64 MICROSECONDS  (adaptive_time_microseconds; MySQL default)
			//   io.debezium.time.NanoTime  -> int64 NANOSECONDS
			// Previously this always divided by 1000 (assumed ms), so a MySQL TIME
			// of 12:34:56 (MicroTime = 45_296_000_000 µs) became "12582:13:20",
			// which Postgres rejects as out of range -> the whole batch was DLQ'd
			// and ZERO rows replicated for any table containing a TIME column.
			// Match ONLY the time-of-day logical types by suffix. A substring
			// match on ".time" wrongly catches io.debezium.time.Year (MySQL YEAR),
			// io.debezium.time.Interval, io.debezium.time.MicroDuration, etc. —
			// e.g. YEAR=2026 was read as 2026 ms and rewritten to "00:00:02.026000",
			// then INSERTed into the destination's BIGINT year column → type error →
			// whole batch DLQ'd → ZERO rows for any table with a YEAR column.
			if strings.HasSuffix(name, ".time") || strings.HasSuffix(name, ".microtime") || strings.HasSuffix(name, ".nanotime") {
				v := toInt64(val)
				// Midnight (v==0) is valid and already handled. A negative TIME is
				// legal in MySQL (range -838:59:59..838:59:59); the old `if v < 0
				// { continue }` left it as a RAW int64 → type error on INSERT →
				// batch DLQ'd. Emit a signed literal instead. Non-negative values
				// are byte-identical to before (sign == "").
				sign := ""
				if v < 0 {
					sign = "-"
					v = -v
				}
				var sec, micros int64
				switch {
				case strings.Contains(name, "microtime"):
					sec = v / 1_000_000
					micros = v % 1_000_000
				case strings.Contains(name, "nanotime"):
					sec = v / 1_000_000_000
					micros = (v % 1_000_000_000) / 1000
				default: // io.debezium.time.Time -> milliseconds
					sec = v / 1000
					micros = (v % 1000) * 1000
				}
				h := sec / 3600
				m := (sec % 3600) / 60
				s := sec % 60
				if micros > 0 {
					row[col] = fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, h, m, s, micros)
				} else {
					row[col] = fmt.Sprintf("%s%02d:%02d:%02d", sign, h, m, s)
				}
				continue
			}
		}
	}
}

func formatTimestampForDest(destType string, tm time.Time) string {
	switch canonicalConnectorType(destType) {
	case "mysql":
		return tm.Format("2006-01-02 15:04:05.000000")
	default:
		// Postgres accepts RFC3339 timestamps; it will coerce to timestamp without tz when needed.
		return tm.Format(time.RFC3339Nano)
	}
}

// coerceZonedTimestampForDest converts a Debezium ZonedTimestamp ISO-8601 string
// (e.g. "2026-06-09T12:34:56.000000Z") into the destination's datetime literal.
// PostgreSQL accepts ISO-8601 verbatim; MySQL DATETIME rejects the 'T' separator
// and trailing 'Z'/offset, so we parse and reformat via formatTimestampForDest.
// On parse failure we fall back to a textual fix so the row is never silently
// dropped (better a best-effort value than a whole DLQ'd batch).
func coerceZonedTimestampForDest(destType, s string) string {
	if canonicalConnectorType(destType) != "mysql" {
		return s
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if tm, err := time.Parse(layout, s); err == nil {
			return formatTimestampForDest(destType, tm.UTC())
		}
	}
	out := strings.Replace(s, "T", " ", 1)
	return strings.TrimSuffix(out, "Z")
}

// coerceZonedTimeForDest converts a Debezium ZonedTime ISO-8601 time-of-day
// string (e.g. "07:23:00Z", "07:23:00.123456+00:00") into the destination's TIME
// literal. PostgreSQL accepts it verbatim; MySQL TIME wants HH:MM:SS[.ffffff]
// with no timezone designator.
func coerceZonedTimeForDest(destType, s string) string {
	if canonicalConnectorType(destType) != "mysql" {
		return s
	}
	s = strings.TrimSuffix(s, "Z")
	// Strip a trailing numeric offset like "+00:00" / "-05:30".
	if n := len(s); n >= 6 {
		tail := s[n-6:]
		if (tail[0] == '+' || tail[0] == '-') && tail[3] == ':' {
			s = s[:n-6]
		}
	}
	return s
}

func decodeDecimalBase64(b64 string, scale int) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
	if err != nil {
		return "", err
	}
	if len(raw) == 0 {
		return "0", nil
	}
	i := new(big.Int).SetBytes(raw)
	// Interpret as signed two's complement.
	if raw[0]&0x80 != 0 {
		mod := new(big.Int).Lsh(big.NewInt(1), uint(len(raw))*8)
		i.Sub(i, mod)
	}
	s := i.Text(10)
	if scale <= 0 {
		return s, nil
	}
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = strings.TrimPrefix(s, "-")
	}
	if len(s) <= scale {
		s = strings.Repeat("0", scale-len(s)+1) + s
	}
	intPart := s[:len(s)-scale]
	frac := s[len(s)-scale:]
	if intPart == "" {
		intPart = "0"
	}
	out := intPart + "." + frac
	if neg {
		out = "-" + out
	}
	return out, nil
}

// schemaLessCDCWarnOnce guards a single startup-style warning when CDC messages
// arrive without a Connect schema envelope (see deriveColumnTypesForDestination).
var schemaLessCDCWarnOnce sync.Once

func deriveColumnTypesForDestination(cfg *WorkerConfig, connectSchema map[string]interface{}, op string) map[string]string {
	if cfg == nil {
		return nil
	}
	if connectSchema == nil {
		// Schema-less CDC stream (value.converter.schemas.enable=false): no per-field
		// type metadata, so relational sinks fall back to creating every new column as
		// TEXT/VARCHAR — data is preserved but type fidelity and index/range performance
		// are lost. The planner (cdc_config_merger) enables schemas by default; warn once
		// so an accidental schema-less config is visible instead of silently degrading.
		if dt := canonicalConnectorType(cfg.DestinationConnector); dt != "" &&
			!isObjectStorageConnector(dt) && !isDataWarehouseConnector(dt) {
			schemaLessCDCWarnOnce.Do(func() {
				logf("warning", "warn: CDC messages have no Connect schema envelope "+
					"(value.converter.schemas.enable=false?); relational destination %q will create "+
					"new columns as TEXT — type fidelity lost. Enable schemas in the Debezium config.", dt)
			})
		}
		return nil
	}
	destType := canonicalConnectorType(cfg.DestinationConnector)
	if destType == "" {
		return nil
	}
	// Only apply for relational DB destinations (warehouses/object storage have their own DDL semantics).
	if isObjectStorageConnector(destType) || isDataWarehouseConnector(destType) {
		return nil
	}
	rowField := "after"
	if strings.EqualFold(strings.TrimSpace(op), "d") {
		rowField = "before"
	}
	rowSchema := findConnectSchemaField(connectSchema, rowField)
	if rowSchema == nil {
		return nil
	}
	fields := connectStructFields(rowSchema)
	if len(fields) == 0 {
		return nil
	}
	out := make(map[string]string, len(fields))
	for _, f := range fields {
		col := strings.TrimSpace(toString(f["field"]))
		if col == "" {
			continue
		}
		out[col] = mapConnectFieldToDDLType(destType, f)
	}
	return out
}

func findConnectSchemaField(schema map[string]interface{}, field string) map[string]interface{} {
	if schema == nil {
		return nil
	}
	raw, ok := schema["fields"].([]interface{})
	if !ok || len(raw) == 0 {
		return nil
	}
	for _, it := range raw {
		m, ok := it.(map[string]interface{})
		if !ok || m == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(toString(m["field"])), field) {
			return m
		}
	}
	return nil
}

func connectStructFields(schema map[string]interface{}) []map[string]interface{} {
	if schema == nil {
		return nil
	}
	raw, ok := schema["fields"].([]interface{})
	if !ok || len(raw) == 0 {
		return nil
	}
	out := make([]map[string]interface{}, 0, len(raw))
	for _, it := range raw {
		if m, ok := it.(map[string]interface{}); ok && m != nil {
			out = append(out, m)
		}
	}
	return out
}

func mapConnectFieldToDDLType(destType string, schema map[string]interface{}) string {
	// destType is already canonicalized by caller.
	t := strings.ToLower(strings.TrimSpace(toString(schema["type"])))
	name := strings.ToLower(strings.TrimSpace(toString(schema["name"])))

	// Helper for (precision,scale) parsing.
	scale := func() (int, bool) {
		if p, ok := schema["parameters"].(map[string]interface{}); ok && p != nil {
			if v, ok := p["scale"]; ok {
				s, err := strconv.Atoi(strings.TrimSpace(toString(v)))
				if err == nil {
					return s, true
				}
			}
		}
		return 0, false
	}
	// Debezium carries the source column's real precision in the schema params
	// (connect.decimal.precision). Honour it so a DECIMAL(65,30) source isn't
	// silently capped at precision 38 (which rejects high-magnitude values →
	// INSERT error → row condemned). Returns 0,false for VariableScaleDecimal.
	precision := func() (int, bool) {
		if p, ok := schema["parameters"].(map[string]interface{}); ok && p != nil {
			for _, k := range []string{"connect.decimal.precision", "precision"} {
				if v, ok := p[k]; ok {
					if pr, err := strconv.Atoi(strings.TrimSpace(toString(v))); err == nil && pr > 0 {
						return pr, true
					}
				}
			}
		}
		return 0, false
	}

	isDecimal := (t == "bytes" && strings.Contains(name, "decimal")) || strings.Contains(name, "variablescaledecimal")
	isJSON := strings.Contains(name, "json")
	isUUID := strings.Contains(name, "uuid")

	// Debezium time logical types
	isZonedTS := strings.Contains(name, "zonedtimestamp")
	isMicroTS := strings.Contains(name, "microtimestamp")
	isNanoTS := strings.Contains(name, "nanotimestamp")
	isTS := strings.HasSuffix(name, "timestamp") || strings.Contains(name, ".timestamp")
	isDate := strings.HasSuffix(name, ".date")
	isMicroTime := strings.Contains(name, "microtime") && !strings.Contains(name, "microtimestamp")
	isNanoTime := strings.Contains(name, "nanotime") && !strings.Contains(name, "nanotimestamp")
	isTime := strings.HasSuffix(name, ".time") || isMicroTime || isNanoTime

	switch destType {
	case "postgresql":
		switch {
		case isUUID:
			return "UUID"
		case isJSON:
			return "JSONB"
		case isDecimal:
			sc, hasSc := scale()
			if pr, ok := precision(); ok {
				if pr > 1000 { // PG numeric max precision
					pr = 1000
				}
				if sc > pr {
					sc = pr
				}
				return fmt.Sprintf("NUMERIC(%d,%d)", pr, sc)
			}
			if hasSc {
				return fmt.Sprintf("NUMERIC(38,%d)", sc)
			}
			return "NUMERIC"
		case isDate:
			return "DATE"
		case isZonedTS:
			return "TIMESTAMPTZ"
		case isMicroTS || isNanoTS || isTS:
			return "TIMESTAMP"
		case isTime:
			return "TIME"
		}
		switch t {
		case "int8", "int16":
			return "SMALLINT"
		case "int32":
			return "INTEGER"
		case "int64":
			return "BIGINT"
		case "float32", "float":
			// Kafka Connect's JsonConverter serialises Schema.Type.FLOAT32 as the
			// JSON string "float" (and FLOAT64 as "double"), NOT "float32"/"float64".
			// Matching only the Go enum names sent every MySQL FLOAT/DOUBLE to the
			// default TEXT branch — type fidelity silently lost.
			return "REAL"
		case "float64", "double":
			return "DOUBLE PRECISION"
		case "boolean":
			return "BOOLEAN"
		case "bytes":
			// JsonConverter encodes bytes as base64 strings. Unless we explicitly decode to bytea,
			// storing as TEXT is safer. (Decimal is handled above.)
			return "TEXT"
		case "string":
			return "TEXT"
		case "map", "array", "struct":
			return "JSONB"
		default:
			return "TEXT"
		}
	case "mysql":
		switch {
		case isUUID:
			return "CHAR(36)"
		case isJSON:
			return "JSON"
		case isDecimal:
			sc, hasSc := scale()
			if pr, ok := precision(); ok {
				if pr > 65 { // MySQL DECIMAL max precision
					pr = 65
				}
				if sc > pr {
					sc = pr
				}
				return fmt.Sprintf("DECIMAL(%d,%d)", pr, sc)
			}
			if hasSc {
				return fmt.Sprintf("DECIMAL(38,%d)", sc)
			}
			return "DECIMAL(38,10)"
		case isDate:
			return "DATE"
		case isZonedTS || isMicroTS || isNanoTS || isTS:
			// MySQL has no true timestamptz; store as DATETIME and preserve source ts in metadata.
			return "DATETIME(6)"
		case isTime:
			return "TIME"
		}
		switch t {
		case "int8":
			return "TINYINT"
		case "int16":
			return "SMALLINT"
		case "int32":
			return "INT"
		case "int64":
			return "BIGINT"
		case "float32", "float": // Connect JSON serialises FLOAT32 as "float"
			return "FLOAT"
		case "float64", "double": // …and FLOAT64 as "double"
			return "DOUBLE"
		case "boolean":
			return "TINYINT(1)"
		case "bytes":
			// JsonConverter encodes bytes as a base64 STRING. Mapping to BLOB
			// would store the base64 ASCII as the blob's literal bytes (silent
			// corruption — wrong bytes). Match the Postgres side and keep base64
			// in TEXT: lossy on type, but the value round-trips intact and
			// MySQL→PG→MySQL stays consistent. True BLOB/BYTEA binary fidelity
			// (decode base64 → raw bytes) is a tracked future enhancement.
			return "TEXT"
		case "string":
			return "TEXT"
		case "map", "array", "struct":
			return "JSON"
		default:
			return "TEXT"
		}
	default:
		return ""
	}
}

// extractTableFromTopicOrSource extracts table name from Debezium topic or source field
func extractTableFromTopicOrSource(topic string, payload map[string]interface{}) string {
	// First try source metadata
	if source, ok := payload["source"].(map[string]interface{}); ok {
		tableName := toString(source["table"])
		if tableName != "" {
			schema := toString(source["schema"])
			db := toString(source["db"])
			if schema != "" {
				return schema + "." + tableName
			}
			if db != "" {
				return db + "." + tableName
			}
			return tableName
		}
	}

	// Fallback: parse from topic name (format: prefix.db.table or prefix.schema.table)
	parts := strings.Split(topic, ".")
	if len(parts) >= 3 {
		// Last part is table, second-to-last is schema/db
		return parts[len(parts)-2] + "." + parts[len(parts)-1]
	}
	if len(parts) >= 2 {
		return parts[len(parts)-1]
	}
	return topic
}

func headerValue(headers []kafka.Header, key string) []byte {
	for _, h := range headers {
		if strings.EqualFold(h.Key, key) {
			return h.Value
		}
	}
	return nil
}

func toString(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	case []byte:
		return string(t)
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
}

// parseSQLServerLSN converts a SQL Server LSN string ("0000002a:00000948:0003",
// a hex-formatted binary(10)) into a monotonic int64 ordering key. SQL Server
// does not emit a numeric lsn/pos, so without this the sink's ordering /
// idempotency LSN would be 0 for every SQL Server change event.
//
// The full LSN is 80 bits (VLF sequence : log-block offset : slot). We keep the
// most-significant 60 bits (VLF sequence + block offset) — which always fits a
// positive int64 and preserves commit ordering. The dropped low bits are the
// intra-block slot tiebreaker, for which source ts_ms / event_serial_no already
// disambiguate. Returns 0 for an empty/unparseable value.
func parseSQLServerLSN(s string) int64 {
	h := strings.NewReplacer(":", "", " ", "").Replace(strings.TrimSpace(s))
	if h == "" {
		return 0
	}
	if len(h) > 15 {
		h = h[:15] // most-significant 60 bits; always fits a positive int64
	}
	v, err := strconv.ParseUint(h, 16, 64)
	if err != nil {
		return 0
	}
	return int64(v)
}

// normalizeMongoID reduces a MongoDB _id — which Debezium serializes as MongoDB
// Extended JSON (ObjectId -> {"$oid":"..."}, long -> {"$numberLong":"..."}, or a
// bare scalar) — to a stable string suitable for a relational primary key. In the
// Debezium message key the _id arrives as an Extended-JSON *string*, so a string
// value that itself parses as JSON is decoded one further level.
func normalizeMongoID(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return ""
		}
		if strings.HasPrefix(s, "{") {
			var m map[string]interface{}
			if json.Unmarshal([]byte(s), &m) == nil {
				return normalizeMongoIDFromMap(m)
			}
		}
		// A quoted JSON string ("\"abc\"") -> unwrap to the bare value.
		if len(s) >= 2 && strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"") {
			var inner string
			if json.Unmarshal([]byte(s), &inner) == nil {
				return inner
			}
		}
		return s
	case map[string]interface{}:
		return normalizeMongoIDFromMap(t)
	default:
		return toString(v)
	}
}

// normalizeMongoIDFromMap unwraps a MongoDB Extended-JSON _id object to its
// scalar string form; unknown shapes fall back to compact JSON (still lossless).
func normalizeMongoIDFromMap(m map[string]interface{}) string {
	for _, k := range []string{"$oid", "$numberLong", "$numberInt", "$numberDouble", "$numberDecimal", "$uuid", "$symbol"} {
		if val, ok := m[k]; ok {
			return toString(val)
		}
	}
	if b, err := json.Marshal(m); err == nil {
		return string(b)
	}
	return ""
}

// mongoPackedColumnTypes returns the FIXED destination DDL for the MongoDB
// "packed" mapping: the document _id as the primary-key column plus one JSON
// column holding the whole document. Postgres takes a TEXT PK + JSONB; MySQL
// needs a bounded VARCHAR for the PK (TEXT cannot be a key without a prefix
// length) + JSON. Emitted with ColumnTypesAreDDL=true so the destination passes
// them through verbatim (there is no per-field Connect schema for a Mongo doc).
func mongoPackedColumnTypes(destType string) map[string]string {
	switch destType {
	case "mysql":
		return map[string]string{"_id": "VARCHAR(255)", "document": "JSON"}
	default: // postgresql and relational default
		return map[string]string{"_id": "TEXT", "document": "JSONB"}
	}
}

// decodeMongoDocument implements the MongoDB "packed" CDC mapping. Debezium's
// MongoDB connector emits before/after as JSON *strings* (io.debezium.data.Json),
// not nested structs, so the relational map cast in parseCDCMessage returns nil
// and every Mongo change would silently drop (RowCount=0 — the loudest failure
// mode in this bundle). Here we json-unmarshal the document and land it as the
// packed shape:
//
//	_id       TEXT/VARCHAR primary key (document _id, normalized to a string)
//	document  JSONB/JSON   the whole document, lossless (Extended JSON preserved)
//
// It FAILS LOUD (returns an error, so the caller dead-letters the message) rather
// than ever producing a silent zero-row no-op. capture.mode=change_streams_update_full
// guarantees a complete 'after' for create/update; deletes carry only the key
// (no before-image unless pre-images are enabled), so the _id comes from the key.
func decodeMongoDocument(cfg *WorkerConfig, sm *SinkMessage, payload map[string]interface{}, op string) error {
	// _id from the Debezium message key ({"id": "<extended-json>"}) — the single
	// field present on every op including delete/tombstone.
	rawKeyID := ""
	if sm.PK != nil {
		if v, ok := sm.PK["id"]; ok {
			rawKeyID = normalizeMongoID(v)
		} else if v, ok := sm.PK["_id"]; ok {
			rawKeyID = normalizeMongoID(v)
		}
	}

	// Document payload: after for create/read/update, before for delete.
	docField := "after"
	if strings.EqualFold(op, "d") {
		docField = "before"
	}
	var doc map[string]interface{}
	if raw, ok := payload[docField].(string); ok && strings.TrimSpace(raw) != "" {
		if err := json.Unmarshal([]byte(raw), &doc); err != nil {
			return fmt.Errorf("mongodb cdc: failed to parse %s document JSON (op=%s): %w", docField, op, err)
		}
	}

	// Prefer the document's own _id; fall back to the message key.
	idStr := rawKeyID
	if doc != nil {
		if v, ok := doc["_id"]; ok {
			if s := normalizeMongoID(v); s != "" {
				idStr = s
			}
		}
	}
	if idStr == "" {
		return fmt.Errorf("mongodb cdc: could not determine _id (op=%s; no key id and no document _id)", op)
	}

	switch op {
	case "c", "r", "u":
		if doc == nil {
			return fmt.Errorf("mongodb cdc: op=%s carries no 'after' document (require capture.mode=change_streams_update_full)", op)
		}
		row := map[string]interface{}{"_id": idStr, "document": doc}
		sm.After = row
		sm.Data = []map[string]interface{}{row}
		sm.RowCount = 1
	case "d":
		row := map[string]interface{}{"_id": idStr}
		if doc != nil {
			row["document"] = doc
		}
		sm.Before = row
		sm.Data = []map[string]interface{}{row}
		sm.RowCount = 1
	default:
		return fmt.Errorf("mongodb cdc: unsupported op %q", op)
	}

	// Packed key + fixed DDL. Override the generic key extraction, which used the
	// Debezium key field name "id"; the packed destination table keys on "_id".
	sm.PK = map[string]interface{}{"_id": idStr}
	sm.KeyFields = []string{"_id"}
	sm.ColumnTypes = mongoPackedColumnTypes(canonicalConnectorType(cfg.DestinationConnector))
	sm.ColumnTypesAreDDL = true
	return nil
}

// normalizeJSONNumbers walks a value decoded with json.Decoder.UseNumber and
// replaces every json.Number with a concrete Go numeric type: int64 when the
// literal is an exact integer, else float64. This preserves full int64 precision
// for BIGINT values (a plain json.Unmarshal forces every number through float64,
// which rounds values near ±2^63) while restoring the exact type shape every
// downstream consumer already handles — DDL derivation, the transform engine
// (convertValue/toFloat64 accept int64/float64 but not json.Number), and JSON
// re-marshal to the destination connector. A magnitude that fits neither int64
// nor float64 is kept as its string form rather than dropped. Mutates maps and
// slices in place; returns the (possibly replaced) value for scalar callers.
func normalizeJSONNumbers(v interface{}) interface{} {
	switch t := v.(type) {
	case map[string]interface{}:
		for k, sub := range t {
			t[k] = normalizeJSONNumbers(sub)
		}
		return t
	case []interface{}:
		for i, sub := range t {
			t[i] = normalizeJSONNumbers(sub)
		}
		return t
	case json.Number:
		if i, err := t.Int64(); err == nil {
			return i
		}
		if f, err := t.Float64(); err == nil {
			return f
		}
		return t.String()
	default:
		return v
	}
}

func toInt64(v interface{}) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case json.Number:
		n, _ := t.Int64()
		return n
	case string:
		n, _ := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		return n
	default:
		return 0
	}
}

// cfgBool reads a boolean destination-config value, accepting a native bool, the
// strings true/false/1/0/yes/no/on/off (case-insensitive), or a number (non-zero =
// true). Returns def when the key is absent, nil, or unparseable.
func cfgBool(m map[string]interface{}, key string, def bool) bool {
	if m == nil {
		return def
	}
	v, ok := m[key]
	if !ok || v == nil {
		return def
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		switch strings.ToLower(strings.TrimSpace(t)) {
		case "true", "1", "yes", "y", "on":
			return true
		case "false", "0", "no", "n", "off":
			return false
		}
		return def
	case float64:
		return t != 0
	case int:
		return t != 0
	case int64:
		return t != 0
	case json.Number:
		n, _ := t.Int64()
		return n != 0
	}
	return def
}

// isDebeziumSnapshot returns true if Debezium source metadata indicates a snapshot event.
// Debezium may encode source.snapshot as:
// - bool (true/false)
// - string ("true", "last", "incremental", "false")
func isDebeziumSnapshot(source map[string]interface{}) bool {
	if source == nil {
		return false
	}
	v, ok := source["snapshot"]
	if !ok || v == nil {
		return false
	}
	switch t := v.(type) {
	case bool:
		return t
	case string:
		s := strings.ToLower(strings.TrimSpace(t))
		return s == "true" || s == "last" || s == "incremental"
	default:
		// sometimes snapshot can be numeric-ish; treat non-zero as true
		return toInt64(v) != 0
	}
}

func ackKeyFor(pipelineID, executionID, table string, batchOffset int64) string {
	safeTable := strings.ReplaceAll(table, " ", "_")
	safeTable = strings.ReplaceAll(safeTable, "/", "_")
	safeTable = strings.ReplaceAll(safeTable, "\\", "_")
	return fmt.Sprintf("sink_ack:%s:%s:%s:%d", pipelineID, executionID, safeTable, batchOffset)
}

// fetchFromMinIO retrieves a claim-check payload from object storage, retrying
// transient failures with bounded exponential backoff before giving up (the
// caller then dead-letters). The dominant transient is NoSuchKey: the producer
// writes the object to MinIO and only THEN publishes the Kafka pointer
// (orchestrator stageDataToMinIO → produceBatchWithOutbox), so a pointer whose
// object is momentarily unreadable is a not-yet-visible / wrong-replica read,
// not a permanent miss — exactly the read-after-write window that a leaked or
// DNS-round-robined `minio` endpoint widens into a data-losing DLQ. Retrying a
// few times turns that drop into a successful fetch. A genuinely absent object
// just exhausts the budget and dead-letters as before (no change to that path).
func fetchFromMinIO(ctx context.Context, httpClient *http.Client, claimCheckURL string, expectedRows int64) ([]map[string]interface{}, error) {
	maxAttempts := minioFetchMaxAttempts()
	backoff := 200 * time.Millisecond
	const maxBackoff = 3 * time.Second
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		rows, err := fetchFromMinIOOnce(ctx, httpClient, claimCheckURL)
		if err == nil && (expectedRows <= 0 || len(rows) > 0) {
			if attempt > 1 {
				logEvent("info", "claim-check fetch recovered after retry",
					"attempt", strconv.Itoa(attempt),
					"rows_fetched", strconv.Itoa(len(rows)),
					"claim_check_url", claimCheckURL)
			}
			return rows, nil
		}
		if err == nil {
			// Successful read but ZERO rows although the message header promised
			// row_count>0: the MinIO read-after-write / `minio` DNS-alias split-brain
			// failure mode (the object reads back empty instead of NoSuchKey). Treat it
			// as a transient read failure and retry — never return ([], nil) here, or the
			// caller positive-acks and commits, silently dropping the entire batch.
			err = fmt.Errorf("minio read returned 0 rows but header promised row_count=%d (read-after-write/alias split-brain)", expectedRows)
		}
		lastErr = err
		// Deterministic errors (malformed pointer, bad params) can never succeed
		// on retry — fail fast so the message dead-letters immediately.
		if !isRetryableMinioFetchErr(err) {
			return nil, err
		}
		if attempt < maxAttempts {
			sleep := backoff + time.Duration(rand.Intn(250))*time.Millisecond
			logEvent("warn", "claim-check fetch failed; retrying",
				"attempt", strconv.Itoa(attempt),
				"max_attempts", strconv.Itoa(maxAttempts),
				"sleep_ms", strconv.FormatInt(sleep.Milliseconds(), 10),
				"claim_check_url", claimCheckURL,
				"error", err.Error())
			if !ctxAwareSleep(ctx, sleep) {
				return nil, fmt.Errorf("minio fetch cancelled after %d attempt(s): %w", attempt, err)
			}
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
	return nil, fmt.Errorf("minio fetch failed after %d attempt(s): %w", maxAttempts, lastErr)
}

// minioFetchMaxAttempts is the bounded retry budget for fetchFromMinIO,
// overridable via MINIO_FETCH_MAX_ATTEMPTS (clamped to [1,10]); default 5.
// destHTTPTimeout is the per-request ceiling for one destination MCP call
// (a single upsert/import/fetch attempt). It bounds how long ONE wedged
// destination can stall the single serial consume loop (and, across retries,
// compounds). Default 120s — deliberately UNCHANGED: a legitimate large write
// (big import_data / object PUT) must not be cut off, which would DLQ good data
// and hurt delivery. Operators with a slow-but-healthy dest can raise it, or
// lower it (e.g. 30s) to fail over to the DLQ faster, via
// RSYNC_SINK_HTTP_TIMEOUT_SECONDS. Clamped to [10s, 600s].
func destHTTPTimeout() time.Duration {
	n := 120
	if v := strings.TrimSpace(os.Getenv("RSYNC_SINK_HTTP_TIMEOUT_SECONDS")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			n = parsed
		}
	}
	if n < 10 {
		n = 10
	}
	if n > 600 {
		n = 600
	}
	return time.Duration(n) * time.Second
}

func minioFetchMaxAttempts() int {
	n := 5
	if v := strings.TrimSpace(os.Getenv("MINIO_FETCH_MAX_ATTEMPTS")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			n = parsed
		}
	}
	if n < 1 {
		n = 1
	}
	if n > 10 {
		n = 10
	}
	return n
}

// isRetryableMinioFetchErr reports whether a fetchFromMinIOOnce error is worth
// retrying. NoSuchKey/not-found and network/timeouts are transient (read races,
// blips, round-robined endpoints); malformed-pointer / bad-param / unknown-shape
// errors are deterministic and fail fast. Default is retry: DLQ on exhaustion is
// the safe fallback, so an unrecognized error errs toward one more attempt.
func isRetryableMinioFetchErr(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	for _, perm := range []string{
		"must start with s3://",
		"must be s3://",
		"missing required param",
		"unknown payload shape",
	} {
		if strings.Contains(s, perm) {
			return false
		}
	}
	return true
}

func fetchFromMinIOOnce(ctx context.Context, httpClient *http.Client, claimCheckURL string) ([]map[string]interface{}, error) {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name": "minio_read_from_staging",
			"arguments": map[string]interface{}{
				"claim_check_url": claimCheckURL,
				"config":          map[string]interface{}{},
			},
		},
	}
	b, _ := json.Marshal(reqBody)
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://minio-mcp:8000/mcp", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("minio response not json: %w", err)
	}
	res, ok := out["result"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("minio missing result")
	}
	if success, _ := res["success"].(bool); !success {
		return nil, fmt.Errorf("minio read failed: %v", res["error"])
	}
	dataObj, ok := res["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("minio result missing data")
	}
	payload, ok := dataObj["data"].(map[string]interface{})
	if ok {
		// Orchestrator staging format: {"data": {"data": [...], ...}}
		if inner, ok := payload["data"].([]interface{}); ok {
			return ifaceRowsToMaps(inner), nil
		}
	}
	// Fallback: if minio returned directly as list
	if inner, ok := dataObj["data"].([]interface{}); ok {
		return ifaceRowsToMaps(inner), nil
	}
	return nil, fmt.Errorf("minio returned unknown payload shape")
}

// deleteFromMinIO removes a claim-check staging object after its rows have been
// durably written to the destination. Mirrors fetchFromMinIO but calls the
// minio connector's delete_from_staging tool. Returns an error only so the
// caller can log it; callers treat the result as best-effort.
func deleteFromMinIO(ctx context.Context, httpClient *http.Client, claimCheckURL string) error {
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name": "minio_delete_from_staging",
			"arguments": map[string]interface{}{
				"claim_check_url": claimCheckURL,
				"config":          map[string]interface{}{},
			},
		},
	}
	b, _ := json.Marshal(reqBody)
	req, _ := http.NewRequestWithContext(ctx, "POST", "http://minio-mcp:8000/mcp", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("minio delete response not json: %w", err)
	}
	res, ok := out["result"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("minio delete missing result")
	}
	if success, _ := res["success"].(bool); !success {
		return fmt.Errorf("minio delete failed: %v", res["error"])
	}
	return nil
}

func ifaceRowsToMaps(rows []interface{}) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(rows))
	for _, it := range rows {
		if m, ok := it.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
}

func ensureWriteState(writeStates map[string]*tableWriteState, sm *SinkMessage) *tableWriteState {
	if writeStates == nil || sm == nil {
		return &tableWriteState{}
	}
	key := writeStateKey(sm)
	st := writeStates[key]
	if st == nil {
		st = &tableWriteState{}
		writeStates[key] = st
	}
	if st.dataset == "" {
		st.dataset = slugify(sm.Dataset)
		if st.dataset == "" {
			st.dataset = slugify(sm.PipelineID)
		}
	}
	if st.dbOrSchema == "" {
		st.dbOrSchema = sanitizePathPart(sm.DBOrSchema)
		if st.dbOrSchema == "" {
			st.dbOrSchema = "default"
		}
	}
	if st.dt == "" {
		st.dt = strings.TrimSpace(sm.Dt)
		if st.dt == "" {
			st.dt = time.Now().UTC().Format("2006-01-02")
		}
	}
	if st.runMode == "" {
		st.runMode = strings.ToLower(strings.TrimSpace(sm.RunMode))
		if st.runMode == "" {
			st.runMode = "resume"
		}
	}
	return st
}

func sumInt64(xs []int64) int64 {
	var s int64
	for _, v := range xs {
		s += v
	}
	return s
}

func loadKeysAndCountsFromLedger(ctx context.Context, db *sql.DB, sm *SinkMessage) ([]string, []int64, error) {
	if db == nil || sm == nil {
		return nil, nil, nil
	}
	// Note: dest_key is destination-truth (written by sink after successful write).
	rows, err := db.QueryContext(
		ctx,
		`SELECT dest_key, rows_written
		 FROM pipeline_batch_acks
		 WHERE pipeline_id = $1 AND execution_id = $2 AND table_name = $3 AND rows_written >= 0
		 ORDER BY batch_offset ASC`,
		sm.PipelineID, sm.ExecutionID, sm.Table,
	)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var keys []string
	var counts []int64
	for rows.Next() {
		var k string
		var c int64
		if err := rows.Scan(&k, &c); err != nil {
			return nil, nil, err
		}
		if strings.TrimSpace(k) == "" {
			continue
		}
		keys = append(keys, k)
		counts = append(counts, c)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return keys, counts, nil
}

func callDestinationTool(ctx context.Context, httpClient *http.Client, cfg *WorkerConfig, destType, op string, args map[string]interface{}) (map[string]interface{}, error) {
	toolName := fmt.Sprintf("%s_%s", destType, op)
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name":      toolName,
			"arguments": args,
		},
	}
	body, _ := json.Marshal(reqBody)

	hosts := destinationHostCandidates(destType, cfg.DestinationVersion)
	var lastErr error
	for _, host := range hosts {
		u := fmt.Sprintf("http://%s:8000/mcp", host)
		req, _ := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var out map[string]interface{}
		if err := json.Unmarshal(raw, &out); err != nil {
			lastErr = fmt.Errorf("dest response not json: %w", err)
			continue
		}
		res := out
		if nested, ok := out["result"].(map[string]interface{}); ok && nested != nil {
			res = nested
		}
		success, _ := res["success"].(bool)
		if !success {
			if e, ok := res["error"].(string); ok && strings.TrimSpace(e) != "" {
				lastErr = fmt.Errorf("dest error: %s", e)
			} else {
				lastErr = fmt.Errorf("dest error: %v", res["error"])
			}
			// DIAG: destination tool call returned success=false. This is the silent-drop
			// blind spot — surface the tool, host and error so 0-row writes are explainable.
			logEvent("warn", "destination tool call failed", "tool", toolName, "host", host, "error", lastErr.Error())
			continue
		}
		// DIAG: destination tool call succeeded — log tool + any returned row/table info
		// so a "completed but 0 rows" run is fully traceable in SigNoz.
		logEvent("info", "destination tool call ok", "tool", toolName, "host", host,
			"table", toString(res["table"]), "rows_written", toString(res["rows_written"]),
			"rows", toString(res["rows"]), "imported", toString(res["imported"]))
		return res, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("destination tool call failed")
	}
	return nil, lastErr
}

type DDLSupport struct {
	// mu guards Enabled + resolved. Enabled may flip false->true lazily long
	// after startup (see supported), so every read MUST go through supported()
	// rather than touching Enabled directly.
	mu       sync.Mutex
	Enabled  bool
	resolved bool // true once a get_capabilities probe returned a definitive answer
	// Ensured tracks per-table ensured columns so we can re-run ensure_table only when schema drifts.
	// key: "<destType>:<schema.table>", value: map[column]struct{}
	Ensured sync.Map
	// Drift, when non-nil, receives one already-applied schema-change event per
	// column the CDC apply path adds on its own (see reportAppliedSchemaDrift).
	// Nil disables reporting entirely — the batch path never reports from here,
	// because the executor's detectAndEmitSchemaDrift is the only writer for batch.
	Drift *kafka.Writer
}

func toBool(v interface{}) bool {
	switch t := v.(type) {
	case bool:
		return t
	case string:
		s := strings.TrimSpace(strings.ToLower(t))
		return s == "true" || s == "1" || s == "yes" || s == "y"
	case json.Number:
		n, _ := t.Int64()
		return n != 0
	case float64:
		return t != 0
	case int:
		return t != 0
	case int64:
		return t != 0
	default:
		return false
	}
}

// probeDDLSupport queries the destination's get_capabilities and reports whether
// it can auto-create destination tables. gotAnswer distinguishes a definitive
// reply (HTTP OK + parsed) from a transient failure (connector unreachable / not
// yet ready / non-JSON). Only a definitive reply is cacheable; a transient miss
// must be retryable so a cold-start probe failure is not latched forever.
func probeDDLSupport(ctx context.Context, httpClient *http.Client, cfg *WorkerConfig, destType string) (enabled bool, gotAnswer bool) {
	if strings.TrimSpace(destType) == "" {
		return false, true // nothing to create — a definitive "no DDL needed"
	}
	res, err := callDestinationTool(ctx, httpClient, cfg, destType, "get_capabilities", map[string]interface{}{})
	if err != nil || res == nil {
		return false, false // transient — caller may retry later
	}
	// Capabilities may be present at top-level and/or under "capabilities".
	supportsDDL := toBool(res["supports_ddl"])
	autoCreate := toBool(res["auto_create_destination_tables"])
	if caps, ok := res["capabilities"].(map[string]interface{}); ok && caps != nil {
		if !supportsDDL {
			supportsDDL = toBool(caps["supports_ddl"])
		}
		if !autoCreate {
			autoCreate = toBool(caps["auto_create_destination_tables"])
		}
	}
	return supportsDDL && autoCreate, true
}

// resolveDDLSupport runs the best-effort startup probe. A transient failure here
// (e.g. the destination MCP connector not yet reachable during a cold stack
// start) leaves resolved=false so supported() re-probes on the first real write,
// rather than permanently disabling auto-create-table for the worker's lifetime.
// This is the root-cause fix for the "0 rows landed / relation does not exist"
// class: previously a single cold-start probe miss latched Enabled=false and every
// ensure_table call silently no-oped.
func resolveDDLSupport(ctx context.Context, httpClient *http.Client, cfg *WorkerConfig, destType string) *DDLSupport {
	enabled, gotAnswer := probeDDLSupport(ctx, httpClient, cfg, destType)
	ddl := &DDLSupport{Enabled: enabled, resolved: gotAnswer}
	if !gotAnswer {
		logf("warning", "destination DDL capability probe failed at startup (dest=%s); will re-probe lazily on first write so a cold-start race cannot disable auto-create-table", destType)
	}
	return ddl
}

// supported reports whether the destination supports auto-create-table, lazily
// re-probing get_capabilities when the startup probe did not get a definitive
// answer. The destination connector is guaranteed reachable by the time rows
// actually flow, so this self-heals a cold-start probe failure. Guarded by mu so
// the lazy false->true transition is safe across the batch + CDC goroutines that
// share one *DDLSupport. Every read of Enabled must go through this method.
func (d *DDLSupport) supported(ctx context.Context, httpClient *http.Client, cfg *WorkerConfig, destType string) bool {
	if d == nil {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.resolved {
		return d.Enabled
	}
	enabled, gotAnswer := probeDDLSupport(ctx, httpClient, cfg, destType)
	if gotAnswer {
		d.Enabled = enabled
		d.resolved = true
		if enabled {
			logf("info", "destination DDL support resolved on retry (dest=%s): auto-create-table now enabled", destType)
		}
	}
	return d.Enabled
}

func columnsFromRows(rows []map[string]interface{}) []string {
	if len(rows) == 0 {
		return nil
	}
	// Union keys across ALL rows, not just rows[0]. Wide-format transforms like
	// json_flatten produce ragged rows — each source record's nested JSON has a
	// different key set, so the flattened columns differ row to row. Deriving the
	// column list from the first row alone silently drops every column that only
	// appears in later rows. First-seen order is preserved for stable output.
	cols := make([]string, 0, len(rows[0]))
	seen := map[string]struct{}{}
	for _, row := range rows {
		if row == nil {
			continue
		}
		for k := range row {
			kk := strings.TrimSpace(k)
			if kk == "" {
				continue
			}
			if _, ok := seen[kk]; ok {
				continue
			}
			seen[kk] = struct{}{}
			cols = append(cols, kk)
		}
	}
	return cols
}

func inferKeyFieldsForRow(row map[string]interface{}) []string {
	if row == nil {
		return nil
	}
	if _, ok := row["id"]; ok {
		return []string{"id"}
	}
	// If there's exactly one *_id field, prefer it.
	cands := []string{}
	for k := range row {
		kk := strings.TrimSpace(k)
		if strings.HasSuffix(strings.ToLower(kk), "_id") {
			cands = append(cands, kk)
		}
	}
	if len(cands) == 1 {
		return []string{cands[0]}
	}
	return nil
}

// pipelineID identifies which pipeline owns the destination namespace.
// When supplied, destination connectors (PG/MySQL) write a row to
// `_rsync_pipelines` under ensure_table so reload-mode drop_table can
// refuse to destroy another pipeline's tables. Empty string means
// "no ownership tracking" (pre-round-4 behavior + CDC adhoc runs).
// failOnMissingColumns turns the additive reconciler into a fail-closed gate:
// when true, a column that was previously ensured but is ABSENT from the current
// row is treated as a destructive source change (DROP/RENAME COLUMN or
// incompatible type change) and returns a fatalError so the caller can halt.
// Callers MUST set it only for full-image upsert events (op c/r/u) — never for
// delete images, which under non-FULL replica identity carry only the PK and
// would otherwise look like "every data column was dropped".
// reportDrift makes an additive reconcile visible to the control plane (see
// reportAppliedSchemaDrift). ONLY the CDC apply paths set it: for batch, the
// executor's detectAndEmitSchemaDrift is the sole drift producer, and reporting
// here too would file every change twice.
func ensureDestinationTable(ctx context.Context, httpClient *http.Client, cfg *WorkerConfig, ddl *DDLSupport, destType string, targetTable string, namespace string, rows []map[string]interface{}, keyFields []string, columnTypes map[string]string, pipelineID string, typesAreDDL bool, failOnMissingColumns bool, reportDrift bool) error {
	// supported() lazily re-resolves DDL capability when the startup probe missed
	// (cold-start race), so a brand-new destination table is still created here
	// instead of every INSERT failing with "relation does not exist".
	if ddl == nil || !ddl.supported(ctx, httpClient, cfg, destType) {
		return nil
	}
	destType = canonicalConnectorType(destType)
	if destType == "" {
		return nil
	}
	key := ensuredCacheKey(destType, namespace, targetTable)
	if strings.TrimSpace(targetTable) == "" {
		return nil
	}
	cols := columnsFromRows(rows)
	if len(cols) == 0 {
		// Nothing to ensure (but don't mark as ensured).
		// DIAG: a 0-column ensure means the row set was empty — ensure_table is skipped,
		// so the table is never created and the subsequent import writes nothing. This is
		// the silent "completed but 0 rows / table missing" path; make it visible.
		logEvent("warn", "ensureDestinationTable: 0 columns from rows — skipping table create (empty batch?)",
			"table", targetTable, "rows", fmt.Sprintf("%d", len(rows)), "column_types", fmt.Sprintf("%d", len(columnTypes)))
		return nil
	}
	keys := []string{}
	if len(keyFields) > 0 {
		keys = append(keys, keyFields...)
	}

	// Snapshot the previously-ensured column set ONCE. The fail-closed gate, the
	// additive merge, the already-ensured short-circuit and the drift report all
	// ask the same question; re-Loading gave four chances to get four answers.
	// prev == nil means "we have not ensured this table in this process yet".
	var prev map[string]struct{}
	if v, ok := ddl.Ensured.Load(key); ok {
		if p, ok := v.(map[string]struct{}); ok {
			prev = p
		}
	}

	// Fail-closed schema-evolution gate. A column we previously ensured for this
	// table that is now absent from a full-image row means the source dropped or
	// renamed it (or changed it to an incompatible type). The additive reconciler
	// below cannot express a removal, so it would silently keep writing the old
	// schema while the source diverges — exactly the silent-corruption class. Halt
	// instead, via a fatalError the CDC apply paths escalate to os.Exit. Runs
	// BEFORE the "already ensured" short-circuit so a drop is caught even when no
	// new columns were added. Guarded to full-image callers (see the doc comment).
	if failOnMissingColumns && prev != nil {
		curSet := make(map[string]struct{}, len(cols)+len(keys))
		for _, c := range cols {
			curSet[c] = struct{}{}
		}
		for _, c := range keys {
			curSet[c] = struct{}{}
		}
		for c := range prev {
			if strings.HasPrefix(c, "__pk__") {
				continue
			}
			if _, present := curSet[c]; !present {
				return fatalError{err: fmt.Errorf(
					"destructive schema change on table %q: column %q was previously "+
						"present but is absent from the source row (DROP/RENAME COLUMN or "+
						"incompatible type change); halting fail-closed", targetTable, c)}
			}
		}
	}

	// IMPORTANT: Never infer key fields from row data for DDL.
	//
	// Why:
	// - heuristics can be wrong (e.g. orders.customer_id is not unique)
	// - map iteration can be nondeterministic, producing different "guessed keys" across runs
	// - wrong unique indexes permanently poison future runs with duplicate-key DLQs
	//
	// We only create/reconcile unique indexes when the producer (executor) provides explicit key fields.

	// Merge with already ensured columns (so subsequent drift adds columns without re-creating everything).
	merged := map[string]struct{}{}
	for c := range prev {
		merged[c] = struct{}{}
	}
	for _, c := range cols {
		merged[c] = struct{}{}
	}
	for _, c := range keys {
		merged[c] = struct{}{}
		// Track key-fields separately so a later discovery of PKs triggers ensure_table again
		// (even if the PK columns already existed).
		merged["__pk__"+c] = struct{}{}
	}

	// If we already ensured all requested columns, skip.
	if prev != nil {
		all := true
		for c := range merged {
			if _, exists := prev[c]; !exists {
				all = false
				break
			}
		}
		if all {
			return nil
		}
	}

	colsMerged := make([]string, 0, len(merged))
	for c := range merged {
		if strings.HasPrefix(c, "__pk__") {
			continue
		}
		colsMerged = append(colsMerged, c)
	}

	typed := map[string]interface{}{}
	if len(columnTypes) > 0 {
		for _, c := range colsMerged {
			if t, ok := columnTypes[c]; ok && strings.TrimSpace(t) != "" {
				typed[c] = strings.TrimSpace(t)
			}
		}
	}

	ensureParams := map[string]interface{}{
		"config":       cfg.DestinationConfig,
		"table":        targetTable,
		"columns":      colsMerged,
		"column_types": typed,
		"key_fields":   keys,
		// Best-effort: if keys are provided, allow the destination connector to reconcile
		// previously auto-created unique indexes that don't match the desired PK.
		"replace_unique_indexes": len(keys) > 0,
		// Tell the destination whether column_types are FINAL DDL types (CDC) or
		// canonical source names (batch). Without this the connector re-maps a
		// resolved "DOUBLE PRECISION"/"JSONB"/"TIMESTAMPTZ" down to TEXT.
		"types_are_ddl": typesAreDDL,
	}
	// Append-only history mode: the table intentionally has NO PK/unique constraint
	// (it stores every change version). Empty key_fields here MUST NOT trigger the
	// connector's synthetic-PK fallback (_rsync_row_hash + a unique index), which would
	// reintroduce a uniqueness constraint that breaks the delete-tombstone insert and
	// silently dedup rows. synthetic_pk:false is the hard opt-out; append_mode is a
	// forward-compatible explicit signal. Only set when cdcAppendMode — default upsert
	// callers send neither, so connector behavior is unchanged there.
	if cdcAppendMode(cfg) {
		ensureParams["synthetic_pk"] = false
		ensureParams["append_mode"] = true
	}
	// Round-4 destination-namespace ownership: PG/MySQL connectors write a
	// row to _rsync_pipelines when pipeline_id is supplied. Without this the
	// ownership-gated drop_table refusal in reload mode can't engage. Empty
	// string is acceptable (pre-round-4 callers / adhoc CDC); connectors
	// guard with `if pipeline_id and schema:` before INSERT.
	if strings.TrimSpace(pipelineID) != "" {
		ensureParams["pipeline_id"] = pipelineID
	}
	// Forward the authoritative per-pipeline namespace so the connector creates the
	// table in the right database (targetTable is bare for single-namespace dests).
	addNamespaceParam(ensureParams, namespace)
	_, err := callDestinationTool(ctx, httpClient, cfg, destType, "ensure_table", ensureParams)
	if err != nil {
		return err
	}
	ddl.Ensured.Store(key, merged)

	// Tell the control plane what CDC just applied on its own. prev == nil means
	// this is the first ensure for this table in this process — a worker restart
	// re-ensures every table, and calling that "drift" would file a schema change
	// on every deploy. Only a table we had already ensured, now growing a column,
	// is a real source change.
	if reportDrift && prev != nil {
		if added := addedColumnNames(prev, merged); len(added) > 0 {
			reportAppliedSchemaDrift(ctx, ddl.Drift, cfg, pipelineID, targetTable, namespace, added, typed)
		}
	}
	return nil
}

// addedColumnNames returns the columns present in merged but not in prev, sorted so
// the emitted DDL (and therefore the schema_change_approvals UNIQUE (pipeline_id, ddl)
// key) is stable across runs rather than following Go's randomized map order.
// __pk__-prefixed bookkeeping entries are not columns and are excluded.
func addedColumnNames(prev, merged map[string]struct{}) []string {
	added := make([]string, 0, len(merged))
	for c := range merged {
		if strings.HasPrefix(c, "__pk__") {
			continue
		}
		if _, existed := prev[c]; !existed {
			added = append(added, c)
		}
	}
	sort.Strings(added)
	return added
}

// reportAppliedSchemaDrift files one already-applied schema change per column the CDC
// apply path just added to the destination by itself.
//
// CDC's ensure_table reconciler applies additive drift the moment a new column shows
// up in a row and asks nobody. That is the design and it is NOT changed here. What was
// missing is that it also TOLD nobody: the same `ALTER TABLE … ADD COLUMN` that stops a
// BATCH pipeline and waits for a human in the Schema changes tab passed through a CDC
// pipeline with no row, no badge and no notification. Two halves of the product
// answered "who approves a schema change?" oppositely, and only one of them said so.
//
// applied=true is the whole point of the message: the healer records it as history
// (status `applied`) instead of queueing an approval for a change that is already
// live in the destination.
//
// Best-effort by construction. The destination has already committed the change, so a
// broker hiccup here must never fail or stall the data path — it costs a log line and
// a missing history row, not a stalled stream.
func reportAppliedSchemaDrift(ctx context.Context, w *kafka.Writer, cfg *WorkerConfig, pipelineID, table, namespace string, added []string, columnTypes map[string]interface{}) {
	if w == nil || len(added) == 0 {
		return
	}
	if strings.TrimSpace(pipelineID) == "" {
		pipelineID = strings.TrimSpace(cfg.PipelineID)
	}
	if pipelineID == "" {
		// schema_change_approvals.pipeline_id is NOT NULL and FK-checked; an
		// unattributable change can only be dropped.
		logEvent("warn", "schema drift applied but not reported: no pipeline_id", "table", table)
		return
	}

	// Qualify the table the way the batch detector does, so both producers' DDL reads
	// the same in the UI. The destination connector re-resolves the real namespace at
	// apply time; this string is for the human.
	qualified := table
	if ns := strings.TrimSpace(namespace); ns != "" && !strings.Contains(table, ".") {
		qualified = ns + "." + table
	}

	msgs := make([]kafka.Message, 0, len(added))
	for _, col := range added {
		colType := "unknown"
		if t, ok := columnTypes[col].(string); ok && strings.TrimSpace(t) != "" {
			colType = strings.TrimSpace(t)
		}
		// Same deterministic shape the executor's batch detector emits, so the
		// healer's parseAddColumnDDL reads either producer.
		ddlText := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", qualified, col, colType)
		b, err := json.Marshal(map[string]interface{}{
			"event_type":  "SCHEMA_CHANGE_DETECTED",
			"pipeline_id": pipelineID,
			"timestamp":   time.Now().UTC().Format(time.RFC3339),
			"schema_change": map[string]interface{}{
				"change_type": "add_column",
				"table":       qualified,
				"schema_name": strings.TrimSpace(namespace),
				"column_name": col,
				"column_type": colType,
				"ddl":         ddlText,
				"risk_level":  "low",
				"detected_at": time.Now().UTC().Format(time.RFC3339),
				// Already live in the destination — record it, don't queue it.
				"applied": true,
			},
			"context": map[string]interface{}{
				"source": "kafka_mcp_sink",
				"mode":   "cdc",
			},
			// Nothing to decide: the change is applied. Kept explicit so a reader of
			// the topic doesn't have to infer it from `applied`.
			"action_needed": false,
		})
		if err != nil {
			logEvent("warn", "schema drift applied but not reported: marshal failed",
				"table", qualified, "column", col, "error", err.Error())
			continue
		}
		msgs = append(msgs, kafka.Message{Key: []byte(pipelineID), Value: b})
	}
	if len(msgs) == 0 {
		return
	}

	// Bound the write: the caller is on the CDC apply path holding an uncommitted
	// batch, and an unreachable broker must not hold it there.
	wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := w.WriteMessages(wctx, msgs...); err != nil {
		logEvent("warn", "schema drift applied but not reported: publish failed",
			"table", qualified, "columns", strings.Join(added, ","), "error", err.Error())
		return
	}
	logEvent("info", "reported already-applied schema drift",
		"table", qualified, "columns", strings.Join(added, ","))
}

// writeToDestination writes one object-write unit (one physical object) for object
// storage, or one batch of rows for relational/warehouse destinations. For object
// storage partSegs is the Hive "col=val/" prefix for this unit's partition (empty = no
// column partitioning) and partSuffix is the file-rolling chunk index ("" = legacy
// single-file key); the caller splits a batch into units via splitRowsForObjectPartition
// + chunkRowsForFileRolling and invokes this once per unit. partSegs/partSuffix are
// ignored for relational/warehouse destinations.
func writeToDestination(ctx context.Context, httpClient *http.Client, cfg *WorkerConfig, ddl *DDLSupport, sm *SinkMessage, rows []map[string]interface{}, partSegs string, partSuffix string) (writtenRows int64, destKey string, err error) {
	// For object storage, force deterministic keys to make retries idempotent.
	destCfg := cfg.DestinationConfig
	destType := canonicalConnectorType(cfg.DestinationConnector)
	looksLikeObjectStorage := isObjectStorageConnector(destType)
	isWarehouse := isDataWarehouseConnector(destType)

	// Canonical table resolution. Forward the namespace ONLY for relational dests:
	// they bare-ify the table and forward the namespace separately (mirrors the CDC
	// paths 963-970, 3390-3392), which fixes the PG silent-drop where a real
	// namespace + source-qualified table made the connector ignore the namespace and
	// write into the leaked source schema (read>0, landed=0). Warehouses honor the
	// same bare-table + namespace contract (the connector's ensure_table/load/drop_table
	// override config["schema"] with the forwarded namespace), so they bare the table
	// here and receive the namespace below — otherwise the pipeline's destination_namespace
	// is silently ignored and rows land in the connection's default schema while the
	// reload-cleanup DROP (which DID forward the namespace) targets a different schema →
	// duplicate accumulation. Object storage namespaces via its own key layout, so it
	// still resolves with no namespace.
	nsForTable := sm.DBOrSchema
	if looksLikeObjectStorage {
		nsForTable = ""
	}
	targetTable := resolveDestTableForWrite(destType, destCfg, sm.Table, nsForTable)

	// Best-effort: auto-create destination table if connector advertises support.
	// Only apply for non-object-storage, non-warehouse DB destinations (warehouses may require staging/DDL policies).
	// Document DBs (MongoDB) have no DDL/ensure_table — collections auto-create on first write — so skip it.
	if !looksLikeObjectStorage && !isWarehouse && !isDocumentDBConnector(destType) {
		// Prefer explicit key-fields when provided by the producer (executor).
		keyFields := []string{}
		if sm != nil && len(sm.KeyFields) > 0 {
			keyFields = append([]string{}, sm.KeyFields...)
		}
		smPipelineID := ""
		if sm != nil {
			smPipelineID = sm.PipelineID
		}
		var batchColumnTypes map[string]string
		typesAreDDL := false
		if sm != nil && len(sm.ColumnTypes) > 0 {
			batchColumnTypes = sm.ColumnTypes
			typesAreDDL = sm.ColumnTypesAreDDL
		}
		smNamespace := ""
		if sm != nil {
			smNamespace = sm.DBOrSchema
		}
		// Non-CDC batch write path: additive reconcile only (no fail-closed drop gate,
		// and no drift report — the executor's detectAndEmitSchemaDrift owns batch).
		if err := ensureDestinationTable(ctx, httpClient, cfg, ddl, destType, targetTable, smNamespace, rows, keyFields, batchColumnTypes, smPipelineID, typesAreDDL, false, false); err != nil {
			return 0, destKey, err
		}
	}

	params := map[string]interface{}{
		"config": destCfg,
		"data":   rows,
		"table":  targetTable,
	}
	// Forward the authoritative destination namespace on the write call. targetTable
	// is bare, so without this the connector writes to config["database"]/["schema"].
	// Relational AND warehouse dests honor it: the connector's load()/ensure_table
	// override config["schema"] with the forwarded namespace when the table is bare
	// (see connector _config_with_namespace). This makes the WRITE land in the same
	// schema the reload-cleanup DROP already targets (drop forwards sm.DBOrSchema
	// unconditionally at ~3478) — without it the pipeline's destination_namespace is
	// silently dropped for warehouses and rows pile up in the connection default schema.
	// Object storage keys by its own layout and takes no per-row namespace.
	if sm != nil && !looksLikeObjectStorage {
		addNamespaceParam(params, sm.DBOrSchema)
	}
	if sm != nil && len(sm.KeyFields) > 0 {
		params["key_fields"] = append([]string{}, sm.KeyFields...)
		params["primary_key_fields"] = append([]string{}, sm.KeyFields...)
	}
	// Forward column types to the destination so its bind helpers can do
	// type-aware encoding (e.g. MySQL JSON columns need json.dumps'd
	// values — without this, PG JSONB → MySQL JSON pipelines fail with
	// "Invalid JSON text" because primitive PG values arrive as bare
	// strings and MySQL refuses to parse them as JSON). Same map already
	// passed to ensure_table above; the write path needs it too.
	if sm != nil && len(sm.ColumnTypes) > 0 {
		ct := make(map[string]string, len(sm.ColumnTypes))
		for k, v := range sm.ColumnTypes {
			ct[k] = v
		}
		params["column_types"] = ct
	}

	if looksLikeObjectStorage {
		bucket := firstStr(destCfg, "bucket", "bucket_name")
		container := firstStr(destCfg, "container")
		prefix := firstStr(destCfg, "path_prefix", "prefix", "base_prefix", "key_prefix", "base_path", "path")
		if prefix == "" {
			prefix = "test-aws-s3"
		}
		format := firstStr(destCfg, "file_format", "format")
		if format == "" {
			format = "csv"
		}
		compression := firstStr(destCfg, "compression")
		if compression == "" {
			compression = "none"
		}
		// New deterministic object storage layout:
		// {prefix}/{dataset}/{db_or_schema}/{table}/dt=YYYY-MM-DD/part-{offset:06d}.{format}[.{compression}]
		dataset := slugify(sm.Dataset)
		if dataset == "" {
			dataset = slugify(sm.PipelineID)
		}
		dbOrSchema := sanitizePathPart(sm.DBOrSchema)
		if dbOrSchema == "" {
			dbOrSchema = "default"
		}
		tablePart := sm.Table
		if idx := strings.LastIndex(tablePart, "."); idx >= 0 && idx+1 < len(tablePart) {
			tablePart = tablePart[idx+1:]
		}
		tablePart = sanitizePathPart(tablePart)
		dt := strings.TrimSpace(sm.Dt)
		if dt == "" {
			dt = time.Now().UTC().Format("2006-01-02")
		}
		ext := fileExt(format, compression)
		destKey = partKey(prefix, dataset, dbOrSchema, tablePart, partSegs, dt, sm.BatchOffset, partSuffix, ext)
		params["key"] = destKey
		if canonicalConnectorType(destType) == "azure-blob" {
			if container != "" {
				params["container"] = container
			} else if bucket != "" {
				// Back-compat: allow bucket field to serve as container.
				params["container"] = bucket
			}
		} else if bucket != "" {
			params["bucket"] = bucket
		}
		// Provide both legacy and newer naming variants; connector will normalize.
		params["format"] = format
		params["file_format"] = format
		params["compression"] = compression
	}

	// Always use MCP JSON-RPC tool invocation for broad compatibility.
	// Many connectors do NOT support legacy direct-method calls like "<connector>_import_data"
	// (especially when connector_type includes '-' which cannot be a Python attribute).
	toolName := fmt.Sprintf("%s_import_data", destType)
	// Batch runs should be idempotent when key-fields are available. Use upsert semantics
	// so reruns (resume/reload) don't DLQ on unique constraint violations.
	if !looksLikeObjectStorage && !isWarehouse && sm != nil && len(sm.KeyFields) > 0 {
		toolName = fmt.Sprintf("%s_upsert_data", destType)
	}
	// Keyless relational destination + not append-mode → synthetic-PK path.
	// ensureDestinationTable created _rsync_row_hash (NOT NULL) + a unique index for this
	// case; the write must go through upsert_data(synthetic_pk=true) so the connector
	// computes the hash and upserts on it. A plain import_data would insert a NULL hash
	// and the NOT NULL constraint would reject every row — a silent drop of a keyless
	// (e.g. MySQL GIPK) table. Matches ensureDestinationTable's own synthetic decision.
	// Document DBs never synthesize a PK (Mongo auto-assigns _id), so a keyless source
	// stays a plain import_data insert — excluded here.
	if !looksLikeObjectStorage && !isWarehouse && !isDocumentDBConnector(destType) && (sm == nil || len(sm.KeyFields) == 0) && !cdcAppendMode(cfg) {
		toolName = fmt.Sprintf("%s_upsert_data", destType)
		params["synthetic_pk"] = true
	}
	if !looksLikeObjectStorage && isWarehouse {
		toolName = fmt.Sprintf("%s_load", destType)
	}
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]interface{}{
			"name":      toolName,
			"arguments": params,
		},
	}
	body, _ := json.Marshal(reqBody)

	hosts := destinationHostCandidates(destType, cfg.DestinationVersion)
	var lastErr error
	for _, host := range hosts {
		u := fmt.Sprintf("http://%s:8000/mcp", host)
		req, _ := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var out map[string]interface{}
		if err := json.Unmarshal(raw, &out); err != nil {
			lastErr = fmt.Errorf("dest response not json: %w", err)
			continue
		}

		// Destination MCPs may respond in either shape:
		// - Direct result: {"success": true, ...}
		// - JSON-RPC: {"jsonrpc":"2.0","id":...,"result":{"success": true, ...}}
		res := out
		if nested, ok := out["result"].(map[string]interface{}); ok && nested != nil {
			res = nested
		}

		success, _ := res["success"].(bool)
		if !success {
			// If the connector doesn't support upsert/delete, fall back to import_data.
			// Many append-only connectors (e.g. Google Sheets) don't implement upsert_data.
			importDataTool := fmt.Sprintf("%s_import_data", destType)
			if toolName != importDataTool {
				errMsg := strings.ToLower(fmt.Sprint(res["error"]))
				if strings.Contains(errMsg, "not found") || strings.Contains(errMsg, "method not found") || strings.Contains(errMsg, "unknown tool") {
					reqBody["params"].(map[string]interface{})["name"] = importDataTool
					body, _ = json.Marshal(reqBody)
					req, _ = http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					resp, err = httpClient.Do(req)
					if err != nil {
						lastErr = err
						continue
					}
					raw, _ = io.ReadAll(resp.Body)
					resp.Body.Close()
					if err := json.Unmarshal(raw, &out); err != nil {
						lastErr = fmt.Errorf("dest fallback response not json: %w", err)
						continue
					}
					res = out
					if nested, ok := out["result"].(map[string]interface{}); ok && nested != nil {
						res = nested
					}
					success, _ = res["success"].(bool)
				}
			}
			if !success {
				if e, ok := res["error"].(string); ok && strings.TrimSpace(e) != "" {
					lastErr = fmt.Errorf("dest error: %s", e)
				} else {
					lastErr = fmt.Errorf("dest error: %v", res["error"])
				}
				continue
			}
		}

		// STRICT: the dest MCP MUST report a write-count field. Accept any
		// canonical name in destWriteCountFields — all are equivalent
		// positive write signals (insert / upsert / merge / load / etc.).
		// Falling back to len(rows) created silent data-loss: an MCP that
		// returned {"success":true} without a count — or whose request body
		// was discarded by a downstream broken connection — got us recording
		// rowcount=len(rows) acks for batches where 0 rows actually landed.
		// The sink then committed Kafka offsets and dedup keys against rows
		// that didn't exist downstream.
		// To re-enable the old behavior set RSYNC_SINK_TRUST_LEN_FALLBACK=1
		// (operator override only; should never be on in prod).
		if n, ok := extractDestRowCount(res); ok {
			return n, destKey, nil
		}
		if strings.TrimSpace(os.Getenv("RSYNC_SINK_TRUST_LEN_FALLBACK")) == "1" {
			return int64(len(rows)), destKey, nil
		}
		// No usable count — refuse to claim success. Caller treats this as a
		// transient error → retry → eventually DLQ-poison after maxAttempts.
		lastErr = fmt.Errorf("dest response missing write-count field (any of %v): %s",
			destWriteCountFields, truncateForErr(raw, 200))
		continue
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("destination call failed")
	}
	return 0, destKey, lastErr
}

func truncateForErr(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// destWriteCountFields is the canonical, exhaustive list of result-map keys
// that a destination MCP may use to report how many rows it wrote/accepted
// for a batch. All seven are equivalent positive write-count signals and
// the sink treats them identically:
//
//   - rows_inserted   — vanilla INSERT path (Postgres/MySQL import_data,
//     S3/GCS/Azure object writes, Snowflake/BigQuery small
//     loads)
//   - rows_written    — historical alias used by some early connectors and
//     the per-table dedup ledger
//   - rows_upserted   — Postgres/MySQL upsert_data (ON CONFLICT / ON
//     DUPLICATE KEY) — emitted on every batch with key
//     fields, so this is the hot path for DB→DB pipelines
//   - rows_merged     — warehouse MERGE / CDC apply (BigQuery/Snowflake
//     merge() path)
//   - rows_loaded     — bulk load via COPY / LoadJobConfig (BigQuery /
//     Snowflake large-batch path)
//   - rows_affected   — generic SQL execute-style result
//   - rows_deleted    — delete-only operation count
//
// Adding a new dest connector? If its write returns a synonym (e.g.
// "rows_imported", "rows_appended"), add it here rather than handling it at
// the call site — both the strict batch path and the streaming CDC path
// read from this list, so a single change covers them.
var destWriteCountFields = []string{
	"rows_inserted",
	"rows_written",
	"rows_upserted",
	"rows_merged",
	"rows_loaded",
	"rows_affected",
	"rows_deleted",
}

// extractDestRowCount returns the row count a destination MCP reported in
// its result map and whether a count was present. Tries each canonical
// field in destWriteCountFields and returns the first non-negative value
// found. Tolerates both float64 (JSON number) and integer wire encodings
// via toInt64.
func extractDestRowCount(res map[string]interface{}) (int64, bool) {
	for _, k := range destWriteCountFields {
		if v, ok := res[k]; ok {
			if n := toInt64(v); n >= 0 {
				return n, true
			}
		}
	}
	return 0, false
}

func destinationHostCandidates(connectorType, version string) []string {
	// Prefer versioned container name used by compose/JIT: rsync-ai-<connector>-vX-Y-Z-mcp
	connectorType = strings.TrimSpace(connectorType)
	// Canonical connector IDs use kebab-case; the Kafka producer may send either form.
	// Docker network aliases always use hyphens, so normalize here.
	connectorType = strings.ReplaceAll(connectorType, "_", "-")
	v := strings.TrimSpace(version)
	v = strings.TrimPrefix(v, "v")
	vPart := strings.ReplaceAll(v, ".", "-")

	candidates := make([]string, 0, 5)
	if connectorType != "" && vPart != "" && vPart != "latest" {
		candidates = append(candidates, fmt.Sprintf("rsync-ai-%s-v%s-mcp", connectorType, vPart))
	}
	// Service name fallback (docker-compose): <connector>-mcp
	candidates = append(candidates, fmt.Sprintf("%s-mcp", connectorType))
	// Alias fallback: rsync-ai-<connector>-mcp
	candidates = append(candidates, fmt.Sprintf("rsync-ai-%s-mcp", connectorType))
	// Underscore variant
	candidates = append(candidates, fmt.Sprintf("rsync-ai-%s-mcp", strings.ReplaceAll(connectorType, "-", "_")))
	return uniqStrings(candidates)
}

func uniqStrings(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	return out
}

func firstStr(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func emitTableStats(ctx context.Context, w *kafka.Writer, sm *SinkMessage, mode, status string, readRows, insertedRows, bytesCommitted int64) error {
	event := buildTableStatsEvent(sm, mode, status, readRows, insertedRows, bytesCommitted)
	b, _ := json.Marshal(event)

	return w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(sm.PipelineID),
		Value: b,
		Headers: []kafka.Header{
			{Key: "trace_id", Value: []byte(sm.TraceID)},
		},
	})
}

// buildTableStatsEvent builds the batch-mode TABLE_STATS payload. Split out from the
// write for the same reason as buildCDCTableStatsEvent: the identity/counts contract
// the api-gateway projector reads is then unit-testable without a broker.
func buildTableStatsEvent(sm *SinkMessage, mode, status string, readRows, insertedRows, bytesCommitted int64) map[string]interface{} {
	table, tableName := tableIdentityForStats(sm)

	event := map[string]interface{}{
		"schema_version": 2,
		"event_type":     "TABLE_STATS",
		"pipeline_id":    sm.PipelineID,
		"execution_id":   sm.ExecutionID,
		"trace_id":       sm.TraceID,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"stage":          "executor",
		"stage_group":    "executing",
		"status":         "processing",
		"message":        fmt.Sprintf("Table statistics update: %s", tableName),
		"metadata": map[string]interface{}{
			"source": "kafka_mcp_sink",
			"mode":   mode,
			"status": status,
			"table":  table,
			"counts": map[string]interface{}{
				"read_rows":       readRows,
				"inserted_rows":   insertedRows,
				"bytes_committed": bytesCommitted,
			},
			"started_at":   time.Now().UTC().Format(time.RFC3339),
			"completed_at": time.Now().UTC().Format(time.RFC3339),
		},
	}
	return event
}

// sendToDLQ parks a record the destination will never accept. `table` is the
// qualified source table the record belongs to ("" when it could not be parsed);
// it is required, not optional, because a DLQ routing IS a row loss and the only
// way to report it is against the table that lost it. On a successful publish this
// bumps both the aggregate dlqRouted counter and the per-table split — every DLQ
// route in this worker is counted here exactly once, so no call site needs to
// (and none should) increment those counters itself.
func sendToDLQ(ctx context.Context, w *kafka.Writer, msg kafka.Message, err error, metrics *Metrics, table string) error {
	// Fail-closed: never "drop" DLQ messages. If DLQ is unavailable, callers must halt.
	if w == nil {
		if metrics != nil {
			atomic.AddUint64(&metrics.dlqPublishFailures, 1)
		}
		return fmt.Errorf("DLQ writer is nil")
	}

	srcTopic := strings.TrimSpace(msg.Topic)
	if srcTopic == "" {
		srcTopic = "unknown"
	}
	dlqTopic := srcTopic + ".dlq"

	dlqPayload := map[string]interface{}{
		"error":     err.Error(),
		"topic":     srcTopic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"raw_value": string(msg.Value),
	}
	// Best-effort: expose DMS-style op codes alongside Debezium op to avoid confusion.
	// Debezium uses op=c/u/d/r (r=snapshot read); DMS-style uses I/U/D (treat r as I).
	{
		var payload map[string]interface{}
		if uerr := json.Unmarshal(msg.Value, &payload); uerr == nil && payload != nil {
			if op, ok := payload["op"].(string); ok && strings.TrimSpace(op) != "" {
				dlqPayload["debezium_op"] = strings.TrimSpace(op)
				dlqPayload["op"] = bronzeOpFromDebezium(op)
			}
		}
	}
	b, _ := json.Marshal(dlqPayload)

	// Retry with exponential backoff
	var lastErr error
	backoff := 100 * time.Millisecond
	maxBackoff := 5 * time.Second
	maxRetries := 3

	for attempt := 0; attempt <= maxRetries; attempt++ {
		writeErr := w.WriteMessages(ctx, kafka.Message{Topic: dlqTopic, Key: msg.Key, Value: b})
		if writeErr == nil {
			if metrics != nil {
				atomic.AddUint64(&metrics.dlqRouted, 1)
				if t := strings.TrimSpace(table); t != "" {
					incrementCounter(&metrics.dlqByTable, t, 1)
				}
			}
			// Always log every DLQ routing so a bad record is never silently
			// shed. A DLQ routing is a recoverable bad-record event → warn.
			// trace_id/table come from the source message headers so the line
			// is correlatable in SigNoz even though sm is not in scope here.
			logEvent("warn", "message routed to DLQ",
				"trace_id", strings.TrimSpace(string(headerValue(msg.Headers, "trace_id"))),
				"table", strings.TrimSpace(string(headerValue(msg.Headers, "table"))),
				"dlq_topic", dlqTopic,
				"topic", srcTopic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"reason", err.Error(),
			)
			return nil
		}
		lastErr = writeErr

		if attempt == maxRetries {
			break
		}

		// Exponential backoff with jitter
		jitter := time.Duration(rand.Int63n(int64(backoff / 10)))
		sleepTime := backoff + jitter
		if sleepTime > maxBackoff {
			sleepTime = maxBackoff
		}

		select {
		case <-ctx.Done():
			if metrics != nil {
				atomic.AddUint64(&metrics.dlqPublishFailures, 1)
			}
			return ctx.Err()
		case <-time.After(sleepTime):
		}

		backoff *= 2
	}

	if metrics != nil {
		atomic.AddUint64(&metrics.dlqPublishFailures, 1)
	}
	// DLQ publish exhausted all retries — this is a worker-level failure (the
	// caller will typically halt). Surface it loudly with correlation fields.
	logEvent("error", "DLQ publish failed after retries",
		"trace_id", strings.TrimSpace(string(headerValue(msg.Headers, "trace_id"))),
		"table", strings.TrimSpace(string(headerValue(msg.Headers, "table"))),
		"dlq_topic", dlqTopic,
		"topic", srcTopic,
		"partition", msg.Partition,
		"offset", msg.Offset,
		"reason", err.Error(),
		"publish_error", lastErr.Error(),
	)
	return lastErr
}

func serveMetrics(port int, metrics *Metrics) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		metrics.mu.Lock()
		lastErr := metrics.lastError
		lastTopic := metrics.lastTopic
		metrics.mu.Unlock()
		// Freshness signal for stall detection: seconds since the last message was
		// processed. -1 means "not yet processed anything" (fresh/idle worker). A
		// consumer that ALSO knows there is Kafka lag (the orchestrator sentinel,
		// which reads authoritative broker-side consumer-group lag) can combine the
		// two — high staleness AND positive lag ⇒ the worker is wedged, not idle.
		// The serve goroutine is independent of the (possibly blocked) consume loop,
		// so this stays observable even while a write is stuck.
		lastProc := atomic.LoadInt64(&metrics.lastProcessedAtUnixMs)
		secondsSinceLastProcessed := int64(-1)
		if lastProc > 0 {
			secondsSinceLastProcessed = (time.Now().UTC().UnixMilli() - lastProc) / 1000
		}
		out := map[string]interface{}{
			"started_at":                 metrics.startedAt.UTC().Format(time.RFC3339),
			"processed":                  atomic.LoadUint64(&metrics.processed),
			"skipped":                    atomic.LoadUint64(&metrics.skipped),
			"failed":                     atomic.LoadUint64(&metrics.failed),
			"last_error":                 lastErr,
			"dlq_publish_failures_total": atomic.LoadUint64(&metrics.dlqPublishFailures),
			"dlq_routed_total":           atomic.LoadUint64(&metrics.dlqRouted),
			"kafka": map[string]interface{}{
				"topic":                        lastTopic,
				"partition":                    atomic.LoadInt64(&metrics.lastKafkaPartition),
				"offset":                       atomic.LoadInt64(&metrics.lastKafkaOffset),
				"lag":                          atomic.LoadInt64(&metrics.lastKafkaLag),
				"last_committed_offset":        atomic.LoadInt64(&metrics.lastCommittedOffset),
				"last_processed_at_ms":         atomic.LoadInt64(&metrics.lastProcessedAtUnixMs),
				"last_committed_at_ms":         atomic.LoadInt64(&metrics.lastCommittedAtUnixMs),
				"seconds_since_last_processed": secondsSinceLastProcessed,
			},
			"cdc_source": map[string]interface{}{
				"last_source_ts_ms": atomic.LoadInt64(&metrics.lastSourceTSUnixMs),
				"last_lag_ms":       atomic.LoadInt64(&metrics.lastSourceLagMs),
			},
			// CDC-specific counters
			"cdc": map[string]interface{}{
				"inserts": atomic.LoadUint64(&metrics.cdcInserts),
				"updates": atomic.LoadUint64(&metrics.cdcUpdates),
				"deletes": atomic.LoadUint64(&metrics.cdcDeletes),
				"reads":   atomic.LoadUint64(&metrics.cdcReads),
			},
		}
		b, _ := json.Marshal(out)
		_, _ = w.Write(b)
	})

	addr := fmt.Sprintf("0.0.0.0:%d", port)
	// Explicit timeouts (Slowloris / slow-body hardening — gosec G114).
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      120 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	_ = srv.ListenAndServe()
}

func loadMax(m *sync.Map, table string) int64 {
	if v, ok := m.Load(table); ok {
		if n, ok := v.(int64); ok {
			return n
		}
	}
	return 0
}

// addAndLoad atomically adds `delta` to the value stored at `table` and
// returns the new total. Used to keep cumulative read/written counters per
// (execution, table) since the orchestrator's per-batch sm.BatchOffset is
// not a reliable cumulative offset (it stays at 0 with cursor paging).
func addAndLoad(m *sync.Map, table string, delta int64) int64 {
	for {
		cur := loadMax(m, table)
		next := cur + delta
		if m.CompareAndSwap(table, cur, next) {
			return next
		}
		// On miss (key absent), Store and read back.
		if _, loaded := m.LoadOrStore(table, next); !loaded {
			return next
		}
	}
}

// meterBytes gates the (cheap) byte metering; default ON, disable with RSYNC_METER_BYTES=false.
var meterBytes = strings.ToLower(strings.TrimSpace(os.Getenv("RSYNC_METER_BYTES"))) != "false"

// jsonByteLen returns the JSON-serialized byte length of v (0 on error / when metering off).
func jsonByteLen(v interface{}) int64 {
	if !meterBytes || v == nil {
		return 0
	}
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return int64(len(b))
}

// committedBatchBytes is the logical committed byte volume of a batch write: the exact
// on-wire object size for a blob (sm.Size, no structured rows), else the JSON size of the
// committed rows (scaled by the confirmed fraction on a partial commit).
func committedBatchBytes(sm *SinkMessage, writtenRows int64) int64 {
	if !meterBytes || sm == nil {
		return 0
	}
	if len(sm.Data) == 0 && sm.Size > 0 {
		return sm.Size // blob/object-storage lane: exact size
	}
	n := int64(len(sm.Data))
	if n == 0 {
		return 0
	}
	total := jsonByteLen(sm.Data)
	if writtenRows > 0 && writtenRows < n {
		return total * writtenRows / n
	}
	return total
}

// cdcRowBytes is the logical byte volume of one committed CDC change: the after-image for
// create/read/update, the before-image (key) for delete.
func cdcRowBytes(sm *SinkMessage) int64 {
	if !meterBytes || sm == nil {
		return 0
	}
	if strings.ToLower(strings.TrimSpace(sm.CDCOp)) == "d" {
		if b := jsonByteLen(sm.Before); b > 0 {
			return b
		}
		return jsonByteLen(sm.After)
	}
	if b := jsonByteLen(sm.After); b > 0 {
		return b
	}
	return jsonByteLen(sm.Before)
}

// ledgerRetryBackoff returns a capped exponential backoff for ledger retries.
// Linear 2s growth feels reasonable for the first minute and we then cap at
// 60s to avoid silent multi-minute pauses on each retry. Exposed as a
// package-level helper so the dedup-SELECT branch and the ack-INSERT branch
// share the same policy.
func ledgerRetryBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Duration(attempt) * 2 * time.Second
	if d > 60*time.Second {
		d = 60 * time.Second
	}
	return d
}

// ctxAwareSleep sleeps for d or until ctx is cancelled. Returns true if the
// sleep completed normally, false if it was cut short by ctx cancellation.
// Use it instead of time.Sleep on any retry loop that might run for more
// than a few seconds — without it, SIGTERM during a sustained Postgres
// outage would be blocked by an unbreakable Sleep.
func ctxAwareSleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// retryUntilSuccessOrCancel runs attempt repeatedly until it returns nil or
// ctx is cancelled. Each failed attempt is logged with the configured
// backoff. Used to drive the ack-insert retry loop and unit-tested with a
// fake `attempt` closure (no DB required).
func retryUntilSuccessOrCancel(ctx context.Context, label string, metrics *Metrics, attempt func(context.Context) error) error {
	n := 0
	for {
		err := attempt(ctx)
		if err == nil {
			return nil
		}
		n++
		if metrics != nil {
			atomic.AddUint64(&metrics.failed, 1)
			metrics.setErr(err)
		}
		backoff := ledgerRetryBackoff(n)
		logf("warning",
			"%s failed (attempt %d, sleeping %s before retry — partition paused until ledger recovers): %v",
			label, n, backoff, err)
		if !ctxAwareSleep(ctx, backoff) {
			return ctx.Err()
		}
	}
}

// persistBatchAckToPostgresWithRetry retries the ack insert until either it
// lands or ctx is cancelled. We retry forever (no max attempts) because the
// destination write already happened — committing the Kafka offset without
// a corresponding ledger row would silently break exactly-once on the next
// redelivery. A sustained Postgres outage will park the partition; on
// recovery we land the ack and proceed.
//
// Returns nil on success, ctx.Err() if shutdown interrupted us.
func persistBatchAckToPostgresWithRetry(
	ctx context.Context,
	db *sql.DB,
	sm *SinkMessage,
	writtenRows, rowsRead int64,
	destKey, kafkaTopic string,
	partition int,
	offset int64,
	metrics *Metrics,
) error {
	return retryUntilSuccessOrCancel(ctx, "ack persist", metrics, func(c context.Context) error {
		return persistBatchAckToPostgres(c, db, sm, writtenRows, rowsRead, destKey, kafkaTopic, partition, offset)
	})
}

// persistBatchAckToPostgres writes a batch ACK to the durable Postgres ledger.
// Idempotency is on (pipeline_id, execution_id, table_name, kafka_topic,
// kafka_partition, kafka_offset) — migration 054. The old (batch_offset)
// key collided across cursor-paged batches.
func persistBatchAckToPostgres(ctx context.Context, db *sql.DB, sm *SinkMessage, writtenRows, rowsRead int64, destKey, kafkaTopic string, partition int, offset int64) error {
	query := `
		INSERT INTO pipeline_batch_acks (
			pipeline_id, execution_id, table_name, batch_offset,
			rows_written, rows_read, dest_key, storage_type,
			kafka_topic, kafka_partition, kafka_offset, acked_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (pipeline_id, execution_id, table_name, kafka_topic, kafka_partition, kafka_offset) DO NOTHING
	`
	_, err := db.ExecContext(ctx, query,
		sm.PipelineID, sm.ExecutionID, sm.Table, sm.BatchOffset,
		writtenRows, rowsRead, destKey, sm.StorageType,
		kafkaTopic, partition, offset, time.Now().UTC(),
	)
	return err
}

// maxAckErrLen caps the sink error string stored in pipeline_batch_acks.last_error
// so a pathological error message cannot bloat the ledger row.
const maxAckErrLen = 2000

// persistNegativeBatchAckToPostgresWithRetry records a NEGATIVE ack (rows_written=0
// + last_error) when a batch is DLQ'd after exhausting retries. It uses the same
// retry-forever-until-cancel semantics as the positive ack: the ledger row must be
// durable BEFORE the Kafka offset is committed, otherwise the executor's landed-row
// reconciliation cannot distinguish a genuine destination drop from a slow/absent
// ledger — the empty-"Data transfer failed:" bug. Returns nil on success, ctx.Err()
// if shutdown interrupted us (caller then leaves the offset uncommitted).
func persistNegativeBatchAckToPostgresWithRetry(
	ctx context.Context,
	db *sql.DB,
	sm *SinkMessage,
	rowsRead int64,
	errMsg, kafkaTopic string,
	partition int,
	offset int64,
	metrics *Metrics,
) error {
	return retryUntilSuccessOrCancel(ctx, "negative ack persist", metrics, func(c context.Context) error {
		return persistNegativeBatchAckToPostgres(c, db, sm, rowsRead, errMsg, kafkaTopic, partition, offset)
	})
}

// persistNegativeBatchAckToPostgres writes a NEGATIVE batch ack: rows_written=0 and
// last_error carrying the sink's failure reason. The executor's sumBatchAcks then
// reports (landed=0, ackRows>0), tripping the existing silent-drop hard-fail path
// (executor.go) with the REAL reason surfaced in /state instead of an empty error.
// The idempotency key matches the positive ack so a redelivery cannot double-insert.
func persistNegativeBatchAckToPostgres(ctx context.Context, db *sql.DB, sm *SinkMessage, rowsRead int64, errMsg, kafkaTopic string, partition int, offset int64) error {
	if len(errMsg) > maxAckErrLen {
		errMsg = errMsg[:maxAckErrLen]
	}
	query := `
		INSERT INTO pipeline_batch_acks (
			pipeline_id, execution_id, table_name, batch_offset,
			rows_written, rows_read, dest_key, storage_type,
			kafka_topic, kafka_partition, kafka_offset, last_error, acked_at
		) VALUES ($1, $2, $3, $4, 0, $5, '', $6, $7, $8, $9, $10, $11)
		ON CONFLICT (pipeline_id, execution_id, table_name, kafka_topic, kafka_partition, kafka_offset) DO NOTHING
	`
	_, err := db.ExecContext(ctx, query,
		sm.PipelineID, sm.ExecutionID, sm.Table, sm.BatchOffset,
		rowsRead, sm.StorageType,
		kafkaTopic, partition, offset, errMsg, time.Now().UTC(),
	)
	return err
}

// pgAckLedger* bound the multi-row ack INSERT so cols*chunk stays under
// PostgreSQL's 65535 bind-parameter cap (16 * 4000 = 64000).
const (
	pgAckLedgerCols  = 16
	pgAckLedgerChunk = 4000
)

// persistCDCAcksBatch writes the best-effort audit-ledger rows for an ENTIRE
// flushed CDC batch in one (chunked) multi-row INSERT instead of one remote
// round-trip per message. This is the high-volume throughput fix: the previous
// per-message loop issued len(batch) serial INSERTs to the remote ledger
// Postgres on the consume goroutine (~1000 round-trips ≈ tens of seconds per
// flush), starving Kafka consumption. Semantics are identical to the
// per-message persistCDCAckToPostgres it replaces on the batch lanes:
// writtenRows is 1 per message (one CDC event = one row), the same
// (pipeline_id, execution_id, table_name, kafka_topic, kafka_partition,
// kafka_offset) idempotency key is used, and the write stays best-effort /
// non-fatal — exactly-once is enforced by _rsync_cdc_offsets committed in the
// destination upsert transaction, NOT by this ledger.
func persistCDCAcksBatch(ctx context.Context, db *sql.DB, sms []*SinkMessage, messages []kafka.Message, destKey string) error {
	if db == nil {
		return nil
	}
	// CDC keys the ledger by execution_id == pipeline_id (parseCDCMessage), and no
	// executions row exists for that id — so every ack INSERT below used to fail
	// fk_batch_acks_execution (23503) and pipeline_batch_acks stayed permanently empty
	// for CDC. Pre-create the same synthetic executions row the transform-log path
	// already relies on. Once per flush (not per message), ON CONFLICT DO NOTHING, and
	// still best-effort: the helper only logs, and the ack write stays non-fatal.
	if len(sms) > 0 && sms[0] != nil {
		ensureExecutionRowForCDCAudit(ctx, db, sms[0].PipelineID, sms[0].ExecutionID)
	}
	var firstErr error
	for _, ins := range buildCDCAckInserts(sms, messages, destKey, time.Now().UTC()) {
		if _, err := db.ExecContext(ctx, ins.query, ins.args...); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// cdcAckInsert is one chunked multi-row INSERT (statement + bind args).
type cdcAckInsert struct {
	query string
	args  []interface{}
}

// buildCDCAckInserts renders a flushed CDC batch's best-effort audit rows into
// chunked multi-row INSERTs, each ≤ pgAckLedgerChunk rows so cols*rows stays
// under PostgreSQL's 65535 bind-parameter cap. Pure (no I/O) for testability.
// rows_written is fixed at 1 per message, matching the per-message path it
// replaces, and the same idempotency key is used so redelivery is a no-op.
func buildCDCAckInserts(sms []*SinkMessage, messages []kafka.Message, destKey string, now time.Time) []cdcAckInsert {
	n := len(sms)
	if n == 0 || len(messages) < n {
		return nil
	}
	const prefix = `INSERT INTO pipeline_batch_acks (` +
		`pipeline_id, execution_id, table_name, batch_offset, ` +
		`rows_written, rows_read, dest_key, storage_type, ` +
		`kafka_topic, kafka_partition, kafka_offset, ` +
		`cdc_op, cdc_tx_id, cdc_lsn, cdc_source_ts, acked_at) VALUES `
	const suffix = ` ON CONFLICT (pipeline_id, execution_id, table_name, kafka_topic, kafka_partition, kafka_offset) DO NOTHING`
	out := make([]cdcAckInsert, 0, (n+pgAckLedgerChunk-1)/pgAckLedgerChunk)
	for start := 0; start < n; start += pgAckLedgerChunk {
		end := start + pgAckLedgerChunk
		if end > n {
			end = n
		}
		tuples := make([]string, 0, end-start)
		args := make([]interface{}, 0, (end-start)*pgAckLedgerCols)
		for i := start; i < end; i++ {
			sm := sms[i]
			m := messages[i]
			base := len(args)
			ph := make([]string, pgAckLedgerCols)
			for c := 0; c < pgAckLedgerCols; c++ {
				ph[c] = "$" + strconv.Itoa(base+c+1)
			}
			tuples = append(tuples, "("+strings.Join(ph, ",")+")")
			args = append(args,
				sm.PipelineID, sm.ExecutionID, sm.Table, m.Offset,
				int64(1), sm.RowCount, destKey, "cdc",
				m.Topic, m.Partition, m.Offset,
				sm.CDCOp, sm.TxID, sm.LSN, sm.SourceTS, now)
		}
		out = append(out, cdcAckInsert{query: prefix + strings.Join(tuples, ",") + suffix, args: args})
	}
	return out
}

// persistCDCAckToPostgres writes a CDC ACK to the durable Postgres ledger
func persistCDCAckToPostgres(ctx context.Context, db *sql.DB, sm *SinkMessage, writtenRows int64, destKey, kafkaTopic string, partition int, offset int64) error {
	query := `
		INSERT INTO pipeline_batch_acks (
			pipeline_id, execution_id, table_name, batch_offset,
			rows_written, rows_read, dest_key, storage_type,
			kafka_topic, kafka_partition, kafka_offset,
			cdc_op, cdc_tx_id, cdc_lsn, cdc_source_ts, acked_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
		ON CONFLICT (pipeline_id, execution_id, table_name, kafka_topic, kafka_partition, kafka_offset) DO NOTHING
	`
	// For CDC, use kafka offset as batch_offset for legacy readers; the
	// real idempotency key is (pipeline_id, execution_id, table_name,
	// kafka_topic, kafka_partition, kafka_offset).
	_, err := db.ExecContext(ctx, query,
		sm.PipelineID, sm.ExecutionID, sm.Table, offset,
		writtenRows, sm.RowCount, destKey, "cdc",
		kafkaTopic, partition, offset,
		sm.CDCOp, sm.TxID, sm.LSN, sm.SourceTS, time.Now().UTC(),
	)
	return err
}

// topicResolveTimeout bounds how long the worker waits for its subscribed topics
// to exist before joining the consumer group. On a broker with
// auto.create.topics.enable=true (the Kafka default) the first probe creates the
// topic and the wait ends in well under a second, so this ceiling only bites when
// auto-create is off and we are genuinely waiting on the producer. Override with
// RSYNC_SINK_TOPIC_WAIT_SECONDS. Clamped to [0s, 300s]; 0 disables the wait.
func topicResolveTimeout() time.Duration {
	n := 30
	if v := strings.TrimSpace(os.Getenv("RSYNC_SINK_TOPIC_WAIT_SECONDS")); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil {
			n = parsed
		}
	}
	if n < 0 {
		n = 0
	}
	if n > 300 {
		n = 300
	}
	return time.Duration(n) * time.Second
}

// waitForTopicPartitions blocks until every topic reports at least one partition,
// or until timeout elapses.
//
// Why this exists (KI-CDC-1, round 2). Debezium creates its per-table topics
// LAZILY — only when the first change event is emitted — so the sink routinely
// joins the consumer group BEFORE the topic exists. kafka-go computes a
// generation's partition assignment exactly once, at join time, and a missing
// topic yields ZERO assigned partitions. The only thing that can rescue such a
// member is the partition watcher noticing a *change* in the partition count
// (consumergroup.go: `if len(ops) != oParts { return }` ends the generation and
// forces a rejoin).
//
// That rescue is a coin flip, which is why WatchPartitionChanges alone did not
// fix this. The watcher's own readPartitions is a Metadata request, and with
// auto.create.topics.enable=true it CREATES the topic as a side effect. If that
// first call then returns cleanly with 1 partition, the watcher records
// oParts=1 and settles into its ticker — the count never changes again, no
// rebalance is ever triggered, and the member stays subscribed to nothing for
// the life of the process while Debezium happily produces into the topic the
// watcher just created. Every streamed row is silently dropped, with no error
// logged anywhere. If instead that first call errors, the watcher returns, the
// generation ends, and the rejoin picks up the partition — so the same run
// passes most of the time and fails the rest.
//
// Resolving the topics here, before kafka.NewReader, removes the race outright:
// by the time we join, the topics exist and makeAssignments always sees their
// partitions. WatchPartitionChanges stays enabled as a second line of defence
// for partitions added to an already-assigned topic later.
func waitForTopicPartitions(broker string, topics []string, timeout time.Duration) {
	if len(topics) == 0 || timeout <= 0 {
		return
	}

	pending := make(map[string]bool, len(topics))
	for _, t := range topics {
		if tt := strings.TrimSpace(t); tt != "" {
			pending[tt] = true
		}
	}
	if len(pending) == 0 {
		return
	}

	started := time.Now()
	deadline := started.Add(timeout)
	for attempt := 0; ; attempt++ {
		if conn, err := dialBroker("tcp", broker); err == nil {
			for t := range pending {
				// A Metadata request. Under auto-create this is also what brings the
				// topic into being, which is precisely the point: we would rather
				// create it here, deterministically, than have the partition watcher
				// create it behind our back after the assignment is already fixed.
				if parts, perr := conn.ReadPartitions(t); perr == nil && len(parts) > 0 {
					delete(pending, t)
				}
			}
			conn.Close()
		}

		if len(pending) == 0 {
			if attempt > 0 {
				logf("info", "all %d subscribed topic(s) resolved after %s; joining consumer group",
					len(topics), time.Since(started).Round(time.Millisecond))
			}
			return
		}

		if !time.Now().Before(deadline) {
			missing := make([]string, 0, len(pending))
			for t := range pending {
				missing = append(missing, t)
			}
			sort.Strings(missing)
			// Loud on purpose: the failure this guards against is otherwise SILENT —
			// a member assigned no partitions logs nothing at all while data flows past.
			logf("warning", "topic(s) [%s] still report no partitions after %s; joining the group anyway. "+
				"If the producer creates them later, this member may be assigned no partitions until a "+
				"rebalance occurs and rows will be silently skipped.",
				strings.Join(missing, ", "), timeout)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Consumer stall watchdog — KI-CDC-SINK-EMPTY-ASSIGNMENT
// ---------------------------------------------------------------------------
//
// kafka-go computes a group member's partition assignment exactly once, when it
// joins (consumergroup.go assignTopicPartitions -> conn.readPartitions). That read
// is allowed to come back empty: an UnknownTopicOrPartition — or a plain empty
// result — is swallowed on the stated grounds that "a topic watcher can trigger a
// rebalance when the topic comes into being". When it does come back empty the
// member is assigned nothing, logs `received empty assignments` followed by
// `subscribed to topics and partitions: map[]`, and then sits in a Stable group for
// the life of the process while records flow past it. Nothing is logged at error
// level, the process stays alive and its metrics port keeps answering — so every
// liveness check we have (including start_sink's readiness probe, which only checks
// process-alive plus metrics-port-answering) reports a healthy sink that is
// consuming precisely nothing. The pipeline then fails much later and much further
// away, as "destination never received the rows".
//
// WatchPartitionChanges does not rescue it: the watcher forces a rejoin only when
// the partition COUNT CHANGES (`if len(ops) != oParts { return }`), so when its
// first observation already matches the final count there is never a change to
// react to.
//
// Reproduced directly against a live broker with a standalone kafka-go harness: the
// wedge hits roughly one join in five, and at wedge time the broker reports the
// topic present with its full partition count. So this is NOT "the topic did not
// exist yet" — which is why resolving topics before NewReader does not fix it
// (measured: 1/6 wedges without the pre-resolve, 4/18 with). Closing the reader and
// re-joining the same group recovered 4 of 4 wedged trials, so forcing a rejoin is
// the remedy the evidence actually supports.
//
// This watchdog forces that rejoin the cheapest safe way: it exits the process and
// lets the Python supervisor respawn it (_supervisor_loop -> _restart_worker) with a
// fresh join. Exiting is equivalent to recreating the reader here because a worker
// process owns exactly one reader, and it reuses the supervisor's already-tested
// backoff and crash-loop breaker instead of adding a second restart path.

const (
	// 60s: twice minStallWatchdogSeconds, and well inside the 180s that the CDC e2e
	// allows for the first row to land, so a wedged run recovers rather than fails.
	defaultStallWatchdogSeconds = 60
	// Below RAPID_RESTART_WINDOW_SECONDS (30s, connector.py) every watchdog restart
	// would count as a rapid restart and trip the supervisor's crash-loop breaker
	// after MAX_RAPID_RESTARTS, turning a recoverable wedge into a dead worker.
	minStallWatchdogSeconds   = 30
	maxStallWatchdogSeconds   = 3600
	stallWatchdogPollInterval = 15 * time.Second
	// The consume loop polls with a 1s timeout, so a live loop refreshes lastPoll far
	// more often than this. A gap this wide means the loop is blocked inside message
	// handling (a slow destination write), not starved of messages — and killing that
	// worker would abort in-flight work rather than fix anything.
	consumeLoopAliveWindow = 30 * time.Second
)

// gStallRestart records that shutdown was initiated by the stall watchdog rather
// than by SIGTERM, so main can exit non-zero and be respawned.
var gStallRestart atomic.Bool

// stallWatchdogTimeout is how long the consumer may fetch nothing, while records are
// waiting, before we force a rejoin. Override with RSYNC_SINK_STALL_WATCHDOG_SECONDS;
// 0 (or negative) disables the watchdog, other values are clamped to
// [minStallWatchdogSeconds, maxStallWatchdogSeconds]. Mirrors destHTTPTimeout().
func stallWatchdogTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv("RSYNC_SINK_STALL_WATCHDOG_SECONDS"))
	if raw == "" {
		return defaultStallWatchdogSeconds * time.Second
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return defaultStallWatchdogSeconds * time.Second
	}
	if n <= 0 {
		return 0
	}
	if n < minStallWatchdogSeconds {
		n = minStallWatchdogSeconds
	}
	if n > maxStallWatchdogSeconds {
		n = maxStallWatchdogSeconds
	}
	return time.Duration(n) * time.Second
}

// consumerActivity is the consume loop's heartbeat, read by the watchdog goroutine.
// The two clocks are deliberately separate: pollTick says "the loop is running",
// messageTick says "the loop is receiving data". Only their combination tells a
// wedged member apart from a busy one.
type consumerActivity struct {
	lastPollUnixNano    atomic.Int64
	lastMessageUnixNano atomic.Int64
}

func newConsumerActivity(now time.Time) *consumerActivity {
	a := &consumerActivity{}
	a.lastPollUnixNano.Store(now.UnixNano())
	// Seeded with the start time so the first stall window is also the grace period
	// for joining and receiving the first record.
	a.lastMessageUnixNano.Store(now.UnixNano())
	return a
}

func (a *consumerActivity) pollTick()    { a.lastPollUnixNano.Store(time.Now().UnixNano()) }
func (a *consumerActivity) messageTick() { a.lastMessageUnixNano.Store(time.Now().UnixNano()) }

func (a *consumerActivity) lastPoll() time.Time {
	return time.Unix(0, a.lastPollUnixNano.Load())
}

func (a *consumerActivity) lastMessage() time.Time {
	return time.Unix(0, a.lastMessageUnixNano.Load())
}

// stallSnapshot is everything the restart decision depends on, gathered at one
// instant so the decision itself is a pure function and can be unit-tested.
type stallSnapshot struct {
	now         time.Time
	lastPoll    time.Time
	lastMessage time.Time
	dataWaiting bool
}

// shouldRestartForStall reports whether the three facts that together cannot
// describe a healthy sink all hold: the consume loop is alive and polling, it has
// fetched nothing for the whole stall window, and the broker still holds records the
// group has not consumed. Any one of them alone is normal.
func shouldRestartForStall(s stallSnapshot, stall time.Duration) bool {
	if stall <= 0 {
		return false
	}
	if !s.dataWaiting {
		return false // idle stream, not a wedge
	}
	if s.now.Sub(s.lastMessage) < stall {
		return false // consuming
	}
	if s.now.Sub(s.lastPoll) > consumeLoopAliveWindow {
		return false // blocked in message handling, not starved
	}
	return true
}

// unconsumedRecords reports whether groupID still has records waiting on any
// partition of topics, along with a human-readable detail for the log.
//
// The floor it compares the end offset against is the group's committed offset. When
// the group has never committed (CommittedOffset < 0 — exactly the wedge case, since
// a member that consumed nothing committed nothing) the floor falls back to where
// this reader would have started: the partition's first offset when reading from the
// earliest offset, otherwise the end offset observed when the watchdog started, held
// in baseline. Without that fallback a `latest` reader would look wedged whenever the
// topic merely had pre-existing data.
func unconsumedRecords(ctx context.Context, broker, groupID string, topics []string, fromEarliest bool, baseline map[string]int64) (bool, string, error) {
	conn, err := dialBroker("tcp", broker)
	if err != nil {
		return false, "", fmt.Errorf("dial %s: %w", broker, err)
	}
	parts, err := conn.ReadPartitions(topics...)
	closeErr := conn.Close()
	if err != nil {
		return false, "", fmt.Errorf("read partitions: %w", err)
	}
	if closeErr != nil {
		logf("warn", "stall watchdog: closing metadata connection: %v", closeErr)
	}
	if len(parts) == 0 {
		// No partitions at all: there is nothing to consume, so nothing is waiting.
		return false, "", nil
	}

	offsetReq := &kafka.ListOffsetsRequest{Addr: brokerAddr(broker), Topics: map[string][]kafka.OffsetRequest{}}
	committedReq := &kafka.OffsetFetchRequest{Addr: brokerAddr(broker), GroupID: groupID, Topics: map[string][]int{}}
	for _, p := range parts {
		offsetReq.Topics[p.Topic] = append(offsetReq.Topics[p.Topic], kafka.FirstOffsetOf(p.ID), kafka.LastOffsetOf(p.ID))
		committedReq.Topics[p.Topic] = append(committedReq.Topics[p.Topic], p.ID)
	}

	client := &kafka.Client{Addr: brokerAddr(broker), Transport: kafkaTransport(), Timeout: 10 * time.Second}
	offsets, err := client.ListOffsets(ctx, offsetReq)
	if err != nil {
		return false, "", fmt.Errorf("list offsets: %w", err)
	}
	committed, err := client.OffsetFetch(ctx, committedReq)
	if err != nil {
		return false, "", fmt.Errorf("offset fetch: %w", err)
	}

	committedBy := map[string]int64{}
	for topic, cps := range committed.Topics {
		for _, cp := range cps {
			if cp.Error != nil {
				continue
			}
			committedBy[fmt.Sprintf("%s/%d", topic, cp.Partition)] = cp.CommittedOffset
		}
	}

	for topic, pos := range offsets.Topics {
		for _, po := range pos {
			key := fmt.Sprintf("%s/%d", topic, po.Partition)
			if _, seen := baseline[key]; !seen {
				baseline[key] = po.LastOffset
			}
			floor, ok := committedBy[key]
			if !ok || floor < 0 {
				if fromEarliest {
					floor = po.FirstOffset
				} else {
					floor = baseline[key]
				}
			}
			if po.LastOffset > floor {
				return true, fmt.Sprintf("%s end=%d floor=%d committed=%d", key, po.LastOffset, floor, committedBy[key]), nil
			}
		}
	}
	return false, "", nil
}

// watchConsumerStall polls until the consumer is provably wedged, then calls onStall
// once and returns. It is deliberately quiet on the healthy path: the only broker
// round-trips happen after the cheap in-process clocks already say the consumer has
// fetched nothing for a whole stall window.
func watchConsumerStall(ctx context.Context, broker, groupID string, topics []string, fromEarliest bool, act *consumerActivity, stall time.Duration, onStall func(detail string)) {
	if stall <= 0 || act == nil || onStall == nil || len(topics) == 0 || strings.TrimSpace(groupID) == "" {
		return
	}
	logEvent("info", "consumer stall watchdog armed",
		"stall_seconds", int(stall.Seconds()), "group", groupID, "topics", strings.Join(topics, ","))

	baseline := map[string]int64{}
	ticker := time.NewTicker(stallWatchdogPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		now := time.Now()
		lastMessage := act.lastMessage()
		if now.Sub(lastMessage) < stall {
			continue // consuming; don't touch the broker at all
		}

		waiting, detail, err := unconsumedRecords(ctx, broker, groupID, topics, fromEarliest, baseline)
		if err != nil {
			// Can't see the broker, so can't distinguish a wedge from an outage.
			// Restarting on a metadata blip would be worse than waiting.
			logf("warn", "stall watchdog: cannot read broker offsets, not restarting: %v", err)
			continue
		}
		if shouldRestartForStall(stallSnapshot{now: now, lastPoll: act.lastPoll(), lastMessage: lastMessage, dataWaiting: waiting}, stall) {
			onStall(detail)
			return
		}
	}
}
