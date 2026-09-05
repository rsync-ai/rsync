package executor

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	log "github.com/sirupsen/logrus"
)

// landedReconcileDecision is the pure classification of an ack-ledger reconcile
// result for a sink-dispatched batch run. Extracted from the executor batch
// completion path so the total-drop / partial / unverified decision is
// unit-testable without a live ledger. The object/blob lane keeps its own mirror
// in blob_lane.go.
type landedReconcileDecision struct {
	AckEvidencedDrop     bool   // total OR evidenced-partial drop → terminal failure
	UnverifiedCompletion bool   // ambiguous acks (object lane / receipt shortfall) → fail-soft
	LandedRows           int64  // landed count fed to the source-probe guard
	SinkErr              string // sink error (defaulted for the zero-ack kafka case)
	Reason               string // human-readable reason; set for AckEvidencedDrop and for the receipt-shortfall UnverifiedCompletion
	Status               string // executor status when AckEvidencedDrop (total vs partial)
}

// classifyLandedReconcile decides how a sink-dispatched batch run resolves from
// its destination-truth ack ledger (pipeline_batch_acks).
//
//   - landed==0 && ackRows>0: POSITIVE drop evidence — the sink wrote acks that
//     sum to 0 landed. Fail closed (silent_drop_detected).
//   - landed==0 && ackRows==0: no acks within the reconcile deadline. A
//     SINK-dispatched run ALWAYS persists an ack per batch — positive on a
//     successful write, negative (rows_written=0) on poison after the in-place
//     retries are exhausted. This holds for BOTH sink lanes: direct-kafka
//     (directKafkaMessages>0) AND the MinIO claim-check lane (minioFilesCreated>0)
//     — both go through kafka-mcp-sink, which writes to pipeline_batch_acks. So
//     zero acks + zero landed for either sink lane is a stall or total drop (the
//     per-write timeout × retry budget can exceed the reconcile deadline, so the
//     negative acks have not landed yet): fail CLOSED with an actionable reason
//     instead of the empty-error "unverified_completion" (which resolves
//     downstream to EXECUTION_FAILED with no reason and reads as a stuck run).
//     KI-NLCHAT-TYPECONVERT-FALSE-SUCCESS: previously only directKafkaMessages>0
//     failed closed, so a claim-check-lane batch whose consumer transform DLQ'd
//     (0 landed, 0 acks) fell into fail-soft and reported `completed` — a false
//     success with silent data loss. Both lanes now fail closed. The object/blob
//     DESTINATION lane keeps its own fail-soft mirror in blob_lane.go (a different
//     function that never reaches here). Failing closed is the safe posture: an
//     unnecessary failure is recoverable (warehouse reload is idempotent); a
//     silent stuck run is not.
//   - landed<dispatched: partial shortfall, resolved in three tiers because a
//     smaller landed count is AMBIGUOUS. (1) NEGATIVE-ack poison evidence
//     (sinkErr!="") → permanent DLQ loss → fail CLOSED (silent_partial_drop_detected).
//     (2) No sink error but a RECEIPT shortfall (received = SUM(rows_read) < dispatched)
//     → the sink pulled fewer rows than we dispatched, so the missing rows never
//     reached the destination lane (upstream transform DLQ / dropped batch) or the
//     sink hasn't drained → landing UNCONFIRMED → fail-soft (unverified_completion),
//     NOT a silent success. (3) received>=dispatched but landed<dispatched → benign
//     upsert/dedup merge (the dest wrote fewer rows than it received) → stays success,
//     leaving d.LandedRows=dispatched so the downstream 5% guard doesn't false-fail it.
//     The received>0 guard on tier 2 falls back to tier-3 behavior for a sink build
//     that doesn't populate rows_read (version skew), so healthy runs are never
//     false-flagged.
//
// `dispatched` is the row count for the WHOLE execution, not for the current
// executor dispatch: a chunked continuation re-invokes this executor stage under
// the SAME execution id, and `landed`/`received` are ledger sums over that id, so
// feeding a single chunk's count here let earlier chunks' acks cover a later
// chunk's drop (KI-SILENTDROP-ACK-SUM-SPANS-CHUNKS). The caller derives it from
// the producer outbox — see sumDispatchedRows. `outboxBatches` is that outbox's
// row count and stands in for the per-dispatch kafka/MinIO counters when the
// final dispatch of a chunked run exported nothing of its own (a table whose row
// count lands exactly on a chunk boundary resumes, reads 0 rows, and would
// otherwise report success having verified nothing).
func classifyLandedReconcile(dispatched, landed, received int64, ackRows, directKafkaMessages, minioFilesCreated, outboxBatches int, sinkErr, executionID string) landedReconcileDecision {
	d := landedReconcileDecision{LandedRows: dispatched, SinkErr: sinkErr, Status: "silent_drop_detected"}
	dispatchedViaSink := directKafkaMessages > 0 || minioFilesCreated > 0 || outboxBatches > 0
	switch {
	case landed == 0 && ackRows > 0:
		d.LandedRows = 0
		d.AckEvidencedDrop = true
		d.Reason = fmt.Sprintf("destination silently dropped all rows: dispatched %d, ack ledger confirmed 0 landed across %d ack batches (execution %s)", dispatched, ackRows, executionID)
	case landed == 0 && ackRows == 0:
		if dispatchedViaSink {
			d.LandedRows = 0
			d.AckEvidencedDrop = true
			if strings.TrimSpace(d.SinkErr) == "" {
				d.SinkErr = "no destination acks recorded within the reconcile deadline — sink stalled or destination unreachable"
			}
			d.Reason = fmt.Sprintf("destination write unconfirmed: dispatched %d rows via sink but no acks were recorded within the reconcile deadline (execution %s) — failing closed as a total drop", dispatched, executionID)
		} else {
			d.UnverifiedCompletion = true
		}
	case landed < dispatched:
		if strings.TrimSpace(sinkErr) != "" {
			// Tier 1: NEGATIVE-ack poison evidence → the sink DLQ'd at least one batch
			// → those rows are permanently lost. Fail closed.
			d.LandedRows = landed
			d.AckEvidencedDrop = true
			d.Status = "silent_partial_drop_detected"
			d.Reason = fmt.Sprintf("destination partially dropped rows: dispatched %d, ack ledger confirmed only %d landed after the sink DLQ'd at least one batch (execution %s)", dispatched, landed, executionID)
		} else if received > 0 && received < dispatched {
			// Tier 2: receipt shortfall, no sink error. SUM(rows_read) shows the sink
			// received only `received` of `dispatched`, so the missing rows never
			// reached the destination lane or the sink hasn't drained. Landing is
			// UNCONFIRMED → fail-soft, not silent success. Benign upsert/dedup is
			// EXCLUDED (it has received==dispatched → tier 3). Keep d.LandedRows=dispatched
			// so the downstream 5% probe guard doesn't also fire on the same run.
			d.UnverifiedCompletion = true
			d.Reason = fmt.Sprintf("destination landing unconfirmed for %d of %d dispatched rows: the sink recorded %d received / %d landed within the reconcile deadline (execution %s) — rows never reached the destination lane or the sink has not drained", dispatched-received, dispatched, received, landed, executionID)
		}
		// else (tier 3) received>=dispatched: benign upsert/dedup undercount → stays success.
	}
	if d.AckEvidencedDrop && strings.TrimSpace(d.SinkErr) != "" {
		d.Reason += fmt.Sprintf("; sink error: %s", d.SinkErr)
	}
	return d
}

// reconcileLandedRows returns the number of rows the kafka-mcp-sink actually
// WROTE to the destination for this run, as recorded in the destination-truth
// ledger pipeline_batch_acks (the sink INSERTs one ack row per batch with
// rows_written = real destination writes; see kafka-sink-worker
// persistBatchAckToPostgres).
//
// Why this exists: on the kafka-sink batch path the executor only knows how many
// rows it READ + DISPATCHED to Kafka (totalRows). The sink writes to the
// destination asynchronously, so "dispatch finished" != "rows landed". When the
// sink crash-loops (e.g. an ownership-gate refusal, a bad credential, a schema
// error) the executor used to report success with 0 rows landed — the
// premature-completion bug. By summing the ack ledger we get destination truth.
//
// It bound-WAITS for the sink to drain: it polls until the landed count reaches
// `expected` (dispatched totalRows) or the deadline elapses, exiting early the
// moment they match so a healthy fast sink adds no latency. Returns
// (landedRows, receivedRows, ackRowCount, lastErr). receivedRows = SUM(rows_read),
// the count the sink actually pulled from Kafka (used to tell a benign upsert
// undercount from a real receipt shortfall). ackRowCount == 0 means the sink wrote
// NO ack rows at all for this run — either a non-ledger path or the ledger is
// unavailable — in which case the caller must NOT treat 0 landed as a drop
// (fail-soft: avoid false failures on paths that don't use this ledger).
func (a *Agent) reconcileLandedRows(ctx context.Context, pipelineID, executionID string, expected int64, deadline time.Duration) (landed, received int64, ackRows int, lastErr string) {
	if a.db == nil || strings.TrimSpace(pipelineID) == "" || strings.TrimSpace(executionID) == "" {
		if a.db != nil && strings.TrimSpace(pipelineID) != "" && strings.TrimSpace(executionID) == "" {
			log.WithField("pipeline_id", pipelineID).Warn("landed-row reconciliation bypassed: empty execution_id — destination landing cannot be verified")
		}
		return 0, 0, 0, ""
	}
	start := time.Now()
	pollEvery := 2 * time.Second
	for {
		l, r, n, e := sumBatchAcks(ctx, a.db, pipelineID, executionID)
		landed, received, ackRows, lastErr = l, r, n, e
		// Drained: the sink has acked at least as many rows as we dispatched.
		if expected > 0 && landed >= expected {
			log.WithFields(log.Fields{
				"pipeline_id":  pipelineID,
				"execution_id": executionID,
				"dispatched":   expected,
				"landed":       landed,
				"received":     received,
				"ack_batches":  ackRows,
				"waited_ms":    time.Since(start).Milliseconds(),
			}).Info("landed-row reconciliation: destination drained, counts reconciled")
			return landed, received, ackRows, lastErr
		}
		if time.Since(start) >= deadline {
			log.WithFields(log.Fields{
				"pipeline_id":  pipelineID,
				"execution_id": executionID,
				"dispatched":   expected,
				"landed":       landed,
				"received":     received,
				"ack_batches":  ackRows,
				"waited_ms":    time.Since(start).Milliseconds(),
			}).Warn("landed-row reconciliation: deadline reached before destination fully drained")
			return landed, received, ackRows, lastErr
		}
		select {
		case <-ctx.Done():
			return landed, received, ackRows, lastErr
		case <-time.After(pollEvery):
		}
	}
}

// batchAckReconcileDeadline is the max time reconcileLandedRows waits for the
// sink to drain before giving up and using whatever landed so far. Generous by
// default so a healthy-but-slow sink reaches the dispatched count and exits
// early (the wait short-circuits the instant landed >= dispatched). Override
// with BATCH_ACK_RECONCILE_DEADLINE_SECONDS.
func batchAckReconcileDeadline() time.Duration {
	const def = 120 * time.Second
	v := strings.TrimSpace(os.Getenv("BATCH_ACK_RECONCILE_DEADLINE_SECONDS"))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return time.Duration(n) * time.Second
}

// sumBatchAcks returns (SUM(rows_written), SUM(rows_read), COUNT(*), MAX(last_error))
// from the destination-truth ack ledger for a run. landed = rows the sink WROTE to the
// destination; received = rows the sink PULLED from Kafka for this run (batch sizes,
// recorded on BOTH positive and negative acks). COUNT(*)==0 distinguishes "no acks yet /
// path doesn't use the ledger" from "acks exist but wrote 0 rows". lastErr is the sink's
// failure reason recorded on a NEGATIVE ack (rows_written=0) when a batch was DLQ'd; it is
// "" on healthy runs (positive acks leave last_error NULL). The received total is the
// discriminator that separates a benign upsert/dedup undercount (received==dispatched, dest
// merged) from a real receipt shortfall (received<dispatched, rows never reached the sink).
func sumBatchAcks(ctx context.Context, db *sql.DB, pipelineID, executionID string) (landed, received int64, ackRows int, lastErr string) {
	var l sql.NullInt64
	var r sql.NullInt64
	var n sql.NullInt64
	var e sql.NullString
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(rows_written), 0), COALESCE(SUM(rows_read), 0), COUNT(*), MAX(last_error)
		FROM pipeline_batch_acks
		WHERE pipeline_id = $1 AND execution_id = $2
	`, pipelineID, executionID).Scan(&l, &r, &n, &e)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id":  pipelineID,
			"execution_id": executionID,
		}).Warn("landed-row reconciliation: ack-ledger query failed (treating as no acks)")
		return 0, 0, 0, ""
	}
	return l.Int64, r.Int64, int(n.Int64), e.String
}

// sumDispatchedRows returns (SUM(row_count), COUNT(*)) from the PRODUCER outbox
// pipeline_batch_outbox — the dispatch-side twin of pipeline_batch_acks, keyed on
// the same (pipeline_id, execution_id) pair, one row per batch handed to Kafka on
// either sink lane (inline chunks and MinIO claim-check references both go through
// produceBatchWithOutbox). Migration 037 created it for exactly this: "compare
// outbox vs acks to find missing batches".
//
// Why the caller needs it — KI-SILENTDROP-ACK-SUM-SPANS-CHUNKS. The executor's
// in-process totalRows counts only the CURRENT dispatch, but a chunked
// continuation re-invokes the executor stage under the SAME execution id
// (PolicyCodeNeedsContinuation → deterministic re-dispatch), so sumBatchAcks sums
// the acks of EVERY chunk. Comparing one chunk's dispatch against every chunk's
// acks let four chunks' worth of acks cover the fifth chunk's total drop. Summing
// the outbox puts BOTH sides of the comparison on the whole execution.
//
// Only 'produced'/'acked' rows count: 'pending' means the Kafka produce was never
// confirmed and 'failed' means it errored, so neither reached the bus. An
// UNDERCOUNT is deliberately safe — the caller floors this at its own in-process
// count, so an unmigrated table, a failed query, or a produce stuck at 'pending'
// degrades to exactly today's behavior and can never weaken the check. An
// OVERCOUNT cannot force a false hard failure either: the only shortfall tier that
// fails closed needs a negative ack from the sink itself (sinkErr != ""), which is
// evidence independent of this denominator; a denominator-only shortfall lands in
// the fail-soft receipt-shortfall tier.
func sumDispatchedRows(ctx context.Context, db *sql.DB, pipelineID, executionID string) (dispatched int64, batches int) {
	if db == nil || strings.TrimSpace(pipelineID) == "" || strings.TrimSpace(executionID) == "" {
		return 0, 0
	}
	var s, n sql.NullInt64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(row_count), 0), COUNT(*)
		FROM pipeline_batch_outbox
		WHERE pipeline_id = $1 AND execution_id = $2
		  AND status IN ('produced', 'acked')
	`, pipelineID, executionID).Scan(&s, &n)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id":  pipelineID,
			"execution_id": executionID,
		}).Warn("dispatch reconciliation: producer-outbox query failed (falling back to this dispatch's own row count)")
		return 0, 0
	}
	return s.Int64, int(n.Int64)
}

// reconcileInputs resolves the two things the landing reconcile needs from the
// executor's per-dispatch counters plus the execution-wide producer outbox:
// `dispatched`, the denominator both reconcileLandedRows and
// classifyLandedReconcile are measured against, and `viaSink`, whether this
// execution put anything on the sink at all (a non-sink transfer must keep its
// prior behaviour and never be reconciled against an ack ledger it does not use).
//
// Two rules, each with a failure mode behind it:
//
//   - dispatched = max(thisDispatchRows, outboxRows). The outbox spans the whole
//     execution and thisDispatchRows spans one chunk, so the max is the
//     whole-execution count whenever the outbox is healthy — and degrades to
//     exactly the pre-fix denominator when the outbox is empty, unmigrated,
//     lagging, or its query failed. The floor is what makes the change unable to
//     WEAKEN the check, only strengthen it.
//   - viaSink is true if EITHER the per-dispatch counters or the outbox says so.
//     The counters alone are 0 on the final dispatch of a chunked run whose table
//     ended exactly on a chunk boundary (it resumes, reads 0 rows, exports
//     nothing) — which skipped verification for the entire run and reported
//     success having checked nothing.
func reconcileInputs(thisDispatchRows int64, directKafkaMessages, minioFilesCreated int, outboxRows int64, outboxBatches int) (dispatched int64, viaSink bool) {
	dispatched = thisDispatchRows
	if outboxRows > dispatched {
		dispatched = outboxRows
	}
	return dispatched, directKafkaMessages > 0 || minioFilesCreated > 0 || outboxBatches > 0
}

// SilentDropResult is the outcome of post-flight verification.
//
// "Silent drop" is the failure class where:
//   - the executor finished without raising an error
//   - readRows or writtenRows reports 0 (or fewer than expected)
//   - the source actually had data to deliver
//
// Before Phase 1 this case reported Status="success" with 0 rows landed —
// the UI showed green, the destination was empty, operators discovered it
// only when they noticed missing data. The check below catches it before
// the executor's "success" response leaves the building.
type SilentDropResult struct {
	// SilentDrop is true when the check determined data was lost without
	// the executor noticing.
	SilentDrop bool

	// Status is the executor status to use when SilentDrop is true.
	// Empty when SilentDrop is false.
	Status string

	// Reason is a human-readable explanation surfaced to the operator.
	// The string is what they see when they click into the failed run.
	Reason string

	// SourceRowCount is the count we observed at the source (-1 if we
	// couldn't probe). Useful for the run-detail page.
	SourceRowCount int64
}

// CheckForSilentDrop compares what the executor read from source against
// what it wrote to destination, plus an optional source-count probe when
// both came back zero. Returns SilentDrop=true if data is being lost.
//
// readRows / writtenRows come from the in-process counters maintained by
// executeDataTransfer's batch loop. They're authoritative for what the
// process saw; the source-count probe is a safety net for the path where
// the source connector returned an empty result that doesn't reflect the
// actual upstream state (the failure mode the Shopify→Postgres test
// surfaced).
func CheckForSilentDrop(
	ctx context.Context,
	agent SilentDropProber,
	sourceType string,
	sourceConfig map[string]string,
	sourceTable string,
	readRows int64,
	writtenRows int64,
) SilentDropResult {
	// Case 1: read N rows but wrote 0. Destination silently dropped.
	if readRows > 0 && writtenRows == 0 {
		return SilentDropResult{
			SilentDrop:     true,
			Status:         "silent_drop_detected",
			Reason:         fmt.Sprintf("destination silently dropped all rows: read %d from source, wrote 0 to destination", readRows),
			SourceRowCount: readRows,
		}
	}

	// Case 2: partial drop (>5% of source rows missing at destination).
	// Strict threshold is configurable per-pipeline later; 5% is the
	// initial safe default that doesn't false-positive on small batch
	// timing jitter.
	if readRows > 0 && writtenRows < (readRows*95/100) {
		return SilentDropResult{
			SilentDrop:     true,
			Status:         "silent_partial_drop_detected",
			Reason:         fmt.Sprintf("destination silently dropped some rows: read %d from source, wrote %d to destination (%.1f%% missing)", readRows, writtenRows, float64(readRows-writtenRows)*100.0/float64(readRows)),
			SourceRowCount: readRows,
		}
	}

	// Case 3: both zero. Could be a legitimate empty source OR the
	// connector returning empty when source actually has data. Probe
	// the source once for a non-empty signal.
	if readRows == 0 && writtenRows == 0 {
		count, ok := probeSourceCount(ctx, agent, sourceType, sourceConfig, sourceTable)
		if ok && count > 0 {
			return SilentDropResult{
				SilentDrop:     true,
				Status:         "silent_drop_detected",
				Reason:         fmt.Sprintf("connector returned 0 rows but source actually has %d %s rows — likely a connector bug or auth/scope misconfiguration", count, sourceTable),
				SourceRowCount: count,
			}
		}
		// Source genuinely empty (or probe failed). Honest empty success.
		return SilentDropResult{SilentDrop: false}
	}

	// Happy path: counts agree (within tolerance).
	return SilentDropResult{SilentDrop: false}
}

// SilentDropProber is the narrow interface CheckForSilentDrop needs from
// the executor agent. Defined here (rather than on *Agent directly) so the
// check is unit-testable without standing up a real Agent.
type SilentDropProber interface {
	probeSource(ctx context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error)
}

// probeSourceCount asks the source MCP for a row count using whichever
// method the connector supports. Returns (count, ok). When ok=false the
// caller treats the probe as inconclusive (genuine empty source assumed).
//
// We deliberately never issue a SELECT COUNT(*): no connector exposes a
// `query` operation — every metadata.json, relational ones included, lists
// only test_connection / validate_config / discover_schema /
// get_capabilities / export / import_data — so such a call resolves to a
// nonexistent `<conn>_query` tool and yields nothing but an "Unknown tool"
// log (KI-SILENTDROP-QUERY-FALLBACK). The single probe is the connector's
// standard discover_schema with include_row_counts=true, honored by every
// relational connector; when the response carries no count the probe is
// inconclusive and the caller assumes a genuine empty source.
func probeSourceCount(
	ctx context.Context,
	agent SilentDropProber,
	sourceType string,
	sourceConfig map[string]string,
	sourceTable string,
) (int64, bool) {
	// Try discover_schema first — works for both SaaS and DB connectors
	// that report row_count metadata.
	resp, err := agent.probeSource(ctx, mcp.ExecuteRequest{
		Connector: sourceType,
		Operation: "discover_schema",
		Config:    sourceConfig,
		Params: map[string]interface{}{
			"config":             sourceConfig,
			"include_row_counts": true,
		},
	})
	if err == nil && resp != nil && resp.Success {
		if count, ok := extractTableRowCount(resp.Result, sourceTable); ok {
			return count, true
		}
	}

	return 0, false
}

// extractTableRowCount pulls a row count for “tableName“ out of a
// discover_schema response. Tries every shape the various connectors
// return — flat “tables“ list, nested under data, etc.
func extractTableRowCount(result map[string]interface{}, tableName string) (int64, bool) {
	if result == nil {
		return 0, false
	}
	tname := strings.ToLower(strings.TrimSpace(tableName))
	var tablesRaw interface{}
	if v, ok := result["tables"]; ok {
		tablesRaw = v
	} else if data, ok := result["data"].(map[string]interface{}); ok {
		if v, ok := data["tables"]; ok {
			tablesRaw = v
		}
	}
	tablesList, ok := tablesRaw.([]interface{})
	if !ok {
		return 0, false
	}
	for _, t := range tablesList {
		tMap, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		name, _ := tMap["name"].(string)
		if strings.ToLower(strings.TrimSpace(name)) != tname {
			continue
		}
		for _, key := range []string{"row_count", "rowCount", "total_count", "count"} {
			if v, ok := tMap[key]; ok {
				if n, ok := toInt64(v); ok {
					return n, true
				}
			}
		}
	}
	return 0, false
}

func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float32:
		return int64(n), true
	case float64:
		return int64(n), true
	}
	return 0, false
}
