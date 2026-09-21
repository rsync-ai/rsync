package main

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"time"
)

// Restart-safe per-table CDC counters.
//
// The sink reports a CDC table's inserts/updates/deletes as running totals kept in
// process memory, and the api-gateway projector stores them with GREATEST(stored,
// reported). A worker restart used to start the totals at zero again, so after a
// restart the stored count only moved once the new process had counted past it, and
// every row delivered before the restart was lost from the total. Replayed offsets
// (a restart before the Kafka commit) were counted a second time instead.
//
// pipeline_batch_acks already records every CDC offset once, keyed by Kafka topic,
// partition and offset. The counters now follow it: at start they load the
// ledger's totals, and a flush adds only the messages its ledger INSERT was the
// first to record (persistCDCAcksBatch / persistCDCAckToPostgres).
//
// Not covered: the bytes counter (the ledger has no bytes column), and messages whose
// ledger write failed — they are still counted live, as before, but a restart cannot
// recover them.

// cdcSeedTimeout bounds the startup ledger read so a slow database delays the
// worker by at most this long; on timeout the counters start at zero as before.
const cdcSeedTimeout = 30 * time.Second

// cdcStatsExecutionID is the execution_id CDC messages carry, and so the one their
// ledger rows and TABLE_STATS are keyed by. In CDC mode, or when the worker has no
// execution id, it is the pipeline id: a stable bucket for a stream that never
// ends. parseCDCMessage and the startup seed both use it so they cannot disagree.
func cdcStatsExecutionID(cfg *WorkerConfig) string {
	pipelineID := strings.TrimSpace(cfg.PipelineID)
	executionID := strings.TrimSpace(cfg.ExecutionID)
	if strings.EqualFold(strings.TrimSpace(cfg.SinkMode), "cdc") || executionID == "" {
		return pipelineID
	}
	return executionID
}

// cdcAckCounts turns a single-row ledger write into "count this message": yes when
// the row was new, and yes when the ledger could not answer, which keeps the
// counters moving while the ledger is down.
func cdcAckCounts(added bool, err error) bool {
	return added || err != nil
}

// cdcCounterSeedQuery totals a pipeline's positive CDC acks per table and op.
// Negative acks (last_error set) are rows that never landed, so they are excluded.
const cdcCounterSeedQuery = `
	SELECT table_name, LOWER(TRIM(COALESCE(cdc_op, ''))), COUNT(*)
	FROM pipeline_batch_acks
	WHERE pipeline_id = $1 AND execution_id = $2
	  AND storage_type = 'cdc' AND last_error IS NULL
	GROUP BY 1, 2`

// seedCDCCountersFromLedger loads the ledger's per-table totals into the empty
// counters: c and r into inserts, u into updates, d into deletes. Best-effort: on
// any error it logs and leaves the counters as they were.
func seedCDCCountersFromLedger(ctx context.Context, db *sql.DB, pipelineID, executionID string, inserts, updates, deletes *sync.Map) {
	pipelineID = strings.TrimSpace(pipelineID)
	executionID = strings.TrimSpace(executionID)
	if db == nil || !looksLikeUUID(pipelineID) || !looksLikeUUID(executionID) {
		return
	}
	qctx, cancel := context.WithTimeout(ctx, cdcSeedTimeout)
	defer cancel()

	rows, err := db.QueryContext(qctx, cdcCounterSeedQuery, pipelineID, executionID)
	if err != nil {
		logf("warning", "cdc counter seed skipped, counts start at zero (ledger read failed: %v)", err)
		return
	}
	defer rows.Close()

	type seed struct {
		m     *sync.Map
		table string
		n     int64
	}
	var seeds []seed
	for rows.Next() {
		var table, op string
		var n int64
		if err := rows.Scan(&table, &op, &n); err != nil {
			logf("warning", "cdc counter seed skipped, counts start at zero (ledger row unreadable: %v)", err)
			return
		}
		var m *sync.Map
		switch op {
		case "c", "r":
			m = inserts
		case "u":
			m = updates
		case "d":
			m = deletes
		default:
			continue
		}
		seeds = append(seeds, seed{m: m, table: table, n: n})
	}
	if err := rows.Err(); err != nil {
		logf("warning", "cdc counter seed skipped, counts start at zero (ledger read failed: %v)", err)
		return
	}
	// Apply only after the whole result was read, so a read that fails halfway
	// cannot leave some tables seeded and others not.
	tables := map[string]struct{}{}
	for _, s := range seeds {
		incrementCounter(s.m, s.table, s.n)
		tables[s.table] = struct{}{}
	}
	logEvent("info", "seeded cdc table counters from ack ledger", "tables", len(tables))
}
