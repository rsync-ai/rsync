package main

// Regression coverage for KI-CDC-ACKLEDGER-FK-MISMATCH.
//
// CDC keys its stats by execution_id == pipeline_id (a stable streaming counter
// bucket, set in parseCDCMessage), but pipeline_batch_acks.execution_id carries a
// real FK to executions(id) (fk_batch_acks_execution, migration 043:68-69). No
// executions row with id = pipeline_id exists for a normal CDC pipeline, so every
// ack INSERT failed with 23503 and pipeline_batch_acks stayed permanently empty
// for CDC — per-batch log noise forever, and any future audit view reading that
// table would show zero acks while data was actually flowing.
//
// The fix reuses the guard the sibling transform-log path already relies on:
// pre-insert the synthetic executions row before the ack write. These tests pin
// the three properties that make it correct rather than merely present — the
// guard runs BEFORE the acks, runs ONCE per flush, and refuses to write garbage
// ids.
//
// Uses a minimal in-process database/sql driver (no live Postgres, no new module
// dependency), like negative_ack_test.go — but this one records EVERY query, not
// just the last, because ordering and call count are the assertions.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/segmentio/kafka-go"
)

// ackFKConn records every ExecContext query in order. Implementing
// driver.ExecerContext routes db.ExecContext straight here, bypassing Prepare.
type ackFKConn struct {
	mu      sync.Mutex
	queries []string
	// failAcks makes the pipeline_batch_acks INSERTs fail the way the FK
	// violation used to, so a test can prove the guard still runs first and the
	// error is still returned (non-fatal at the call site).
	failAcks error
}

func (c *ackFKConn) Prepare(string) (driver.Stmt, error) { return nil, io.EOF }
func (c *ackFKConn) Close() error                        { return nil }
func (c *ackFKConn) Begin() (driver.Tx, error)           { return nil, io.EOF }

func (c *ackFKConn) ExecContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, q)
	if c.failAcks != nil && strings.Contains(q, "pipeline_batch_acks") {
		return nil, c.failAcks
	}
	return driver.RowsAffected(1), nil
}

func (c *ackFKConn) recorded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.queries))
	copy(out, c.queries)
	return out
}

// ackFKDriver hands out ackFKActiveConn. sql.Register is process-global and
// one-shot, hence both the sync.Once and a driver name distinct from
// negative_ack_test.go's "negack_capture".
type ackFKDriver struct{}

func (ackFKDriver) Open(string) (driver.Conn, error) { return ackFKActiveConn, nil }

var (
	ackFKRegisterOnce sync.Once
	ackFKActiveConn   *ackFKConn
)

func newAckFKDB(t *testing.T, conn *ackFKConn) *sql.DB {
	t.Helper()
	ackFKRegisterOnce.Do(func() { sql.Register("ackfk_capture", ackFKDriver{}) })
	ackFKActiveConn = conn
	db, err := sql.Open("ackfk_capture", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	return db
}

// The CDC convention under test: execution_id == pipeline_id, and both must pass
// looksLikeUUID (36 chars, dashes at 8/13/18/23) or the guard declines to write.
const (
	ackFKPipelineID = "11111111-1111-1111-1111-111111111111"
	ackFKExecID     = ackFKPipelineID
)

func ackFKMessages(t *testing.T, n int) ([]*SinkMessage, []kafka.Message) {
	t.Helper()
	sms := make([]*SinkMessage, n)
	msgs := make([]kafka.Message, n)
	for i := 0; i < n; i++ {
		sms[i] = &SinkMessage{
			PipelineID:  ackFKPipelineID,
			ExecutionID: ackFKExecID,
			Table:       "public.orders",
			StorageType: "cdc",
			BatchOffset: int64(i),
			RowCount:    1,
		}
		msgs[i] = kafka.Message{Topic: "cdc.public.orders", Partition: i % 3, Offset: int64(i)}
	}
	return sms, msgs
}

func countMatching(queries []string, needle string) int {
	n := 0
	for _, q := range queries {
		if strings.Contains(q, needle) {
			n++
		}
	}
	return n
}

// TestPersistCDCAcksBatch_EnsuresExecutionRowBeforeAckInsert is the core guard:
// the synthetic executions row must be written BEFORE the first
// pipeline_batch_acks INSERT. Order is the whole fix — a guard that ran after
// the acks would leave the FK violation exactly as it was.
func TestPersistCDCAcksBatch_EnsuresExecutionRowBeforeAckInsert(t *testing.T) {
	conn := &ackFKConn{}
	db := newAckFKDB(t, conn)
	defer db.Close()

	sms, msgs := ackFKMessages(t, 3)
	if err := persistCDCAcksBatch(context.Background(), db, sms, msgs, "dest"); err != nil {
		t.Fatalf("persistCDCAcksBatch: %v", err)
	}

	queries := conn.recorded()
	if len(queries) < 2 {
		t.Fatalf("expected an executions guard plus at least one ack insert, got %d query/queries: %v", len(queries), queries)
	}

	execIdx, ackIdx := -1, -1
	for i, q := range queries {
		if execIdx == -1 && strings.Contains(q, "INSERT INTO executions") {
			execIdx = i
		}
		if ackIdx == -1 && strings.Contains(q, "pipeline_batch_acks") {
			ackIdx = i
		}
	}
	if execIdx == -1 {
		t.Fatalf("no INSERT INTO executions guard was issued — the FK violation is unfixed: %v", queries)
	}
	if ackIdx == -1 {
		t.Fatalf("no pipeline_batch_acks insert was issued: %v", queries)
	}
	if execIdx > ackIdx {
		t.Fatalf("executions guard ran at %d, AFTER the ack insert at %d — ordering is the fix", execIdx, ackIdx)
	}
	// The guard must be idempotent: a redelivered flush re-runs it.
	if !strings.Contains(queries[execIdx], "ON CONFLICT (id) DO NOTHING") {
		t.Fatalf("executions guard is not idempotent, missing ON CONFLICT: %q", queries[execIdx])
	}
}

// TestPersistCDCAcksBatch_EnsuresExecutionRowOncePerFlush pins the "once per
// flush, not once per message" property. CDC flushes are hot — one extra INSERT
// per message would add a write per change event for the life of every stream.
func TestPersistCDCAcksBatch_EnsuresExecutionRowOncePerFlush(t *testing.T) {
	conn := &ackFKConn{}
	db := newAckFKDB(t, conn)
	defer db.Close()

	sms, msgs := ackFKMessages(t, 25)
	if err := persistCDCAcksBatch(context.Background(), db, sms, msgs, "dest"); err != nil {
		t.Fatalf("persistCDCAcksBatch: %v", err)
	}

	if got := countMatching(conn.recorded(), "INSERT INTO executions"); got != 1 {
		t.Fatalf("expected exactly 1 executions guard for a 25-message flush, got %d", got)
	}
}

// TestEnsureExecutionRowForCDCAudit_RefusesNonUUIDIdentifiers proves the guard
// will not manufacture rows from junk. `executions.id` is a uuid column, so a
// non-UUID id would itself error (22P02) — turning a fixed FK warning into a new
// one. The helper must short-circuit before touching the DB.
func TestEnsureExecutionRowForCDCAudit_RefusesNonUUIDIdentifiers(t *testing.T) {
	cases := []struct {
		name        string
		pipelineID  string
		executionID string
	}{
		{name: "both blank", pipelineID: "", executionID: ""},
		{name: "blank pipeline", pipelineID: "", executionID: ackFKExecID},
		{name: "blank execution", pipelineID: ackFKPipelineID, executionID: ""},
		{name: "non-uuid pipeline", pipelineID: "pipe-1", executionID: ackFKExecID},
		{name: "non-uuid execution", pipelineID: ackFKPipelineID, executionID: "exec-1"},
		{name: "right length wrong dashes", pipelineID: ackFKPipelineID, executionID: "111111111111111111111111111111111111"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := &ackFKConn{}
			db := newAckFKDB(t, conn)
			defer db.Close()

			ensureExecutionRowForCDCAudit(context.Background(), db, tc.pipelineID, tc.executionID)

			if got := conn.recorded(); len(got) != 0 {
				t.Fatalf("expected no query for %s, got %v", tc.name, got)
			}
		})
	}

	t.Run("nil db does not panic", func(t *testing.T) {
		ensureExecutionRowForCDCAudit(context.Background(), nil, ackFKPipelineID, ackFKExecID)
	})

	t.Run("valid uuids do write", func(t *testing.T) {
		conn := &ackFKConn{}
		db := newAckFKDB(t, conn)
		defer db.Close()

		ensureExecutionRowForCDCAudit(context.Background(), db, ackFKPipelineID, ackFKExecID)

		got := conn.recorded()
		if len(got) != 1 || !strings.Contains(got[0], "INSERT INTO executions") {
			t.Fatalf("expected one executions insert, got %v", got)
		}
	})
}

// TestPersistCDCAcksBatch_StaysBestEffortWhenAcksStillFail keeps the severity
// contract intact: this ledger is explicitly an audit trail for CDC (real
// exactly-once lives in _rsync_cdc_offsets, committed with the data write). If
// the ack INSERT still fails for some other reason, the guard must still have
// run and the error must be RETURNED (the caller logs it non-fatally) rather
// than panicking or being swallowed here.
func TestPersistCDCAcksBatch_StaysBestEffortWhenAcksStillFail(t *testing.T) {
	conn := &ackFKConn{failAcks: io.ErrUnexpectedEOF}
	db := newAckFKDB(t, conn)
	defer db.Close()

	sms, msgs := ackFKMessages(t, 2)
	err := persistCDCAcksBatch(context.Background(), db, sms, msgs, "dest")
	if err == nil {
		t.Fatal("expected the ack insert error to be returned so the caller can log it")
	}
	if got := countMatching(conn.recorded(), "INSERT INTO executions"); got != 1 {
		t.Fatalf("guard must run even when the ack write fails, got %d executions inserts", got)
	}
}

// TestPersistCDCAcksBatch_NoMessagesIsANoOp guards the empty-flush path: with
// nothing to ack there is nothing to anchor, so the guard must not create a
// synthetic executions row (which would otherwise appear for pipelines that
// never streamed a single event).
func TestPersistCDCAcksBatch_NoMessagesIsANoOp(t *testing.T) {
	conn := &ackFKConn{}
	db := newAckFKDB(t, conn)
	defer db.Close()

	if err := persistCDCAcksBatch(context.Background(), db, nil, nil, "dest"); err != nil {
		t.Fatalf("persistCDCAcksBatch(nil): %v", err)
	}
	if got := conn.recorded(); len(got) != 0 {
		t.Fatalf("expected no queries for an empty flush, got %v", got)
	}
}
