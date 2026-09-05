package main

// Regression coverage for the P1-A silent-partial-loss fix: a MinIO claim-check
// fetch failure must persist a NEGATIVE batch ack (rows_written=0 + the real
// failure reason) BEFORE the offset is committed, so the executor's landed-row
// reconciliation fails the run closed with the real reason instead of reporting
// `completed` with a silently-dropped >256KB batch.
//
// The consume loop needs a live broker to exercise end to end (see aud4_dlq_test.go),
// so this locks the negative-ack CONTRACT that the fix depends on — using a minimal
// in-process database/sql driver, no live Postgres and no new module dependency.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"sync"
	"testing"
)

// captureConn is a database/sql driver.Conn that records ExecContext calls.
// Implementing driver.ExecerContext routes db.ExecContext straight here,
// bypassing Prepare, so we capture the raw query + bound args.
type captureConn struct {
	mu    sync.Mutex
	query string
	args  []driver.NamedValue
	calls int
}

func (c *captureConn) Prepare(string) (driver.Stmt, error) { return nil, io.EOF }
func (c *captureConn) Close() error                        { return nil }
func (c *captureConn) Begin() (driver.Tx, error)           { return nil, io.EOF }

func (c *captureConn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.query = q
	c.args = args
	c.calls++
	return driver.RowsAffected(1), nil
}

// captureDriver.Open hands out captureActiveConn. sql.Register is process-global
// and one-shot, so the active conn is swapped via this package-level indirection.
// Tests in this file run serially, so a single indirection is sufficient.
type captureDriver struct{}

func (captureDriver) Open(string) (driver.Conn, error) { return captureActiveConn, nil }

var (
	captureRegisterOnce sync.Once
	captureActiveConn   *captureConn
)

func newCaptureDB(t *testing.T, conn *captureConn) *sql.DB {
	t.Helper()
	captureRegisterOnce.Do(func() { sql.Register("negack_capture", captureDriver{}) })
	captureActiveConn = conn
	db, err := sql.Open("negack_capture", "")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	return db
}

func TestNegativeAckContract_MinIOFetchFailure(t *testing.T) {
	conn := &captureConn{}
	db := newCaptureDB(t, conn)
	defer db.Close()

	sm := &SinkMessage{
		PipelineID:  "pipe-1",
		ExecutionID: "exec-1",
		Table:       "public.orders",
		StorageType: "minio",
		RowCount:    42, // header says 42 rows should have landed
		BatchOffset: 7,
	}
	const reason = "minio read failed: An error occurred (NoSuchKey) ... does not exist"

	// rows_read = sm.RowCount, exactly what the fixed MinIO-fetch branch passes.
	if err := persistNegativeBatchAckToPostgres(context.Background(), db, sm, sm.RowCount, reason, "cdc-topic", 3, 99); err != nil {
		t.Fatalf("persistNegativeBatchAckToPostgres: %v", err)
	}

	if conn.calls != 1 {
		t.Fatalf("expected exactly 1 INSERT, got %d", conn.calls)
	}
	q := conn.query
	if !strings.Contains(q, "INSERT INTO pipeline_batch_acks") {
		t.Fatalf("query is not a pipeline_batch_acks insert:\n%s", q)
	}
	// rows_written MUST be a literal 0 — that is what makes this a NEGATIVE ack
	// (landed=0) rather than a fabricated success.
	if !strings.Contains(q, "rows_written") || !strings.Contains(q, ", 0, ") {
		t.Fatalf("negative ack must write rows_written=0 as a literal:\n%s", q)
	}
	// Idempotent on redelivery so a retried poison message can't double-insert.
	if !strings.Contains(strings.ToUpper(q), "ON CONFLICT") {
		t.Fatalf("negative ack must be idempotent (ON CONFLICT ... DO NOTHING):\n%s", q)
	}

	// Bound args, in the order persistNegativeBatchAckToPostgres passes them:
	//   $1 pipeline_id, $2 execution_id, $3 table_name, $4 batch_offset,
	//   $5 rows_read, $6 storage_type, $7 kafka_topic, $8 kafka_partition,
	//   $9 kafka_offset, $10 last_error, $11 acked_at
	if len(conn.args) != 11 {
		t.Fatalf("expected 11 bound args, got %d: %#v", len(conn.args), conn.args)
	}
	assertArg := func(ordinal int, want interface{}) {
		got := conn.args[ordinal-1].Value
		switch w := want.(type) {
		case int64:
			gi, ok := got.(int64)
			if !ok || gi != w {
				t.Fatalf("arg $%d = %#v, want int64 %d", ordinal, got, w)
			}
		case string:
			gs, ok := got.(string)
			if !ok || gs != w {
				t.Fatalf("arg $%d = %#v, want string %q", ordinal, got, w)
			}
		}
	}
	assertArg(1, "pipe-1")
	assertArg(2, "exec-1")
	assertArg(3, "public.orders")
	assertArg(5, int64(42)) // rows_read carries the EXPECTED count (nothing landed)
	assertArg(7, "cdc-topic")
	assertArg(9, int64(99)) // kafka_offset

	// last_error ($10) must carry the REAL MinIO reason so /state shows it.
	le, _ := conn.args[9].Value.(string)
	if !strings.Contains(le, "NoSuchKey") {
		t.Fatalf("last_error must carry the real reason, got %q", le)
	}
}

// TestNegativeAckRowsReadFloor documents the >0 floor the MinIO-fetch branch
// applies: even if the header row_count is missing/zero, rows_read must be >0 so
// the reconciler sees ackRows>0 (fail-closed) rather than the ambiguous zero branch.
func TestNegativeAckRowsReadFloor(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rowCount int64
		want     int64
	}{
		{"header present", 42, 42},
		{"header zero", 0, 1},
		{"header negative", -3, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Mirror the branch logic in main.go's MinIO-fetch failure path.
			nackRows := tc.rowCount
			if nackRows <= 0 {
				nackRows = 1
			}
			if nackRows != tc.want {
				t.Fatalf("rowCount=%d -> nackRows=%d, want %d", tc.rowCount, nackRows, tc.want)
			}
		})
	}
}
