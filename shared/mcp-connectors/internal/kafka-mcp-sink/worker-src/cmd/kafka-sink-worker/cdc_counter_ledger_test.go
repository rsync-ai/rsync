package main

// Coverage for restart-safe per-table CDC counters (cdc_counter_ledger.go).
//
// The bug: the per-table insert/update/delete totals lived only in process memory
// and the projector keeps GREATEST(stored, reported), so a sink restart threw away
// every row counted before it (a 5,000-row table showed 1,773) and a replay after
// the restart counted the same offsets twice. The counters now follow
// pipeline_batch_acks: seeded from it at start, and a flush adds only the messages
// whose ledger row it was the first to write.
//
// Uses ackFKConn (ack_ledger_fk_guard_test.go), which applies the ledger's unique
// key the way ON CONFLICT DO NOTHING does.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// ledgerTestMessages builds CDC messages for offsets [from, to) on public.orders, with
// the same partition rule as ackFKMessages so a replayed offset has the same key.
func ledgerTestMessages(from, to int, op string) ([]*SinkMessage, []kafka.Message) {
	var sms []*SinkMessage
	var msgs []kafka.Message
	for i := from; i < to; i++ {
		sms = append(sms, &SinkMessage{
			PipelineID:  ackFKPipelineID,
			ExecutionID: ackFKExecID,
			Table:       "public.orders",
			StorageType: "cdc",
			RowCount:    1,
			CDCOp:       op,
		})
		msgs = append(msgs, kafka.Message{Topic: "cdc.public.orders", Partition: i % 3, Offset: int64(i)})
	}
	return sms, msgs
}

// okDestination answers every destination tool call with success.
type okDestination struct{}

func (okDestination) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"result":{"success":true}}`)),
		Request:    r,
	}, nil
}

type cdcTestCounters struct {
	inserts, updates, deletes *sync.Map
}

func newCDCTestCounters() cdcTestCounters {
	return cdcTestCounters{inserts: &sync.Map{}, updates: &sync.Map{}, deletes: &sync.Map{}}
}

// flushObjectBatch runs one successful cdcObjectBatcher.flushBatch — the object
// storage lane — against pgDB (nil: no ledger) and the given counters.
func flushObjectBatch(t *testing.T, pgDB *sql.DB, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message) {
	t.Helper()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: "cdc.public.orders"})
	defer reader.Close()
	b := &cdcObjectBatcher{
		cfg:          &WorkerConfig{PipelineID: ackFKPipelineID, SinkMode: "cdc"},
		destType:     "gcs",
		destCfg:      map[string]interface{}{},
		params:       cdcBatchingParams{maxRetries: 0, backoff: time.Millisecond},
		reader:       reader,
		hw:           newHighWaterTracker(),
		pgDB:         pgDB,
		httpClient:   &http.Client{Transport: okDestination{}},
		eventsWriter: &kafka.Writer{}, // no address: the stats emit fails, the counters are still updated
		metrics:      &Metrics{},
		cdcInserts:   c.inserts,
		cdcUpdates:   c.updates,
		cdcDeletes:   c.deletes,
		cdcBytes:     &sync.Map{},
		batches:      map[string]*cdcObjectBatch{},
	}
	events := make([]map[string]interface{}, len(sms))
	for i := range events {
		events[i] = map[string]interface{}{"id": i}
	}
	batch := &cdcObjectBatch{
		topic:       "cdc.public.orders",
		table:       "public.orders",
		format:      "jsonl",
		compression: "none",
		bucket:      "bucket",
		prefix:      "prefix",
		firstOffset: msgs[0].Offset,
		lastOffset:  msgs[len(msgs)-1].Offset,
		events:      events,
		messages:    msgs,
		sms:         sms,
	}
	const key = "test-batch"
	b.batches[key] = batch
	b.flushBatch(context.Background(), key, batch, "test_flush")
	if _, pending := b.batches[key]; pending {
		t.Fatal("flushBatch did not complete the batch; the counters under test were never reached")
	}
}

// dbDestination is a relational or document destination that reports how many rows
// each write carried, as flushBatch requires. With rejectMultiRow it refuses every
// multi-row write with a row-level error, which sends flushBatch into per-row
// recovery; singleRowWrites counts the one-row writes that recovery makes.
type dbDestination struct {
	rejectMultiRow  bool
	singleRowWrites *int32
}

func (d dbDestination) RoundTrip(r *http.Request) (*http.Response, error) {
	var req struct {
		Params struct {
			Arguments struct {
				Data []json.RawMessage `json:"data"`
			} `json:"arguments"`
		} `json:"params"`
	}
	raw, _ := io.ReadAll(r.Body)
	_ = json.Unmarshal(raw, &req)
	n := len(req.Params.Arguments.Data)
	result := fmt.Sprintf(`{"success":true,"rows_upserted":%d}`, n)
	if d.rejectMultiRow && n > 1 {
		result = `{"success":false,"error":"integer out of range"}`
	}
	if n == 1 && d.singleRowWrites != nil {
		atomic.AddInt32(d.singleRowWrites, 1)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"jsonrpc":"2.0","id":1,"result":` + result + `}`)),
		Request:    r,
	}, nil
}

// flushDBBatch runs one cdcDBBatcher.flushBatch — the lane a PostgreSQL → MongoDB
// pipeline's inserts and updates take — against pgDB (nil: no ledger), the given
// counters and destination.
func flushDBBatch(t *testing.T, pgDB *sql.DB, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message, dest http.RoundTripper) {
	t.Helper()
	captureFailClosed(t) // a fail-closed exit panics instead of ending the test binary
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{"127.0.0.1:1"}, Topic: "cdc.public.orders"})
	defer reader.Close()
	b := &cdcDBBatcher{
		cfg:          &WorkerConfig{PipelineID: ackFKPipelineID, SinkMode: "cdc"},
		destType:     "mongodb",
		destCfg:      map[string]interface{}{},
		params:       cdcBatchingParams{maxRetries: 0, backoff: time.Millisecond},
		reader:       reader,
		hw:           newHighWaterTracker(),
		pgDB:         pgDB,
		httpClient:   &http.Client{Transport: dest},
		ddl:          &DDLSupport{},
		eventsWriter: &kafka.Writer{},
		metrics:      &Metrics{},
		cdcInserts:   c.inserts,
		cdcUpdates:   c.updates,
		cdcDeletes:   c.deletes,
		cdcBytes:     &sync.Map{},
		batches:      map[string]*cdcDBBatch{},
	}
	rows := make([]map[string]interface{}, len(sms))
	for i := range rows {
		rows[i] = map[string]interface{}{"id": msgs[i].Offset}
	}
	batch := &cdcDBBatch{
		table:       "public.orders",
		targetTable: "orders",
		topic:       "cdc.public.orders",
		rows:        rows,
		messages:    msgs,
		sms:         sms,
		keyFields:   []string{"id"},
		firstOffset: msgs[0].Offset,
		lastOffset:  msgs[len(msgs)-1].Offset,
	}
	const key = "test-batch"
	b.batches[key] = batch
	b.flushBatch(context.Background(), key, batch, "test_flush")
	if _, pending := b.batches[key]; pending {
		t.Fatal("flushBatch did not complete the batch; the counters under test were never reached")
	}
}

// deliverDeletes applies each message through processCDCEvent, the lane every CDC
// delete takes on a relational or document destination.
func deliverDeletes(t *testing.T, pgDB *sql.DB, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message) {
	t.Helper()
	cfg := &WorkerConfig{PipelineID: ackFKPipelineID, SinkMode: "cdc", DestinationConnector: "mongodb", DestinationConfig: map[string]interface{}{}}
	client := &http.Client{Transport: dbDestination{}}
	for i, sm := range sms {
		sm.PK = map[string]interface{}{"id": msgs[i].Offset}
		sm.KeyFields = []string{"id"}
		commit, err := processCDCEvent(context.Background(), newHighWaterTracker(), pgDB, client, cfg,
			&DDLSupport{Enabled: false, resolved: true}, &kafka.Writer{}, nil, msgs[i], sm, &Metrics{},
			c.inserts, c.updates, c.deletes, &sync.Map{}, time.Minute)
		if err != nil || !commit {
			t.Fatalf("delete at offset %d was not applied (commit=%v): %v", msgs[i].Offset, commit, err)
		}
	}
}

// storedAfterRestartAndReplay is the reported failure end to end: a sink delivers
// offsets 0-4, restarts before committing 2-4, is handed 2-4 again followed by new
// offsets 5-6, and the projector stores the larger of the two processes' reports.
// Seven distinct rows were delivered, so a correct counter stores 7.
func storedAfterRestartAndReplay(t *testing.T, withLedger bool, op string, counter func(cdcTestCounters) *sync.Map,
	deliver func(t *testing.T, db *sql.DB, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message)) int64 {
	t.Helper()
	var db *sql.DB
	if withLedger {
		db = newAckFKDB(t, &ackFKConn{})
		defer db.Close()
	}

	p1 := newCDCTestCounters()
	sms, msgs := ledgerTestMessages(0, 5, op)
	deliver(t, db, p1, sms, msgs)
	if got := loadCounter(counter(p1), "public.orders"); got != 5 {
		t.Fatalf("process 1 counted %d, want 5", got)
	}

	// Restart: fresh counters, seeded the way main() seeds them.
	p2 := newCDCTestCounters()
	if withLedger {
		seedCDCCountersFromLedger(context.Background(), db, ackFKPipelineID, ackFKExecID, p2.inserts, p2.updates, p2.deletes)
	}
	sms, msgs = ledgerTestMessages(2, 7, op)
	deliver(t, db, p2, sms, msgs)

	p1Total, p2Total := loadCounter(counter(p1), "public.orders"), loadCounter(counter(p2), "public.orders")
	if p1Total > p2Total {
		return p1Total
	}
	return p2Total
}

// assertRestartAndReplayCountsDistinctRows checks the scenario with the ledger, and
// runs it without the ledger as a control: that is the old behaviour, and if it also
// reached 7 the scenario would not reproduce the bug and the first check proves nothing.
func assertRestartAndReplayCountsDistinctRows(t *testing.T, op string, counter func(cdcTestCounters) *sync.Map,
	deliver func(t *testing.T, db *sql.DB, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message)) {
	t.Helper()
	if got := storedAfterRestartAndReplay(t, true, op, counter, deliver); got != 7 {
		t.Fatalf("stored count after restart + replay = %d, want 7 (distinct rows delivered)", got)
	}
	if got := storedAfterRestartAndReplay(t, false, op, counter, deliver); got == 7 {
		t.Fatalf("control without the ledger also reached 7; the scenario does not reproduce the bug")
	}
}

func insertCounter(c cdcTestCounters) *sync.Map { return c.inserts }
func updateCounter(c cdcTestCounters) *sync.Map { return c.updates }
func deleteCounter(c cdcTestCounters) *sync.Map { return c.deletes }

// Object storage destinations (GCS and the other object stores).
func TestCDCCounters_SurviveRestartAndReplay(t *testing.T) {
	assertRestartAndReplayCountsDistinctRows(t, "c", insertCounter,
		func(t *testing.T, db *sql.DB, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message) {
			flushObjectBatch(t, db, c, sms, msgs)
		})
}

// Relational and document destinations: inserts and updates through the DB batcher.
func TestCDCCounters_DBBatcherSurvivesRestartAndReplay(t *testing.T) {
	for _, tc := range []struct {
		op      string
		counter func(cdcTestCounters) *sync.Map
	}{{"c", insertCounter}, {"r", insertCounter}, {"u", updateCounter}} {
		t.Run(tc.op, func(t *testing.T) {
			assertRestartAndReplayCountsDistinctRows(t, tc.op, tc.counter,
				func(t *testing.T, db *sql.DB, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message) {
					flushDBBatch(t, db, c, sms, msgs, dbDestination{})
				})
		})
	}
}

// The same batches when the destination refuses the whole-batch write and every row
// is written on its own by per-row recovery.
func TestCDCCounters_DBBatcherPerRowRecoverySurvivesRestartAndReplay(t *testing.T) {
	var singleRowWrites int32
	assertRestartAndReplayCountsDistinctRows(t, "c", insertCounter,
		func(t *testing.T, db *sql.DB, c cdcTestCounters, sms []*SinkMessage, msgs []kafka.Message) {
			flushDBBatch(t, db, c, sms, msgs, dbDestination{rejectMultiRow: true, singleRowWrites: &singleRowWrites})
		})
	if singleRowWrites == 0 {
		t.Fatal("no single-row writes: per-row recovery never ran, so this test covered the whole-batch path again")
	}
}

// Deletes, which bypass the DB batcher.
func TestCDCCounters_DeletesSurviveRestartAndReplay(t *testing.T) {
	assertRestartAndReplayCountsDistinctRows(t, "d", deleteCounter, deliverDeletes)
}

func TestPersistCDCAcksBatch_ReplayedFlushIsNotCountedTwice(t *testing.T) {
	db := newAckFKDB(t, &ackFKConn{})
	defer db.Close()
	sms, msgs := ledgerTestMessages(0, 4, "u")

	first, err := persistCDCAcksBatch(context.Background(), db, sms, msgs, "dest")
	if err != nil {
		t.Fatalf("first flush: %v", err)
	}
	for i, ok := range first {
		if !ok {
			t.Fatalf("first flush: message %d not counted: %v", i, first)
		}
	}

	again, err := persistCDCAcksBatch(context.Background(), db, sms, msgs, "dest")
	if err != nil {
		t.Fatalf("replayed flush: %v", err)
	}
	for i, ok := range again {
		if ok {
			t.Fatalf("replayed flush: message %d counted again: %v", i, again)
		}
	}
}

func TestPersistCDCAcksBatch_OffsetRepeatedInOneFlushCountsOnce(t *testing.T) {
	db := newAckFKDB(t, &ackFKConn{})
	defer db.Close()
	sms, msgs := ledgerTestMessages(0, 2, "c")
	sms = append(sms, sms[1])
	msgs = append(msgs, msgs[1])

	counted, err := persistCDCAcksBatch(context.Background(), db, sms, msgs, "dest")
	if err != nil {
		t.Fatalf("persistCDCAcksBatch: %v", err)
	}
	if want := []bool{true, true, false}; !equalBools(counted, want) {
		t.Fatalf("counted = %v, want %v", counted, want)
	}
}

// A returned key that matches nothing in the flush means the keys are not being
// compared like for like (a type or trimming mismatch). The counters must then keep
// the old count-everything behaviour rather than silently count nothing.
func TestPersistCDCAcksBatch_UnexplainedReturnedKeyKeepsFlushCounted(t *testing.T) {
	sms, msgs := ledgerTestMessages(0, 3, "c")
	conn := &ackFKConn{strayKey: &cdcAckKey{table: "public.other", topic: "cdc.public.other", offset: 99}}
	db := newAckFKDB(t, conn)
	defer db.Close()
	// Replay: every real row is already in the ledger, so only the stray key comes back.
	if _, err := persistCDCAcksBatch(context.Background(), db, sms, msgs, "dest"); err != nil {
		t.Fatalf("seed flush: %v", err)
	}

	counted, err := persistCDCAcksBatch(context.Background(), db, sms, msgs, "dest")
	if err != nil {
		t.Fatalf("persistCDCAcksBatch: %v", err)
	}
	if want := []bool{true, true, true}; !equalBools(counted, want) {
		t.Fatalf("counted = %v, want %v", counted, want)
	}
}

// A flush larger than one INSERT chunk: each chunk's answer must land on its own
// messages, not on the first chunk's indexes.
func TestPersistCDCAcksBatch_SecondChunkMarksItsOwnMessages(t *testing.T) {
	n := pgAckLedgerChunk + 1
	sms, msgs := ledgerTestMessages(0, n, "c")
	conn := &ackFKConn{ledger: map[cdcAckKey]string{}}
	for _, i := range []int{0, n - 1} {
		conn.ledger[cdcAckKey{table: sms[i].Table, topic: msgs[i].Topic, partition: int64(msgs[i].Partition), offset: msgs[i].Offset}] = "c"
	}
	db := newAckFKDB(t, conn)
	defer db.Close()

	counted, err := persistCDCAcksBatch(context.Background(), db, sms, msgs, "dest")
	if err != nil {
		t.Fatalf("persistCDCAcksBatch: %v", err)
	}
	if got := countMatching(conn.recorded(), "INSERT INTO pipeline_batch_acks"); got != 2 {
		t.Fatalf("expected 2 chunked INSERTs for %d messages, got %d", n, got)
	}
	notCounted := 0
	for i, ok := range counted {
		if !ok {
			notCounted++
			if i != 0 && i != n-1 {
				t.Fatalf("message %d not counted, but only 0 and %d were already in the ledger", i, n-1)
			}
		}
	}
	if notCounted != 2 {
		t.Fatalf("%d messages not counted, want 2 (offsets 0 and %d)", notCounted, n-1)
	}
}

func TestPersistCDCAcksBatch_NoLedgerCountsEverything(t *testing.T) {
	sms, msgs := ledgerTestMessages(0, 3, "d")
	counted, err := persistCDCAcksBatch(context.Background(), nil, sms, msgs, "dest")
	if err != nil {
		t.Fatalf("persistCDCAcksBatch(nil db): %v", err)
	}
	if want := []bool{true, true, true}; !equalBools(counted, want) {
		t.Fatalf("counted = %v, want %v", counted, want)
	}
}

// The per-event lane (deletes, append-only mode, per-row recovery) writes one ack
// at a time. It must report whether the row was new, and it must anchor the
// executions row itself: in append-only mode no batch flush ever does.
func TestPersistCDCAckToPostgres_ReportsNewRowsAndAnchorsExecution(t *testing.T) {
	const pipelineID = "22222222-2222-2222-2222-222222222222"
	cdcAuditExecutionEnsured.Delete(pipelineID + "|" + pipelineID)
	t.Cleanup(func() { cdcAuditExecutionEnsured.Delete(pipelineID + "|" + pipelineID) })

	conn := &ackFKConn{}
	db := newAckFKDB(t, conn)
	defer db.Close()
	sm := &SinkMessage{PipelineID: pipelineID, ExecutionID: pipelineID, Table: "public.orders", StorageType: "cdc", RowCount: 1, CDCOp: "c"}

	steps := []struct {
		offset int64
		want   bool
	}{
		{offset: 10, want: true},
		{offset: 10, want: false}, // replay
		{offset: 11, want: true},
	}
	for _, s := range steps {
		added, err := persistCDCAckToPostgres(context.Background(), db, sm, 1, "orders", "cdc.public.orders", 0, s.offset)
		if err != nil {
			t.Fatalf("offset %d: %v", s.offset, err)
		}
		if added != s.want {
			t.Fatalf("offset %d: added = %v, want %v", s.offset, added, s.want)
		}
	}

	queries := conn.recorded()
	if got := countMatching(queries, "INSERT INTO executions"); got != 1 {
		t.Fatalf("executions anchor ran %d times over 3 acks, want once per process", got)
	}
	if !strings.Contains(queries[0], "INSERT INTO executions") {
		t.Fatalf("first query must anchor the executions row, got %q", queries[0])
	}

	t.Run("failed write still counts", func(t *testing.T) {
		failing := &ackFKConn{failAcks: io.ErrUnexpectedEOF}
		fdb := newAckFKDB(t, failing)
		defer fdb.Close()
		added, err := persistCDCAckToPostgres(context.Background(), fdb, sm, 1, "orders", "cdc.public.orders", 0, 12)
		if err == nil || added {
			t.Fatalf("got (%v, %v), want (false, error)", added, err)
		}
		if !cdcAckCounts(added, err) {
			t.Fatal("a failed ack write must still count the row, as before the ledger was consulted")
		}
	})
}

func TestCDCAckCounts(t *testing.T) {
	cases := []struct {
		added bool
		err   error
		want  bool
	}{
		{added: true, want: true},
		{added: false, want: false},
		{added: false, err: io.EOF, want: true},
	}
	for _, c := range cases {
		if got := cdcAckCounts(c.added, c.err); got != c.want {
			t.Errorf("cdcAckCounts(%v, %v) = %v, want %v", c.added, c.err, got, c.want)
		}
	}
}

func TestSeedCDCCountersFromLedger_MapsOpsToCounters(t *testing.T) {
	conn := &ackFKConn{seedRows: [][]driver.Value{
		{"public.orders", "c", int64(7)},
		{"public.orders", "r", int64(3)},
		{"public.orders", "u", int64(2)},
		{"public.orders", "d", int64(1)},
		{"public.users", "c", int64(5)},
		{"public.users", "t", int64(9)}, // truncate: not a row counter
		{"public.users", "", int64(4)},
	}}
	db := newAckFKDB(t, conn)
	defer db.Close()
	c := newCDCTestCounters()

	seedCDCCountersFromLedger(context.Background(), db, " "+ackFKPipelineID+" ", ackFKExecID+"\n", c.inserts, c.updates, c.deletes)

	checks := []struct {
		name  string
		m     *sync.Map
		table string
		want  int64
	}{
		{"orders inserts (c + r)", c.inserts, "public.orders", 10},
		{"orders updates", c.updates, "public.orders", 2},
		{"orders deletes", c.deletes, "public.orders", 1},
		{"users inserts", c.inserts, "public.users", 5},
		{"users updates", c.updates, "public.users", 0},
		{"users deletes", c.deletes, "public.users", 0},
	}
	for _, ch := range checks {
		if got := loadCounter(ch.m, ch.table); got != ch.want {
			t.Errorf("%s = %d, want %d", ch.name, got, ch.want)
		}
	}

	queries := conn.recorded()
	if len(queries) != 1 {
		t.Fatalf("expected one seed query, got %v", queries)
	}
	for _, clause := range []string{"storage_type = 'cdc'", "last_error IS NULL", "pipeline_id = $1", "execution_id = $2"} {
		if !strings.Contains(queries[0], clause) {
			t.Errorf("seed query is missing %q (failed or non-CDC acks would be counted): %s", clause, queries[0])
		}
	}
	if len(conn.seedArgs) != 2 || conn.seedArgs[0].Value != ackFKPipelineID || conn.seedArgs[1].Value != ackFKExecID {
		t.Errorf("seed args = %v, want trimmed pipeline and execution ids", conn.seedArgs)
	}
}

func TestSeedCDCCountersFromLedger_SkipsWithoutUsableIDs(t *testing.T) {
	cases := []struct{ name, pipelineID, executionID string }{
		{"blank pipeline", "", ackFKExecID},
		{"blank execution", ackFKPipelineID, ""},
		{"non-uuid pipeline", "pipe-1", ackFKExecID},
		{"non-uuid execution", ackFKPipelineID, "exec-1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := &ackFKConn{seedRows: [][]driver.Value{{"public.orders", "c", int64(1)}}}
			db := newAckFKDB(t, conn)
			defer db.Close()
			c := newCDCTestCounters()
			seedCDCCountersFromLedger(context.Background(), db, tc.pipelineID, tc.executionID, c.inserts, c.updates, c.deletes)
			if got := conn.recorded(); len(got) != 0 {
				t.Fatalf("expected no query, got %v", got)
			}
		})
	}
	t.Run("nil db does not panic", func(t *testing.T) {
		c := newCDCTestCounters()
		seedCDCCountersFromLedger(context.Background(), nil, ackFKPipelineID, ackFKExecID, c.inserts, c.updates, c.deletes)
	})
}

// A read that fails, up front or partway through the rows, must leave every
// counter at zero rather than seed some tables and not others.
func TestSeedCDCCountersFromLedger_ReadFailureSeedsNothing(t *testing.T) {
	rows := [][]driver.Value{{"public.orders", "c", int64(7)}, {"public.users", "u", int64(3)}}
	cases := []struct {
		name string
		conn *ackFKConn
	}{
		{"query fails", &ackFKConn{seedRows: rows, failSeed: io.ErrUnexpectedEOF}},
		{"rows fail after reading", &ackFKConn{seedRows: rows, seedRowsErr: io.ErrUnexpectedEOF}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := newAckFKDB(t, tc.conn)
			defer db.Close()
			c := newCDCTestCounters()
			seedCDCCountersFromLedger(context.Background(), db, ackFKPipelineID, ackFKExecID, c.inserts, c.updates, c.deletes)
			if got := loadCounter(c.inserts, "public.orders"); got != 0 {
				t.Errorf("orders inserts = %d, want 0", got)
			}
			if got := loadCounter(c.updates, "public.users"); got != 0 {
				t.Errorf("users updates = %d, want 0", got)
			}
		})
	}
}

func TestCDCStatsExecutionID(t *testing.T) {
	const pipelineID, executionID = "pipe", "exec"
	cases := []struct {
		name string
		cfg  WorkerConfig
		want string
	}{
		{"cdc mode uses the pipeline id", WorkerConfig{PipelineID: pipelineID, ExecutionID: executionID, SinkMode: "cdc"}, pipelineID},
		{"cdc mode is case and space insensitive", WorkerConfig{PipelineID: " " + pipelineID, ExecutionID: executionID, SinkMode: " CDC "}, pipelineID},
		{"batch mode uses the execution id", WorkerConfig{PipelineID: pipelineID, ExecutionID: " " + executionID + " ", SinkMode: "batch"}, executionID},
		{"no execution id falls back to the pipeline id", WorkerConfig{PipelineID: pipelineID, ExecutionID: "  ", SinkMode: "batch"}, pipelineID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := cdcStatsExecutionID(&tc.cfg); got != tc.want {
				t.Fatalf("cdcStatsExecutionID = %q, want %q", got, tc.want)
			}
		})
	}
}

func equalBools(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
