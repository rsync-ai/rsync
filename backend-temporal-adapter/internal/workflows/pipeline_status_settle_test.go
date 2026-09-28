package workflows

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// The postflight silent-drop guard reads pipeline_run_table_stats, which the projector
// fills asynchronously from the sink's TABLE_STATS events. On prod (2026-09-26, execution
// 65f0c413) the last table's final stats landed ~2s after the workflow completed, the
// guard read "public.subscriptions: selected but never reported (no stats row)", and a
// run that had landed all 75,230 rows was failed. These tests drive the real queries
// through a fake driver whose stats table changes between polls, the way the projector's
// writes do.

type fakeStatRow struct {
	table, bare string
	read        int64
	landed      int64
	status      string
}

// fakeStatsDB is the state behind the fake driver. arriveAfter delays a row until the
// stats query has been answered that many times — a TABLE_STATS still in flight.
type fakeStatsDB struct {
	mu          sync.Mutex
	rows        []fakeStatRow
	late        []fakeStatRow
	arriveAfter int
	statsReads  int
	selected    []string
	failStats   bool
}

func (s *fakeStatsDB) snapshot() []fakeStatRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statsReads++
	if s.late != nil && s.statsReads > s.arriveAfter {
		s.rows = append(s.rows, s.late...)
		s.late = nil
	}
	return append([]fakeStatRow(nil), s.rows...)
}

var (
	fakeStatsRegistry     sync.Map // dsn -> *fakeStatsDB
	registerFakeStatsOnce sync.Once
)

const fakeStatsDriverName = "workflows-postflight-fake-stats"

func openFakeStatsDB(t *testing.T, state *fakeStatsDB) *sql.DB {
	t.Helper()
	registerFakeStatsOnce.Do(func() { sql.Register(fakeStatsDriverName, fakeStatsDriver{}) })
	dsn := t.Name()
	fakeStatsRegistry.Store(dsn, state)
	t.Cleanup(func() { fakeStatsRegistry.Delete(dsn) })
	db, err := sql.Open(fakeStatsDriverName, dsn)
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type fakeStatsDriver struct{}

func (fakeStatsDriver) Open(dsn string) (driver.Conn, error) {
	v, ok := fakeStatsRegistry.Load(dsn)
	if !ok {
		return nil, errors.New("fake stats db not registered: " + dsn)
	}
	return &fakeStatsConn{state: v.(*fakeStatsDB)}, nil
}

type fakeStatsConn struct{ state *fakeStatsDB }

func (c *fakeStatsConn) Prepare(q string) (driver.Stmt, error) {
	return &fakeStatsStmt{state: c.state, query: q}, nil
}
func (c *fakeStatsConn) Close() error              { return nil }
func (c *fakeStatsConn) Begin() (driver.Tx, error) { return nil, errors.New("no tx in fake") }

type fakeStatsStmt struct {
	state *fakeStatsDB
	query string
}

func (s *fakeStatsStmt) Close() error  { return nil }
func (s *fakeStatsStmt) NumInput() int { return -1 }
func (s *fakeStatsStmt) Exec([]driver.Value) (driver.Result, error) {
	return nil, errors.New("no exec in fake")
}

func (s *fakeStatsStmt) Query([]driver.Value) (driver.Rows, error) {
	q := s.query
	switch {
	case strings.Contains(q, "pipeline_run_table_stats") && strings.Contains(q, "AS rr"):
		// postflightSilentDropCheck: tbl, bare, rr, landed, st
		var out [][]driver.Value
		for _, r := range s.state.snapshot() {
			out = append(out, []driver.Value{r.table, r.bare, r.read, r.landed, r.status})
		}
		return &fakeRows{cols: []string{"tbl", "bare", "rr", "landed", "st"}, data: out}, nil
	case strings.Contains(q, "pipeline_run_table_stats"):
		// tableStatsSettled: tbl, bare, st
		if s.state.failStats {
			return nil, errors.New("stats query failed")
		}
		var out [][]driver.Value
		for _, r := range s.state.snapshot() {
			out = append(out, []driver.Value{r.table, r.bare, r.status})
		}
		return &fakeRows{cols: []string{"tbl", "bare", "st"}, data: out}, nil
	case strings.Contains(q, "selected_tables"):
		b, _ := json.Marshal(s.state.selected)
		return &fakeRows{cols: []string{"text"}, data: [][]driver.Value{{string(b)}}}, nil
	case strings.Contains(q, "pipeline_batch_acks"):
		return &fakeRows{cols: []string{"max"}, data: [][]driver.Value{{nil}}}, nil
	}
	return nil, errors.New("unexpected query in fake: " + q)
}

type fakeRows struct {
	cols []string
	data [][]driver.Value
	i    int
}

func (r *fakeRows) Columns() []string { return r.cols }
func (r *fakeRows) Close() error      { return nil }
func (r *fakeRows) Next(dest []driver.Value) error {
	if r.i >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.i])
	r.i++
	return nil
}

// shrinkSettleWindow keeps the tests fast; the prod grace is a minute.
func shrinkSettleWindow(t *testing.T, grace, interval time.Duration) {
	t.Helper()
	oldG, oldI := statsSettleGrace, statsSettleInterval
	statsSettleGrace, statsSettleInterval = grace, interval
	t.Cleanup(func() { statsSettleGrace, statsSettleInterval = oldG, oldI })
}

const settleExecID = "65f0c413-0000-0000-0000-000000000000"

func completed(table string, rows int64) fakeStatRow {
	bare := table
	if i := strings.LastIndex(table, "."); i >= 0 {
		bare = table[i+1:]
	}
	return fakeStatRow{table: table, bare: bare, read: rows, landed: rows, status: "completed"}
}

// The prod failure, replayed: every table landed, the last one's stats row arrives after
// the check starts. Waiting for it turns the false "silent drop" into a clean pass. The
// control — the same state judged immediately, as the guard did before — must still
// report the drop, or this test would pass without the wait doing anything.
func TestPostflightWaitsForLateTableStats(t *testing.T) {
	shrinkSettleWindow(t, 2*time.Second, 5*time.Millisecond)

	newState := func() *fakeStatsDB {
		return &fakeStatsDB{
			selected:    []string{"public.users", "public.messages", "public.subscriptions"},
			rows:        []fakeStatRow{completed("public.users", 5005), completed("public.messages", 20005)},
			late:        []fakeStatRow{completed("public.subscriptions", 1205)},
			arriveAfter: 2,
		}
	}

	t.Run("control: judged immediately, the late table reads as dropped", func(t *testing.T) {
		db := openFakeStatsDB(t, newState())
		reason, drop := postflightSilentDropCheck(context.Background(), db, settleExecID)
		if !drop || !strings.Contains(reason, "public.subscriptions: selected but never reported") {
			t.Fatalf("control must reproduce the prod false failure; got drop=%v reason=%q", drop, reason)
		}
	})

	t.Run("after waiting for the stats to settle, no drop", func(t *testing.T) {
		state := newState()
		db := openFakeStatsDB(t, state)
		beats := 0
		if !awaitTableStatsSettled(context.Background(), db, settleExecID, func() { beats++ }) {
			t.Fatal("stats should have settled once the late row arrived")
		}
		if beats == 0 {
			t.Fatal("the wait must heartbeat between polls; the activity's heartbeat timeout is shorter than the grace")
		}
		if reason, drop := postflightSilentDropCheck(context.Background(), db, settleExecID); drop {
			t.Fatalf("every table landed, yet the guard reported a drop: %q", reason)
		}
	})
}

// A table whose stats are still 'running' has not been finalised by the sink (its EOF
// has not been handled). Its partial landed count must not be judged as a partial drop.
func TestPostflightWaitsForRunningTableToFinalise(t *testing.T) {
	shrinkSettleWindow(t, 2*time.Second, 5*time.Millisecond)
	state := &fakeStatsDB{
		selected: []string{"public.swipes"},
		rows:     []fakeStatRow{{table: "public.swipes", bare: "swipes", read: 40005, landed: 30000, status: "running"}},
	}
	db := openFakeStatsDB(t, state)

	if _, drop := postflightSilentDropCheck(context.Background(), db, settleExecID); !drop {
		t.Fatal("control: a mid-flight running row must read as a partial drop when judged immediately")
	}

	go func() {
		time.Sleep(30 * time.Millisecond)
		state.mu.Lock()
		state.rows[0] = completed("public.swipes", 40005)
		state.mu.Unlock()
	}()
	if !awaitTableStatsSettled(context.Background(), db, settleExecID, nil) {
		t.Fatal("stats should settle once the running row is finalised")
	}
	if reason, drop := postflightSilentDropCheck(context.Background(), db, settleExecID); drop {
		t.Fatalf("finalised table reported as dropped: %q", reason)
	}
}

// A table that really never reports must still fail the run: the wait gives up after the
// grace and the guard judges what arrived, exactly as before.
func TestPostflightStillFailsAGenuinelyMissingTable(t *testing.T) {
	shrinkSettleWindow(t, 40*time.Millisecond, 5*time.Millisecond)
	db := openFakeStatsDB(t, &fakeStatsDB{
		selected: []string{"public.users", "public.subscriptions"},
		rows:     []fakeStatRow{completed("public.users", 5005)},
	})

	start := time.Now()
	if awaitTableStatsSettled(context.Background(), db, settleExecID, nil) {
		t.Fatal("a table that never reports must not count as settled")
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("gave up after %v, before the grace elapsed", elapsed)
	}
	reason, drop := postflightSilentDropCheck(context.Background(), db, settleExecID)
	if !drop || !strings.Contains(reason, "public.subscriptions") {
		t.Fatalf("genuinely missing table must still fail the run; got drop=%v reason=%q", drop, reason)
	}
}

// No stats rows at all means the stats path isn't live for this run; the guard judges
// nothing, so the wait must not hold every such run for the full grace.
func TestPostflightDoesNotWaitWhenNoStatsReported(t *testing.T) {
	shrinkSettleWindow(t, 5*time.Second, 5*time.Millisecond)
	db := openFakeStatsDB(t, &fakeStatsDB{selected: []string{"public.users"}})

	start := time.Now()
	if !awaitTableStatsSettled(context.Background(), db, settleExecID, nil) {
		t.Fatal("zero stats rows should count as settled")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waited %v on a run with no stats rows", elapsed)
	}
}

func TestWaitUntilSettledStopsOnErrorAndCancel(t *testing.T) {
	t.Run("probe error", func(t *testing.T) {
		shrinkSettleWindow(t, 5*time.Second, 5*time.Millisecond)
		db := openFakeStatsDB(t, &fakeStatsDB{failStats: true})
		start := time.Now()
		if awaitTableStatsSettled(context.Background(), db, settleExecID, nil) {
			t.Fatal("a failing probe must not report settled")
		}
		if time.Since(start) > time.Second {
			t.Fatal("a failing probe must stop the wait, not retry until the grace")
		}
	})
	t.Run("context cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		probe := func(context.Context) (bool, error) {
			calls++
			if calls == 2 {
				cancel()
			}
			return false, nil
		}
		start := time.Now()
		if waitUntilSettled(ctx, probe, nil, 5*time.Second, 5*time.Millisecond) {
			t.Fatal("cancelled wait must not report settled")
		}
		if time.Since(start) > time.Second {
			t.Fatal("cancelled wait kept polling")
		}
	})
	t.Run("nil db is settled", func(t *testing.T) {
		if !awaitTableStatsSettled(context.Background(), nil, settleExecID, nil) {
			t.Fatal("nil db has nothing to wait for")
		}
	})
}

// The tests above drive the wait directly; none of them proves the activity still calls
// it. writePipelineStatus (the body behind UpdatePipelineStatusActivity and
// FinalizeCompletedRunActivity) needs a live DB and an activity context, so the
// wiring is pinned against the source, as TestTheTerminalWriteSiteStillRaisesTheAlert
// does: the wait must run before the check it protects, inside the same function.
func TestTheTerminalWriteSiteWaitsBeforeJudgingStats(t *testing.T) {
	const path = "pipeline_status_activity.go"
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read %s, which is the site under test: %v", path, err)
	}
	src := string(raw)
	if len(src) < 10_000 {
		t.Fatalf("%s read back as %d bytes; that is not the activity", path, len(src))
	}

	fnIdx := strings.Index(src, "func writePipelineStatus(")
	if fnIdx < 0 {
		t.Fatal("cannot find writePipelineStatus; this guard is now inert")
	}
	body := src[fnIdx:]
	if end := strings.Index(body[1:], "\nfunc "); end >= 0 {
		body = body[:end+1]
	}

	checkIdx := strings.Index(body, "postflightSilentDropCheck(ctx, db, executionID)")
	if checkIdx < 0 {
		t.Fatal("writePipelineStatus no longer calls postflightSilentDropCheck; " +
			"this guard is anchored to that call and is now inert")
	}
	waitIdx := strings.Index(body, "awaitTableStatsSettled(ctx, db, executionID,")
	if waitIdx < 0 {
		t.Fatal("writePipelineStatus judges table stats without waiting for them to " +
			"settle — a run whose last TABLE_STATS is still in flight is failed as a silent drop")
	}
	if waitIdx > checkIdx {
		t.Fatal("the settle wait runs after the silent-drop check, so it protects nothing")
	}
}
