package main

// A layout v2 re-snapshot must replace a table's folder, not add a second copy of every
// row to it. The orchestrator leaves a cdc_object_reload_requests row when it sends the
// snapshot signal; the sink consumes it on the first snapshot batch that is not older
// than the request, starting a new generation (bump + folder clean) in the same
// transaction. These tests pin who pays for the check (snapshot batches only), which
// batch consumes it, and that a failed clean keeps the request.

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func v2ReloadTableKey(t *testing.T, b *cdcObjectBatcher) string {
	t.Helper()
	_, sm := v2CDCMessage(1, true, 1)
	key, err := objectLayoutV2TableKey(cdcObjectLayoutV2Table(b.cfg, sm))
	if err != nil {
		t.Fatalf("table key: %v", err)
	}
	return b.cfg.PipelineID + "|" + key
}

func TestObjectLayoutV2ReloadDue(t *testing.T) {
	const req = int64(1_800_000_000_000)
	cases := []struct {
		name string
		ts   int64
		want bool
	}{
		{"after the request", req + 5_000, true},
		{"inside the clock skew", req - 30_000, true},
		{"older than the skew: a snapshot from before the request", req - 30_001, false},
		{"unknown event time never consumes", 0, false},
	}
	for _, c := range cases {
		if got := objectLayoutV2ReloadDue(c.ts, req); got != c.want {
			t.Errorf("%s: due = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestCDCBatcherLayoutV2ReloadRequestStartsNewGeneration(t *testing.T) {
	ctx := t.Context()
	store := newFakeLoadStore()
	b, rec := v2CDCBatcher(t, store)
	k := v2ReloadTableKey(t, b)
	ts := time.Date(2026, 9, 18, 10, 11, 12, 345_000_000, time.UTC).UnixMilli()
	loadKey := func(n string) string {
		return "exports/sales/datingapp/public/users/dt=2026-09-18/LOAD0000000" + n + ".parquet"
	}
	flushSnapshot := func(b *cdcObjectBatcher, first int64, ts int64, n int) {
		for i := 0; i < n; i++ {
			msg, sm := v2CDCMessage(first+int64(i), true, ts+int64(i)*1000)
			b.add(ctx, msg, sm)
		}
		b.flushDue(ctx, time.Now().Add(time.Hour))
	}

	// The initial snapshot: one clean (generation 0), LOAD 1.
	flushSnapshot(b, 10, ts, 2)
	if store.cleans != 1 || store.reloadQueries != 1 {
		t.Fatalf("initial snapshot: cleans=%d reloadQueries=%d, want 1/1", store.cleans, store.reloadQueries)
	}

	// Re-snapshot requested an hour later.
	req := ts + time.Hour.Milliseconds()
	store.reloads["p-flush-interval|cdc.public.users"] = req

	// A change batch never looks for the request, and cleans nothing.
	msg, sm := v2CDCMessage(12, false, req+500)
	b.add(ctx, msg, sm)
	b.flushDue(ctx, time.Now().Add(time.Hour))
	if store.reloadQueries != 1 || store.cleans != 1 {
		t.Fatalf("a change batch: reloadQueries=%d cleans=%d, want 1/1 (no query, no clean)", store.reloadQueries, store.cleans)
	}

	// The first snapshot batch of the new snapshot consumes it: new generation, the
	// folder emptied before the LOAD file, numbering back at 1, the request deleted.
	deletesBefore := len(rec.named("delete_prefix"))
	importsBefore := len(rec.named("import_data"))
	flushSnapshot(b, 13, req+1000, 2)
	if store.cleans != 2 || store.gen[k] != 1 || store.cleaned[k] != 1 {
		t.Fatalf("after the reload: cleans=%d gen=%d cleaned=%d, want 2/1/1", store.cleans, store.gen[k], store.cleaned[k])
	}
	if _, ok := store.reloads["p-flush-interval|cdc.public.users"]; ok {
		t.Fatal("the consumed reload request was not deleted")
	}
	if n := len(rec.named("delete_prefix")) - deletesBefore; n != 2 {
		t.Fatalf("the reload deleted %d prefixes, want 2 (data + sidecar)", n)
	}
	rec.mu.Lock()
	var order []string
	for _, c := range rec.calls {
		order = append(order, c.tool)
	}
	rec.mu.Unlock()
	if last := order[len(order)-1]; !strings.HasSuffix(last, "import_data") || !strings.HasSuffix(order[len(order)-2], "delete_prefix") {
		t.Fatalf("the folder must be emptied before the new LOAD file, tool order %v", order)
	}
	imports := rec.named("import_data")
	if len(imports) != importsBefore+1 || imports[len(imports)-1].args["key"] != loadKey("1") {
		t.Fatalf("the reloaded snapshot wrote %v, want %s (numbering restarts in the new generation)", imports[importsBefore:], loadKey("1"))
	}

	// The rest of that snapshot writes on, in the same generation, without a clean.
	flushSnapshot(b, 15, req+3000, 1)
	if store.cleans != 2 || store.gen[k] != 1 {
		t.Fatalf("a later batch of the same snapshot: cleans=%d gen=%d, want 2/1", store.cleans, store.gen[k])
	}
	if imports := rec.named("import_data"); imports[len(imports)-1].args["key"] != loadKey("2") {
		t.Fatalf("second batch of the reload wrote %v, want %s", imports[len(imports)-1].args["key"], loadKey("2"))
	}

	// A restart redelivers the reload's snapshot batch: no request left, so no clean,
	// and the same LOAD file is rewritten.
	b2, rec2 := v2CDCBatcher(t, store)
	flushSnapshot(b2, 13, req+1000, 2)
	if store.cleans != 2 || store.gen[k] != 1 {
		t.Fatalf("a restart without a new request cleaned the folder: cleans=%d gen=%d", store.cleans, store.gen[k])
	}
	if got := rec2.named("import_data"); len(got) != 1 || got[0].args["key"] != loadKey("1") {
		t.Fatalf("redelivered reload batch wrote %v, want the same %s", got, loadKey("1"))
	}
}

func TestCDCBatcherLayoutV2ReloadRequestIgnoresOlderSnapshotBatch(t *testing.T) {
	ctx := t.Context()
	store := newFakeLoadStore()
	b, _ := v2CDCBatcher(t, store)
	k := v2ReloadTableKey(t, b)
	req := time.Date(2026, 9, 18, 10, 0, 0, 0, time.UTC).UnixMilli()
	store.reloads["p-flush-interval|cdc.public.users"] = req

	// A snapshot read a minute before the request belongs to an earlier snapshot
	// (a restart redelivering it): it must not start the reload.
	msg, sm := v2CDCMessage(10, true, req-time.Minute.Milliseconds())
	b.add(ctx, msg, sm)
	b.flushDue(ctx, time.Now().Add(time.Hour))
	if store.reloadQueries != 1 {
		t.Fatalf("reloadQueries = %d, want 1 (one cheap check per snapshot batch)", store.reloadQueries)
	}
	if _, ok := store.reloads["p-flush-interval|cdc.public.users"]; !ok {
		t.Fatal("an older snapshot batch consumed the reload request")
	}
	if store.gen[k] != 0 {
		t.Fatalf("generation = %d, want 0 (no reload started)", store.gen[k])
	}
}

func TestObjectLayoutV2ReloadCleanErrorKeepsRequest(t *testing.T) {
	ctx := t.Context()
	store := newFakeLoadStore()
	store.reloads["p|topic"] = 1000
	cleaned := objectLayoutV2Cleaned{}
	failing := func(context.Context) error { return errors.New("delete_prefix: listing incomplete") }
	if ok, err := cleaned.reload(ctx, store, "p", "db/public/users", "topic", 2000, failing); err == nil || ok {
		t.Fatalf("reload with a failing clean = %v, %v; want an error", ok, err)
	}
	if _, ok := store.reloads["p|topic"]; !ok {
		t.Fatal("a failed clean dropped the reload request")
	}
	if cleaned["db/public/users"] || store.gen["p|db/public/users"] != 0 {
		t.Fatal("a failed clean was recorded as a new cleaned generation")
	}
}

func TestCDCBatcherLayoutV2ReloadWithoutDatabaseKeepsUnavailableError(t *testing.T) {
	b, _ := v2CDCBatcher(t, newObjectLoadStore(nil))
	_, sm := v2CDCMessage(10, true, time.Now().UnixMilli())
	tbl := cdcObjectLayoutV2Table(b.cfg, sm)
	tableKey, err := objectLayoutV2TableKey(tbl)
	if err != nil {
		t.Fatal(err)
	}
	batch := &cdcObjectBatch{topic: "cdc.public.users", firstOffset: 10, firstEventTS: sm.SourceTS}
	if _, err := b.objectLayoutV2FlushKeyOnce(t.Context(), batch, sm, tbl, tableKey, sm.SourceTS); !errors.Is(err, errObjectLoadStoreUnavailable) {
		t.Fatalf("snapshot flush key with no database = %v, want errObjectLoadStoreUnavailable as before", err)
	}
}

// reloadDBConn is a minimal database/sql driver standing in for the two tables
// consumeReload touches. A transaction snapshots the state on Begin and restores it on
// Rollback, so a test can prove what a rolled-back reload left behind.
type reloadDBState struct {
	requestedMs int64 // 0 = no reload request
	counter     bool
	gen         int64
	cleaned     int64
	next        int64
}

type reloadDBConn struct {
	mu                         sync.Mutex
	st, saved                  reloadDBState
	queries                    []string
	begins, commits, rollbacks int
	// consumedWhileWaiting deletes the request as the counter lock is granted, the
	// way a sink for another partition that held the lock first would have.
	consumedWhileWaiting bool
}

func (c *reloadDBConn) Prepare(string) (driver.Stmt, error) { return nil, io.EOF }
func (c *reloadDBConn) Close() error                        { return nil }
func (c *reloadDBConn) Begin() (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.begins++
	c.saved = c.st
	return reloadDBTx{c}, nil
}

type reloadDBTx struct{ c *reloadDBConn }

func (t reloadDBTx) Commit() error {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.c.commits++
	return nil
}

func (t reloadDBTx) Rollback() error {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.c.rollbacks++
	t.c.st = t.c.saved
	return nil
}

func (c *reloadDBConn) note(q string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, q)
}

func (c *reloadDBConn) ExecContext(_ context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case strings.Contains(q, "INSERT INTO object_load_counters"):
		c.queries = append(c.queries, "insert counter")
		if !c.st.counter {
			c.st.counter, c.st.gen, c.st.cleaned, c.st.next = true, 0, -1, 1
		}
	case strings.Contains(q, "UPDATE object_load_counters SET generation"):
		c.queries = append(c.queries, "bump")
		g := args[2].Value.(int64)
		c.st.gen, c.st.cleaned, c.st.next = g, g, 1
	case strings.Contains(q, "DELETE FROM cdc_object_reload_requests"):
		c.queries = append(c.queries, "delete request")
		c.st.requestedMs = 0
	default:
		return nil, io.EOF
	}
	return driver.RowsAffected(1), nil
}

func (c *reloadDBConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case strings.Contains(q, "FROM cdc_object_reload_requests"):
		name := "read request"
		if strings.Contains(q, "FOR UPDATE") {
			name = "lock request"
		}
		c.queries = append(c.queries, name)
		rows := &ackFKRows{cols: []string{"requested_ms"}}
		if c.st.requestedMs != 0 {
			rows.vals = [][]driver.Value{{c.st.requestedMs}}
		}
		return rows, nil
	case strings.Contains(q, "SELECT generation FROM object_load_counters"):
		c.queries = append(c.queries, "lock counter")
		if c.consumedWhileWaiting {
			c.st.requestedMs, c.saved.requestedMs = 0, 0
		}
		return &ackFKRows{cols: []string{"generation"}, vals: [][]driver.Value{{c.st.gen}}}, nil
	}
	return nil, io.EOF
}

type reloadDBConnector struct{ c *reloadDBConn }

func (k reloadDBConnector) Connect(context.Context) (driver.Conn, error) { return k.c, nil }
func (k reloadDBConnector) Driver() driver.Driver                        { return reloadDBDriver(k) }

type reloadDBDriver struct{ c *reloadDBConn }

func (d reloadDBDriver) Open(string) (driver.Conn, error) { return d.c, nil }

func TestPGObjectLoadStoreConsumeReload(t *testing.T) {
	ctx := t.Context()
	const req = int64(1_800_000_000_000)
	open := func(st reloadDBState) (*reloadDBConn, *pgObjectLoadStore) {
		c := &reloadDBConn{st: st}
		db := sql.OpenDB(reloadDBConnector{c})
		t.Cleanup(func() { _ = db.Close() })
		return c, &pgObjectLoadStore{db: db}
	}
	cleanOK := func(c *reloadDBConn) func(context.Context) error {
		return func(context.Context) error { c.note("clean"); return nil }
	}

	t.Run("no request: one query, no transaction", func(t *testing.T) {
		c, s := open(reloadDBState{})
		if ok, err := s.consumeReload(ctx, ackFKPipelineID, "db/public/users", "cdc.public.users", req, cleanOK(c)); ok || err != nil {
			t.Fatalf("consumeReload = %v, %v; want false, nil", ok, err)
		}
		if len(c.queries) != 1 || c.begins != 0 {
			t.Fatalf("queries=%v begins=%d, want one read and no transaction", c.queries, c.begins)
		}
	})

	t.Run("older batch: one query, request kept", func(t *testing.T) {
		c, s := open(reloadDBState{requestedMs: req})
		if ok, err := s.consumeReload(ctx, ackFKPipelineID, "db/public/users", "cdc.public.users", req-time.Minute.Milliseconds(), cleanOK(c)); ok || err != nil {
			t.Fatalf("consumeReload = %v, %v; want false, nil", ok, err)
		}
		if len(c.queries) != 1 || c.begins != 0 || c.st.requestedMs != req {
			t.Fatalf("queries=%v begins=%d request=%d, want one read, no transaction, request kept", c.queries, c.begins, c.st.requestedMs)
		}
	})

	t.Run("clean error rolls back and keeps the request", func(t *testing.T) {
		c, s := open(reloadDBState{requestedMs: req, counter: true, gen: 3, cleaned: 3, next: 7})
		failing := func(context.Context) error { return errors.New("delete_prefix: listing incomplete") }
		if ok, err := s.consumeReload(ctx, ackFKPipelineID, "db/public/users", "cdc.public.users", req, failing); ok || err == nil {
			t.Fatalf("consumeReload = %v, %v; want the clean error", ok, err)
		}
		if c.commits != 0 || c.rollbacks != 1 {
			t.Fatalf("commits=%d rollbacks=%d, want 0/1", c.commits, c.rollbacks)
		}
		if c.st != (reloadDBState{requestedMs: req, counter: true, gen: 3, cleaned: 3, next: 7}) {
			t.Fatalf("state after a failed clean = %+v, want unchanged (request kept, no bump)", c.st)
		}
	})

	t.Run("fresh batch: bump, clean and delete in one transaction", func(t *testing.T) {
		c, s := open(reloadDBState{requestedMs: req, counter: true, gen: 3, cleaned: 3, next: 7})
		if ok, err := s.consumeReload(ctx, ackFKPipelineID, "db/public/users", "cdc.public.users", req+1000, cleanOK(c)); !ok || err != nil {
			t.Fatalf("consumeReload = %v, %v; want true, nil", ok, err)
		}
		if c.begins != 1 || c.commits != 1 || c.rollbacks != 0 {
			t.Fatalf("begins=%d commits=%d rollbacks=%d, want one committed transaction", c.begins, c.commits, c.rollbacks)
		}
		if c.st != (reloadDBState{counter: true, gen: 4, cleaned: 4, next: 1}) {
			t.Fatalf("state = %+v, want generation 4 cleaned, numbering at 1, request deleted", c.st)
		}
		want := "read request,insert counter,lock counter,lock request,clean,bump,delete request"
		if got := strings.Join(c.queries, ","); got != want {
			t.Fatalf("order = %s\nwant    %s (counter lock before the request lock, clean under both)", got, want)
		}
	})

	t.Run("request consumed by another writer while this one waited", func(t *testing.T) {
		c, s := open(reloadDBState{requestedMs: req, counter: true, gen: 3, cleaned: 3, next: 7})
		c.consumedWhileWaiting = true
		clean := func(context.Context) error { t.Error("cleaned although the request was gone"); return nil }
		if ok, err := s.consumeReload(ctx, ackFKPipelineID, "db/public/users", "cdc.public.users", req, clean); ok || err != nil {
			t.Fatalf("consumeReload = %v, %v; want false, nil", ok, err)
		}
		if c.commits != 0 || c.rollbacks != 1 || c.st.gen != 3 {
			t.Fatalf("commits=%d rollbacks=%d gen=%d, want a rolled-back no-op", c.commits, c.rollbacks, c.st.gen)
		}
	})

	t.Run("no database", func(t *testing.T) {
		var s *pgObjectLoadStore
		if _, err := s.consumeReload(ctx, "p", "t", "topic", req, nil); !errors.Is(err, errObjectLoadStoreUnavailable) {
			t.Fatalf("err = %v, want errObjectLoadStoreUnavailable", err)
		}
	})
}
