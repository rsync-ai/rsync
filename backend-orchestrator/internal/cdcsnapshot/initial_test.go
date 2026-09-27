package cdcsnapshot

import (
	"context"
	"database/sql/driver"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

// The initial (full) load is recorded as a cdc_snapshot_requests row with source
// "initial", so the pipeline page can say "Full load in progress · 2 / 3 tables"
// and then "Load completed, replication ongoing", the way AWS DMS does.
//
// It is recorded from the snapshot rows the stats consumer SEES, not when the
// executor starts the connector: Debezium skips the snapshot whenever the
// connector already has offsets, so a row written at start would sit "in
// progress" on every restart and on every pipeline that loaded before this
// existed. Nothing observed → no row → the page says nothing about a load.

func initialRequest(status string) Request {
	r := sentRequest("blocking", "public.users", "public.orders")
	r.Source = SourceInitial
	r.Status = status
	return r
}

func TestNormalizeSource_CallersCannotForgeAnInitialLoad(t *testing.T) {
	// The API passes a caller's source straight through NormalizeSource; an
	// "initial" request from a caller would overwrite the pipeline's load status.
	if got := NormalizeSource("initial"); got != SourceResnapshot {
		t.Fatalf(`NormalizeSource("initial") = %q, want %q`, got, SourceResnapshot)
	}
}

func TestApplyObservations_InitialLoadAcceptsEveryTable(t *testing.T) {
	r := initialRequest(StatusSent)
	obs := []Observation{
		{Table: "public.users", Rows: 10, LastSeen: t0.Add(time.Second)},
		// Not in the include list the row was created with (a regex include
		// list, or a table added since): still part of the load.
		{Table: "public.audit", Rows: 4, LastSeen: t0.Add(time.Second), TableDone: true},
	}
	if !ApplyObservations(&r, obs, t0.Add(2*time.Second)) || r.Status != StatusStarted {
		t.Fatalf("want started: %+v", r)
	}
	if len(r.Tables) != 3 || r.Tables[2] != "public.audit" {
		t.Fatalf("an unknown table must be added to the load, tables = %v", r.Tables)
	}
	if len(r.CompletedTables) != 1 || r.CompletedTables[0] != "public.audit" {
		t.Fatalf("completed = %v", r.CompletedTables)
	}
	// Control: a Re-snapshot still ignores tables it did not ask for.
	q := sentRequest("blocking", "public.users")
	if ApplyObservations(&q, []Observation{{Table: "public.audit", Rows: 4, LastSeen: t0.Add(time.Second)}}, t0.Add(2*time.Second)) {
		t.Fatalf("a re-snapshot request took another table's rows: %+v", q)
	}
}

// A load whose table list came from what was observed so far must not call
// itself finished because every table SEEN so far is done: the next table's
// rows may simply not have arrived yet. Only Debezium's "last" ends it.
func TestApplyObservations_InitialLoadEndsOnlyOnDebeziumsLastMarker(t *testing.T) {
	r := initialRequest(StatusStarted)
	r.StartedAt, r.LastProgressAt = tp(t0), tp(t0)
	obs := []Observation{
		{Table: "public.users", Rows: 1, LastSeen: t0.Add(time.Second), TableDone: true},
		{Table: "public.orders", Rows: 1, LastSeen: t0.Add(time.Second), TableDone: true},
	}
	ApplyObservations(&r, obs, t0.Add(2*time.Second))
	if r.Status != StatusStarted {
		t.Fatalf("every listed table done is not the end of an initial load: %+v", r)
	}
	ApplyObservations(&r, []Observation{{Table: "public.audit", Rows: 1, LastSeen: t0.Add(3 * time.Second), TableDone: true, AllDone: true}}, t0.Add(4*time.Second))
	if r.Status != StatusCompleted || r.CompletedAt == nil {
		t.Fatalf(`source.snapshot "last" must complete it: %+v`, r)
	}
}

func TestWatch_InitialLoad(t *testing.T) {
	tm := DefaultTiming
	started := func(mode string, progress *time.Time) Request {
		r := initialRequest(StatusStarted)
		r.Mode, r.StartedAt, r.LastProgressAt = mode, tp(t0), progress
		return r
	}
	cases := []struct {
		name    string
		r       Request
		now     time.Time
		tracked bool
		want    Action
	}{
		// Never re-sent: nothing sent it in the first place.
		{"sent, long quiet → never resend", initialRequest(StatusSent), t0.Add(time.Hour), true, ActNone},
		{"blocking, quiet past the re-snapshot stall → still running", started("blocking", tp(t0)), t0.Add(tm.StallTimeout), true, ActNone},
		{"blocking, quiet past the initial stall → not confirmed", started("blocking", tp(t0)), t0.Add(tm.InitialStall), true, ActUnconfirmed},
		{"incremental, idle → complete", started("incremental", tp(t0)), t0.Add(tm.IdleComplete), true, ActComplete},
		// The hybrid executor's batch load: no snapshot rows at all, the
		// executor finishes it. Not "untracked", not stalled.
		{"hybrid batch load (no progress clock)", started("blocking", nil), t0.Add(24 * time.Hour), false, ActNone},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, msg := Watch(c.r, c.now, c.tracked, tm)
			if got != c.want {
				t.Fatalf("Watch = %v (%q), want %v", got, msg, c.want)
			}
			if got == ActUnconfirmed && msg == "" {
				t.Fatal("an unconfirmed load needs a reason")
			}
		})
	}
}

// --- Dispatcher.Observe -------------------------------------------------------

var (
	listForObserve = `FROM cdc_snapshot_requests\s+WHERE pipeline_id = \$1::uuid`
	initialBlocked = `SELECT EXISTS \(\s*SELECT 1 FROM cdc_snapshot_requests`
	insertInitial  = regexp.QuoteMeta(`INSERT INTO cdc_snapshot_requests`)
)

func noRequests() *sqlmock.Rows { return sqlmock.NewRows(requestCols) }

// failOnWarning fails the test on any warning logged while it runs. Observe
// logs a store error and carries on, so an unexpected query — a load recorded
// that must not be — would otherwise pass unnoticed.
func failOnWarning(t *testing.T) {
	t.Helper()
	hook := logtest.NewGlobal()
	t.Cleanup(func() {
		log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
		for _, e := range hook.AllEntries() {
			if e.Level <= log.WarnLevel {
				t.Errorf("unexpected %s: %s %v", e.Level, e.Message, e.Data)
			}
		}
	})
}

// onlyID fails the test when progress is saved for any other request: an
// unexpected exec is only logged by Observe, so it would not fail on its own.
type onlyID struct {
	t    *testing.T
	want string
}

func (m onlyID) Match(v driver.Value) bool {
	if v != m.want {
		m.t.Errorf("progress saved for request %v; only %s may move", v, m.want)
	}
	return v == m.want
}

func TestDispatcher_ObserveRecordsTheInitialLoad(t *testing.T) {
	failOnWarning(t)
	d, mock, now := newTestDispatcher(t, running(nil), &fakeProducer{})
	conn := d.connect.(*fakeConnect)
	conn.cfg["table.include.list"] = `public\.users,public\.orders,public\.audit`
	read := now.Add(-20 * time.Second)

	mock.ExpectQuery(listForObserve).WithArgs("p1", sqlmock.AnyArg()).WillReturnRows(noRequests())
	mock.ExpectQuery(initialBlocked).WithArgs("p1", read).WillReturnRows(sqlmock.NewRows([]string{"x"}).AddRow(false))
	mock.ExpectQuery(insertInitial).
		WithArgs("p1", "rsync_cdc_p1", "blocking",
			`["public.users","public.orders","public.audit"]`, // the connector's tables, unescaped
			"started", `["public.users"]`, read, *now, *now, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("i1"))

	d.Observe("p1", "rsync_cdc_p1", []Observation{
		{Table: "public.users", Rows: 900, FirstSeen: read, LastSeen: read.Add(5 * time.Second), TableDone: true},
		{Table: "public.orders", Rows: 40, FirstSeen: read.Add(6 * time.Second), LastSeen: read.Add(8 * time.Second)},
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher_ObserveNamesAnIncrementalLoad(t *testing.T) {
	failOnWarning(t)
	d, mock, now := newTestDispatcher(t, running(nil), &fakeProducer{})
	read := now.Add(-time.Minute)
	mock.ExpectQuery(listForObserve).WillReturnRows(noRequests())
	mock.ExpectQuery(initialBlocked).WillReturnRows(sqlmock.NewRows([]string{"x"}).AddRow(false))
	// No include list in the config: the observed tables are the list.
	mock.ExpectQuery(insertInitial).
		WithArgs("p1", "rsync_cdc_p1", "incremental", `["public.users"]`, "started", `[]`, read, *now, *now, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("i1"))
	d.Observe("p1", "rsync_cdc_p1", []Observation{{Table: "public.users", Rows: 5, FirstSeen: read, LastSeen: read, Incremental: true}})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A regex include list names no tables: fall back to what was observed rather
// than report a load of "public..*".
func TestDispatcher_ObserveIgnoresARegexIncludeList(t *testing.T) {
	failOnWarning(t)
	d, mock, now := newTestDispatcher(t, running(nil), &fakeProducer{})
	d.connect.(*fakeConnect).cfg["table.include.list"] = `public\..*`
	read := now.Add(-time.Minute)
	mock.ExpectQuery(listForObserve).WillReturnRows(noRequests())
	mock.ExpectQuery(initialBlocked).WillReturnRows(sqlmock.NewRows([]string{"x"}).AddRow(false))
	mock.ExpectQuery(insertInitial).
		WithArgs("p1", "rsync_cdc_p1", "blocking", `["public.users"]`, "started", `[]`, read, *now, *now, nil).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("i1"))
	d.Observe("p1", "rsync_cdc_p1", []Observation{{Table: "public.users", Rows: 5, FirstSeen: read, LastSeen: read}})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Rows of a Re-snapshot are that request's, never a new full load — including
// the tail of one that has just completed.
func TestDispatcher_ObserveLeavesARequestsRowsToIt(t *testing.T) {
	failOnWarning(t)
	d, mock, now := newTestDispatcher(t, running(nil), &fakeProducer{})
	done := now.Add(-10 * time.Second)
	rows := noRequests().AddRow("r1", "p1", "rsync_cdc_p1", "blocking", `["public.users"]`, "resnapshot", "completed",
		1, `["public.users"]`, "", false, t0, t0, t0, t0, t0, done, done)
	mock.ExpectQuery(listForObserve).WillReturnRows(rows)
	// No InitialBlocked, no INSERT: sqlmock fails on an unexpected query.
	d.Observe("p1", "rsync_cdc_p1", []Observation{{Table: "public.users", Rows: 3, FirstSeen: done.Add(-time.Second), LastSeen: done.Add(-time.Second)}})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A consumer that starts from the beginning of a topic re-reads a load that
// finished long ago; that is not a load happening now.
func TestDispatcher_ObserveIgnoresOldRows(t *testing.T) {
	failOnWarning(t)
	d, mock, now := newTestDispatcher(t, running(nil), &fakeProducer{})
	mock.ExpectQuery(listForObserve).WillReturnRows(noRequests())
	old := now.Add(-InitialObserveWindow - time.Minute)
	d.Observe("p1", "rsync_cdc_p1", []Observation{{Table: "public.users", Rows: 3, FirstSeen: old, LastSeen: old, AllDone: true}})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher_ObserveDoesNotRecordASecondLoad(t *testing.T) {
	failOnWarning(t)
	d, mock, now := newTestDispatcher(t, running(nil), &fakeProducer{})
	read := now.Add(-time.Minute)
	mock.ExpectQuery(listForObserve).WillReturnRows(noRequests())
	mock.ExpectQuery(initialBlocked).WillReturnRows(sqlmock.NewRows([]string{"x"}).AddRow(true))
	d.Observe("p1", "rsync_cdc_p1", []Observation{{Table: "public.users", Rows: 3, FirstSeen: read, LastSeen: read}})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// An open initial load takes every table's rows and saves the table list it
// grew, so the page's "k / n tables" counts them.
func TestDispatcher_ObserveCreditsTheOpenInitialLoad(t *testing.T) {
	failOnWarning(t)
	d, mock, now := newTestDispatcher(t, running(nil), &fakeProducer{})
	sent := now.Add(-time.Minute)
	rows := noRequests().AddRow("i1", "p1", "rsync_cdc_p1", "blocking", `["public.users"]`, "initial", "started",
		0, `[]`, "", false, sent, sent, sent, sent, sent, sent, nil)
	mock.ExpectQuery(listForObserve).WillReturnRows(rows)
	mock.ExpectExec(regexp.QuoteMeta(`SET status = $2, started_at = $3, last_progress_at = $4`)).
		WithArgs("i1", "started", sqlmock.AnyArg(), *now, `["public.users"]`, nil, `["public.users","public.orders"]`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	d.Observe("p1", "rsync_cdc_p1", []Observation{
		{Table: "public.users", Rows: 3, FirstSeen: sent, LastSeen: now.Add(-time.Second), TableDone: true},
		{Table: "public.orders", Rows: 3, FirstSeen: sent, LastSeen: now.Add(-time.Second)},
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Nothing re-sends an initial load: there is no signal to send.
func TestDispatcher_TickNeverSendsAnInitialLoad(t *testing.T) {
	prod := &fakeProducer{}
	d, mock, now := newTestDispatcher(t, running(nil), prod)
	*now = t0.Add(3 * time.Hour)
	d.startedAt = t0
	for i := 0; i < 2; i++ {
		rows := noRequests().AddRow("i1", "p1", "rsync_cdc_p1", "blocking", `["public.users"]`, "initial", "sent",
			0, `[]`, "", false, t0, t0, t0, t0, nil, nil, nil)
		mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(rows)
		if err := d.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(DefaultTiming.ReadyStable)
	}
	if len(prod.sent) != 0 || len(d.readySince) != 0 {
		t.Fatal("the dispatcher treated an initial load as a signal to send")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A Re-snapshot running while the initial load is open keeps its rows: its
// "last" marker is not the end of the load.
func TestDispatcher_ObserveKeepsARequestsRowsFromTheInitialLoad(t *testing.T) {
	failOnWarning(t)
	d, mock, now := newTestDispatcher(t, running(nil), &fakeProducer{})
	sent := now.Add(-time.Minute)
	rows := noRequests().
		AddRow("i1", "p1", "rsync_cdc_p1", "incremental", `["public.users","public.orders"]`, "initial", "started",
			0, `[]`, "", false, sent, sent, sent, sent, sent, sent, nil).
		AddRow("r1", "p1", "rsync_cdc_p1", "blocking", `["public.orders"]`, "resnapshot", "sent",
			1, `[]`, "", false, sent, sent, sent, sent, nil, nil, nil)
	mock.ExpectQuery(listForObserve).WillReturnRows(rows)
	mock.ExpectExec(regexp.QuoteMeta(`SET status = $2, started_at = $3, last_progress_at = $4`)).
		WithArgs(onlyID{t, "r1"}, "completed", *now, *now, `["public.orders"]`, *now, `["public.orders"]`).
		WillReturnResult(sqlmock.NewResult(0, 1))
	d.Observe("p1", "rsync_cdc_p1", []Observation{
		{Table: "public.orders", Rows: 3, FirstSeen: sent, LastSeen: now.Add(-time.Second), TableDone: true, AllDone: true},
	})
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// --- Store.BeginInitial and wrap ---------------------------------------------

// Two runs of one pipeline can both pass BeginInitial's supersede before either
// inserts, and migration 118's unique index lets one open load through. The
// other stands down with ErrInitialLoadOpen, the way InsertInitial does, rather
// than surfacing a unique violation. store_pg_test.go races it for real.
func TestBeginInitial_LosingARaceIsNotAnError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE cdc_snapshot_requests\s+SET status = 'unconfirmed'`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(insertInitial + `[\s\S]*ON CONFLICT DO NOTHING\s+RETURNING`).WillReturnRows(noRequests())
	mock.ExpectRollback()

	_, err = NewStore(db).BeginInitial(context.Background(), Request{PipelineID: "p1", ConnectorName: "c", Mode: "blocking"})
	if !errors.Is(err, ErrInitialLoadOpen) {
		t.Fatalf("losing the race returned %v, want ErrInitialLoadOpen", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

type sqlStateErr struct{ code, msg string }

func (e sqlStateErr) Error() string    { return e.msg }
func (e sqlStateErr) SQLState() string { return e.code }

// A gateway older than migration 118 rejects source 'initial' with a CHECK
// violation, which means "not recorded yet". Any other error that happens to
// name the constraint is a real error.
func TestWrap_OnlyTheSourceCheckMeansAnOlderSchema(t *testing.T) {
	const named = `new row for relation "cdc_snapshot_requests" violates check constraint "cdc_snapshot_requests_source_check"`
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{"the CHECK violation", sqlStateErr{"23514", named}, true},
		{"a driver that reports no SQLSTATE", errors.New("pq: " + named), true},
		{"another error naming the constraint", sqlStateErr{"23505", "duplicate key; detail names cdc_snapshot_requests_source_check"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := errors.Is(wrap(tc.err), ErrUnavailable); got != tc.want {
				t.Fatalf("wrap(%v) unavailable = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
