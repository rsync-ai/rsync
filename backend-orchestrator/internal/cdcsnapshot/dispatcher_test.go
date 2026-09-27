package cdcsnapshot

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type fakeConnect struct {
	state ConnectorState
	cfg   map[string]interface{}
}

func (f *fakeConnect) State(context.Context, string) (ConnectorState, error) { return f.state, nil }
func (f *fakeConnect) Config(context.Context, string) (map[string]interface{}, error) {
	return f.cfg, nil
}

type produced struct{ topic, key, value string }

type fakeProducer struct {
	sent []produced
	err  error
}

func (f *fakeProducer) EnsureSignalTopic(string) error { return nil }
func (f *fakeProducer) ProduceWithContext(_ context.Context, topic string, key, value []byte) error {
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, produced{topic, string(key), string(value)})
	return nil
}

var requestCols = []string{"id", "pipeline_id", "connector_name", "mode", "tables", "source", "status",
	"attempts", "completed_tables", "last_error", "cleans_folder",
	"not_before", "requested_at", "sent_at", "last_sent_at", "started_at", "last_progress_at", "completed_at"}

func queuedRow(cleans bool) *sqlmock.Rows {
	return sqlmock.NewRows(requestCols).AddRow("r1", "p1", "rsync_cdc_p1", "blocking", `["public.users"]`, "table_edit", "queued",
		0, `[]`, "", cleans, t0, t0, nil, nil, nil, nil, nil)
}

func newTestDispatcher(t *testing.T, cs ConnectorState, prod *fakeProducer) (*Dispatcher, sqlmock.Sqlmock, *time.Time) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	conn := &fakeConnect{state: cs, cfg: map[string]interface{}{
		"signal.kafka.topic": "rsync.signals.p1",
		"topic.prefix":       "rsync_cdc_p1",
	}}
	d := NewDispatcher(NewStore(db), prod, conn, true)
	now := t0.Add(time.Minute)
	d.now = func() time.Time { return now }
	// A dispatcher that has been running since before any request; the restart
	// grace has its own test.
	d.startedAt = t0.Add(-time.Hour)
	return d, mock, &now
}

// The queue's reason to exist: while the old task (without the new table) still
// runs, nothing is sent; once the new task runs for ReadyStable, the markers
// are armed and THEN the signal is produced.
func TestDispatcher_WaitsForTheNewTaskThenArmsMarkersThenSends(t *testing.T) {
	prod := &fakeProducer{}
	d, mock, now := newTestDispatcher(t, running([]string{`public\.orders`}), prod)
	conn := d.connect.(*fakeConnect)

	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(true))
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(prod.sent) != 0 || len(d.readySince) != 0 {
		t.Fatal("the connector counted as ready while its running task does not capture the table")
	}

	conn.state = running([]string{`public\.orders`, `public\.users`})
	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(true))
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(prod.sent) != 0 {
		t.Fatal("sent before the connector stayed ready for ReadyStable")
	}

	*now = now.Add(DefaultTiming.ReadyStable)
	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(true))
	mock.ExpectExec(regexp.QuoteMeta(`SET status = 'sent'`)).WithArgs("r1", "queued", 0).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO cdc_object_reload_requests`).WithArgs("p1", "rsync_cdc_p1.public.users", "r1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(prod.sent) != 1 {
		t.Fatalf("want one signal, got %d", len(prod.sent))
	}
	got := prod.sent[0]
	if got.topic != "rsync.signals.p1" || got.key != "rsync_cdc_p1" ||
		got.value != `{"data":{"data-collections":["public.users"],"type":"BLOCKING"},"type":"execute-snapshot"}` {
		t.Fatalf("signal = %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher_ProduceFailureUndoesTheClaim(t *testing.T) {
	prod := &fakeProducer{err: errors.New("broker down")}
	d, mock, now := newTestDispatcher(t, running([]string{`public\.users`}), prod)

	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(false))
	_ = d.Tick(context.Background())
	*now = now.Add(DefaultTiming.ReadyStable)
	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(false))
	mock.ExpectExec(regexp.QuoteMeta(`SET status = 'sent'`)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta(`attempts = GREATEST(attempts - 1, 0)`)).
		WithArgs("r1", "queued", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcher_PausedConnectorNeverFailsTheRequest(t *testing.T) {
	prod := &fakeProducer{}
	d, mock, now := newTestDispatcher(t, ConnectorState{Found: true, State: "PAUSED"}, prod)
	*now = t0.Add(time.Hour)
	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(false))
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	// No Finish(failed) expected: sqlmock fails on an unexpected Exec.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if len(prod.sent) != 0 {
		t.Fatal("sent to a paused connector")
	}
}

func TestDispatcher_MissingConnectorFailsAfterTheDeadline(t *testing.T) {
	prod := &fakeProducer{}
	d, mock, now := newTestDispatcher(t, ConnectorState{Found: false}, prod)
	*now = t0.Add(DefaultTiming.FailAfter)
	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(queuedRow(false))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE cdc_snapshot_requests`)).
		WithArgs("r1", "queued", "failed", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// A sent object-storage request whose markers the sink already consumed did
// start; re-sending it would empty the folder a second time.
func TestDispatcher_ConsumedMarkersMeanStartedNotResend(t *testing.T) {
	prod := &fakeProducer{}
	d, mock, now := newTestDispatcher(t, running(nil), prod)
	*now = t0.Add(DefaultTiming.SendTimeout + time.Second)
	row := sqlmock.NewRows(requestCols).AddRow("r1", "p1", "rsync_cdc_p1", "blocking", `["public.users"]`, "resnapshot", "sent",
		1, `[]`, "", true, t0, t0, t0, t0, nil, nil, nil)
	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(row)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM cdc_object_reload_requests`).
		WithArgs("p1", `["rsync_cdc_p1.public.users"]`).WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(0))
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE cdc_snapshot_requests`)).
		WithArgs("r1", "sent", "started", nil).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if len(prod.sent) != 0 {
		t.Fatal("re-sent a snapshot the sink had already started writing")
	}
}

// A blocking snapshot still running while the orchestrator restarted must not
// be sent again (and its folder emptied again) just because the downtime read
// as a stall. Its clocks restart at the dispatcher's first tick.
func TestDispatcher_RestartDoesNotReadDowntimeAsAStall(t *testing.T) {
	prod := &fakeProducer{}
	d, mock, now := newTestDispatcher(t, running(nil), prod)
	d.startedAt = time.Time{} // a fresh process
	*now = t0.Add(DefaultTiming.StallTimeout + 30*time.Minute)
	restart := *now
	// Two ticks ReadyStable apart: a resend would start its readiness streak on
	// the first and send on the second.
	for i := 0; i < 2; i++ {
		row := sqlmock.NewRows(requestCols).AddRow("r1", "p1", "rsync_cdc_p1", "blocking", `["public.users"]`, "resnapshot", "started",
			1, `[]`, "", true, t0, t0, t0, t0, t0, t0, nil)
		mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(row)
		if err := d.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		if len(d.readySince) != 0 || len(prod.sent) != 0 {
			t.Fatal("started re-sending a blocking snapshot because the orchestrator was down")
		}
		*now = now.Add(DefaultTiming.ReadyStable)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if !d.startedAt.Equal(restart) {
		t.Fatalf("startedAt = %v, want the first tick %v", d.startedAt, restart)
	}
}

// The grace is one StallTimeout from the restart, not forever.
func TestClampToStart_StallStillFiresAfterTheGrace(t *testing.T) {
	start := t0.Add(time.Hour)
	last := t0
	r := Request{Status: StatusStarted, Mode: "blocking", Attempts: 1, SentAt: &last, LastSentAt: &last, LastProgressAt: &last}
	if a, _ := Watch(ClampToStart(r, start), start.Add(DefaultTiming.StallTimeout-time.Second), true, DefaultTiming); a != ActNone {
		t.Fatalf("inside the grace: action %v, want none", a)
	}
	if a, _ := Watch(ClampToStart(r, start), start.Add(DefaultTiming.StallTimeout), true, DefaultTiming); a != ActResend {
		t.Fatalf("after the grace: action %v, want resend", a)
	}
	// Clocks newer than the start are left alone.
	fresh := start.Add(time.Minute)
	r.LastProgressAt = &fresh
	if got := ClampToStart(r, start).LastProgressAt; !got.Equal(fresh) {
		t.Fatalf("moved a clock newer than the start: %v", got)
	}
	// The caller's request is not modified.
	if !r.SentAt.Equal(t0) {
		t.Fatal("ClampToStart modified its argument")
	}
}

// A blocking snapshot that started and then stalled on every attempt aborted
// (BLOCKING-SNAPSHOT-PORTAL on prod: three attempts, each emptied the folder and
// died part-way). It closes as failed, and is not sent a fourth time.
func TestDispatcher_StalledBlockingSnapshotOutOfAttemptsFails(t *testing.T) {
	prod := &fakeProducer{}
	d, mock, now := newTestDispatcher(t, running(nil), prod)
	*now = t0.Add(DefaultTiming.StallTimeout + time.Second)
	row := sqlmock.NewRows(requestCols).AddRow("r1", "p1", "rsync_cdc_p1", "blocking", `["public.users"]`, "resnapshot", "started",
		DefaultTiming.MaxAttempts, `[]`, "", true, t0, t0, t0, t0, t0, t0, nil)
	mock.ExpectQuery(`FROM cdc_snapshot_requests`).WillReturnRows(row)
	mock.ExpectExec(regexp.QuoteMeta(`UPDATE cdc_snapshot_requests`)).
		WithArgs("r1", "started", "failed", sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := d.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if len(prod.sent) != 0 {
		t.Fatal("re-sent a blocking snapshot that is out of attempts")
	}
}
