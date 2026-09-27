package executor

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"

	"github.com/rsync-ai/backend-orchestrator/internal/cdcsnapshot"
)

// A hybrid pipeline's Debezium connector streams from P and never snapshots, so
// no snapshot row records its initial load: the executor has to, or the page
// can never say "Full load in progress" / "Load completed".

const hybridLoadPipeline = "33333333-3333-3333-3333-333333333333"

var (
	hybridSupersedeSQL = regexp.QuoteMeta(`UPDATE cdc_snapshot_requests`) + `\s+SET status = 'unconfirmed'`
	hybridInsertSQL    = regexp.QuoteMeta(`INSERT INTO cdc_snapshot_requests`)
	hybridFinishSQL    = regexp.QuoteMeta(`UPDATE cdc_snapshot_requests`) + `\s+SET status = \$3`
)

func hybridLoadTask() ExecutorTask {
	return ExecutorTask{PipelineID: hybridLoadPipeline, Params: map[string]interface{}{
		"tables": []interface{}{"public.orders", "public.customers"},
	}}
}

var hybridRequestCols = []string{"id", "pipeline_id", "connector_name", "mode", "tables", "source", "status",
	"attempts", "completed_tables", "last_error", "cleans_folder",
	"not_before", "requested_at", "sent_at", "last_sent_at", "started_at", "last_progress_at", "completed_at"}

func hybridStartedRow(mock sqlmock.Sqlmock) {
	now := time.Now()
	mock.ExpectQuery(hybridInsertSQL).
		WithArgs(hybridLoadPipeline, "cdc-33333333", "blocking", `["public.orders","public.customers"]`).
		WillReturnRows(sqlmock.NewRows(hybridRequestCols).
			AddRow("load-1", hybridLoadPipeline, "cdc-33333333", "blocking", `["public.orders","public.customers"]`, "initial", "started",
				0, "[]", "", false, now, now, now, now, now, nil, nil))
}

func TestHybridInitialLoad_RecordedAroundTheBatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ok      bool
		status  string
		message interface{}
	}{
		{"batch landed", true, cdcsnapshot.StatusCompleted, nil},
		{"batch failed", false, cdcsnapshot.StatusFailed, hybridLoadFailedMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			a := &Agent{db: db}

			mock.ExpectBegin()
			mock.ExpectExec(hybridSupersedeSQL).WithArgs(hybridLoadPipeline, cdcsnapshot.SupersededMessage).
				WillReturnResult(sqlmock.NewResult(0, 1))
			hybridStartedRow(mock)
			mock.ExpectCommit()
			mock.ExpectExec(hybridFinishSQL).WithArgs("load-1", cdcsnapshot.StatusStarted, tc.status, tc.message).
				WillReturnResult(sqlmock.NewResult(0, 1))

			load := a.beginHybridInitialLoad(context.Background(), hybridLoadTask(), "cdc-33333333")
			if load == nil {
				t.Fatal("the batch load was not recorded")
			}
			a.finishHybridInitialLoad(context.Background(), load, tc.ok)
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// last_error is shown on the page: it names the failure, never the batch
// error text, which can quote row values.
func TestHybridInitialLoad_FailureMessageCarriesNoRowData(t *testing.T) {
	if strings.ContainsAny(hybridLoadFailedMessage, "%{}") || hybridLoadFailedMessage == "" {
		t.Fatalf("hybridLoadFailedMessage must be a fixed sentence: %q", hybridLoadFailedMessage)
	}
}

// Recording is best-effort: no database, or a queue not migrated yet, never
// stops the load.
func TestHybridInitialLoad_NotRecordedWithoutTheQueue(t *testing.T) {
	(&Agent{}).finishHybridInitialLoad(context.Background(), (&Agent{}).beginHybridInitialLoad(context.Background(), hybridLoadTask(), "c"), true)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec(hybridSupersedeSQL).WillReturnError(errors.New(`pq: relation "cdc_snapshot_requests" does not exist`))
	mock.ExpectRollback()
	a := &Agent{db: db}
	if load := a.beginHybridInitialLoad(context.Background(), hybridLoadTask(), "c"); load != nil {
		t.Fatalf("got %+v without a queue", load)
	}
	a.finishHybridInitialLoad(context.Background(), nil, false)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func captureHybridLog(t *testing.T) *logtest.Hook {
	t.Helper()
	hook := logtest.NewGlobal()
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(make(log.LevelHooks)) })
	return hook
}

// Losing BeginInitial's race to another run of the pipeline is not a failure to
// record: the other run's load is the one on the page, so nothing is recorded
// and nothing is warned about.
func TestHybridInitialLoad_LosingARaceIsNotAWarning(t *testing.T) {
	hook := captureHybridLog(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectExec(hybridSupersedeSQL).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(hybridInsertSQL).WillReturnRows(sqlmock.NewRows(hybridRequestCols))
	mock.ExpectRollback()

	a := &Agent{db: db}
	if load := a.beginHybridInitialLoad(context.Background(), hybridLoadTask(), "cdc-33333333"); load != nil {
		t.Fatalf("recorded %+v after losing the race", load)
	}
	for _, e := range hook.AllEntries() {
		if e.Level <= log.WarnLevel {
			t.Errorf("losing the race logged %s: %s", e.Level, e.Message)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Finish matching no row means something else closed this run's load first (a
// newer run superseded it), so this run's outcome is not on the page. Say so;
// the control is a Finish that lands, which says nothing.
func TestHybridInitialLoad_AnAlreadyClosedLoadIsLogged(t *testing.T) {
	for _, tc := range []struct {
		name    string
		matched int64
		warn    bool
	}{
		{"closed by another run", 0, true},
		{"control: closed by this run", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := captureHybridLog(t)
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			mock.ExpectExec(hybridFinishSQL).WillReturnResult(sqlmock.NewResult(0, tc.matched))

			a := &Agent{db: db}
			a.finishHybridInitialLoad(context.Background(), &cdcsnapshot.Request{
				ID: "load-1", PipelineID: hybridLoadPipeline, Status: cdcsnapshot.StatusStarted,
			}, true)
			warned := false
			for _, e := range hook.AllEntries() {
				warned = warned || (e.Level == log.WarnLevel && e.Data["pipeline_id"] == hybridLoadPipeline)
			}
			if warned != tc.warn {
				t.Fatalf("warned = %v, want %v", warned, tc.warn)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
