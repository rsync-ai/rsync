package handlers

// Tests for the orphan sink-worker reaper (KI-CDC-SINK-WORKER-NO-REAPER). The bug class:
// a sink-worker stop that fails during a pipeline Stop or Delete is never tried again,
// so the worker keeps writing for a pipeline that is gone. The safety half matters as
// much: the reaper must never stop a worker unless the database positively says its
// pipeline is gone.

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

const (
	reaperOrphanPID  = "abd8a64d-1f2e-4c3b-9a7d-5e6f70819234"
	reaperRunningPID = "c228373b-0000-4000-8000-000000000001" // stands in for the kept prod pipeline
	reaperOrphanGrp  = "sink-abd8a64d"
	reaperRunningGrp = "sink-c228373b-stream"
	reaperOwnerQuery = `SELECT status, updated_at < NOW\(\) - make_interval\(secs => \$2\) FROM pipelines WHERE id = \$1::uuid`
)

// fakeReaperSinks answers list_sinks with workers and records stop_sink calls.
type fakeReaperSinks struct {
	mu       sync.Mutex
	workers  []map[string]interface{}
	listErr  error
	stopFail func(group string) string
	stops    []string
}

func (f *fakeReaperSinks) ExecuteWithContext(_ context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch req.Operation {
	case "list_sinks":
		if f.listErr != nil {
			return nil, f.listErr
		}
		ws := make([]interface{}, 0, len(f.workers))
		for _, w := range f.workers {
			ws = append(ws, w)
		}
		return &mcp.ExecuteResponse{Success: true, Result: map[string]interface{}{"workers": ws}}, nil
	case "stop_sink":
		cfg, _ := req.Params["config"].(map[string]interface{})
		group, _ := cfg["consumer_group"].(string)
		f.stops = append(f.stops, group)
		if f.stopFail != nil {
			if why := f.stopFail(group); why != "" {
				return &mcp.ExecuteResponse{Success: false, Error: why}, nil
			}
		}
		return &mcp.ExecuteResponse{Success: true}, nil
	}
	return &mcp.ExecuteResponse{Success: false, Error: "unexpected operation " + req.Operation}, nil
}

func (f *fakeReaperSinks) stopped() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.stops...)
}

func reaperWorker(group, pid string) map[string]interface{} {
	return map[string]interface{}{"consumer_group": group, "pipeline_id": pid}
}

func newReaperDB(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func expectOwnerDeleted(mock sqlmock.Sqlmock, pid string) {
	mock.ExpectQuery(reaperOwnerQuery).WithArgs(pid, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"status", "settled"}))
}

func expectOwnerStatus(mock sqlmock.Sqlmock, pid, status string, settled bool) {
	mock.ExpectQuery(reaperOwnerQuery).WithArgs(pid, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"status", "settled"}).AddRow(status, settled))
}

func reaperCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// A delete whose stop_sink failed left a worker running. The reaper stops it on the
// second look — and never touches the running pipeline's worker beside it.
func TestSinkWorkerReaper_StopsTheWorkerOfADeletedPipelineAndNothingElse(t *testing.T) {
	db, mock := newReaperDB(t)
	sinks := &fakeReaperSinks{workers: []map[string]interface{}{
		reaperWorker(reaperOrphanGrp, reaperOrphanPID),
		reaperWorker(reaperRunningGrp, reaperRunningPID),
	}}
	r := newSinkWorkerReaper(db, sinks, sinkReaperEnforce, time.Minute, time.Minute)
	ctx := reaperCtx(t)

	expectOwnerDeleted(mock, reaperOrphanPID)
	expectOwnerStatus(mock, reaperRunningPID, "running", true)
	if acts := r.Tick(ctx); len(acts) != 0 || len(sinks.stopped()) != 0 {
		t.Fatalf("first look must only record a suspect; actions=%+v stops=%v", acts, sinks.stopped())
	}

	expectOwnerDeleted(mock, reaperOrphanPID)
	expectOwnerDeleted(mock, reaperOrphanPID) // the re-check right before the stop
	expectOwnerStatus(mock, reaperRunningPID, "running", true)
	acts := r.Tick(ctx)
	if got := sinks.stopped(); !reflect.DeepEqual(got, []string{reaperOrphanGrp}) {
		t.Fatalf("stop_sink calls = %v, want only the deleted pipeline's group %q", got, reaperOrphanGrp)
	}
	if len(acts) != 1 || !acts[0].Stopped || acts[0].Reason != "pipeline deleted" {
		t.Fatalf("actions = %+v", acts)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// The retry: a stop that fails is tried again on the next tick until it lands.
func TestSinkWorkerReaper_RetriesAStopThatFailed(t *testing.T) {
	db, mock := newReaperDB(t)
	failures := 1
	sinks := &fakeReaperSinks{
		workers: []map[string]interface{}{reaperWorker(reaperOrphanGrp, reaperOrphanPID)},
		stopFail: func(string) string {
			if failures > 0 {
				failures--
				return "worker did not exit after SIGTERM"
			}
			return ""
		},
	}
	r := newSinkWorkerReaper(db, sinks, sinkReaperEnforce, time.Minute, time.Minute)
	ctx := reaperCtx(t)

	expectOwnerDeleted(mock, reaperOrphanPID)
	r.Tick(ctx)
	expectOwnerDeleted(mock, reaperOrphanPID)
	expectOwnerDeleted(mock, reaperOrphanPID)
	if acts := r.Tick(ctx); len(acts) != 1 || acts[0].Stopped || acts[0].Failure == "" {
		t.Fatalf("second tick should try and fail; actions=%+v", acts)
	}
	expectOwnerDeleted(mock, reaperOrphanPID)
	expectOwnerDeleted(mock, reaperOrphanPID)
	if acts := r.Tick(ctx); len(acts) != 1 || !acts[0].Stopped {
		t.Fatalf("third tick should retry and stop; actions=%+v", acts)
	}
	if got := sinks.stopped(); len(got) != 2 {
		t.Fatalf("stop_sink calls = %v, want 2 (the failure, then the retry)", got)
	}
}

// A CDC Stop whose stop_sink failed: status 'stopped' past the grace period is gone.
func TestSinkWorkerReaper_StopsTheWorkerOfAPipelineStoppedPastTheGrace(t *testing.T) {
	db, mock := newReaperDB(t)
	sinks := &fakeReaperSinks{workers: []map[string]interface{}{reaperWorker(reaperOrphanGrp, reaperOrphanPID)}}
	r := newSinkWorkerReaper(db, sinks, sinkReaperEnforce, time.Minute, time.Minute)
	ctx := reaperCtx(t)
	expectOwnerStatus(mock, reaperOrphanPID, "stopped", true)
	r.Tick(ctx)
	expectOwnerStatus(mock, reaperOrphanPID, "stopped", true)
	expectOwnerStatus(mock, reaperOrphanPID, "stopped", true)
	if acts := r.Tick(ctx); len(acts) != 1 || !acts[0].Stopped || acts[0].Reason != "pipeline stopped" {
		t.Fatalf("actions = %+v", acts)
	}
}

// Every answer that is not a positive "gone" keeps the worker, across many ticks.
func TestSinkWorkerReaper_NeverStopsWithoutAPositiveGoneAnswer(t *testing.T) {
	cases := []struct {
		name   string
		pid    string
		expect func(sqlmock.Sqlmock)
	}{
		{"running pipeline", reaperRunningPID, func(m sqlmock.Sqlmock) { expectOwnerStatus(m, reaperRunningPID, "running", true) }},
		{"active pipeline", reaperRunningPID, func(m sqlmock.Sqlmock) { expectOwnerStatus(m, reaperRunningPID, "active", true) }},
		{"paused pipeline", reaperRunningPID, func(m sqlmock.Sqlmock) { expectOwnerStatus(m, reaperRunningPID, "paused", true) }},
		{"stopped inside the grace period", reaperRunningPID, func(m sqlmock.Sqlmock) { expectOwnerStatus(m, reaperRunningPID, "stopped", false) }},
		{"lookup error", reaperRunningPID, func(m sqlmock.Sqlmock) {
			m.ExpectQuery(reaperOwnerQuery).WillReturnError(errors.New("connection refused"))
		}},
		{"pipeline id is not a UUID", "sink-legacy", func(sqlmock.Sqlmock) {}},
		{"no pipeline id", "", func(sqlmock.Sqlmock) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newReaperDB(t)
			sinks := &fakeReaperSinks{workers: []map[string]interface{}{reaperWorker(reaperRunningGrp, tc.pid)}}
			r := newSinkWorkerReaper(db, sinks, sinkReaperEnforce, time.Minute, time.Minute)
			for i := 0; i < 3; i++ {
				tc.expect(mock)
				r.Tick(reaperCtx(t))
			}
			if got := sinks.stopped(); len(got) != 0 {
				t.Fatalf("stopped %v; a worker must be kept unless the DB says its pipeline is gone", got)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// A pipeline started between the second look and the stop is caught by the re-check.
func TestSinkWorkerReaper_RechecksRightBeforeStopping(t *testing.T) {
	db, mock := newReaperDB(t)
	sinks := &fakeReaperSinks{workers: []map[string]interface{}{reaperWorker(reaperOrphanGrp, reaperOrphanPID)}}
	r := newSinkWorkerReaper(db, sinks, sinkReaperEnforce, time.Minute, time.Minute)
	ctx := reaperCtx(t)
	expectOwnerStatus(mock, reaperOrphanPID, "stopped", true)
	r.Tick(ctx)
	expectOwnerStatus(mock, reaperOrphanPID, "stopped", true)
	expectOwnerStatus(mock, reaperOrphanPID, "running", true)
	r.Tick(ctx)
	if got := sinks.stopped(); len(got) != 0 {
		t.Fatalf("stopped %v after the re-check said running", got)
	}
}

// A failed listing forgets the suspects: two SUCCESSFUL looks in a row are required.
func TestSinkWorkerReaper_AFailedListingResetsTheSightings(t *testing.T) {
	db, mock := newReaperDB(t)
	sinks := &fakeReaperSinks{workers: []map[string]interface{}{reaperWorker(reaperOrphanGrp, reaperOrphanPID)}}
	r := newSinkWorkerReaper(db, sinks, sinkReaperEnforce, time.Minute, time.Minute)
	ctx := reaperCtx(t)
	expectOwnerDeleted(mock, reaperOrphanPID)
	r.Tick(ctx)
	sinks.listErr = errors.New("sink service unreachable")
	r.Tick(ctx)
	sinks.listErr = nil
	expectOwnerDeleted(mock, reaperOrphanPID)
	r.Tick(ctx)
	if got := sinks.stopped(); len(got) != 0 {
		t.Fatalf("stopped %v after a failed listing broke the run of sightings", got)
	}
}

// Dry run reports and never stops.
func TestSinkWorkerReaper_DryRunNeverStops(t *testing.T) {
	db, mock := newReaperDB(t)
	sinks := &fakeReaperSinks{workers: []map[string]interface{}{reaperWorker(reaperOrphanGrp, reaperOrphanPID)}}
	r := newSinkWorkerReaper(db, sinks, sinkReaperDryRun, time.Minute, time.Minute)
	ctx := reaperCtx(t)
	var acts []sinkReapAction
	for i := 0; i < 3; i++ {
		expectOwnerDeleted(mock, reaperOrphanPID)
		acts = r.Tick(ctx)
	}
	if got := sinks.stopped(); len(got) != 0 {
		t.Fatalf("dry run stopped %v", got)
	}
	if len(acts) != 1 || acts[0].Stopped || acts[0].ConsumerGroup != reaperOrphanGrp {
		t.Fatalf("dry run should report the orphan; actions=%+v", acts)
	}
}

// Dormant by default: unset, empty, "false" and typos are all off.
func TestSinkWorkerReaper_ModeDefaultsOff(t *testing.T) {
	for in, want := range map[string]sinkReaperMode{
		"": sinkReaperOff, "off": sinkReaperOff, "false": sinkReaperOff, "true": sinkReaperOff,
		"enforced": sinkReaperOff, "dry_run": sinkReaperDryRun, "DRY-RUN": sinkReaperDryRun,
		" enforce ": sinkReaperEnforce,
	} {
		if got := parseSinkReaperMode(in); got != want {
			t.Errorf("parseSinkReaperMode(%q) = %v, want %v", in, got, want)
		}
	}
	t.Setenv("SINK_WORKER_REAPER_MODE", "")
	if r := NewSinkWorkerReaperFromEnv(&sql.DB{}, mcp.NewServerManager(t.TempDir())); r != nil {
		t.Fatal("an unset SINK_WORKER_REAPER_MODE must start no reaper")
	}
}
