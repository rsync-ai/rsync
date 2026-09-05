package sentinel

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func newBatchSentinel(t *testing.T) (*BatchSentinel, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	// kafkaManager nil ⇒ emitBatchIssue persists the issue and skips the domain event,
	// which is exactly the surface these tests care about.
	s := &BatchSentinel{db: db, stallThreshold: DefaultBatchStallThreshold}
	return s, mock, func() { _ = db.Close() }
}

func noOpenIssues(mock sqlmock.Sqlmock) {
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
}

// The stall predicate carries four load-bearing clauses. Each one, if dropped, turns
// the sentinel from useful into actively harmful, and none of them is obvious from
// reading the surrounding Go — so they are asserted here rather than left to review.
func TestStalledBatchRunsQueryKeepsItsCarveOuts(t *testing.T) {
	q := stalledBatchRunsQuery

	// 1. CDC is carved out. Without this the batch sentinel would raise a stall on
	//    every healthy idle CDC stream, which by design sits quiet between changes.
	if !strings.Contains(q, "sync_mode IS DISTINCT FROM 'cdc'") || !strings.Contains(q, "cdc_mode IS NULL") {
		t.Error("stall query no longer excludes CDC pipelines")
	}

	// 2. Runs parked at the table-selection HITL are excluded. That park is the single
	//    most common "pipeline hangs" report, and it is a run WAITING FOR A HUMAN — not
	//    a stall. Alerting on it would bury real stalls under known-benign noise.
	if !strings.Contains(q, "waiting_for_user") {
		t.Error("stall query no longer excludes runs parked at HITL")
	}

	// 3. Quiet on EVERY surface, via GREATEST over start_time / progress / table stats.
	//    Keying on any single surface would fire while another was still advancing.
	if !strings.Contains(q, "GREATEST") {
		t.Error("stall query no longer requires quiet across all progress surfaces")
	}

	// 4. pipeline_progress.updated_at is a bare TIMESTAMP written by a trigger calling
	//    NOW() in the session timezone. `AT TIME ZONE 'UTC'` would shift it by the
	//    server's offset and invent (or hide) stalls purely as a function of where the
	//    database happens to be deployed.
	if strings.Contains(q, "AT TIME ZONE") {
		t.Error("stall query reasserts a timezone on pipeline_progress.updated_at; use ::timestamptz")
	}
	if !strings.Contains(q, "pp.updated_at::timestamptz") {
		t.Error("stall query no longer casts the naive pipeline_progress.updated_at")
	}
}

// A negative ack is rows_written=0 WITH a reason. Either half alone is not the signal:
// a zero-row batch with no error is an empty chunk, and an error with rows written is
// a partial success the run already accounts for.
func TestNegativeAckQueryRequiresBothHalvesOfTheSignal(t *testing.T) {
	q := negativeAckQuery
	if !strings.Contains(q, "a.rows_written = 0") {
		t.Error("ack query no longer requires rows_written = 0")
	}
	if !strings.Contains(q, `COALESCE(a.last_error, '') <> ''`) {
		t.Error("ack query no longer requires a recorded reason")
	}
	// Only in-flight runs: a finished run's rejections are the executor's post-mortem
	// to report, and re-raising them here would duplicate every past failure.
	if !strings.Contains(q, "e.status      = 'running'") || !strings.Contains(q, "e.end_time   IS NULL") {
		t.Error("ack query no longer scopes to in-flight executions")
	}
	// pipeline_batch_acks.execution_id is UUID: created VARCHAR(255) by migration 036,
	// converted by 043_batch_table_foreign_keys.sql:37-46 before the FK to executions(id).
	// executions.id is UUID. So the join must compare uuid to uuid.
	//
	// This assertion previously demanded `e.id::text = a.execution_id`, which is
	// `text = uuid` — no such operator, so ackDrainTick errored on every 60s tick and
	// this whole detector shipped dead while the suite stayed green. That is the lesson
	// worth keeping: sqlmock matches the SQL STRING and never the column types, so it
	// will happily confirm a query the database cannot run. A string assertion can pin
	// the shape of a query; only an integration test can prove it executes.
	if !strings.Contains(q, "e.id = a.execution_id") {
		t.Error("ack query no longer joins executions on the uuid key")
	}
	if strings.Contains(q, "e.id::text = a.execution_id") {
		t.Error("ack query reintroduced the text/uuid join — this errors on every tick")
	}
}

func TestProgressTickEmitsStallIssue(t *testing.T) {
	s, mock, done := newBatchSentinel(t)
	defer done()

	mock.ExpectQuery(`FROM pipelines p`).WillReturnRows(
		sqlmock.NewRows([]string{"id", "name", "execution_id", "last_movement", "status", "stage"}).
			AddRow("p1", "nightly orders", "e1", time.Now().Add(-45*time.Minute), "running", "executor"))
	mock.ExpectExec(`INSERT INTO sentinel_active_issues`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	// resolveStaleIssues sees the same issue still open, so nothing is cleared.
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(stallIssueID("p1")))
	// A DELETE here would mean the resolver cleared the issue the INSERT above
	// just raised. This expectation exists only so the test can observe that
	// call; it must stay UNFULFILLED. See the assertion below for why it is
	// written as a deliberate negative rather than simply omitted.
	mock.ExpectExec(`DELETE FROM sentinel_active_issues`).
		WillReturnResult(sqlmock.NewResult(0, 1))

	s.progressTick(context.Background())

	// Read backwards on purpose. An UNEXPECTED call is invisible to sqlmock's
	// assertion API — it errors at the driver, and resolveStaleIssues swallows
	// delete errors with `continue` so the tick completes normally. Omitting the
	// DELETE expectation therefore proves nothing: this test passed for months
	// while the resolver deleted every issue the same tick raised. Expecting the
	// DELETE and requiring it to go UNMET is what turns that silent call into a
	// visible failure.
	err := mock.ExpectationsWereMet()
	if err == nil {
		t.Fatal("resolveStaleIssues DELETEd the stall issue progressTick had just raised — " +
			"the map key and the row id have drifted apart again; both must come from stallIssueID")
	}
	if !strings.Contains(err.Error(), "DELETE") {
		t.Fatalf("expected only the deliberate DELETE guard to be unmet, got: %v", err)
	}
}

// The raise site and the resolve site exchange one string: the issue id. When they
// disagree the detector still logs, still writes a row, and still passes a tick —
// and then silently destroys its own output on the next one. This pins the contract
// on both classes at once so neither can drift back.
func TestIssueIDsRoundTripThroughResolveStaleIssues(t *testing.T) {
	for _, tc := range []struct {
		name   string
		id     string
		prefix string
	}{
		{"stall", stallIssueID("p1"), batchStallIssuePrefix},
		{"ack", ackIssueID("p1", "orders"), batchAckIssuePrefix},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !strings.HasPrefix(tc.id, tc.prefix) {
				t.Fatalf("id %q does not carry prefix %q — resolveStaleIssues scopes by "+
					"`id LIKE prefix || '%%'` and would never see it", tc.id, tc.prefix)
			}
			s, mock, done := newBatchSentinel(t)
			defer done()

			mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
				WithArgs(string(ComponentTypeBatchPipeline), tc.prefix).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(tc.id))

			// The id is marked open under exactly the key the detectors use.
			s.resolveStaleIssues(context.Background(), tc.prefix, map[string]bool{tc.id: true})

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("an open issue was cleared: %v", err)
			}
		})
	}
}

// A run that starts moving again must have its stall issue cleared, or the health
// surface keeps showing a problem that no longer exists.
func TestProgressTickResolvesIssueWhenRunRecovers(t *testing.T) {
	s, mock, done := newBatchSentinel(t)
	defer done()

	mock.ExpectQuery(`FROM pipelines p`).WillReturnRows(
		sqlmock.NewRows([]string{"id", "name", "execution_id", "last_movement", "status", "stage"}))
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(batchStallIssuePrefix + "p1"))
	mock.ExpectExec(`DELETE FROM sentinel_active_issues`).
		WithArgs(batchStallIssuePrefix + "p1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	s.progressTick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// The two issue classes are independent problems with independent fixes. A run that
// resumes moving must NOT clear an unresolved write-rejection on the same pipeline.
func TestResolveStaleIssuesIsScopedToItsOwnPrefix(t *testing.T) {
	s, mock, done := newBatchSentinel(t)
	defer done()

	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WithArgs(string(ComponentTypeBatchPipeline), batchStallIssuePrefix).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(batchStallIssuePrefix + "p1"))
	mock.ExpectExec(`DELETE FROM sentinel_active_issues`).
		WithArgs(batchStallIssuePrefix + "p1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	s.resolveStaleIssues(context.Background(), batchStallIssuePrefix, map[string]bool{})

	// The prefix is passed as a bound parameter, so the ack class was never even
	// listed — WithArgs above is the assertion.
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

func TestAckDrainTickEmitsPerTableIssue(t *testing.T) {
	s, mock, done := newBatchSentinel(t)
	defer done()

	mock.ExpectQuery(`FROM pipeline_batch_acks a`).WillReturnRows(
		sqlmock.NewRows([]string{"id", "name", "execution_id", "table_name", "rejected", "sample_error", "last_rejected_at"}).
			AddRow("p1", "nightly orders", "e1", "orders", int64(12),
				"invalid input syntax for type bigint", time.Now()))
	mock.ExpectExec(`INSERT INTO sentinel_active_issues`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`SELECT id FROM sentinel_active_issues`).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(batchAckIssuePrefix + "p1:orders"))

	s.ackDrainTick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// One unscannable row must not blind the sentinel to the rest of the tick — the same
// discipline collectActiveCDCPipelines follows.
func TestProgressTickSurvivesABadRow(t *testing.T) {
	s, mock, done := newBatchSentinel(t)
	defer done()

	mock.ExpectQuery(`FROM pipelines p`).WillReturnRows(
		sqlmock.NewRows([]string{"id", "name", "execution_id", "last_movement", "status", "stage"}).
			AddRow("p1", "bad", "e1", "not-a-timestamp", "running", "executor").
			AddRow("p2", "good", "e2", time.Now().Add(-45*time.Minute), "running", "executor"))
	// Only the good row produces an issue.
	mock.ExpectExec(`INSERT INTO sentinel_active_issues`).
		WillReturnResult(sqlmock.NewResult(1, 1))
	noOpenIssues(mock)

	s.progressTick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// Detection must never take the process down: a panic inside a tick is contained by
// recoverTick, and a failed query just ends the tick quietly.
func TestTicksAreFailSoft(t *testing.T) {
	s, mock, done := newBatchSentinel(t)
	defer done()

	mock.ExpectQuery(`FROM pipelines p`).WillReturnError(errors.New("connection reset by peer"))
	s.progressTick(context.Background())

	mock.ExpectQuery(`FROM pipeline_batch_acks a`).WillReturnError(errors.New("connection reset by peer"))
	s.ackDrainTick(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}

	// A sentinel with no database is inert rather than a nil-pointer panic — Start()
	// is called from main.go before every dependency is guaranteed.
	noDB := &BatchSentinel{stallThreshold: DefaultBatchStallThreshold}
	noDB.progressTick(context.Background())
	noDB.ackDrainTick(context.Background())
}

func TestNewBatchSentinelUsesDefaults(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	s := NewBatchSentinel(db, nil)
	if s.progressInterval != DefaultBatchProgressInterval {
		t.Errorf("progressInterval = %v, want %v", s.progressInterval, DefaultBatchProgressInterval)
	}
	if s.ackInterval != DefaultBatchAckDrainInterval {
		t.Errorf("ackInterval = %v, want %v", s.ackInterval, DefaultBatchAckDrainInterval)
	}
	if s.stallThreshold != DefaultBatchStallThreshold {
		t.Errorf("stallThreshold = %v, want %v", s.stallThreshold, DefaultBatchStallThreshold)
	}
	// The stall threshold must stay far below the healer's 4h zombie timeout —
	// otherwise the sentinel adds nothing the zombie sweeper wasn't already going to
	// find, which is the entire gap it exists to close.
	if s.stallThreshold >= 4*time.Hour {
		t.Errorf("stallThreshold %v is not meaningfully earlier than the 4h zombie timeout", s.stallThreshold)
	}
}

// The derived name sink-<pid8> is only correct for the default CDC shape. A
// streaming_only pipeline registers sink-<pid8>-<eid8>, so probing (or restarting)
// the derived group would read a group that does not exist and report success.
func TestResolveSinkConsumerGroupPrefersTheRegisteredGroup(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	s := &CDCSentinel{db: db}
	mock.ExpectQuery(`FROM pipeline_dependencies`).
		WillReturnRows(sqlmock.NewRows([]string{"identifier"}).AddRow("sink-abcd1234-ef567890"))

	if got := s.resolveSinkConsumerGroup(context.Background(), "p1"); got != "sink-abcd1234-ef567890" {
		t.Errorf("consumer group = %q, want the manifest value", got)
	}
}

func TestResolveSinkConsumerGroupFallsBack(t *testing.T) {
	pipelineID := "8f1c2d3e-4a5b-6c7d-8e9f-0a1b2c3d4e5f"
	want := sinkConsumerGroup(pipelineID)

	tests := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
	}{
		{
			// No manifest row: an older pipeline provisioned before the dependency
			// manifest existed. The derived name is the best available guess.
			name: "no dependency row",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`FROM pipeline_dependencies`).WillReturnError(sql.ErrNoRows)
			},
		},
		{
			name: "query error",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`FROM pipeline_dependencies`).WillReturnError(errors.New("timeout"))
			},
		},
		{
			// A blank identifier is a corrupt manifest row; probing group "" would
			// silently match nothing.
			name: "blank identifier",
			setup: func(m sqlmock.Sqlmock) {
				m.ExpectQuery(`FROM pipeline_dependencies`).
					WillReturnRows(sqlmock.NewRows([]string{"identifier"}).AddRow("   "))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			tt.setup(mock)

			s := &CDCSentinel{db: db}
			if got := s.resolveSinkConsumerGroup(context.Background(), pipelineID); got != want {
				t.Errorf("consumer group = %q, want the derived fallback %q", got, want)
			}
		})
	}

	// No database at all (unit contexts, startup before wiring): still a usable name.
	s := &CDCSentinel{}
	if got := s.resolveSinkConsumerGroup(context.Background(), pipelineID); got != want {
		t.Errorf("nil db: consumer group = %q, want %q", got, want)
	}
}
