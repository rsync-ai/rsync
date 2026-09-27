package sentinel

// The per-consumer census behind the pipeline page's Consumers card.
//
// Two things it must get right, both of them about not lying by omission:
//
//  1. A consumer that no longer exists must DISAPPEAR, not linger at its last
//     lag. The Sentinel already learned this once — its former per-topic consumer
//     check (removed with the agent command bus) was rewritten to rewrite every row
//     each tick because a recovered consumer kept
//     "Consumer group closed" and a lag of 4500 indefinitely, and the API served
//     it as current. A census written as an upsert would reintroduce exactly that
//     for a topic that was removed from the pipeline or a -batch group that
//     finished.
//
//  2. The role must describe the group that is RUNNING, not the pipeline's
//     current mode. A pipeline switched from hybrid to streaming-only still has a
//     -batch group draining its backfill.

import (
	"context"
	"database/sql"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
)

func TestSinkGroupRole_ReadsTheSuffixNotThePipelineMode(t *testing.T) {
	for _, tc := range []struct {
		group string
		want  string
		why   string
	}{
		{"rsync.sink-aa4c1a3c", roleSink, "the common CDC streaming shape"},
		{"rsync.sink-aa4c1a3c-batch", roleSinkBatch, "hybrid-CDC backfill worker"},
		{"rsync.sink-aa4c1a3c-stream", roleSinkStream, "cdc_mode streaming_only | never"},
		{"sink-aa4c1a3c", roleSink, "the bare spelling a pre-namespace pipeline still runs under"},
		{"sink-aa4c1a3c-batch", roleSinkBatch, "bare spelling, batch"},
	} {
		if got := sinkGroupRole(tc.group); got != tc.want {
			t.Errorf("sinkGroupRole(%q) = %q, want %q (%s)", tc.group, got, tc.want, tc.why)
		}
	}
}

// The census replaces the pipeline's rows inside one transaction. Delete-then-insert
// rather than upsert, so a (group, topic) pair that has gone away is gone from the
// card too; one transaction, so a reader never catches the pipeline with no
// consumers at all.
func TestWriteConsumerCensus_ReplacesTheRowsInOneTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	members := 1
	readings := []consumerReading{{
		group: "rsync.sink-aa4c1a3c",
		role:  roleSink,
		lagByTopic: map[string]int64{
			// Deliberately out of order: the writer sorts, so a row's position is
			// stable between ticks and a diff of two censuses is readable.
			"rsync.cdc-aa4c1a3c.public.users":  0,
			"rsync.cdc-aa4c1a3c.public.orders": 120,
		},
		// Per topic, not the group's total: the column is "the group's committed
		// position on this topic", and the census used to repeat the group-wide sum
		// on every row.
		committedByTopic: map[string]int64{
			"rsync.cdc-aa4c1a3c.public.users":  4100,
			"rsync.cdc-aa4c1a3c.public.orders": 98000,
		},
		state:     "Stable",
		members:   members,
		described: true,
	}}

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM pipeline_consumer_lag")).
		WithArgs("p1").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_consumer_lag")).
		WithArgs("p1", "rsync.sink-aa4c1a3c", "rsync.cdc-aa4c1a3c.public.orders", roleSink,
			int64(120), int64(98000), "Stable", members).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_consumer_lag")).
		WithArgs("p1", "rsync.sink-aa4c1a3c", "rsync.cdc-aa4c1a3c.public.users", roleSink,
			int64(0), int64(4100), "Stable", members).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	s := &CDCSentinel{db: db}
	s.writeConsumerCensus(context.Background(), "p1", readings)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// A group the broker could not be asked about stores state and members NULL.
// Writing "" and 0 instead would render as a nameless state with no members —
// which reads like a dead consumer rather than an unmeasured one.
func TestWriteConsumerCensus_UndescribedGroupStoresNulls(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM pipeline_consumer_lag")).
		WithArgs("p1").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_consumer_lag")).
		WithArgs("p1", "g", "t", roleSink, int64(7), int64(5), nil, nil).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	s := &CDCSentinel{db: db}
	s.writeConsumerCensus(context.Background(), "p1", []consumerReading{{
		group:            "g",
		role:             roleSink,
		lagByTopic:       map[string]int64{"t": 7},
		committedByTopic: map[string]int64{"t": 5},
		described:        false,
	}})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// A failed row write rolls the whole census back rather than committing a partial
// one: half a pipeline's consumers, presented as all of them, under-reports the
// backlog.
func TestWriteConsumerCensus_APartialWriteIsRolledBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM pipeline_consumer_lag")).
		WithArgs("p1").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO pipeline_consumer_lag")).
		WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	s := &CDCSentinel{db: db}
	s.writeConsumerCensus(context.Background(), "p1", []consumerReading{{
		group: "g", role: roleSink, lagByTopic: map[string]int64{"t": 1},
	}})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// The census is pure observation added alongside a safety-critical alarm. Nothing
// in it may panic or abort the tick.
func TestRecordConsumerCensus_MissingDepsAreANoOp(t *testing.T) {
	// No DB and no Kafka manager: the guard returns before touching either.
	s := &CDCSentinel{}
	s.recordConsumerCensus(context.Background(), "p1", "g", kafka.ConsumerGroupDrain{})
}

// A group with no committed topics contributes no rows. GetConsumerGroupDrain
// returns an empty LagByTopic for a group that has never committed anywhere, and
// inventing a row for it would show a consumer reading nothing as a consumer
// reading zero — a claim about a topic it may not be assigned.
func TestWriteConsumerCensus_NoTopicsWritesNoRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM pipeline_consumer_lag")).
		WithArgs("p1").WillReturnResult(sqlmock.NewResult(0, 3))
	mock.ExpectCommit()

	s := &CDCSentinel{db: db}
	s.writeConsumerCensus(context.Background(), "p1", []consumerReading{{
		group: "g", role: roleSink, lagByTopic: map[string]int64{},
	}})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}
