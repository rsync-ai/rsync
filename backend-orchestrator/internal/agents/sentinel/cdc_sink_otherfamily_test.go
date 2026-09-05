package sentinel

// Coverage for the per-source sink-drain gap: checkSourceDBLag only reached the
// family-agnostic sink-drain check (checkSinkConsumerLag → maybeAutoRestartSink) from its
// PostgreSQL branch (via cdc_resources) and its MySQL branch (via connections). Oracle,
// SQL Server, and MongoDB CDC pipelines drain to the destination through the SAME shared
// sink worker (consumer group sink-<short8>) yet were never sink-checked or auto-restarted —
// they had no source-lag branch to piggyback the sink check onto. These tests lock the
// discovery that closes the gap.

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestCheckSourceDBLag_AlsoChecksOtherFamilySinks is the RED gate for the fix. checkSourceDBLag
// must, in addition to its PostgreSQL (cdc_resources) and MySQL (connections) discovery, issue a
// third discovery query for the remaining CDC families (oracle/sqlserver/mongodb) so their sinks
// get the same dead-sink drain check. Before the fix only two queries are issued, so the third
// expectation goes unmatched and ExpectationsWereMet() reports it — the intended runtime RED.
//
// kafkaManager is left nil so checkSinkConsumerLag returns immediately (no Kafka, no extra DB
// I/O): the test isolates *discovery*, not the lag math (already covered elsewhere).
func TestCheckSourceDBLag_AlsoChecksOtherFamilySinks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	// 1) PostgreSQL slot discovery — return no rows (no PG source-lag/sink probes fire).
	mock.ExpectQuery(`JOIN cdc_resources cr`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connection_id", "resource_name"}))

	// 2) MySQL connection discovery — return no rows (no MySQL source-lag/sink probes fire).
	mock.ExpectQuery(`ILIKE 'mysql%'`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "source_connection_id"}))

	// 3) Other-family discovery (oracle/sqlserver/mongodb) — MUST be issued by checkSourceDBLag.
	//    Returning one oracle pipeline also exercises the discovered → sink-check path.
	mock.ExpectQuery(`ILIKE 'oracle%'`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type"}).
			AddRow("ora-pipe-1", "oracle-cdc", "oracle"))

	s := &CDCSentinel{db: db} // kafkaManager nil ⇒ checkSinkConsumerLag is a no-op

	s.checkSourceDBLag(context.Background())

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("checkSourceDBLag did not run the other-family (oracle/sqlserver/mongodb) sink discovery: %v", err)
	}
}

// TestDiscoverOtherFamilyCDCSinks_NormalizesDBTypeLabel locks that the discovered targets carry a
// canonical dbType label — connector_type aliases are folded (oracledb→oracle, mssql→sqlserver)
// via cdc.NormalizeDBType so the emitted issue metadata is consistent regardless of how the source
// connection was named.
func TestDiscoverOtherFamilyCDCSinks_NormalizesDBTypeLabel(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`ILIKE 'oracle%'`).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "connector_type"}).
			AddRow("p-ora", "ora-cdc", "oracledb").  // alias → oracle
			AddRow("p-mss", "mss-cdc", "mssql").      // alias → sqlserver
			AddRow("p-mongo", "mongo-cdc", "mongodb"))

	s := &CDCSentinel{db: db}
	got := s.discoverOtherFamilyCDCSinks(context.Background())

	want := map[string]string{"p-ora": "oracle", "p-mss": "sqlserver", "p-mongo": "mongodb"}
	if len(got) != len(want) {
		t.Fatalf("discovered %d targets, want %d: %+v", len(got), len(want), got)
	}
	for _, tgt := range got {
		if w, ok := want[tgt.pipelineID]; !ok || tgt.dbType != w {
			t.Errorf("target %q: dbType = %q, want %q", tgt.pipelineID, tgt.dbType, w)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}

// TestDiscoverOtherFamilyCDCSinks_QueryErrorReturnsNil locks fail-safe behaviour: a discovery
// query error must yield no targets (and no panic), so a transient control-DB hiccup can never
// fan out sink checks against bogus pipeline ids.
func TestDiscoverOtherFamilyCDCSinks_QueryErrorReturnsNil(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery(`ILIKE 'oracle%'`).WillReturnError(errors.New("boom"))

	s := &CDCSentinel{db: db}
	if got := s.discoverOtherFamilyCDCSinks(context.Background()); got != nil {
		t.Errorf("on query error, want nil targets, got %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}
