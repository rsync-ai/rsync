package cdc

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// CDC Stop keeps the replication slot and publication: they are the position
// Start resumes from. The periodic reapers must therefore select only rows of a
// DELETED pipeline; only the WAL watchdog may drop a stopped pipeline's slot.
func TestReapableQueriesKeepStoppedPipelines(t *testing.T) {
	for _, rt := range []string{"replication_slot", "publication"} {
		q := reapableResourceQuery(rt, false)
		if strings.Contains(q, "'stopped'") {
			t.Errorf("%s: periodic reaper selects stopped pipelines:\n%s", rt, q)
		}
		if !strings.Contains(q, "cr.pipeline_id IS NULL OR p.id IS NULL") {
			t.Errorf("%s: periodic reaper no longer selects deleted pipelines:\n%s", rt, q)
		}
		if !strings.Contains(q, "cr.resource_type = '"+rt+"'") {
			t.Errorf("%s: wrong resource type:\n%s", rt, q)
		}
	}
	if q := reapableResourceQuery("replication_slot", true); !strings.Contains(q, "p.status = 'stopped'") {
		t.Errorf("WAL-pressure query must include stopped pipelines:\n%s", q)
	}
}

func TestReapSlotsUnderWALPressureTouchesOnlyNamedSlots(t *testing.T) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer mockDB.Close()
	cols := []string{
		"id", "pipeline_id", "connection_id", "source_table",
		"resource_type", "resource_name", "status",
		"database_type", "metadata", "created_at", "deleted_at", "last_verified_at",
	}
	mock.ExpectQuery(`p.status = 'stopped'`).
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("r1", "p1", "conn1", nil, "replication_slot", "debezium_other_slot", "active",
				"postgresql", []byte(`{}`), time.Unix(1700000000, 0), nil, nil))

	// The only reapable row is not one the watchdog measured as CRITICAL, so
	// nothing is dropped and no connection config is ever read.
	n, err := NewPostgreSQLManager(mockDB).ReapSlotsUnderWALPressure(context.Background(), []string{"debezium_critical_slot"})
	if err != nil || n != 0 {
		t.Fatalf("dropped=%d err=%v, want 0 <nil>", n, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	// No names → no query at all.
	if n, err := NewPostgreSQLManager(mockDB).ReapSlotsUnderWALPressure(context.Background(), nil); err != nil || n != 0 {
		t.Fatalf("dropped=%d err=%v", n, err)
	}
}

// The exported getters are what the reapers call, so pin THEIR SQL, not only
// the builder's.
func TestReapableGettersSQL(t *testing.T) {
	var seen string
	matcher := sqlmock.QueryMatcherFunc(func(_, actual string) error { seen = actual; return nil })
	for _, c := range []struct {
		name        string
		call        func(ctx context.Context, m *PostgreSQLManager) error
		wantStopped bool
	}{
		{"GetReapableSlots", func(ctx context.Context, m *PostgreSQLManager) error {
			_, err := GetReapableSlots(ctx, m.db)
			return err
		}, false},
		{"GetReapablePublications", func(ctx context.Context, m *PostgreSQLManager) error {
			_, err := GetReapablePublications(ctx, m.db)
			return err
		}, false},
		{"GetWALPressureReapableSlots", func(ctx context.Context, m *PostgreSQLManager) error {
			_, err := GetWALPressureReapableSlots(ctx, m.db)
			return err
		}, true},
	} {
		mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery("").WillReturnRows(sqlmock.NewRows([]string{"id"}))
		seen = ""
		_ = c.call(context.Background(), NewPostgreSQLManager(mockDB))
		if got := strings.Contains(seen, "'stopped'"); got != c.wantStopped {
			t.Errorf("%s: selects stopped=%v, want %v\n%s", c.name, got, c.wantStopped, seen)
		}
		mockDB.Close()
	}
}
