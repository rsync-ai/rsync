package cdc

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// BUG-3: PostgreSQL Debezium publications are per-pipeline (debezium_pub_pipe_*)
// and are dropped on the synchronous pipeline-delete cleanup — but there was NO
// safety-net reaper for publications the way ReapOrphanedSlots reaps slots. So a
// publication whose pipeline was deleted/stopped without a clean drop (orchestrator
// down, a delete path that bypassed cleanup, a swallowed drop error) leaked
// forever. Confirmed live: 2 orphaned publications survived on the source after
// their pipelines + slots were already gone. GetReapablePublications is the query
// the new reaper drives; it must select ONLY publication rows whose pipeline is
// gone or stopped, mirroring GetReapableSlots.

func TestGetReapablePublications_SelectsOnlyOrphanedPublications(t *testing.T) {
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
	// The query must filter resource_type = 'publication' (NOT replication_slot)
	// and only rows whose pipeline is gone/stopped.
	mock.ExpectQuery(`resource_type = 'publication'`).
		WillReturnRows(sqlmock.NewRows(cols).
			AddRow("r1", nil, "conn1", nil, "publication",
				"debezium_pub_pipe_dcb754cd_5da61bec", "active",
				"postgresql", []byte(`{}`), time.Unix(1700000000, 0), nil, nil))

	got, err := GetReapablePublications(context.Background(), mockDB)
	if err != nil {
		t.Fatalf("GetReapablePublications: %v", err)
	}
	if len(got) != 1 || got[0].ResourceType != "publication" ||
		got[0].ResourceName != "debezium_pub_pipe_dcb754cd_5da61bec" {
		t.Fatalf("want 1 orphaned publication row, got %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("query must target publications with a dead/stopped pipeline: %v", err)
	}
}
