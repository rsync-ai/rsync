package sentinel

// cdc_sink_respawn_unprovisioned_test.go — issue #15 of the 2026-09-16 MongoDB→GCS run.
//
// A chat-created CDC pipeline is status='running', sync_mode='cdc' while it is still
// waiting on the table-selection HITL. The absent-worker rung saw sink_status=not_found,
// re-issued start_sink, failed with "Connector cdc-<pid8> not found" and counted the
// attempt toward a terminal escalation. A sink that was never started is not absent.

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

const provisionedQueryRe = `SELECT EXISTS \(\s*SELECT 1 FROM pipeline_dependencies`

func TestEnsureSinkWorkerPresent_NotProvisionedYet_NoProbeNoAttempt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	s := &CDCSentinel{
		db:               db,
		sinkRespawnState: map[string]*connRestartState{},
		// Past the startup grace, so only the new gate can stop the rung.
		startedAt:  time.Now().Add(-time.Hour),
		mcpManager: &mcp.ServerManager{},
	}

	mock.ExpectQuery(provisionedQueryRe).
		WithArgs("aaa0ded3-0000-0000-0000-000000000001").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	// No further query is expected: resolving the consumer group, probing the container
	// and restarting the sink must all be skipped. sqlmock fails any unexpected query.

	s.ensureSinkWorkerPresent(context.Background(), "aaa0ded3-0000-0000-0000-000000000001", "p", "mongodb")

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected DB interaction: %v", err)
	}
	if st, ok := s.sinkRespawnState["aaa0ded3-0000-0000-0000-000000000001"]; ok {
		t.Fatalf("an unprovisioned pipeline must not touch the respawn budget, got %+v", *st)
	}
}

func TestCDCStreamProvisioned(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows *sqlmock.Rows
		err  error
		want bool
	}{
		{name: "manifest or connector row exists", rows: sqlmock.NewRows([]string{"exists"}).AddRow(true), want: true},
		{name: "nothing provisioned (awaiting table selection)", rows: sqlmock.NewRows([]string{"exists"}).AddRow(false), want: false},
		{name: "lookup failed is not evidence of absence", err: errors.New("db down"), want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			q := mock.ExpectQuery(provisionedQueryRe).WithArgs("p1")
			if tc.err != nil {
				q.WillReturnError(tc.err)
			} else {
				q.WillReturnRows(tc.rows)
			}
			s := &CDCSentinel{db: db}
			if got := s.cdcStreamProvisioned(context.Background(), "p1"); got != tc.want {
				t.Fatalf("cdcStreamProvisioned = %v, want %v", got, tc.want)
			}
		})
	}
}

// The gate must key on the facts the executor writes, not on pipeline status: the
// pipeline is already 'running' while it awaits table selection.
func TestCDCStreamProvisionedQuery_KeysOnExecutorRecords(t *testing.T) {
	for _, want := range []string{"kafka_sink_worker", "debezium_task", "cdc_resources", "'active'"} {
		if !regexp.MustCompile(regexp.QuoteMeta(want)).MatchString(cdcStreamProvisionedQuery) {
			t.Errorf("cdcStreamProvisionedQuery must reference %s", want)
		}
	}
}
