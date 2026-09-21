package assessor

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

const healthPipelineID = "883cae79-cb1b-4541-a2c1-3520f3d3e5b6"

func newHealthMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db, mock
}

func TestCheckPostgresSlotCapacity(t *testing.T) {
	slotRow := func(max, used, own int) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"max", "used", "own"}).AddRow(max, used, own)
	}

	t.Run("all slots used and none is this pipeline's blocks the run", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("pg_replication_slots").
			WithArgs("debezium_slot_pipe_883cae79_").
			WillReturnRows(slotRow(10, 10, 0))

		got := checkPostgresSlotCapacity(context.Background(), db, healthPipelineID)

		if got.Severity != SeverityError || got.Passed {
			t.Fatalf("severity=%q passed=%v; want error/false — slot creation would fail", got.Severity, got.Passed)
		}
		if got.Remediation == nil || len(got.Remediation.SQLToRun) == 0 {
			t.Fatal("blocking finding carries no fix")
		}
	})

	t.Run("all slots used but one is this pipeline's passes", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("pg_replication_slots").WillReturnRows(slotRow(10, 10, 1))

		got := checkPostgresSlotCapacity(context.Background(), db, healthPipelineID)

		if !got.Passed {
			t.Fatalf("a pipeline that already owns its slot must not be blocked: %+v", got)
		}
	})

	t.Run("a free slot passes", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("pg_replication_slots").WillReturnRows(slotRow(10, 3, 0))

		if got := checkPostgresSlotCapacity(context.Background(), db, healthPipelineID); !got.Passed {
			t.Fatalf("want pass, got %+v", got)
		}
	})

	t.Run("no pipeline id sends an empty prefix so no slot counts as its own", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("pg_replication_slots").WithArgs("").WillReturnRows(slotRow(4, 4, 0))

		if got := checkPostgresSlotCapacity(context.Background(), db, ""); got.Severity != SeverityError {
			t.Fatalf("want error, got %+v", got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a query failure is a warning, never a block", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("pg_replication_slots").WillReturnError(sql.ErrConnDone)

		got := checkPostgresSlotCapacity(context.Background(), db, healthPipelineID)
		if got.Severity != SeverityWarning {
			t.Fatalf("want warning, got %+v", got)
		}
	})
}

func expectSetting(mock sqlmock.Sqlmock, name, value string) {
	q := mock.ExpectQuery("pg_settings").WithArgs(name)
	if value == "" {
		q.WillReturnRows(sqlmock.NewRows([]string{"setting"}))
		return
	}
	q.WillReturnRows(sqlmock.NewRows([]string{"setting"}).AddRow(value))
}

func TestPostgresSettingAdvisories(t *testing.T) {
	cases := []struct {
		name     string
		setting  string
		value    string
		check    func(context.Context, *sql.DB) (Check, bool)
		wantOK   bool
		wantPass bool
	}{
		{"unlimited wal keep size is an advisory", "max_slot_wal_keep_size", "-1", checkPostgresMaxSlotWALKeepSize, true, false},
		{"a wal keep size limit passes", "max_slot_wal_keep_size", "51200", checkPostgresMaxSlotWALKeepSize, true, true},
		{"no wal keep size setting (PG12) yields no check", "max_slot_wal_keep_size", "", checkPostgresMaxSlotWALKeepSize, false, false},
		{"5s sender timeout is an advisory", "wal_sender_timeout", "5000", checkPostgresWALSenderTimeout, true, false},
		{"disabled sender timeout passes", "wal_sender_timeout", "0", checkPostgresWALSenderTimeout, true, true},
		{"default sender timeout passes", "wal_sender_timeout", "60000", checkPostgresWALSenderTimeout, true, true},
		{"small decoding memory is an advisory", "logical_decoding_work_mem", "4096", checkPostgresLogicalDecodingWorkMem, true, false},
		{"default decoding memory passes", "logical_decoding_work_mem", "65536", checkPostgresLogicalDecodingWorkMem, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newHealthMock(t)
			expectSetting(mock, tc.setting, tc.value)

			got, ok := tc.check(context.Background(), db)

			if ok != tc.wantOK {
				t.Fatalf("ok=%v; want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if got.Passed != tc.wantPass {
				t.Fatalf("passed=%v; want %v (%s)", got.Passed, tc.wantPass, got.Message)
			}
			// Advisories never block: they stay info even when they fail.
			if got.Severity != SeverityInfo {
				t.Fatalf("severity=%q; an advisory must stay info so it never gates a run", got.Severity)
			}
			if !got.Passed && (got.Remediation == nil || len(got.Remediation.SQLToRun) == 0) {
				t.Fatal("failing advisory carries no fix")
			}
		})
	}
}

func TestCheckPostgresPublicationPrivilege(t *testing.T) {
	roleRow := func(super, admin bool) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"user", "super", "admin"}).AddRow("rsync", super, admin)
	}

	t.Run("existing publication passes without a role lookup", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("pg_publication").
			WithArgs("debezium_pub_pipe_883cae79_").
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

		got, ok := checkPostgresPublicationPrivilege(context.Background(), db, healthPipelineID)
		if !ok || !got.Passed {
			t.Fatalf("want pass, got ok=%v %+v", ok, got)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("a platform admin role passes", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("pg_publication").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectQuery("cloudsqlsuperuser").WillReturnRows(roleRow(false, true))

		if got, ok := checkPostgresPublicationPrivilege(context.Background(), db, healthPipelineID); !ok || !got.Passed {
			t.Fatalf("want pass, got ok=%v %+v", ok, got)
		}
	})

	t.Run("a plain user is an advisory, never a block", func(t *testing.T) {
		db, mock := newHealthMock(t)
		mock.ExpectQuery("pg_publication").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
		mock.ExpectQuery("rolsuper").WillReturnRows(roleRow(false, false))

		got, ok := checkPostgresPublicationPrivilege(context.Background(), db, healthPipelineID)
		if !ok || got.Passed {
			t.Fatalf("want a failing advisory, got ok=%v %+v", ok, got)
		}
		if got.Severity != SeverityInfo {
			t.Fatalf("severity=%q; must stay info — managed platforms grant this in ways the catalog does not show", got.Severity)
		}
		if !strings.Contains(got.Message, `"rsync"`) {
			t.Fatalf("message does not name the user: %s", got.Message)
		}
	})
}

// An advisory that fails must not count as a failure, or STRICT_PREFLIGHT would
// block the run on it.
func TestSummarize_FailingInfoIsNotBlocking(t *testing.T) {
	r := &Result{Checks: []Check{
		{Code: "A", Severity: SeverityInfo, Passed: true},
		{Code: "B", Severity: SeverityInfo, Passed: false},
	}}
	Summarize(r)
	if r.BlocksStart() {
		t.Fatalf("a failing info advisory blocked the start: %+v", r)
	}
	if r.WarningCount != 1 || r.OverallStatus != "warning" {
		t.Fatalf("warning_count=%d status=%q; want 1/warning", r.WarningCount, r.OverallStatus)
	}
}
