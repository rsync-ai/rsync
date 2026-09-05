package sentinel

import (
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// TestSinkAutoRestartEnabled_DefaultsOffAndParsesTrue locks the feature flag that gates the
// autonomous sink-restart rung. It MUST default OFF everywhere (cloud + OSS) — this is an
// operational-safety gate like CDC_RECOVERY_ENABLED, not an edition gate, so it defaults to
// the same value in every deployment and is only flipped on where an operator has opted in.
// Only a case-insensitive, whitespace-trimmed "true" enables it; anything else (including
// "1") stays off, so a stray/garbage value can never silently arm a restart loop.
func TestSinkAutoRestartEnabled_DefaultsOffAndParsesTrue(t *testing.T) {
	cases := []struct {
		name string
		set  bool
		val  string
		want bool
	}{
		{"unset_defaults_off", false, "", false},
		{"true_enables", true, "true", true},
		{"upper_true_enables", true, "TRUE", true},
		{"padded_true_enables", true, "  true  ", true},
		{"false_stays_off", true, "false", false},
		{"one_is_not_true", true, "1", false},
		{"garbage_stays_off", true, "yes", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.set {
				t.Setenv("CDC_SINK_AUTORESTART_ENABLED", tc.val)
			} else {
				t.Setenv("CDC_SINK_AUTORESTART_ENABLED", "")
			}
			if got := sinkAutoRestartEnabled(); got != tc.want {
				t.Errorf("sinkAutoRestartEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSinkStaleBound_DefaultAndOverride locks the destination-apply staleness threshold used
// as wedge signal #2. The default matches the UI liveness bound (300s) so the two surfaces
// agree on "idle". A positive duration override is honoured; a non-positive or unparseable
// value falls back to the default rather than arming a hair-trigger (0s) restart.
func TestSinkStaleBound_DefaultAndOverride(t *testing.T) {
	cases := []struct {
		name string
		val  string
		want time.Duration
	}{
		{"unset_defaults_5m", "", 5 * time.Minute},
		{"override_10m", "10m", 10 * time.Minute},
		{"zero_falls_back", "0", 5 * time.Minute},
		{"negative_falls_back", "-30s", 5 * time.Minute},
		{"garbage_falls_back", "soon", 5 * time.Minute},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CDC_SINK_STALE_BOUND", tc.val)
			if got := sinkStaleBound(); got != tc.want {
				t.Errorf("sinkStaleBound() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestLoadSinkAppliedProgress_ReadsDestinationApplyColumn locks wedge signal #2's data source:
// the destination-apply marker MAX(last_applied_ts) from pipeline_run_table_stats scoped to
// mode='cdc'. This is the sink's OWN post-apply timestamp (destination truth) — deliberately
// NOT the source-side last_event_ts, which stays fresh while the sink is wedged and would make
// a wedge look alive. The expectation keys on MAX(last_applied_ts): against a read of any other
// column sqlmock never matches and the helper returns an invalid time — the RED that proves the
// wrong column. Family-agnostic (mode='cdc' covers MySQL and PostgreSQL sources alike).
func TestLoadSinkAppliedProgress_ReadsDestinationApplyColumn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	pipelineID := "2cb685ed-4cf7-445b-9f77-071794d25423"
	applied := time.Now().Add(-10 * time.Minute) // 600s ago → beyond the 300s stale bound

	mock.ExpectQuery(`MAX\(last_applied_ts\) FROM pipeline_run_table_stats`).
		WithArgs(pipelineID).
		WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(applied))

	got := loadSinkAppliedProgress(context.Background(), db, pipelineID)

	if !got.Valid {
		t.Fatalf("expected a valid last_applied_ts (wrong column/table queried?), got NULL")
	}
	if secs := int64(time.Since(got.Time).Seconds()); secs < 300 {
		t.Errorf("applied staleness = %ds, want > 300s so the wedge gate can observe it", secs)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}
