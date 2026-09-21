package projector

import (
	"errors"
	"strings"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
)

// Every TABLE_STATS event upsertTableStats drops must be counted under its own reason and
// logged — warn for a shape no producer should send, debug for a deliberate no-op — and
// must still not touch the database or return an error.

// captureDebugLogs records every log entry at debug and above for the duration of the
// test, and restores the standard logger's level, output and hooks afterwards.
func captureDebugLogs(t *testing.T) *test.Hook {
	t.Helper()
	logger := log.StandardLogger()
	prevOut, prevLevel := logger.Out, logger.Level
	prevHooks := make(log.LevelHooks, len(logger.Hooks))
	for lvl, hs := range logger.Hooks {
		prevHooks[lvl] = append([]log.Hook(nil), hs...)
	}
	hook := test.NewLocal(logger)
	logger.SetLevel(log.DebugLevel)
	logger.SetOutput(newDiscard())
	t.Cleanup(func() {
		logger.ReplaceHooks(prevHooks)
		logger.SetOutput(prevOut)
		logger.SetLevel(prevLevel)
	})
	return hook
}

// skipEntries returns the TABLE_STATS skip log entries only.
func skipEntries(hook *test.Hook) []log.Entry {
	var out []log.Entry
	for _, e := range hook.AllEntries() {
		if _, ok := e.Data["reason"]; ok && e.Data["event_type"] == "TABLE_STATS" {
			out = append(out, *e)
		}
	}
	return out
}

// validSkipTestEvent is a well-formed sink CDC TABLE_STATS event; each case below breaks
// exactly one thing about it.
func validSkipTestEvent() map[string]interface{} {
	return cdcTableStatsEvent(statsTable(), map[string]interface{}{"source": "kafka_mcp_sink"})
}

func skipTestMeta(ev map[string]interface{}) map[string]interface{} {
	return ev["metadata"].(map[string]interface{})
}

func TestUpsertTableStats_EverySkipIsCountedAndLoggedByReason(t *testing.T) {
	cases := []struct {
		reason tableStatsSkipReason
		name   string
		level  log.Level
		mutate func(ev map[string]interface{})
	}{
		{tableStatsSkipMissingPipelineID, "missing_pipeline_id", log.WarnLevel,
			func(ev map[string]interface{}) { delete(ev, "pipeline_id") }},
		{tableStatsSkipMissingMetadata, "missing_metadata", log.WarnLevel,
			func(ev map[string]interface{}) { delete(ev, "metadata") }},
		{tableStatsSkipMissingMode, "missing_mode", log.WarnLevel,
			func(ev map[string]interface{}) { delete(skipTestMeta(ev), "mode") }},
		{tableStatsSkipUnsupportedMode, "unsupported_mode", log.DebugLevel,
			func(ev map[string]interface{}) { skipTestMeta(ev)["mode"] = "incremental" }},
		{tableStatsSkipMissingTable, "missing_table", log.WarnLevel,
			func(ev map[string]interface{}) { delete(skipTestMeta(ev), "table") }},
		{tableStatsSkipEmptyTableName, "empty_table_name", log.WarnLevel,
			func(ev map[string]interface{}) {
				skipTestMeta(ev)["table"] = map[string]interface{}{"schema": "s", "qualified_name": "s.t"}
			}},
	}
	if len(cases) != int(numTableStatsSkipReasons) {
		t.Fatalf("%d cases for %d skip reasons — a new reason needs a case", len(cases), numTableStatsSkipReasons)
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hook := captureDebugLogs(t)

			// No expectations: any statement at all fails the call, so a nil error below
			// also proves the skip never reached the database.
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("sqlmock.New: %v", err)
			}
			defer db.Close()
			p := &EventProjector{db: db}

			ev := validSkipTestEvent()
			tc.mutate(ev)
			if err := p.upsertTableStats(ev); err != nil {
				t.Fatalf("upsertTableStats returned %v for a skipped event; a skip is not a projection failure", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatalf("unexpected database activity: %v", err)
			}

			counts := p.TableStatsSkipCounts()
			if len(counts) != int(numTableStatsSkipReasons) {
				t.Errorf("TableStatsSkipCounts has %d reasons, want all %d (including zeros)", len(counts), numTableStatsSkipReasons)
			}
			for name, n := range counts {
				want := uint64(0)
				if name == tc.name {
					want = 1
				}
				if n != want {
					t.Errorf("skip count %s = %d, want %d", name, n, want)
				}
			}

			entries := skipEntries(hook)
			if len(entries) != 1 {
				t.Fatalf("want exactly one skip log line, got %d", len(entries))
			}
			e := entries[0]
			if e.Data["reason"] != tc.name {
				t.Errorf("log reason = %v, want %s", e.Data["reason"], tc.name)
			}
			if e.Level != tc.level {
				t.Errorf("log level = %s, want %s", e.Level, tc.level)
			}
			if e.Data["skipped_total"] != uint64(1) {
				t.Errorf("log skipped_total = %v, want 1", e.Data["skipped_total"])
			}
			// Privacy: only labels and identifiers, never anything from the counters.
			for k := range e.Data {
				switch k {
				case "event_type", "reason", "skipped_total", "pipeline_id", "mode", "source":
				default:
					t.Errorf("skip log carries field %q, which is not on the allow-list", k)
				}
			}
		})
	}
}

// A producer stuck on a bad shape emits on every flush of every table: the counter is
// exact, the warn lines are sampled at powers of two.
func TestUpsertTableStats_SkipWarningsAreSampled(t *testing.T) {
	hook := captureDebugLogs(t)
	p := &EventProjector{}

	for i := 0; i < 5; i++ {
		ev := validSkipTestEvent()
		delete(skipTestMeta(ev), "table")
		if err := p.upsertTableStats(ev); err != nil {
			t.Fatalf("upsertTableStats: %v", err)
		}
	}

	if got := p.TableStatsSkipCounts()["missing_table"]; got != 5 {
		t.Errorf("missing_table count = %d, want 5", got)
	}
	entries := skipEntries(hook)
	if len(entries) != 5 {
		t.Fatalf("want 5 skip log entries (warn or debug), got %d", len(entries))
	}
	wantLevels := []log.Level{log.WarnLevel, log.WarnLevel, log.DebugLevel, log.WarnLevel, log.DebugLevel}
	for i, e := range entries {
		if e.Level != wantLevels[i] {
			t.Errorf("skip #%d logged at %s, want %s", i+1, e.Level, wantLevels[i])
		}
		if e.Data["skipped_total"] != uint64(i+1) {
			t.Errorf("skip #%d skipped_total = %v, want %d", i+1, e.Data["skipped_total"], i+1)
		}
	}
}

// Producer-supplied labels are bounded before they reach a log line.
func TestUpsertTableStats_SkipLogLabelsAreTruncated(t *testing.T) {
	hook := captureDebugLogs(t)
	p := &EventProjector{}

	ev := validSkipTestEvent()
	skipTestMeta(ev)["mode"] = strings.Repeat("m", 500)
	if err := p.upsertTableStats(ev); err != nil {
		t.Fatalf("upsertTableStats: %v", err)
	}
	entries := skipEntries(hook)
	if len(entries) != 1 {
		t.Fatalf("want one skip entry, got %d", len(entries))
	}
	if got, _ := entries[0].Data["mode"].(string); len(got) != maxSkipLabelLen {
		t.Errorf("logged mode has length %d, want it truncated to %d", len(got), maxSkipLabelLen)
	}
}

// Positive control: the unmodified event is NOT a skip — it reaches the database and
// counts nothing. Without this, every case above could pass against an upsert that
// skipped everything.
func TestUpsertTableStats_WellFormedEventIsNotASkip(t *testing.T) {
	hook := captureDebugLogs(t)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	sentinel := errors.New("begin reached")
	mock.ExpectBegin().WillReturnError(sentinel)

	p := &EventProjector{db: db}
	if err := p.upsertTableStats(validSkipTestEvent()); !errors.Is(err, sentinel) {
		t.Fatalf("upsertTableStats = %v, want the Begin error: a well-formed event must reach the database", err)
	}
	for name, n := range p.TableStatsSkipCounts() {
		if n != 0 {
			t.Errorf("skip count %s = %d for a well-formed event", name, n)
		}
	}
	if n := len(skipEntries(hook)); n != 0 {
		t.Errorf("%d skip log line(s) for a well-formed event", n)
	}
}
