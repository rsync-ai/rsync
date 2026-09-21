package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

// A TABLE_STATS publish failure must be loud (log + /status last_error + counter)
// and must not change control flow. Before noteTableStatsEmit, every emit site was
// `_ = emit…TableStats(...)`, so all three were silent.

func statsEmitTestMessage() *SinkMessage {
	return &SinkMessage{
		PipelineID:  "pipe-stats-1",
		ExecutionID: "pipe-stats-1",
		TraceID:     "trace-stats-1",
		Table:       "public.orders",
	}
}

func TestNoteTableStatsEmit_ErrorIsLoggedCountedAndRecorded(t *testing.T) {
	metrics := &Metrics{}
	sm := statsEmitTestMessage()

	recs := captureLogLines(t, func() {
		noteTableStatsEmit(metrics, sm, "cdc", errors.New("kafka write: dial tcp 10.0.0.9:9092: connection refused"))
	})

	if len(recs) != 1 {
		t.Fatalf("want exactly one log line for one failed emit, got %d: %v", len(recs), recs)
	}
	rec := recs[0]
	for k, want := range map[string]string{
		"level":       "warning",
		"pipeline_id": "pipe-stats-1",
		"trace_id":    "trace-stats-1",
		"table":       "public.orders",
		"stats_mode":  "cdc",
	} {
		if got, _ := rec[k].(string); got != want {
			t.Errorf("log field %q = %q, want %q (record: %v)", k, got, want, rec)
		}
	}
	if got, _ := rec["error"].(string); !strings.Contains(got, "connection refused") {
		t.Errorf("log field error = %q, want it to carry the emit error", got)
	}

	if got := atomic.LoadUint64(&metrics.tableStatsEmitFailures); got != 1 {
		t.Errorf("tableStatsEmitFailures = %d, want 1", got)
	}
	metrics.mu.Lock()
	lastErr := metrics.lastError
	metrics.mu.Unlock()
	if !strings.Contains(lastErr, "table stats emit failed") || !strings.Contains(lastErr, "public.orders") || !strings.Contains(lastErr, "connection refused") {
		t.Errorf("metrics.lastError = %q, want the emit failure with its table", lastErr)
	}

	// A second failure keeps counting — it is a counter, not a flag.
	captureLogLines(t, func() { noteTableStatsEmit(metrics, sm, "batch", errors.New("again")) })
	if got := atomic.LoadUint64(&metrics.tableStatsEmitFailures); got != 2 {
		t.Errorf("tableStatsEmitFailures after two failures = %d, want 2", got)
	}
}

func TestNoteTableStatsEmit_NilErrorIsANoOp(t *testing.T) {
	metrics := &Metrics{}
	recs := captureLogLines(t, func() {
		noteTableStatsEmit(metrics, statsEmitTestMessage(), "cdc", nil)
	})
	if len(recs) != 0 {
		t.Errorf("a successful emit must not log, got %d line(s): %v", len(recs), recs)
	}
	if got := atomic.LoadUint64(&metrics.tableStatsEmitFailures); got != 0 {
		t.Errorf("tableStatsEmitFailures = %d after a successful emit, want 0", got)
	}
	if metrics.lastError != "" {
		t.Errorf("metrics.lastError = %q after a successful emit, want empty", metrics.lastError)
	}
}

func TestNoteTableStatsEmit_NilMetricsAndMessageStillLog(t *testing.T) {
	recs := captureLogLines(t, func() {
		noteTableStatsEmit(nil, nil, "batch", errors.New("boom"))
	})
	if len(recs) != 1 {
		t.Fatalf("want one log line even without metrics/message, got %d", len(recs))
	}
	if got, _ := recs[0]["stats_mode"].(string); got != "batch" {
		t.Errorf("stats_mode = %q, want batch", got)
	}
}

// The real emit functions against a broker that refuses connections: proves the
// error the helper handles is the one emit actually returns, end to end.
func TestNoteTableStatsEmit_RealWriterFailureIsCounted(t *testing.T) {
	w := &kafka.Writer{
		Addr:            kafka.TCP("127.0.0.1:1"),
		Topic:           "pipeline.domain.events",
		MaxAttempts:     1,
		WriteBackoffMin: time.Millisecond,
		WriteBackoffMax: time.Millisecond,
	}
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	metrics := &Metrics{}
	sm := statsEmitTestMessage()
	var cdcErr, batchErr error
	captureLogLines(t, func() {
		cdcErr = emitCDCTableStats(ctx, w, sm, 1, 2, 3, 4, 0)
		noteTableStatsEmit(metrics, sm, "cdc", cdcErr)
		batchErr = emitTableStats(ctx, w, sm, "batch", "running", 10, 10, 100)
		noteTableStatsEmit(metrics, sm, "batch", batchErr)
	})
	// Control: the fixture really fails, otherwise the count below proves nothing.
	if cdcErr == nil || batchErr == nil {
		t.Fatalf("fixture broken: emit to an unreachable broker returned nil (cdc=%v batch=%v)", cdcErr, batchErr)
	}
	if got := atomic.LoadUint64(&metrics.tableStatsEmitFailures); got != 2 {
		t.Errorf("tableStatsEmitFailures = %d, want 2", got)
	}
}

// discardedStatsEmit matches an emit whose error is thrown away.
var discardedStatsEmit = regexp.MustCompile(`_\s*=\s*emit(?:CDC)?TableStats\(`)

// statsEmitCall matches any call of either emit function (declarations excluded below).
var statsEmitCall = regexp.MustCompile(`\bemit(?:CDC)?TableStats\(`)

// Census: no emit site in the package may discard its error, and every call must be
// routed through noteTableStatsEmit on the same line, so the sites cannot drift.
func TestNoTableStatsEmitResultIsDiscarded(t *testing.T) {
	// Positive controls: the patterns must match what they claim to detect, or a
	// zero below would be vacuous.
	for _, s := range []string{
		`_ = emitCDCTableStats(ctx, b.eventsWriter, lastSM, 1, 2, 3, 4, 5)`,
		`	_ = emitTableStats(ctx, eventsWriter, sm, "batch", "running", 1, 2, 3)`,
		`_=emitTableStats(`,
	} {
		if !discardedStatsEmit.MatchString(s) {
			t.Fatalf("census regex failed its positive control: %q", s)
		}
	}
	if discardedStatsEmit.MatchString(`noteTableStatsEmit(metrics, sm, "cdc", emitCDCTableStats(ctx, w, sm, 1, 2, 3, 4, 5))`) {
		t.Fatalf("census regex flags a handled call (negative control)")
	}

	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue // prose about the old pattern is not a call site
			}
			if discardedStatsEmit.MatchString(line) {
				t.Errorf("%s:%d discards a TABLE_STATS emit error; wrap it in noteTableStatsEmit: %s", f, i+1, trimmed)
			}
			if !statsEmitCall.MatchString(line) || strings.HasPrefix(trimmed, "func ") {
				continue
			}
			calls++
			if !strings.Contains(line, "noteTableStatsEmit(") {
				t.Errorf("%s:%d calls a TABLE_STATS emit without noteTableStatsEmit on the same line: %s", f, i+1, trimmed)
			}
		}
	}
	// A census that finds nothing proves nothing: main.go has 8 emit sites today.
	if calls < 8 {
		t.Errorf("found %d TABLE_STATS emit call(s), want >= 8 — the census is not seeing the call sites", calls)
	}
}
