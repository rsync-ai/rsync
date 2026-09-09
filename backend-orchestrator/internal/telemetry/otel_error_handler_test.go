package telemetry

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

// captureLog redirects the logrus standard logger for the duration of fn and
// returns what was written to it.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()

	var buf bytes.Buffer
	prevOut := log.StandardLogger().Out
	prevFormatter := log.StandardLogger().Formatter
	prevLevel := log.GetLevel()

	log.SetOutput(&buf)
	log.SetFormatter(&log.TextFormatter{DisableColors: true, DisableTimestamp: true})
	log.SetLevel(log.DebugLevel)
	t.Cleanup(func() {
		log.SetOutput(prevOut)
		log.SetFormatter(prevFormatter)
		log.SetLevel(prevLevel)
	})

	fn()

	if prevOut == nil {
		log.SetOutput(io.Discard)
	}
	return buf.String()
}

func countLines(s string) int {
	n := 0
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}

// The first failure must always be reported. A handler that suppressed it would
// trade a flooded log for a silent one, which is not the fix.
func TestFirstExportErrorIsAlwaysLogged(t *testing.T) {
	h := &collapsingErrorHandler{interval: time.Hour}

	out := captureLog(t, func() { h.Handle(errors.New("connection refused")) })

	if got := countLines(out); got != 1 {
		t.Fatalf("first error produced %d log lines, want 1: %q", got, out)
	}
	if !strings.Contains(out, "connection refused") {
		t.Fatalf("the first line dropped the underlying error: %q", out)
	}
}

// The defect itself: one line per failed export, unbounded. A thousand failures
// inside the summary interval must add nothing to the log.
func TestRepeatedExportErrorsInsideTheIntervalAreSuppressed(t *testing.T) {
	h := &collapsingErrorHandler{interval: time.Hour}

	out := captureLog(t, func() {
		for i := 0; i < 1000; i++ {
			h.Handle(errors.New("connection refused"))
		}
	})

	if got := countLines(out); got != 1 {
		t.Fatalf("1000 failures produced %d log lines, want 1: %q", got, out)
	}
	if h.suppressed != 999 {
		t.Fatalf("suppressed count = %d, want 999", h.suppressed)
	}
}

// Suppression is bounded, not permanent: once the interval has passed the
// condition is restated, and the restatement says how many were held back. A
// handler that never spoke again would hide an outage that is still happening.
func TestTheConditionIsRestatedAfterTheInterval(t *testing.T) {
	h := &collapsingErrorHandler{interval: time.Hour}

	out := captureLog(t, func() {
		for i := 0; i < 42; i++ {
			h.Handle(errors.New("connection refused"))
		}
		// Age the handler past its interval instead of sleeping through it.
		h.mu.Lock()
		h.lastLogged = time.Now().Add(-2 * time.Hour)
		h.mu.Unlock()
		h.Handle(errors.New("connection refused"))
	})

	if got := countLines(out); got != 2 {
		t.Fatalf("got %d log lines, want 2 (first + summary): %q", got, out)
	}
	// 42 calls in the loop: the 1st is logged, the next 41 are held, and the
	// call after the clock is aged makes 42.
	if !strings.Contains(out, "suppressed=42") {
		t.Fatalf("the summary did not carry the suppressed count: %q", out)
	}
	if h.suppressed != 0 {
		t.Fatalf("suppressed count = %d after a summary, want it reset to 0", h.suppressed)
	}
}

func TestNilErrorIsIgnored(t *testing.T) {
	h := &collapsingErrorHandler{interval: time.Hour}

	out := captureLog(t, func() { h.Handle(nil) })

	if got := countLines(out); got != 0 {
		t.Fatalf("a nil error produced %d log lines, want 0: %q", got, out)
	}
	if h.started {
		t.Fatal("a nil error consumed the one always-logged slot")
	}
}

// The wiring, which is the half a unit test of the type alone cannot prove: the
// SDK reports export failures through otel.Handle, so the handler is only doing
// anything if it is the one registered globally.
func TestInstallRegistersTheHandlerWithTheSDK(t *testing.T) {
	installCollapsingErrorHandler()

	out := captureLog(t, func() {
		for i := 0; i < 50; i++ {
			otel.Handle(errors.New("traces export: connection refused"))
		}
	})

	if got := countLines(out); got != 1 {
		t.Fatalf("50 errors through otel.Handle produced %d log lines, want 1: %q", got, out)
	}
	if !strings.Contains(out, "traces export: connection refused") {
		t.Fatalf("otel.Handle did not reach this package's handler: %q", out)
	}
}
