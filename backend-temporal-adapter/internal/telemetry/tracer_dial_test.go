package telemetry

import (
	"net"
	"testing"
	"time"
)

// blackholeListener accepts TCP connections and then says nothing. gRPC's
// WithBlock() dial completes the TCP handshake and then waits for the HTTP/2
// SETTINGS frame that never arrives, so the connection never reaches Ready.
//
// A closed port would NOT reproduce the bug: that fails fast with "connection
// refused" and the unbounded dial returns immediately. The whole defect is the
// case where the peer is reachable but never becomes ready -- which is exactly
// what a half-started or wedged collector looks like.
func blackholeListener(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Hold it open and never write. Closing would let gRPC fail fast.
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	return ln.Addr().String()
}

// TestInitTracerReturnsWhenCollectorNeverBecomesReady is the regression guard for
// the adapter hanging at startup with a collector that is present-but-not-ready.
//
// The assertion is that InitTracer RETURNS, not that it succeeds. Before the dial
// was bounded, this test does not fail with a wrong value -- it never finishes,
// which is precisely how the bug presented in production: no error, no log line,
// no process.
func TestInitTracerReturnsWhenCollectorNeverBecomesReady(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", blackholeListener(t))
	t.Setenv("OTEL_EXPORTER_OTLP_DIAL_TIMEOUT", "300ms")

	done := make(chan error, 1)
	go func() {
		_, err := InitTracer("temporal-adapter-test")
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected a dial error against a collector that never becomes ready, got nil")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("InitTracer did not return within 5s: the OTLP dial is unbounded again, " +
			"so main.go's non-fatal branch is unreachable and the adapter hangs at startup")
	}
}

// TestInitTracerIsANoopWhenDisabled pins the cheap path: with OTEL_ENABLED unset,
// nothing is dialled at all, so a blackholed endpoint costs nothing.
func TestInitTracerIsANoopWhenDisabled(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", blackholeListener(t))

	start := time.Now()
	shutdown, err := InitTracer("temporal-adapter-test")
	if err != nil {
		t.Fatalf("disabled tracer must not error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("disabled tracer must still return a usable shutdown fn")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("disabled tracer dialled anyway (took %s)", elapsed)
	}
}

// TestOTLPDialTimeoutNeverFallsBackToUnbounded: every rejected input must land on
// the default, because the one value this must never resolve to is "no timeout".
func TestOTLPDialTimeoutNeverFallsBackToUnbounded(t *testing.T) {
	for _, raw := range []string{"", "   ", "not-a-duration", "0s", "-3s"} {
		t.Setenv("OTEL_EXPORTER_OTLP_DIAL_TIMEOUT", raw)
		if got := otlpDialTimeout(); got != defaultOTLPDialTimeout {
			t.Errorf("otlpDialTimeout(%q) = %s, want %s", raw, got, defaultOTLPDialTimeout)
		}
	}
	t.Setenv("OTEL_EXPORTER_OTLP_DIAL_TIMEOUT", "1500ms")
	if got := otlpDialTimeout(); got != 1500*time.Millisecond {
		t.Errorf("otlpDialTimeout(1500ms) = %s, want 1.5s", got)
	}
}
