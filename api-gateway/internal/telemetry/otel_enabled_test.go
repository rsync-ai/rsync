package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
)

// OTEL_ENABLED was loaded into TelemetryConfig.Enabled and then never read:
// InitTracerWithConfig forwarded every other field to
// InitTracerWithConfigOptions and dropped Enabled. The flag was decorative.
//
// That matters because the endpoint defaults to localhost:4317 (the sidecar
// pattern) and the gRPC exporter connects lazily, so a deployment that ships no
// collector — the quickstart bundle — gets no startup error, just an export
// retry per batch forever. Setting OTEL_ENABLED=false was the documented way out
// and did nothing.

func TestLoadTelemetryConfigParsesOTELEnabled(t *testing.T) {
	cases := []struct {
		raw  string
		want bool
	}{
		{"", true}, // unset, and compose's ${VAR:-} empty string: keep the default
		{"true", true},
		{"TRUE", true}, // was false before: the old check was a literal == "true"
		{"1", true},
		{"false", false},
		{"FALSE", false},
		{"0", false},
		{"off", false},
		{"nonsense", true}, // unparseable must not silently disable telemetry
	}
	for _, c := range cases {
		t.Run("OTEL_ENABLED="+c.raw, func(t *testing.T) {
			t.Setenv("OTEL_ENABLED", c.raw)
			if got := LoadTelemetryConfig("api-gateway").Enabled; got != c.want {
				t.Errorf("Enabled = %v, want %v", got, c.want)
			}
		})
	}
}

// The behavioural guard: with Enabled=false no recording tracer provider may be
// installed, so no exporter exists and there is no retry loop.
//
// Two ways this assertion goes vacuous, both hit while writing it: a type
// assertion against *sdktrace.TracerProvider passes on the broken code too
// (otel.GetTracerProvider() can hand back the global delegating wrapper), and
// IsRecording() reads false on an already-ended span whatever created it. So:
// ask the provider for a span and read IsRecording() while it is still open.
// This is the only test in the package, so the global starts out as the SDK no-op.
func TestInitTracerWithConfigHonoursDisabled(t *testing.T) {
	before := otel.GetTracerProvider()
	t.Cleanup(func() { otel.SetTracerProvider(before) })

	shutdown, err := InitTracerWithConfig(TelemetryConfig{
		OTLPEndpoint:   "localhost:4317",
		ServiceName:    "api-gateway",
		ServiceVersion: "1.0.0",
		Enabled:        false,
		SamplingRate:   1.0,
		Insecure:       true,
	})
	if err != nil {
		t.Fatalf("InitTracerWithConfig returned error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown func is nil; main.go defers a call to it")
	}
	// IsRecording() must be read BEFORE End(): an ended span reports false no
	// matter what provider made it, which would make this assertion vacuous.
	_, span := otel.GetTracerProvider().Tracer("guard").Start(context.Background(), "probe")
	recording := span.IsRecording()
	span.End()
	if recording {
		t.Error("a recording TracerProvider was installed despite Enabled=false — its exporter retries to the endpoint forever")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown returned error: %v", err)
	}
}
