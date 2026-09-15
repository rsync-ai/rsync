package mcp

import (
	"strings"
	"testing"
)

// The bug this guards: the first connection test for a connector whose container has not
// finished deploying falls back to a stdio subprocess inside the orchestrator, whose
// interpreter carries no connector drivers, and the operator is handed a bare
// "No module named 'pymongo'" — an error about the fallback that reads as an error about
// the connector. The annotation is the only place the transport is known.
func TestAnnotateStdioFailureNamesTheTransportAndTheDependencyFailure(t *testing.T) {
	const raw = "No module named 'pymongo'"

	// Control: the raw connector error carries none of this on its own. Without this the
	// assertions below would look equally satisfied by a no-op annotation.
	if strings.Contains(raw, stdioFallbackMarker) {
		t.Fatalf("raw error already contains %q; this test cannot distinguish annotated from unannotated", stdioFallbackMarker)
	}

	degraded := &ServerInfo{
		ConnType:  "stdio",
		DepsError: "failed to create venv: exit status 1",
	}
	got := annotateStdioFailure("mongodb", "v1.0.0", degraded, raw)

	if !strings.Contains(got, raw) {
		t.Errorf("annotation dropped the original error: %q", got)
	}
	for _, want := range []string{stdioFallbackMarker, "mongodb@v1.0.0", "failed to create venv", "retry"} {
		if !strings.Contains(got, want) {
			t.Errorf("annotated error missing %q: %q", want, got)
		}
	}

	// Healthy deps: still name the transport (the container was unreachable, which is the
	// actionable fact), but do not claim a dependency failure that did not happen.
	healthy := &ServerInfo{ConnType: "stdio"}
	gotHealthy := annotateStdioFailure("mongodb", "v1.0.0", healthy, raw)
	if !strings.Contains(gotHealthy, stdioFallbackMarker) {
		t.Errorf("healthy-deps stdio failure must still name the transport: %q", gotHealthy)
	}
	if strings.Contains(gotHealthy, "dependencies could not be installed") {
		t.Errorf("claimed a dependency failure that did not occur: %q", gotHealthy)
	}

	// Idempotent: a re-annotated error must not grow a second copy.
	if again := annotateStdioFailure("mongodb", "v1.0.0", degraded, got); again != got {
		t.Errorf("annotation is not idempotent:\n first: %q\nsecond: %q", got, again)
	}

	// An empty error stays empty — never manufacture a failure message.
	if empty := annotateStdioFailure("mongodb", "v1.0.0", degraded, ""); empty != "" {
		t.Errorf("empty error was annotated into %q", empty)
	}
}
