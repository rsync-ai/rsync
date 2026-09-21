package handlers

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
)

// The orchestrator's "still being set up" connection-test message must survive the
// gateway's scrubber intact and be recognized here, or the UI falls back to showing it
// as a hard failure. See KI-FIRST-CONNECTION-TEST-FALLS-BACK-TO-AN-UNUSABLE-STDIO-INTERPRETER.
func TestConnectorDeployingMessageIsRecognizedAndSurvivesScrub(t *testing.T) {
	// Verbatim shape of backend-orchestrator mcp.ConnectorDeployingMessage("MongoDB").
	msg := "The MongoDB connector is still being set up for first use — this can take a minute or two. Please try again shortly."

	if !isConnectorDeployingMessage(msg) {
		t.Fatalf("not recognized as deploying: %q", msg)
	}
	if got := llmscrub.Scrub(msg); got != msg {
		t.Errorf("scrubber altered the deploying message:\n got: %q\nwant: %q", got, msg)
	}
	if !isConnectorDeployingMessage(llmscrub.Scrub(msg)) {
		t.Error("scrubbed message lost the marker")
	}

	// Controls: real failures are not "deploying".
	for _, s := range []string{
		"Authentication failed.",
		"No module named 'pymongo'",
		"",
	} {
		if isConnectorDeployingMessage(s) {
			t.Errorf("wrongly classified as deploying: %q", s)
		}
	}
	// And the retryable message must not be mistaken for an OAuth activation delay.
	if isOAuthAuthError(msg) {
		t.Errorf("deploying message trips the OAuth retry classifier: %q", msg)
	}
}

// Lockstep guard: the gateway cannot import the orchestrator's internal/mcp package, so
// read the constant from source and compare.
func TestConnectorDeployingMarkerMatchesOrchestrator(t *testing.T) {
	path := filepath.Join("..", "..", "..", "backend-orchestrator", "internal", "mcp", "connector_deploying.go")
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read %s: %v (moved? update this guard, do not delete it)", path, err)
	}
	want := "const ConnectorDeployingMarker = " + strconv.Quote(connectorDeployingMarker)
	if !strings.Contains(string(src), want) {
		t.Errorf("%s no longer declares %s — gateway marker drifted from the orchestrator", path, want)
	}
}
