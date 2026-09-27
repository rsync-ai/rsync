package workers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor"
)

func findService(services []preflightService, name string) (preflightService, bool) {
	for _, s := range services {
		if s.name == name {
			return s, true
		}
	}
	return preflightService{}, false
}

// On prod (2026-09-26) a Postgres → GCS batch run logged "Checking minio-mcp…
// ready", which read as a check of the wrong service. MinIO is checked on every
// batch run because the executor stages any large batch there whatever the
// destination, so the check stays — but it says what it is for, and it is
// optional because the executor falls back to chunked Kafka.
func TestBatchPreflightChecksMinioAsAnOptionalStagingStore(t *testing.T) {
	p := &infraPreflightStage{}
	task := executor.ExecutorTask{
		Source:      &executor.ConnectorConfig{Type: "postgresql"},
		Destination: &executor.ConnectorConfig{Type: "gcs"},
	}

	services := p.requiredServices(task, false)
	minio, ok := findService(services, "minio-mcp")
	if !ok {
		t.Fatalf("batch preflight no longer checks minio-mcp; services = %+v", services)
	}
	if !strings.Contains(minio.displayName(), "staging") {
		t.Errorf("minio-mcp is shown to the user as %q — it must say it is the staging store", minio.displayName())
	}
	if minio.fallback == "" {
		t.Error("minio-mcp is required again: an unreachable MinIO would fail a run the executor can finish over chunked Kafka")
	}
	if minio.pollAttempts() >= preflightKafkaConnectRetries {
		t.Errorf("optional minio-mcp polls %d times, as long as a required service", minio.pollAttempts())
	}

	// Every CDC service is required: none of them has a fallback.
	for _, s := range p.requiredServices(task, true) {
		if s.fallback != "" {
			t.Errorf("CDC service %s became optional (fallback %q)", s.name, s.fallback)
		}
		if s.name == "minio-mcp" {
			t.Error("CDC preflight checks minio-mcp, which CDC does not use")
		}
	}
}

func TestAnUnreachableOptionalServiceDegradesInsteadOfFailing(t *testing.T) {
	orig := preflightInfraRetryDelay
	preflightInfraRetryDelay = time.Millisecond
	t.Cleanup(func() { preflightInfraRetryDelay = orig })

	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer down.Close()

	// A nil agent has no MCP manager, so the tool-generator start is skipped.
	p := &infraPreflightStage{httpClient: down.Client()}
	optional := preflightService{
		name:      "minio-mcp",
		label:     "large-batch staging store (minio-mcp)",
		kind:      "mcp_core",
		healthURL: down.URL,
		mcpName:   "minio",
		fallback:  "large batches will be sent to Kafka in chunks instead",
	}

	got := p.checkOne(context.Background(), "pid", "eid", optional)
	if !got.OK || got.Mode != "fallback" {
		t.Fatalf("optional service down: got OK=%v Mode=%q, want OK=true Mode=fallback", got.OK, got.Mode)
	}
	if got.Err == nil {
		t.Error("the fallback result dropped the underlying error")
	}

	// Control: the same service without a fallback is required and fails.
	required := optional
	required.fallback = ""
	if r := p.checkOne(context.Background(), "pid", "eid", required); r.OK {
		t.Fatalf("a required service that is down passed preflight: %+v", r)
	}
}

func TestSummarizePreflightSeparatesFailuresFromFallbacks(t *testing.T) {
	failures, degraded := summarizePreflight([]preflightResult{
		{Service: "postgresql MCP (source)", OK: true, Mode: "docker_http"},
		{Service: "large-batch staging store (minio-mcp)", OK: true, Mode: "fallback",
			Err: errors.New("down"), Fallback: "large batches will be sent to Kafka in chunks instead"},
	})
	if len(failures) != 0 {
		t.Fatalf("a fallback counted as a failure: %v", failures)
	}
	if len(degraded) != 1 || !strings.Contains(degraded[0], "Kafka in chunks") {
		t.Fatalf("degraded = %v, want the fallback named", degraded)
	}

	failures, _ = summarizePreflight([]preflightResult{
		{Service: "kafka-connect", OK: false, Err: errors.New("connect_unavailable")},
	})
	if len(failures) != 1 || !strings.Contains(failures[0], "kafka-connect") {
		t.Fatalf("failures = %v, want kafka-connect", failures)
	}
}
