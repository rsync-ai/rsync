package sentinel

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// The Kafka Connect health probe used to hardcode "http://kafka-connect:8083/"
// while every other Connect caller in the orchestrator resolved KAFKA_CONNECT_URL.
// On any deployment that does not name the service literally "kafka-connect" —
// Kubernetes, where the Service carries the Helm release prefix — that one probe
// resolved nothing and pinned infrastructure:kafka-connect to unhealthy forever,
// which is indistinguishable from a real Connect outage on the surface the
// sentinel and the healer read.
//
// These tests are the guard. The first one fails if the URL is hardcoded again:
// the stub server is on a random loopback port that no hardcoded name can reach.
func TestCheckKafkaConnectHealth_UsesEnvURL(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Deliberately WITHOUT a trailing slash: the probe must append exactly one.
	t.Setenv("KAFKA_CONNECT_URL", srv.URL)

	h := NewHealthMonitor(nil, nil, DefaultSentinelConfig(), nil)
	h.checkKafkaConnectHealth(context.Background())

	got := infraHealth(t, h, "infrastructure:kafka-connect")
	if got.Status != HealthStatusHealthy {
		t.Fatalf("probe did not reach the configured URL: status=%q err=%q (KAFKA_CONNECT_URL=%s)",
			got.Status, got.LastError, srv.URL)
	}
	if gotPath != "/" {
		t.Fatalf("Connect root probe hit %q, want %q", gotPath, "/")
	}
	// The recorded URL is what an operator sees when diagnosing; it must be the
	// one actually probed, not the compile-time default.
	if url, _ := got.Metadata["url"].(string); !strings.HasPrefix(url, srv.URL) {
		t.Fatalf("recorded url = %q, want it to start with %q", url, srv.URL)
	}
}

// A URL that already ends in "/" must not become "//" — Connect answers on "/"
// and some ingress paths 404 a doubled slash, which would read as an outage.
func TestCheckKafkaConnectHealth_TrailingSlashNotDoubled(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	t.Setenv("KAFKA_CONNECT_URL", srv.URL+"/")

	h := NewHealthMonitor(nil, nil, DefaultSentinelConfig(), nil)
	h.checkKafkaConnectHealth(context.Background())

	if gotPath != "/" {
		t.Fatalf("Connect root probe hit %q, want %q", gotPath, "/")
	}
	if got := infraHealth(t, h, "infrastructure:kafka-connect"); got.Status != HealthStatusHealthy {
		t.Fatalf("status=%q err=%q", got.Status, got.LastError)
	}
}

// Unset must keep the compose-network default, so nothing about the existing
// docker-compose deployments changes.
func TestCheckKafkaConnectHealth_DefaultsToComposeServiceName(t *testing.T) {
	t.Setenv("KAFKA_CONNECT_URL", "")
	if got := kafkaConnectURLFromEnv(); got != "http://kafka-connect:8083" {
		t.Fatalf("default = %q, want %q", got, "http://kafka-connect:8083")
	}
}
