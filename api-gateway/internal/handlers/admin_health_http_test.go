package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// #53: Admin → Health listed no orchestrator, Kafka Connect or CDC sink.
func TestProbeHTTPService(t *testing.T) {
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failing.Close()
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closedURL := closed.URL
	closed.Close()

	if got := probeHTTPService("kafka-connect", ok.URL+"/"); got.Status != "up" || got.Service != "kafka-connect" {
		t.Errorf("2xx: got %+v, want up", got)
	}
	if got := probeHTTPService("orchestrator", failing.URL+"/health"); got.Status != "down" || got.Error != "health check returned HTTP 503" {
		t.Errorf("503: got %+v, want down with the status code", got)
	}
	if got := probeHTTPService("kafka-mcp-sink", closedURL+"/health"); got.Status != "down" || got.Error == "" {
		t.Errorf("unreachable: got %+v, want down with an error", got)
	}
}

func TestKafkaSinkURL(t *testing.T) {
	t.Setenv("KAFKA_SINK_URL", "")
	if got := kafkaSinkURL(); got != "http://kafka-mcp-sink-mcp:8000" {
		t.Errorf("default = %q", got)
	}
	t.Setenv("KAFKA_SINK_URL", " http://sink:9000 ")
	if got := kafkaSinkURL(); got != "http://sink:9000" {
		t.Errorf("override = %q", got)
	}
}

// #53: the Admin → Executions status filter was free text; a select now sends one
// value per status, and each covers every spelling a row can carry.
func TestAdminExecutionStatusValues(t *testing.T) {
	cases := map[string][]string{
		"success":          {"success", "completed"},
		"Failed":           {"failed", "error", "credential_check_failed", "silent_drop_detected", "silent_partial_drop_detected"},
		"cancelled":        {"cancelled", "canceled"},
		"running":          {"running"},
		"waiting_for_user": {"waiting_for_user"},
	}
	for in, want := range cases {
		got := adminExecutionStatusValues(in)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: got %v, want %v", in, got, want)
		}
	}
}
