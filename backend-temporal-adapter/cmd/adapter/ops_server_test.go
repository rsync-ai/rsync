package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestOpsMuxServesVersion pins what api-gateway's drift check reads from
// http://temporal-adapter:8082/version: a 200 with the build's commit and time.
// Before this route existed the probe got "connection refused" (it named :8080,
// where nothing listens) and the check could never report all_agree.
func TestOpsMuxServesVersion(t *testing.T) {
	t.Setenv("GIT_COMMIT", "0123abc")
	t.Setenv("BUILD_TIME", "2026-09-18T10:00:00Z")
	srv := httptest.NewServer(newOpsMux())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/version")
	if err != nil {
		t.Fatalf("GET /version: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET /version status = %d, want 200", resp.StatusCode)
	}
	var got versionInfo
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode /version: %v", err)
	}
	if got.Service != "backend-temporal-adapter" || got.Commit != "0123abc" || got.BuiltAt != "2026-09-18T10:00:00Z" {
		t.Errorf("/version = %+v, want service backend-temporal-adapter, commit 0123abc, built_at 2026-09-18T10:00:00Z", got)
	}
	if got.StartedAt == "" {
		t.Error("/version started_at is empty")
	}
}

// TestOpsMuxVersionFallsBackWithoutBuildArgs: an image built without the
// GIT_COMMIT/BUILD_TIME build args must say so ("dev"/"unknown", the same
// fallbacks as api-gateway) rather than report an empty commit.
func TestOpsMuxVersionFallsBackWithoutBuildArgs(t *testing.T) {
	t.Setenv("GIT_COMMIT", "")
	t.Setenv("BUILD_TIME", "")
	rec := httptest.NewRecorder()
	newOpsMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/version", nil))
	var got versionInfo
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatalf("decode /version: %v", err)
	}
	if got.Commit != "dev" || got.BuiltAt != "unknown" {
		t.Errorf("/version commit=%q built_at=%q, want dev/unknown", got.Commit, got.BuiltAt)
	}
}

// TestOpsMuxStillServesMetrics: moving the mux into newOpsMux must not drop the
// Prometheus endpoint the OTEL Collector scrapes.
func TestOpsMuxStillServesMetrics(t *testing.T) {
	rec := httptest.NewRecorder()
	newOpsMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want 200", rec.Code)
	}
}
