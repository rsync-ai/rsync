package main

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// defaultOpsAddr is the adapter's only HTTP listener (env METRICS_ADDR
// overrides it). It serves Prometheus /metrics and the build /version, nothing
// else. Internal-only: docker-compose.prod.yml drops the host mapping and the
// dev compose binds it to 127.0.0.1.
//
// api-gateway's deployment-drift check probes http://temporal-adapter:8082/version
// (internal/handlers/admin_drift.go), and its admin_drift_test.go reads this
// constant and the /version route below — change the port there too.
const defaultOpsAddr = ":8082"

// adapterStartedAt lets /version report uptime, which exposes "we deployed but
// the old container is still answering".
var adapterStartedAt = time.Now().UTC()

// versionInfo is api-gateway's handlers.VersionInfo shape, so the drift check
// reads every service the same way.
type versionInfo struct {
	Service    string `json:"service"`
	Commit     string `json:"commit"`
	BuiltAt    string `json:"built_at"`
	StartedAt  string `json:"started_at"`
	UptimeSecs int64  `json:"uptime_secs"`
}

// newOpsMux builds the handler for the listener on defaultOpsAddr.
func newOpsMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("GET /version", serveVersion)
	return mux
}

// serveVersion reports the build the image was made from. GIT_COMMIT and
// BUILD_TIME come from the Dockerfile's build args; an image built without
// them reports "dev"/"unknown", which the drift check flags.
func serveVersion(w http.ResponseWriter, _ *http.Request) {
	commit := os.Getenv("GIT_COMMIT")
	if commit == "" {
		commit = "dev"
	}
	builtAt := os.Getenv("BUILD_TIME")
	if builtAt == "" {
		builtAt = "unknown"
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(versionInfo{
		Service:    "backend-temporal-adapter",
		Commit:     commit,
		BuiltAt:    builtAt,
		StartedAt:  adapterStartedAt.Format(time.RFC3339),
		UptimeSecs: int64(time.Since(adapterStartedAt).Seconds()),
	})
}
