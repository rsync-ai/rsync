// Package metrics defines the application-level Prometheus collectors for
// api-gateway. All collectors auto-register on package import via promauto,
// so the existing `/metrics` endpoint in `cmd/server/main.go` exposes them
// with no further wiring.
//
// Cardinality is bounded by design:
//   - `route` is the gin route *pattern* (c.FullPath(), e.g.
//     "/api/v1/connections/:id") — a small fixed set, NOT the raw path.
//     Unmatched requests are bucketed under "unmatched" so a flood of
//     bogus URLs cannot explode label cardinality.
//   - `method` is the HTTP verb (GET, POST, ...).
//   - `status` is the numeric HTTP status code as a string.
//   - `reason` on auth failures is unauthorized|forbidden.
//   - `result` on pipeline-run triggers is accepted|rejected.
//
// NO labels include free-form user input (pipeline_id, connection_id,
// raw URL). High-cardinality labels would explode memory — those belong
// on log-based dashboards, not Prometheus labels.
//
// F-Obs-2 (api-gateway domain metrics).
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// HTTPRequestsTotal counts every HTTP request handled by the gateway,
// labelled by method, route pattern, and response status code. Powers
// the 4xx/5xx rate, request-volume, and per-endpoint error dashboards.
var HTTPRequestsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rsync_gateway_http_requests_total",
		Help: "HTTP requests handled by api-gateway by method + route + status",
	},
	[]string{"method", "route", "status"},
)

// HTTPRequestDurationSeconds histograms request latency by method +
// route pattern. Use for p95/p99 latency SLIs per endpoint.
var HTTPRequestDurationSeconds = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "rsync_gateway_http_request_duration_seconds",
		Help:    "Latency of api-gateway HTTP requests by method + route",
		Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	},
	[]string{"method", "route"},
)

// AuthFailuresTotal counts requests rejected for authentication or
// authorization reasons (401 / 403). A spike is the leading signal of a
// credential-stuffing attempt or a broken/expired token after a deploy.
var AuthFailuresTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rsync_gateway_auth_failures_total",
		Help: "Auth failures by reason (unauthorized=401, forbidden=403)",
	},
	[]string{"reason"},
)

// PipelineRunTriggersTotal counts user-initiated pipeline run requests
// (POST /pipelines/:id/run), split by whether the gateway accepted the
// trigger (2xx) or rejected it (4xx/5xx, e.g. rate-limited or invalid).
var PipelineRunTriggersTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "rsync_gateway_pipeline_run_triggers_total",
		Help: "Pipeline run triggers by result (accepted|rejected)",
	},
	[]string{"result"},
)
