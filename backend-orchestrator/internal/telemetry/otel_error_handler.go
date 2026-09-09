package telemetry

// The OTel SDK reports every failed batch export through otel.Handle, and the
// SDK's default handler writes that error to the standard library logger. One
// unstructured line per export attempt, for as long as the collector is
// unreachable, with no backoff and no cap -- the SDK treats each failure as a
// fresh error rather than as a condition that is already being reported.
//
// Measured on a self-host install whose bundle ships no collector: 500 of the
// orchestrator's last 500 log lines were "traces export: ... connection
// refused", and ten minutes of its log held nothing else. That is not cosmetic.
// `docker logs` is the entire observability story for the self-host bundle, so a
// collector that is merely absent takes it away; and because the lines come from
// the standard logger rather than logrus, they also break the JSON stream that
// anything parsing these services' logs reads.
//
// OTEL_ENABLED=false (docker-compose.quickstart.yml) stops the exporter from
// being built at all, which covers the self-host bundle. This covers the half no
// flag can: cloud runs a collector deliberately, and a collector that restarts or
// goes away produces the identical spew with telemetry correctly configured.
// Collapse it into one warning plus a periodic summary carrying the suppressed
// count, emitted through logrus so it stays structured like everything else.
//
// This file is byte-identical in backend-orchestrator, api-gateway and
// backend-temporal-adapter -- the three Go services that build an OTLP exporter.
// They are separate modules with no shared telemetry package, so the copies stay
// in lockstep by guard rather than by import: patch all three or none.
// llm-service/tests/test_otel_export_errors_are_collapsed.py enforces both that
// every exporter-building package has this file and that the copies are equal.

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

// otelErrorSummaryInterval bounds how often a continuing export failure is
// restated. Five minutes turns ~7200 lines a day per service into ~288.
const otelErrorSummaryInterval = 5 * time.Minute

type collapsingErrorHandler struct {
	mu         sync.Mutex
	interval   time.Duration
	started    bool
	suppressed int
	lastLogged time.Time
}

// Handle implements otel.ErrorHandler. The first error is always emitted, so a
// broken exporter is never silent -- suppression that could hide the first
// report would trade a flooded log for no log, which is not an improvement.
// Everything after it is counted and restated at most once per interval.
func (h *collapsingErrorHandler) Handle(err error) {
	if err == nil {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	if !h.started {
		h.started = true
		h.lastLogged = now
		log.WithError(err).Warn("OpenTelemetry export failed; further failures are summarised, not repeated")
		return
	}

	h.suppressed++
	if now.Sub(h.lastLogged) < h.interval {
		return
	}
	log.WithError(err).WithField("suppressed", h.suppressed).Warn("OpenTelemetry export still failing")
	h.suppressed = 0
	h.lastLogged = now
}

// installCollapsingErrorHandler replaces the SDK's default error handler. Call it
// before constructing an exporter: registering it afterwards leaves a window in
// which the default handler is still the one on duty.
func installCollapsingErrorHandler() {
	otel.SetErrorHandler(&collapsingErrorHandler{interval: otelErrorSummaryInterval})
}
