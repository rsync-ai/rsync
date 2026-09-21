package sentinel

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// DefaultSourceFreshnessStallAfter is how long a RUNNING CDC source connector may hold
// the same committed stream position before the freshness alarm fires. Overridable via
// CDC_SOURCE_FRESHNESS_STALL_AFTER (minimum 30s).
//
// Twenty minutes is four missed heartbeats at the shipped five-minute interval, so a
// single slow or skipped heartbeat never alarms.
const DefaultSourceFreshnessStallAfter = 20 * time.Minute

// sourceFreshnessState is what the freshness alarm remembers about one connector
// between ticks.
type sourceFreshnessState struct {
	// fingerprint identifies the connector's committed source position. Its contents
	// are opaque and connector-specific (a Mongo resume token, a Postgres LSN, a MySQL
	// binlog coordinate); only whether it CHANGED is ever interpreted.
	fingerprint string
	// movingAt is the last time the connector was seen making progress: its committed
	// source position changed.
	movingAt time.Time
}

// decideSourceFreshnessAlarm says whether a source connector that Kafka Connect calls
// RUNNING has actually stopped advancing through its source's change stream.
//
// Connect's task state cannot answer this. Debezium retries a retriable error on a loop
// without ever leaving RUNNING, so a connector whose MongoDB resume token has aged out
// of the oplog reports a fully green /status while it moves nothing — for as long as
// nobody looks at the container logs (KI-CDC-MONGO-RESUME-TOKEN-SILENT-STALL). The
// committed source offset is the signal that does answer it: a streaming connector
// advances it, a looping one never does.
//
// This is only sound when the connector emits heartbeats, because a genuinely idle
// source has nothing to advance past and would otherwise alarm for being quiet. The
// caller enforces that gate (sourceHeartbeatEnabled); with heartbeats on, the position
// advances on a timer whether or not the source is writing, so "idle but alive" and
// "dead" stop looking alike.
//
// The first reading for a connector only starts the clock, because it says nothing about
// how long the position has been frozen.
func decideSourceFreshnessAlarm(prev sourceFreshnessState, seen bool, fingerprint string, now time.Time, stallAfter time.Duration) (sourceFreshnessState, bool) {
	next := sourceFreshnessState{fingerprint: fingerprint, movingAt: prev.movingAt}
	if !seen || fingerprint != prev.fingerprint {
		next.movingAt = now
		return next, false
	}
	return next, now.Sub(prev.movingAt) >= stallAfter
}

// observeSourceFreshness records this tick's committed source position for a connector
// and returns whether its freshness alarm should be raised, and for how long the
// position has not moved.
func (s *CDCSentinel) observeSourceFreshness(connectorName, fingerprint string, now time.Time) (bool, time.Duration) {
	stallAfter := walDurationFromEnv("CDC_SOURCE_FRESHNESS_STALL_AFTER", DefaultSourceFreshnessStallAfter)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sourceFreshness == nil {
		s.sourceFreshness = make(map[string]sourceFreshnessState)
	}
	prev, seen := s.sourceFreshness[connectorName]
	next, alarm := decideSourceFreshnessAlarm(prev, seen, fingerprint, now, stallAfter)
	s.sourceFreshness[connectorName] = next
	return alarm, now.Sub(next.movingAt)
}

// forgetSourceFreshness drops a connector's freshness bookkeeping, so a connector that
// is deleted and later recreated under the same name starts from a clean slate instead
// of inheriting a stale frozen-since timestamp.
func (s *CDCSentinel) forgetSourceFreshness(keep map[string]bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name := range s.sourceFreshness {
		if !keep[name] {
			delete(s.sourceFreshness, name)
		}
	}
}

// sourceStalledIssueID keys the freshness alarm. It is a class of its own: neither lag
// resolver may clear it, and it must not collide with cdc-connector-down-*, which needs
// Connect to admit the connector failed. This alarm exists for the case where it never
// does.
func sourceStalledIssueID(pipelineID string) string {
	return fmt.Sprintf("cdc-source-stalled-%s", pipelineID)
}

// checkSourceFreshness is the RUNNING-connector counterpart of handleFailedConnector:
// it asks whether a connector Kafka Connect calls healthy is actually moving.
//
// Every rung is a skip rather than an alarm when it cannot answer — no offsets endpoint
// (Connect < 3.6), no heartbeats configured, no committed position yet. A watchdog that
// alarms on its own blindness is worse than no watchdog.
func (s *CDCSentinel) checkSourceFreshness(ctx context.Context, pipelineID, connectorName string, connState interface{}) {
	if s.db == nil || pipelineID == "" {
		return
	}

	cfg, ok := s.fetchSourceConfig(ctx, connectorName)
	if !ok || !sourceHeartbeatEnabled(cfg) {
		// Without heartbeats a frozen position means "the source had no writes", which is
		// perfectly healthy. Only a heartbeating connector is expected to advance on a
		// timer, and only then does frozen mean stalled.
		return
	}

	offsets, ok := s.fetchSourceOffsets(ctx, connectorName)
	if !ok {
		return
	}
	fingerprint, ok := sourceOffsetFingerprint(offsets)
	if !ok {
		return
	}

	stalled, stalledFor := s.observeSourceFreshness(connectorName, fingerprint, time.Now())
	issueID := sourceStalledIssueID(pipelineID)
	if !stalled {
		// A position that moved is the only positive proof this connector is alive, so it
		// is the only thing that clears the alarm.
		s.resolveLagIssue(ctx, issueID, pipelineID)
		return
	}

	// Deliberately free of the phrases the re-snapshot rule matches ("resume token",
	// "change stream history lost", …): a frozen position has several possible causes and
	// naming one of them here would make the diagnoser confidently pick it every time.
	// The connector's own error text, when Connect exposes any, is appended below and is
	// what should drive the classification.
	description := fmt.Sprintf(
		"CDC source connector %s has not advanced its position in the source's change stream for %s, although Kafka Connect still reports it RUNNING. Heartbeats are enabled on this connector, so an idle source would still be advancing — no change events are reaching the destination. Likely causes: the stored stream position is no longer available on the source (for MongoDB, the oplog has rolled past it), the source is unreachable, or the connector is retrying a permanent error without failing. Check the connector task log for the underlying error",
		connectorName, stalledFor.Round(time.Second))
	if errText := diagnosableErrorText(traceFromConnectorState(connState), maxIssueErrorText); errText != "" {
		description += ": " + errText
	}

	s.emitCDCIssue(ctx, issueID,
		IssueTypeSourceStreamStalled, IssueSeverityCritical,
		"source_stream_stalled", pipelineID, "", "", description,
		map[string]interface{}{
			"connector":           connectorName,
			"stalled_for_seconds": int64(stalledFor.Seconds()),
			"connector_state":     "RUNNING",
			"detected_by":         "cdc_sentinel_source_freshness",
		})

	log.WithFields(log.Fields{
		"pipeline_id": pipelineID,
		"connector":   connectorName,
		"stalled_for": stalledFor.Round(time.Second).String(),
	}).Error("🛡️ Sentinel raised a source-stream-stalled issue (connector RUNNING but not advancing)")
}

// sourceOffsetFingerprint reduces a Kafka Connect GET /connectors/<name>/offsets payload
// to a single comparable string.
//
// Shape (KIP-875, Connect 3.6+):
//
//	{"offsets":[{"partition":{"server_id":"..."},"offset":{"resume_token":"...","sec":N,"ord":N}}]}
//
// The offset object is re-encoded rather than read field by field on purpose: the keys
// differ per connector class (resume_token/sec/ord for MongoDB, lsn for Postgres,
// file/pos for MySQL), and the alarm only ever asks whether the value changed. Go
// marshals map keys in sorted order, so the encoding is stable across ticks.
//
// Returns ok=false when the payload carries no offsets at all — a connector that has not
// committed a position yet is starting up, not stalled.
func sourceOffsetFingerprint(payload map[string]interface{}) (string, bool) {
	if payload == nil {
		return "", false
	}
	offsets, ok := payload["offsets"].([]interface{})
	if !ok || len(offsets) == 0 {
		return "", false
	}
	encoded, err := json.Marshal(offsets)
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

// sourceHeartbeatEnabled reports whether a connector is configured to emit heartbeats,
// which is what makes a frozen offset mean "stalled" rather than "idle".
//
// It reads exactly one key. A Debezium source config also carries
// mongodb.connection.string, database.password and friends, so the payload is parsed and
// the single value extracted — never logged, never returned, never handed to an LLM.
func sourceHeartbeatEnabled(config map[string]interface{}) bool {
	if config == nil {
		return false
	}
	raw, ok := config["heartbeat.interval.ms"]
	if !ok {
		return false
	}
	ms, err := strconv.Atoi(strings.TrimSpace(fmt.Sprint(raw)))
	return err == nil && ms > 0
}

// fetchSourceOffsets reads a connector's committed source position via the Connect REST
// API (KIP-875, Connect 3.6+).
//
// Older workers answer 404 or 405 here. That is not an error worth alarming on — it just
// means this rung is unavailable on that cluster — so the caller skips the connector and
// the rest of the Sentinel is unaffected.
func (s *CDCSentinel) fetchSourceOffsets(ctx context.Context, connectorName string) (map[string]interface{}, bool) {
	return s.getConnectJSON(ctx, "/connectors/"+connectorName+"/offsets")
}

// fetchSourceConfig reads a connector's config so the heartbeat gate can be evaluated.
// The payload contains credentials; see sourceHeartbeatEnabled for how it is handled.
func (s *CDCSentinel) fetchSourceConfig(ctx context.Context, connectorName string) (map[string]interface{}, bool) {
	return s.getConnectJSON(ctx, "/connectors/"+connectorName+"/config")
}

// getConnectJSON GETs a Connect endpoint and decodes a JSON object, reporting ok=false
// for any transport error, non-2xx status, or undecodable body. Every caller here treats
// a failure as "this rung has nothing to say about this connector this tick".
func (s *CDCSentinel) getConnectJSON(ctx context.Context, path string) (map[string]interface{}, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.connectURL+path, nil)
	if err != nil {
		return nil, false
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false
	}
	var out map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, false
	}
	return out, true
}
