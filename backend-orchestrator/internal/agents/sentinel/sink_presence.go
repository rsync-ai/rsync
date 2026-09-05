package sentinel

// sink_presence.go — "does the sink container hold a worker for this consumer
// group?", asked once and answered in three values.
//
// This used to be a bool (sinkWorkerAbsent) living inside the CDC respawn rung,
// and the bool was the bug. The container has four answers — not_found, running,
// stopped, crashed — plus every way a call can fail to produce one at all: a
// transport error, a nil body, or the status-less {"success": false, "error": ...}
// its generic exception handler returns. A bool has room for "absent" and "not
// absent", so all of the second group landed in "not absent", and the caller read
// that as PRESENT:
//
//	if !absent { s.resolveLagIssue(ctx, sinkAbsentIssueID(pipelineID), pipelineID) }
//
// A sink container erroring on every sink_status call therefore deleted the
// escalation that said its worker was missing. The alarm cleared itself precisely
// when it was least safe to clear.
//
// Three values make that unrepresentable: only sinkPresencePresent resolves
// anything, only sinkPresenceAbsent acts, and sinkPresenceUnknown does neither —
// it leaves whatever the last definite answer established standing.
//
// It lives in its own file, on no receiver, because two sentinels need it. CDC
// probes on the source-lag tick and may respawn; batch probes on its own tick and
// only escalates. Sharing the probe is the point: two implementations of "is the
// sink running?" that disagree is the exact class of bug this package keeps
// producing.

import (
	"context"
	"strings"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// sinkPresence is what one sink_status call established.
type sinkPresence int

const (
	// sinkPresenceUnknown — the call failed, or the container answered something
	// outside its own vocabulary. NOT a synonym for healthy: it is the absence of
	// evidence, and the callers treat it as such.
	sinkPresenceUnknown sinkPresence = iota
	// sinkPresencePresent — the container holds a worker for this group. It may be
	// running, stopped, or crashed; in all three the container's own supervisor owns
	// the lifecycle and nothing out here should intervene.
	sinkPresencePresent
	// sinkPresenceAbsent — the container answered not_found: it holds no worker for
	// this group at all. The only value that arms a respawn or raises an alarm.
	sinkPresenceAbsent
)

func (p sinkPresence) String() string {
	switch p {
	case sinkPresencePresent:
		return "present"
	case sinkPresenceAbsent:
		return "absent"
	default:
		return "unknown"
	}
}

// sinkStatusNotFound is the sink container's literal answer when it holds no worker
// for a consumer group. It is the ONLY value that means absence — "stopped" and
// "crashed" mean the worker is registered and the container's own supervisor owns
// its lifecycle.
const sinkStatusNotFound = "not_found"

// sinkStatusesMeaningRegistered is the container's full vocabulary for "I have this
// worker" (connector.py sink_status: running / stopped / crashed). Enumerated rather
// than treated as a default so an unrecognised string — a future status, a truncated
// body, a proxy's error page — reads as unknown instead of quietly as healthy. That
// default is what this file exists to remove.
var sinkStatusesMeaningRegistered = map[string]bool{
	"running": true,
	"stopped": true,
	"crashed": true,
}

// sinkStatusProbe is the single call the presence probe makes. Narrow on purpose:
// *mcp.Client satisfies it, and so does a test double, which is what lets the
// tri-state logic be tested without a live container.
type sinkStatusProbe interface {
	ExecuteWithContext(ctx context.Context, req mcp.ExecuteRequest) (*mcp.ExecuteResponse, error)
}

// probeSinkPresence asks the sink container about ONE consumer group.
//
// consumerGroup must come from handlers.ResolveSinkConsumerGroup (or, for batch,
// from the dependency manifest directly) — never from re-deriving "sink-<pid8>".
// The executor mints three shapes, and a wrong name here is not a silent no-op: the
// container correctly answers not_found for a group it never created, so a healthy
// pipeline reads as absent.
func probeSinkPresence(ctx context.Context, probe sinkStatusProbe, consumerGroup string) sinkPresence {
	if probe == nil {
		return sinkPresenceUnknown
	}

	resp, err := probe.ExecuteWithContext(ctx, mcp.ExecuteRequest{
		Connector: "kafka-mcp-sink",
		Operation: "sink_status",
		Config:    map[string]string{},
		Params: map[string]interface{}{
			"config": map[string]interface{}{
				"consumer_group": consumerGroup,
			},
		},
	})
	if err != nil {
		log.WithError(err).WithField("consumer_group", consumerGroup).
			Debug("🛡️ sink presence: sink_status call failed (unknown)")
		return sinkPresenceUnknown
	}
	if resp == nil || resp.Result == nil {
		return sinkPresenceUnknown
	}

	// Read the connector's own `status`, never resp.Success: Success is false for
	// BOTH not_found and every genuine error (bad params, container erroring,
	// transport failure), so keying on it would call a healthy worker absent on any
	// hiccup.
	raw, _ := resp.Result["status"].(string)
	status := strings.ToLower(strings.TrimSpace(raw))

	switch {
	case status == sinkStatusNotFound:
		return sinkPresenceAbsent
	case sinkStatusesMeaningRegistered[status]:
		return sinkPresencePresent
	default:
		log.WithFields(log.Fields{
			"consumer_group": consumerGroup,
			"status":         raw,
		}).Debug("🛡️ sink presence: sink_status returned no recognised status (unknown)")
		return sinkPresenceUnknown
	}
}
