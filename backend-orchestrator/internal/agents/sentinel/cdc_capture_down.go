package sentinel

import (
	"context"
	"fmt"
	"strings"

	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
	log "github.com/sirupsen/logrus"
)

// CaptureDownFloor is how many consecutive unhealthy `debezium_task` probes the
// dependency probe (workers/dependency_probe.go, one probe per 15 s) must record
// before the capture-stopped issue is raised: ~60 s, the same floor the probe's own
// mcp_dest recovery waits (maybeRecover recoveryFloor), so a Connect rebalance or a
// connector restart the Sentinel itself issued never alarms.
const CaptureDownFloor = 4

// captureStoppedIssueID keys the capture-stopped issue. A class of its own:
// cdc-connector-down-* is the Sentinel's TERMINAL verdict after its restart budget is
// spent, and it stops the pipeline; this one is raised while the pipeline is still
// running and cleared by the tick that sees the capture healthy again. Sharing an id
// would let this resolver delete the terminal verdict.
func captureStoppedIssueID(pipelineID string) string {
	return fmt.Sprintf("cdc-capture-stopped-%s", pipelineID)
}

// captureDependencyQuery reads the dependency probe's verdict on each running CDC
// pipeline's CAPTURE side — its most recent `debezium_task` dependency (one row per
// Start; the connector name can change between runs, so an older row can name a
// connector that no longer exists and must not speak for the live one).
//
// It reads the probe's table rather than asking Kafka Connect again because the probe
// already distinguishes every way capture can be dead — Connect unreachable (the
// OOM-killed worker of KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED), connector 404, connector
// or task FAILED/UNASSIGNED — and, crucially, still answers when Connect does not: the
// Sentinel's own getDebeziumStatus returns early on an unreachable Connect, which is
// exactly the case this issue exists for.
const captureDependencyQuery = `
		SELECT DISTINCT ON (d.pipeline_id)
		       d.pipeline_id::text,
		       d.identifier,
		       COALESCE(h.status, 'unknown'),
		       COALESCE(h.last_error, ''),
		       COALESCE(h.consecutive_failures, 0)
		FROM pipeline_dependencies d
		JOIN pipelines p ON p.id = d.pipeline_id
		LEFT JOIN pipeline_dependency_health h ON h.dependency_id = d.id
		WHERE d.kind = 'debezium_task'
		  AND p.status = 'running'
		  AND (p.sync_mode = 'cdc' OR p.cdc_mode IS NOT NULL)
		ORDER BY d.pipeline_id, d.created_at DESC
	`

// captureDependency is one row of captureDependencyQuery.
type captureDependency struct {
	pipelineID          string
	connector           string
	status              string
	lastError           string
	consecutiveFailures int
}

// captureIsDown is the verdict: unhealthy for at least CaptureDownFloor consecutive
// probes. "degraded" (connector PAUSED/STOPPED on purpose, or a dropped source table,
// which has its own reporting) and "unknown" (not probed yet) never raise it.
func captureIsDown(dep captureDependency) bool {
	return dep.status == "unhealthy" && dep.consecutiveFailures >= CaptureDownFloor
}

// surfaceCaptureDown puts a dead CDC capture side on the pipeline itself
// (KI-DEBEZIUM-WORKER-DEATH-NOT-SURFACED).
//
// Before this, the only place a dead Debezium worker was recorded was
// pipeline_dependency_health. Debezium is the only producer into a CDC pipeline's
// topics, so with it dead every topic drains to lag 0 — the reading of a healthy idle
// lane — while pipelines.status stayed 'running' and no issue, event or notification
// was raised. The Sentinel's FAILED-connector path (handleFailedConnector) could not
// help: it needs Connect to answer, and an OOM-killed worker answers nothing.
//
// What this raises is an issue row + SENTINEL_ALERT domain event + user notification
// through emitCDCIssue, the same path every other Sentinel detector uses; the heal
// sweep then diagnoses its description per the healer rule (an unreachable Connect is
// "connection refused" → backoff-retry; a missing connector → escalate) as a
// recommendation, never an unattended action (heal/issue_sweep.go capIssueConfidence).
//
// It deliberately does NOT move pipelines.status out of 'running'. Every observer of a
// CDC stream — this Sentinel, the dependency probe, the WAL watchdog — selects running
// pipelines only, so a 'failed' pipeline would stop being probed and the recovery that
// clears this issue could never be observed. The list badge already reads the same
// dependency verdict (api-gateway pipelines.go, the pipeline_dependency_health EXISTS
// branch of the status CASE) and shows 'failed'.
//
// Recovery is not here: a FAILED connector or task is restarted by
// handleFailedConnector (bounded, 3 per 24 h, then escalate + stop); a worker that is
// down is restarted by its container policy, not by the orchestrator.
func (s *CDCSentinel) surfaceCaptureDown(ctx context.Context) {
	if s.db == nil {
		return
	}
	rows, err := s.db.QueryContext(ctx, captureDependencyQuery)
	if err != nil {
		log.WithError(err).Warn("🛡️ Sentinel could not read CDC capture dependency health")
		return
	}
	var deps []captureDependency
	for rows.Next() {
		var d captureDependency
		if err := rows.Scan(&d.pipelineID, &d.connector, &d.status, &d.lastError, &d.consecutiveFailures); err != nil {
			log.WithError(err).Warn("🛡️ Sentinel failed to scan a CDC capture dependency row")
			continue
		}
		deps = append(deps, d)
	}
	_ = rows.Close()

	for _, d := range deps {
		issueID := captureStoppedIssueID(d.pipelineID)
		if !captureIsDown(d) {
			// Only a probe that stopped reading unhealthy clears it; the resolver is a
			// DELETE by this pipeline's own id, so it cannot touch another class.
			s.resolveLagIssue(ctx, issueID, d.pipelineID)
			continue
		}
		// The fixed text avoids every phrase the diagnoser's CDC-provisioning rule matches
		// ("debezium", "replication slot", "change stream", …), which is checked ahead of
		// the transient-network rule: naming Debezium here would escalate every outage,
		// including a Connect restart that clears on its own. The probe's own last_error
		// is what classifies it.
		description := fmt.Sprintf(
			"CDC capture stopped: capture connector %s has been unhealthy for %d consecutive health probes, so no source changes are being captured. Consumer lag will read 0 because the producer is gone, not because the destination is caught up",
			d.connector, d.consecutiveFailures)
		if errText := llmscrub.ScrubMax(strings.TrimSpace(d.lastError), maxIssueErrorText); errText != "" {
			description += ": " + errText
		}
		s.emitCDCIssue(ctx, issueID,
			IssueTypeCaptureStopped, IssueSeverityCritical,
			"capture_stopped", d.pipelineID, "", "", description,
			map[string]interface{}{
				"connector":            d.connector,
				"consecutive_failures": d.consecutiveFailures,
				"detected_by":          "cdc_sentinel_capture_dependency",
			})
	}
}
