package workflows

// Terminal-run alerting — the one site that tells a team a run stopped or failed.
//
// Why here and nowhere else
// ─────────────────────────
// A run can fail in dozens of places: the executor returns Status:"failed" at 13+
// call sites, the workflow emits STAGE_FAILED at ~30. None of those is where a RUN
// becomes terminally failed — Architecture Phase 1 made the workers stateless (they
// emit to pipeline.domain.events and write no DB row), the executor is reachable by
// three different dispatch paths (Redis correlation poller, the Kafka control topic,
// and the native Temporal activity), and api-gateway's Event Projector is explicitly
// best-effort and loses on conflict. Instrumenting any of them would give either
// duplicate alerts or silence, depending on which path the run took.
//
// UpdatePipelineStatusActivity is the single authoritative terminal-write site, and
// it is reached from a deterministic defer in NLPipelineWorkflowV2 that fires on
// EVERY exit path ("We do this in a deterministic defer so EVERY exit path updates
// the DB row once" — nl_pipeline_v2_workflow.go). One emit there therefore covers
// every way a run can end, exactly once. It also covers the case that motivated the
// "no data loss" requirement: the postflight silent-drop guard runs in that same
// function and downgrades completed→failed when destination-truth row stats show a
// drop, so a run that LOOKED successful still alerts.
//
// Delivery
// ────────
// The payload is the shape api-gateway's notifier consumes off rsync.notifications
// (internal/notifier/notifier.go: notificationPayload + structuredErrorView), which
// persists it to pipeline_notifications and fans it out to Slack and email. Three
// details of that contract are load-bearing and easy to break:
//
//   - The envelope path activates ONLY when error.code is non-empty. An envelope
//     with a blank code is silently ignored and the alert degrades to the legacy
//     top-level shape, losing severity and remediation.
//   - error.dedup_subject narrows the notifier's 60-minute dedup window to a single
//     event. We set it to the execution id, so a pipeline that fails on three
//     consecutive scheduled runs raises three alerts, while an at-least-once
//     redelivery of THIS activity raises one.
//   - The topic must be qualified with kafkaclient.Topic(). KAFKA_TOPIC_PREFIX is
//     applied by the orchestrator's kafka.Manager at its produce chokepoint, and by
//     the notifier at subscribe time; this package talks to sarama directly, so an
//     unqualified literal here would publish to a topic nobody reads, and Kafka
//     raises nothing at either end.
//
// Best-effort by construction: emitTerminalRunNotification returns nothing and is
// called AFTER the terminal transaction commits. A broker outage must never turn a
// recorded failure into a failed activity that Temporal then retries.

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/rsync-ai/shared/kafkaclient"
	log "github.com/sirupsen/logrus"
)

// notificationsTopic is the canonical spelling. Nothing may hand this to sarama
// directly — buildTerminalNotification's caller qualifies it via kafkaclient.Topic.
const notificationsTopic = "rsync.notifications"

// The stable codes this site emits. Each one has an entry in api-gateway's
// notifier catalog (internal/notifier/catalog.go); TestEveryEmittedCodeHasCatalogCopy
// keeps that true. A code with no entry still renders a safe neutral sentence — the
// notifier never puts a raw code in a headline — but it loses the curated
// "is my data still moving" line, which is the whole point of the alert.
const (
	// codeRunFailed — a run ended in failure for a reason nothing upstream
	// classified. system_error / developer mirrors the repo's own convention for an
	// unclassified failure (diagnose.FromDiagnosis, CategoryUnknown).
	codeRunFailed = "PIPELINE_RUN_FAILED"

	// codeRunStopped — a run ended before finishing without failing: a cancel, or a
	// workflow terminated out from under it. The user asked to hear about this too;
	// a run that silently stops is indistinguishable from one still working.
	codeRunStopped = "PIPELINE_RUN_STOPPED"

	// codeSilentDrop — the postflight guard downgraded completed→failed because the
	// destination did not receive what the source read. Already in the catalog
	// ("Some rows may be missing"), so it is reused rather than minted.
	codeSilentDrop = "RSYNC_BUG_SILENT_DROP"
)

// structuredErrorEnvelope is the projection of diagnose.StructuredError that the
// notifier reads. It is declared here rather than imported because the orchestrator
// (which owns diagnose) and this adapter are separate Go modules; the wire contract,
// not the type, is what is shared. Field names must match structuredErrorView in
// api-gateway/internal/notifier/notifier.go exactly — a mismatch is silent.
type structuredErrorEnvelope struct {
	FailureType     string `json:"failure_type"`
	Code            string `json:"code"`
	Severity        string `json:"severity"`
	Audience        string `json:"audience"`
	UserMessage     string `json:"user_message"`
	InternalMessage string `json:"internal_message,omitempty"`
	PipelineID      string `json:"pipeline_id,omitempty"`
	OccurredAt      string `json:"occurred_at,omitempty"`
	DedupSubject    string `json:"dedup_subject,omitempty"`
}

// terminalNotification is the top-level wire shape, matching the healer's
// notifyUserStructured payload so both producers look identical to the consumer.
type terminalNotification struct {
	Type       string                  `json:"type"`
	PipelineID string                  `json:"pipeline_id"`
	Message    string                  `json:"message"`
	Timestamp  string                  `json:"timestamp"`
	ActionURL  string                  `json:"action_url"`
	Error      structuredErrorEnvelope `json:"error"`
}

// buildTerminalNotification renders the alert for one terminal run outcome, or
// reports ok=false when this outcome must not alert.
//
// Pure (no I/O, no clock) so the wire contract is unit-testable: `now` is supplied
// by the caller.
func buildTerminalNotification(pipelineID, executionID, status, errorMessage string, silentDrop bool, now time.Time) (terminalNotification, bool) {
	pipelineID = strings.TrimSpace(pipelineID)
	if pipelineID == "" {
		// The notifier drops a payload with no pipeline id (it cannot resolve an
		// owner to deliver to), so emitting one would be pure noise on the topic.
		return terminalNotification{}, false
	}

	env := structuredErrorEnvelope{
		FailureType: "system_error",
		Audience:    "developer",
		PipelineID:  pipelineID,
		OccurredAt:  now.UTC().Format(time.RFC3339),
		// Scope dedup to THIS run. Without it the notifier's 60-minute window would
		// collapse every failure of a pipeline into the first one — including a
		// scheduled pipeline retrying every 15 minutes, which is exactly the case a
		// team most needs to see repeated.
		DedupSubject: strings.TrimSpace(executionID),
	}

	switch status {
	case "failed":
		env.Severity = "error"
		if silentDrop {
			env.Code = codeSilentDrop
			env.UserMessage = "Some rows read from the source did not reach the destination, so this run was marked failed."
		} else {
			env.Code = codeRunFailed
			env.UserMessage = "The pipeline run failed before it finished."
		}
	case "stopped":
		// Not a failure: a cancel is usually deliberate. It still gets an alert
		// because a team cannot otherwise tell a cancelled run from a running one.
		env.Severity = "warning"
		env.FailureType = "config_error"
		env.Audience = "user"
		env.Code = codeRunStopped
		env.UserMessage = "The pipeline run was stopped before it finished."
	default:
		// "completed", "running", "streaming_active" and anything else: nothing is
		// wrong, so nothing is sent. A success alert would train the team to ignore
		// the channel the failure alert has to arrive on.
		return terminalNotification{}, false
	}

	// The raw reason goes to internal_message (kept for support) AND to the
	// top-level message, which is the body Slack and email render under the
	// catalog's headline. Without it the alert says a run failed but not why.
	if reason := strings.TrimSpace(errorMessage); reason != "" {
		env.InternalMessage = reason
	}

	message := env.UserMessage
	if env.InternalMessage != "" {
		message = env.InternalMessage
	}

	return terminalNotification{
		Type:       "structured_error_notification",
		PipelineID: pipelineID,
		Message:    message,
		Timestamp:  now.UTC().Format(time.RFC3339),
		ActionURL:  "/pipelines/" + pipelineID,
		Error:      env,
	}, true
}

// emitTerminalRunNotification publishes the alert for a terminal run outcome.
//
// Deliberately returns nothing: every failure path here is logged and swallowed.
// This runs after the terminal DB transaction has already committed, so surfacing
// an error would make Temporal retry an activity whose work is done — re-running the
// UPDATEs and, worse, re-emitting the alert. It takes no context.Context for the
// same reason: the activity context can be cancelled the moment the workflow is torn
// down, which is precisely when the alert matters most.
func emitTerminalRunNotification(pipelineID, executionID, status, errorMessage string, silentDrop bool) {
	if activityCtx == nil || activityCtx.KafkaProducer == nil {
		// Normal in unit tests and in a worker started without a producer; the DB
		// row still records the outcome, so this degrades to "no alert", not to a
		// lost failure.
		log.Debug("terminal run notification skipped: no Kafka producer wired")
		return
	}

	notification, ok := buildTerminalNotification(pipelineID, executionID, status, errorMessage, silentDrop, time.Now())
	if !ok {
		return
	}

	body, err := json.Marshal(notification)
	if err != nil {
		log.Warnf("terminal run notification: marshal failed (ignored): %v", err)
		return
	}

	// Keyed by pipeline id so every alert for one pipeline lands on the same
	// partition and is therefore delivered in order.
	msg := &sarama.ProducerMessage{
		Topic: kafkaclient.Topic(notificationsTopic),
		Key:   sarama.StringEncoder(notification.PipelineID),
		Value: sarama.ByteEncoder(body),
	}
	if _, _, err := activityCtx.KafkaProducer.SendMessage(msg); err != nil {
		log.Warnf("terminal run notification: publish failed (ignored): %v", err)
		return
	}

	log.WithFields(log.Fields{
		"pipeline_id":  notification.PipelineID,
		"execution_id": executionID,
		"status":       status,
		"code":         notification.Error.Code,
	}).Warn("🔔 Published terminal run notification")
}
