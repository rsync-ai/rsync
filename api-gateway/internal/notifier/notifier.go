// Package notifier consumes the rsync.notifications / rsync.healer.actions /
// rsync.healer.results Kafka topics, persists each event into
// pipeline_notifications, and delivers it via the configured channels
// (Slack webhook + email) to the owning user.
//
// This closes G1 / F-Obs-1 — pre-fix the healer + sentinel + executor
// agents emitted notifications into the void because no service
// subscribed. The dead-producer log lines were tracked in the audit;
// this is the consumer.
//
// Scope (pilot-ready):
//   * Persist EVERY event into pipeline_notifications keyed by
//     dedup_key so the UI can render an inbox.
//   * Best-effort external delivery via:
//       - Slack webhook (env NOTIFIER_SLACK_WEBHOOK_URL) — POST
//         minimal payload
//       - SMTP email to the pipeline owner (env SMTP_HOST/SMTP_USER/
//         SMTP_PASSWORD/SMTP_FROM — same vars the explorer share/email
//         flow already uses)
//   * If neither channel is configured, persist-only mode (the UI
//     still surfaces unread).
//
// Out of scope for this PR:
//   * Per-user channel preferences (everyone gets the same channels).
//   * PagerDuty / Opsgenie / webhook.site integrations.
//   * Retry on delivery failure (we mark delivery_status=failed and
//     move on; the row stays in the DB for manual replay).
package notifier

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/smtp"
	"os"
	"strings"
	"time"

	rsynckafka "api-gateway/internal/kafka"
	"api-gateway/internal/slack"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	log "github.com/sirupsen/logrus"
)

const (
	// The canonical spellings. Nothing may hand these to sarama directly --
	// notifierTopics below is the only thing that turns them into wire names.
	notifyTopic   = "rsync.notifications"
	healerActions = "rsync.healer.actions"
	healerResults = "rsync.healer.results"

	// Dedup window: same (pipeline_id, type, action_url) within this
	// many minutes is dropped without re-delivery. Prevents an alert
	// storm when the healer retries the same failure mode N times.
	dedupWindowMinutes = 60
)

// notifierTopics holds the three subscriptions as they appear ON THE WIRE,
// resolved once at wiring time.
//
// Every producer of these topics -- the healer (healer.go:1318, :1383), the CDC
// WAL watchdog (cdc_wal_watchdog.go:369) and healthwatch (watchdog.go:333) --
// publishes through backend-orchestrator's kafka.Manager, and Manager qualifies
// at its Produce/Consume chokepoints (manager.go:321, :396, :920). So the name
// that actually reaches the broker carries KAFKA_TOPIC_PREFIX, including for
// these three, whose literals already spell "rsync." themselves.
//
// At the default prefix that is a no-op -- Topic("rsync.notifications") matches
// its own prefix and passes through unchanged -- which is exactly why
// subscribing to the bare literal worked, and why the defect stayed invisible.
// Set KAFKA_TOPIC_PREFIX=acme., the shape a customer on a shared cluster
// deploys, and the healer writes acme.rsync.notifications while this consumer
// sits on rsync.notifications. Kafka raises nothing for a subscription nobody
// produces to, so every Slack and email alert stops with no error at either end
// -- including the alert that would have reported the outage.
//
// Resolving here rather than at each use also keeps the router in handleMessage
// honest: it compares against the same qualified strings it subscribed to, so a
// fix to the subscription cannot leave the routing switch behind.
type notifierTopics struct {
	notify        string
	healerActions string
	healerResults string
}

func resolveNotifierTopics() notifierTopics {
	return notifierTopics{
		notify:        kafkaclient.Topic(notifyTopic),
		healerActions: kafkaclient.Topic(healerActions),
		healerResults: kafkaclient.Topic(healerResults),
	}
}

// all returns the subscription list in the order the consumer group receives it.
func (t notifierTopics) all() []string {
	return []string{t.notify, t.healerActions, t.healerResults}
}

// Notifier wraps the sarama ConsumerGroup that reads from the
// notification topics.
type Notifier struct {
	db           *sql.DB
	consumer     sarama.ConsumerGroup
	cancel       context.CancelFunc
	done         chan struct{}
	slackWebhook string
	smtpHost     string
	smtpPort     string
	smtpUser     string
	smtpPassword string
	smtpFrom     string
	emailEnabled bool
	slackEnabled bool
	httpClient   *http.Client
	// appBaseURL is the public frontend origin used to turn a persisted
	// relative action_url into an absolute link at delivery time.
	appBaseURL string
	// interactiveApprovals is true when Slack request-signing is configured, so
	// the inbound interactivity receiver is live and drift alerts can carry real
	// Approve/Reject buttons instead of just a "View in rsync-ai" link.
	interactiveApprovals bool
	// topics are the wire names this consumer subscribed to, so the router in
	// handleMessage matches against the same strings.
	topics notifierTopics
}

// Start initializes the notifier and starts consuming. The provided
// ctx (typically appCtx) controls the consumer loop lifetime.
func Start(ctx context.Context, db *sql.DB, kafkaBrokers []string) (*Notifier, error) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_4_0_0
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	// OffsetOldest so a fresh notifier picks up any unread alerts
	// that landed while it was offline. Idempotency-by-dedup_key
	// prevents duplicate inserts.
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Same SASL/TLS and client.id as every other Kafka client in this service.
	// Without it this consumer is silently PLAINTEXT on an authenticated
	// cluster and every Slack/email alert stops — including the ones that would
	// have reported the outage.
	if err := rsynckafka.ApplySarama(cfg, kafkaBrokers); err != nil {
		return nil, fmt.Errorf("notifier: kafka security: %w", err)
	}

	// Namespaced under the same KAFKA_TOPIC_PREFIX as the topics below, so one
	// PREFIXED grant covers both on a customer-managed cluster. This consumer
	// is the one where a silent stall costs the most: an unauthorized group id
	// stops every Slack and email alert, including the alert that would have
	// reported the outage.
	groupID := kafkaclient.Group("api-gateway-notifier")

	consumer, err := sarama.NewConsumerGroup(kafkaBrokers, groupID, cfg)
	if err != nil {
		return nil, fmt.Errorf("notifier: create consumer group: %w", err)
	}

	consumerCtx, cancel := context.WithCancel(ctx)

	// APP_BASE_URL is the public frontend origin used to turn a persisted
	// relative action_url (e.g. /pipelines/{id}/schema-changes) into an
	// absolute link that resolves in Slack/email. Same env var + default the
	// api-gateway auth/invite emails already use, so prod is configured with
	// no new wiring; local/dev falls back to the frontend dev origin.
	appBaseURL := strings.TrimRight(strings.TrimSpace(os.Getenv("APP_BASE_URL")), "/")
	if appBaseURL == "" {
		appBaseURL = "http://localhost:3000"
	}

	// Only emit interactive Approve/Reject buttons when Slack request-signing is
	// configured — otherwise the inbound receiver can't verify clicks and the
	// buttons would be dead, so we keep the plain link button.
	interactiveApprovals := strings.TrimSpace(os.Getenv("SLACK_SIGNING_SECRET")) != ""

	n := &Notifier{
		db:                   db,
		consumer:             consumer,
		cancel:               cancel,
		done:                 make(chan struct{}),
		slackWebhook:         strings.TrimSpace(os.Getenv("NOTIFIER_SLACK_WEBHOOK_URL")),
		smtpHost:             strings.TrimSpace(os.Getenv("SMTP_HOST")),
		smtpPort:             strings.TrimSpace(os.Getenv("SMTP_PORT")),
		smtpUser:             strings.TrimSpace(os.Getenv("SMTP_USER")),
		smtpPassword:         strings.TrimSpace(os.Getenv("SMTP_PASSWORD")),
		smtpFrom:             strings.TrimSpace(os.Getenv("SMTP_FROM")),
		httpClient:           &http.Client{Timeout: 10 * time.Second},
		appBaseURL:           appBaseURL,
		interactiveApprovals: interactiveApprovals,
		topics:               resolveNotifierTopics(),
	}
	n.slackEnabled = n.slackWebhook != ""
	n.emailEnabled = n.smtpHost != "" && n.smtpFrom != ""

	log.WithFields(log.Fields{
		"slack_enabled":         n.slackEnabled,
		"email_enabled":         n.emailEnabled,
		"app_base_url":          n.appBaseURL,
		"interactive_approvals": n.interactiveApprovals,
		"topics":                strings.Join(n.topics.all(), ","),
		"consumer_group":        groupID,
	}).Info("🔔 Starting notifier consumer")

	go n.run(consumerCtx)
	return n, nil
}

// Stop gracefully shuts down the notifier consumer.
func (n *Notifier) Stop() {
	if n == nil {
		return
	}
	n.cancel()
	select {
	case <-n.done:
	case <-time.After(5 * time.Second):
		log.Warn("notifier: timed out waiting for consumer to stop")
	}
	if err := n.consumer.Close(); err != nil {
		log.WithError(err).Warn("notifier: error closing consumer")
	}
}

func (n *Notifier) run(ctx context.Context) {
	defer close(n.done)
	topics := n.topics.all()
	for {
		if err := n.consumer.Consume(ctx, topics, n); err != nil {
			if ctx.Err() != nil {
				return
			}
			log.WithError(err).Warn("notifier: consume returned, retrying in 5s")
			time.Sleep(5 * time.Second)
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// Setup, Cleanup, ConsumeClaim implement sarama.ConsumerGroupHandler.
func (n *Notifier) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (n *Notifier) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (n *Notifier) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}
		if err := n.handleMessage(session.Context(), msg.Topic, msg.Value); err != nil {
			// Don't abort the claim on a single bad message — log and
			// keep going so one malformed payload doesn't poison the
			// whole subscription.
			log.WithError(err).WithField("topic", msg.Topic).Warn("notifier: handleMessage error")
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

// notificationPayload mirrors the shape healer.go::notifyUser emits.
// Extra fields are tolerated and land in metadata.
//
// The Error field carries the StructuredError envelope (introduced 2026-05-30).
// When present, it overrides Type/Message with richer fields (code, severity,
// remediation, audience). When absent (legacy notifications), the consumer
// falls back to the top-level Message field.
type notificationPayload struct {
	Type       string          `json:"type"`
	PipelineID string          `json:"pipeline_id"`
	Message    string          `json:"message"`
	Timestamp  string          `json:"timestamp"`
	ActionURL  string          `json:"action_url"`
	Error      json.RawMessage `json:"error,omitempty"`
}

// structuredErrorView is a minimal projection of diagnose.StructuredError
// — kept here so the notifier doesn't have to import the orchestrator's
// diagnose package. The wire format is owned by the contract in
// pkg/diagnose/structured_error.go.
type structuredErrorView struct {
	FailureType     string          `json:"failure_type"`
	Code            string          `json:"code"`
	Severity        string          `json:"severity"`
	Audience        string          `json:"audience"`
	UserMessage     string          `json:"user_message"`
	InternalMessage string          `json:"internal_message,omitempty"`
	Remediation     json.RawMessage `json:"remediation,omitempty"`
	SourceDBType    string          `json:"source_db_type,omitempty"`
	OccurredAt      string          `json:"occurred_at,omitempty"`
	// DedupSubject narrows the dedup window to THIS event instead of its whole code
	// family — see makeDedupKey. Machine-built by the producer, never shown to a user.
	// This projection must carry it: unmarshal-then-remarshal here is what persists
	// metadata.structured_error, so a field missing from this struct is dropped.
	DedupSubject string `json:"dedup_subject,omitempty"`
}

// healingResultPayload mirrors healer.HealingResult (backend-orchestrator/
// internal/agents/healer/healer.go). It is the shape published to
// rsync.healer.results and carries NO type and NO message field — which is why
// the inbox used to show a bare "rsync.healer.results" row with an empty body
// for every schema-change event. normalizeHealingResult turns the useful ones
// into real copy and suppresses the rest.
type healingResultPayload struct {
	PipelineID string `json:"pipeline_id"`
	ChangeType string `json:"change_type"`
	Table      string `json:"table"`
	Status     string `json:"status"`
	Reason     string `json:"reason"`
}

// healerActionPayload mirrors healer.HealerAction (rsync.healer.actions).
// Same problem: no type, no message.
type healerActionPayload struct {
	PipelineID string `json:"pipeline_id"`
	Action     string `json:"action"`
	Reason     string `json:"reason"`
	Details    string `json:"details"`
}

// normalizeHealingResult rewrites a rsync.healer.results event into the common
// notification shape, and reports whether it belongs in a user's inbox at all.
//
// Only status="applied" is kept. The others are deliberately suppressed:
//   - pending_approval already emits its own SCHEMA_DRIFT_DETECTED notification
//     on rsync.notifications, so keeping it here double-posted every drift event
//   - failed already emits its own failure notification
//   - skipped is internal telemetry with nothing for a user to do
//
// Every status is still recorded independently by the healer's
// recordHealingResult, so suppressing here loses no history.
func normalizeHealingResult(raw []byte, p *notificationPayload) (code string, params map[string]string, keep bool) {
	var hr healingResultPayload
	if err := json.Unmarshal(raw, &hr); err != nil {
		return "", nil, false
	}
	if hr.Status != "applied" {
		return "", nil, false
	}

	table := strings.TrimSpace(hr.Table)
	change := strings.TrimSpace(strings.ReplaceAll(hr.ChangeType, "_", " "))
	if change == "" {
		change = "a schema change"
	}
	msg := capitalizeFirst(change)
	if table != "" {
		msg += " on " + table
	}
	msg += " has been applied to your destination."
	if reason := strings.TrimSpace(hr.Reason); reason != "" {
		msg += " " + capitalizeFirst(reason)
	}

	p.Type = "schema_change_applied"
	p.Message = msg
	if strings.TrimSpace(p.ActionURL) == "" {
		p.ActionURL = "/pipelines/" + hr.PipelineID + "/schema-changes"
	}
	return codeSchemaChangeApplied, map[string]string{"table": table}, true
}

// normalizeHealerAction rewrites a rsync.healer.actions event into the common
// shape. The type is intentionally left empty so the copy resolves through
// topicDefaults ("Automatic recovery in progress") rather than a machine-shaped
// action name.
func normalizeHealerAction(raw []byte, p *notificationPayload) bool {
	var ha healerActionPayload
	if err := json.Unmarshal(raw, &ha); err != nil {
		return false
	}
	msg := strings.TrimSpace(ha.Reason)
	if d := strings.TrimSpace(ha.Details); d != "" {
		if msg != "" {
			msg += " "
		}
		msg += d
	}
	p.Type = ""
	p.Message = msg
	if strings.TrimSpace(p.ActionURL) == "" && strings.TrimSpace(ha.PipelineID) != "" {
		p.ActionURL = "/pipelines/" + ha.PipelineID
	}
	return true
}

// prettySourceType turns a stored connector type into something we can drop
// into a sentence ("Reconnect your PostgreSQL account"). Unknown types fall
// back to Title Case rather than being shown raw.
func prettySourceType(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "":
		return ""
	case "postgresql", "postgres":
		return "PostgreSQL"
	case "mysql":
		return "MySQL"
	case "sqlserver", "mssql":
		return "SQL Server"
	case "mongodb", "mongo":
		return "MongoDB"
	case "oracle":
		return "Oracle"
	case "clickhouse":
		return "ClickHouse"
	default:
		return capitalizeFirst(strings.TrimSpace(t))
	}
}

func (n *Notifier) handleMessage(ctx context.Context, topic string, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	var p notificationPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	// The healer's result/action topics carry their own struct shapes with no
	// type and no message. Normalize them into the common payload before any
	// copy resolution, and drop the ones that are pure internal telemetry.
	var (
		normalizedCode string
		extraParams    = map[string]string{}
	)
	switch topic {
	case n.topics.healerResults:
		code, params, keep := normalizeHealingResult(raw, &p)
		if !keep {
			return nil
		}
		normalizedCode = code
		for k, v := range params {
			extraParams[k] = v
		}
	case n.topics.healerActions:
		if !normalizeHealerAction(raw, &p) {
			return nil
		}
	}

	if strings.TrimSpace(p.PipelineID) == "" {
		// No pipeline context — can't route to a user. Drop.
		return nil
	}

	// Look up the pipeline's owner and display name. Notifications without an
	// owner stay in the DB orphaned (queryable by admin) but skip external
	// delivery. The name is what lets copy say "orders-sync ran into a problem"
	// instead of leaving the user to guess which of their pipelines broke.
	var (
		userID       sql.NullString
		pipelineName sql.NullString
	)
	err := n.db.QueryRowContext(ctx,
		`SELECT created_by, COALESCE(name, '') FROM pipelines WHERE id = $1`, p.PipelineID,
	).Scan(&userID, &pipelineName)
	if err != nil {
		return fmt.Errorf("pipeline owner lookup: %w", err)
	}
	if !userID.Valid || strings.TrimSpace(userID.String) == "" {
		return nil
	}
	extraParams["pipeline"] = strings.TrimSpace(pipelineName.String)

	// If the payload carries a StructuredError envelope, prefer its
	// fields over the legacy top-level shape. This delivers the richer
	// UX (severity, code, remediation, audience) introduced 2026-05-30
	// while still tolerating older notifications without the envelope.
	var se *structuredErrorView
	if len(p.Error) > 0 {
		var parsed structuredErrorView
		if err := json.Unmarshal(p.Error, &parsed); err == nil && parsed.Code != "" {
			se = &parsed
			// Backfill top-level fields from the envelope so downstream
			// rendering (email, Slack, dedup key) sees a consistent message.
			if p.Message == "" {
				p.Message = parsed.UserMessage
			}
			if p.Type == "" {
				p.Type = strings.ToLower(parsed.FailureType)
			}
		}
	}

	severity := classifySeverity(p.Type)
	if se != nil && se.Severity != "" {
		severity = se.Severity
	}

	// The stable code drives both the user-facing copy and dedup.
	code := normalizedCode
	if se != nil && se.Code != "" {
		code = se.Code
	}
	if se != nil && se.SourceDBType != "" {
		extraParams["source"] = prettySourceType(se.SourceDBType)
	}

	// Everything the user reads is resolved here, through the catalog in
	// catalog.go. A raw error code, event type or Kafka topic name must never
	// reach a headline — pre-fix the bell showed "LEGACY_UNCLASSIFIED" and
	// "rsync.healer.results" because it rendered exactly those.
	rendered := resolve(code, p.Type, topic, severity, extraParams)
	severity = rendered.Severity

	// Dedup on the code when we have one — different codes for the same
	// pipeline should fire separately even with the same action_url — and on
	// the raw type otherwise.
	dedupIdentity := code
	if dedupIdentity == "" {
		dedupIdentity = p.Type
	}
	// ...and on the producer's per-event subject when it supplied one, so two
	// DIFFERENT drifts on the same pipeline are two notifications rather than one.
	dedupSubject := ""
	if se != nil {
		dedupSubject = strings.TrimSpace(se.DedupSubject)
	}
	dedupKey := makeDedupKey(p.PipelineID, dedupIdentity, p.ActionURL, dedupSubject)

	// Dedup: skip if an identical event landed within the dedup window.
	var existingID sql.NullString
	err = n.db.QueryRowContext(ctx, `
		SELECT id::text FROM pipeline_notifications
		WHERE dedup_key = $1 AND created_at > NOW() - INTERVAL '`+fmt.Sprintf("%d minutes", dedupWindowMinutes)+`'
		ORDER BY created_at DESC LIMIT 1
	`, dedupKey).Scan(&existingID)
	if err == nil && existingID.Valid {
		log.WithFields(log.Fields{
			"dedup_key": dedupKey,
			"existing":  existingID.String,
		}).Debug("notifier: dedup hit, skipping")
		return nil
	}
	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("dedup lookup: %w", err)
	}

	metaMap := map[string]interface{}{
		"source_topic": topic,
		"raw_type":     p.Type,
		"raw_ts":       p.Timestamp,
		// User-facing extras resolved from the catalog. They live in metadata
		// rather than dedicated columns so adding copy to catalog.go needs no
		// migration. The API projects them onto the notification row.
		"impact":       rendered.Impact,
		"action_label": rendered.ActionLabel,
	}
	if pn := strings.TrimSpace(pipelineName.String); pn != "" {
		metaMap["pipeline_name"] = pn
	}
	if code != "" {
		// Kept for support ("quote this code"), never shown as the headline.
		metaMap["error_code"] = code
	}
	// Embed the full structured error envelope into metadata so the
	// frontend can render code, remediation steps, copy-pasteable SQL,
	// doc_url, etc. Persisting it here means the UI doesn't need a
	// separate "fetch error details" round-trip.
	if se != nil {
		metaMap["structured_error"] = se
		metaMap["error_code"] = se.Code
		metaMap["failure_type"] = se.FailureType
		metaMap["audience"] = se.Audience
		if se.SourceDBType != "" {
			metaMap["source_db_type"] = se.SourceDBType
		}
	}
	metaBytes, _ := json.Marshal(metaMap)

	notifID := uuid.New().String()
	_, err = n.db.ExecContext(ctx, `
		INSERT INTO pipeline_notifications
			(id, pipeline_id, user_id, type, severity, title, message, action_url, metadata, delivery_status, dedup_key, created_at)
		VALUES
			($1, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, 'pending', $10, NOW())
	`, notifID, p.PipelineID, userID.String, p.Type, severity, rendered.Title, p.Message, p.ActionURL, metaBytes, dedupKey)
	if err != nil {
		return fmt.Errorf("insert notification: %w", err)
	}

	// A drift notification carries interactive Approve/Reject buttons only when
	// it maps to a specific pipeline AND the structured error is schema drift —
	// that's the one event the inbound receiver knows how to action.
	actionable := se != nil && se.Code == "SCHEMA_DRIFT_DETECTED" && strings.TrimSpace(p.PipelineID) != ""

	// External delivery is best-effort.
	delivered, deliveryErr := n.deliver(ctx, userID.String, p, rendered, actionable)
	status := "delivered"
	var errStr sql.NullString
	if !delivered {
		status = "failed"
		if deliveryErr != nil {
			errStr = sql.NullString{String: deliveryErr.Error(), Valid: true}
		}
	}
	if !n.slackEnabled && !n.emailEnabled {
		status = "suppressed" // no channel configured; persist-only mode
	}
	_, _ = n.db.ExecContext(ctx, `
		UPDATE pipeline_notifications
		SET delivery_status = $1, delivered_at = CASE WHEN $1 = 'delivered' THEN NOW() ELSE delivered_at END,
		    delivery_error = $2
		WHERE id = $3::uuid
	`, status, errStr, notifID)

	log.WithFields(log.Fields{
		"notification_id": notifID,
		"pipeline_id":     p.PipelineID,
		"type":            p.Type,
		"severity":        severity,
		"delivery_status": status,
	}).Info("🔔 Notification persisted")

	return nil
}

func (n *Notifier) deliver(ctx context.Context, userID string, p notificationPayload, r Rendered, actionable bool) (bool, error) {
	if !n.slackEnabled && !n.emailEnabled {
		return false, nil
	}
	anyDelivered := false
	var lastErr error
	if n.slackEnabled {
		if err := n.sendSlack(ctx, p, r, actionable); err != nil {
			lastErr = err
		} else {
			anyDelivered = true
		}
	}
	if n.emailEnabled {
		if err := n.sendEmail(ctx, userID, p, r); err != nil {
			lastErr = err
		} else {
			anyDelivered = true
		}
	}
	return anyDelivered, lastErr
}

// bodyText joins the event's detail message with the catalog's impact line —
// the "is my data still moving" sentence. Used verbatim by Slack and email so
// external delivery reads exactly like the in-app bell.
func bodyText(message, impact string) string {
	message = strings.TrimSpace(message)
	impact = strings.TrimSpace(impact)
	switch {
	case message == "":
		return impact
	case impact == "":
		return message
	default:
		return message + "\n" + impact
	}
}

// slackPayload builds the Slack message body. For an actionable schema-drift
// alert with interactivity configured it emits Block Kit with real Approve /
// Reject buttons (action_id + value=pipeline_id) that POST back to the inbound
// receiver; the value carries the pipeline id, which the receiver resolves to
// the pending change at click time. Otherwise it emits the legacy attachment
// with a plain "View in rsync-ai" link — unchanged behavior for every other
// notification. Pure (no I/O) so it's unit-testable.
func (n *Notifier) slackPayload(p notificationPayload, r Rendered, actionable bool) map[string]interface{} {
	viewURL := absoluteActionURL(n.appBaseURL, p.ActionURL)
	title := r.Title
	body := bodyText(p.Message, r.Impact)

	if n.interactiveApprovals && actionable {
		return map[string]interface{}{
			"text": fmt.Sprintf("%s: %s", title, body), // notification fallback
			"blocks": []map[string]interface{}{
				{
					"type": "section",
					"text": map[string]interface{}{"type": "mrkdwn", "text": fmt.Sprintf("*%s*\n%s", title, body)},
				},
				{
					"type":   "section",
					"fields": []map[string]interface{}{{"type": "mrkdwn", "text": "*Pipeline*\n" + p.PipelineID}},
				},
				{
					"type": "actions",
					"elements": []map[string]interface{}{
						{
							"type":      "button",
							"style":     "primary",
							"text":      map[string]interface{}{"type": "plain_text", "text": "Approve"},
							"action_id": slack.ActionApproveSchemaChange,
							"value":     p.PipelineID,
						},
						{
							"type":      "button",
							"style":     "danger",
							"text":      map[string]interface{}{"type": "plain_text", "text": "Reject"},
							"action_id": slack.ActionRejectSchemaChange,
							"value":     p.PipelineID,
						},
						{
							"type": "button",
							"text": map[string]interface{}{"type": "plain_text", "text": "View in rsync-ai"},
							"url":  viewURL,
						},
					},
				},
			},
		}
	}

	pipelineLabel := r.PipelineName
	if pipelineLabel == "" {
		pipelineLabel = p.PipelineID
	}

	return map[string]interface{}{
		"text": fmt.Sprintf("*%s*\n%s", title, body),
		"attachments": []map[string]interface{}{
			{
				"color": slackColorFor(r.Severity),
				"fields": []map[string]interface{}{
					{"title": "Pipeline", "value": pipelineLabel, "short": true},
				},
				"actions": []map[string]interface{}{
					{
						"type": "button",
						"text": r.ActionLabel,
						"url":  viewURL,
					},
				},
			},
		},
	}
}

func (n *Notifier) sendSlack(ctx context.Context, p notificationPayload, r Rendered, actionable bool) error {
	payload := n.slackPayload(p, r, actionable)
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, "POST", n.slackWebhook, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return fmt.Errorf("slack webhook returned HTTP %d", resp.StatusCode)
}

func (n *Notifier) sendEmail(ctx context.Context, userID string, p notificationPayload, r Rendered) error {
	// Look up the user's email — every notification goes to the
	// pipeline owner. Per-user channel prefs are out of scope for
	// the pilot.
	var email sql.NullString
	err := n.db.QueryRowContext(ctx,
		`SELECT email FROM users WHERE id = $1`, userID,
	).Scan(&email)
	if err != nil {
		return fmt.Errorf("user email lookup: %w", err)
	}
	if !email.Valid || strings.TrimSpace(email.String) == "" {
		return fmt.Errorf("user has no email")
	}

	port := n.smtpPort
	if port == "" {
		port = "587"
	}
	addr := n.smtpHost + ":" + port

	var auth smtp.Auth
	if n.smtpUser != "" && n.smtpPassword != "" {
		auth = smtp.PlainAuth("", n.smtpUser, n.smtpPassword, n.smtpHost)
	}

	pipelineLabel := r.PipelineName
	if pipelineLabel == "" {
		pipelineLabel = p.PipelineID
	}

	subject := fmt.Sprintf("[rsync-ai %s] %s", strings.ToUpper(r.Severity), r.Title)
	body := fmt.Sprintf(
		"%s\r\n\r\nPipeline: %s\r\n%s: %s\r\n\r\n--\r\nAutomated message from rsync-ai notifier",
		strings.ReplaceAll(bodyText(p.Message, r.Impact), "\n", "\r\n"),
		pipelineLabel,
		r.ActionLabel,
		absoluteActionURL(n.appBaseURL, p.ActionURL),
	)
	msg := []byte(strings.Join([]string{
		"From: " + n.smtpFrom,
		"To: " + email.String,
		"Subject: " + subject,
		"Content-Type: text/plain; charset=utf-8",
		"",
		body,
	}, "\r\n"))

	return smtp.SendMail(addr, auth, n.smtpFrom, []string{email.String}, msg)
}

// classifySeverity maps event types to {info, warning, critical}.
// Conservative: anything we don't recognize is "info" so it appears
// in the inbox but doesn't page anyone.
func classifySeverity(eventType string) string {
	t := strings.ToLower(eventType)
	switch {
	case strings.Contains(t, "failed"),
		strings.Contains(t, "exhausted"),
		strings.Contains(t, "poison"),
		strings.Contains(t, "fatal"):
		return "critical"
	case strings.Contains(t, "schema_change"),
		strings.Contains(t, "drift"),
		strings.Contains(t, "approval"),
		strings.Contains(t, "retry"),
		strings.Contains(t, "warning"):
		return "warning"
	}
	return "info"
}

// absoluteActionURL turns a persisted (relative) action_url into an absolute
// link for external delivery (Slack button, email body). The DB keeps the
// relative path host-agnostic; we prefix the public app base URL only at
// delivery time. Already-absolute URLs pass through unchanged, and an empty
// path falls back to the base so a Slack button never carries an empty URL.
func absoluteActionURL(base, actionURL string) string {
	u := strings.TrimSpace(actionURL)
	if u == "" {
		return base
	}
	if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		return u
	}
	if !strings.HasPrefix(u, "/") {
		u = "/" + u
	}
	return base + u
}

// makeDedupKey builds the idempotency key for the pre-insert window check.
//
// `subject` is what makes the key mean "this event" rather than "any event of this
// class". Without it, (pipeline, code, action_url) is CONSTANT for a whole family:
// every schema drift on a pipeline resolves to code SCHEMA_DRIFT_DETECTED and the same
// /pipelines/{id}/schema-changes deep link, so the first drift in an hour was notified
// and every later one — a different table, a different column, a DROP after an ADD —
// was silently swallowed. The approval row was still filed, so the drift was visible if
// you went looking; the alert that tells you to go looking was not sent.
//
// Producers set it (diagnose.StructuredError.DedupSubject) to a STABLE machine-built
// identity for the specific thing the event is about — never rendered copy, which for
// LLM-authored messages varies between retries of the same event and would turn dedup
// off entirely. Empty subject = pre-existing behavior, so no producer is forced to care.
func makeDedupKey(pipelineID, eventType, actionURL, subject string) string {
	h := sha1.Sum([]byte(pipelineID + "|" + eventType + "|" + actionURL + "|" + subject))
	return hex.EncodeToString(h[:])
}

func slackColorFor(severity string) string {
	switch severity {
	case "critical":
		return "danger"
	case "warning":
		return "warning"
	}
	return "good"
}
