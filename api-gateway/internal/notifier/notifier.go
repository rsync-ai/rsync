// Package notifier consumes the rsync.notifications Kafka topic (plus
// rsync.healer.results when RSYNC_SCHEMA_DRIFT_ENABLED=true), persists each
// event into pipeline_notifications, and delivers it via the configured
// channels (Slack webhook + email) to the owning user.
//
// This closes G1 / F-Obs-1 — pre-fix the healer + sentinel + executor
// agents emitted notifications into the void because no service
// subscribed. The dead-producer log lines were tracked in the audit;
// this is the consumer.
//
// Scope (pilot-ready):
//   - Persist EVERY event into pipeline_notifications keyed by
//     dedup_key so the UI can render an inbox.
//   - Best-effort external delivery via:
//   - one instance-wide Slack incoming webhook
//   - SMTP email to the pipeline owner
//     configured by an admin in the UI (notification_channel_settings,
//     migration 102), falling back to the NOTIFIER_SLACK_WEBHOOK_URL /
//     SMTP_* env vars when no admin has saved channels. See channels.go.
//   - Every alert has a category (categories.go). The admin mutes
//     categories for Slack; each owner mutes categories, or all email,
//     for themselves. A muted alert is still persisted, with
//     delivery_status = 'skipped'.
//   - If neither channel is configured, persist-only mode (the UI
//     still surfaces unread).
//
// Out of scope:
//   - Per-user Slack destinations (Slack is one instance channel).
//   - PagerDuty / Opsgenie / webhook.site integrations.
//   - Retry on delivery failure (we mark delivery_status=failed and
//     move on; the row stays in the DB for manual replay).
package notifier

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"api-gateway/internal/config"
	rsynckafka "api-gateway/internal/kafka"
	"api-gateway/internal/safehttp"
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
	healerResults = "rsync.healer.results"

	// Dedup window: same (pipeline_id, type, action_url) within this
	// many minutes is dropped without re-delivery. Prevents an alert
	// storm when the healer retries the same failure mode N times.
	dedupWindowMinutes = 60
)

// notifierTopics holds the subscriptions as they appear ON THE WIRE, resolved
// once at wiring time.
//
// healerResults is set only when RSYNC_SCHEMA_DRIFT_ENABLED=true
// (config.SchemaDriftEnabled). rsync.healer.results is one of the three
// schema-drift topics the orchestrator creates and produces only with that flag
// on, and a sarama consumer group auto-creates what it subscribes to, so
// subscribing unconditionally would re-create the topic on every installation
// that runs with drift off. With the flag off the field is empty, all() leaves
// it out, and handleMessage never routes to it.
//
// Every producer of these topics -- the healer (notifyUser and its
// healing-result publish), the CDC WAL watchdog and healthwatch -- publishes
// through backend-orchestrator's kafka.Manager, and Manager qualifies at its
// Produce/Consume chokepoints. So the name that actually reaches the broker
// carries KAFKA_TOPIC_PREFIX, including for these, whose literals already spell
// "rsync." themselves.
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
	healerResults string // "" unless schema drift is enabled
}

func resolveNotifierTopics() notifierTopics {
	t := notifierTopics{notify: kafkaclient.Topic(notifyTopic)}
	if config.SchemaDriftEnabled() {
		t.healerResults = kafkaclient.Topic(healerResults)
	}
	return t
}

// all returns the subscription list in the order the consumer group receives it.
func (t notifierTopics) all() []string {
	out := []string{t.notify}
	if t.healerResults != "" {
		out = append(out, t.healerResults)
	}
	return out
}

// Notifier wraps the sarama ConsumerGroup that reads from the
// notification topics.
type Notifier struct {
	db       *sql.DB
	consumer sarama.ConsumerGroup
	cancel   context.CancelFunc
	done     chan struct{}
	// httpClient posts Slack webhooks. SSRF-guarded (safehttp), because an
	// admin-supplied URL is otherwise a request to anywhere this pod can reach.
	httpClient *http.Client
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

	appBaseURL := appBaseURLFromEnv()

	// Only emit interactive Approve/Reject buttons when Slack request-signing is
	// configured — otherwise the inbound receiver can't verify clicks and the
	// buttons would be dead, so we keep the plain link button.
	interactiveApprovals := strings.TrimSpace(os.Getenv("SLACK_SIGNING_SECRET")) != ""

	n := &Notifier{
		db:                   db,
		consumer:             consumer,
		cancel:               cancel,
		done:                 make(chan struct{}),
		httpClient:           safehttp.NewClient(slackTimeout),
		appBaseURL:           appBaseURL,
		interactiveApprovals: interactiveApprovals,
		topics:               resolveNotifierTopics(),
	}

	// Logged once for the operator. Delivery re-reads the settings (cached
	// channelCacheTTL), so an admin's change applies without a restart.
	startupCtx, startupCancel := context.WithTimeout(ctx, 5*time.Second)
	channels, channelsErr := LoadChannelConfig(startupCtx, db)
	startupCancel()
	if channelsErr != nil {
		log.WithError(channelsErr).Warn("notifier: could not read notification channel settings at startup; will retry on each alert")
	}

	log.WithFields(log.Fields{
		"channel_source":        channels.Source,
		"slack_enabled":         channels.Slack.Enabled,
		"email_enabled":         channels.Email.Enabled,
		"app_base_url":          n.appBaseURL,
		"interactive_approvals": n.interactiveApprovals,
		"topics":                strings.Join(n.topics.all(), ","),
		"schema_drift_enabled":  n.topics.healerResults != "",
		"consumer_group":        groupID,
	}).Info("🔔 Starting notifier consumer")

	go n.run(consumerCtx)
	return n, nil
}

// NewEmitter builds a Notifier that consumes nothing: Publish hands it an event
// in-process instead of over Kafka. The gateway uses it for events it raises
// itself — a new pre-migration assessment issue — so they get the same
// persist → dedup → Slack/email path, and the same catalog copy, as the
// orchestrator's alerts, without a producer round-trip through the broker.
func NewEmitter(db *sql.DB) *Notifier {
	return &Notifier{
		db:                   db,
		httpClient:           safehttp.NewClient(slackTimeout),
		appBaseURL:           appBaseURLFromEnv(),
		interactiveApprovals: strings.TrimSpace(os.Getenv("SLACK_SIGNING_SECRET")) != "",
		topics:               resolveNotifierTopics(),
	}
}

// Publish delivers one rsync.notifications-shaped event as though it had
// arrived on the notification topic.
func (n *Notifier) Publish(ctx context.Context, raw []byte) error {
	return n.handleMessage(ctx, n.topics.notify, raw)
}

// appBaseURLFromEnv is the public frontend origin used to turn a persisted
// relative action_url (e.g. /pipelines/{id}/schema-changes) into an absolute
// link that resolves in Slack/email. Same env var + default the api-gateway
// auth/invite emails already use, so prod is configured with no new wiring;
// local/dev falls back to the frontend dev origin.
func appBaseURLFromEnv() string {
	if v := strings.TrimRight(strings.TrimSpace(os.Getenv("APP_BASE_URL")), "/"); v != "" {
		return v
	}
	return "http://localhost:3000"
}

// Stop gracefully shuts down the notifier consumer.
func (n *Notifier) Stop() {
	if n == nil || n.cancel == nil {
		return // nil, or an emitter (NewEmitter) that never consumed
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

// permanentError marks a failure that re-reading the same message can never
// fix: a payload that will not parse, or one naming a pipeline that is gone.
// Those are dropped, offset committed, so one bad message cannot wedge a topic.
//
// Everything else is retryable BY DEFAULT — including error classes nobody
// anticipated. That default is the point: see ConsumeClaim.
type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// permanent wraps a formatted error as unprocessable.
func permanent(format string, args ...interface{}) error {
	return permanentError{err: fmt.Errorf(format, args...)}
}

func isPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

// How long one message is retried in place before the claim is given up on.
// Four attempts two seconds apart rides out a database restart without forcing
// a consumer-group rebalance.
const handleAttempts = 4

// var, not const, so tests can exercise the retry loop without sleeping.
var handleRetryDelay = 2 * time.Second

func (n *Notifier) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if msg == nil {
			continue
		}
		err := n.handleWithRetry(session.Context(), msg)
		if err != nil && !isPermanent(err) {
			// Retries are spent and the failure is still one that re-processing
			// could fix — the database is down, most likely. Return WITHOUT
			// marking the offset: the session ends and the broker redelivers
			// from the last commit, which is what makes this at-least-once.
			//
			// Committing here instead, as this loop used to do for every error
			// alike, destroyed every alert that arrived during an outage.
			// Nothing ever re-reads a Kafka message whose offset has moved on,
			// so those alerts were not delayed, they were gone — and the only
			// trace was one Warn line.
			//
			// The cost of the other choice is that a failure misclassified as
			// retryable stalls this subscription. That is the deliberate trade:
			// a stalled notifier is loud (this log, plus consumer lag that
			// keeps growing), a silently dropped alert is not.
			log.WithError(err).WithFields(log.Fields{
				"topic":     msg.Topic,
				"partition": msg.Partition,
				"offset":    msg.Offset,
			}).Error("notifier: giving up after retries; aborting claim so the message is redelivered")
			return err
		}
		if err != nil {
			// Permanent. Drop it and keep going so one malformed payload
			// doesn't poison the whole subscription.
			log.WithError(err).WithFields(log.Fields{
				"topic":     msg.Topic,
				"partition": msg.Partition,
				"offset":    msg.Offset,
			}).Warn("notifier: dropping unprocessable message")
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

// handleWithRetry runs handleMessage, retrying in place while the failure looks
// retryable. Returns nil once it succeeds, or the final error.
func (n *Notifier) handleWithRetry(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var err error
	for attempt := 1; attempt <= handleAttempts; attempt++ {
		if err = n.handleMessage(ctx, msg.Topic, msg.Value); err == nil {
			return nil
		}
		if isPermanent(err) || attempt == handleAttempts {
			return err
		}
		log.WithError(err).WithFields(log.Fields{
			"topic":   msg.Topic,
			"offset":  msg.Offset,
			"attempt": attempt,
		}).Warn("notifier: retryable failure handling message, retrying")
		select {
		case <-ctx.Done():
			return err
		case <-time.After(handleRetryDelay):
		}
	}
	return err
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
		// Bytes that are not JSON will not become JSON on a redelivery.
		return permanent("unmarshal: %v", err)
	}

	// The healer's result topic carries its own struct shape with no type and no
	// message. Normalize it into the common payload before any copy resolution,
	// and drop the events that are pure internal telemetry. healerResults is ""
	// when schema drift is off, and nothing is subscribed to it then.
	var (
		normalizedCode string
		extraParams    = map[string]string{}
	)
	if n.topics.healerResults != "" && topic == n.topics.healerResults {
		code, params, keep := normalizeHealingResult(raw, &p)
		if !keep {
			return nil
		}
		normalizedCode = code
		for k, v := range params {
			extraParams[k] = v
		}
	}

	// Who is this alert for? Two shapes arrive on these topics:
	//
	//   - a PIPELINE alert, which routes to that pipeline's owner; and
	//   - an INSTANCE alert, which is about no pipeline and belongs to whoever
	//     operates the instance.
	//
	// The second used to have nowhere to go. healthwatch/watchdog.go sends its
	// connector-version-regression alert with the synthetic pipeline_id
	// "system"; that literal reached `WHERE id = $1` against a uuid column,
	// Postgres rejected it as malformed input, and the error path below logged
	// and dropped it — every time, for the entire life of that agent. Treating
	// it as an instance alert is what makes that producer reach anyone.
	pipelineID := strings.TrimSpace(p.PipelineID)
	var (
		recipients   []string
		pipelineName string
	)
	if pipelineID == "" || isInstanceScope(pipelineID) {
		pipelineID = ""
		admins, err := n.instanceRecipients(ctx)
		if err != nil {
			return fmt.Errorf("instance recipient lookup: %w", err)
		}
		if len(admins) == 0 {
			// Nobody to tell, and user_id is NOT NULL so there is not even a
			// row to leave behind for later. Logged at Error because an
			// instance with no active admin cannot receive ops alerts at all —
			// that is a deployment problem, not a quiet edge case.
			log.WithFields(log.Fields{
				"topic": topic,
				"type":  p.Type,
			}).Error("notifier: instance alert has no active admin to deliver to; dropping")
			return nil
		}
		recipients = admins
		// Keeps uncoded fallback copy ("{pipeline} needs attention") readable
		// for an alert that is about no pipeline at all.
		extraParams["pipeline"] = instanceScopeLabel
	} else {
		if _, perr := uuid.Parse(pipelineID); perr != nil {
			// Neither a uuid nor the instance sentinel. Re-reading the same
			// bytes will not turn it into one.
			return permanent("pipeline_id %q is neither a uuid nor an instance scope", pipelineID)
		}
		// Look up the pipeline's owner and display name. The name is what lets
		// copy say "orders-sync ran into a problem" instead of leaving the user
		// to guess which of their pipelines broke.
		var owner, name sql.NullString
		err := n.db.QueryRowContext(ctx,
			`SELECT created_by, COALESCE(name, '') FROM pipelines WHERE id = $1`, pipelineID,
		).Scan(&owner, &name)
		if errors.Is(err, sql.ErrNoRows) {
			// The pipeline was deleted. There will never be an owner to route
			// to, so retrying this forever would wedge the topic for nothing.
			return permanent("pipeline %s no longer exists", pipelineID)
		}
		if err != nil {
			return fmt.Errorf("pipeline owner lookup: %w", err)
		}
		if !owner.Valid || strings.TrimSpace(owner.String) == "" {
			return nil
		}
		recipients = []string{strings.TrimSpace(owner.String)}
		pipelineName = strings.TrimSpace(name.String)
		extraParams["pipeline"] = pipelineName
	}

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
	dedupKey := makeDedupKey(pipelineID, dedupIdentity, p.ActionURL, dedupSubject)

	// Dedup: skip if an identical event landed within the dedup window.
	var existingID sql.NullString
	err := n.db.QueryRowContext(ctx, `
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
	if pipelineName != "" {
		metaMap["pipeline_name"] = pipelineName
	}
	if code != "" {
		// Kept for support ("quote this code"), never shown as the headline.
		metaMap["error_code"] = code
	}
	// The category decides who is told (see planDelivery). Persisted so a
	// 'skipped' row can say which mute skipped it.
	category := CategoryFor(code, p.Type)
	metaMap["category"] = category
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

	// NULL pipeline_id is how an instance alert says "about no pipeline"
	// (migration 112). Casting an empty string to uuid would error instead.
	pipelineArg := sql.NullString{String: pipelineID, Valid: pipelineID != ""}

	// One row per recipient — the bell is per-user, so an instance alert that
	// landed in only one admin's inbox is invisible to every other admin.
	//
	// All rows in ONE transaction: a fan-out that got halfway and then failed
	// would be papered over on redelivery by the dedup check above, leaving the
	// remaining admins permanently without the alert.
	tx, err := n.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin notification insert: %w", err)
	}
	notifIDs := make([]string, len(recipients))
	for i, recipient := range recipients {
		notifIDs[i] = uuid.New().String()
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO pipeline_notifications
				(id, pipeline_id, user_id, type, severity, title, message, action_url, metadata, delivery_status, dedup_key, created_at)
			VALUES
				($1, $2::uuid, $3::uuid, $4, $5, $6, $7, $8, $9, 'pending', $10, NOW())
		`, notifIDs[i], pipelineArg, recipient, p.Type, severity, rendered.Title, p.Message, p.ActionURL, metaBytes, dedupKey); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert notification: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit notification insert: %w", err)
	}

	// A drift notification carries interactive Approve/Reject buttons only when
	// it maps to a specific pipeline AND the structured error is schema drift —
	// that's the one event the inbound receiver knows how to action.
	actionable := se != nil && se.Code == "SCHEMA_DRIFT_DETECTED" && pipelineID != ""

	for i, recipient := range recipients {
		// Slack is a single instance-wide webhook and the alert list is a
		// single instance-wide list, so they carry the alert exactly once
		// however many people hold a row for it. Everyone still gets their own
		// email, subject to their own mutes.
		status, deliveryErr := n.deliver(ctx, recipient, p, rendered, actionable, category, i == 0)
		var errStr sql.NullString
		if deliveryErr != nil {
			// Recorded even when another channel delivered, so a half-broken
			// setup is visible on the row instead of hiding behind 'delivered'.
			errStr = sql.NullString{String: deliveryErr.Error(), Valid: true}
		}
		_, _ = n.db.ExecContext(ctx, `
			UPDATE pipeline_notifications
			SET delivery_status = $1, delivered_at = CASE WHEN $1 = 'delivered' THEN NOW() ELSE delivered_at END,
			    delivery_error = $2
			WHERE id = $3::uuid
		`, status, errStr, notifIDs[i])

		log.WithFields(log.Fields{
			"notification_id": notifIDs[i],
			"pipeline_id":     pipelineID,
			"instance_alert":  pipelineID == "",
			"type":            p.Type,
			"severity":        severity,
			"category":        category,
			"delivery_status": status,
		}).Info("🔔 Notification persisted")
	}

	return nil
}

// instanceScopeLabel stands in for a pipeline name in copy that expects one,
// for an alert that is about the instance rather than any pipeline.
const instanceScopeLabel = "This instance"

// instanceScopeIDs are the synthetic pipeline ids producers use to say "this is
// not about a pipeline". They are matched case-insensitively and must never be
// parsed as uuids.
var instanceScopeIDs = map[string]bool{
	"system":   true, // healthwatch/watchdog.go
	"instance": true,
	"global":   true,
}

func isInstanceScope(pipelineID string) bool {
	return instanceScopeIDs[strings.ToLower(strings.TrimSpace(pipelineID))]
}

// instanceRecipients returns the user ids that receive instance-level alerts:
// every active admin. Admin here is the instance role (users.role), not a
// workspace role — an instance alert is not scoped to a workspace, and the
// workspace admin of one workspace has no standing to be told about the host.
//
// Ordered by created_at so the fan-out is deterministic, which also makes the
// "instance channels go to the first recipient only" rule in handleMessage
// stable rather than whatever order Postgres felt like returning.
func (n *Notifier) instanceRecipients(ctx context.Context) ([]string, error) {
	rows, err := n.db.QueryContext(ctx, `
		SELECT id::text
		FROM users
		WHERE role = 'admin' AND COALESCE(status, 'active') = 'active'
		ORDER BY created_at, id
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		if id = strings.TrimSpace(id); id != "" {
			ids = append(ids, id)
		}
	}
	return ids, rows.Err()
}

// deliver sends one alert through every channel its category is not muted on,
// and returns the delivery_status to record plus any send error.
// instanceChannels is false for every recipient after the first of a fanned-out
// instance alert: the Slack webhook and the admin alert list are instance-wide,
// so sending them once per recipient would post the same alert N times.
func (n *Notifier) deliver(ctx context.Context, userID string, p notificationPayload, r Rendered, actionable bool, category string, instanceChannels bool) (string, error) {
	cfg, err := CachedChannelConfig(ctx, n.db)
	if err != nil {
		return StatusFailed, err
	}

	prefs := DefaultEmailPreferences()
	if cfg.Email.Enabled {
		saved, err := ReadEmailPreferences(ctx, n.db, userID)
		if err != nil {
			// Sending an alert the owner muted is recoverable; dropping one they
			// wanted because a preference read failed is not.
			log.WithError(err).WithField("user_id", userID).Warn("notifier: could not read email preferences; sending with defaults")
		} else {
			prefs = saved
		}
	}

	plan := planDelivery(cfg, category, prefs)
	if !instanceChannels {
		plan.slack = false
		plan.listEmail = nil
		if !plan.ownerEmail {
			// Everything that would have carried this copy is either muted or
			// already sent to on another recipient's behalf.
			return StatusSkipped, nil
		}
		plan.status = ""
	}
	if plan.status != "" {
		return plan.status, nil
	}

	delivered := false
	var errs []string
	if plan.slack {
		if err := sendSlackWebhook(ctx, n.httpClient, cfg.Slack, n.slackPayload(p, r, actionable)); err != nil {
			errs = append(errs, "slack: "+err.Error())
		} else {
			delivered = true
		}
	}
	ownerAddress := ""
	if plan.ownerEmail {
		addr, err := n.ownerEmailAddress(ctx, userID)
		if err == nil {
			err = n.sendEmail(ctx, cfg.Email, addr, p, r, fmt.Sprintf(
				"You can choose which alerts you receive by email in your rsync-ai settings: %s",
				absoluteActionURL(n.appBaseURL, "/settings")))
		}
		if err != nil {
			errs = append(errs, "email: "+err.Error())
		} else {
			delivered = true
			ownerAddress = addr
		}
	}
	for _, to := range plan.listEmail {
		// One copy per person: an owner who already got theirs is skipped. An
		// owner whose own copy failed still gets the list copy.
		if ownerAddress != "" && strings.EqualFold(to, ownerAddress) {
			continue
		}
		// Each address is its own send, so one bad address neither blocks the
		// others nor reveals the list in a shared To: header.
		if err := n.sendEmail(ctx, cfg.Email, to, p, r, fmt.Sprintf(
			"You receive this because an rsync-ai admin added this address to the instance alert list. Admins manage the list at: %s",
			absoluteActionURL(n.appBaseURL, "/admin/notifications"))); err != nil {
			// delivery_error lives on the owner's notification row, so the
			// address (admin-managed, possibly outside the org) goes to the log
			// only.
			log.WithError(err).WithField("to", to).Warn("notifier: alert list email failed")
			errs = append(errs, "email (alert list): "+err.Error())
		} else {
			delivered = true
		}
	}

	status := StatusFailed
	if delivered {
		status = StatusDelivered
	}
	if len(errs) > 0 {
		return status, errors.New(strings.Join(errs, "; "))
	}
	return status, nil
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

// ownerEmailAddress is the pipeline owner's account address.
func (n *Notifier) ownerEmailAddress(ctx context.Context, userID string) (string, error) {
	var email sql.NullString
	err := n.db.QueryRowContext(ctx,
		`SELECT email FROM users WHERE id = $1`, userID,
	).Scan(&email)
	if err != nil {
		return "", fmt.Errorf("user email lookup: %w", err)
	}
	if !email.Valid || strings.TrimSpace(email.String) == "" {
		return "", fmt.Errorf("user has no email")
	}
	return email.String, nil
}

// sendEmail sends one alert to one address. footer says why this address gets
// it and where to change that, which differs for the owner and the alert list.
func (n *Notifier) sendEmail(ctx context.Context, ch EmailChannel, to string, p notificationPayload, r Rendered, footer string) error {
	pipelineLabel := r.PipelineName
	if pipelineLabel == "" {
		pipelineLabel = p.PipelineID
	}

	subject := fmt.Sprintf("[rsync-ai %s] %s", strings.ToUpper(r.Severity), r.Title)
	body := fmt.Sprintf(
		"%s\n\nPipeline: %s\n%s: %s\n\n%s\n\n--\nAutomated message from rsync-ai notifier",
		bodyText(p.Message, r.Impact),
		pipelineLabel,
		r.ActionLabel,
		absoluteActionURL(n.appBaseURL, p.ActionURL),
		footer,
	)

	return sendSMTP(ctx, ch, to, emailMessage{Subject: subject, Body: body})
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
