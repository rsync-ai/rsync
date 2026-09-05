package healer

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/backend-orchestrator/internal/connections"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	appmetrics "github.com/rsync-ai/backend-orchestrator/internal/metrics"
	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
)

var healerTracer = otel.Tracer("healer-agent")

const (
	HealerTopic = "rsync.healer.schema-changes"

	// ResultsTopic + NotifyTopic — producers exist (healer emits healing results
	// + user-visible notifications) but no consumer service subscribes today.
	// Wiring a notifier consumer that delivers to email / Slack / webhook is
	// tracked as G1 / F-Obs-1 (P0) in CAPABILITIES.md. The PG-side
	// `pipeline_run_events` writes in heal/worker.go + heal/executors.go are the
	// current user-visible channel; these topics will become the delivery
	// channel once the notifier service lands. Do NOT remove the produce calls —
	// they're intentional infrastructure for the upcoming consumer.
	//
	// ActionTopic is the reserved topic for execution-failure action records.
	// Its producer cluster — publishAction, its helper actionOutcome, and the
	// HealerAction struct they marshal — was reached solely from the reactive
	// DLQ-error classifier that was removed when the healer consolidated on the
	// canonical diagnose→heal path (the `heal` package's HealWorker). The topic
	// name + that cluster are kept as reserved infra for the same future notifier
	// consumer; they are currently unwired (no live caller of publishAction).
	ResultsTopic = "rsync.healer.results"
	NotifyTopic  = "rsync.notifications"
	ActionTopic  = "rsync.healer.actions"

	LLMServiceURL   = "http://llm-service:5000"
	AnalysisTimeout = 30 * time.Second
)

// Agent represents the Healer agent for self-healing pipelines
type Agent struct {
	kafkaManager *kafka.Manager
	db           *sql.DB
	ctx          context.Context
	cancel       context.CancelFunc
	httpClient   *http.Client
	// mcpClient invokes destination connector tools (e.g. ensure_table) so an
	// approved column change (add_column or a safe type widening) lands via the
	// SAME path the batch write uses, instead of running source-shaped DDL
	// directly against the dest. See ensureColumnViaConnector.
	mcpClient *mcp.Client
}

// HealerAction represents an action taken by the Healer agent
type HealerAction struct {
	PipelineID string `json:"pipeline_id"`
	Action     string `json:"action"` // "retry", "notify", "replan"
	Reason     string `json:"reason"`
	Timestamp  string `json:"timestamp"`
	Details    string `json:"details,omitempty"`
}

// SchemaChangeEvent represents a schema change event from kafka-mcp-sink
type SchemaChangeEvent struct {
	EventType    string                 `json:"event_type"`
	PipelineID   string                 `json:"pipeline_id"`
	Timestamp    string                 `json:"timestamp"`
	SchemaChange SchemaChange           `json:"schema_change"`
	Context      map[string]interface{} `json:"context"`
	ActionNeeded bool                   `json:"action_needed"`
}

// SchemaChange represents details of a schema change
type SchemaChange struct {
	ChangeType string `json:"change_type"`
	Table      string `json:"table"`
	Database   string `json:"database"`
	SchemaName string `json:"schema_name"`
	ColumnName string `json:"column_name"`
	ColumnType string `json:"column_type"`
	DDL        string `json:"ddl"`
	RiskLevel  string `json:"risk_level"`
	DetectedAt string `json:"detected_at"`
	Applied    bool   `json:"applied"`
	Error      string `json:"error"`
}

// LLMAnalysisResponse represents the LLM's analysis of a schema change
type LLMAnalysisResponse struct {
	SafeToAutoMigrate bool     `json:"safe_to_auto_migrate"`
	Reasoning         string   `json:"reasoning"`
	SuggestedDDL      string   `json:"suggested_ddl,omitempty"`
	Risks             []string `json:"risks,omitempty"`
	RequiresApproval  bool     `json:"requires_approval"`
	NotifyUser        bool     `json:"notify_user"`
	UserMessage       string   `json:"user_message,omitempty"`
}

// HealingResult represents the result of a healing operation
type HealingResult struct {
	PipelineID   string `json:"pipeline_id"`
	ChangeType   string `json:"change_type"`
	Table        string `json:"table"`
	Status       string `json:"status"` // "applied", "skipped", "pending_approval", "failed"
	Reason       string `json:"reason"`
	DDLApplied   string `json:"ddl_applied,omitempty"`
	ErrorMessage string `json:"error,omitempty"`
}

// NewAgent creates a new Healer agent. toolsDir is the MCP connectors root used
// to build the destination-connector client for applying approved additive DDL
// via ensure_table (empty is tolerated — it resolves to the default tools dir).
func NewAgent(kafkaManager *kafka.Manager, db *sql.DB, toolsDir string) *Agent {
	ctx, cancel := context.WithCancel(context.Background())

	return &Agent{
		kafkaManager: kafkaManager,
		db:           db,
		ctx:          ctx,
		cancel:       cancel,
		mcpClient:    mcp.NewClient(mcp.NewServerManager(toolsDir)),
		httpClient: &http.Client{
			Timeout: AnalysisTimeout,
		},
	}
}

// ApprovedChangeTopic is published by api-gateway when a user approves a pending DDL.
const ApprovedChangeTopic = "rsync.healer.approved-changes"

// schemaSubscription is a topic→handler binding for the schema-drift path.
// Required=true means a failed subscription is fatal (return error); Required=false
// means best-effort (log + continue).
type schemaSubscription struct {
	Topic    string
	Handler  kafka.ConsumeHandlerWithContext
	Required bool
}

// schemaDriftSubscriptions is the SINGLE source of truth for which topics the
// drift-detect → approve path consumes: exactly HealerTopic + ApprovedChangeTopic
// and NEVER the executor/planner DLQs. Centralised here so a broker-free unit
// test can assert the DLQ topics never creep into this path (subscribing them
// would double-react to execution failures already handled by the executor
// worker's executeWithHealer + suggestRecoveryAction and, asynchronously, by the
// canonical diagnose→heal HealWorker).
func (a *Agent) schemaDriftSubscriptions() []schemaSubscription {
	return []schemaSubscription{
		{Topic: HealerTopic, Handler: a.handleSchemaChangeMessage, Required: true},
		{Topic: ApprovedChangeTopic, Handler: a.handleApprovedChange, Required: false},
	}
}

// StartSchemaOnly begins consuming ONLY the two schema-drift topics
// (HealerTopic + ApprovedChangeTopic) — the proactive drift-detect → approve
// path. It deliberately omits any executor/planner DLQ subscriptions: reactive
// DLQ consumers would double-react to execution failures already handled
// synchronously by the executor worker's executeWithHealer + suggestRecoveryAction
// and asynchronously by the canonical diagnose→heal HealWorker. (An earlier full
// Start() that also subscribed those DLQs and ran a second, divergent LLM error
// classifier was removed when the healer consolidated on that canonical path.)
// handleSchemaChangeMessage has no synchronous twin, so subscribing the two
// schema topics in isolation is collision-free. Gated by RSYNC_SCHEMA_DRIFT_ENABLED
// at the call site (workers/executor.go ExecutorWorker.Start); off → never
// called → dormant.
func (a *Agent) StartSchemaOnly() error {
	log.Info("[HealerAgent] Starting schema-drift consumers (drift-detect → approve)...")

	for _, s := range a.schemaDriftSubscriptions() {
		if err := a.kafkaManager.ConsumeWithContext(s.Topic, s.Handler); err != nil {
			if s.Required {
				return fmt.Errorf("failed to start consumer for %s: %w", s.Topic, err)
			}
			log.Warnf("[HealerAgent] Could not start consumer for %s: %v", s.Topic, err)
			continue
		}
		log.Info("[HealerAgent] Listening on ", s.Topic)
	}

	return nil
}

// handleApprovedChange processes a user-approved schema change and applies the DDL.
func (a *Agent) handleApprovedChange(ctx context.Context, msg *sarama.ConsumerMessage) error {
	_, span := healerTracer.Start(ctx, "handle_approved_change")
	defer span.End()

	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		log.Errorf("[HealerAgent] Failed to unmarshal approved change: %v", err)
		return err
	}

	approvalID, _ := payload["approval_id"].(string)
	pipelineID, _ := payload["pipeline_id"].(string)
	ddl, _ := payload["ddl"].(string)
	changeType, _ := payload["change_type"].(string)
	tableName, _ := payload["table_name"].(string)

	span.SetAttributes(
		attribute.String("pipeline_id", pipelineID),
		attribute.String("approval_id", approvalID),
	)

	// Destructive DDL is never applied, approved or not — applyMigration would
	// refuse it a few lines below and we would then mark the user's approval
	// `failed` and alarm them, for approving a change the UI told them they would
	// have to run by hand. api-gateway no longer dispatches these; this is the
	// backstop for messages already on the topic and for any other producer.
	// Leave the row `approved` (decision recorded) and say so, quietly.
	if notAutoAppliedDDL(ddl) {
		reason := "Approved, but destructive DDL is never auto-applied — run it manually on the destination"
		if isAdvisoryDDL(ddl) {
			reason = "Approved — this drift is a notice, not a statement; there is nothing to apply to the destination"
		}
		log.Infof("[HealerAgent] Approved DDL is not auto-applied by design (approval %s, pipeline %s): %s",
			approvalID, pipelineID, ddl)
		// recordHealingResult writes through a.db without a nil check, matching the
		// a.db != nil guards used elsewhere in this handler.
		if a.db != nil {
			a.recordHealingResult(ctx, &HealingResult{
				PipelineID: pipelineID,
				ChangeType: changeType,
				Table:      tableName,
				Status:     "skipped",
				Reason:     reason,
				DDLApplied: "",
			})
		}
		return nil
	}

	log.Infof("[HealerAgent] Applying user-approved DDL for pipeline %s (approval %s): %s", pipelineID, approvalID, ddl)

	if err := a.applyMigration(ctx, pipelineID, ddl); err != nil {
		log.Errorf("[HealerAgent] Failed to apply approved DDL (approval %s): %v", approvalID, err)
		if a.db != nil && approvalID != "" {
			_, _ = a.db.ExecContext(ctx, `
				UPDATE schema_change_approvals
				SET status = 'failed', error_message = $1, updated_at = NOW()
				WHERE id = $2
			`, err.Error(), approvalID)
		}
		a.notifyUser(pipelineID, fmt.Sprintf("⚠️ Approved DDL failed to apply: %s", err.Error()))
		return nil
	}

	log.Infof("[HealerAgent] ✅ Approved DDL applied successfully (approval %s)", approvalID)

	if a.db != nil && approvalID != "" {
		_, _ = a.db.ExecContext(ctx, `
			UPDATE schema_change_approvals
			SET status = 'applied', applied_at = NOW(), updated_at = NOW()
			WHERE id = $1
		`, approvalID)
	}

	result := &HealingResult{
		PipelineID: pipelineID,
		ChangeType: changeType,
		Table:      tableName,
		Status:     "applied",
		Reason:     "User approved DDL migration",
		DDLApplied: ddl,
	}
	a.recordHealingResult(ctx, result)
	a.publishHealingResult(result)

	return nil
}

// Stop gracefully shuts down the healer agent
func (a *Agent) Stop() {
	log.Info("[HealerAgent] Stopping...")
	a.cancel()
}

// handleSchemaChangeMessage processes incoming schema change events (Original Logic)
func (a *Agent) handleSchemaChangeMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
	_, span := healerTracer.Start(ctx, "handle_schema_change")
	defer span.End()

	log.Infof("[HealerAgent] Received schema change event from offset %d", msg.Offset)

	// Parse event using smart deserialization (handles both Avro and JSON)
	var event SchemaChangeEvent
	if err := kafka.SmartDeserialize(msg.Value, &event); err != nil {
		log.Errorf("[HealerAgent] Failed to unmarshal event: %v", err)
		return err
	}

	span.SetAttributes(
		attribute.String("pipeline_id", event.PipelineID),
		attribute.String("event_type", event.EventType),
		attribute.String("change_type", event.SchemaChange.ChangeType),
		attribute.String("table", event.SchemaChange.Table),
	)

	log.Infof("[HealerAgent] Processing %s for pipeline %s: %s on %s",
		event.EventType, event.PipelineID,
		event.SchemaChange.ChangeType, event.SchemaChange.Table)

	// A change the producer already applied is history, not a decision. The CDC sink's
	// additive reconciler adds a new source column to the destination the moment it
	// appears (kafka-mcp-sink reportAppliedSchemaDrift) — by the time this message
	// lands, the column is live. Running the normal path on it would produce one of two
	// wrong answers: `pending_approval`, inviting a human to approve something that has
	// already happened, or auto-apply, re-running DDL against a destination that has it.
	// Record it as applied and say so. This is the ONLY reason CDC drift is now visible
	// at all; before it, the same ADD COLUMN that blocks a batch pipeline on human
	// approval passed through a CDC pipeline with no row, no badge and no notification.
	if event.SchemaChange.Applied {
		return a.recordAlreadyAppliedChange(ctx, &event)
	}

	// Analyze the change using LLM
	analysis, err := a.analyzeWithLLM(ctx, &event)
	if err != nil {
		log.Errorf("[HealerAgent] LLM analysis failed: %v, using rule-based fallback", err)
		analysis = a.fallbackAnalysis(&event)
	}

	// Take action based on analysis
	result := a.processAnalysis(ctx, &event, analysis)

	// Record the result
	a.recordHealingResult(ctx, result)

	// Publish result for tracking
	a.publishHealingResult(result)

	return nil
}

func (a *Agent) publishAction(pipelineID, action, reason, details string) {
	healerAction := HealerAction{
		PipelineID: pipelineID,
		Action:     action,
		Reason:     reason,
		Details:    details,
		Timestamp:  time.Now().UTC().Format(time.RFC3339),
	}

	actionBytes, err := json.Marshal(healerAction)
	if err != nil {
		log.Errorf("[HealerAgent] Failed to marshal action: %v", err)
		return
	}

	if a.kafkaManager != nil {
		if err := a.kafkaManager.Produce(ActionTopic, []byte(pipelineID), actionBytes); err != nil {
			log.Errorf("[HealerAgent] Failed to publish action: %v", err)
		}
	}

	// F-Obs-2: record the execution-failure healing action. A bounded set of
	// actions (retry|notify|replan|unknown); anything other than an automated
	// retry means recovery deferred to a human/re-plan → escalated. This feeds
	// the same rsync_healer_actions_total series as the schema-change path.
	appmetrics.HealerActionsTotal.
		WithLabelValues(action, actionOutcome(action)).
		Inc()
}

// actionOutcome maps a publishAction action to the bounded outcome label.
// retry is an automated recovery attempt (success); every other action hands
// off to a human or the planner → escalated.
func actionOutcome(action string) string {
	if action == "retry" {
		return "success"
	}
	return "escalated"
}

// extractJSONObject pulls a JSON object out of an LLM response that may be
// wrapped in a markdown code fence (```json … ```) or padded with prose.
// Models frequently fence their output even when asked for bare JSON; feeding
// that raw to json.Unmarshal fails with "invalid character '`'" and silently
// drops us to the rule-based fallback. Callers pass the result to
// json.Unmarshal; if no object is found the trimmed input is returned so the
// caller still surfaces a real parse error.
func extractJSONObject(s string) string {
	s = strings.TrimSpace(s)
	// Strip a leading ``` / ```json fence and its matching trailing ```.
	if strings.HasPrefix(s, "```") {
		if nl := strings.IndexByte(s, '\n'); nl != -1 {
			s = s[nl+1:] // drop the opening fence line (``` or ```json)
		} else {
			s = strings.TrimPrefix(s, "```")
		}
		if idx := strings.LastIndex(s, "```"); idx != -1 {
			s = s[:idx] // drop the closing fence
		}
		s = strings.TrimSpace(s)
	}
	// If prose still surrounds the object, slice from the first { to the last }.
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		if start := strings.IndexByte(s, '{'); start != -1 {
			if end := strings.LastIndexByte(s, '}'); end > start {
				s = s[start : end+1]
			}
		}
	}
	return strings.TrimSpace(s)
}

func (a *Agent) analyzeWithLLM(ctx context.Context, event *SchemaChangeEvent) (*LLMAnalysisResponse, error) {
	prompt := buildAnalysisPrompt(event)

	reqBody, _ := json.Marshal(map[string]interface{}{
		"prompt_name": "chat/schema_healer",
		"variables": map[string]interface{}{
			"prompt": prompt,
		},
	})

	req, err := http.NewRequestWithContext(ctx, "POST", LLMServiceURL+"/v1/completion", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("LLM returned status %d: %s", resp.StatusCode, string(body))
	}

	var llmResp map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&llmResp); err != nil {
		return nil, err
	}

	content, ok := llmResp["content"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid LLM response format")
	}

	var analysis LLMAnalysisResponse
	if err := json.Unmarshal([]byte(extractJSONObject(content)), &analysis); err != nil {
		return nil, fmt.Errorf("failed to parse LLM analysis: %w (content: %s)", err, content)
	}

	return &analysis, nil
}

func buildAnalysisPrompt(event *SchemaChangeEvent) string {
	return fmt.Sprintf(`Analyze this schema change and determine if it's safe to auto-migrate.

## Schema Change
- **Pipeline ID**: %s
- **Change Type**: %s
- **Table**: %s.%s
- **Column**: %s (%s)
- **DDL**: %s
- **Risk Level**: %s

## Context
- Auto-apply enabled: %v
- Skip destructive enabled: %v

## Rules
1. ADD COLUMN (nullable) -> Safe to auto-apply
2. DROP COLUMN -> NOT safe, requires approval (data loss risk)
3. MODIFY COLUMN (widening) -> Safe if data won't be truncated
4. MODIFY COLUMN (narrowing) -> NOT safe, requires approval
5. CREATE TABLE -> Safe to auto-apply
6. DROP TABLE -> NOT safe, requires approval

Respond with JSON:
{
  "safe_to_auto_migrate": true/false,
  "reasoning": "Brief explanation",
  "suggested_ddl": "DDL to apply if different from source",
  "risks": ["list of potential risks"],
  "requires_approval": true/false,
  "notify_user": true/false,
  "user_message": "Message for user if notification needed"
}`,
		event.PipelineID,
		event.SchemaChange.ChangeType,
		event.SchemaChange.Database,
		event.SchemaChange.Table,
		event.SchemaChange.ColumnName,
		event.SchemaChange.ColumnType,
		event.SchemaChange.DDL,
		event.SchemaChange.RiskLevel,
		event.Context["auto_apply_enabled"],
		event.Context["skip_destructive_enabled"],
	)
}

func (a *Agent) fallbackAnalysis(event *SchemaChangeEvent) *LLMAnalysisResponse {
	change := event.SchemaChange

	switch change.ChangeType {
	case "add_column":
		return &LLMAnalysisResponse{
			SafeToAutoMigrate: true,
			Reasoning:         "ADD COLUMN is generally safe for data pipelines",
			RequiresApproval:  false,
			NotifyUser:        false,
		}

	case "drop_column", "drop_table":
		return &LLMAnalysisResponse{
			SafeToAutoMigrate: false,
			Reasoning:         "Destructive operations require manual approval",
			RequiresApproval:  true,
			NotifyUser:        true,
			Risks:             []string{"Data loss", "Breaking downstream dependencies"},
			UserMessage: fmt.Sprintf("Schema change detected: %s on %s. Manual approval required.",
				change.ChangeType, change.Table),
		}

	case "modify_column":
		return &LLMAnalysisResponse{
			SafeToAutoMigrate: false,
			Reasoning:         "Column type changes may cause data truncation",
			RequiresApproval:  true,
			NotifyUser:        true,
			Risks:             []string{"Data truncation", "Type conversion errors"},
			UserMessage: fmt.Sprintf("Column type change on %s.%s requires review.",
				change.Table, change.ColumnName),
		}

	case "create_table":
		return &LLMAnalysisResponse{
			SafeToAutoMigrate: true,
			Reasoning:         "CREATE TABLE is safe - no existing data affected",
			RequiresApproval:  false,
			NotifyUser:        false,
		}

	default:
		return &LLMAnalysisResponse{
			SafeToAutoMigrate: false,
			Reasoning:         "Unknown change type, requiring manual review",
			RequiresApproval:  true,
			NotifyUser:        true,
		}
	}
}

// schemaDriftAutoApplyEnabled reports whether the operator has opted in to letting
// the healer apply "safe" (additive) DDL to the destination WITHOUT human approval.
// It is a deliberately separate opt-in from RSYNC_SCHEMA_DRIFT_ENABLED (which only
// turns on detection): enabling detection alone must never silently mutate a
// destination schema. Default OFF — only the literal "true" opts in, mirroring
// schemaDriftEnabled() in the executor package.
func schemaDriftAutoApplyEnabled() bool {
	return os.Getenv("RSYNC_SCHEMA_DRIFT_AUTOAPPLY") == "true"
}

// shouldAutoApply reports whether a classified schema change may be applied to the
// destination without human approval. Auto-apply requires ALL of: the operator
// opt-in (autoApplyEnabled), the classifier deeming the change safe, and the change
// not independently requiring approval. Anything destructive or approval-required
// fails closed regardless of the opt-in, and a nil analysis never auto-applies.
func shouldAutoApply(analysis *LLMAnalysisResponse, autoApplyEnabled bool) bool {
	if analysis == nil {
		return false
	}
	return autoApplyEnabled && analysis.SafeToAutoMigrate && !analysis.RequiresApproval
}

func (a *Agent) processAnalysis(ctx context.Context, event *SchemaChangeEvent, analysis *LLMAnalysisResponse) *HealingResult {
	result := &HealingResult{
		PipelineID: event.PipelineID,
		ChangeType: event.SchemaChange.ChangeType,
		Table:      event.SchemaChange.Table,
	}

	// Auto-apply is opt-in. With RSYNC_SCHEMA_DRIFT_AUTOAPPLY off (the default), a
	// change the classifier deems safe is NOT applied silently — it routes to the
	// approval queue below so a human sees it in the /schema-changes surface first.
	autoApplyEnabled := schemaDriftAutoApplyEnabled()

	// An advisory note has nothing to apply, so it must not enter the auto-apply
	// branch even with the opt-in on: applyMigration would refuse it and the user
	// would get a "Migration failed" alert for a change that asked nothing of them.
	// Falling through routes it to the approval queue, which is where a notice
	// belongs — visible in /schema-changes, dismissable, never executed.
	proposedDDL := analysis.SuggestedDDL
	if proposedDDL == "" {
		proposedDDL = event.SchemaChange.DDL
	}

	if shouldAutoApply(analysis, autoApplyEnabled) && !isAdvisoryDDL(proposedDDL) {
		ddl := proposedDDL

		if err := a.applyMigration(ctx, event.PipelineID, ddl); err != nil {
			result.Status = "failed"
			result.Reason = "Migration failed"
			result.ErrorMessage = err.Error()
		} else {
			result.Status = "applied"
			result.Reason = analysis.Reasoning
			result.DDLApplied = ddl
		}
	} else if analysis.RequiresApproval || analysis.SafeToAutoMigrate {
		// Routes here when the change requires approval, OR when it is a "safe"
		// additive change that was NOT auto-applied because the auto-apply opt-in
		// is off. Either way it lands in the approval queue and surfaces in the
		// /schema-changes UI for a human decision — the default, brand-safe path.
		result.Status = "pending_approval"
		result.Reason = analysis.Reasoning

		a.storePendingApproval(ctx, event, analysis)

		// Notify on explicit request, or whenever a safe change is being held for
		// approval only because auto-apply is disabled (the user asked to be told
		// the moment drift happens, so don't stay silent just because it's additive).
		if analysis.NotifyUser || (analysis.SafeToAutoMigrate && !autoApplyEnabled) {
			msg := analysis.UserMessage
			if msg == "" {
				msg = schemaDriftNotificationText(event.SchemaChange.ChangeType, event.SchemaChange.Table, false)
			}
			// Deep-link the notification to the schema changes page and carry the
			// full remediation envelope. Build the canonical schema-drift
			// StructuredError (Category=schema_drift ⇒ code SCHEMA_DRIFT_DETECTED +
			// remediation steps + doc URL), then override its generic user message
			// with the specific change detail. Category is what routes action_url to
			// /pipelines/{id}/schema-changes in notifyUserStructured — the legacy
			// notifyUser path never set it, so drift alerts used to land on the
			// pipeline overview instead. Scrub the message defensively (parity with
			// the legacy notifyUser path) before it is persisted + shown to the user.
			se := diagnose.FromDiagnosis(
				diagnose.Diagnosis{Category: diagnose.CategorySchemaDrift},
				diagnose.Signal{PipelineID: event.PipelineID},
			)
			se.UserMessage = llmscrub.Scrub(msg)
			// Every drift on a pipeline shares this code AND this action_url, so
			// without a subject the notifier's 60-minute window collapsed them all
			// into the first one: drop a column, then drop a table five minutes
			// later, and only the column was ever announced. Built from the
			// structured fields rather than the message, because UserMessage can be
			// LLM-authored and would vary between retries of the SAME drift.
			se.DedupSubject = schemaDriftDedupSubject(event.SchemaChange)
			a.notifyUserStructured(event.PipelineID, se)
		}
	} else {
		result.Status = "skipped"
		result.Reason = analysis.Reasoning
	}

	log.Infof("[HealerAgent] Processed change: %s -> %s (%s)",
		event.SchemaChange.ChangeType, result.Status, result.Reason)

	return result
}

// isDestructiveDDL reports whether this DDL drops or truncates, in which case the
// healer never runs it — not on auto-apply, and not after a user approves it. It
// is the single definition behind both the applyMigration guard and the
// approved-changes handler, so the two cannot drift into disagreeing about the
// same statement. The api-gateway mirrors it in autoApplicableDDL
// (handlers/schema_evolution.go) to tell the user, before they click Approve,
// that approval here is record-only; keep the two in lockstep.
func isDestructiveDDL(ddl string) bool {
	d := strings.ToLower(strings.TrimSpace(ddl))
	return strings.Contains(d, " drop ") || strings.HasPrefix(d, "drop ") ||
		strings.Contains(d, " truncate ") || strings.HasPrefix(d, "truncate ")
}

// isAdvisoryDDL reports whether this "DDL" is a note rather than a statement.
// The detector emits two of them: a new source table appearing
// ("-- drift: table X appeared in source") and a declared-type change the
// destination does not need ("-- drift: X.y declared type changed …", B1).
//
// Both used to read as auto-applicable, so approving one sent a SQL comment to
// the destination, which executed it happily, and the row was marked `applied`
// having applied nothing. That is the same false-success shape as C1, arrived at
// from the other side: there, the user was told nothing would be applied and
// something was attempted; here, they were told it was applied and nothing was.
func isAdvisoryDDL(ddl string) bool {
	return strings.HasPrefix(strings.TrimSpace(ddl), "--")
}

// notAutoAppliedDDL is the single predicate for "approving this records the
// decision and nothing runs". api-gateway mirrors it as !autoApplicableDDL to
// tell the user that BEFORE they click Approve; keep the two in lockstep.
func notAutoAppliedDDL(ddl string) bool {
	return isDestructiveDDL(ddl) || isAdvisoryDDL(ddl)
}

func (a *Agent) applyMigration(ctx context.Context, pipelineID, ddl string) error {
	pipelineID = strings.TrimSpace(pipelineID)
	ddl = strings.TrimSpace(ddl)
	if pipelineID == "" {
		return fmt.Errorf("missing pipeline_id")
	}
	if ddl == "" {
		return fmt.Errorf("missing ddl")
	}

	// Safety guard: do not auto-apply destructive DDL even if the LLM misclassifies.
	if isDestructiveDDL(ddl) {
		return fmt.Errorf("refusing to auto-apply potentially destructive ddl")
	}
	// An advisory note is not a statement. Sending it to the destination would
	// "succeed" (a comment is valid SQL everywhere) and report an apply that never
	// happened; the auto-apply path can reach here without passing through
	// handleApprovedChange, so the guard belongs on both.
	if isAdvisoryDDL(ddl) {
		return fmt.Errorf("refusing to apply advisory drift note as ddl")
	}

	var destConnectionID string
	var destConnectorType string
	err := a.db.QueryRowContext(ctx, `
		SELECT p.destination_connection_id, c.connector_type
		FROM pipelines p
		JOIN connections c ON c.id = p.destination_connection_id
		WHERE p.id = $1
	`, pipelineID).Scan(&destConnectionID, &destConnectorType)

	if err != nil {
		return fmt.Errorf("failed to get destination connection: %w", err)
	}
	destConnectionID = strings.TrimSpace(destConnectionID)
	destConnectorType = strings.ToLower(strings.TrimSpace(destConnectorType))
	if destConnectionID == "" || destConnectorType == "" {
		return fmt.Errorf("destination connection missing for pipeline")
	}

	log.Infof("[HealerAgent] Applying DDL to %s destination for pipeline %s: %s",
		destConnectorType, pipelineID, ddl)

	// MVP: Apply directly for DB destinations we can safely connect to.
	// (Cloud warehouses / APIs should be routed via MCP in a future phase.)
	cfg, cfgErr := connections.NewManager(a.db).Get(ctx, destConnectionID)
	if cfgErr != nil {
		return fmt.Errorf("failed to load destination connection config: %w", cfgErr)
	}

	destConn := normalizeDestConnector(destConnectorType)

	// Additive ADD COLUMN → route through the destination connector's idempotent
	// ensure_table (the SAME path the batch write uses). The stored DDL is
	// source-shaped — a source schema qualifier (e.g. "pipeline_test.categories",
	// wrong for a dest that has "public.categories") and a source type token
	// (e.g. "string", which is not valid Postgres DDL). Running it verbatim
	// against the dest fails; ensure_table instead resolves the per-pipeline dest
	// namespace and maps the normalized type to the dest dialect. So add_column is
	// NEVER executed as raw DDL.
	if table, col, colType, ok := parseAddColumnDDL(ddl); ok {
		return a.ensureColumnViaConnector(ctx, pipelineID, destConn, cfg, table, col, colType, false)
	}

	// Type change (modify_column — approve-only, high-risk) → route through the SAME
	// connector ensure_table path as add_column, for the same reason: the stored DDL
	// ("ALTER TABLE inventory.products ALTER COLUMN price TYPE number") carries a source
	// schema qualifier and a canonical type token, neither valid at the dest, so raw
	// apply fails (KI-DRIFT-TYPECHANGE-APPLY-FAILS). ensure_table resolves the dest
	// namespace, maps the canonical type to the dest dialect, and applies a SAFE, non-
	// lossy widening on the existing column (MySQL via _WIDEN; Postgres via the mirrored
	// widen path). strict=true → a non-widening/narrowing change reports a clear error
	// (the user approved a change that cannot be applied safely) instead of silently
	// succeeding. Destructive DROP/TRUNCATE is already refused above.
	if table, col, colType, ok := parseModifyColumnDDL(ddl); ok {
		return a.ensureColumnViaConnector(ctx, pipelineID, destConn, cfg, table, col, colType, true)
	}

	// Any other DDL shape keeps the direct-apply path.
	switch destConn {
	case "postgresql":
		return applyDDLPostgres(ctx, cfg, ddl)
	case "mysql":
		return applyDDLMySQL(ctx, cfg, ddl)
	default:
		return fmt.Errorf("unsupported destination for auto-migration: %s", destConnectorType)
	}
}

// normalizeDestConnector folds destination connector aliases to the canonical MCP
// connector name — the value the mcp.Client uses for both the connector dir and the
// "<type>_<operation>" tool name. Mirrors the executor's dest routing (postgres→
// postgresql, mariadb→mysql). The healer must NOT import the executor package
// (executor imports healer), so this is kept in lockstep by hand.
func normalizeDestConnector(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "postgres", "postgresql":
		return "postgresql"
	case "mysql", "mariadb":
		return "mysql"
	default:
		return strings.ToLower(strings.TrimSpace(t))
	}
}

// parseAddColumnDDL extracts (table, column, columnType) from the detector's
// deterministic add_column DDL "ALTER TABLE <table> ADD COLUMN <col> <type...>"
// (executor/schema_drift.go). <table> is source-qualified and <type> is the
// source's normalized token — both are corrected at apply time by the destination
// connector's ensure_table. Returns ok=false for any other DDL shape (e.g.
// ALTER COLUMN … TYPE, DROP COLUMN) so the caller falls back to direct apply.
func parseAddColumnDDL(ddl string) (table, column, columnType string, ok bool) {
	fields := strings.Fields(ddl)
	// Minimum: ALTER TABLE <t> ADD COLUMN <col> <type>  → 7 tokens.
	if len(fields) < 7 {
		return "", "", "", false
	}
	if !strings.EqualFold(fields[0], "ALTER") || !strings.EqualFold(fields[1], "TABLE") ||
		!strings.EqualFold(fields[3], "ADD") || !strings.EqualFold(fields[4], "COLUMN") {
		return "", "", "", false
	}
	table = strings.TrimSpace(fields[2])
	column = strings.TrimSpace(fields[5])
	columnType = strings.TrimSpace(strings.Join(fields[6:], " "))
	if table == "" || column == "" || columnType == "" {
		return "", "", "", false
	}
	return table, column, columnType, true
}

// parseModifyColumnDDL extracts (table, column, columnType) from the detector's
// deterministic modify_column DDL "ALTER TABLE <table> ALTER COLUMN <col> TYPE <type...>"
// (executor/schema_drift.go). Exactly like add_column, <table> is source-qualified and
// <type> is the source's canonical token ("number"/"string") — neither is valid at the
// destination, so running the DDL verbatim fails (KI-DRIFT-TYPECHANGE-APPLY-FAILS). Both
// are corrected at apply time by the destination connector's ensure_table (which resolves
// the dest namespace, maps canonical→dialect, and applies a SAFE widening on the existing
// column). Returns ok=false for any other DDL shape so the caller falls back to direct apply.
func parseModifyColumnDDL(ddl string) (table, column, columnType string, ok bool) {
	fields := strings.Fields(ddl)
	// Minimum: ALTER TABLE <t> ALTER COLUMN <col> TYPE <type>  → 8 tokens.
	if len(fields) < 8 {
		return "", "", "", false
	}
	if !strings.EqualFold(fields[0], "ALTER") || !strings.EqualFold(fields[1], "TABLE") ||
		!strings.EqualFold(fields[3], "ALTER") || !strings.EqualFold(fields[4], "COLUMN") ||
		!strings.EqualFold(fields[6], "TYPE") {
		return "", "", "", false
	}
	table = strings.TrimSpace(fields[2])
	column = strings.TrimSpace(fields[5])
	columnType = strings.TrimSpace(strings.Join(fields[7:], " "))
	if table == "" || column == "" || columnType == "" {
		return "", "", "", false
	}
	return table, column, columnType, true
}

// bareTable strips a schema/namespace qualifier ("pipeline_test.categories" →
// "categories"). The destination connector resolves the dest schema from the
// forwarded namespace, so it must receive the bare table name.
func bareTable(qualified string) string {
	q := strings.TrimSpace(qualified)
	if i := strings.LastIndex(q, "."); i >= 0 && i < len(q)-1 {
		return q[i+1:]
	}
	return q
}

// DestinationQualifiedTable renders the object name that DESTRUCTIVE, never-auto-applied
// DDL must carry, given the table's SOURCE key and the pipeline's destination namespace.
//
// drop_column and drop_table are refused by both auto-apply gates (autoApplicableDDL in
// api-gateway, notAutoAppliedDDL here) before AND after approval, so their DDL string is
// never executed by us — it is an instruction, shown to the user as "apply this on the
// destination yourself". It was being built from the SOURCE key
// (`<source_schema>.<table>`), which on this product is almost never the destination's
// name for the same table: the destination lives in the pipeline's own namespace
// (`rsync_<src-schema>_<id8>`). Handing someone `DROP TABLE public.orders` to run against
// the destination is not a harmless typo — `public.orders` very plausibly EXISTS on the
// destination and is a different, real table.
//
// Empty or "default" namespace = legacy pipeline with nothing persisted; the connector
// writes to its own default schema, so the bare table name is what resolves in the user's
// destination session. The source qualifier is never a correct answer here.
//
// Only the never-applied statements go through this. add_column/modify_column keep the
// source key because the healer rewrites them at apply time (bareTable +
// destinationNamespace in ensureColumnViaConnector), and changing their text would
// re-key the schema_change_approvals UNIQUE(pipeline_id, ddl) dedup for no gain.
func DestinationQualifiedTable(destNamespace, sourceQualified string) string {
	t := bareTable(sourceQualified)
	if ns := strings.TrimSpace(destNamespace); ns != "" && !strings.EqualFold(ns, "default") {
		return ns + "." + t
	}
	return t
}

// destinationNamespace returns the per-pipeline dest namespace persisted at
// pipelines.config->>'destination_namespace' (the HITL namespace). Empty for
// legacy pipelines — the connector then falls back to its own default schema.
// Mirrors executor.resolveDestinationNamespace's DB read (not imported: executor
// imports healer).
func (a *Agent) destinationNamespace(ctx context.Context, pipelineID string) string {
	if a.db == nil {
		return ""
	}
	var ns sql.NullString
	err := a.db.QueryRowContext(ctx, `
		SELECT NULLIF(TRIM(COALESCE(config->>'destination_namespace', '')), '')
		FROM pipelines WHERE id = $1
	`, pipelineID).Scan(&ns)
	if err != nil || !ns.Valid {
		return ""
	}
	if n := strings.TrimSpace(ns.String); !strings.EqualFold(n, "default") {
		return n
	}
	return ""
}

// ensureColumnViaConnector applies a drifted column change (add_column OR modify_column
// type change) to the destination by invoking the destination MCP connector's idempotent
// ensure_table tool — the same path the batch write uses. The connector resolves the dest
// schema from the forwarded namespace and maps the normalized column type to the dest
// dialect (e.g. "string" → TEXT), so a source-shaped column definition lands correctly:
//   - add_column: ensure_table's ADD COLUMN IF NOT EXISTS creates the column.
//   - modify_column: ensure_table detects the existing column and applies a SAFE, non-lossy
//     widening (MySQL via _WIDEN, Postgres via the mirrored widen path).
// types_are_ddl is left false so the type is treated as a canonical token, not final DDL.
// synthetic_pk is set false so a keyless migration call does NOT inject the Fivetran-style
// _rsync_row_hash/_rsync_synced_at columns + unique index onto an existing table (this is a
// pure column ensure, not a table (re)creation). strict makes a requested type change that
// is NOT a safe widening report a clear error instead of silently succeeding — used for
// approved type changes, where the user must learn an unsafe cast could not be applied.
func (a *Agent) ensureColumnViaConnector(ctx context.Context, pipelineID, destConn string, cfg map[string]string, srcTable, column, columnType string, strict bool) error {
	if a.mcpClient == nil {
		return fmt.Errorf("mcp client not configured for connector-based apply")
	}
	if destConn != "postgresql" && destConn != "mysql" {
		return fmt.Errorf("unsupported destination for auto-migration: %s", destConn)
	}

	table := bareTable(srcTable)
	if table == "" {
		return fmt.Errorf("could not resolve destination table from %q", srcTable)
	}

	params := map[string]interface{}{
		"table":        table,
		"columns":      []string{column},
		"column_types": map[string]string{column: columnType},
		// A column-migration call never (re)creates keys; block the keyless
		// synthetic-PK fallback so we don't pollute an existing table.
		"synthetic_pk": false,
	}
	if strict {
		params["strict_type_change"] = true
	}
	if pipelineID != "" {
		params["pipeline_id"] = pipelineID
	}
	// Forward the per-pipeline dest namespace (both keys, mirroring the sink's
	// addNamespaceParam) only when it is real; empty → connector default schema.
	if ns := a.destinationNamespace(ctx, pipelineID); ns != "" {
		params["namespace"] = ns
		params["db_or_schema"] = ns
	}

	log.Infof("[HealerAgent] ensure_table column change on %s dest (pipeline %s): table=%s column=%s type=%q strict=%v",
		destConn, pipelineID, table, column, columnType, strict)

	resp, err := a.mcpClient.ExecuteWithContext(ctx, mcp.ExecuteRequest{
		Connector: destConn,
		Version:   "latest",
		Operation: "ensure_table",
		Config:    cfg,
		Params:    params,
	})
	if err != nil {
		return fmt.Errorf("ensure_table call failed: %w", err)
	}
	if resp == nil || !resp.Success {
		msg := "unknown error"
		if resp != nil && strings.TrimSpace(resp.Error) != "" {
			msg = resp.Error
		}
		return fmt.Errorf("ensure_table did not succeed: %s", msg)
	}
	return nil
}

func applyDDLPostgres(ctx context.Context, cfg map[string]string, ddl string) error {
	host := strings.TrimSpace(cfg["host"])
	port := strings.TrimSpace(cfg["port"])
	user := strings.TrimSpace(cfg["user"])
	pass := cfg["password"]
	dbname := strings.TrimSpace(cfg["database"])
	if dbname == "" {
		dbname = strings.TrimSpace(cfg["dbname"])
	}
	// Host-aware SSL: stored PG connections carry no sslmode, so a bare
	// "disable" default made auto-migration DDL against a managed PG
	// (Azure/RDS) fail with "no pg_hba.conf entry … no encryption".
	sslmode := cdc.ResolvePostgresSSLMode(
		map[string]interface{}{"sslmode": cfg["sslmode"], "ssl_mode": cfg["ssl_mode"]}, host)
	if host == "" || user == "" || dbname == "" {
		return fmt.Errorf("postgres config missing host/user/database")
	}
	if port == "" {
		port = "5432"
	}

	// Use a short, bounded timeout for DDL apply.
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// NOTE: requires pq driver registered in the running binary.
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", host, port, user, pass, dbname, sslmode)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(reqCtx); err != nil {
		return fmt.Errorf("postgres ping failed: %w", err)
	}
	if _, err := db.ExecContext(reqCtx, ddl); err != nil {
		return fmt.Errorf("postgres ddl exec failed: %w", err)
	}
	return nil
}

func applyDDLMySQL(ctx context.Context, cfg map[string]string, ddl string) error {
	host := strings.TrimSpace(cfg["host"])
	port := strings.TrimSpace(cfg["port"])
	user := strings.TrimSpace(cfg["user"])
	pass := cfg["password"]
	dbname := strings.TrimSpace(cfg["database"])
	if dbname == "" {
		dbname = strings.TrimSpace(cfg["schema"])
	}
	if host == "" || user == "" || dbname == "" {
		return fmt.Errorf("mysql config missing host/user/database")
	}
	if port == "" {
		port = "3306"
	}

	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// NOTE: requires mysql driver registered in the running binary.
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&multiStatements=false", user, pass, host, port, dbname)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(reqCtx); err != nil {
		return fmt.Errorf("mysql ping failed: %w", err)
	}
	if _, err := db.ExecContext(reqCtx, ddl); err != nil {
		return fmt.Errorf("mysql ddl exec failed: %w", err)
	}
	return nil
}

// recordAlreadyAppliedChange files a schema change the producer has already applied to
// the destination, so it appears on the pipeline's schema changes page as history rather
// than vanishing. It never queues an approval and never applies anything.
//
// It exists because batch and CDC answer "who approves a schema change?" oppositely:
// batch stops and waits for a human, CDC's sink applies additive changes on the spot.
// Both answers are defensible; what was not defensible is that CDC's answer was
// invisible. This does NOT change CDC's behaviour — additive changes are still applied
// without asking, drops are still not propagated, and column types are still never
// altered. It changes only whether the user is told.
func (a *Agent) recordAlreadyAppliedChange(ctx context.Context, event *SchemaChangeEvent) error {
	result := &HealingResult{
		PipelineID: event.PipelineID,
		ChangeType: event.SchemaChange.ChangeType,
		Table:      event.SchemaChange.Table,
		Status:     "applied",
		Reason:     "Applied automatically by the CDC sink's additive schema reconciler",
		DDLApplied: event.SchemaChange.DDL,
	}

	if a.db != nil {
		// status='applied' + applied_at, never 'pending': there is nothing to approve.
		// ON CONFLICT keeps the UNIQUE (pipeline_id, ddl) row authoritative if the same
		// DDL was previously queued — a change that is live in the destination must not
		// keep rendering as a pending decision.
		if _, err := a.db.ExecContext(ctx, `
			INSERT INTO schema_change_approvals (
				pipeline_id, change_type, table_name, ddl,
				reasoning, status, reviewed_by, reviewed_at, applied_at, created_at
			) VALUES ($1, $2, $3, $4, $5, 'applied', $6, NOW(), NOW(), NOW())
			ON CONFLICT (pipeline_id, ddl) DO UPDATE SET
				status = 'applied',
				reasoning = EXCLUDED.reasoning,
				applied_at = NOW(),
				error_message = NULL,
				updated_at = NOW()
		`,
			event.PipelineID,
			event.SchemaChange.ChangeType,
			event.SchemaChange.Table,
			event.SchemaChange.DDL,
			result.Reason,
			appliedByCDCSink,
		); err != nil {
			log.Errorf("[HealerAgent] Failed to record already-applied schema change: %v", err)
		}

		a.recordHealingResult(ctx, result)
	}

	a.publishHealingResult(result)

	// Tell the user. Category=schema_drift deep-links the notification to the schema
	// changes page, same as the batch path — the difference is the wording, which must
	// not ask for a decision that is no longer available.
	se := diagnose.FromDiagnosis(
		diagnose.Diagnosis{Category: diagnose.CategorySchemaDrift},
		diagnose.Signal{PipelineID: event.PipelineID},
	)
	se.UserMessage = llmscrub.Scrub(
		schemaDriftNotificationText(event.SchemaChange.ChangeType, event.SchemaChange.Table, true))
	a.notifyUserStructured(event.PipelineID, se)

	log.Infof("[HealerAgent] Recorded already-applied change: %s on %s (pipeline %s)",
		event.SchemaChange.ChangeType, event.SchemaChange.Table, event.PipelineID)
	return nil
}

// schemaDriftNotificationText renders the user-facing drift notification: the applied
// variant for a change CDC has already made, the review variant for one waiting on a
// human. Both are deliberately APOSTROPHE-FREE, and that is load-bearing, not style.
//
// Every notification body passes through llmscrub.Scrub, whose dangling-quote rule is
// fail-closed: a log pipeline can truncate a quoted literal open, so an unpaired ' masks
// everything after it. That is right for error text carrying row values and wrong for
// author-written copy — the previous sentence ended "…in the pipeline's Schema Changes
// tab" and reached users as "…in the pipeline'[redacted]", cutting off the one
// instruction the notification existed to give.
//
// Both variants also name the SCHEMA CHANGES PAGE, not a tab. The apostrophe fix above
// preserved the words "Schema Changes tab", and no such tab exists: the pipeline detail
// renders exactly six (Overview, Execution History, Steps/DAG, Table statistics,
// Transforms, Data flow). Drift lives on its own route, /pipelines/{id}/schema-changes,
// which this alert already deep-links (Category=schema_drift ⇒ actionURLForError). So the
// sentence survived the scrubber and still could not be followed — the user reaches the
// pipeline, counts six tabs, and stops. Naming the alert's own Review change link is the
// reliable direction: it is the one route in that does not depend on the amber drift
// badge, which renders only while something is pending.
//
// Extracted so TestDriftNotificationCopySurvivesScrub can assert the rendered sentence
// passes through Scrub unchanged. Weakening the scrubber instead would trade a user
// data-leak guard for a copy nit, and would have to be mirrored in the Python scrubber.
func schemaDriftNotificationText(changeType, table string, applied bool) string {
	if applied {
		return fmt.Sprintf(
			"Schema change applied automatically: %s on %s. CDC pipelines apply new columns as they appear, so no approval was required. It is recorded on the schema changes page for this pipeline, which the Review change link opens.",
			changeType, table)
	}
	return fmt.Sprintf(
		"Schema change detected: %s on %s. Review and approve it on the schema changes page for this pipeline, which the Review change link opens.",
		changeType, table)
}

// schemaDriftDedupSubject builds the per-event identity the notifier folds into its
// dedup key (diagnose.StructuredError.DedupSubject).
//
// It deliberately does NOT include the DDL. Two facts about the same drift can reach
// here with differently-rendered DDL — the batch detector's synthesized statement and
// the CDC path's normalized one — and keying on it would file both. change_type +
// table + column is the smallest tuple that separates two genuinely different drifts
// while still collapsing retries of one.
func schemaDriftDedupSubject(sc SchemaChange) string {
	parts := []string{
		strings.ToLower(strings.TrimSpace(sc.ChangeType)),
		strings.ToLower(strings.TrimSpace(sc.Table)),
		strings.ToLower(strings.TrimSpace(sc.ColumnName)),
	}
	subject := strings.Join(parts, ":")
	if subject == "::" {
		// Nothing identifying at all — fall back to the old whole-family behavior
		// rather than inventing a key that splits every retry into its own row.
		return ""
	}
	return subject
}

// appliedByCDCSink is the reviewed_by attribution for a change no human reviewed.
// Naming the actor keeps the schema changes page honest: the row says who applied it,
// and it was not the user.
const appliedByCDCSink = "system:cdc-sink"

func (a *Agent) storePendingApproval(ctx context.Context, event *SchemaChangeEvent, analysis *LLMAnalysisResponse) {
	_, err := a.db.ExecContext(ctx, `
		INSERT INTO schema_change_approvals (
			pipeline_id, change_type, table_name, ddl, 
			reasoning, risks, status, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, 'pending', NOW())
		ON CONFLICT (pipeline_id, ddl) DO UPDATE SET
			reasoning = EXCLUDED.reasoning,
			status = 'pending',
			updated_at = NOW()
	`,
		event.PipelineID,
		event.SchemaChange.ChangeType,
		event.SchemaChange.Table,
		event.SchemaChange.DDL,
		analysis.Reasoning,
		fmt.Sprintf("%v", analysis.Risks),
	)

	if err != nil {
		log.Errorf("[HealerAgent] Failed to store pending approval: %v", err)
	}
}

// notifyUser sends a notification with just a string message.
//
// This is the legacy path kept for backward compatibility with the 12+
// existing callers. NEW code should use notifyUserStructured, which
// produces a full StructuredError envelope with a code + remediation.
// Migrate callers incrementally; each migration improves the user's
// notification UX from "Pipeline failed: <raw error>" to
// "MYSQL_BINLOG_FORMAT_NOT_ROW: here's the SQL to fix it".
func (a *Agent) notifyUser(pipelineID, message string) {
	// Scrub row values / PII before the message is persisted to
	// pipeline_notifications and shown to the user. Raw DB/sink errors carry
	// literal row data ("Failing row contains (...)", "Key (col)=(val)",
	// "Duplicate entry 'jane@acme.com'"), which must never reach a user-facing
	// or persisted notification.
	message = llmscrub.Scrub(message)
	// Wrap the legacy string into a minimal StructuredError so the
	// notifier consumer always sees a consistent envelope shape on the
	// wire. failure_type=system_error + audience=user is the safe
	// default for legacy callers that don't classify the failure.
	se := diagnose.NewStructuredError(
		"LEGACY_UNCLASSIFIED",
		diagnose.FailureTypeSystemError,
		diagnose.AudienceUser,
		message,
	)
	se.PipelineID = pipelineID
	se.InternalMessage = message
	a.notifyUserStructured(pipelineID, se)
}

// actionURLForError returns the RELATIVE frontend deep-link path for a
// notification. Schema-drift errors link straight to the pipeline's schema
// changes page (where the approve/reject cards render); every other error
// links to the pipeline overview. The path is intentionally relative — the
// notifier consumer prefixes the absolute host (APP_BASE_URL) at delivery
// time, so persisted rows stay host-agnostic.
func actionURLForError(pipelineID string, se *diagnose.StructuredError) string {
	if se != nil && se.Category == diagnose.CategorySchemaDrift {
		return fmt.Sprintf("/pipelines/%s/schema-changes", pipelineID)
	}
	return fmt.Sprintf("/pipelines/%s", pipelineID)
}

// notifyUserStructured emits a notification containing a full
// StructuredError envelope. This is the preferred path — callers that
// have a Diagnosis should build the StructuredError via
// diagnose.FromDiagnosis() and pass it here.
//
// Wire format (Kafka topic rsync.notifications):
//
//	{
//	  "type": "structured_error_notification",
//	  "pipeline_id": "...",
//	  "timestamp": "RFC3339",
//	  "action_url": "/pipelines/{id}",
//	  "error": <StructuredError JSON>,
//	  // legacy fields preserved for old consumers:
//	  "message": "<user_message>",
//	}
func (a *Agent) notifyUserStructured(pipelineID string, se *diagnose.StructuredError) {
	if se == nil {
		return
	}
	if se.PipelineID == "" {
		se.PipelineID = pipelineID
	}
	actionURL := actionURLForError(pipelineID, se)

	notification := map[string]interface{}{
		"type":        "structured_error_notification",
		"pipeline_id": pipelineID,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"action_url":  actionURL,
		"error":       se,
		// Backward-compatibility: legacy consumers still read "message".
		"message": se.UserMessage,
	}

	notificationBytes, err := json.Marshal(notification)
	if err != nil {
		log.Errorf("[HealerAgent] Failed to marshal notification: %v", err)
		return
	}

	if a.kafkaManager != nil {
		if err := a.kafkaManager.Produce(NotifyTopic, nil, notificationBytes); err != nil {
			log.Errorf("[HealerAgent] Failed to publish notification: %v", err)
		}
	}

	log.WithFields(log.Fields{
		"pipeline_id":  pipelineID,
		"code":         se.Code,
		"failure_type": se.FailureType,
		"audience":     se.Audience,
	}).Infof("[HealerAgent] Sent structured notification: %s", se.UserMessage)
}

func (a *Agent) recordHealingResult(ctx context.Context, result *HealingResult) {
	_, err := a.db.ExecContext(ctx, `
		INSERT INTO healing_history (
			pipeline_id, change_type, table_name, 
			status, reason, ddl_applied, error_message, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
	`,
		result.PipelineID,
		result.ChangeType,
		result.Table,
		result.Status,
		result.Reason,
		result.DDLApplied,
		result.ErrorMessage,
	)

	if err != nil {
		log.Errorf("[HealerAgent] Failed to record healing result: %v", err)
	}

	// F-Obs-2: record the schema-change healing action by change type and
	// resolved outcome so the SigNoz "Healer actions by outcome" panel and
	// the escalation alert have data. Both labels are bounded enums.
	appmetrics.HealerActionsTotal.
		WithLabelValues(result.ChangeType, healingOutcome(result.Status)).
		Inc()
}

// healingOutcome maps a HealingResult.Status to the bounded outcome label
// used by rsync_healer_actions_total (success|failure|escalated|skipped).
// pending_approval means automated recovery deferred to a human → escalated.
func healingOutcome(status string) string {
	switch status {
	case "applied":
		return "success"
	case "failed":
		return "failure"
	case "pending_approval":
		return "escalated"
	default:
		return "skipped"
	}
}

func (a *Agent) publishHealingResult(result *HealingResult) {
	resultBytes, err := json.Marshal(result)
	if err != nil {
		log.Errorf("[HealerAgent] Failed to marshal result: %v", err)
		return
	}

	if a.kafkaManager != nil {
		if err := a.kafkaManager.Produce(ResultsTopic, nil, resultBytes); err != nil {
			log.Errorf("[HealerAgent] Failed to publish result: %v", err)
		}
	}
}
