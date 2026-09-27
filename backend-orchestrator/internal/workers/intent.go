package workers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/telemetry"
	"github.com/rsync-ai/shared/correlation"
)

// ==============================================================================
// INTENT WORKER - PURELY AGENTIC & GENERIC
// ==============================================================================
// This worker follows the Temporal/Conductor pattern:
// 1. Claims "intent" requests from the Redis correlation store (startRedisPoller)
// 2. Makes autonomous decisions (calls LLM to parse intent)
// 3. Writes the result back to the correlation store (RouteResult)
// 4. NEVER emits domain events (only orchestrator does)
// 5. Is STATELESS and horizontally scalable
// 6. Is GENERIC (works with ANY natural language request)
// ==============================================================================

// IntentWorker parses natural language requests into structured intent
type IntentWorker struct {
	kafkaManager      *kafka.Manager
	llmServiceURL     string
	httpClient        *http.Client
	ctx               context.Context
	cancel            context.CancelFunc
	tracer            trace.Tracer
	progressEmitter   *ProgressEmitter
	correlationClient *correlation.Client
	workerID          string
}

// NewIntentWorker creates a new intent worker
func NewIntentWorker(kafkaManager *kafka.Manager, db *sql.DB) *IntentWorker {
	log.Info("🔧 IntentWorker: NewIntentWorker() called")
	ctx, cancel := context.WithCancel(context.Background())

	llmServiceURL := os.Getenv("LLM_SERVICE_URL")
	if llmServiceURL == "" {
		llmServiceURL = "http://planner:5011"
	}

	// Initialize correlation client for V2 workflows
	redisAddr := os.Getenv("REDIS_ADDRESS")
	if redisAddr == "" {
		redisAddr = os.Getenv("REDIS_ADDR")
	}
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")
	correlationClient, err := correlation.NewClient(redisAddr, redisPassword)
	if err != nil {
		log.WithError(err).Warn("Failed to initialize correlation client for Intent worker")
	}

	w := &IntentWorker{
		kafkaManager:      kafkaManager,
		llmServiceURL:     llmServiceURL,
		progressEmitter:   NewProgressEmitter(kafkaManager, db),
		correlationClient: correlationClient,
		workerID:          fmt.Sprintf("intent-worker-%s", uuid.New().String()[:8]),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		ctx:    ctx,
		cancel: cancel,
		tracer: otel.Tracer("intent-worker"),
	}

	log.Info("✅ IntentWorker: Initialized")
	return w
}

// Execute processes a task and returns a result
// This is the ONLY method that implements the Worker interface
func (w *IntentWorker) Execute(ctx context.Context, task Task) TaskResult {
	ctx, span := w.tracer.Start(ctx, "intent.execute",
		trace.WithAttributes(
			attribute.String("task_id", task.TaskID),
			attribute.String("pipeline_id", task.PipelineID),
		),
	)
	defer span.End()

	log.WithFields(log.Fields{
		"task_id":     task.TaskID,
		"pipeline_id": task.PipelineID,
		"user_id":     task.UserID,
	}).Info("🧠 Intent Worker: Processing task")

	// Emit STAGE_STARTED event
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_STARTED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Understanding your data movement request",
		Summary:     "Understanding request",
		Progress: ProgressInfo{
			Percent:     10,
			CurrentStep: 1,
			TotalSteps:  7,
			Stage:       "intent",
		},
	})

	// Heartbeats while intent parsing is running (5–10s)
	stopHeartbeat := w.progressEmitter.StartStageHeartbeat(ctx, ProgressEvent{
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     "Still understanding your request…",
		Summary:     "Understanding request",
		Progress: ProgressInfo{
			Percent:     10,
			CurrentStep: 1,
			TotalSteps:  7,
			Stage:       "intent",
		},
	}, 7*time.Second)
	defer stopHeartbeat()

	// Extract user request from payload
	userRequest, ok := task.Payload["user_request"].(string)
	if !ok || userRequest == "" {
		err := fmt.Errorf("missing or invalid user_request in task payload")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return TaskResult{
			TaskID:      task.TaskID,
			WorkflowID:  task.WorkflowID,
			StepID:      task.StepID,
			Status:      "failed",
			Error:       err.Error(),
			CompletedAt: time.Now(),
			TraceID:     task.TraceID,
		}
	}

	// Call LLM to parse intent (AUTONOMOUS DECISION)
	intent, err := w.parseIntent(ctx, userRequest, task.TraceID)
	if err != nil {
		// Stop heartbeats immediately on failure to avoid late STAGE_PROGRESS overwriting newer stages.
		stopHeartbeat()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		log.WithError(err).Error("❌ Intent Worker: Failed to parse intent")

		// Emit STAGE_FAILED event
		w.progressEmitter.EmitProgress(ctx, ProgressEvent{
			EventType:   "STAGE_FAILED",
			PipelineID:  task.PipelineID,
			ExecutionID: getExecutionID(task),
			Status:      "failed",
			Message:     fmt.Sprintf("Failed to understand request: %v", err),
			Progress: ProgressInfo{
				Percent:     10,
				CurrentStep: 1,
				TotalSteps:  7,
				Stage:       "intent",
			},
		})

		return TaskResult{
			TaskID:      task.TaskID,
			WorkflowID:  task.WorkflowID,
			StepID:      task.StepID,
			Status:      "failed",
			Error:       fmt.Sprintf("failed to parse intent: %v", err),
			CompletedAt: time.Now(),
			TraceID:     task.TraceID,
		}
	}

	// Return structured intent (GENERIC OUTPUT)
	span.SetStatus(codes.Ok, "Intent parsed successfully")
	log.WithFields(log.Fields{
		"task_id":              task.TaskID,
		"source_category":      intent["source_category"],
		"destination_category": intent["destination_category"],
	}).Info("✅ Intent Worker: Successfully parsed intent")

	// Stop heartbeats immediately on success to avoid late STAGE_PROGRESS overwriting newer stages.
	stopHeartbeat()

	// Emit STAGE_COMPLETED event
	sourceType := fmt.Sprintf("%v", intent["source_category"])
	destType := fmt.Sprintf("%v", intent["destination_category"])
	w.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_COMPLETED",
		PipelineID:  task.PipelineID,
		ExecutionID: getExecutionID(task),
		Status:      "processing",
		Message:     fmt.Sprintf("Understood: sync %s to %s", sourceType, destType),
		Summary:     "Intent parsed",
		Progress: ProgressInfo{
			Percent:     15,
			CurrentStep: 1,
			TotalSteps:  7,
			Stage:       "intent",
		},
		Metadata: map[string]interface{}{
			"source_type":      sourceType,
			"destination_type": destType,
			"confidence":       intent["confidence"],
			"artifacts": map[string]interface{}{
				"intent": map[string]interface{}{
					"version":      1,
					"generated_by": "intent",
					"timestamp":    time.Now().UTC().Format(time.RFC3339),
					"data": map[string]interface{}{
						"intent": intent,
					},
				},
			},
		},
	})

	return TaskResult{
		TaskID:     task.TaskID,
		WorkflowID: task.WorkflowID,
		StepID:     task.StepID,
		Status:     "success",
		Output: map[string]interface{}{
			"intent":               intent,
			"source_category":      intent["source_category"],
			"source_hints":         intent["source_hints"],
			"destination_category": intent["destination_category"],
			"destination_hints":    intent["destination_hints"],
			"entities_mentioned":   intent["entities_mentioned"],
			"confidence":           intent["confidence"],
		},
		NextAction:  "resolver", // Agent suggests next step
		CompletedAt: time.Now(),
		TraceID:     task.TraceID,
	}
}

// parseIntent calls the LLM service to parse natural language
// This is where the AUTONOMOUS DECISION happens
func (w *IntentWorker) parseIntent(ctx context.Context, userRequest string, traceID string) (map[string]interface{}, error) {
	ctx, span := w.tracer.Start(ctx, "intent.llm_call",
		trace.WithAttributes(
			attribute.String("trace_id", traceID),
		),
	)
	defer span.End()

	// =====================================================================
	// Fast-path: explicit "X to Y" connector intent extraction
	// =====================================================================
	// Root cause fix:
	// - Previously we relied on planner /plan steps to infer source/destination tools.
	// - When tools weren't installed (or typed in lowercase), the planner could fall back to mysql/s3,
	//   and even treat "postgresql" as a table name, leading to wrong table selection UI.
	// Here we deterministically extract connector names when the user explicitly names them.
	if src, dst, ok := extractExplicitSourceDest(userRequest); ok {
		intent := map[string]interface{}{
			"source_category":      src,
			"destination_category": dst,
			"source_hints":         []string{},
			"destination_hints":    []string{},
			"entities_mentioned":   []string{},
			"confidence":           0.98,
			"original_plan": map[string]interface{}{
				"steps": []interface{}{
					map[string]interface{}{"tool": src, "params": map[string]interface{}{}},
					map[string]interface{}{"tool": dst, "params": map[string]interface{}{}},
				},
			},
		}
		return intent, nil
	}

	// Prepare LLM request (POST /plan expects "natural_language" field)
	llmRequest := map[string]interface{}{
		"natural_language": userRequest,
	}

	reqBody, err := json.Marshal(llmRequest)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to marshal LLM request: %w", err)
	}

	// Call LLM service (using /plan endpoint which handles intent parsing)
	req, err := http.NewRequestWithContext(ctx, "POST", w.llmServiceURL+"/plan", bytes.NewBuffer(reqBody))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range telemetry.InjectTraceToHeaders(ctx) {
		req.Header.Set(k, v)
	}

	startTime := time.Now()
	resp, err := w.httpClient.Do(req)
	span.SetAttributes(attribute.Int64("llm.latency_ms", time.Since(startTime).Milliseconds()))

	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("LLM request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		err := fmt.Errorf("LLM returned status %d: %s", resp.StatusCode, string(body))
		span.RecordError(err)
		return nil, err
	}

	// Parse LLM response (POST /plan returns plan, not intent)
	var llmResponse map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&llmResponse); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("failed to decode LLM response: %w", err)
	}

	// Extract plan from response and transform to intent format
	plan, ok := llmResponse["plan"].(map[string]interface{})
	if !ok {
		// Check if there's an error field
		if errorMsg, exists := llmResponse["error"]; exists && errorMsg != nil {
			err := fmt.Errorf("LLM planning failed: %v", errorMsg)
			span.RecordError(err)
			return nil, err
		}
		err := fmt.Errorf("invalid LLM response: missing 'plan' field")
		span.RecordError(err)
		return nil, err
	}

	// Transform plan into intent format
	// The Intent Worker needs: source_category, destination_category, etc.
	// Extract from plan steps
	steps := plan["steps"].([]interface{})
	if len(steps) < 2 {
		return nil, fmt.Errorf("plan has insufficient steps")
	}

	sourceStep := steps[0].(map[string]interface{})
	destStep := steps[1].(map[string]interface{})

	intent := map[string]interface{}{
		"source_category":      sourceStep["tool"],
		"source_hints":         []string{fmt.Sprintf("%v", sourceStep["params"])},
		"destination_category": destStep["tool"],
		"destination_hints":    []string{fmt.Sprintf("%v", destStep["params"])},
		"entities_mentioned":   []string{},
		"confidence":           0.9,
		// Include original plan for downstream agents
		"original_plan": plan,
	}

	span.SetStatus(codes.Ok, "Intent parsed successfully")
	return intent, nil
}

func normalizeConnectorAlias(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	switch s {
	case "postgres", "postgre", "pg":
		return "postgresql"
	case "sf":
		return "snowflake"
	case "s3", "aws-s3", "aws s3", "aws_s3":
		return "aws-s3"
	default:
		return s
	}
}

// knownConnectorIDs is the set of connector ids (and single-word aliases, after
// normalizeConnectorAlias) that we ship. It is used to guard UNANCHORED
// "X to Y" / "X -> Y" NL matches from capturing entity nouns (e.g. "products"
// in "shopify products to postgresql") as connector ids. (BUG-12)
var knownConnectorIDs = map[string]bool{
	// Databases / warehouses / storage
	"mysql": true, "postgresql": true, "mongodb": true, "aws-s3": true,
	"snowflake": true, "bigquery": true, "redshift": true, "kafka": true,
	"gcs": true, "azure-blob": true, "oracle": true, "sqlserver": true,
	"mssql": true, "sqlite": true, "minio": true,
	// SaaS / API connectors
	"shopify": true, "hubspot": true, "salesforce": true, "pipedrive": true,
	"zoho-crm": true, "zendesk": true, "freshdesk": true, "jira": true,
	"slack": true, "github": true, "notion": true, "google": true,
	"dropbox": true, "stripe": true, "mailchimp": true, "intercom": true,
}

func isKnownConnector(s string) bool {
	return knownConnectorIDs[strings.TrimSpace(strings.ToLower(s))]
}

// normalizeConnectorPhrases collapses common multi-word connector phrases into
// canonical connector ids (kebab-case). This prevents the fast-path extractor
// from incorrectly capturing only a prefix token (e.g. "aws" from "aws s3").
func normalizeConnectorPhrases(msg string) string {
	// Lowercase and normalize whitespace to single spaces.
	normalized := strings.ToLower(strings.TrimSpace(msg))
	normalized = strings.Join(strings.Fields(normalized), " ")

	// Canonicalize frequent multi-word connector phrases.
	// NOTE: Keep this focused on connectors we ship; prefer the canonical id.
	replacer := strings.NewReplacer(
		"aws s3", "aws-s3",
		"amazon s3", "aws-s3",
		"aws_s3", "aws-s3",
		"google cloud storage", "gcs",
		"azure blob storage", "azure-blob",
		"azure blob", "azure-blob",
	)
	return replacer.Replace(normalized)
}

// extractExplicitSourceDest detects explicit connector mentions in common NL patterns.
// Examples:
// - "sync postgresql to snowflake"
// - "migrate from postgres to snowflake"
func extractExplicitSourceDest(msg string) (string, string, bool) {
	// Normalize multi-word connector phrases first so regex token capture works.
	m := normalizeConnectorPhrases(msg)

	// ---------------------------------------------------------------------
	// Flexible "from X ... into/to Y" detection (CDC NL often mentions tables
	// between connectors, e.g. "from mysql table db.t into postgres table x").
	// ---------------------------------------------------------------------
	stop := map[string]bool{
		"data": true, "table": true, "tables": true, "database": true, "db": true, "schema": true,
		// Avoid treating cloud/provider prefixes as connector ids by themselves
		"aws": true, "amazon": true, "azure": true, "google": true,
		// Articles, possessives, and role nouns that NL phrasing places right
		// after from/to/into — never valid connector ids. Without this guard
		// `from the "shopify" source ... into the "..." destination` captured
		// ("the","the") and failed connector resolution downstream.
		"the": true, "a": true, "an": true, "my": true, "your": true, "our": true,
		"its": true, "their": true, "this": true, "that": true, "these": true, "those": true,
		"source": true, "destination": true, "dest": true, "sink": true, "target": true,
		"connection": true, "connections": true, "named": true, "called": true, "pipeline": true,
	}
	reFrom := regexp.MustCompile(`\bfrom\s+([a-z0-9][a-z0-9_-]+)\b`)
	reToInto := regexp.MustCompile(`\b(?:to|into)\s+([a-z0-9][a-z0-9_-]+)\b`)
	if loc := reFrom.FindStringSubmatchIndex(m); len(loc) == 4 {
		srcRaw := m[loc[2]:loc[3]]
		rest := m[loc[1]:]
		if loc2 := reToInto.FindStringSubmatchIndex(rest); len(loc2) == 4 {
			dstRaw := rest[loc2[2]:loc2[3]]
			src := normalizeConnectorAlias(srcRaw)
			dst := normalizeConnectorAlias(dstRaw)
			// Like the loose patterns below, only trust this unanchored match
			// when BOTH tokens are registered connector ids. Otherwise a phrase
			// like `from the "shopify" source ...` (where the token after "from"
			// is an article) falls through to proper connection resolution / HITL
			// instead of resolving a bogus connector id.
			if src != "" && dst != "" && !stop[src] && !stop[dst] &&
				isKnownConnector(src) && isKnownConnector(dst) {
				return src, dst, true
			}
		}
	}

	// Anchored patterns carry an explicit verb/preposition ("sync", "migrate
	// from", "from", "copy from") so the captured tokens are unambiguous user
	// intent — trust them as-is.
	anchored := []*regexp.Regexp{
		regexp.MustCompile(`\bsync\s+([a-z0-9][a-z0-9_-]+)\s+to\s+([a-z0-9][a-z0-9_-]+)\b`),
		regexp.MustCompile(`\bmigrate\s+from\s+([a-z0-9][a-z0-9_-]+)\s+to\s+([a-z0-9][a-z0-9_-]+)\b`),
		regexp.MustCompile(`\bfrom\s+([a-z0-9][a-z0-9_-]+)\s+to\s+([a-z0-9][a-z0-9_-]+)\b`),
		regexp.MustCompile(`\bcopy\s+from\s+([a-z0-9][a-z0-9_-]+)\s+to\s+([a-z0-9][a-z0-9_-]+)\b`),
	}
	// Loose shorthand patterns have no anchoring verb, e.g. "pipedrive to s3" or
	// "pipedrive -> s3". A noun phrase like "shopify products to postgresql"
	// would otherwise capture ("products","postgresql"). Guard them by requiring
	// BOTH normalized tokens to be registered connector ids; otherwise fall
	// through to the LLM resolver rather than guess. (BUG-12)
	loose := []*regexp.Regexp{
		regexp.MustCompile(`\b([a-z0-9][a-z0-9_-]+)\s+to\s+([a-z0-9][a-z0-9_-]+)\b`),
		regexp.MustCompile(`\b([a-z0-9][a-z0-9_-]+)\s*->\s*([a-z0-9][a-z0-9_-]+)\b`),
	}

	for _, re := range anchored {
		matches := re.FindStringSubmatch(m)
		if len(matches) == 3 {
			src := normalizeConnectorAlias(matches[1])
			dst := normalizeConnectorAlias(matches[2])
			if src == "" || dst == "" || stop[src] || stop[dst] {
				continue
			}
			return src, dst, true
		}
	}

	for _, re := range loose {
		matches := re.FindStringSubmatch(m)
		if len(matches) == 3 {
			src := normalizeConnectorAlias(matches[1])
			dst := normalizeConnectorAlias(matches[2])
			if src == "" || dst == "" || stop[src] || stop[dst] {
				continue
			}
			// Only trust an unanchored match when both sides are known connectors.
			if !isKnownConnector(src) || !isKnownConnector(dst) {
				continue
			}
			return src, dst, true
		}
	}
	return "", "", false
}

// Start launches the Redis correlation poller, the only way intent requests
// reach this worker (the Temporal adapter writes them to the correlation store).
func (w *IntentWorker) Start() error {
	if w.correlationClient == nil {
		log.Warn("⚠️  IntentWorker: Correlation client not initialized - V2 workflows will not work")
		return nil
	}
	go w.startRedisPoller()
	log.Info("✅ IntentWorker: Redis poller started for V2 correlation requests")
	return nil
}

// Stop gracefully shuts down the worker
func (w *IntentWorker) Stop() error {
	log.Info("🛑 Stopping Intent Worker")
	w.cancel()
	return nil
}

// getExecutionID extracts execution_id from task context
func getExecutionID(task Task) string {
	// First try task.ExecutionID (new field)
	if task.ExecutionID != "" {
		return task.ExecutionID
	}
	// Fallback to context (legacy)
	if execID, ok := task.Context["execution_id"].(string); ok {
		return execID
	}
	return ""
}
