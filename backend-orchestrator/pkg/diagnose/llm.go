// llm.go — v2 LLM-backed Diagnoser, chained BEHIND the rules.
//
// ChainDiagnoser runs the RuleBasedDiagnoser first, always. The LLM is
// consulted only when the rules return CategoryUnknown — so every error
// class the rules recognise (CDC provisioning failures in particular,
// which MUST escalate per the CLAUDE.md rule) structurally never
// reaches the LLM.
//
// Safety rails on the LLM result:
//   - category/action are validated against the closed enums; anything
//     outside them discards the LLM answer and keeps the rules result.
//   - confidence is clamped to [0, llmConfidenceCap] where the cap sits
//     below heal's AutoBand (0.85) — the LLM can suggest, but can never
//     trigger auto-execution on its own.
//   - any transport/decode/validation error falls back to the rules
//     result; the healer path never fails because the LLM did.
//
// Privacy: signal.ErrorMessage passes llmscrub.ScrubMax before it enters
// the prompt. Everything else in the prompt is metadata (connector
// types, stage, row counts) — never row values, credentials, or PII.
package diagnose

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
	log "github.com/sirupsen/logrus"
)

const (
	defaultLLMServiceURL = "http://llm-service:5000"
	llmRequestTimeout    = 15 * time.Second
	// Max runes of scrubbed error text included in the prompt.
	llmErrorMessageCap = 1500
	// Cap the LLM's confidence just below the auto-execute band so an LLM
	// diagnosis can reach HITL but never auto-execute. Derived from the single
	// source of truth AutoExecuteBand (0.85) so it can never drift above it;
	// TestLLMConfidenceCapBelowAutoExecuteBand fails closed if it ever does.
	llmConfidenceCap = AutoExecuteBand - 0.01
)

var (
	validLLMCategories = map[Category]bool{
		CategoryAuthExpired:  true,
		CategoryAuthScope:    true,
		CategoryRateLimit:    true,
		CategorySchemaDrift:  true,
		CategoryNetwork:      true,
		CategoryConnectorBug: true,
		CategoryDestCapacity: true,
		CategoryUserConfig:   true,
		CategoryUnknown:      true,
	}
	validLLMActions = map[Action]bool{
		ActionRegenerateConnector: true,
		ActionRefreshAuth:         true,
		ActionBackoffRetry:        true,
		ActionRequestUserConfig:   true,
		ActionEscalate:            true,
		ActionNoOp:                true,
	}

	// Guards the one-line "which diagnoser did we build" log in NewFromEnv.
	// Both production call sites construct a Diagnoser per sweep
	// (internal/agents/heal/worker.go and .../issue_sweep.go), so an
	// unconditional log would repeat on every sweep tick.
	diagnoserModeLogOnce sync.Once
)

// LLMDiagnoser classifies a Signal by calling the llm-service
// OpenAI-compatible chat endpoint. It satisfies Diagnoser, but callers
// should normally use it via ChainDiagnoser so the rules run first.
type LLMDiagnoser struct {
	BaseURL    string
	Model      string
	HTTPClient *http.Client
}

// NewLLMDiagnoserFromEnv builds an LLMDiagnoser from env vars. This is the
// single, canonical LLM error-diagnosis path — it replaced a second, divergent
// LLM classifier that used to live in the healer's dead DLQ consumer.
func NewLLMDiagnoserFromEnv() *LLMDiagnoser {
	// Diagnoser-specific override FIRST, mirroring the model resolution below.
	// LLM_SERVICE_URL is a shared name with two consumers that disagree:
	// internal/workers/intent.go reads it with a fallback of
	// http://planner:5011, while this diagnoser falls back to
	// http://llm-service:5000. Neither is set anywhere today, so both run on
	// their own default — and an operator who sets the shared name to steer one
	// of them would silently repoint the other. RSYNC_LLM_DIAGNOSER_URL steers
	// only this one. Behaviour is unchanged when it is unset or empty.
	baseURL := os.Getenv("RSYNC_LLM_DIAGNOSER_URL")
	if baseURL == "" {
		baseURL = os.Getenv("LLM_SERVICE_URL")
	}
	if baseURL == "" {
		baseURL = defaultLLMServiceURL
	}
	model := os.Getenv("RSYNC_LLM_DIAGNOSER_MODEL")
	if model == "" {
		model = os.Getenv("LLM_MODEL")
	}
	if model == "" {
		model = "gpt-4o-mini"
	}
	return &LLMDiagnoser{
		BaseURL:    baseURL,
		Model:      model,
		HTTPClient: &http.Client{Timeout: llmRequestTimeout},
	}
}

// Diagnose satisfies the Diagnoser interface. On any LLM failure it
// returns the same low-confidence unknown fallback the rules produce.
func (d *LLMDiagnoser) Diagnose(signal Signal) Diagnosis {
	dx, err := d.diagnoseLLM(signal)
	if err != nil {
		log.WithError(err).Warn("diagnose: LLM diagnoser failed")
		return Diagnosis{
			Category:        CategoryUnknown,
			SuggestedAction: ActionEscalate,
			Confidence:      0.3,
			Rationale:       "no rule matched; human inspection required",
		}
	}
	return dx
}

// llmDiagnosisResponse is the JSON object the model is instructed to emit.
type llmDiagnosisResponse struct {
	Category   string  `json:"category"`
	Action     string  `json:"action"`
	Confidence float64 `json:"confidence"`
	Rationale  string  `json:"rationale"`
}

func (d *LLMDiagnoser) diagnoseLLM(signal Signal) (Diagnosis, error) {
	ctx, cancel := context.WithTimeout(context.Background(), llmRequestTimeout)
	defer cancel()

	messages := []map[string]string{
		{
			"role":    "system",
			"content": llmDiagnoseSystemPrompt(),
		},
		{
			"role":    "user",
			"content": buildLLMDiagnosePrompt(signal),
		},
	}
	reqBody, err := json.Marshal(map[string]interface{}{
		"model":       d.Model,
		"messages":    messages,
		"temperature": 0.1,
		"response_format": map[string]string{
			"type": "json_object",
		},
	})
	if err != nil {
		return Diagnosis{}, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", d.BaseURL+"/v1/chat/completions", bytes.NewBuffer(reqBody))
	if err != nil {
		return Diagnosis{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := d.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: llmRequestTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return Diagnosis{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return Diagnosis{}, fmt.Errorf("LLM returned status %d: %s", resp.StatusCode, string(body))
	}

	// OpenAI-compatible envelope: { "choices": [ { "message": { "content": "..." } } ] }
	var llmResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&llmResp); err != nil {
		return Diagnosis{}, fmt.Errorf("failed to decode LLM response: %w", err)
	}
	if len(llmResp.Choices) == 0 {
		return Diagnosis{}, fmt.Errorf("invalid LLM response: no choices")
	}

	var parsed llmDiagnosisResponse
	if err := json.Unmarshal([]byte(llmResp.Choices[0].Message.Content), &parsed); err != nil {
		return Diagnosis{}, fmt.Errorf("failed to parse LLM diagnosis: %w", err)
	}

	category := Category(parsed.Category)
	if !validLLMCategories[category] {
		return Diagnosis{}, fmt.Errorf("LLM returned unknown category %q", parsed.Category)
	}
	action := Action(parsed.Action)
	if !validLLMActions[action] {
		return Diagnosis{}, fmt.Errorf("LLM returned unknown action %q", parsed.Action)
	}

	confidence := parsed.Confidence
	if confidence < 0 {
		confidence = 0
	}
	if confidence > llmConfidenceCap {
		confidence = llmConfidenceCap
	}

	rationale := parsed.Rationale
	if rationale == "" {
		rationale = "classified by LLM diagnoser"
	}

	return Diagnosis{
		Category:        category,
		SuggestedAction: action,
		Confidence:      confidence,
		Rationale:       "llm: " + rationale,
	}, nil
}

func llmDiagnoseSystemPrompt() string {
	return `You are a data-pipeline reliability engineer. Classify a pipeline execution failure into exactly one category and recommend exactly one recovery action, in strict JSON.

Valid categories: auth_expired, auth_scope, rate_limit, schema_drift, network, connector_bug, dest_capacity, user_config, unknown.
Valid actions: regenerate_connector, refresh_auth, backoff_retry, request_user_config, escalate_to_human, no_op.

Respond with a JSON object: {"category": "...", "action": "...", "confidence": 0.0-1.0, "rationale": "one line"}.
Confidence is your certainty the category is correct. When uncertain, use category "unknown" with action "escalate_to_human" and low confidence.`
}

// buildLLMDiagnosePrompt renders the user message. The error message is
// scrubbed + truncated (privacy contract: schema metadata and user text
// only — never row values, credentials, or PII); everything else here
// is metadata.
func buildLLMDiagnosePrompt(signal Signal) string {
	return fmt.Sprintf(`Classify this pipeline execution failure.

- Stage: %s
- Executor status: %s
- Source connector type: %s
- Destination connector type: %s
- Source row count: %d
- Rows written to destination: %d
- Tables that read rows but landed none: %d of %d tables reporting
- Error message (sensitive values redacted):
%s`,
		// Closed-set metadata today ("executor"/"sentinel", an execution
		// status, connector type names), so Scrub is a no-op on every real
		// value. Applied anyway so the metadata-only privacy claim holds by
		// construction rather than depending on every future caller keeping
		// these fields closed-set.
		llmscrub.Scrub(signal.Stage),
		llmscrub.Scrub(signal.ExecutorStatus),
		llmscrub.Scrub(signal.SourceType),
		llmscrub.Scrub(signal.DestinationType),
		signal.SourceRowCount,
		signal.WrittenRows,
		// Counts only — no table names. The prompt stays metadata-only per the
		// LLM data-privacy rule; a count cannot carry a row value or a PII column.
		signal.TablesWithNoLandedRows,
		signal.TablesObserved,
		llmscrub.ScrubMax(signal.ErrorMessage, llmErrorMessageCap),
	)
}

// ChainDiagnoser — rules first, LLM only for what the rules can't name.
type ChainDiagnoser struct {
	Rules Diagnoser
	LLM   *LLMDiagnoser
}

// Diagnose runs the rules and returns their result whenever they
// produced a real classification. Only a CategoryUnknown result is
// handed to the LLM; on any LLM failure the rules result stands.
func (c *ChainDiagnoser) Diagnose(signal Signal) Diagnosis {
	rules := c.Rules.Diagnose(signal)
	if rules.Category != CategoryUnknown || c.LLM == nil {
		return rules
	}
	dx, err := c.LLM.diagnoseLLM(signal)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id":  signal.PipelineID,
			"execution_id": signal.ExecutionID,
		}).Warn("diagnose: LLM diagnoser failed; keeping rules result")
		return rules
	}
	log.WithFields(log.Fields{
		"pipeline_id":  signal.PipelineID,
		"execution_id": signal.ExecutionID,
		"category":     dx.Category,
		"action":       dx.SuggestedAction,
		"confidence":   dx.Confidence,
	}).Debug("diagnose: LLM diagnoser response")
	return dx
}

// NewFromEnv returns the production Diagnoser. With
// RSYNC_LLM_DIAGNOSER_ENABLED=true it chains the LLM diagnoser behind
// the rules; otherwise (the default) it returns the rules alone, so no
// HTTP client is ever constructed.
//
// Which of the two was built is logged ONCE per process, at Info. Without
// that line the flag is invisible from outside: a rules-only orchestrator and
// a rules+LLM one are indistinguishable in the logs until something fails.
func NewFromEnv() Diagnoser {
	enabled := os.Getenv("RSYNC_LLM_DIAGNOSER_ENABLED") == "true"
	diagnoserModeLogOnce.Do(func() {
		mode := "rules-only"
		if enabled {
			mode = "rules+llm"
		}
		log.WithFields(log.Fields{
			"diagnoser": mode,
			"flag":      "RSYNC_LLM_DIAGNOSER_ENABLED",
			"enabled":   enabled,
		}).Info("diagnose: constructed diagnoser")
	})
	if !enabled {
		return New()
	}
	return &ChainDiagnoser{
		Rules: New(),
		LLM:   NewLLMDiagnoserFromEnv(),
	}
}
