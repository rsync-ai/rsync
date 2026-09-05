package heal

// executors.go — Phase 4 concrete Executor implementations.
//
// Each executor satisfies the Executor interface defined in heal.go and
// performs exactly one healing action. They are registered with a Healer
// via Healer.Register() in the HealWorker setup.
//
// Action → Executor mapping:
//
//	ActionRefreshAuth         → RefreshAuthExecutor
//	ActionBackoffRetry        → BackoffRetryExecutor
//	ActionRegenerateConnector → RegenerateConnectorExecutor
//	ActionRequestUserConfig   → RequestUserConfigExecutor
//
// All executors write back to the same shared Postgres DB the rest of
// the platform uses. BackoffRetry also makes an HTTP call to api-gateway
// to re-enqueue the Temporal workflow.

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rsync-ai/backend-orchestrator/pkg/diagnose"
	log "github.com/sirupsen/logrus"
)

// ── RefreshAuthExecutor ────────────────────────────────────────────────────
// Transitions execution to waiting_for_credential_reauth so the UI shows
// a "Reconnect credential" prompt. Does NOT re-run the pipeline — that
// happens after the user re-auths and clicks retry.

type RefreshAuthExecutor struct{ DB *sql.DB }

func (e *RefreshAuthExecutor) Action() diagnose.Action { return diagnose.ActionRefreshAuth }
func (e *RefreshAuthExecutor) HITLSafe() bool           { return true }

func (e *RefreshAuthExecutor) Run(ctx context.Context, sig diagnose.Signal) error {
	if sig.ExecutionID == "" {
		return fmt.Errorf("RefreshAuthExecutor: ExecutionID is required")
	}
	_, err := e.DB.ExecContext(ctx, `
		UPDATE executions
		   SET status        = 'waiting_for_credential_reauth',
		       error_message = CASE WHEN error_message IS NULL OR error_message = ''
		                            THEN 'waiting_for_credential_reauth: healer detected auth expiry'
		                            ELSE 'waiting_for_credential_reauth: ' || error_message
		                       END,
		       end_time      = NULL
		 WHERE id = $1
	`, sig.ExecutionID)
	if err != nil {
		return fmt.Errorf("RefreshAuthExecutor: db update: %w", err)
	}
	writeHealEvent(ctx, e.DB, sig, "healer_refresh_auth",
		"Healer requested credential re-authentication")
	log.WithFields(log.Fields{
		"execution_id": sig.ExecutionID,
		"pipeline_id":  sig.PipelineID,
	}).Info("healer: RefreshAuth — execution awaiting re-auth")
	return nil
}

// ── BackoffRetryExecutor ──────────────────────────────────────────────────
// Re-enqueues the pipeline via the api-gateway run endpoint. Used for
// transient failures (rate-limit, network blip) where a fresh attempt is
// the right first move.

type BackoffRetryExecutor struct {
	DB             *sql.DB
	HTTPClient     *http.Client
	APIGatewayURL  string // e.g. "http://api-gateway:8080"
}

func (e *BackoffRetryExecutor) Action() diagnose.Action { return diagnose.ActionBackoffRetry }
func (e *BackoffRetryExecutor) HITLSafe() bool           { return false }

// maxHealerRetriesPerPipeline is the maximum number of times the healer will
// automatically re-enqueue a pipeline within the lookbackWindow. Once the cap
// is reached the healer escalates instead of retrying so operators see the
// failure rather than it spinning silently.
const (
	maxHealerRetriesPerPipeline = 3
	retryLookbackWindow         = "24 hours"
)

// backoffRetryDelay is the pause before the healer re-enqueues a pipeline, giving
// a transient upstream (rate-limit, network blip) a moment to recover. A package
// var rather than a const so tests can shrink it to zero; production keeps 5s.
var backoffRetryDelay = 5 * time.Second

func (e *BackoffRetryExecutor) Run(ctx context.Context, sig diagnose.Signal) error {
	if sig.PipelineID == "" {
		return fmt.Errorf("BackoffRetryExecutor: PipelineID is required")
	}

	// Guard: check how many times we have already auto-retried this pipeline
	// via the healer. If we've hit the cap, escalate instead of spinning forever.
	if e.DB != nil {
		var recentRetries int
		err := e.DB.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM pipeline_run_events
			WHERE pipeline_id  = $1
			  AND event_type   = 'healer_backoff_retry'
			  AND created_at  >= NOW() - $2::interval
		`, sig.PipelineID, retryLookbackWindow).Scan(&recentRetries)
		if err == nil && recentRetries >= maxHealerRetriesPerPipeline {
			desc := fmt.Sprintf(
				"Healer has already retried this pipeline %d times in the last %s — automatic retry cap reached. "+
					"Please investigate the root cause and resume the pipeline manually.",
				recentRetries, retryLookbackWindow,
			)
			writeHealEvent(ctx, e.DB, sig, "healer_retry_cap_reached", desc)
			log.WithFields(log.Fields{
				"pipeline_id":   sig.PipelineID,
				"execution_id":  sig.ExecutionID,
				"recent_retries": recentRetries,
			}).Warn("healer: BackoffRetry — retry cap reached, escalating to operator")
			// Surface to the user via the pipeline_progress waiting_for_user state.
			_, _ = e.DB.ExecContext(ctx, `
				INSERT INTO pipeline_progress
				    (pipeline_id, execution_id, status, current_stage,
				     blocking_reason_type, blocking_reason_description, updated_at)
				VALUES ($1, $2, 'waiting_for_user', 'executor',
				        'retry_cap_reached', $3, NOW())
				ON CONFLICT (pipeline_id) DO UPDATE
				   SET status                      = 'waiting_for_user',
				       current_stage               = 'executor',
				       execution_id                = EXCLUDED.execution_id,
				       blocking_reason_type        = EXCLUDED.blocking_reason_type,
				       blocking_reason_description = EXCLUDED.blocking_reason_description,
				       updated_at                  = NOW()
			`, sig.PipelineID, nullableStr(sig.ExecutionID), desc)
			return nil // not an error — the cap is working as intended
		}
	}

	gwURL := strings.TrimRight(e.APIGatewayURL, "/")
	if gwURL == "" {
		gwURL = strings.TrimRight(os.Getenv("API_GATEWAY_INTERNAL_URL"), "/")
	}
	if gwURL == "" {
		gwURL = "http://api-gateway:8080"
	}

	// Target the InternalServiceMiddleware-guarded route, NOT the user-facing
	// /api/v1/pipelines/:id/run — that one sits under AuthRequiredMiddleware and
	// fail-closes 401 in prod for a tokenless request, so the healer retry never
	// actually re-ran (KI-HEAL-RERUN-UNAUTH). This route authenticates via the
	// shared X-Internal-Secret below, mirroring the OAuth-refresh client.
	url := fmt.Sprintf("%s/api/v1/internal/pipelines/%s/run", gwURL, sig.PipelineID)

	body, _ := json.Marshal(map[string]interface{}{
		"triggered_by": "healer",
		"reason":       "backoff_retry",
	})

	// Small back-off before the re-run request.
	select {
	case <-time.After(backoffRetryDelay):
	case <-ctx.Done():
		return ctx.Err()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("BackoffRetryExecutor: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Source", "healer") // diagnostic breadcrumb (api-gateway does not gate on this)
	// Authenticate the service-to-service call. INTERNAL_SERVICE_SECRET is shared
	// to both orchestrator + api-gateway in prod (docker-compose.prod.yml) and is
	// the same secret the OAuth-refresh path uses. Mirror refresh_client.go: set
	// the header only when present so local/dev without the secret still builds a
	// request (api-gateway's InternalServiceMiddleware rejects it with a visible
	// 401 rather than the request silently passing).
	if secret := os.Getenv("INTERNAL_SERVICE_SECRET"); secret != "" {
		req.Header.Set("X-Internal-Secret", secret)
	}

	client := e.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("BackoffRetryExecutor: POST run: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("BackoffRetryExecutor: run endpoint returned %d", resp.StatusCode)
	}

	writeHealEvent(ctx, e.DB, sig, "healer_backoff_retry", "Healer re-enqueued pipeline after transient failure")
	log.WithFields(log.Fields{
		"pipeline_id":  sig.PipelineID,
		"execution_id": sig.ExecutionID,
	}).Info("healer: BackoffRetry — pipeline re-enqueued")
	return nil
}

// ── RegenerateConnectorExecutor ───────────────────────────────────────────
// Transitions execution to waiting_for_user with blocking_reason_type =
// 'connector_generation'. The frontend's HITL modal already knows how to
// handle this blocking type and will show the "Regenerate connector" CTA.

type RegenerateConnectorExecutor struct{ DB *sql.DB }

func (e *RegenerateConnectorExecutor) Action() diagnose.Action {
	return diagnose.ActionRegenerateConnector
}
func (e *RegenerateConnectorExecutor) HITLSafe() bool { return true }

func (e *RegenerateConnectorExecutor) Run(ctx context.Context, sig diagnose.Signal) error {
	if sig.ExecutionID == "" || sig.PipelineID == "" {
		return fmt.Errorf("RegenerateConnectorExecutor: PipelineID and ExecutionID are required")
	}

	// Update pipeline_progress so the frontend polling detects waiting_for_user.
	_, err := e.DB.ExecContext(ctx, `
		INSERT INTO pipeline_progress
		    (pipeline_id, execution_id, status, current_stage,
		     blocking_reason_type, blocking_reason_description, updated_at)
		VALUES ($1, $2, 'waiting_for_user', 'executor',
		        'connector_generation',
		        'Healer: connector likely emits zero rows — please regenerate',
		        NOW())
		ON CONFLICT (pipeline_id) DO UPDATE
		   SET status                      = 'waiting_for_user',
		       current_stage               = 'executor',
		       execution_id                = EXCLUDED.execution_id,
		       blocking_reason_type        = EXCLUDED.blocking_reason_type,
		       blocking_reason_description = EXCLUDED.blocking_reason_description,
		       updated_at                  = NOW()
	`, sig.PipelineID, sig.ExecutionID)
	if err != nil {
		return fmt.Errorf("RegenerateConnectorExecutor: upsert pipeline_progress: %w", err)
	}

	writeHealEvent(ctx, e.DB, sig, "healer_regen_connector",
		"Healer flagged connector for regeneration — awaiting operator approval")
	log.WithFields(log.Fields{
		"pipeline_id":  sig.PipelineID,
		"execution_id": sig.ExecutionID,
	}).Info("healer: RegenerateConnector — waiting for HITL approval")
	return nil
}

// ── RequestUserConfigExecutor ─────────────────────────────────────────────
// Transitions execution to waiting_for_user with blocking_reason_type =
// 'user_input_required'. Surfaces a banner in the pipeline detail page
// asking the user to fix the configuration.

type RequestUserConfigExecutor struct{ DB *sql.DB }

func (e *RequestUserConfigExecutor) Action() diagnose.Action {
	return diagnose.ActionRequestUserConfig
}
func (e *RequestUserConfigExecutor) HITLSafe() bool { return true }

func (e *RequestUserConfigExecutor) Run(ctx context.Context, sig diagnose.Signal) error {
	if sig.ExecutionID == "" || sig.PipelineID == "" {
		return fmt.Errorf("RequestUserConfigExecutor: PipelineID and ExecutionID are required")
	}

	desc := "Healer: invalid or missing configuration — please review pipeline settings"
	if sig.ErrorMessage != "" {
		desc = fmt.Sprintf("Healer: %s", sig.ErrorMessage)
	}

	_, err := e.DB.ExecContext(ctx, `
		INSERT INTO pipeline_progress
		    (pipeline_id, execution_id, status, current_stage,
		     blocking_reason_type, blocking_reason_description, updated_at)
		VALUES ($1, $2, 'waiting_for_user', 'executor',
		        'user_input_required', $3, NOW())
		ON CONFLICT (pipeline_id) DO UPDATE
		   SET status                      = 'waiting_for_user',
		       current_stage               = 'executor',
		       execution_id                = EXCLUDED.execution_id,
		       blocking_reason_type        = EXCLUDED.blocking_reason_type,
		       blocking_reason_description = EXCLUDED.blocking_reason_description,
		       updated_at                  = NOW()
	`, sig.PipelineID, sig.ExecutionID, desc)
	if err != nil {
		return fmt.Errorf("RequestUserConfigExecutor: upsert pipeline_progress: %w", err)
	}

	writeHealEvent(ctx, e.DB, sig, "healer_request_config",
		"Healer requires operator to fix pipeline configuration")
	log.WithFields(log.Fields{
		"pipeline_id":  sig.PipelineID,
		"execution_id": sig.ExecutionID,
	}).Info("healer: RequestUserConfig — waiting for user to fix config")
	return nil
}

// ── helpers ────────────────────────────────────────────────────────────────

// writeHealEvent inserts a pipeline_run_events row so the execution
// timeline in the UI shows what the healer decided. Best-effort: errors
// are logged but not propagated (the primary executor action already
// succeeded).
func writeHealEvent(ctx context.Context, db *sql.DB, sig diagnose.Signal, eventType, desc string) {
	if db == nil || sig.PipelineID == "" {
		return
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"healer_action": eventType,
		"description":   desc,
		"pipeline_id":   sig.PipelineID,
		"execution_id":  sig.ExecutionID,
	})
	_, err := db.ExecContext(ctx, `
		INSERT INTO pipeline_run_events
		    (pipeline_id, execution_id, event_id, event_type, severity, payload, redacted)
		VALUES ($1, $2, $3, $4, 'info', $5, false)
		ON CONFLICT DO NOTHING
	`,
		sig.PipelineID,
		nullableStr(sig.ExecutionID),
		fmt.Sprintf("healer-%s-%d", eventType, time.Now().UnixNano()),
		eventType,
		payload,
	)
	if err != nil {
		log.WithError(err).Warn("healer: failed to write heal event")
	}
}

func nullableStr(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
