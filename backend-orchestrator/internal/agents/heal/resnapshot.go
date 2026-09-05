package heal

// resnapshot.go — the executor for ActionReSnapshot.
//
// diagnose.go emits this action at confidence 0.8 for the one failure class a
// retry provably cannot fix: the CDC stream position is gone. A MongoDB resume
// token that aged out of the oplog, an Oracle SCN past redo/archive retention.
// Retrying from a position that no longer exists fails identically every time,
// which is why the rule routes here instead of to backoff_retry.
//
// Nothing was registered to receive it, so Heal fell through its registry lookup
// and answered `no executor registered for action "re_snapshot"` — the healer
// describing its own wiring to an operator who needed a next step.
//
// This executor does not re-implement re-provisioning. It calls the recovery
// endpoint that already exists (handlers.RecoverCDCPipeline, routed at
// /api/v1/cdc/pipelines/:pipeline_id/recover), which enforces, in order: the
// CDC_RECOVERY_ENABLED flag, the workspace-role check, and the requirement that
// the connector or one of its tasks actually be FAILED. Reusing it means the
// healer cannot reach a state an operator pressing the same button could not.

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

// ReSnapshotExecutor re-establishes a CDC stream whose position is unrecoverable.
type ReSnapshotExecutor struct {
	DB         *sql.DB
	HTTPClient *http.Client
	// OrchestratorURL is the base URL of the service hosting the recovery route.
	// That service is this one — the healer runs inside the orchestrator — so
	// this is a loopback call. It goes over HTTP rather than calling the handler
	// directly because the handler is a gin.HandlerFunc: its guards read from the
	// request context, and reaching past them to an internal function would mean
	// reproducing the flag check, the role check and the FAILED-state check here,
	// where they would drift.
	OrchestratorURL string
}

func (e *ReSnapshotExecutor) Action() diagnose.Action { return diagnose.ActionReSnapshot }

// HITLSafe is false. A re-snapshot re-reads from the source; it belongs in the
// same class as BackoffRetry starting a run, and nothing has authorised the
// healer to do either unattended.
//
// Note this only governs Heal's middle band — the `Confidence >= AutoBand`
// branch calls Run without consulting HITLSafe. What keeps a re-snapshot off the
// unattended path is that the emitting rule sits at 0.8, below the band;
// resnapshot_test.go asserts that against the real diagnoser so a future rule
// cannot raise it without the check failing.
func (e *ReSnapshotExecutor) HITLSafe() bool { return false }

// snapshotModeRecovery re-establishes a position and streams forward from it.
// Deliberately not "initial", which would re-read the source in full: the
// diagnosis is that the position was lost, not that the data is wrong.
const snapshotModeRecovery = "recovery"

func (e *ReSnapshotExecutor) Run(ctx context.Context, sig diagnose.Signal) error {
	if sig.PipelineID == "" {
		return fmt.Errorf("ReSnapshotExecutor: PipelineID is required")
	}

	base := strings.TrimRight(e.OrchestratorURL, "/")
	if base == "" {
		base = strings.TrimRight(os.Getenv("ORCHESTRATOR_INTERNAL_URL"), "/")
	}
	if base == "" {
		base = strings.TrimRight(os.Getenv("ORCHESTRATOR_URL"), "/")
	}
	if base == "" {
		base = "http://orchestrator:8080"
	}

	url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/recover", base, sig.PipelineID)
	body, _ := json.Marshal(map[string]interface{}{
		"snapshot_mode": snapshotModeRecovery,
		"triggered_by":  "healer",
		"reason":        "cdc_position_lost",
	})

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("ReSnapshotExecutor: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Internal-Source", "healer")
	// Same service-to-service authentication as BackoffRetryExecutor. The
	// recovery handler's workspace-role gate short-circuits for an internal
	// principal (cdc_authz.go cdcPrincipal), so without this header the call
	// fail-closes 401 — visibly, which is the behaviour we want when the secret
	// is missing rather than a silent pass.
	if secret := os.Getenv("INTERNAL_SERVICE_SECRET"); secret != "" {
		req.Header.Set("X-Internal-Secret", secret)
	}

	client := e.HTTPClient
	if client == nil {
		// The handler gives itself a 90s budget for the Kafka Connect round
		// trips, so anything shorter here would time out on a working recovery.
		client = &http.Client{Timeout: 120 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ReSnapshotExecutor: POST recover: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		// 403 is the expected answer on production, where CDC_RECOVERY_ENABLED is
		// unset by standing policy. It has to reach the ledger as a failure: the
		// diagnosis was right and the remedy was refused, which is a different
		// thing for an operator to read than "re-snapshotted".
		writeHealEvent(ctx, e.DB, sig, "healer_resnapshot_refused",
			fmt.Sprintf("Healer requested a CDC re-snapshot; the recovery endpoint returned %d", resp.StatusCode))
		return fmt.Errorf("ReSnapshotExecutor: recovery endpoint returned %d", resp.StatusCode)
	}

	writeHealEvent(ctx, e.DB, sig, "healer_resnapshot",
		"Healer re-established the CDC stream from a fresh position (the previous position had aged out)")
	log.WithFields(log.Fields{
		"pipeline_id":   sig.PipelineID,
		"execution_id":  sig.ExecutionID,
		"snapshot_mode": snapshotModeRecovery,
	}).Info("healer: ReSnapshot — CDC stream re-established")
	return nil
}
