package handlers

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"api-gateway/internal/security"

	"github.com/gin-gonic/gin"
	"github.com/rsync-ai/backend-orchestrator/pkg/llmscrub"
	log "github.com/sirupsen/logrus"
)

func orchestratorBaseURL() string {
	if v := os.Getenv("ORCHESTRATOR_URL"); v != "" {
		return v
	}
	// Docker default (api-gateway -> orchestrator)
	return "http://orchestrator:8080"
}

// setInternalServiceSecret attaches the shared service-to-service secret so the
// orchestrator's requirePrincipal middleware accepts this proxied call as a
// trusted internal principal. The orchestrator CDC/agent endpoints are no longer
// anonymously reachable, so every api-gateway → orchestrator proxy call MUST
// carry this header. No-op when INTERNAL_SERVICE_SECRET is unset (dev/e2e).
func setInternalServiceSecret(req *http.Request) {
	if s := os.Getenv("INTERNAL_SERVICE_SECRET"); s != "" {
		req.Header.Set("X-Internal-Secret", s)
	}
}

// forwardOrchestratorJSON relays an orchestrator answer to the browser.
//
// KI-CDC-CONTROL-502-BODY-NOT-IN-TOAST: every proxy below used to do
// `body, _ := io.ReadAll(resp.Body)` and forward whatever came back. That
// discards the read error (a mid-body client timeout lands there), so a failed
// or partial read forwards the upstream status with a ZERO-LENGTH
// application/json body — and a blank error body is exactly the input that made
// the CDC control toast collapse to the bare literal "Request failed".
//
// Two changes, both narrow:
//   - the read error is logged (scrubbed, per the LLM/log privacy rule and
//     matching error_response.go) instead of being thrown away;
//   - a blank >= 400 body is replaced by an actionable one naming the action and
//     the HTTP status. The status code is ALWAYS forwarded verbatim, and a body
//     that has any content at all — success or failure — is forwarded untouched.
func forwardOrchestratorJSON(c *gin.Context, resp *http.Response, action string) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.WithFields(log.Fields{
			"action": action,
			"status": resp.StatusCode,
			"path":   c.FullPath(),
		}).WithField("error", llmscrub.Scrub(err.Error())).Warn("orchestrator response body unreadable")
	}
	if len(bytes.TrimSpace(body)) == 0 && resp.StatusCode >= 400 {
		c.JSON(resp.StatusCode, gin.H{
			"error":   "orchestrator_error",
			"message": fmt.Sprintf("%s failed: the orchestrator answered HTTP %d with no detail", action, resp.StatusCode),
		})
		return
	}
	c.Data(resp.StatusCode, "application/json", body)
}

// GetPipelineCDCStatus returns CDC connector status for a pipeline (best-effort).
// Proxies to backend-orchestrator, which calls Debezium MCP (Kafka Connect).
// Ownership is enforced here because the orchestrator endpoint has no caller
// identity — it would otherwise act on any pipeline_id we hand it.
func GetPipelineCDCStatus(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/status", orchestratorBaseURL(), pipelineID)
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, url, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build request"})
		return
	}
	if traceID := c.GetHeader("X-Trace-ID"); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	setInternalServiceSecret(req)
	resp, err := client.Do(req)
	if err != nil {
		respondError(c, http.StatusBadGateway, "orchestrator_unreachable", "Orchestrator is unreachable", err)
		return
	}
	defer resp.Body.Close()

	forwardOrchestratorJSON(c, resp, "CDC status")
}

// RestartPipelineCDC restarts the underlying CDC connector for a pipeline (best-effort).
// Proxies to backend-orchestrator. Ownership gate is local (the orchestrator
// has no caller identity over this internal HTTP call).
func RestartPipelineCDC(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/restart", orchestratorBaseURL(), pipelineID)
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, url, bytes.NewReader([]byte(`{}`)))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build request"})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if traceID := c.GetHeader("X-Trace-ID"); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	setInternalServiceSecret(req)
	resp, err := client.Do(req)
	if err != nil {
		respondError(c, http.StatusBadGateway, "orchestrator_unreachable", "Orchestrator is unreachable", err)
		return
	}
	defer resp.Body.Close()

	forwardOrchestratorJSON(c, resp, "Restart")
}

// RecoverPipelineCDC triggers a guarded CDC recovery flow for a FAILED connector.
// Proxies to backend-orchestrator.
//
// POST /api/v1/pipelines/:id/cdc/recover
// body: { "operator": "...", "reason": "...", "snapshot_mode": "initial|recovery|never", "reset_offsets": true, "dry_run": false }
func RecoverPipelineCDC(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	raw, _ := io.ReadAll(c.Request.Body)
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}

	url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/recover", orchestratorBaseURL(), pipelineID)
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build request"})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if traceID := c.GetHeader("X-Trace-ID"); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}

	client := &http.Client{Timeout: 45 * time.Second}
	setInternalServiceSecret(req)
	resp, err := client.Do(req)
	if err != nil {
		respondError(c, http.StatusBadGateway, "orchestrator_unreachable", "Orchestrator is unreachable", err)
		return
	}
	defer resp.Body.Close()

	forwardOrchestratorJSON(c, resp, "Recover")
}

// BackfillPipelineCDCTables triggers a Debezium ad-hoc snapshot (backfill) for specified tables.
// Proxies to backend-orchestrator.
//
// POST /api/v1/pipelines/:id/cdc/backfill
// body: { "tables": ["db.table1", ...], "mode": "incremental"|"blocking" }
func BackfillPipelineCDCTables(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	// Pass through request body as-is.
	raw, _ := io.ReadAll(c.Request.Body)
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}

	url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/backfill", orchestratorBaseURL(), pipelineID)
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build request"})
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if traceID := c.GetHeader("X-Trace-ID"); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	setInternalServiceSecret(req)
	resp, err := client.Do(req)
	if err != nil {
		respondError(c, http.StatusBadGateway, "orchestrator_unreachable", "Orchestrator is unreachable", err)
		return
	}
	defer resp.Body.Close()

	forwardOrchestratorJSON(c, resp, "Reload")
}

// PausePipelineCDC pauses a CDC pipeline by pausing the Kafka Connect connector.
// Proxies to backend-orchestrator PUT /api/v1/cdc/pipelines/:id/pause.
func PausePipelineCDC(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/pause", orchestratorBaseURL(), pipelineID)
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPut, url, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build request"})
		return
	}
	if traceID := c.GetHeader("X-Trace-ID"); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	setInternalServiceSecret(req)
	resp, err := client.Do(req)
	if err != nil {
		respondError(c, http.StatusBadGateway, "orchestrator_unreachable", "Orchestrator is unreachable", err)
		return
	}
	defer resp.Body.Close()

	forwardOrchestratorJSON(c, resp, "Pause")
}

// ResumePipelineCDC resumes a paused CDC pipeline by resuming the Kafka Connect connector.
// Proxies to backend-orchestrator PUT /api/v1/cdc/pipelines/:id/resume.
func ResumePipelineCDC(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}
	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSMember); !ok {
		return
	}

	url := fmt.Sprintf("%s/api/v1/cdc/pipelines/%s/resume", orchestratorBaseURL(), pipelineID)
	req, err := http.NewRequestWithContext(c.Request.Context(), http.MethodPut, url, nil)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to build request"})
		return
	}
	if traceID := c.GetHeader("X-Trace-ID"); traceID != "" {
		req.Header.Set("X-Trace-ID", traceID)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	setInternalServiceSecret(req)
	resp, err := client.Do(req)
	if err != nil {
		respondError(c, http.StatusBadGateway, "orchestrator_unreachable", "Orchestrator is unreachable", err)
		return
	}
	defer resp.Body.Close()

	forwardOrchestratorJSON(c, resp, "Resume")
}
