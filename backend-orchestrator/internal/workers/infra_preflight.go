package workers

// ==============================================================================
// INFRASTRUCTURE PRE-FLIGHT STAGE
// ==============================================================================
// Runs automatically before the executor for every pipeline. Checks and starts
// all required infrastructure (MCP containers, Kafka Connect, core services)
// so the user never has to do any manual work.
//
// For BATCH pipelines:
//   - Source MCP container   (stdio fallback ok)
//   - Destination MCP container (stdio fallback ok; HTTP attempted first)
//   - MinIO MCP               (large-batch staging; OPTIONAL — see below)
//
// For CDC pipelines:
//   - Destination MCP container (HTTP required — kafka-mcp-sink is cross-container)
//   - kafka-connect            (Debezium connector engine)
//   - debezium-mcp             (MCP bridge to kafka-connect)
//   - kafka-mcp-sink           (Kafka → destination writer)
//
// MinIO is checked for EVERY batch run, whatever the destination: the executor
// stages any batch over the inline limit there (claim-check) and sends the sink
// a URL. It is optional because the executor falls back to chunked Kafka when
// staging fails (executor.go stageDataToMinIO callers), so an unreachable MinIO
// degrades the run instead of failing it. Its progress lines say "large-batch
// staging store" because a bare "minio-mcp" on a GCS run read as a check of the
// wrong service (prod, 2026-09-26).
//
// MCP containers are started via the MCP server manager, which calls the
// tool-generator deploy API and polls until the container is healthy (up to
// DeployWaitTimeout). Core infra services (kafka-connect, debezium-mcp, etc.)
// use Docker's restart: unless-stopped — we just poll until they come back.
// ==============================================================================

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

const (
	// How long to wait for a user connector MCP container to start via tool-generator.
	preflightMCPTimeout = 90 * time.Second

	// How long to wait for core infra services (kafka-connect, debezium-mcp, etc.)
	// that have restart: unless-stopped. kafka-connect can take 60-90s to initialize
	// (JVM + plugin scanning). We wait up to 120s so a crash-restart doesn't fail the
	// pipeline — Docker will bring it back, we just need patience.
	preflightKafkaConnectRetries = 24 // 24 × 5s = 120s total

	// An optional service has a fallback, so it is not worth two minutes of the
	// run's time: 6 × 5s = 30s, then the tool-generator start, then the fallback.
	preflightOptionalRetries = 6
)

// preflightInfraRetryDelay is the gap between health polls. A var so tests can
// shrink it.
var preflightInfraRetryDelay = 5 * time.Second

// preflightService describes a single service to check during pre-flight.
type preflightService struct {
	name        string // logs and the `docker compose logs` hint
	label       string // user-facing name in progress messages; empty = name
	kind        string // "mcp_user", "mcp_core", "kafka_connect"
	requireHTTP bool   // relevant for mcp_user / mcp_core
	healthURL   string // for mcp_core / kafka_connect — direct HTTP health check
	mcpName     string // connector name passed to mcpManager.StartServer
	mcpVersion  string
	// fallback makes the service optional: when set and the service cannot be
	// reached, the run continues and the user is told this instead. Empty means
	// the service is required and an unreachable one fails the run.
	fallback string
}

func (s preflightService) displayName() string {
	if s.label != "" {
		return s.label
	}
	return s.name
}

func (s preflightService) pollAttempts() int {
	if s.fallback != "" {
		return preflightOptionalRetries
	}
	return preflightKafkaConnectRetries
}

// preflightResult is the outcome of a single service check.
type preflightResult struct {
	Service string
	OK      bool
	Mode    string // "docker_http", "stdio", "already_healthy", "started", "fallback"
	Err     error
	// Fallback is what the run does instead, when Mode is "fallback".
	Fallback string
}

// infraPreflightStage drives the pre-flight check for one pipeline execution.
type infraPreflightStage struct {
	agent           *executor.Agent
	progressEmitter *ProgressEmitter
	httpClient      *http.Client
}

func newInfraPreflightStage(agent *executor.Agent, emitter *ProgressEmitter) *infraPreflightStage {
	return &infraPreflightStage{
		agent:           agent,
		progressEmitter: emitter,
		httpClient:      &http.Client{Timeout: 5 * time.Second},
	}
}

// Run executes all pre-flight checks, starts missing services, and emits
// stage progress events. Returns a non-nil error only if a CRITICAL service
// cannot be started (pipeline cannot proceed).
func (p *infraPreflightStage) Run(ctx context.Context, task Task, execTask executor.ExecutorTask) error {
	pipelineID := task.PipelineID
	execID := getExecutionID(task)

	isCDC := false
	if sm, ok := execTask.Params["sync_mode"].(string); ok {
		isCDC = sm == "cdc" || sm == "streaming"
	}
	if !isCDC {
		if sm, ok := task.Payload["sync_mode"].(string); ok {
			isCDC = sm == "cdc" || sm == "streaming"
		}
	}

	services := p.requiredServices(execTask, isCDC)
	log.WithFields(log.Fields{
		"pipeline_id": pipelineID,
		"exec_id":     execID,
		"is_cdc":      isCDC,
		"services":    len(services),
	}).Info("🔍 Infra preflight: starting")
	if len(services) == 0 {
		log.Warn("🔍 Infra preflight: no services required, skipping")
		return nil
	}

	p.emitProgress(ctx, pipelineID, execID, "STAGE_STARTED", 70,
		"Checking infrastructure", "Preparing infrastructure…")

	results := p.checkAll(ctx, pipelineID, execID, services)

	failures, degraded := summarizePreflight(results)
	if len(failures) > 0 {
		msg := "Infrastructure pre-flight failed: " + strings.Join(failures, "; ")
		p.emitProgress(ctx, pipelineID, execID, "STAGE_FAILED", 70, msg, "Infrastructure check failed")
		return fmt.Errorf("%s", msg)
	}

	msg := "All required services are running"
	if len(degraded) > 0 {
		msg += " (" + strings.Join(degraded, "; ") + ")"
	}
	p.emitProgress(ctx, pipelineID, execID, "STAGE_COMPLETED", 78, msg, "Infrastructure ready")
	return nil
}

// summarizePreflight splits the results into failures (a required service is
// down: the run stops) and degradations (an optional one is down: the run goes
// on without it, and the completion message says what it does instead).
func summarizePreflight(results []preflightResult) (failures, degraded []string) {
	for _, r := range results {
		switch {
		case !r.OK:
			failures = append(failures, fmt.Sprintf("%s: %v", r.Service, r.Err))
		case r.Mode == "fallback":
			degraded = append(degraded, fmt.Sprintf("%s unavailable — %s", r.Service, r.Fallback))
		}
	}
	return failures, degraded
}

// requiredServices builds the list of services to check based on pipeline config.
func (p *infraPreflightStage) requiredServices(execTask executor.ExecutorTask, isCDC bool) []preflightService {
	var services []preflightService

	srcName, srcVer := "", ""
	if execTask.Source != nil {
		srcName = strings.TrimSpace(execTask.Source.Type)
		srcVer = strings.TrimSpace(execTask.Source.Version)
	}
	dstName, dstVer := "", ""
	if execTask.Destination != nil {
		dstName = strings.TrimSpace(execTask.Destination.Type)
		dstVer = strings.TrimSpace(execTask.Destination.Version)
	}

	if srcName != "" {
		services = append(services, preflightService{
			name:        fmt.Sprintf("%s MCP (source)", srcName),
			kind:        "mcp_user",
			requireHTTP: false, // batch source tolerates stdio
			mcpName:     srcName,
			mcpVersion:  srcVer,
		})
	}

	if dstName != "" {
		services = append(services, preflightService{
			name: fmt.Sprintf("%s MCP (destination)", dstName),
			kind: "mcp_user",
			// CDC destination MUST be HTTP — kafka-mcp-sink is a separate container
			requireHTTP: isCDC,
			mcpName:     dstName,
			mcpVersion:  dstVer,
		})
	}

	if isCDC {
		kafkaConnectURL := strings.TrimRight(os.Getenv("KAFKA_CONNECT_URL"), "/")
		if kafkaConnectURL == "" {
			kafkaConnectURL = "http://kafka-connect:8083"
		}
		services = append(services,
			preflightService{
				name:      "kafka-connect",
				label:     "Kafka Connect",
				kind:      "kafka_connect",
				healthURL: kafkaConnectURL + "/",
			},
			preflightService{
				name:       "debezium-mcp",
				kind:       "mcp_core",
				healthURL:  fmt.Sprintf("http://%s-debezium-v1-0-0-mcp:8000/health", mcp.StackPrefix()),
				mcpName:    "debezium",
				mcpVersion: "v1.0.0",
			},
			preflightService{
				name:       "kafka-mcp-sink",
				kind:       "mcp_core",
				healthURL:  fmt.Sprintf("http://%s-kafka-mcp-sink-v1-0-0-mcp:8000/health", mcp.StackPrefix()),
				mcpName:    "kafka-mcp-sink",
				mcpVersion: "v1.0.0",
			},
		)
	} else {
		// Batch: MinIO stages any batch over the inline limit, whatever the
		// destination. Optional — the executor falls back to chunked Kafka.
		services = append(services, preflightService{
			name:       "minio-mcp",
			label:      "large-batch staging store (minio-mcp)",
			kind:       "mcp_core",
			healthURL:  fmt.Sprintf("http://%s-minio-v1-0-0-mcp:8000/health", mcp.StackPrefix()),
			mcpName:    "minio",
			mcpVersion: "v1.0.0",
			fallback:   "large batches will be sent to Kafka in chunks instead",
		})
	}

	return services
}

// checkAll runs all service checks concurrently and returns results in order.
func (p *infraPreflightStage) checkAll(
	ctx context.Context, pipelineID, execID string, services []preflightService,
) []preflightResult {
	results := make([]preflightResult, len(services))
	var wg sync.WaitGroup

	for i, svc := range services {
		wg.Add(1)
		go func(idx int, s preflightService) {
			defer wg.Done()
			r := p.checkOne(ctx, pipelineID, execID, s)
			results[idx] = r
		}(i, svc)
	}

	wg.Wait()
	return results
}

// checkOne verifies and if necessary starts a single service. An optional
// service (fallback set) that cannot be reached is reported as a fallback, not a
// failure.
func (p *infraPreflightStage) checkOne(
	ctx context.Context, pipelineID, execID string, svc preflightService,
) preflightResult {
	p.emitServiceProgress(ctx, pipelineID, execID, fmt.Sprintf("Checking %s…", svc.displayName()))

	var r preflightResult
	switch svc.kind {
	case "mcp_user":
		r = p.checkMCPUser(ctx, pipelineID, execID, svc)
	case "mcp_core":
		r = p.checkMCPCore(ctx, pipelineID, execID, svc)
	case "kafka_connect":
		r = p.checkKafkaConnect(ctx, pipelineID, execID, svc)
	default:
		return preflightResult{Service: svc.name, OK: true, Mode: "skip"}
	}

	if !r.OK && svc.fallback != "" {
		log.Warnf("⚠️  Infra preflight: optional %s unavailable (%v) — %s", svc.name, r.Err, svc.fallback)
		p.emitServiceProgress(ctx, pipelineID, execID,
			fmt.Sprintf("%s unavailable — %s", svc.displayName(), svc.fallback))
		return preflightResult{Service: svc.displayName(), OK: true, Mode: "fallback", Err: r.Err, Fallback: svc.fallback}
	}
	return r
}

// checkMCPUser starts user-connector MCP containers (mysql, postgresql, etc.)
// via the MCP server manager, which calls tool-generator and polls until ready.
func (p *infraPreflightStage) checkMCPUser(
	ctx context.Context, pipelineID, execID string, svc preflightService,
) preflightResult {
	mgr := p.agent.MCPManager()
	if mgr == nil {
		return preflightResult{Service: svc.name, OK: true, Mode: "skip"}
	}

	ver := svc.mcpVersion
	if ver == "" {
		ver = "latest"
	}
	cfg := mcp.ServerConfig{
		Name:              svc.mcpName,
		Version:           ver,
		RequireHTTP:       svc.requireHTTP,
		DeployWaitTimeout: preflightMCPTimeout,
	}

	info, err := mgr.StartServer(cfg)
	if err != nil {
		if svc.requireHTTP {
			// CDC destination — hard failure; pipeline cannot proceed
			log.Errorf("❌ Infra preflight: %s failed to start as HTTP: %v", svc.name, err)
			return preflightResult{Service: svc.name, OK: false, Err: err}
		}
		// Batch — stdio fallback available; not a blocking failure
		log.Warnf("⚠️  Infra preflight: %s could not start Docker HTTP container (stdio fallback will be used): %v", svc.name, err)
		p.emitServiceProgress(ctx, pipelineID, execID, fmt.Sprintf("%s: using stdio fallback", svc.displayName()))
		return preflightResult{Service: svc.name, OK: true, Mode: "stdio_fallback"}
	}

	mode := "docker_http"
	if info != nil && info.ConnType == "stdio" {
		mode = "stdio"
	}
	log.Infof("✅ Infra preflight: %s ready (%s)", svc.name, mode)
	p.emitServiceProgress(ctx, pipelineID, execID, fmt.Sprintf("%s ready", svc.displayName()))
	return preflightResult{Service: svc.name, OK: true, Mode: mode}
}

// checkMCPCore checks core infra MCP services (debezium-mcp, kafka-mcp-sink, minio-mcp).
// These have restart: unless-stopped — Docker brings them back automatically after crashes.
// We poll until healthy (up to 120s; 30s for an optional service), emitting live progress so
// the user sees what's happening. If still not reachable, we attempt a start via
// tool-generator before giving up (checkOne then decides failure vs fallback).
func (p *infraPreflightStage) checkMCPCore(
	ctx context.Context, pipelineID, execID string, svc preflightService,
) preflightResult {
	if ok, mode := p.pollWithProgress(ctx, pipelineID, execID, svc); ok {
		return preflightResult{Service: svc.name, OK: true, Mode: mode}
	}

	// Still not reachable — attempt deploy via tool-generator as last resort.
	mgr := p.agent.MCPManager()
	if mgr != nil && svc.mcpName != "" {
		ver := svc.mcpVersion
		if ver == "" {
			ver = "v1.0.0"
		}
		p.emitServiceProgress(ctx, pipelineID, execID, fmt.Sprintf("Starting %s via tool-generator…", svc.displayName()))
		cfg := mcp.ServerConfig{
			Name:              svc.mcpName,
			Version:           ver,
			RequireHTTP:       true,
			DeployWaitTimeout: preflightMCPTimeout,
		}
		if _, err := mgr.StartServer(cfg); err == nil {
			log.Infof("✅ Infra preflight: %s started via tool-generator", svc.name)
			p.emitServiceProgress(ctx, pipelineID, execID, fmt.Sprintf("%s started", svc.displayName()))
			return preflightResult{Service: svc.name, OK: true, Mode: "started"}
		}
	}

	err := fmt.Errorf("%s is not reachable — check `docker compose logs %s`", svc.name, svc.name)
	log.Errorf("❌ Infra preflight: %v", err)
	return preflightResult{Service: svc.name, OK: false, Err: err}
}

// checkKafkaConnect verifies Kafka Connect is reachable.
// kafka-connect takes 60-90s to initialize (JVM + Debezium plugin scanning) after a restart.
// We wait up to 120s with live progress events — no hard fail during restart window.
func (p *infraPreflightStage) checkKafkaConnect(
	ctx context.Context, pipelineID, execID string, svc preflightService,
) preflightResult {
	if ok, mode := p.pollWithProgress(ctx, pipelineID, execID, svc); ok {
		return preflightResult{Service: svc.name, OK: true, Mode: mode}
	}

	// kafka-connect cannot be started via tool-generator (it is Confluent/JVM infra, not an MCP).
	// At this point it has been down for 120s+ which means Docker failed to restart it.
	err := fmt.Errorf("connect_unavailable")
	log.Errorf("❌ Infra preflight: kafka-connect not reachable after 120s — check memory (KAFKA_HEAP_OPTS) and `docker compose logs kafka-connect`")
	return preflightResult{Service: svc.name, OK: false, Err: err}
}

// pollWithProgress polls svc's health URL, emitting a progress event every 5 attempts (25s)
// so the user sees live feedback during long restarts. Returns (true, mode) on success.
func (p *infraPreflightStage) pollWithProgress(
	ctx context.Context, pipelineID, execID string, svc preflightService,
) (bool, string) {
	serviceName := svc.displayName()
	attempts := svc.pollAttempts()
	for attempt := 1; attempt <= attempts; attempt++ {
		checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		req, err := http.NewRequestWithContext(checkCtx, http.MethodGet, svc.healthURL, nil)
		if err != nil {
			cancel()
			return false, ""
		}
		resp, doErr := p.httpClient.Do(req)
		cancel()

		if doErr == nil && resp != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			resp.Body.Close()
			mode := "already_healthy"
			if attempt > 1 {
				mode = "recovered"
			}
			log.Infof("✅ Infra preflight: %s healthy (attempt %d)", serviceName, attempt)
			p.emitServiceProgress(ctx, pipelineID, execID, fmt.Sprintf("%s ready", serviceName))
			return true, mode
		}
		if resp != nil {
			resp.Body.Close()
		}

		// Emit live progress every 5 attempts so the user isn't staring at a blank spinner.
		if attempt%5 == 0 || attempt == 1 {
			waited := time.Duration(attempt) * preflightInfraRetryDelay
			remaining := time.Duration(attempts-attempt) * preflightInfraRetryDelay
			log.Warnf("⏳ Infra preflight: %s not yet ready (waited %s, up to %s remaining)", serviceName, waited, remaining)
			p.emitServiceProgress(ctx, pipelineID, execID,
				fmt.Sprintf("Waiting for %s to start (%.0fs elapsed)…", serviceName, waited.Seconds()))
		}

		if attempt < attempts {
			select {
			case <-ctx.Done():
				return false, ""
			case <-time.After(preflightInfraRetryDelay):
			}
		}
	}
	return false, ""
}

// emitProgress emits a pipeline-level progress event for the infra_preflight stage.
func (p *infraPreflightStage) emitProgress(
	ctx context.Context, pipelineID, execID, eventType string, percent int, message, summary string,
) {
	if p.progressEmitter == nil {
		return
	}
	p.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   eventType,
		PipelineID:  pipelineID,
		ExecutionID: execID,
		Stage:       "infra_preflight",
		Status:      "processing",
		Message:     message,
		Summary:     summary,
		// infra_preflight is step 7 of 8 (validator is 6); the previous
		// hardcoded "step 6 / 74%" caused a backward jump after the
		// validator's 75% emit. Keep monotonic with the executor's
		// later 80% emit.
		Progress: ProgressInfo{
			Percent:     percent,
			CurrentStep: 7,
			TotalSteps:  8,
			Stage:       "infra_preflight",
		},
	})
}

// emitServiceProgress emits a STAGE_PROGRESS sub-event for a specific service check.
func (p *infraPreflightStage) emitServiceProgress(ctx context.Context, pipelineID, execID, message string) {
	if p.progressEmitter == nil {
		return
	}
	p.progressEmitter.EmitProgress(ctx, ProgressEvent{
		EventType:   "STAGE_PROGRESS",
		PipelineID:  pipelineID,
		ExecutionID: execID,
		Stage:       "infra_preflight",
		Status:      "processing",
		Message:     message,
		Summary:     "Preparing infrastructure",
		Progress: ProgressInfo{
			Percent:     78,
			CurrentStep: 7,
			TotalSteps:  8,
			Stage:       "infra_preflight",
		},
	})
}
