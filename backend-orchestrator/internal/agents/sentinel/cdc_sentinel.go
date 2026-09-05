package sentinel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	_ "github.com/rsync-ai/shared/pgdriver"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"

	"github.com/rsync-ai/backend-orchestrator/internal/cdc"
	"github.com/rsync-ai/backend-orchestrator/internal/connections"
	"github.com/rsync-ai/backend-orchestrator/internal/handlers"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
	"github.com/rsync-ai/backend-orchestrator/internal/utils"
)

var cdcSentinelTracer = otel.Tracer("cdc-sentinel")

// Per-table CDC statistics deliberately do NOT live here. They are produced by
// internal/agents/cdcstats, which consumes the Debezium topics directly and emits
// TABLE_STATS domain events (wired in cmd/orchestrator/main.go behind
// ENABLE_CDC_TABLE_STATS). The Sentinel once carried a second, parallel attempt at the
// same thing that could never work — it polled `/connectors/<name>/metrics`, a route the
// Kafka Connect REST API does not have — so it is gone rather than duplicated. Do not
// reintroduce a stats path here; extend cdcstats instead.

const (
	// How often to check all active CDC pipelines
	SentinelPollInterval = 60 * time.Second
	// How often to check source DB lag
	SourceLagPollInterval = 2 * time.Minute
	// Alert when PostgreSQL replication slot lag exceeds 100 MB
	PGSlotLagAlertBytes int64 = 100 * 1024 * 1024
	// Alert when real MySQL source binlog lag (bytes the source has produced beyond
	// the Debezium connector's committed position) exceeds 100 MB. Mirrors
	// PGSlotLagAlertBytes — this is the true source-side measurement.
	MySQLBinlogLagAlertBytes int64 = 100 * 1024 * 1024
	// Fallback only: alert when the CDC sink Kafka consumer lag exceeds 1000 messages.
	// Used when the source-side binlog lag can't be computed (connector not started,
	// offset purged, or an older Kafka Connect without the offsets REST endpoint).
	MySQLKafkaLagAlert int64 = 1000
	// Alert when the CDC SINK's own consumer group lags by more than this many events —
	// i.e. Debezium is producing change events but the sink is not draining them to the
	// destination (dead / wedged / crash-looping sink). Distinct from MySQLKafkaLagAlert
	// (a source-lag fallback); this is the always-on "is the sink alive and delivering"
	// check for every CDC pipeline. Overridable via CDC_SINK_KAFKA_LAG_ALERT.
	SinkKafkaLagAlert int64 = 1000

	// --- WAL watchdog (Part B) defaults ---
	// How often the WAL watchdog inspects retained WAL per replication slot.
	DefaultWALWatchdogInterval = 2 * time.Minute
	// WARN tier: a slot retaining more than this much WAL on the source raises a
	// warning alert (no action). 512 MB.
	DefaultWALWarnBytes int64 = 512 * 1024 * 1024
	// CRITICAL tier: a slot retaining more than this much WAL is dangerous (the
	// source can run out of disk). 2 GB. For a stopped/deleted pipeline the slot
	// is auto-dropped; for a running/paused pipeline it escalates as critical.
	DefaultWALCriticalBytes int64 = 2 * 1024 * 1024 * 1024

	// --- Restart-attempt cap (FINDING-04) ---
	// A connector restart can only fix a *transient* fault. A permanently-FAILED
	// connector (bad credentials, dropped slot/publication, invalid config) can never
	// be revived by restarting. Without a cap the Sentinel re-detects + re-restarts +
	// re-escalates such a connector on EVERY poll forever (restart spam + Healer/notify
	// spam), and because the pipeline stays status='running' nothing ever reaps its
	// leaked replication slot/WAL. These bounds give up after a few spaced-out attempts
	// and transition the pipeline to 'stopped' so the existing reapers reclaim the slot.
	MaxConnectorRestartAttempts = 3
	// Don't restart the same connector more than once per cooldown (spaces attempts out
	// across several polls instead of hammering every 60s).
	ConnectorRestartCooldown = 5 * time.Minute
	// Rolling window after which a connector's restart-attempt count resets to zero, so a
	// connector that fails transiently every few weeks isn't permanently considered dead.
	ConnectorRestartWindow = 24 * time.Hour
)

// These restart-policy bounds are overridable via env for operators running very large
// initial snapshots (where each blocking-snapshot restart re-reads the whole table from
// scratch). Defaults are unchanged from the consts above.
func maxConnectorRestartAttempts() int {
	if raw := strings.TrimSpace(os.Getenv("CDC_SENTINEL_MAX_RESTARTS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return MaxConnectorRestartAttempts
}

func connectorRestartWindow() time.Duration {
	if raw := strings.TrimSpace(os.Getenv("CDC_SENTINEL_RESTART_WINDOW")); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			return d
		}
	}
	return ConnectorRestartWindow
}

func connectorRestartCooldown() time.Duration {
	if raw := strings.TrimSpace(os.Getenv("CDC_SENTINEL_RESTART_COOLDOWN")); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			return d
		}
	}
	return ConnectorRestartCooldown
}

func kafkaConnectURLFromEnv() string {
	v := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL"))
	if v == "" {
		return "http://kafka-connect:8083"
	}
	return v
}

func pollIntervalFromEnv() time.Duration {
	v := strings.TrimSpace(os.Getenv("CDC_SENTINEL_POLL_INTERVAL"))
	if v == "" {
		return SentinelPollInterval
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	return SentinelPollInterval
}

// connRestartState tracks per-connector restart attempts so the Sentinel can give up
// on a permanently-FAILED connector instead of restarting it forever (FINDING-04).
type connRestartState struct {
	attempts     int       // restart attempts inside the current rolling window
	firstAttempt time.Time // start of the current window
	lastAttempt  time.Time // last restart attempt (for cooldown spacing)
	escalated    bool      // terminal escalation already sent (dedup)
}

// CDCSentinel monitors the health of active Debezium connectors
type CDCSentinel struct {
	db           *sql.DB
	kafkaManager *kafka.Manager
	httpClient   *http.Client
	ctx          context.Context
	cancel       context.CancelFunc
	workerID     string
	connectURL   string
	pollInterval time.Duration
	// mu guards restartState, sinkRestartState, sinkRespawnState, sinkProgress, startedAt,
	// and mcpManager — all touched from more than one of the Sentinel's background goroutines
	// (monitoringLoop + sourceLagLoop + walWatchdogLoop) and, for mcpManager, from the startup
	// goroutine via SetMCPManager. Without it, concurrent map iteration+write is a Go runtime
	// fatal that crashes the whole orchestrator (SENT-C).
	mu sync.Mutex
	// Per-connector restart-attempt tracking (FINDING-04). Keyed by connector name.
	restartState map[string]*connRestartState
	// Per-pipeline SINK restart-attempt tracking for the autonomous wedge-recovery rung
	// (Phase B). Keyed by pipeline_id (a sink has no Kafka-Connect connector name), so it is
	// deliberately separate from restartState above. Same cap/window/cooldown semantics.
	sinkRestartState map[string]*connRestartState
	// Per-pipeline SINK respawn-attempt tracking for the ABSENT-worker rung
	// (cdc_sink_respawn.go). Separate from sinkRestartState because the two rungs fire on
	// different triggers and must not share a budget: a wedge restart burning the cap would
	// otherwise stop us from recovering a genuinely absent worker.
	sinkRespawnState map[string]*connRestartState
	// Per-pipeline previous destination-apply marker (MAX(last_applied_ts)), so the wedge
	// gate can tell a flat (non-advancing) sink from a slow-but-progressing one across ticks.
	sinkProgress map[string]sql.NullTime
	// startedAt anchors the respawn rung's startup grace window, so we don't race the CDC
	// executor's own start_sink while the stack is still coming up.
	startedAt time.Time
	// mcpManager is needed to issue the sink stop/start restart. It is plumbed in AFTER
	// construction (the ServerManager is built later in main.go than the Sentinel), via
	// SetMCPManager; until then the autonomous rung is a no-op even when enabled.
	mcpManager *mcp.ServerManager

	// --- WAL watchdog (Part B) config ---
	walWatchdogInterval time.Duration
	walWarnBytes        int64
	walCriticalBytes    int64
	walAutoAct          bool
}

func NewCDCSentinel(db *sql.DB, kafkaManager *kafka.Manager) *CDCSentinel {
	ctx, cancel := context.WithCancel(context.Background())
	return &CDCSentinel{
		db:           db,
		kafkaManager: kafkaManager,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		ctx:              ctx,
		cancel:           cancel,
		workerID:         fmt.Sprintf("cdc-sentinel-%s", uuid.New().String()[:8]),
		connectURL:       kafkaConnectURLFromEnv(),
		pollInterval:     pollIntervalFromEnv(),
		restartState:     make(map[string]*connRestartState),
		sinkRestartState: make(map[string]*connRestartState),
		sinkRespawnState: make(map[string]*connRestartState),
		sinkProgress:     make(map[string]sql.NullTime),

		walWatchdogInterval: walDurationFromEnv("CDC_WAL_WATCHDOG_INTERVAL", DefaultWALWatchdogInterval),
		walWarnBytes:        walBytesFromEnv("CDC_WAL_WARN_BYTES", DefaultWALWarnBytes),
		walCriticalBytes:    walBytesFromEnv("CDC_WAL_CRITICAL_BYTES", DefaultWALCriticalBytes),
		walAutoAct:          walBoolFromEnv("CDC_WAL_WATCHDOG_AUTOACT", true),
	}
}

// SetMCPManager plumbs in the MCP ServerManager the autonomous sink-restart rung needs to
// issue stop_sink/start_sink. It is called once from main.go after the ServerManager is
// constructed (which happens later than the Sentinel). Guarded by mu because the sourceLagLoop
// goroutine reads mcpManager concurrently. Until this is called the rung is a safe no-op.
func (s *CDCSentinel) SetMCPManager(m *mcp.ServerManager) {
	s.mu.Lock()
	s.mcpManager = m
	s.mu.Unlock()
}

// Start begins the monitoring loop
func (s *CDCSentinel) Start() error {
	log.Info("🛡️ Starting CDC Sentinel (Proactive Debezium Monitoring + Source Lag)")
	s.mu.Lock()
	s.startedAt = time.Now()
	s.mu.Unlock()
	go s.monitoringLoop()
	go s.sourceLagLoop()
	go s.walWatchdogLoop()
	return nil
}

func (s *CDCSentinel) Stop() error {
	log.Info("🛑 Stopping CDC Sentinel")
	s.cancel()
	return nil
}

// recoverTick contains a panic raised inside a single Sentinel poll so it cannot kill
// the surrounding monitoring goroutine. The external Debezium/Kafka-Connect JSON the
// Sentinel parses can take unexpected shapes; before this guard a single malformed
// payload would panic, kill the loop, and silently stop CDC monitoring for the lifetime
// of the process while pipelines still showed "running" (SENT-A).
func recoverTick(name string) {
	if r := recover(); r != nil {
		log.WithField("loop", name).Errorf("🛡️ Sentinel %s tick recovered from panic: %v", name, r)
	}
}

func (s *CDCSentinel) monitoringLoop() {
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkActivePipelines(s.ctx)
		}
	}
}

// activeCDCPipelinesQuery selects the running CDC pipelines the Sentinel monitors.
//
// It selects ONLY `id` — the one column the caller reads — and that is load-bearing,
// not tidiness. It used to also select `name` and `source_connection_id` and scan all
// three into plain `string`s. But `pipelines.source_connection_id` is
// `REFERENCES connections(id) ON DELETE SET NULL` (001_init_schema.sql), so deleting a
// source connection leaves a NULL there and the scan fails with "converting NULL to
// string is unsupported". The row was then skipped — meaning a running CDC pipeline
// whose source connection had been deleted was silently dropped from CDC monitoring
// entirely, while the log said only "failed to scan row" (the error was discarded).
//
// Adding a column here is fine; adding a NULLABLE one and scanning it into a bare
// `string` reintroduces the bug. Scan nullable columns into sql.NullString.
const activeCDCPipelinesQuery = `
		SELECT id
		FROM pipelines
		WHERE status = 'running'
		AND (sync_mode = 'cdc' OR cdc_mode IS NOT NULL)
	`

// collectActiveCDCPipelines returns (short8 -> pipeline_id, pipeline_id -> true) for every
// running CDC pipeline. A row that fails to scan is logged WITH its error and skipped; the
// rest of the tick still proceeds, so one bad row cannot blind the Sentinel to the others.
func (s *CDCSentinel) collectActiveCDCPipelines(ctx context.Context) (map[string]string, map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx, activeCDCPipelinesQuery)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	activePipelineByShort := make(map[string]string) // short8 -> pipeline_id
	activePipelines := make(map[string]bool)         // full pipeline_id -> true
	for rows.Next() {
		var pID string
		if err := rows.Scan(&pID); err != nil {
			log.WithError(err).Warn("🛡️ Sentinel failed to scan active-CDC-pipeline row — this pipeline is NOT being monitored this tick")
			continue
		}
		activePipelines[pID] = true
		if len(pID) >= 8 {
			activePipelineByShort[pID[:8]] = pID
		} else if pID != "" {
			activePipelineByShort[pID] = pID
		}
	}
	if err := rows.Err(); err != nil {
		// Partial results are still worth using — return them alongside the error so the
		// caller can decide, rather than throwing away pipelines we already collected.
		log.WithError(err).Warn("🛡️ Sentinel hit a row-iteration error listing active CDC pipelines")
	}
	return activePipelineByShort, activePipelines, nil
}

// checkActivePipelines iterates over all active CDC pipelines and verifies their connector status
func (s *CDCSentinel) checkActivePipelines(ctx context.Context) {
	defer recoverTick("check_pipelines")
	ctx, span := cdcSentinelTracer.Start(ctx, "sentinel.check_pipelines")
	defer span.End()

	// 1. Get all active CDC pipelines from DB. Only the short8 index is needed here — the
	// full-id set was read by the table-stats path, which is gone (real per-table stats come
	// from internal/agents/cdcstats). collectActiveCDCPipelines still returns it because
	// cdc_sentinel_active_pipelines_test.go asserts on it directly.
	activePipelineByShort, _, err := s.collectActiveCDCPipelines(ctx)
	if err != nil {
		log.WithError(err).Error("🛡️ Sentinel failed to fetch active pipelines")
		return
	}

	// REVISED STRATEGY: List all connectors from the Debezium service directly via MCP
	// and cross-reference with our expected state.

	// We need to invoke the 'debezium_list_connectors' MCP tool.
	// Since we are inside the Go binary, we can't easily call "ourselves" if the MCP server is remote (container).
	// BUT, the Executor knows how to call MCP.
	// Sentinel is part of Orchestrator, so it can make HTTP requests to the MCP container directly if known.
	// OR use the 'executor.Agent' logic?

	// Simplest path for MVP: Assume MCP container is at 'http://mcp-debezium:8000' or similar
	// But `debezium` connector code is dynamic.

	// Wait, `shared/mcp-connectors/internal/debezium/versions/v1.0.0/connector.py` is the MCP server.
	// It runs as a sidecar or a dedicated service.
	// The `ServerManager` started it.

	// FOR LIVE READINESS: We'll do a generic probe to the Kafka Connect API (via the Debezium MCP tool interface).
	// We will manually construct a call to the Debezium MCP tool.

	status, err := s.getDebeziumStatus(ctx)
	if err != nil {
		log.WithError(err).Warn("🛡️ Sentinel could not reach Debezium/Connect")
		return
	}

	// Status maps connector_name -> status object
	seenConnectors := make(map[string]bool, len(status))
	for connName, connState := range status {
		seenConnectors[connName] = true
		stateMap, _ := connState.(map[string]interface{})
		stateStr, _ := stateMap["state"].(string)

		// Find associated pipeline
		pipelineID := s.findPipelineIDForConnector(ctx, connName, activePipelineByShort)

		if stateStr == "FAILED" {
			// Only act on connectors we can associate to an active CDC pipeline (best-effort).
			if pipelineID == "" {
				// Avoid spamming Healer for unrelated connectors.
				continue
			}
			s.handleFailedConnector(ctx, pipelineID, connName, connState)
		} else if stateStr == "RUNNING" {
			// Connector is healthy again — clear any restart-attempt bookkeeping so a
			// future failure starts from a clean slate (FINDING-04).
			s.clearRestartState(connName)
		}
	}

	// Prune restart bookkeeping for connectors that no longer appear in Connect (deleted
	// or reaped) so the map can't grow unbounded.
	s.pruneRestartState(seenConnectors)
}

// pipelineInInitialSnapshot reports whether the pipeline is in its (non-resumable) blocking
// initial-snapshot phase, where every connector restart re-runs the whole snapshot from
// scratch. Best-effort: any error returns false (used only to enrich failure logs).
func (s *CDCSentinel) pipelineInInitialSnapshot(ctx context.Context, pipelineID string) bool {
	if s.db == nil || pipelineID == "" {
		return false
	}
	qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var cdcMode sql.NullString
	if err := s.db.QueryRowContext(qctx, `SELECT cdc_mode FROM pipelines WHERE id = $1`, pipelineID).Scan(&cdcMode); err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(cdcMode.String), "initial")
}

// handleFailedConnector applies the bounded restart policy for a FAILED Debezium
// connector associated with a running CDC pipeline (FINDING-04 + SENT-B).
//
// A restart can only fix a transient fault; a permanently-FAILED connector can never be
// revived by restarting. So we cap restarts (MaxConnectorRestartAttempts within a rolling
// ConnectorRestartWindow, spaced by ConnectorRestartCooldown). When the cap is exhausted
// we stop restarting, escalate to the Healer exactly once, and transition the pipeline to
// 'stopped' — which lets the existing reapers (cdc_reconciler / wal_watchdog) reclaim the
// leaked replication slot + WAL. The pipeline is only moved out of 'running' AFTER attempts
// are exhausted, so a still-recoverable connector's anchor slot is never dropped prematurely.
func (s *CDCSentinel) handleFailedConnector(ctx context.Context, pipelineID, connName string, connState interface{}) {
	now := time.Now()
	inSnapshot := s.pipelineInInitialSnapshot(ctx, pipelineID)

	s.mu.Lock()
	st := s.restartState[connName]
	if st == nil {
		st = &connRestartState{firstAttempt: now}
		s.restartState[connName] = st
	}
	// Reset the window if it has rolled over.
	if now.Sub(st.firstAttempt) > connectorRestartWindow() {
		st.attempts = 0
		st.firstAttempt = now
		st.escalated = false
	}
	attempts := st.attempts
	lastAttempt := st.lastAttempt
	alreadyEscalated := st.escalated
	s.mu.Unlock()

	// Cap exhausted → terminal. Stop restarting; escalate + stop the pipeline once.
	if attempts >= maxConnectorRestartAttempts() {
		if alreadyEscalated {
			// Already handled this terminal failure; stay quiet until the connector is
			// reaped or recovers (which clears the state).
			return
		}
		termFields := log.Fields{
			"connector":   connName,
			"pipeline_id": pipelineID,
			"attempts":    attempts,
		}
		if inSnapshot {
			termFields["snapshot_phase"] = true
			termFields["remediation"] = "pipeline was in the blocking initial snapshot; each restart re-ran it from scratch. Use cdc_initial_load=batch or an incremental snapshot so history resumes instead of restarting."
		}
		log.WithFields(termFields).Error("🛡️ Sentinel: connector permanently FAILED after max restart attempts — stopping pipeline and escalating")

		s.markPipelineStopped(ctx, pipelineID, connName, attempts)
		s.triggerHealer(ctx, pipelineID, connName, connState)

		s.mu.Lock()
		if cur := s.restartState[connName]; cur != nil {
			cur.escalated = true
		}
		s.mu.Unlock()
		return
	}

	// Cooldown: don't restart the same connector more than once per ConnectorRestartCooldown.
	if !lastAttempt.IsZero() && now.Sub(lastAttempt) < connectorRestartCooldown() {
		return
	}

	// Record the attempt BEFORE restarting so even a Kafka-Connect "accepted" (204/409)
	// response counts toward the cap — otherwise a connector that accepts every restart
	// request but immediately re-fails would loop forever (SENT-B).
	s.mu.Lock()
	if cur := s.restartState[connName]; cur != nil {
		cur.attempts++
		cur.lastAttempt = now
		attempts = cur.attempts
	}
	s.mu.Unlock()

	warnFields := log.Fields{
		"connector":   connName,
		"pipeline_id": pipelineID,
		"attempt":     attempts,
		"max":         maxConnectorRestartAttempts(),
	}
	if inSnapshot {
		warnFields["snapshot_phase"] = true
		warnFields["note"] = "connector is in the blocking initial snapshot — this restart re-runs the entire snapshot from scratch (not resumable); consider cdc_initial_load=batch or an incremental snapshot"
	}
	log.WithFields(warnFields).Warn("🛡️ Sentinel detected FAILED connector — attempting auto-restart")

	// Attempt a direct restart via the Kafka Connect REST API. A successful HTTP response
	// only means the restart was ACCEPTED, not that the connector recovered — recovery is
	// confirmed only when a later poll observes state=RUNNING (which clears the state). So
	// we never treat the restart response itself as "recovered"; the cap above bounds it.
	if accepted := s.restartConnector(ctx, connName); accepted {
		log.WithFields(log.Fields{
			"connector":   connName,
			"pipeline_id": pipelineID,
			"attempt":     attempts,
		}).Info("🛡️ Sentinel restart request accepted — awaiting RUNNING confirmation on next poll")
		return
	}

	// The restart request itself failed (kafka-connect unreachable / unexpected status).
	// Escalate to the Healer for visibility; the cap still governs eventual termination.
	log.WithFields(log.Fields{
		"connector":   connName,
		"pipeline_id": pipelineID,
	}).Warn("🛡️ Sentinel connector restart request failed — escalating to Healer")
	s.triggerHealer(ctx, pipelineID, connName, connState)
}

// clearRestartState drops restart bookkeeping for a connector that is healthy again.
func (s *CDCSentinel) clearRestartState(connName string) {
	s.mu.Lock()
	delete(s.restartState, connName)
	s.mu.Unlock()
}

// pruneRestartState removes bookkeeping for connectors no longer reported by Connect.
func (s *CDCSentinel) pruneRestartState(seen map[string]bool) {
	s.mu.Lock()
	for connName := range s.restartState {
		if !seen[connName] {
			delete(s.restartState, connName)
		}
	}
	s.mu.Unlock()
}

// markPipelineStopped transitions a pipeline whose CDC connector is permanently FAILED out
// of 'running' so the existing reapers can reclaim its replication slot + WAL. The
// status='running' guard makes this idempotent (only the first call transitions).
func (s *CDCSentinel) markPipelineStopped(ctx context.Context, pipelineID, connName string, attempts int) {
	reason := fmt.Sprintf("CDC connector %s permanently failed after %d restart attempts (auto-stopped by Sentinel)", connName, attempts)
	res, err := s.db.ExecContext(ctx, `
		UPDATE pipelines
		SET status = 'stopped', error_message = $2, updated_at = NOW()
		WHERE id = $1 AND status = 'running'
	`, pipelineID, reason)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Error("🛡️ Sentinel failed to stop permanently-failed pipeline")
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.WithFields(log.Fields{
			"connector":   connName,
			"pipeline_id": pipelineID,
		}).Warn("🛡️ Sentinel stopped pipeline with permanently-failed CDC connector — slot/WAL will be reaped")
	}
}

var connectorShortIDRe = regexp.MustCompile(`(?i)[0-9a-f]{8}`)

func (s *CDCSentinel) findPipelineIDForConnector(ctx context.Context, connectorName string, active map[string]string) string {
	// 1) Try cdc_resources mapping (if connector name is recorded as a resource).
	var pid sql.NullString
	_ = s.db.QueryRowContext(ctx, `
		SELECT pipeline_id::text
		FROM cdc_resources
		WHERE resource_name = $1
		  AND pipeline_id IS NOT NULL
		ORDER BY created_at DESC
		LIMIT 1
	`, connectorName).Scan(&pid)
	if pid.Valid && pid.String != "" {
		return pid.String
	}

	// 2) Heuristic: connector names often include pipeline short id (first8 of UUID), e.g. debezium_<short>_<hash>.
	m := connectorShortIDRe.FindString(strings.ToLower(connectorName))
	if m != "" {
		if full, ok := active[m]; ok {
			return full
		}
	}

	return ""
}

// getDebeziumStatus calls the Debezium MCP tool to get the status of all connectors
func (s *CDCSentinel) getDebeziumStatus(ctx context.Context) (map[string]interface{}, error) {
	// We need to call the 'debezium' MCP container.
	// In the Orchestrator, we should use ServerManager to find the address.
	// For this MVP file, we'll assume we can use the ToolGenerator or direct address.
	// ACTUALLY: The best way is to assume standard Debezium MCP access via HTTP if running in docker-compose.
	// The `connector.py` binds to 8000 (or MCP_PORT).

	// Let's assume there's a service "mcp-debezium" or similar.
	// If not, we fall back to calling the underlying Kafka Connect directly since Sentinel is internal infra.
	resp, err := s.httpClient.Get(s.connectURL + "/connectors?expand=status")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var fullStatus map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&fullStatus); err != nil {
		return nil, err
	}

	// Format: { "connector-name": { "status": { "connector": {"state": "RUNNING"}, "tasks": [...] } } }

	// Kafka Connect can return shapes we don't expect: a connector mid-creation with no
	// "tasks" yet, an error object instead of a status, or a state field that isn't a
	// string. Every type assertion below uses comma-ok and skips the connector on a
	// mismatch — an unchecked assertion here would panic and, with no recover on the
	// monitoring goroutine, silently stop all CDC monitoring (SENT-A).
	results := make(map[string]interface{})
	for name, details := range fullStatus {
		detailsMap, ok := details.(map[string]interface{})
		if !ok {
			continue
		}
		statusObj, ok := detailsMap["status"].(map[string]interface{})
		if !ok {
			continue
		}
		connectorState, ok := statusObj["connector"].(map[string]interface{})
		if !ok {
			// Connector exists but has no connector-level status yet (still starting).
			continue
		}

		finalState, _ := connectorState["state"].(string)

		// Tasks may be absent (connector created, tasks not yet assigned).
		if tasks, ok := statusObj["tasks"].([]interface{}); ok {
			for _, t := range tasks {
				taskMap, ok := t.(map[string]interface{})
				if !ok {
					continue
				}
				taskState, _ := taskMap["state"].(string)
				if taskState == "FAILED" {
					finalState = "FAILED"
					// Capture trace from task if possible
					if trace, ok := taskMap["trace"]; ok {
						connectorState["trace"] = trace
					}
				}
			}
		}

		results[name] = map[string]interface{}{
			"state": finalState,
			"raw":   statusObj,
		}
	}

	return results, nil
}

// restartConnector calls the Kafka Connect REST API to restart a FAILED connector and all its tasks.
// Returns true if the restart request was ACCEPTED by Kafka Connect (HTTP 200/202/204/409), false on
// any transport error or unexpected status. "Accepted" is NOT "recovered": Connect returns 202 when
// the restart is asynchronous (includeTasks=true / onlyFailed=true — the exact query we send below),
// 204 when it schedules a synchronous restart, and 409 when the connector is mid-rebalance — in none
// of these is the connector confirmed healthy. The caller treats a true return only as "request
// accepted, awaiting a later RUNNING observation", and bounds total attempts via the restart cap
// (FINDING-04 / SENT-B).
// Kafka Connect persists connector configs in its internal Kafka topics, so this is safe to call
// without re-registering connector configuration — the connector will resume from its last offset.
func (s *CDCSentinel) restartConnector(ctx context.Context, connectorName string) bool {
	restartURL := fmt.Sprintf("%s/connectors/%s/restart?includeTasks=true&onlyFailed=true", s.connectURL, connectorName)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, restartURL, nil)
	if err != nil {
		log.WithError(err).WithField("connector", connectorName).Warn("🛡️ Failed to build connector restart request")
		return false
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.WithError(err).WithField("connector", connectorName).Warn("🛡️ Connector restart HTTP call failed (kafka-connect may be down)")
		return false
	}
	defer resp.Body.Close()

	// Kafka Connect returns 202 Accepted when the restart is asynchronous (it is, because we pass
	// includeTasks=true & onlyFailed=true), 204 No Content for a synchronous scheduled restart, and
	// 200 OK on some versions. All mean the request was accepted; none confirm recovery (that's
	// verified by a later RUNNING poll). 409 (mid-rebalance) is handled separately below.
	if resp.StatusCode == http.StatusAccepted ||
		resp.StatusCode == http.StatusNoContent ||
		resp.StatusCode == http.StatusOK {
		return true
	}
	if resp.StatusCode == http.StatusConflict {
		// 409 = connector is already rebalancing — the restart can't be issued right now,
		// but it isn't an error either. Count it as an accepted attempt and re-check next poll.
		log.WithField("connector", connectorName).Info("🛡️ Connector restart returned 409 (rebalancing) — will re-check on next poll")
		return true
	}

	log.WithFields(log.Fields{
		"connector":   connectorName,
		"http_status": resp.StatusCode,
	}).Warn("🛡️ Connector restart returned unexpected status")
	return false
}

// triggerHealer records the Sentinel's terminal verdict on a connector: it has
// failed, and restarting it did not bring it back.
//
// This used to produce to HealerDLQ, and nothing consumed that topic — the
// healer subscribes "agent.executor.requests" and deliberately does NOT take
// the ".dlq" sibling, because a payload on it is a sentinel alert rather than an
// executor task and re-running it as one would be wrong. That reasoning is
// still right; the mistake was the destination, not the refusal. So the verdict
// now lands in sentinel_active_issues, which the heal sweep reads, the
// monitoring API serves, and the self-healing panel renders.
//
// Severity is critical, not warning: unlike lag, there is no version of this
// that resolves on its own.
func (s *CDCSentinel) triggerHealer(ctx context.Context, pipelineID string, connectorName string, state interface{}) {
	description := fmt.Sprintf(
		"CDC connector %s is failed and did not recover from a restart", connectorName)

	s.emitCDCIssue(ctx, connectorIssueID(pipelineID),
		IssueTypeConnectorDown, IssueSeverityCritical,
		"connector_down", pipelineID, "", "", description,
		map[string]interface{}{
			"connector":       connectorName,
			"connector_state": state,
			"detected_by":     "cdc_sentinel",
		})

	log.WithFields(log.Fields{
		"pipeline_id": pipelineID,
		"connector":   connectorName,
	}).Error("🛡️ Sentinel raised a connector-down issue")
}

// sourceLagLoop periodically checks source-database replication lag for all active CDC pipelines.
// PostgreSQL: queries pg_replication_slots.confirmed_flush_lsn delta on the source.
// MySQL: measures real source binlog lag (current position − connector's committed
// binlog offset via SHOW BINARY LOGS), falling back to the Kafka sink consumer-lag
// proxy only when the committed offset can't be read.
func (s *CDCSentinel) sourceLagLoop() {
	interval := walDurationFromEnv("CDC_SOURCE_LAG_POLL_INTERVAL", SourceLagPollInterval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkSourceDBLag(s.ctx)
		}
	}
}

// checkSourceDBLag inspects replication lag on the source database for each running
// CDC pipeline. The two source families are discovered differently because they
// track position differently:
//
//   - PostgreSQL is discovered via cdc_resources: the replication slot is a real
//     server-side object recorded at provisioning, and its name is required to probe
//     pg_replication_slots.
//   - MySQL is discovered via the source connection: MySQL CDC records NO server-side
//     resource (the Debezium server_id is just client config, never written to
//     cdc_resources), so lag is measured from the pipeline + its source connection
//     alone (connector name = cdc-<short8>). Gating MySQL on cdc_resources — as an
//     earlier version did — meant the MySQL lag check never actually ran.
func (s *CDCSentinel) checkSourceDBLag(ctx context.Context) {
	defer recoverTick("source_lag")
	// PostgreSQL — slot-based discovery via cdc_resources.
	pgRows, err := s.db.QueryContext(ctx, `
		SELECT p.id, p.name, cr.connection_id, cr.resource_name
		FROM pipelines p
		JOIN cdc_resources cr ON cr.pipeline_id = p.id AND cr.status = 'active'
		WHERE p.status = 'running'
		  AND (p.sync_mode = 'cdc' OR p.cdc_mode IS NOT NULL)
		  AND cr.database_type = 'postgresql'
		  AND cr.resource_type = 'replication_slot'
	`)
	if err != nil {
		log.WithError(err).Debug("🛡️ sourceLagLoop: failed to query PG slot resources")
	} else {
		for pgRows.Next() {
			var pipelineID, pipelineName, connID, slotName string
			if err := pgRows.Scan(&pipelineID, &pipelineName, &connID, &slotName); err != nil {
				continue
			}
			s.checkPGSlotLag(ctx, pipelineID, pipelineName, connID, slotName)
			// Also assert the SINK is draining Kafka. checkPGSlotLag only proves
			// Debezium is consuming the WAL; a dead/wedged sink leaves the slot
			// current while nothing lands at the destination (the "green but dead"
			// gap). Raises the distinct cdc-sink-lag-* issue.
			s.checkSinkConsumerLag(ctx, pipelineID, pipelineName, "postgresql")
		}
		pgRows.Close()
	}

	// MySQL — connection-based discovery (no cdc_resource to rely on).
	myRows, err := s.db.QueryContext(ctx, `
		SELECT p.id, p.name, p.source_connection_id
		FROM pipelines p
		JOIN connections c ON c.id = p.source_connection_id
		WHERE p.status = 'running'
		  AND (p.sync_mode = 'cdc' OR p.cdc_mode IS NOT NULL)
		  AND c.connector_type ILIKE 'mysql%'
	`)
	if err != nil {
		log.WithError(err).Debug("🛡️ sourceLagLoop: failed to query MySQL CDC pipelines")
	} else {
		for myRows.Next() {
			var pipelineID, pipelineName, connID string
			if err := myRows.Scan(&pipelineID, &pipelineName, &connID); err != nil {
				continue
			}
			s.checkMySQLBinlogLag(ctx, pipelineID, pipelineName, connID)
			// Always-on sink-drain check (distinct cdc-sink-lag-* issue). The MySQL
			// binlog path only falls back to a consumer-lag proxy when the binlog offset
			// is unreadable, so a dead sink with a readable binlog offset went unnoticed.
			s.checkSinkConsumerLag(ctx, pipelineID, pipelineName, "mysql")
		}
		myRows.Close()
	}

	// Other CDC families (oracle/sqlserver/mongodb) — connection-based discovery. They have no
	// dedicated source-lag probe (checkPGSlotLag/checkMySQLBinlogLag are PG/MySQL-specific), but
	// their change events reach the destination through the SAME shared sink worker (consumer
	// group sink-<short8>), so they are equally exposed to the "green but dead sink" wedge that
	// checkSinkConsumerLag detects — and, when armed, that maybeAutoRestartSink recovers. Without
	// this pass only PG and MySQL sinks were ever checked. Family-agnostic: dbType is a label only.
	for _, tgt := range s.discoverOtherFamilyCDCSinks(ctx) {
		s.checkSinkConsumerLag(ctx, tgt.pipelineID, tgt.pipelineName, tgt.dbType)
	}
}

// cdcSinkTarget identifies a running CDC pipeline that needs the family-agnostic sink-drain
// check but has no dedicated source-lag probe (oracle/sqlserver/mongodb). dbType is a display
// label only — the sink check and auto-restart rung key on the pipeline id and mode='cdc'.
type cdcSinkTarget struct {
	pipelineID   string
	pipelineName string
	dbType       string
}

// discoverOtherFamilyCDCSinks returns the running CDC pipelines whose SOURCE connector family is
// oracle, SQL Server, or MongoDB. PostgreSQL (discovered via cdc_resources) and MySQL (via the
// source connection) already run checkSinkConsumerLag inline from their source-lag branches in
// checkSourceDBLag; these three families have no source-lag probe, so without this discovery
// their sinks were never checked for the "green but dead sink" wedge (nor auto-restarted). The
// discovery is connection-based (like MySQL) because oracle/sqlserver/mongodb record no
// PostgreSQL-style server-side resource in cdc_resources. connector_type is normalized purely for
// the log/issue label; the sink-drain check itself is source-family-agnostic.
func (s *CDCSentinel) discoverOtherFamilyCDCSinks(ctx context.Context) []cdcSinkTarget {
	rows, err := s.db.QueryContext(ctx, `
		SELECT p.id, p.name, c.connector_type
		FROM pipelines p
		JOIN connections c ON c.id = p.source_connection_id
		WHERE p.status = 'running'
		  AND (p.sync_mode = 'cdc' OR p.cdc_mode IS NOT NULL)
		  AND (   c.connector_type ILIKE 'oracle%'
		       OR c.connector_type ILIKE 'sqlserver%'
		       OR c.connector_type ILIKE 'mssql%'
		       OR c.connector_type ILIKE 'mongodb%'
		       OR c.connector_type ILIKE 'mongo%')
	`)
	if err != nil {
		log.WithError(err).Debug("🛡️ sourceLagLoop: failed to query other-family CDC pipelines")
		return nil
	}
	defer rows.Close()

	var targets []cdcSinkTarget
	for rows.Next() {
		var pipelineID, pipelineName, connectorType string
		if err := rows.Scan(&pipelineID, &pipelineName, &connectorType); err != nil {
			continue
		}
		targets = append(targets, cdcSinkTarget{
			pipelineID:   pipelineID,
			pipelineName: pipelineName,
			dbType:       cdc.NormalizeDBType(connectorType),
		})
	}
	return targets
}

// checkPGSlotLag connects to the source PostgreSQL and measures replication slot lag in bytes.
func (s *CDCSentinel) checkPGSlotLag(ctx context.Context, pipelineID, pipelineName, connectionID, slotName string) {
	mgr := connections.NewManager(s.db)
	cfg, err := mgr.Get(ctx, connectionID)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Debug("🛡️ PG lag: failed to get connection config")
		return
	}

	host := cfg["host"]
	port := 5432
	if p := strings.TrimSpace(cfg["port"]); p != "" {
		if n, err2 := fmt.Sscanf(p, "%d", &port); n == 0 || err2 != nil {
			port = 5432
		}
	}
	database := cfg["database"]
	if database == "" {
		database = cfg["db_name"]
	}
	user := cfg["user"]
	password := cfg["password"]
	// Host-aware SSL: stored PG connections carry no sslmode, so a bare
	// "disable" default made lag-probing a managed source (Azure/RDS) fail.
	sslmode := cdc.ResolvePostgresSSLMode(
		map[string]interface{}{"sslmode": cfg["sslmode"], "ssl_mode": cfg["ssl_mode"]}, host)

	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s connect_timeout=5",
		host, port, database, user, password, sslmode)

	srcDB, err := sql.Open("postgres", connStr)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Debug("🛡️ PG lag: failed to open connection")
		return
	}
	defer srcDB.Close()

	var lagBytes int64
	err = srcDB.QueryRowContext(ctx,
		`SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)
		 FROM pg_replication_slots WHERE slot_name = $1`, slotName).Scan(&lagBytes)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"slot_name":   slotName,
		}).Debug("🛡️ PG lag: slot query failed (slot may not exist yet)")
		return
	}

	lagMB := float64(lagBytes) / (1024 * 1024)
	log.WithFields(log.Fields{
		"pipeline_id": pipelineID,
		"slot":        slotName,
		"lag_mb":      fmt.Sprintf("%.1f", lagMB),
	}).Debug("🛡️ PG replication slot lag")

	if lagBytes > walBytesFromEnv("CDC_PG_SLOT_LAG_BYTES", PGSlotLagAlertBytes) {
		s.emitSourceLagAlert(ctx, pipelineID, pipelineName, "postgresql",
			fmt.Sprintf("Replication slot lag is %.1f MB (slot: %s) — source changes are not being consumed fast enough", lagMB, slotName),
			map[string]interface{}{
				"lag_bytes": lagBytes,
				"lag_mb":    lagMB,
				"slot_name": slotName,
			})
	} else {
		s.resolveSourceLagAlert(ctx, pipelineID, "postgresql")
	}
}

// checkMySQLBinlogLag measures real source-side MySQL binlog lag — the number of
// bytes of binlog the source has produced beyond the Debezium connector's committed
// position — mirroring the PostgreSQL slot-lag probe (checkPGSlotLag). It reads the
// connector's committed binlog coordinate from the Kafka Connect offsets REST API
// (KIP-875, Kafka Connect 3.6+) and compares it against the source's current binlog
// position via SHOW BINARY LOGS. If the committed offset can't be read (connector not
// started yet, offset purged, or an older Kafka Connect), it falls back to the Kafka
// consumer-lag proxy on the CDC sink consumer group.
func (s *CDCSentinel) checkMySQLBinlogLag(ctx context.Context, pipelineID, pipelineName, connectionID string) {
	// Connector name must match the executor's naming byte-for-byte (cdc-<short8>).
	connectorName := "cdc-" + utils.SafeID8(pipelineID)

	committed, err := s.getConnectorCommittedBinlog(ctx, connectorName)
	if err == nil && !committed.IsZero() {
		lagBytes, current, lerr := cdc.NewMySQLManager(s.db).BinlogLagBytes(ctx, connectionID, committed)
		if lerr == nil {
			lagMB := float64(lagBytes) / (1024 * 1024)
			log.WithFields(log.Fields{
				"pipeline_id":    pipelineID,
				"connector":      connectorName,
				"committed_file": committed.File,
				"current_file":   current.File,
				"lag_mb":         fmt.Sprintf("%.1f", lagMB),
			}).Debug("🛡️ MySQL source binlog lag")

			if lagBytes > walBytesFromEnv("CDC_MYSQL_BINLOG_LAG_BYTES", MySQLBinlogLagAlertBytes) {
				s.emitSourceLagAlert(ctx, pipelineID, pipelineName, "mysql",
					fmt.Sprintf("Source binlog lag is %.1f MB (connector: %s) — source changes are not being captured fast enough", lagMB, connectorName),
					map[string]interface{}{
						"lag_bytes":      lagBytes,
						"lag_mb":         lagMB,
						"measure":        "binlog_bytes",
						"committed_file": committed.File,
						"committed_pos":  committed.Pos,
						"current_file":   current.File,
						"current_pos":    current.Pos,
					})
			} else {
				s.resolveSourceLagAlert(ctx, pipelineID, "mysql")
			}
			return
		}
		log.WithError(lerr).WithField("pipeline_id", pipelineID).
			Debug("🛡️ MySQL lag: source binlog byte-lag unavailable; falling back to consumer-lag proxy")
	} else if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).
			Debug("🛡️ MySQL lag: committed offset unavailable; falling back to consumer-lag proxy")
	}

	// Fallback: Kafka consumer-lag proxy on the CDC sink consumer group.
	s.checkMySQLConsumerLagProxy(ctx, pipelineID, pipelineName)
}

// getConnectorCommittedBinlog reads a Debezium MySQL connector's last-committed
// binlog coordinate from the Kafka Connect offsets REST API
// (GET /connectors/<name>/offsets, KIP-875). Returns a zero BinlogPosition (and a
// non-nil error) when the connector has no offset yet or the endpoint is unavailable.
func (s *CDCSentinel) getConnectorCommittedBinlog(ctx context.Context, connectorName string) (cdc.BinlogPosition, error) {
	url := fmt.Sprintf("%s/connectors/%s/offsets", s.connectURL, connectorName)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return cdc.BinlogPosition{}, err
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return cdc.BinlogPosition{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return cdc.BinlogPosition{}, fmt.Errorf("offsets endpoint returned %d", resp.StatusCode)
	}

	var payload struct {
		Offsets []struct {
			Partition map[string]interface{} `json:"partition"`
			Offset    map[string]interface{} `json:"offset"`
		} `json:"offsets"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return cdc.BinlogPosition{}, err
	}

	// A MySQL source connector has a single source partition; pick the offset row
	// that actually carries a binlog file.
	for _, o := range payload.Offsets {
		file, _ := o.Offset["file"].(string)
		if strings.TrimSpace(file) == "" {
			continue
		}
		bp := cdc.BinlogPosition{File: strings.TrimSpace(file)}
		switch v := o.Offset["pos"].(type) {
		case float64:
			bp.Pos = uint64(v)
		case json.Number:
			if n, e := v.Int64(); e == nil {
				bp.Pos = uint64(n)
			}
		}
		if g, ok := o.Offset["gtids"].(string); ok {
			bp.GTID = g
		}
		return bp, nil
	}
	return cdc.BinlogPosition{}, fmt.Errorf("no MySQL binlog offset present for connector %s", connectorName)
}

// checkMySQLConsumerLagProxy checks Kafka consumer lag on the CDC sink consumer group
// as a fallback proxy for MySQL processing lag when the real source-side binlog lag
// can't be computed.
func (s *CDCSentinel) checkMySQLConsumerLagProxy(ctx context.Context, pipelineID, pipelineName string) {
	if s.kafkaManager == nil {
		return
	}

	// Ask the manifest, don't guess. This site additionally hand-sliced pipelineID[:8]
	// instead of using SafeID8, so it disagreed with the executor for any id shorter
	// than 8 chars as well as for the two non-default naming shapes.
	consumerGroup := s.resolveSinkConsumerGroup(ctx, pipelineID)

	lagMap, err := s.kafkaManager.GetConsumerGroupLag(consumerGroup)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id":    pipelineID,
			"consumer_group": consumerGroup,
		}).Debug("🛡️ MySQL lag: could not get Kafka consumer lag")
		return
	}

	var totalLag int64
	for _, topicLag := range lagMap {
		totalLag += topicLag
	}

	log.WithFields(log.Fields{
		"pipeline_id":    pipelineID,
		"consumer_group": consumerGroup,
		"total_lag":      totalLag,
	}).Debug("🛡️ MySQL CDC Kafka consumer lag")

	if totalLag > MySQLKafkaLagAlert {
		s.emitSourceLagAlert(ctx, pipelineID, pipelineName, "mysql",
			fmt.Sprintf("CDC Kafka consumer lag is %d events (group: %s) — binlog events are not being written to destination fast enough", totalLag, consumerGroup),
			map[string]interface{}{
				"total_lag":      totalLag,
				"consumer_group": consumerGroup,
				"topics":         lagMap,
			})
	} else {
		s.resolveSourceLagAlert(ctx, pipelineID, "mysql")
	}
}

// sinkConsumerGroup is the DERIVED fallback name, kept as a thin alias so the existing
// call sites and tests in this package keep reading naturally. The single definition of
// the "sink-" prefix now lives in handlers.
func sinkConsumerGroup(pipelineID string) string {
	return handlers.DerivedSinkConsumerGroup(pipelineID)
}

// resolveSinkConsumerGroup returns the consumer group the sink ACTUALLY registered,
// falling back to the derived name above.
//
// The derived name is only correct for the default CDC shape. The executor picks the
// group from the pipeline's mode (executor.go, "Distinct consumer group per phase"):
//
//	default CDC                        sink-<pid8>
//	batch-backfill topic               sink-<pid8>-batch
//	cdc_mode streaming_only | never    sink-<pid8>-<eid8>
//
// For the last two, sinkConsumerGroup() names a group that has never existed. The lag
// probe then gets an error or an empty map from a non-existent group — which it treats
// as "can't tell, skip this tick" — so a streaming_only pipeline whose sink is dead
// looks exactly like one whose sink is perfectly healthy, forever. That is the same
// dead-sink blindness checkSinkConsumerLag was written to close, reintroduced through
// the group name.
//
// Rather than mirror the executor's three-way branch here (a second copy of the naming
// rule that can drift from the first — the failure mode this whole function exists to
// fix), read what the executor recorded. dependency_manifest.go registers the sink with
// kind='kafka_sink_worker' and identifier=<the consumer group it actually used>, so the
// manifest is authoritative by construction. Newest row wins; execution-scoped rows sort
// ahead of the NULL-execution ones.
// It delegates to handlers.ResolveSinkConsumerGroup rather than keeping a second copy of
// the manifest query: the restart path in that package must resolve the SAME name, and two
// implementations of "which group is this?" is precisely the class of bug this closes.
func (s *CDCSentinel) resolveSinkConsumerGroup(ctx context.Context, pipelineID string) string {
	return handlers.ResolveSinkConsumerGroup(ctx, s.db, pipelineID)
}

// sinkLagIssueID keys the sink-drain lag issue. Deliberately distinct from the
// source-lag issue ("cdc-lag-%s") so the two concerns never resolve/overwrite each
// other — a healthy source slot must not clear a dead-sink alarm.
func sinkLagIssueID(pipelineID string) string {
	return fmt.Sprintf("cdc-sink-lag-%s", pipelineID)
}

// connectorIssueID keys the terminal connector-down escalation, and is a third
// class again: neither lag resolver may clear it. A connector that is FAILED
// while its consumer lag happens to look fine is exactly the case this exists
// for, so sharing an id — or a prefix — with either lag class would delete the
// alarm at the worst possible moment.
func connectorIssueID(pipelineID string) string {
	return fmt.Sprintf("cdc-connector-down-%s", pipelineID)
}

// checkSinkConsumerLag alarms when the CDC SINK is not draining Kafka to the
// destination — the "green but dead sink" gap. It is DISTINCT from the source-side
// checks (checkPGSlotLag / checkMySQLBinlogLag), which only tell us whether Debezium
// is keeping up with the source: a sink that has died, wedged, or crash-looped leaves
// the source slot/binlog perfectly current (Debezium keeps consuming) while ZERO rows
// reach the destination — so those checks stay green. This reads the authoritative
// broker-side lag on the sink's own consumer group and raises a SEPARATE issue
// (cdc-sink-lag-*) so it never stomps or is stomped by the source-lag issue
// (cdc-lag-*). Runs for every running CDC pipeline regardless of source family: PG had
// no sink check at all, and the MySQL binlog path only fell back to a consumer-lag
// proxy when the binlog offset was unreadable — so both were blind to a dead sink
// whenever source lag looked fine.
func (s *CDCSentinel) checkSinkConsumerLag(ctx context.Context, pipelineID, pipelineName, dbType string) {
	// Presence first, and deliberately ABOVE the kafkaManager guard: a sink worker that no
	// longer exists is invisible to every lag signal below (the consumer group keeps its
	// committed offsets, and on a quiet stream the lag is legitimately 0), so the drain
	// checks can never detect it. This asks the sink container directly. See
	// cdc_sink_respawn.go for why it needs no feature flag.
	s.ensureSinkWorkerPresent(ctx, pipelineID, pipelineName, dbType)

	if s.kafkaManager == nil {
		return
	}
	consumerGroup := s.resolveSinkConsumerGroup(ctx, pipelineID)
	issueID := sinkLagIssueID(pipelineID)

	lagMap, err := s.kafkaManager.GetConsumerGroupLag(consumerGroup)
	if err != nil {
		// The group may not exist yet (sink not started / never consumed). Absence is
		// not proof of health, so do NOT resolve — just skip this tick.
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id":    pipelineID,
			"consumer_group": consumerGroup,
		}).Debug("🛡️ sink lag: could not get Kafka consumer lag")
		return
	}

	var totalLag int64
	for _, topicLag := range lagMap {
		totalLag += topicLag
	}

	log.WithFields(log.Fields{
		"pipeline_id":    pipelineID,
		"consumer_group": consumerGroup,
		"total_lag":      totalLag,
		"db_type":        dbType,
	}).Debug("🛡️ CDC sink consumer lag")

	lagging := totalLag > walBytesFromEnv("CDC_SINK_KAFKA_LAG_ALERT", SinkKafkaLagAlert)
	if lagging {
		s.emitLagIssue(ctx, issueID, "sink_lag", pipelineID, pipelineName, dbType,
			fmt.Sprintf("CDC sink is not draining Kafka: consumer lag is %d events (group: %s) — change events are captured but NOT reaching the destination (the sink worker may be dead, wedged, or crash-looping)", totalLag, consumerGroup),
			map[string]interface{}{
				"total_lag":      totalLag,
				"consumer_group": consumerGroup,
				"topics":         lagMap,
			})
	} else {
		s.resolveLagIssue(ctx, issueID, pipelineID)
	}

	// Phase B — autonomous sink wedge-recovery. This runs on the CORRECTED, topic-scoped lag
	// signal (Phase A), so `lagging` here is real backlog on the sink's OWN topics, never the
	// old cluster-wide phantom. maybeAutoRestartSink is gated behind CDC_SINK_AUTORESTART_ENABLED
	// (default off everywhere) AND a genuine three-signal wedge (lag + stale destination apply +
	// flat destination progress), so it can never restart a healthy or merely-slow sink. It is a
	// no-op unless the flag is on and the MCP manager has been plumbed in.
	s.maybeAutoRestartSink(ctx, pipelineID, pipelineName, dbType, lagging)
}

// emitSourceLagAlert writes a SOURCE-side lag alert (Debezium not keeping up with the
// source WAL/binlog) to sentinel_active_issues and emits a domain event.
func (s *CDCSentinel) emitSourceLagAlert(ctx context.Context, pipelineID, pipelineName, dbType, description string, metadata map[string]interface{}) {
	s.emitLagIssue(ctx, fmt.Sprintf("cdc-lag-%s", pipelineID), "source_lag", pipelineID, pipelineName, dbType, description, metadata)
}

// emitLagIssue upserts a CDC lag issue into sentinel_active_issues and emits a
// SENTINEL_ALERT domain event. issueID keys the dedup, so two DISTINCT concerns —
// source-side WAL/binlog lag (cdc-lag-*) and sink-side Kafka-drain lag
// (cdc-sink-lag-*) — never overwrite or resolve each other. alertType ("source_lag"
// or "sink_lag") is surfaced in the event metadata for the UI/consumers.
func (s *CDCSentinel) emitLagIssue(ctx context.Context, issueID, alertType, pipelineID, pipelineName, dbType, description string, metadata map[string]interface{}) {
	s.emitCDCIssue(ctx, issueID, IssueTypeHighLag, IssueSeverityWarning,
		alertType, pipelineID, pipelineName, dbType, description, metadata)
}

// emitCDCIssue upserts any CDC-pipeline issue into sentinel_active_issues and
// emits the matching SENTINEL_ALERT domain event. Split out of emitLagIssue so
// the connector-down escalation writes through exactly the same path the lag
// alarms do — same table, same dedup-on-id, same event shape — instead of
// inventing a second way to report a problem.
func (s *CDCSentinel) emitCDCIssue(
	ctx context.Context,
	issueID string,
	issueType IssueType,
	severity IssueSeverity,
	alertType, pipelineID, pipelineName, dbType, description string,
	metadata map[string]interface{},
) {
	metaJSON, _ := json.Marshal(metadata)

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sentinel_active_issues (
			id, type, severity, component_id, component_type,
			description, detected_at, occurrence_count, last_occurrence, metadata
		) VALUES ($1, $2, $3, $4, $5, $6, NOW(), 1, NOW(), $7)
		ON CONFLICT (id) DO UPDATE SET
			occurrence_count = sentinel_active_issues.occurrence_count + 1,
			last_occurrence  = NOW(),
			severity         = EXCLUDED.severity,
			description      = EXCLUDED.description,
			metadata         = EXCLUDED.metadata
	`, issueID, string(issueType), string(severity),
		pipelineID, string(ComponentTypeCDCPipeline), description, metaJSON)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"pipeline_id": pipelineID,
			"issue_id":    issueID,
		}).Warn("🛡️ Failed to persist CDC issue")
	}

	// A nil manager is the unit-test shape (and the pre-wiring startup window);
	// the row above is the durable half, so there is nothing to fail here.
	if s.kafkaManager == nil {
		return
	}

	status := "warning"
	if severity == IssueSeverityCritical {
		status = "error"
	}

	// Also emit a domain event so the pipeline overview picks it up.
	event := map[string]interface{}{
		"schema_version": 2,
		"event_type":     "SENTINEL_ALERT",
		"pipeline_id":    pipelineID,
		"execution_id":   "",
		"trace_id":       pipelineID,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"stage":          "cdc_sentinel",
		"stage_group":    "monitoring",
		"status":         status,
		"message":        description,
		"metadata": map[string]interface{}{
			"source":      "cdc_sentinel",
			"db_type":     dbType,
			"alert_type":  alertType,
			"pipeline_id": pipelineID,
			"details":     metadata,
		},
	}
	b, _ := json.Marshal(event)
	_ = s.kafkaManager.ProduceWithHeaders("pipeline.domain.events", []byte(pipelineID), b, map[string]string{
		"trace_id": pipelineID,
	})

	log.WithFields(log.Fields{
		"pipeline_id": pipelineID,
		"db_type":     dbType,
		"alert_type":  alertType,
	}).Warn("🛡️ CDC sentinel alert emitted")
}

// resolveSourceLagAlert removes the SOURCE-side lag issue from sentinel_active_issues
// once source lag is back to normal.
func (s *CDCSentinel) resolveSourceLagAlert(ctx context.Context, pipelineID, dbType string) {
	s.resolveLagIssue(ctx, fmt.Sprintf("cdc-lag-%s", pipelineID), pipelineID)
}

// resolveLagIssue deletes a specific lag issue by id (source_lag or sink_lag) once it
// is back to normal. Keyed by issueID so resolving one concern never clears the other.
func (s *CDCSentinel) resolveLagIssue(ctx context.Context, issueID, pipelineID string) {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM sentinel_active_issues WHERE id = $1`, issueID)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Debug("🛡️ resolveLagIssue: delete failed (may not exist)")
	}
}

