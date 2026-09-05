package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rsync-ai/shared/kafkaclient"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/rsync-ai/shared/pgdriver"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"

	"github.com/rsync-ai/backend-orchestrator/internal/agents/cdcstats"
	"github.com/rsync-ai/backend-orchestrator/internal/agents/consumer"
	"github.com/rsync-ai/backend-orchestrator/internal/agents/executor" // Legacy: Used for HTTP endpoints only
	"github.com/rsync-ai/backend-orchestrator/internal/agents/heal"
	"github.com/rsync-ai/backend-orchestrator/internal/agents/healthwatch"
	"github.com/rsync-ai/backend-orchestrator/internal/agents/retention"
	"github.com/rsync-ai/backend-orchestrator/internal/agents/sentinel"
	"github.com/rsync-ai/backend-orchestrator/internal/assessor"
	"github.com/rsync-ai/backend-orchestrator/internal/config"
	"github.com/rsync-ai/backend-orchestrator/internal/connections"
	"github.com/rsync-ai/backend-orchestrator/internal/handlers"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"

	// "github.com/rsync-ai/backend-orchestrator/internal/scheduler" // Deprecated
	"github.com/rsync-ai/backend-orchestrator/internal/telemetry"
	"github.com/rsync-ai/backend-orchestrator/internal/temporalworker"
	"github.com/rsync-ai/backend-orchestrator/internal/workers"
)

const (
	Version = "1.0.0"
)

// Global config reference
var cfg *config.Config

// localDatabaseHosts are hostnames that only ever point at an in-cluster dev
// Postgres — never a real remote (Azure) database. docker-compose substitutes
// the dev "postgres" service when the remote host is unset.
var localDatabaseHosts = map[string]bool{
	"postgres":  true, // docker-compose service name for the dev Postgres
	"localhost": true,
	"127.0.0.1": true,
	"::1":       true,
	"[::1]":     true,
}

// remoteDatabaseViolation returns a human-readable reason when a deployment that
// declared it requires a remote database (requireRemote, wired from
// RSYNC_REQUIRE_REMOTE_DB) is instead pointed at a local/in-cluster Postgres
// host. An empty return means OK. Pure + side-effect-free so it can be
// unit-tested; requireRemoteDatabase is the thin log.Fatalf wrapper.
func remoteDatabaseViolation(requireRemote bool, host string) string {
	if !requireRemote {
		return ""
	}
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" {
		return "orchestrator DB host is empty"
	}
	if localDatabaseHosts[h] {
		return fmt.Sprintf("orchestrator DB host %q is the local dev Postgres", h)
	}
	return ""
}

// requireRemoteDatabase crashes the orchestrator at startup when it is wired to
// the local dev Postgres but the deployment declared it must be remote
// (RSYNC_REQUIRE_REMOTE_DB, set by docker-compose.prod.yml and inherited by the
// staging overlay). This mirrors the api-gateway + temporal-adapter fail-loud
// guard: docker-compose substitutes the in-cluster dev DB when the remote host
// is unset (e.g. the staging stack relaunched without `--env-file .env.staging`,
// as a CI job does), so the orchestrator would otherwise come up healthy but
// pointed at a database that holds none of the real pipelines. Far better to
// never start. It is intentionally NOT gated on ENVIRONMENT — the staging overlay
// sets ENVIRONMENT=local while still requiring a remote DB, so an ENVIRONMENT
// check would miss exactly the case that bit us.
func requireRemoteDatabase(host string) {
	requireRemote := false
	switch strings.ToLower(strings.TrimSpace(os.Getenv("RSYNC_REQUIRE_REMOTE_DB"))) {
	case "1", "true", "yes", "on":
		requireRemote = true
	}
	if reason := remoteDatabaseViolation(requireRemote, host); reason != "" {
		log.Fatalf("❌ Refusing to start: %s, but this deployment requires a remote database "+
			"(RSYNC_REQUIRE_REMOTE_DB is set). The stack was almost certainly launched without "+
			"`--env-file .env.staging` — point DB_HOST at the real (Azure) database and relaunch.", reason)
	}
}

// cdcControlOutcome maps the status Kafka Connect returned for a CDC control
// action ("restart" / "pause" / "resume") onto the HTTP status the orchestrator
// answers with, plus the operator-facing message when Connect refused.
//
// These three handlers used to answer 200 unconditionally and carry the real
// verdict only in the body's `success` field. api-gateway forwards the upstream
// status verbatim (pipeline_cdc.go `c.Data(resp.StatusCode, …)`) and the UI
// branches on `response.ok` (CDCPipelineActions.tsx), so a refused restart still
// painted a green "CDC connector restarted" toast while the connector carried on
// doing exactly what it had been doing — KI-CDC-CONTROL-ACTIONS-FALSE-SUCCESS-TOAST.
// Answering 502 is what makes the failure reach the operator. Pure so it can be
// unit-tested; the handlers themselves are inline closures on the route group.
func cdcControlOutcome(action, connectorName string, connectStatus int) (ok bool, httpStatus int, errMsg string) {
	if connectStatus >= 200 && connectStatus < 300 {
		return true, http.StatusOK, ""
	}
	return false, http.StatusBadGateway,
		fmt.Sprintf("kafka connect refused the %s of %s (HTTP %d)", action, connectorName, connectStatus)
}

func init() {
	// Load configuration using Viper
	var err error
	cfg, err = config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Setup logging based on config
	setupLogging(cfg)

	// Fail fast on insecure defaults in production.
	{
		env := strings.ToLower(strings.TrimSpace(cfg.Server.Environment))
		isProd := env == "production" || env == "prod"
		if isProd {
			jwt := strings.TrimSpace(os.Getenv("JWT_SECRET"))
			if jwt == "" || jwt == "dev_secret_key_change_in_prod" || len(jwt) < 32 {
				log.Fatalf("Missing/unsafe JWT_SECRET for production (must be >= 32 chars and not a dev default)")
			}
		}
		// ENCRYPTION_KEY must be a real key whenever the deployment declares it is a
		// real remote deployment (RSYNC_REQUIRE_REMOTE_DB) — NOT only when
		// ENVIRONMENT=production. The staging overlay only overrides api-gateway's
		// ENVIRONMENT (to local); the orchestrator here inherits ENVIRONMENT=development,
		// so a prod-only gate skipped staging entirely and let the docker-compose
		// dev-key fallback through, making connection configs undecryptable across
		// services. Mirror requireRemoteDatabase's deploy-intent gate.
		requireRealKey := isProd
		switch strings.ToLower(strings.TrimSpace(os.Getenv("RSYNC_REQUIRE_REMOTE_DB"))) {
		case "1", "true", "yes", "on":
			requireRealKey = true
		}
		if requireRealKey {
			// Support key rotation via ENCRYPTION_KEYS (comma-separated). First entry is primary.
			encKeys := strings.TrimSpace(os.Getenv("ENCRYPTION_KEYS"))
			if encKeys != "" {
				primary := ""
				for _, p := range strings.Split(encKeys, ",") {
					s := strings.TrimSpace(p)
					if s != "" {
						primary = s
						break
					}
				}
				if primary == "" || primary == "dev-encryption-key-32-bytes-long!!" || primary == "dev-only-key-please-change-me!!!" || len(primary) < 32 {
					log.Fatalf("Missing/unsafe ENCRYPTION_KEYS primary (must be >= 32 chars and not a dev default) for a remote-DB deployment")
				}
			} else {
				enc := strings.TrimSpace(os.Getenv("ENCRYPTION_KEY"))
				if enc == "" || enc == "dev-encryption-key-32-bytes-long!!" || enc == "dev-only-key-please-change-me!!!" || len(enc) < 32 {
					log.Fatalf("Missing/unsafe ENCRYPTION_KEY (must be >= 32 chars and not a dev default) for a remote-DB deployment")
				}
			}
		}
	}
}

// setupLogging configures logrus based on configuration with trace context support
func setupLogging(cfg *config.Config) {
	// JSON format for production/docker, text for development
	if cfg.Server.LogFormat == "json" || cfg.Server.Environment == "production" || cfg.Server.Environment == "docker" {
		log.SetFormatter(&log.JSONFormatter{
			TimestampFormat: "2006-01-02T15:04:05.000Z07:00",
			FieldMap: log.FieldMap{
				log.FieldKeyTime:  "timestamp",
				log.FieldKeyLevel: "level",
				log.FieldKeyMsg:   "message",
			},
		})
	} else {
		log.SetFormatter(&log.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02T15:04:05.000",
		})
	}

	// Set log level
	switch cfg.Server.LogLevel {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "warn":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.SetLevel(log.InfoLevel)
	}

	// Override with debug flag
	if cfg.Server.Debug {
		log.SetLevel(log.DebugLevel)
	}

	// Add service field hook
	log.AddHook(&ServiceFieldHook{ServiceName: cfg.Telemetry.ServiceName})

	// Add trace context hook for log-trace correlation in SigNoz
	log.AddHook(telemetry.NewTraceContextHook())

	log.Info("✅ Logging initialized with TraceContext hook (log-trace correlation enabled)")
}

// emitCDCStatusMetrics emits a best-effort DATA_PLANE_METRICS event derived from CDC status checks.
// This gives the UI a real-time stream (via pipeline.domain.events) without requiring connectors to be modified.
// It now includes CDC lag/freshness metrics computed from Kafka consumer group offsets.
func emitCDCStatusMetrics(ctx context.Context, kafkaManager *kafka.Manager, db *sql.DB, pipelineID string, connectorName string, resp executor.ExecutorResponse) {
	if kafkaManager == nil || pipelineID == "" {
		return
	}

	traceID := telemetry.TraceIDFromContext(ctx)
	if strings.TrimSpace(traceID) == "" {
		traceID = pipelineID
	}

	result := resp.Result
	if result == nil {
		result = map[string]interface{}{}
	}

	// Compute CDC lag and freshness from Kafka consumer group offsets.
	//
	// Ask the manifest which group the sink actually registered rather than
	// re-deriving one here. The hand-rolled "sink-<first8(pipeline_id)>" that used to
	// sit on this line was wrong twice over, and both errors are invisible from here:
	//
	//   - It is unqualified. The executor mints the group through kafkaclient.Group
	//     (executor/sink_consumer_group.go:63-77), so the group that exists on the
	//     broker is "rsync.sink-<pid8>" at the DEFAULT prefix -- this is a mismatch at
	//     every prefix, not only a custom one.
	//   - It assumes the majority CDC shape. Batch and streaming_only sinks are
	//     "-batch" / "-stream" / "-<eid8>", so it named a group that never existed for
	//     any of them.
	//
	// GetConsumerGroupLag does not qualify on the caller's behalf -- it hands the id
	// straight to sarama (kafka/manager.go:1046-1061) -- and a group that never
	// committed yields an empty map rather than an error. The call below is guarded on
	// len(lagByTopic) > 0, so every one of those misses left cdcLagMs, cdcFreshnessMs,
	// rowsProcessed and bytesProcessed nil forever, and the UI renders a CDC pipeline
	// with no lag and no throughput exactly like a healthy idle one.
	//
	// ResolveSinkConsumerGroup reads the identifier the executor recorded when it
	// started the sink, which is authoritative by construction; it falls back to the
	// legacy bare name only when no manifest row exists.
	sinkGroupID := handlers.ResolveSinkConsumerGroup(ctx, db, pipelineID)

	var cdcLagMs *int64
	var cdcFreshnessMs *int64
	var rowsProcessed *int64
	var bytesProcessed *int64

	// Best-effort: fetch consumer group lag
	lagByTopic, err := kafkaManager.GetConsumerGroupLag(sinkGroupID)
	if err == nil && len(lagByTopic) > 0 {
		// Sum lag across all topics (usually just one topic per pipeline)
		var totalLag int64
		for _, lag := range lagByTopic {
			totalLag += lag
		}
		// Approximate lag in milliseconds (assume 1 message = 1ms, very rough)
		// In a real system, you'd compute this from Kafka timestamps.
		lagMs := totalLag * 10 // Rough heuristic: 10ms per message lag
		cdcLagMs = &lagMs

		// Freshness: if lag is 0, freshness is ~0; else it's proportional to lag.
		freshnessMs := lagMs
		cdcFreshnessMs = &freshnessMs

		// Estimate rows processed from committed offsets (very rough)
		// In a real system, you'd track this in the sink or via Kafka metrics.
		estimatedRows := int64(0)
		for _, lag := range lagByTopic {
			// If lag is X, assume we've processed (high_water_mark - lag) messages.
			// This is a placeholder; real implementation would query Kafka offsets.
			estimatedRows += (lag * 10) // Placeholder multiplier
		}
		if estimatedRows > 0 {
			rowsProcessed = &estimatedRows
		}
	}

	event := map[string]interface{}{
		"schema_version": 2,
		"event_type":     "DATA_PLANE_METRICS",
		"pipeline_id":    pipelineID,
		"execution_id":   "", // CDC pipelines may not have Temporal execution IDs
		"trace_id":       traceID,
		"timestamp":      time.Now().UTC().Format(time.RFC3339),
		"stage":          "cdc",
		"stage_group":    "executing",
		"status":         "processing",
		"message":        "CDC data plane metrics update",
		"metadata": map[string]interface{}{
			"source":           "cdc_status_poll",
			"metrics_schema":   "v2", // Standardized schema version
			"connector_name":   connectorName,
			"rows_processed":   rowsProcessed,
			"bytes_processed":  bytesProcessed,
			"cdc_lag_ms":       cdcLagMs,
			"cdc_freshness_ms": cdcFreshnessMs,
			"health_status":    result["health_status"],
			"connector_state":  result["connector_state"],
			"task_states":      result["task_states"],
		},
	}

	b, err := json.Marshal(event)
	if err != nil {
		log.WithError(err).WithField("pipeline_id", pipelineID).Warn("emitCDCStatusMetricsEvent: marshal failed")
		return
	}

	if err := kafkaManager.ProduceWithHeadersAndContext(ctx, "pipeline.domain.events", []byte(pipelineID), b, map[string]string{
		"trace_id": traceID,
	}); err != nil {
		log.WithError(err).
			WithFields(log.Fields{"pipeline_id": pipelineID, "trace_id": traceID}).
			Warn("emitCDCStatusMetricsEvent: kafka produce failed")
	}
}

// ServiceFieldHook adds service name to all log entries
type ServiceFieldHook struct {
	ServiceName string
}

func (h *ServiceFieldHook) Levels() []log.Level {
	return log.AllLevels
}

func (h *ServiceFieldHook) Fire(entry *log.Entry) error {
	entry.Data["service"] = h.ServiceName
	return nil
}

func main() {
	log.Info("================================================================================")
	log.Info("🚀 RSYNC AI Go Orchestrator Starting")
	log.Info("================================================================================")
	log.Infof("Version: %s", Version)
	log.Infof("Port: %s", cfg.Server.Port)
	log.Infof("Environment: %s", cfg.Server.Environment)

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Initialize OpenTelemetry with config
	var shutdownTracer func(context.Context) error
	if cfg.Telemetry.Enabled {
		// Initialize trace context hook for log-trace correlation
		// This must be done before InitTracerWithConfig so logs from that function are also captured
		telemetry.InitLogrusWithTraceHook()

		var err error
		shutdownTracer, err = telemetry.InitTracerWithConfig(telemetry.TelemetryConfig{
			OTLPEndpoint:   cfg.Telemetry.OTLPEndpoint,
			ServiceName:    cfg.Telemetry.ServiceName,
			ServiceVersion: cfg.Telemetry.ServiceVersion,
			Enabled:        cfg.Telemetry.Enabled,
			SamplingRate:   cfg.Telemetry.SamplingRate,
			Insecure:       cfg.Telemetry.Insecure,
		})
		if err != nil {
			log.Warnf("⚠️  Failed to initialize telemetry: %v", err)
		} else {
			defer func() {
				if err := shutdownTracer(context.Background()); err != nil {
					log.Errorf("Error shutting down tracer: %v", err)
				}
			}()
		}
	} else {
		log.Info("⏭️  Telemetry disabled (OTEL_ENABLED=false)")
	}

	// Fail-loud backstop: never silently run on the local dev Postgres when the
	// deployment declared it requires a remote (Azure) DB. Mirrors api-gateway +
	// temporal-adapter so a staging/prod launch that lost --env-file .env.staging
	// crashes here instead of serving an empty/wrong database.
	requireRemoteDatabase(cfg.Database.Host)

	// Initialize PostgreSQL database connection using config
	db, err := sql.Open("postgres", cfg.Database.ConnectionString())
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Connection pool limits — prevent exhausting Postgres max_connections under load.
	// Configurable so a shared managed DB (Azure/RDS) with a low max_connections
	// can be sized across all services. Defaults match the previous hardcoded values.
	maxOpen := 25
	if v := strings.TrimSpace(os.Getenv("DB_MAX_OPEN_CONNS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxOpen = n
		}
	}
	maxIdle := 10
	if v := strings.TrimSpace(os.Getenv("DB_MAX_IDLE_CONNS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxIdle = n
		}
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(5 * time.Minute)
	db.SetConnMaxIdleTime(1 * time.Minute)

	// Test database connection
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	log.Infof("✅ Database connected (%s:%s)", cfg.Database.Host, cfg.Database.Port)

	// Initialize Kafka using config
	kafkaConfig := kafka.Config{
		Brokers: cfg.Kafka.Brokers,
		GroupID: cfg.Kafka.GroupID,
	}

	kafkaManager, err := kafka.NewManager(kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to initialize Kafka: %v", err)
	}
	defer kafkaManager.Close()

	log.Info("✅ Kafka manager initialized")

	// Initialize Kafka TopologyManager for plan-time topic provisioning
	topologyManager, err := kafka.NewTopologyManager(kafkaConfig.Brokers)
	if err != nil {
		log.Warnf("⚠️  Failed to initialize TopologyManager: %v", err)
		// Don't fatal - continue without topology management
	} else {
		defer topologyManager.Close()
		log.Info("✅ Kafka TopologyManager initialized (plan-time topic provisioning)")

		// Provision agent control topics up front (required for horizontal scaling).
		// If these topics are auto-created with 1 partition, only one replica will get work.
		partitions := int32(0)
		const maxAgentTopicPartitions int32 = 64
		if v := strings.TrimSpace(os.Getenv("KAFKA_AGENT_TOPIC_PARTITIONS")); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				partitions = int32(n)
			} else {
				log.WithField("value", v).Warn("⚠️  Invalid KAFKA_AGENT_TOPIC_PARTITIONS; using default")
			}
		}
		if partitions <= 0 {
			partitions = 3
		}
		if partitions > maxAgentTopicPartitions {
			log.WithFields(log.Fields{
				"value": partitions,
				"max":   maxAgentTopicPartitions,
			}).Warn("⚠️  KAFKA_AGENT_TOPIC_PARTITIONS too large; clamping to max")
			partitions = maxAgentTopicPartitions
		}
		if err := topologyManager.EnsureAgentControlTopics(context.Background(), partitions); err != nil {
			log.WithError(err).Warn("⚠️  Failed to ensure agent control topics (scaling may be limited)")
		} else {
			log.WithField("partitions", partitions).Info("✅ Ensured agent control topics for scaling")
		}
	}

	// Initialize agents using config
	toolsDir := cfg.MCP.ToolsDir

	// Initialize AgentManager for Sentinel to restart agents
	agentManager := sentinel.NewAgentManager()
	log.Info("✅ AgentManager initialized (enables agent auto-restart)")

	// LEGACY AGENTS (for HTTP endpoints only - connection testing, schema discovery)
	// These are NOT part of the pipeline workflow anymore
	executorAgent := executor.NewAgent(kafkaManager, db, toolsDir)
	defer func() { _ = executorAgent.Stop() }()

	// Dependency liveness probe — observes the runtime dependencies (destination
	// MCP container, source MCP, ...) declared via pipeline_dependencies and
	// writes the observed state to pipeline_dependency_health. The api-gateway
	// /runtime endpoint reads that state to surface "streaming-but-dead" failures
	// instead of silently reporting "Live streaming, 0 rows."
	dependencyProbe := workers.NewDependencyProbe(db, executorAgent.MCPManager())
	go dependencyProbe.Start(context.Background())
	defer dependencyProbe.Stop()
	log.Info("✅ Dependency liveness probe started (15s tick)")

	// CDC reconciler: periodically delete Debezium connectors that no longer
	// have a live pipeline (orphans). This is the production auto-clean for the
	// connector-leak that exhausts the source DB and stalls new pipelines.
	cdcReconciler := workers.NewCDCReconciler(db)
	go cdcReconciler.Start(context.Background())
	defer cdcReconciler.Stop()
	log.Info("✅ CDC reconciler started (orphaned-connector auto-clean)")
	// Note: We don't Start() these agents - they're only used for HTTP endpoint handlers

	// ============================================================================
	// TEMPORAL-BASED WORKFLOW COORDINATION
	// ============================================================================
	// All workflow orchestration is now handled by Temporal.
	// - Temporal server: temporal:7233
	// - Temporal UI: http://localhost:8233
	// - Temporal adapter: backend-temporal-adapter/
	// - API Gateway: Starts workflows via Temporal client
	// ============================================================================
	log.Info("✅ Using Temporal for workflow orchestration (temporal:7233)")
	log.Info("   Pattern: Temporal thinks, Kafka talks, Agents act")

	// ============================================================================
	// CORRELATION STORE (V2 Request/Reply Pattern)
	// ============================================================================
	// Initialize correlation client for V2 workflows
	// V2 Activities wait on Redis, workers write responses to Redis
	// V1 workflows continue to use Kafka (backward compatible)
	if err := workers.InitCorrelationClient(); err != nil {
		log.Fatalf("❌ Failed to initialize correlation client: %v", err)
	}
	log.Info("✅ Correlation client initialized for V2 workflows (Redis-based request/reply)")

	// ============================================================================
	// STARTUP GATING: Stagger worker initialization to prevent rebalance thrashing
	// ============================================================================
	// With 9 workers joining the same consumer group simultaneously, Kafka's
	// rebalance coordinator gets overwhelmed. We stagger startups to allow
	// each worker to join cleanly.
	//
	// Timing: 3s initial delay + 0.5s between workers = ~7.5s total startup
	// This is well within Kafka's group.initial.rebalance.delay.ms=3000
	//
	// SCALING:
	//   This orchestrator scales horizontally up to N replicas where N equals
	//   the partition count of the agent.control.commands.<type> topics
	//   (currently 3). Each replica joins the same per-topic consumer group
	//   ("orchestrator-agent.control.commands.<type>") and Kafka splits
	//   partitions across them via the cooperative-sticky strategy.
	//   Producer-side keys by pipeline_id, so per-pipeline ordering is
	//   preserved when replicas split partitions. To go beyond N=3, increase
	//   partition count on the agent topics (kafka-topics --alter).
	//   Replication factor (broker durability) is independent of this and
	//   does not affect consumer parallelism.
	// ============================================================================

	log.Info("🚦 STARTUP GATING: Staggering worker initialization (prevents rebalance thrashing)")
	time.Sleep(3 * time.Second) // Initial delay for Kafka to stabilize

	// Start workers with staggered delays (500ms between each)
	intentWorker := workers.NewIntentWorker(kafkaManager, db)
	if err := intentWorker.Start(); err != nil {
		log.Fatalf("Failed to start Intent Worker: %v", err)
	}
	log.Info("✅ Intent Worker started (stateless) [1/9]")
	defer intentWorker.Stop()
	time.Sleep(500 * time.Millisecond)

	resolverWorker := workers.NewResolverWorker(kafkaManager, db)
	if err := resolverWorker.Start(); err != nil {
		log.Fatalf("Failed to start Resolver Worker: %v", err)
	}
	log.Info("✅ Resolver Worker started (stateless) [2/9]")
	defer resolverWorker.Stop()
	time.Sleep(500 * time.Millisecond)

	discoveryWorker := workers.NewDiscoveryWorker(kafkaManager, db, toolsDir)
	if err := discoveryWorker.Start(); err != nil {
		log.Fatalf("Failed to start Discovery Worker: %v", err)
	}
	log.Info("✅ Discovery Worker started (stateless) [3/9]")
	defer discoveryWorker.Stop()
	time.Sleep(500 * time.Millisecond)

	plannerWorker := workers.NewPlannerWorker(kafkaManager, db)
	if err := plannerWorker.Start(); err != nil {
		log.Fatalf("Failed to start Planner Worker: %v", err)
	}
	log.Info("✅ Planner Worker started (stateless) [4/9]")
	defer plannerWorker.Stop()
	time.Sleep(500 * time.Millisecond)

	validatorWorker := workers.NewValidatorWorker(kafkaManager, db)
	if err := validatorWorker.Start(); err != nil {
		log.Fatalf("Failed to start Validator Worker: %v", err)
	}
	log.Info("✅ Validator Worker started (stateless) [5/9]")
	defer validatorWorker.Stop()
	time.Sleep(500 * time.Millisecond)

	// Share the legacy executorAgent's MCP server registry with the worker
	// so HTTP discover-schema, the worker's pipeline runs, and the dependency
	// probe all see the SAME stdio subprocess per connector. Without this,
	// each constructor spawns an independent subprocess and one of them ends
	// up returning empty results while the other works correctly — a silent
	// data-loss bug we hit during launch testing.
	executorWorker := workers.NewExecutorWorkerWithAgent(kafkaManager, db, toolsDir, executorAgent)
	if err := executorWorker.Start(); err != nil {
		log.Fatalf("Failed to start Executor Worker: %v", err)
	}
	log.Info("✅ Executor Worker started (stateless) [6/9]")
	defer executorWorker.Stop()
	time.Sleep(500 * time.Millisecond)

	costEstimatorWorker := workers.NewCostEstimatorWorker(kafkaManager)
	if err := costEstimatorWorker.Start(); err != nil {
		log.Fatalf("Failed to start Cost Estimator Worker: %v", err)
	}
	log.Info("✅ Cost Estimator Worker started (stateless) [7/9]")
	defer costEstimatorWorker.Stop()
	time.Sleep(500 * time.Millisecond)

	capabilityResolverWorker := workers.NewCapabilityResolverWorker(kafkaManager, db)
	if err := capabilityResolverWorker.Start(); err != nil {
		log.Fatalf("Failed to start Capability Resolver Worker: %v", err)
	}
	log.Info("✅ Capability Resolver Worker started (stateless) [8/9]")
	defer capabilityResolverWorker.Stop()
	time.Sleep(500 * time.Millisecond)

	connectionValidatorWorker := workers.NewConnectionValidatorWorker(kafkaManager, db, toolsDir)
	if err := connectionValidatorWorker.Start(); err != nil {
		log.Fatalf("Failed to start Connection Validator Worker: %v", err)
	}
	log.Info("✅ Connection Validator Worker started (stateless) [9/9]")
	defer connectionValidatorWorker.Stop()

	// [10/10] Native executor Temporal worker (Phase 3) — ⛔ PARKED, dead scaffolding.
	// Hosts ExecutorNativeActivity on the executor-tasks queue so the V2 workflow
	// *could* dispatch the executor stage as a native Temporal activity
	// (input.ExecutorDispatch == "temporal") instead of via the Redis correlation hop.
	//
	// ⛔ The cutover is NOT wired: nothing ever populates input.ExecutorDispatch
	// (api-gateway RunPipeline + scheduled_run_workflow build the workflow input
	// without it), so every workflow takes the correlation path (ExecutorActivityV2)
	// and this worker — if started — sits idle polling executor-tasks forever. The
	// flag therefore defaults OFF and is intentionally left false in all env files.
	// See CAPABILITIES.md §"Temporal-native executor dispatch (Phase 3)" and the
	// BACKLOG removal item. Do NOT flip EXECUTOR_NATIVE_WORKER=true expecting a
	// behavior change — it only wastes a Temporal connection. Kept (not deleted) so
	// the parked design stays reviewable; removal is tracked in BACKLOG.
	if os.Getenv("EXECUTOR_NATIVE_WORKER") == "true" {
		if nativeWorker, err := temporalworker.New(executorWorker); err != nil {
			log.Warnf("⚠️  Native executor Temporal worker disabled (dial failed): %v", err)
		} else if err := nativeWorker.Start(); err != nil {
			log.Warnf("⚠️  Native executor Temporal worker failed to start: %v", err)
		} else {
			log.Warn("⛔ Native executor Temporal worker started on executor-tasks [PARKED — no workflow dispatches to it; it will idle. Set EXECUTOR_NATIVE_WORKER=false]")
			defer nativeWorker.Stop()
		}
	}

	log.Info("📊 TEMPORAL ARCHITECTURE: 9 stateless workers consuming from agent.control.commands")
	log.Info("   ✅ All workers joined consumer group successfully with staggered startup (7.5s total)")
	log.Info("   Workers: Intent, Resolver, Capability Resolver, Connection Validator,")
	log.Info("            Discovery, Planner, Validator, Cost Estimator, Executor")
	// ============================================================================

	// Initialize Pipeline Scheduler for automated/scheduled runs
	// LEGACY: Scheduler is now handled by API Gateway + Temporal.
	// We keep this block commented out or strictly disabled until full migration check is 100%.
	// pipelineScheduler := scheduler.NewScheduler(db, kafkaManager)
	if cfg.Features.EnableScheduler {
		log.Warn("⚠️  Internal Scheduler is DEPRECATED. Use API Gateway + Temporal for scheduling.")
		// if err := pipelineScheduler.Start(); err != nil {
		// 	log.Warnf("⚠️  Failed to start Pipeline Scheduler: %v", err)
		// } else {
		// 	log.Infof("✅ Pipeline Scheduler started (%d scheduled pipelines)", pipelineScheduler.GetJobCount())
		// 	defer pipelineScheduler.Stop()
		// }
	} else {
		log.Info("⏭️  Internal Scheduler disabled (Legacy).")
	}

	// Initialize Consumer Registry Agent for dynamic consumer management
	consumerConfig := consumer.FromEnv()
	var consumerRegistry *consumer.Registry
	if cfg.Features.EnableConsumerAgent {
		var err error
		consumerRegistry, err = consumer.NewRegistry(
			consumerConfig,
			cfg.Features.ConsumerAutoScale,
			cfg.Features.ConsumerAutoRestart,
		)
		if err != nil {
			log.Warnf("⚠️  Failed to create Consumer Registry: %v", err)
		} else {
			if err := consumerRegistry.Start(context.Background()); err != nil {
				log.Warnf("⚠️  Failed to start Consumer Registry: %v", err)
			} else {
				log.Info("✅ Consumer Registry Agent started (auto-scale, auto-restart)")
				defer consumerRegistry.Stop()
			}
		}
	} else {
		log.Info("⏭️  Consumer Registry Agent disabled (ENABLE_CONSUMER_AGENT=false)")
	}

	// Initialize Retention Manager Agent for data lifecycle management
	retentionConfig := retention.FromEnv()
	var retentionAgent *retention.Agent
	if cfg.Features.EnableRetentionAgent {
		retentionAgent = retention.NewAgent(retentionConfig)
		if err := retentionAgent.Start(context.Background()); err != nil {
			log.Warnf("⚠️  Failed to start Retention Agent: %v", err)
		} else {
			log.Info("✅ Retention Manager Agent started (data lifecycle management)")
			defer retentionAgent.Stop()
		}
	} else {
		log.Info("⏭️  Retention Manager Agent disabled (ENABLE_RETENTION_AGENT=false)")
	}

	// Infra preflight sweep: optionally warm the core MCP connectors at boot (and
	// on an interval if configured) so the first pipeline isn't slowed by a cold
	// container start. Off by default; the per-pipeline preflight remains the
	// authoritative gate, so this is a best-effort, non-fatal warm-up.
	if cfg.Features.EnableInfraPreflightSweep {
		workers.StartInfraPreflightSweep(context.Background(), executorAgent.MCPManager(), cfg.Features.InfraPreflightSweepIntervalSecs)
		log.Infof("✅ Infra preflight sweep enabled (interval=%ds)", cfg.Features.InfraPreflightSweepIntervalSecs)
	} else {
		log.Info("⏭️  Infra preflight sweep disabled (ENABLE_INFRA_PREFLIGHT_SWEEP=false)")
	}

	// Initialize Sentinel Agent for system health monitoring and auto-healing
	var sentinelAgent *sentinel.Agent
	enableSentinel := os.Getenv("ENABLE_SENTINEL") != "false" // Enabled by default
	if enableSentinel {
		sentinelAgent = sentinel.NewAgent(kafkaManager, db, nil, agentManager) // Pass AgentManager for restarts
		if err := sentinelAgent.Start(); err != nil {
			log.Warnf("⚠️  Failed to start Sentinel Agent: %v", err)
		} else {
			log.Info("✅ Sentinel Agent started (System Health & Auto-Healing with agent restart)")
			defer sentinelAgent.Stop()
		}
	} else {
		log.Info("⏭️  Sentinel Agent disabled (ENABLE_SENTINEL=false)")
	}

	// CDC Sentinel: proactively polls Kafka Connect/Debezium and alerts Healer on silent failures.
	// Enabled by default; configure with:
	// - ENABLE_CDC_SENTINEL=false to disable
	// - KAFKA_CONNECT_URL=http://kafka-connect:8083
	// - CDC_SENTINEL_POLL_INTERVAL=60s
	var cdcSentinel *sentinel.CDCSentinel
	enableCDCSentinel := os.Getenv("ENABLE_CDC_SENTINEL") != "false"
	if enableCDCSentinel {
		cdcSentinel = sentinel.NewCDCSentinel(db, kafkaManager)
		if err := cdcSentinel.Start(); err != nil {
			log.Warnf("⚠️  Failed to start CDC Sentinel: %v", err)
		} else {
			log.Info("✅ CDC Sentinel started (Kafka Connect proactive monitoring)")
			defer cdcSentinel.Stop()
		}
	} else {
		log.Info("⏭️  CDC Sentinel disabled (ENABLE_CDC_SENTINEL=false)")
	}

	// Batch Sentinel: the batch counterpart of the CDC Sentinel. Every CDC loop above is
	// gated on sync_mode='cdc', so a batch run that stops moving has NO in-flight observer —
	// the first thing that notices is the 4h zombie sweep. This watches two batch-only
	// surfaces (progress staleness, negative sink acks) and ESCALATES only: it never restarts
	// or rewinds a batch run, because a batch run carries checkpoint/run-mode state that a
	// blind restart can destroy. Enabled by default; configure with:
	// - ENABLE_BATCH_SENTINEL=false to disable
	// - BATCH_SENTINEL_PROGRESS_INTERVAL / BATCH_SENTINEL_ACK_INTERVAL / BATCH_SENTINEL_STALL_THRESHOLD
	//   / BATCH_SENTINEL_SINK_PRESENCE_INTERVAL
	var batchSentinel *sentinel.BatchSentinel
	if os.Getenv("ENABLE_BATCH_SENTINEL") != "false" {
		batchSentinel = sentinel.NewBatchSentinel(db, kafkaManager)
		if err := batchSentinel.Start(); err != nil {
			log.Warnf("⚠️  Failed to start Batch Sentinel: %v", err)
		} else {
			log.Info("✅ Batch Sentinel started (in-flight batch stall + sink-reject + sink-worker-presence detection)")
			defer batchSentinel.Stop()
		}
	} else {
		log.Info("⏭️  Batch Sentinel disabled (ENABLE_BATCH_SENTINEL=false)")
	}

	// CDC Table Stats: consumes Debezium topics and emits TABLE_STATS (feature-flagged).
	// Enable with ENABLE_CDC_TABLE_STATS=true
	cdcStatsAgent := cdcstats.New(db, kafkaManager)
	if err := cdcStatsAgent.Start(); err != nil {
		log.Warnf("⚠️  Failed to start CDC Table Stats agent: %v", err)
	} else {
		defer cdcStatsAgent.Stop()
	}

	// Phase 4 HealWorker — diagnoses failed executions and applies fixes.
	// Enabled by default; disable with ENABLE_HEAL_WORKER=false.
	//
	// NewHealWorker forwards an empty heal.AutoHealHooks{}, so CleanupCDCResourcesFn
	// and RepairOwnershipFn are nil here. That is deliberate, not an oversight —
	// switching to NewHealWorkerWithHooks is a decision, so the reasoning lives at
	// the call site where someone would make it:
	//
	//   - Nothing emits the actions those hooks serve. ActionCleanupCDCResources
	//     and ActionRepairOwnershipRow appear only in their own executors and
	//     tests; no diagnoser rule produces either, so neither executor is
	//     reachable through Heal's registry today.
	//   - Wiring CleanupCDCResourcesFn to cdc.NewPostgreSQLManager(db).CleanupResources
	//     is a signature-exact drop-in, and that is the hazard: CleanupResources
	//     DROPs the pipeline's replication slot and publication (internal/cdc/postgresql.go).
	//     Dropping a slot discards the WAL position, so changes between the drop and
	//     the next provision are simply gone. Arming that to fire unattended
	//     contradicts the healer's own rule that CDC-provisioning failures escalate
	//     rather than self-repair (#714, CLAUDE.md § CDC healer error classification).
	//   - RepairOwnershipFn has no implementation to point at.
	//
	// A nil hook is now honest: the executor returns an error and the attempt is
	// recorded as failed rather than as a fix that happened
	// (internal/agents/heal/auto_executors.go). Before that it returned nil, which
	// Heal reads as success.
	if os.Getenv("ENABLE_HEAL_WORKER") != "false" {
		healWorker := heal.NewHealWorker(db, "")
		healCtx, healCancel := context.WithCancel(context.Background())
		defer healCancel()
		go healWorker.Start(healCtx)
		log.Info("✅ HealWorker started (Phase 4 — Diagnose→Heal loop)")
	} else {
		log.Info("⏭️  HealWorker disabled (ENABLE_HEAL_WORKER=false)")
	}

	// The MCP server manager is shared by the HTTP handlers (via setupRouter) and the CDC
	// Sentinel's autonomous sink-restart rung — it holds per-instance runtime state (spawned
	// servers, port allocation), so there must be exactly ONE instance across both consumers.
	mcpServerManager := mcp.NewServerManager(toolsDir)

	// Plumb it into the Sentinel so its Phase-B rung can reuse the manual stop_sink+start_sink
	// path (handlers.RestartCDCSinkWorker). The Sentinel began polling above, but its first tick
	// is a poll-interval away and the rung is both nil-guarded and flag-gated
	// (CDC_SINK_AUTORESTART_ENABLED, default off), so wiring it in here is race-safe.
	if cdcSentinel != nil {
		cdcSentinel.SetMCPManager(mcpServerManager)
	}

	// Same manager, same reason, for the batch sink-presence probe. Batch asks the sink
	// container the same question ("do you still hold a worker for this consumer group?")
	// but never acts on the answer — it raises an issue and stops. Until this line runs,
	// sinkPresenceTick is a nil-guarded no-op.
	if batchSentinel != nil {
		batchSentinel.SetMCPManager(mcpServerManager)
	}

	// Start HTTP server
	router := setupRouter(kafkaManager, topologyManager, executorAgent, consumerRegistry, retentionAgent, db, mcpServerManager, cdcStatsAgent)

	srv := &http.Server{
		Addr:              ":" + cfg.Server.Port,
		Handler:           router,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      120 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	// Start HTTP server in goroutine
	go func() {
		log.Infof("📡 HTTP API starting on port %s", cfg.Server.Port)
		log.Infof("   Endpoints:")
		log.Infof("   - GET  http://localhost:%s/health", cfg.Server.Port)
		log.Infof("   - GET  http://localhost:%s/agents", cfg.Server.Port)

		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Internal-only Prometheus metrics listener (SEC-L-05).
	// /metrics must NOT be reachable via the public Traefik /orchestrator route.
	// Bound to a separate port reachable only inside the compose network (no host
	// publish, no Traefik label). The otel-collector scrapes orchestrator:<port>/metrics.
	metricsPort := os.Getenv("METRICS_PORT")
	if metricsPort == "" {
		metricsPort = "9090"
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsSrv := &http.Server{
		Addr:              ":" + metricsPort,
		Handler:           metricsMux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Infof("metrics listener on port %s (/metrics)", metricsPort)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Errorf("metrics HTTP server failed: %v", err)
		}
	}()

	log.Info("================================================================================")
	log.Info("✅ Go Orchestrator is running (Phase 7 - NL-Driven Agentic Pipeline)")
	log.Info("   Agents: Intent → Resolver → Discovery → Planner → Validator → Executor")
	log.Info("   Flow: NL Understanding → Connection Matching → Schema → Plan → Validation → Execution")
	log.Info("   Support: Generic MCP connectors (MySQL, PostgreSQL, MongoDB, S3, Snowflake...)")
	// if cfg.Features.EnableScheduler && pipelineScheduler.IsRunning() {
	// 	log.Infof("   Scheduler: Enabled (%d scheduled pipelines)", pipelineScheduler.GetJobCount())
	// }
	log.Infof("   Telemetry: %v (endpoint: %s)", cfg.Telemetry.Enabled, cfg.Telemetry.OTLPEndpoint)
	log.Info("Press Ctrl+C to stop")
	log.Info("================================================================================")

	// Wait for interrupt signal
	<-sigCh
	log.Info("\n⚠️  Shutdown signal received, gracefully stopping...")

	// Graceful shutdown with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Errorf("HTTP server shutdown error: %v", err)
	}

	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		log.Errorf("metrics server shutdown error: %v", err)
	}

	log.Info("✅ Go Orchestrator stopped gracefully")
}

// setupRouter builds the orchestrator's HTTP surface.
//
// It takes no connectors directory: the only thing that ever needed one here
// was the deleted /api/v1/mcp/connectors handler (see the tombstone on the
// /api/v1 group below). Keeping a dead `toolsDir string` parameter would
// re-advertise that this router serves connector metadata. It does not.
func setupRouter(kafkaManager *kafka.Manager, topologyManager *kafka.TopologyManager, executorAgent *executor.Agent, consumerRegistry *consumer.Registry, retentionAgent *retention.Agent, db *sql.DB, mcpServerManager *mcp.ServerManager, cdcStatsAgent *cdcstats.Agent) *gin.Engine {
	// Set Gin to release mode in production
	if os.Getenv("DEBUG") != "true" {
		gin.SetMode(gin.ReleaseMode)
	}

	router := gin.New()
	router.Use(gin.Recovery())

	// Add OTel trace middleware first (replaces legacy TraceMiddleware)
	// This properly integrates with OpenTelemetry for distributed tracing
	router.Use(handlers.OTelTraceMiddleware("orchestrator"))
	router.Use(handlers.CORSMiddleware())
	router.Use(handlers.LoggingMiddleware())

	// /version — canonical "what binary is actually running" answer for the
	// deployment-drift check. Uses build-time GIT_COMMIT and BUILD_TIME envs.
	orchestratorStartedAt := time.Now().UTC()
	router.GET("/version", func(c *gin.Context) {
		commit := os.Getenv("GIT_COMMIT")
		if commit == "" {
			commit = "dev"
		}
		builtAt := os.Getenv("BUILD_TIME")
		if builtAt == "" {
			builtAt = "unknown"
		}
		c.JSON(200, gin.H{
			"service":     "backend-orchestrator",
			"commit":      commit,
			"built_at":    builtAt,
			"started_at":  orchestratorStartedAt.Format(time.RFC3339),
			"uptime_secs": int64(time.Since(orchestratorStartedAt).Seconds()),
		})
	})

	// Health check endpoint
	router.GET("/health", func(c *gin.Context) {
		// schedulerInfo deprecated
		// if pipelineScheduler != nil { ... }

		consumerRegistryInfo := gin.H{
			"enabled": false,
			"running": false,
		}
		if consumerRegistry != nil {
			consumerRegistryInfo = gin.H{
				"enabled": true,
				"running": consumerRegistry.IsRunning(),
				"state":   consumerRegistry.State(),
			}
		}

		retentionAgentInfo := gin.H{
			"enabled": false,
			"running": false,
		}
		if retentionAgent != nil {
			retentionAgentInfo = gin.H{
				"enabled": true,
				"running": retentionAgent.IsRunning(),
				"state":   retentionAgent.State(),
			}
		}

		kafkaConnected := false
		brokers := ""
		if kafkaManager != nil {
			kafkaConnected = kafkaManager.IsConnected()
			brokers = kafkaManager.Config.Brokers
		}

		c.JSON(http.StatusOK, gin.H{
			"status":       "healthy",
			"service":      "rsync-ai-orchestrator",
			"version":      Version,
			"architecture": "Temporal Workflows + Stateless Agent Workers",
			"orchestration": gin.H{
				"type":        "temporal",
				"description": "Temporal handles workflow orchestration, Kafka for agent communication",
				"pattern":     "Temporal thinks, Kafka talks, Agents act",
			},
			"workers": []string{
				"intent-worker",
				"resolver-worker",
				"capability-resolver-worker",
				"connection-validator-worker",
				"discovery-worker",
				"planner-worker",
				"validator-worker",
				"cost-estimator-worker",
				"executor-worker",
			},
			"workflow": "Intent → Resolver → Discovery → Planner → Validator → Executor",
			// "scheduler":         schedulerInfo, // Deprecated
			"consumer_registry": consumerRegistryInfo,
			"retention_agent":   retentionAgentInfo,
			"kafka": gin.H{
				"connected": kafkaConnected,
				"brokers":   brokers,
				"topics": kafkaclient.Topics(
					"agent.control.commands",
					"agent.control.results",
				),
			},
		})
	})

	// Readiness endpoint: fail fast if core dependencies are unavailable.
	router.GET("/ready", func(c *gin.Context) {
		reasons := make([]string, 0)

		if db == nil {
			reasons = append(reasons, "db_not_connected")
		} else {
			ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
			defer cancel()
			if err := db.PingContext(ctx); err != nil {
				reasons = append(reasons, "db_ping_failed")
			}
		}

		if kafkaManager == nil || !kafkaManager.IsConnected() {
			reasons = append(reasons, "kafka_not_connected")
		}

		if consumerRegistry != nil && !consumerRegistry.IsRunning() {
			reasons = append(reasons, "consumer_registry_not_running")
		}
		if retentionAgent != nil && !retentionAgent.IsRunning() {
			reasons = append(reasons, "retention_agent_not_running")
		}

		if len(reasons) > 0 {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"status":  "not_ready",
				"service": "rsync-ai-orchestrator",
				"reasons": reasons,
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{"status": "ready", "service": "rsync-ai-orchestrator"})
	})

	// Scheduler status endpoint
	router.GET("/scheduler", func(c *gin.Context) {
		c.Header("Deprecation", "true")
		// Endpoint is already gone; Sunset is in the past, but included for client tooling.
		c.Header("Sunset", time.Now().UTC().Format(http.TimeFormat))
		// Provide a machine-readable successor hint (relative path).
		c.Header("Link", `</api/v1/pipeline-schedules>; rel="successor-version"`)
		c.JSON(http.StatusGone, gin.H{
			"error":       "deprecated_endpoint",
			"message":     "Internal Scheduler is deprecated. Use API Gateway + Temporal for scheduling.",
			"replacement": "api-gateway:/api/v1/pipeline-schedules",
		})
	})

	// /metrics is served on a separate internal-only listener in main() (SEC-L-05) — not on this public :8080 router.

	// Workers status endpoint
	router.GET("/workers", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"workers": []gin.H{
				{
					"name":           "intent-worker",
					"status":         "running",
					"architecture":   "stateless",
					"consumer_group": "agent-workers",
					"description":    "Parses natural language to understand user intent",
				},
				{
					"name":           "resolver-worker",
					"status":         "running",
					"architecture":   "stateless",
					"consumer_group": "agent-workers",
					"description":    "Finds matching connections based on parsed intent",
				},
				{
					"name":           "capability-resolver-worker",
					"status":         "running",
					"architecture":   "stateless",
					"consumer_group": "agent-workers",
					"description":    "Resolves connector capabilities for the pipeline",
				},
				{
					"name":           "connection-validator-worker",
					"status":         "running",
					"architecture":   "stateless",
					"consumer_group": "agent-workers",
					"description":    "Validates connection configurations",
				},
				{
					"name":           "discovery-worker",
					"status":         "running",
					"architecture":   "stateless",
					"consumer_group": "agent-workers",
					"description":    "Discovers schemas from data sources",
				},
				{
					"name":           "planner-worker",
					"status":         "running",
					"architecture":   "stateless",
					"consumer_group": "agent-workers",
					"description":    "Builds intelligent execution plans",
				},
				{
					"name":           "validator-worker",
					"status":         "running",
					"architecture":   "stateless",
					"consumer_group": "agent-workers",
					"description":    "Validates plans for security and feasibility",
				},
				{
					"name":           "cost-estimator-worker",
					"status":         "running",
					"architecture":   "stateless",
					"consumer_group": "agent-workers",
					"description":    "Estimates cost and resource usage",
				},
				{
					"name":           "executor-worker",
					"status":         "running",
					"architecture":   "stateless",
					"consumer_group": "agent-workers",
					"description":    "Executes validated plans with streaming support",
				},
			},
			"orchestration": gin.H{
				"type":        "temporal",
				"description": "Temporal workflows coordinate agents via Kafka",
			},
			"total":        9,
			"architecture": "Temporal Workflows + Event-Driven Stateless Workers",
			"kafka_topics": gin.H{
				"agent_control_commands": "Temporal adapter sends commands to workers",
				"agent_control_results":  "Workers send results to Temporal adapter",
			},
		})
	})

	// API Routes
	api := router.Group("/api/v1")
	{
		// ============================================================================
		// NOTE: Pipeline creation and management is now handled by API Gateway
		// API Gateway uses Temporal workflows for pipeline orchestration
		// See: api-gateway/internal/handlers/pipelines.go
		// ============================================================================

		// ----------------------------------------------------------------------------
		// DEPRECATED: Orchestrator pipeline execution endpoint
		// ----------------------------------------------------------------------------
		// Keep this route to fail loudly for any stale callers (old UI builds, scripts).
		// The canonical execution endpoint is on API Gateway.
		api.POST("/pipelines/:id/run", func(c *gin.Context) {
			pipelineID := strings.TrimSpace(c.Param("id"))
			log.WithFields(log.Fields{
				"pipeline_id": pipelineID,
				"path":        c.FullPath(),
				"method":      c.Request.Method,
			}).Warn("DEPRECATED endpoint called: orchestrator /api/v1/pipelines/:id/run")

			c.Header("Deprecation", "true")
			c.JSON(http.StatusGone, gin.H{
				"error":       "deprecated_endpoint",
				"message":     "Orchestrator pipeline execution endpoint is deprecated. Use API Gateway /api/v1/pipelines/:id/run instead.",
				"replacement": "api-gateway:/api/v1/pipelines/:id/run",
			})
		})

		// Connection CRUD lives on api-gateway (canonical, user-scoped).
		// Removed 2026-05-22: the orchestrator-side /api/v1/connections
		// routes were a duplicate handler with no auth middleware, port
		// 8081 exposed to host, and a 5-key mask list that leaked
		// access_token / refresh_token / client_secret in responses.
		// All internal orchestrator code paths use connections.Manager.Get
		// against the same DB row, so removing the HTTP layer is loss-less.
		// (Manager.Get itself still needs a userID parameter — see ARCH-3
		// audit notes for the next consolidation PR.)

		// Connector metadata lives on api-gateway (canonical, authenticated).
		// Removed 2026-08-29: the orchestrator-side /api/v1/mcp/connectors,
		// /mcp/connectors/:name and /mcp/connectors/:name/capabilities routes
		// were dead — their handler read `<toolsDir>/<entry>/metadata.json`,
		// a depth-1 layout that has not existed since the connector root
		// copies were deleted, so the listing always returned
		// {"connectors":[],"total":0} and both by-name routes always 404'd.
		// Deleted rather than repaired for the same reason /api/v1/connections
		// went above: this group carries no auth middleware and the default
		// OSS compose publishes ${RSYNC_HP_ORCHESTRATOR:-8081}:8080, so a
		// working listing here would be an unauthenticated connector catalog
		// with config schemas on a self-host box. api-gateway already serves
		// the same data behind AuthRequired + EmailVerified + CSRF +
		// RateLimit + WorkspaceContext (api-gateway/internal/handlers/tools.go
		// ListMCPConnectors). The routes had zero callers repo-wide — the
		// frontend routes every CONNECTORS.* URL at API_GATEWAY_URL
		// (frontend/src/lib/config/api.ts).
		// Pinned by TestOrchestratorServesNoConnectorMetadataRoutes.

		// Pipeline Shapes (available flow patterns) — static data, no disk read.
		api.GET("/pipeline/shapes", handlers.GetPipelineShapes)

		// ============================================================================
		// Agent HTTP endpoints (compat layer)
		// ============================================================================
		// api-gateway uses this endpoint for connection schema discovery when rendering
		// connection metadata (tables/columns/row counts) in the UI.
		//
		// IMPORTANT:
		// - This does NOT start workflows; it directly calls the MCP connector via the
		//   executor agent (which is already wired to spawn MCP containers).
		// - Keep response shape stable: must include { "tables": [...] } (extra fields allowed)
		type discoverSchemaRequest struct {
			Task          string                 `json:"task"`
			ConnectionID  string                 `json:"connection_id"`
			ConnectorType string                 `json:"connector_type"`
			Config        map[string]interface{} `json:"config"`
			UserID        string                 `json:"user_id"`
			// Optional v2 flags (backward compatible; older callers can omit)
			IncludeColumns       *bool `json:"include_columns,omitempty"`
			IncludeRowCounts     *bool `json:"include_row_counts,omitempty"`
			IncludeRelationships *bool `json:"include_relationships,omitempty"`
			IncludeIndexes       *bool `json:"include_indexes,omitempty"`
			MaxTables            *int  `json:"max_tables,omitempty"`
		}

		// SECURITY: requirePrincipal — these agent endpoints accept an arbitrary
		// connection `config` and reach out to it (schema discovery / test /
		// sample-rows), so anonymous access was a data-exfil + SSRF primitive over
		// the public /orchestrator route. Callers are api-gateway proxy handlers
		// (internal secret) or authenticated users.
		api.POST("/agent/discover-schema", requirePrincipal(db), func(c *gin.Context) {
			var req discoverSchemaRequest
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body", "details": err.Error()})
				return
			}

			if req.ConnectorType == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "connector_type is required"})
				return
			}
			if req.Task != "" && req.Task != "discover_schema" {
				// We only support schema discovery here.
				c.JSON(http.StatusBadRequest, gin.H{"error": "unsupported task", "details": req.Task})
				return
			}

			// SECURITY: never log config values.
			cfgKeys := 0
			if req.Config != nil {
				cfgKeys = len(req.Config)
			}
			log.WithFields(log.Fields{
				"connector_type": req.ConnectorType,
				"config_keys":    cfgKeys,
				"connection_id":  req.ConnectionID,
			}).Info("🔎 Agent: discover_schema (HTTP)")

			// Merge optional flags into params
			params := map[string]interface{}{}
			if req.IncludeColumns != nil {
				params["include_columns"] = *req.IncludeColumns
			}
			if req.IncludeRowCounts != nil {
				params["include_row_counts"] = *req.IncludeRowCounts
			}
			if req.IncludeRelationships != nil {
				params["include_relationships"] = *req.IncludeRelationships
			}
			if req.IncludeIndexes != nil {
				params["include_indexes"] = *req.IncludeIndexes
			}
			if req.MaxTables != nil {
				params["max_tables"] = *req.MaxTables
			}

			envelope, err := executorAgent.DiscoverSchemaEnvelope(c.Request.Context(), req.ConnectorType, req.Config, params)
			if err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "schema discovery failed", "details": err.Error()})
				return
			}

			// Return envelope directly (includes tables + v2 metadata fields).
			c.JSON(http.StatusOK, envelope)
		})

		// Test-connection endpoint: invokes the connector's test_connection
		// operation via the executor agent. Replaces the removed
		// /api/v1/connections/test route. Used by api-gateway's
		// performConnectionTest to validate credentials before saving.
		//
		// Generic across ALL MCP connectors: connector_version flows from
		// the api-gateway (read from DB for existing connections, or from
		// the request body for test-before-create) so the test always hits
		// the same versioned container the pipeline will use.
		api.POST("/agent/test-connection", requirePrincipal(db), func(c *gin.Context) {
			var req struct {
				ConnectorType    string            `json:"connector_type" binding:"required"`
				ConnectorVersion string            `json:"connector_version"` // optional; defaults to "latest"
				Config           map[string]string `json:"config"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body", "details": err.Error()})
				return
			}
			if strings.TrimSpace(req.ConnectorVersion) == "" {
				req.ConnectorVersion = "latest"
			}
			log.WithFields(log.Fields{
				"connector_type":    req.ConnectorType,
				"connector_version": req.ConnectorVersion,
				"config_keys":       len(req.Config),
			}).Info("🔌 Agent: test_connection (HTTP)")

			success, testErr := executorAgent.TestConnection(c.Request.Context(), req.ConnectorType, req.ConnectorVersion, req.Config)
			if success {
				c.JSON(http.StatusOK, gin.H{"success": true, "status": "success", "message": "Connection test successful"})
			} else {
				c.JSON(http.StatusOK, gin.H{"success": false, "status": "failed", "message": "Connection test failed", "error": testErr})
			}
		})

		// Sample-rows endpoint: returns the first N rows of a table for
		// UI preview. Generic across REST / GraphQL / SaaS / DB sources —
		// invokes the source connector's standard `export` operation
		// with `limit=N`. Used by the api-gateway's
		// /connections/:id/sample handler for non-DB connectors;
		// without this, every non-mysql / non-postgres preview
		// returned HTTP 500 with "preview not supported for connector
		// type: ...".
		api.POST("/agent/sample-rows", requirePrincipal(db), func(c *gin.Context) {
			var req struct {
				ConnectorType string                 `json:"connector_type" binding:"required"`
				Config        map[string]interface{} `json:"config"`
				Table         string                 `json:"table" binding:"required"`
				Limit         int                    `json:"limit"`
				ConnectionID  string                 `json:"connection_id,omitempty"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body", "details": err.Error()})
				return
			}
			if req.Limit <= 0 {
				req.Limit = 10
			}

			log.WithFields(log.Fields{
				"connector_type": req.ConnectorType,
				"table":          req.Table,
				"limit":          req.Limit,
				"connection_id":  req.ConnectionID,
				"config_keys":    len(req.Config),
			}).Info("👁️  Agent: sample_rows (HTTP)")

			rows, cols, err := executorAgent.SampleRows(c.Request.Context(), req.ConnectorType, req.Config, req.Table, req.Limit)
			if err != nil {
				// Bubble through the actual error so the UI can render
				// something specific rather than a generic "HTTP 500".
				c.JSON(http.StatusBadGateway, gin.H{
					"error":   "sample_rows failed",
					"details": err.Error(),
				})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"rows":      rows,
				"columns":   cols,
				"row_count": len(rows),
			})
		})

		// explorer-query: delegated statement execution for the Data Explorer. Runs a
		// user's statement through the connector's MCP tool — used for warehouses the
		// api-gateway has no native driver for (e.g. BigQuery). When write=false it runs
		// the connector's `export` (rows-returning) tool; when write=true it runs
		// `execute` (no-rows write) and returns an affected-row count. The role-aware
		// statement guard + workspace-scoped connection load are enforced upstream in the
		// api-gateway; this endpoint is S2S-gated by requirePrincipal.
		api.POST("/agent/explorer-query", requirePrincipal(db), func(c *gin.Context) {
			var req struct {
				ConnectorType string                 `json:"connector_type" binding:"required"`
				Config        map[string]interface{} `json:"config"`
				Query         string                 `json:"query" binding:"required"`
				Limit         int                    `json:"limit"`
				ConnectionID  string                 `json:"connection_id,omitempty"`
				Write         bool                   `json:"write,omitempty"`
			}
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body", "details": err.Error()})
				return
			}

			// SECURITY: never log the query text — only shape metadata.
			log.WithFields(log.Fields{
				"connector_type": req.ConnectorType,
				"limit":          req.Limit,
				"connection_id":  req.ConnectionID,
				"config_keys":    len(req.Config),
				"write":          req.Write,
			}).Info("🔎 Agent: explorer_query (HTTP)")

			// Write path: run the connector's MCP `execute` tool and return the
			// affected-row count. Authorization already happened in the api-gateway.
			if req.Write {
				rowsAffected, err := executorAgent.ExplorerExecute(c.Request.Context(), req.ConnectorType, req.Config, req.Query)
				if err != nil {
					c.JSON(http.StatusBadGateway, gin.H{
						"error":   "explorer_execute failed",
						"details": err.Error(),
					})
					return
				}
				c.JSON(http.StatusOK, gin.H{"rows_affected": rowsAffected})
				return
			}

			rows, cols, err := executorAgent.ExplorerQuery(c.Request.Context(), req.ConnectorType, req.Config, req.Query, req.Limit)
			if err != nil {
				c.JSON(http.StatusBadGateway, gin.H{
					"error":   "explorer_query failed",
					"details": err.Error(),
				})
				return
			}
			c.JSON(http.StatusOK, gin.H{
				"rows":      rows,
				"columns":   cols,
				"row_count": len(rows),
			})
		})

		// ============================================================================
		// CDC CONTROL (Demo-grade)
		// ============================================================================
		// Minimal endpoints for UI to show CDC connector status and perform a one-click restart.
		// These call the Executor agent directly (no workflow) and proxy to the Debezium MCP connector.
		//
		// SECURITY: gated by requirePrincipal. The orchestrator /api/v1 group has no
		// global auth and is reachable over the public /orchestrator Traefik route, so
		// these control endpoints previously allowed ANONYMOUS cross-tenant
		// pause/resume/provision/cleanup. requirePrincipal requires an internal-service
		// secret (api-gateway / frontend server-side proxy) OR a valid user session,
		// failing closed in production.
		cdcGrp := api.Group("", requirePrincipal(db))
		{
			// GET /api/v1/cdc/data-pipelines — list CDC pipelines for dashboard count.
			cdcGrp.GET("/cdc/data-pipelines", func(c *gin.Context) {
				// Scope to the authenticated principal. A logged-in user sees only
				// their own CDC pipelines; a trusted internal caller (the Next.js
				// dashboard) may pass an explicit X-User-ID filter or omit it for an
				// unscoped count. The old spoofable X-User-ID-only path let an
				// anonymous caller list every tenant's pipelines.
				authUser, internal := principalUserID(c)
				userID := authUser
				if internal {
					userID = strings.TrimSpace(c.GetHeader("X-User-ID"))
				}
				rows, err := db.Query(
					// NOTE: source_type / destination_type are intentionally NOT selected — those
					// columns do not exist on the pipelines table (no migration ever added them).
					// Selecting them caused this endpoint to 500 with `column "source_type" does
					// not exist`. The dashboard derives source/destination from the connection
					// objects, so the empty struct fields below are harmless.
					`SELECT id, name, status, sync_mode, created_at, updated_at
					 FROM pipelines
					 WHERE sync_mode = 'cdc'
					 AND ($1 = '' OR created_by::text = $1)
					 ORDER BY created_at DESC`,
					userID,
				)
				if err != nil {
					c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
					return
				}
				defer rows.Close()
				type cdcPipeline struct {
					ID              string    `json:"id"`
					Name            string    `json:"name"`
					Status          string    `json:"status"`
					SyncMode        string    `json:"sync_mode"`
					SourceType      string    `json:"source_type"`
					DestinationType string    `json:"destination_type"`
					CreatedAt       time.Time `json:"created_at"`
					UpdatedAt       time.Time `json:"updated_at"`
				}
				var pipelines []cdcPipeline
				for rows.Next() {
					var p cdcPipeline
					if err := rows.Scan(&p.ID, &p.Name, &p.Status, &p.SyncMode, &p.CreatedAt, &p.UpdatedAt); err != nil {
						continue
					}
					pipelines = append(pipelines, p)
				}
				if pipelines == nil {
					pipelines = []cdcPipeline{}
				}
				c.JSON(http.StatusOK, gin.H{
					"pipelines": pipelines,
					"total":     len(pipelines),
				})
			})

			type cdcControlResponse struct {
				Success       bool        `json:"success"`
				PipelineID    string      `json:"pipeline_id"`
				ConnectorName string      `json:"connector_name"`
				Result        interface{} `json:"result,omitempty"`
				Error         string      `json:"error,omitempty"`
				// RecoveryEnabled advertises whether the operator-guarded CDC recovery
				// endpoint is enabled on this deployment (CDC_RECOVERY_ENABLED). Only the
				// status handler populates it; the UI uses it to gate the "Recover"
				// affordance. omitempty keeps it off the restart/pause/resume responses.
				RecoveryEnabled bool `json:"recovery_enabled,omitempty"`
			}

			cdcGrp.GET("/cdc/pipelines/:pipeline_id/status", func(c *gin.Context) {
				pipelineID := strings.TrimSpace(c.Param("pipeline_id"))
				if pipelineID == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline_id is required"})
					return
				}
				if !assertPipelineOwner(c, db, pipelineID) {
					return
				}

				// Advertise whether operator-guarded CDC recovery is enabled on this
				// deployment. The UI uses this to show/hide the "Recover" affordance;
				// the /recover endpoint enforces the same flag (returns 403 when unset).
				recoveryEnabled := strings.EqualFold(strings.TrimSpace(os.Getenv("CDC_RECOVERY_ENABLED")), "true")

				// Mirror executor connector naming convention: cdc-<first8(pipeline_id)>
				safe8 := pipelineID
				if len(safe8) > 8 {
					safe8 = safe8[:8]
				}
				connectorName := fmt.Sprintf("cdc-%s", safe8)

				// Avoid MCP stdio hangs for status checks: query Kafka Connect directly with a tight timeout.
				ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
				defer cancel()

				connectURL := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL"))
				if connectURL == "" {
					connectURL = "http://kafka-connect:8083"
				}
				connectURL = strings.TrimRight(connectURL, "/")

				u := fmt.Sprintf("%s/connectors/%s/status", connectURL, connectorName)
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
				client := &http.Client{Timeout: 5 * time.Second}
				httpResp, err := client.Do(req)
				if err != nil {
					// Kafka Connect is unreachable (not running / wrong hostname).
					// Return a clean "unavailable" status — the raw DNS/TCP error is not
					// useful to the user and looks alarming in the UI.
					c.JSON(http.StatusOK, cdcControlResponse{
						Success:         false,
						PipelineID:      pipelineID,
						ConnectorName:   connectorName,
						RecoveryEnabled: recoveryEnabled,
						Error:           "connect_unavailable",
						Result: map[string]interface{}{
							"connector_name":    connectorName,
							"connect_available": false,
						},
					})
					return
				}
				defer httpResp.Body.Close()

				if httpResp.StatusCode == http.StatusNotFound {
					mctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					go func() {
						defer cancel()
						emitCDCStatusMetrics(mctx, kafkaManager, db, pipelineID, connectorName, executor.ExecutorResponse{
							TaskID:     uuid.NewString(),
							PipelineID: pipelineID,
							Status:     "failed",
							Error:      "not_found",
							Result: map[string]interface{}{
								"connector_name": connectorName,
								"connect_url":    connectURL,
							},
						})
					}()
					c.JSON(http.StatusOK, cdcControlResponse{
						Success:         false,
						PipelineID:      pipelineID,
						ConnectorName:   connectorName,
						RecoveryEnabled: recoveryEnabled,
						Error:           "not_found",
						Result: map[string]interface{}{
							"connector_name": connectorName,
							"connect_url":    connectURL,
						},
					})
					return
				}

				var statusPayload map[string]interface{}
				raw, rerr := io.ReadAll(httpResp.Body)
				if rerr != nil {
					resp := executor.ExecutorResponse{
						TaskID:     uuid.NewString(),
						PipelineID: pipelineID,
						Status:     "failed",
						Error:      fmt.Sprintf("kafka connect read failed: %v", rerr),
						Result: map[string]interface{}{
							"connector_name": connectorName,
							"connect_url":    connectURL,
							"status_code":    httpResp.StatusCode,
						},
					}
					mctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					go func() {
						defer cancel()
						emitCDCStatusMetrics(mctx, kafkaManager, db, pipelineID, connectorName, resp)
					}()
					c.JSON(http.StatusOK, cdcControlResponse{
						Success:         false,
						PipelineID:      pipelineID,
						ConnectorName:   connectorName,
						RecoveryEnabled: recoveryEnabled,
						Error:           "read_failed",
						Result: map[string]interface{}{
							"connector_name": connectorName,
							"connect_url":    connectURL,
							"status_code":    httpResp.StatusCode,
						},
					})
					return
				}
				if uerr := json.Unmarshal(raw, &statusPayload); uerr != nil {
					resp := executor.ExecutorResponse{
						TaskID:     uuid.NewString(),
						PipelineID: pipelineID,
						Status:     "failed",
						Error:      fmt.Sprintf("kafka connect status parse failed: %v", uerr),
						Result: map[string]interface{}{
							"connector_name": connectorName,
							"connect_url":    connectURL,
							"status_code":    httpResp.StatusCode,
						},
					}
					mctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					go func() {
						defer cancel()
						emitCDCStatusMetrics(mctx, kafkaManager, db, pipelineID, connectorName, resp)
					}()
					c.JSON(http.StatusOK, cdcControlResponse{
						Success:         false,
						PipelineID:      pipelineID,
						ConnectorName:   connectorName,
						RecoveryEnabled: recoveryEnabled,
						Error:           "parse_failed",
						Result: map[string]interface{}{
							"connector_name": connectorName,
							"connect_url":    connectURL,
							"status_code":    httpResp.StatusCode,
						},
					})
					return
				}

				// Create an ExecutorResponse-shaped object so emitCDCStatusMetrics can compute lag/freshness.
				resp := executor.ExecutorResponse{
					TaskID:     uuid.NewString(),
					PipelineID: pipelineID,
					Status:     "running",
					Result: map[string]interface{}{
						"connector_name": connectorName,
						"connect_url":    connectURL,
						"data":           statusPayload,
						"status_code":    httpResp.StatusCode,
					},
				}
				mctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				go func() {
					defer cancel()
					emitCDCStatusMetrics(mctx, kafkaManager, db, pipelineID, connectorName, resp)
				}()

				c.JSON(http.StatusOK, cdcControlResponse{
					Success:         httpResp.StatusCode >= 200 && httpResp.StatusCode < 300,
					PipelineID:      pipelineID,
					ConnectorName:   connectorName,
					RecoveryEnabled: recoveryEnabled,
					Result: map[string]interface{}{
						"connector_name": connectorName,
						"status":         statusPayload,
						"status_code":    httpResp.StatusCode,
					},
				})
			})

			cdcGrp.POST("/cdc/pipelines/:pipeline_id/restart", func(c *gin.Context) {
				pipelineID := strings.TrimSpace(c.Param("pipeline_id"))
				if pipelineID == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline_id is required"})
					return
				}
				if !assertPipelineOwner(c, db, pipelineID) {
					return
				}

				safe8 := pipelineID
				if len(safe8) > 8 {
					safe8 = safe8[:8]
				}
				connectorName := fmt.Sprintf("cdc-%s", safe8)

				ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
				defer cancel()

				connectURL := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL"))
				if connectURL == "" {
					connectURL = "http://kafka-connect:8083"
				}
				connectURL = strings.TrimRight(connectURL, "/")

				u := fmt.Sprintf("%s/connectors/%s/restart", connectURL, connectorName)
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
				client := &http.Client{Timeout: 5 * time.Second}
				httpResp, err := client.Do(req)
				if err != nil {
					c.JSON(http.StatusServiceUnavailable, cdcControlResponse{
						Success:       false,
						PipelineID:    pipelineID,
						ConnectorName: connectorName,
						Error:         fmt.Sprintf("kafka connect restart request failed: %v", err),
						Result: map[string]interface{}{
							"connector_name": connectorName,
							"connect_url":    connectURL,
						},
					})
					return
				}
				defer httpResp.Body.Close()

				// Kafka Connect's verdict decides the HTTP status, not just the body
				// (KI-CDC-CONTROL-ACTIONS-FALSE-SUCCESS-TOAST — see cdcControlOutcome).
				restartOK, httpStatus, errMsg := cdcControlOutcome("restart", connectorName, httpResp.StatusCode)

				result := map[string]interface{}{
					"connector_name": connectorName,
					"status_code":    httpResp.StatusCode,
				}

				// "Restart CDC" has to bounce BOTH legs of the pipe. Bouncing only the
				// Debezium connector left the SINK untouched, so the one recovery lever the
				// UI offers could not fix the most common way CDC dies: the sink container
				// restarted, its in-memory worker registry was wiped, and no worker is
				// consuming. Connect would answer 204, the toast would go green, and the
				// destination would still receive nothing. The sink leg is best-effort — the
				// connector restart already succeeded and must not be reported as failed
				// because the sink could not be reached — but its outcome is reported, never
				// swallowed.
				if restartOK {
					sinkCtx, sinkCancel := context.WithTimeout(c.Request.Context(), 45*time.Second)
					if serr := handlers.RestartCDCSinkWorker(sinkCtx, db, mcpServerManager, pipelineID); serr != nil {
						log.WithError(serr).WithFields(log.Fields{
							"pipeline_id":    pipelineID,
							"connector_name": connectorName,
						}).Warn("cdc restart: connector restarted but sink worker restart failed")
						result["sink_restarted"] = false
						result["sink_restart_error"] = serr.Error()
					} else {
						result["sink_restarted"] = true
					}
					sinkCancel()
				}

				c.JSON(httpStatus, cdcControlResponse{
					Success:       restartOK,
					PipelineID:    pipelineID,
					ConnectorName: connectorName,
					Error:         errMsg,
					Result:        result,
				})
			})

			cdcGrp.PUT("/cdc/pipelines/:pipeline_id/pause", func(c *gin.Context) {
				pipelineID := strings.TrimSpace(c.Param("pipeline_id"))
				if pipelineID == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline_id is required"})
					return
				}
				if !assertPipelineOwner(c, db, pipelineID) {
					return
				}
				safe8 := pipelineID
				if len(safe8) > 8 {
					safe8 = safe8[:8]
				}
				connectorName := fmt.Sprintf("cdc-%s", safe8)

				connectURL := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL"))
				if connectURL == "" {
					connectURL = "http://kafka-connect:8083"
				}
				connectURL = strings.TrimRight(connectURL, "/")

				ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
				defer cancel()

				u := fmt.Sprintf("%s/connectors/%s/pause", connectURL, connectorName)
				req, _ := http.NewRequestWithContext(ctx, http.MethodPut, u, nil)
				client := &http.Client{Timeout: 10 * time.Second}
				httpResp, err := client.Do(req)
				if err != nil {
					c.JSON(http.StatusServiceUnavailable, cdcControlResponse{
						Success: false, PipelineID: pipelineID, ConnectorName: connectorName,
						Error: fmt.Sprintf("kafka connect unreachable: %v", err),
					})
					return
				}
				defer httpResp.Body.Close()
				// A refused pause must not answer 200 — the UI reads `response.ok`, so
				// it showed "CDC pipeline paused" while the connector kept streaming
				// (KI-CDC-CONTROL-ACTIONS-FALSE-SUCCESS-TOAST). The DB row was already
				// correctly left alone; only the status lied.
				ok, httpStatus, errMsg := cdcControlOutcome("pause", connectorName, httpResp.StatusCode)
				if ok {
					_, _ = db.Exec("UPDATE pipelines SET status = 'paused', updated_at = NOW() WHERE id = $1", pipelineID)
				}
				c.JSON(httpStatus, cdcControlResponse{
					Success: ok, PipelineID: pipelineID, ConnectorName: connectorName,
					Error:  errMsg,
					Result: map[string]interface{}{"status_code": httpResp.StatusCode},
				})
			})

			cdcGrp.PUT("/cdc/pipelines/:pipeline_id/resume", func(c *gin.Context) {
				pipelineID := strings.TrimSpace(c.Param("pipeline_id"))
				if pipelineID == "" {
					c.JSON(http.StatusBadRequest, gin.H{"error": "pipeline_id is required"})
					return
				}
				if !assertPipelineOwner(c, db, pipelineID) {
					return
				}
				safe8 := pipelineID
				if len(safe8) > 8 {
					safe8 = safe8[:8]
				}
				connectorName := fmt.Sprintf("cdc-%s", safe8)

				connectURL := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL"))
				if connectURL == "" {
					connectURL = "http://kafka-connect:8083"
				}
				connectURL = strings.TrimRight(connectURL, "/")

				ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
				defer cancel()

				u := fmt.Sprintf("%s/connectors/%s/resume", connectURL, connectorName)
				req, _ := http.NewRequestWithContext(ctx, http.MethodPut, u, nil)
				client := &http.Client{Timeout: 10 * time.Second}
				httpResp, err := client.Do(req)
				if err != nil {
					c.JSON(http.StatusServiceUnavailable, cdcControlResponse{
						Success: false, PipelineID: pipelineID, ConnectorName: connectorName,
						Error: fmt.Sprintf("kafka connect unreachable: %v", err),
					})
					return
				}
				defer httpResp.Body.Close()
				// Same as pause: a refused resume answering 200 read as success to
				// every `response.ok` caller (KI-CDC-CONTROL-ACTIONS-FALSE-SUCCESS-TOAST).
				ok, httpStatus, errMsg := cdcControlOutcome("resume", connectorName, httpResp.StatusCode)
				if ok {
					_, _ = db.Exec("UPDATE pipelines SET status = 'running', updated_at = NOW() WHERE id = $1", pipelineID)
				}
				c.JSON(httpStatus, cdcControlResponse{
					Success: ok, PipelineID: pipelineID, ConnectorName: connectorName,
					Error:  errMsg,
					Result: map[string]interface{}{"status_code": httpResp.StatusCode},
				})
			})

			// DMS-like "reload/backfill" for newly added tables (Debezium ad-hoc snapshot).
			cdcGrp.POST("/cdc/pipelines/:pipeline_id/backfill", handlers.BackfillCDCTables(db))
			// Guarded operator-initiated recovery (FAILED → resnapshot / resume).
			cdcGrp.POST("/cdc/pipelines/:pipeline_id/recover", handlers.RecoverCDCPipeline(db))
			// Restart sink worker to pick up newly-added CDC topics.
			cdcGrp.POST("/cdc/pipelines/:pipeline_id/sink/restart", handlers.RestartCDCSink(db, mcpServerManager))

			// CDC Resource Provisioning (Internal - called by API Gateway)
			cdcGrp.POST("/cdc/provision", handlers.ProvisionCDCResources(db))
			cdcGrp.POST("/cdc/cleanup", handlers.CleanupCDCResources(db, mcpServerManager))
			// Phase 2 of pipeline delete: reclaim broker-side topics + consumer
			// groups. Must run AFTER the pipelines row is gone (see the handler
			// doc) — api-gateway calls it once the delete transaction commits.
			cdcGrp.POST("/cdc/kafka-teardown", handlers.TeardownPipelineKafka(db, mcpServerManager, topologyManager, cdcStatsAgent))
			cdcGrp.PUT("/cdc/tables", handlers.UpdateCDCTables(db))
		}

		// Transform Preview (for UI)
		api.POST("/transforms/preview", handlers.PreviewTransforms(executorAgent))

		// Consumer Registry API (dynamic consumer management)
		if consumerRegistry != nil {
			consumerHandlers := consumer.NewHandlers(consumerRegistry)
			consumerAPI := api.Group("/consumers")
			consumerHandlers.RegisterRoutes(consumerAPI)
		}

		// Retention Manager API (data lifecycle management)
		if retentionAgent != nil {
			retentionHandlers := retention.NewHandlers(retentionAgent)
			retentionAPI := api.Group("/retention")
			retentionHandlers.RegisterRoutes(retentionAPI)
		}

		// Topology Manager API (plan-time topic provisioning).
		//
		// requirePrincipal is mandatory here: /api/v1 has no global auth (see
		// auth_middleware.go), and this group is publicly reachable through the
		// Traefik /orchestrator route, whose only middlewares are strip-prefix,
		// security-headers and rate-limit. Unauthenticated it exposed
		// DELETE /topics/:name — a broker-level destructive verb — to the
		// internet. Sibling groups were already gated; this one was the omission.
		// Removing it is pinned by TestTopologyRoutesRequireAPrincipal.
		//
		// Authentication is only half of it: the handler needs `db` to answer
		// which of those topics belong to the CALLER's workspaces, since a valid
		// session in any workspace otherwise reaches every tenant's topics.
		if topologyManager != nil {
			topologyHandler := handlers.NewTopologyHandler(topologyManager, db)
			topologyAPI := api.Group("/topology", requirePrincipal(db))
			topologyHandler.RegisterRoutes(topologyAPI)
			log.Info("✅ Topology API registered at /api/v1/topology (auth: requirePrincipal)")
		}

		// Pre-flight Assessment API (Pillar 1).
		// One SourceAssessor per supported source type. Adding a new
		// PostgreSQL-family or MySQL-family source = one line below.
		assessmentRegistry := assessor.NewRegistry()
		assessmentRegistry.Register(assessor.NewPostgresAssessor())
		assessmentRegistry.Register(assessor.NewMySQLAssessor())
		// Universal fallback: any source WITHOUT a dedicated deep assessor
		// (every SaaS/REST/GraphQL/cloud-storage/warehouse connector) is still
		// pre-flighted via its own MCP test_connection — connectivity, required
		// config and credential/scope validity. Read-only; never mutates source.
		assessmentRegistry.SetDefault(assessor.NewConnectorAssessor(mcp.NewClient(mcpServerManager)))
		connMgr := connections.NewManager(db)
		assessmentHandler := handlers.NewAssessmentHandler(db, connMgr, assessmentRegistry)
		assessmentHandler.RegisterRoutes(api)
		log.WithField("supported_types", assessmentRegistry.SupportedTypes()).
			Info("✅ Pre-flight Assessment API registered at /api/v1/pipelines/:id/assess")

		// Connector Health Watchdog (Pillar 5).
		// Hourly rollup of per-(connector_type, version) success rates;
		// alerts ops via the existing rsync.notifications topic when a
		// new version regresses >20pp below its predecessor.
		// Use context.Background() so the watchdog survives until process
		// shutdown. The other long-running goroutines in this main use
		// the same pattern (see retentionAgent.Start, consumerRegistry.Start).
		watchdog := healthwatch.New(db, kafkaManager)
		go watchdog.Start(context.Background())
		healthVersionsHandler := handlers.NewHealthVersionsHandler(watchdog)
		healthVersionsHandler.RegisterRoutes(api)
		log.Info("✅ Connector Health Watchdog started; admin API at /api/v1/health/connector-versions")

		// NOTE: Workflow-driving agent endpoints are not exposed here.
	}

	return router
}
