package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/saramaauth"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	"github.com/rsync-ai/backend-temporal-adapter/internal/adapter"
	"github.com/rsync-ai/backend-temporal-adapter/internal/db"
	"github.com/rsync-ai/backend-temporal-adapter/internal/metrics"
	"github.com/rsync-ai/backend-temporal-adapter/internal/telemetry"
	"github.com/rsync-ai/backend-temporal-adapter/internal/workflows"
)

// localDatabaseHosts are hostnames that only ever point at an in-cluster dev
// Postgres. A staging or production deployment must use a real managed database
// (an Azure FQDN), never one of these.
var localDatabaseHosts = map[string]bool{
	"postgres":  true, // docker-compose service name for the dev Postgres
	"localhost": true,
	"127.0.0.1": true,
	"::1":       true,
}

// envIsTrue reports whether an env var holds an affirmative value.
func envIsTrue(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// remoteDatabaseViolation returns a human-readable reason when a deployment that
// declared it requires a remote database (requireRemote, wired from
// RSYNC_REQUIRE_REMOTE_DB) is instead pointed at a local/in-cluster Postgres
// host. An empty return means OK. Pure + side-effect-free so it can be
// unit-tested; requireRemoteDatabase is the thin os.Exit wrapper.
func remoteDatabaseViolation(requireRemote bool, host string) string {
	if !requireRemote {
		return ""
	}
	h := strings.ToLower(strings.TrimSpace(host))
	if h == "" {
		return "POSTGRES_HOST is empty"
	}
	if localDatabaseHosts[h] {
		return fmt.Sprintf("POSTGRES_HOST=%q is the local dev Postgres", h)
	}
	return ""
}

// requireRemoteDatabase crashes the adapter at startup when it is wired to the
// local dev Postgres but the deployment declared it must be remote
// (RSYNC_REQUIRE_REMOTE_DB, set by docker-compose.prod.yml and inherited by the
// staging overlay). The adapter owns pipeline control-plane state (executions,
// pipeline_progress) and resolves scheduled runs against this DB. If it falls
// back to the in-cluster dev Postgres while the rest of the stack uses the real
// (Azure) database, scheduled runs silently skip as "pipeline not found
// (orphaned schedule)" — the schedule fires but no run is ever recorded. Far
// better to never start.
//
// It is intentionally NOT gated on ENVIRONMENT: the staging overlay sets
// ENVIRONMENT=local while still requiring a remote DB.
func requireRemoteDatabase() {
	if reason := remoteDatabaseViolation(envIsTrue("RSYNC_REQUIRE_REMOTE_DB"), getEnv("POSTGRES_HOST", "postgres")); reason != "" {
		log.Fatalf("❌ Refusing to start: %s, but this deployment requires a remote database "+
			"(RSYNC_REQUIRE_REMOTE_DB is set). The stack was almost certainly launched without "+
			"`--env-file .env.staging` — set POSTGRES_HOST to the real (Azure) database and relaunch.", reason)
	}
}

// requireRealEncryptionKey crashes the adapter at startup when ENCRYPTION_KEY is
// missing or a known dev default on a real remote-DB deployment. The adapter
// decrypts connection configs inside self-healing activities, so it MUST share the
// exact key api-gateway/orchestrator use. The base docker-compose falls back to a
// dev key (${ENCRYPTION_KEY:-dev-...}) when the var is unset, which silently
// violated that and produced "invalid ciphertext". Like requireRemoteDatabase it is
// gated on deploy-intent (RSYNC_REQUIRE_REMOTE_DB, inherited from
// docker-compose.prod.yml), NOT ENVIRONMENT — the staging overlay leaves the adapter
// on ENVIRONMENT=development, so an ENVIRONMENT check would miss staging.
func requireRealEncryptionKey() {
	if !envIsTrue("RSYNC_REQUIRE_REMOTE_DB") {
		return
	}
	// Prefer the ENCRYPTION_KEYS keyring primary; fall back to ENCRYPTION_KEY.
	key := ""
	if keys := strings.TrimSpace(os.Getenv("ENCRYPTION_KEYS")); keys != "" {
		for _, p := range strings.Split(keys, ",") {
			if s := strings.TrimSpace(p); s != "" {
				key = s
				break
			}
		}
	} else {
		key = strings.TrimSpace(os.Getenv("ENCRYPTION_KEY"))
	}
	if key == "" || len(key) < 32 ||
		key == "dev-encryption-key-32-bytes-long!!" || key == "dev-only-key-please-change-me!!!" {
		log.Fatalf("❌ Refusing to start: missing/unsafe ENCRYPTION_KEY (must be >= 32 chars and not a dev " +
			"default), but this deployment requires a remote database (RSYNC_REQUIRE_REMOTE_DB is set). The " +
			"stack was almost certainly launched without exporting ENCRYPTION_KEY — run " +
			"`set -a; source .env.staging` and relaunch.")
	}
}

func main() {
	// Structured JSON logging + trace-context hook (log-trace correlation in SigNoz)
	telemetry.InitLogging("temporal-adapter")

	// OTel tracer → OTEL Collector → SigNoz
	shutdownTracer, err := telemetry.InitTracer("temporal-adapter")
	if err != nil {
		log.Warnf("OTel tracer init failed (non-fatal): %v", err)
	} else {
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdownTracer(ctx); err != nil {
				log.Warnf("OTel tracer shutdown error: %v", err)
			}
		}()
	}

	log.Info("🚀 Starting Temporal Adapter Service...")

	// Prometheus /metrics endpoint (scraped by the OTEL Collector's
	// `rsync-temporal-adapter` job → SigNoz). Internal-only port 8082;
	// no host mapping. F-Obs-2.
	metricsAddr := getEnv("METRICS_ADDR", ":8082")
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		// Explicit timeouts (Slowloris / slow-body hardening — gosec G114).
		srv := &http.Server{
			Addr:              metricsAddr,
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       60 * time.Second,
			WriteTimeout:      120 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		log.WithField("addr", metricsAddr).Info("📊 Metrics server listening on /metrics")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Warnf("metrics server error: %v", err)
		}
	}()

	// Refuse to start if this remote-DB deployment (staging/prod) is wired to
	// the local dev Postgres — the silent docker-compose fallback that makes
	// scheduled runs skip as "orphaned" because the Azure pipeline isn't in the
	// local DB. See requireRemoteDatabase.
	requireRemoteDatabase()

	// Independent of ENVIRONMENT, refuse to start with a missing/dev ENCRYPTION_KEY
	// on a real remote-DB deployment — the adapter must share the exact key the rest
	// of the stack uses to decrypt connection configs. See requireRealEncryptionKey.
	requireRealEncryptionKey()

	// Initialize Database connection for StateUpdateActivity
	if err := db.Init(); err != nil {
		log.Warnf("⚠️  Database connection failed: %v (StateUpdateActivity will be disabled)", err)
	} else {
		defer db.Close()
		log.Info("✅ Database connection established")
	}

	// Create Temporal client
	temporalAddress := getEnv("TEMPORAL_ADDRESS", "temporal:7233")
	temporalClient, err := client.Dial(client.Options{
		HostPort:  temporalAddress,
		Namespace: "default",
	})
	if err != nil {
		log.Fatalf("Failed to create Temporal client: %v", err)
	}
	defer temporalClient.Close()

	log.WithField("address", temporalAddress).Info("✅ Connected to Temporal server")

	// Create Kafka producer for activities.
	// Accept either env name (compose sets KAFKA_BOOTSTRAP_SERVERS for this
	// service; KAFKA_BROKERS kept for back-compat). Precedence: BROKERS ->
	// BOOTSTRAP_SERVERS -> default, matching the llm-service helpers.
	kafkaBrokers := getEnv("KAFKA_BROKERS", getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
	kafkaProducer, err := createKafkaProducer(kafkaBrokers)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()

	// Initialize activity context (for Kafka producer and DB)
	workflows.InitActivityContext(kafkaProducer)
	workflows.SetDB(db.GetDB())

	// Initialize correlation store for V2 activities (Phase D).
	// REQUIRED, not optional: every V2 activity (Intent/Connector/Planner/Validator/
	// Executor) writes its request through this store, so a nil store is NOT a
	// "disabled" mode — it panics with a nil-pointer dereference on the first activity
	// (nil *correlation.Store receiver → store.go s.redis.Set). Fail loud at startup,
	// the same as the Temporal client and Kafka producer above, instead of silently
	// running an adapter that crashes every workflow. A frequent cause is a Redis auth
	// split-brain: this service started with --env-file .env.staging (REDIS_PASSWORD
	// set) while the running redis container has no --requirepass (or vice-versa) —
	// bring the whole stack up with the SAME env so redis and its clients agree.
	redisAddr := getEnv("REDIS_ADDRESS", "redis:6379")
	redisPassword := getEnv("REDIS_PASSWORD", "")
	if err := workflows.InitCorrelationStore(redisAddr, redisPassword); err != nil {
		log.Fatalf("❌ Refusing to start: correlation store (Redis) init failed: %v "+
			"(REDIS_ADDRESS=%s) — V2 activities cannot run without it", err, redisAddr)
	}

	// Create Temporal worker (registers workflows and activities).
	// The metrics interceptor records activity/workflow outcome + duration
	// for every execution without editing each function. F-Obs-2.
	w := worker.New(temporalClient, "pipeline-workflows", worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{metrics.NewWorkerInterceptor()},
	})

	// Register workflows - V2 ONLY
	w.RegisterWorkflow(workflows.NLPipelineWorkflowV2)         // V2 NL-driven workflow (deterministic, state machine)
	w.RegisterWorkflow(workflows.ScheduledPipelineRunWorkflow) // Scheduled run wrapper (creates execution, starts child)
	w.RegisterWorkflow(workflows.ScheduledModelRunWorkflow)    // Scheduled saved-query model rebuild (migration 085)

	// Register shared activities (used by all workflows)
	w.RegisterActivity(workflows.EmitDomainEventActivity)
	w.RegisterActivity(workflows.SendToPipelineDLQ)
	w.RegisterActivity(workflows.SendToAgentDLQ)
	w.RegisterActivity(workflows.StateUpdateActivity)          // Architecture Phase 1: Authoritative state writer
	w.RegisterActivity(workflows.UpdatePipelineStatusActivity) // Keeps pipelines.status in sync with workflow outcome

	// Register V2 activities (Phase D: Request/Reply pattern)
	w.RegisterActivity(workflows.IntentActivityV2)
	w.RegisterActivity(workflows.ConnectorResolverActivityV2)
	w.RegisterActivity(workflows.ConnectorAvailabilityActivityV2)
	w.RegisterActivity(workflows.GenerateConnectorActivityV2)
	w.RegisterActivity(workflows.ConnectionValidationActivityV2)
	w.RegisterActivity(workflows.ConnectionValidatorActivityV2)
	w.RegisterActivity(workflows.FetchConnectionOAuthTokenIDActivity)
	w.RegisterActivity(workflows.RefreshOAuthTokenActivity)
	w.RegisterActivity(workflows.PlanSchemaDriftRepairActivity)
	w.RegisterActivity(workflows.ApplySchemaDDLActivity)
	w.RegisterActivity(workflows.ValidateSchemaRepairActivity)
	w.RegisterActivity(workflows.PlannerActivityV2)
	w.RegisterActivity(workflows.ValidatorActivityV2)
	w.RegisterActivity(workflows.CostEstimatorActivityV2)
	w.RegisterActivity(workflows.ExecutorActivityV2)
	w.RegisterActivity(workflows.CleanupPartialDataActivityV2)

	// DAG + HITL activities
	w.RegisterActivity(workflows.ExecuteGraphNodeActivityV2)
	w.RegisterActivity(workflows.InterpretNodeInputActivity)

	// Register scheduled run activities
	w.RegisterActivity(workflows.CheckActiveRunActivity)
	w.RegisterActivity(workflows.CreateScheduledExecutionActivity)
	w.RegisterActivity(workflows.FetchPipelineRunContextActivity)
	w.RegisterActivity(workflows.MarkExecutionFailedActivity)
	w.RegisterActivity(workflows.RunModelActivity)

	log.Info("✅ Registered workflows and activities (including scheduled run workflow)")

	// Start Temporal worker
	if err := w.Start(); err != nil {
		log.Fatalf("Failed to start Temporal worker: %v", err)
	}
	defer w.Stop()

	log.Info("✅ Temporal worker started")

	// Create Kafka adapter (consumes agent.results, signals workflows)
	kafkaAdapter, err := adapter.NewKafkaAdapter(kafkaBrokers, temporalClient)
	if err != nil {
		log.Fatalf("Failed to create Kafka adapter: %v", err)
	}

	// Start the adapter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := kafkaAdapter.Start(ctx); err != nil {
		log.Fatalf("Failed to start Kafka adapter: %v", err)
	}

	log.Info("================================================================================")
	log.Info("✅ Temporal Adapter Service is running")
	log.Info("   - Temporal Worker: Executing workflows and activities")
	log.Info("   - Kafka Consumer: agent.control.results (signals workflows)")
	log.Info("   - Kafka Producer: agent.control.commands (via activities)")
	log.Info("   - Pattern: Temporal thinks, Kafka talks, Agents act")
	log.Info("Press Ctrl+C to stop")
	log.Info("================================================================================")

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Info("🛑 Shutting down Temporal Adapter Service...")
	cancel()

	// Graceful shutdown
	kafkaAdapter.Stop()
	w.Stop()
	log.Info("✅ Temporal Adapter Service stopped")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// newProducerConfig builds everything the producer needs except the connection.
//
// Split out from createKafkaProducer purely so it can be tested: the rest of that
// function dials a broker, so without this split there is no way to assert what
// identity or security profile we present without a live cluster.
func newProducerConfig(brokers string) (*sarama.Config, kafkaclient.Config, error) {
	// Security (TLS/SASL) comes from the environment; the address stays whatever
	// the caller was already given, so this changes only HOW we connect.
	//
	// adapter.ServiceName rather than a literal: the consumer in internal/adapter
	// resolves its own config, and the two are one process. Sharing the constant
	// is what stops them presenting two identities to the same cluster.
	security, err := kafkaclient.FromEnvForService(adapter.ServiceName, brokers)
	if err != nil {
		return nil, kafkaclient.Config{}, fmt.Errorf("invalid Kafka security configuration: %w", err)
	}
	security = security.WithBrokers(brokers)

	config := sarama.NewConfig()
	config.Version = sarama.V3_4_0_0
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Timeout = 10 * time.Second

	// []string{brokers} collapsed a multi-broker "b1:9093,b2:9093" into one
	// unresolvable hostname; Apply adds TLS/SASL and leaves PLAINTEXT untouched.
	if err := saramaauth.Apply(config, security); err != nil {
		return nil, kafkaclient.Config{}, fmt.Errorf("invalid Kafka security configuration: %w", err)
	}
	return config, security, nil
}

func createKafkaProducer(brokers string) (sarama.SyncProducer, error) {
	config, security, err := newProducerConfig(brokers)
	if err != nil {
		return nil, err
	}
	return sarama.NewSyncProducer(security.Brokers, config)
}
