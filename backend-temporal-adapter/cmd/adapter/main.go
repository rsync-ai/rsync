package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/saramaauth"
	log "github.com/sirupsen/logrus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/worker"

	"github.com/rsync-ai/backend-temporal-adapter/internal/adapter"
	"github.com/rsync-ai/backend-temporal-adapter/internal/db"
	"github.com/rsync-ai/backend-temporal-adapter/internal/metrics"
	"github.com/rsync-ai/backend-temporal-adapter/internal/telemetry"
	"github.com/rsync-ai/backend-temporal-adapter/internal/workflows"
)

// adapterTaskQueue is the queue this worker polls.
//
// A constant because it is now named twice: once by the worker that polls it, and once
// by the freshness sweep started below. Two literals would be one typo away from a
// workflow that starts successfully and is never picked up by anything — which presents
// as a monitor that simply never reports, the failure mode hardest to notice.
const adapterTaskQueue = "pipeline-workflows"

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

// startupSettingProblems returns one message per setting the adapter needs but was
// not given, each naming the setting and exactly what will not work without it.
// Messages never contain a value. getenv is os.Getenv in main and a map in tests.
//
// These are ERROR lines, not a refusal to start: the start-path census for issue
// #24 found INTERNAL_SERVICE_SECRET empty on the dev compose, the CI gates, a bare
// quickstart compose and Helm, and the adapter still runs pipelines without it.
// The ENCRYPTION_KEY rule mirrors getEncryptionKeyForDecrypt
// (internal/workflows/nl_pipeline_v2_activities.go), the only reader of that key.
func startupSettingProblems(getenv func(string) string) []string {
	var problems []string
	if strings.TrimSpace(getenv("INTERNAL_SERVICE_SECRET")) == "" {
		problems = append(problems, "INTERNAL_SERVICE_SECRET is not set, empty or only spaces. "+
			"Scheduled saved-query runs and the model freshness sweep call the API gateway's "+
			"internal endpoints and will fail on every attempt, so schedules shown as active "+
			"never run. Generate one (for example openssl rand -hex 32) and give the same value "+
			"to api-gateway, orchestrator, temporal-adapter and frontend.")
	}
	// Exact, untrimmed match, like the decrypt helper: "Development" is not development.
	environment := getenv("ENVIRONMENT")
	isDev := environment == "development" || environment == "dev"
	key := strings.TrimSpace(getenv("ENCRYPTION_KEY"))
	switch {
	case key == "" && !isDev:
		problems = append(problems, "ENCRYPTION_KEY is not set, empty or only spaces and "+
			"ENVIRONMENT is not development. "+encryptionKeyConsequence+" (These steps read "+
			"ENCRYPTION_KEY, not ENCRYPTION_KEYS.) Set ENCRYPTION_KEY to the same value the API "+
			"gateway and orchestrator use.")
	case key != "" && len(key) < 32:
		problems = append(problems, "ENCRYPTION_KEY is shorter than 32 characters. "+
			encryptionKeyConsequence+" Set ENCRYPTION_KEY to the same value, at least 32 "+
			"characters long, that the API gateway and orchestrator use.")
	}
	return problems
}

// encryptionKeyConsequence says what stops working when getEncryptionKeyForDecrypt
// rejects the key. Its callers are the three self-healing activities that read a
// connection's stored settings: FetchConnectionOAuthTokenIDActivity,
// PlanSchemaDriftRepairActivity and ApplySchemaDDLActivity.
const encryptionKeyConsequence = "The adapter cannot decrypt stored connection settings, so " +
	"self-healing cannot look up a connection's OAuth token to refresh it, cannot plan a " +
	"schema-drift repair and cannot apply the repair to the destination table."

// startupCheckLogPrefix starts every startup-check line. docs/deployment/env-vars.md
// tells operators to grep for it, so it is part of the contract.
const startupCheckLogPrefix = "Startup check: "

// reportStartupSettingProblems runs the startup check against getenv and writes each
// problem to logger as one ERROR line starting with startupCheckLogPrefix. main passes
// log.StandardLogger() and os.Getenv; tests pass a hooked logger and a map.
func reportStartupSettingProblems(logger *log.Logger, getenv func(string) string) {
	for _, problem := range startupSettingProblems(getenv) {
		logger.Error(startupCheckLogPrefix + problem)
	}
}

func main() {
	// Structured JSON logging + trace-context hook (log-trace correlation)
	telemetry.InitLogging("temporal-adapter")

	// OTel tracer → OTel Collector → your OTLP backend
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

	// Ops listener: Prometheus /metrics (scraped by an OTEL Collector
	// or Prometheus, F-Obs-2) and /version (probed by
	// api-gateway's drift check). Internal-only port 8082; no host mapping.
	// See ops_server.go.
	metricsAddr := getEnv("METRICS_ADDR", defaultOpsAddr)
	go func() {
		// Explicit timeouts (Slowloris / slow-body hardening — gosec G114).
		srv := &http.Server{
			Addr:              metricsAddr,
			Handler:           newOpsMux(),
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       60 * time.Second,
			WriteTimeout:      120 * time.Second,
			IdleTimeout:       120 * time.Second,
		}
		log.WithField("addr", metricsAddr).Info("📊 Ops server listening on /metrics and /version")
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

	// Settings whose absence does not stop the adapter but silently breaks a
	// feature: one ERROR line each, naming the setting and what will not work.
	// Before db.Init and the Temporal client, whose failures can exit the process.
	reportStartupSettingProblems(log.StandardLogger(), os.Getenv)

	// Initialize Database connection for StateUpdateActivity
	if err := db.Init(); err != nil {
		log.Warnf("⚠️  Database connection failed: %v (StateUpdateActivity will be disabled)", err)
	} else {
		defer db.Close()
		log.Info("✅ Database connection established")
	}

	// Create Temporal client.
	//
	// Bounded startup retry, the same shape api-gateway uses (cmd/server/main.go)
	// and for the same cold-boot race: this process can win the start against the
	// Temporal frontend and dial a port nothing is listening on yet. The two
	// services differ in what happens when the retries run out, and the difference
	// is not stylistic. The gateway carries on with a nil client because it serves
	// its whole read surface without Temporal; this adapter cannot -- every worker
	// below is constructed from this client -- so exhausting the budget is still
	// fatal here. What changes is that it is fatal after 60s of trying instead of
	// on the first refused connection.
	//
	// The chart's wait-for-deps initContainer waits on this port too, which is the
	// deterministic fix; this is the one that also holds on compose, on bare metal,
	// and in the window where the frontend accepts a connection just before it is
	// ready to serve -- none of which an initContainer can see.
	temporalAddress := getEnv("TEMPORAL_ADDRESS", "temporal:7233")
	var temporalClient client.Client
	temporalDeadline := time.Now().Add(60 * time.Second)
	for attempt := 1; ; attempt++ {
		c, dialErr := client.Dial(client.Options{
			HostPort:  temporalAddress,
			Namespace: "default",
		})
		if dialErr == nil {
			temporalClient = c
			break
		}
		if time.Now().After(temporalDeadline) {
			log.Fatalf("Failed to create Temporal client at %s after %d attempts over 60s: %v",
				temporalAddress, attempt, dialErr)
		}
		log.Warnf("⏳ Temporal not reachable yet at %s (attempt %d): %v — retrying in 3s",
			temporalAddress, attempt, dialErr)
		time.Sleep(3 * time.Second)
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
	// The fan-out's dispatch activity signal-with-starts a model's refresh loop, which
	// is a client-side operation the workflow API cannot express.
	workflows.SetTemporalClient(temporalClient)

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
	w := worker.New(temporalClient, adapterTaskQueue, worker.Options{
		Interceptors: []interceptor.WorkerInterceptor{metrics.NewWorkerInterceptor()},
	})

	registerWorkflowsAndActivities(w)

	log.Info("✅ Registered workflows and activities (including scheduled run workflow)")

	// Start Temporal worker
	if err := w.Start(); err != nil {
		log.Fatalf("Failed to start Temporal worker: %v", err)
	}
	defer w.Stop()

	log.Info("✅ Temporal worker started")

	// Started AFTER w.Start(), so the queue already has a poller when the first tick
	// lands. Started here at all — rather than by whoever first sets a deadline —
	// because the sweep has to be running before anyone needs it: the models it exists
	// to catch are the ones nothing else is firing for.
	startModelFreshnessSweep(temporalClient)

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

// startModelFreshnessSweep makes sure the singleton freshness sweep is running.
//
// USE_EXISTING is what makes this safe to call at every boot and from every replica: the
// second caller is handed the running execution rather than an AlreadyStarted error, so
// there is exactly one sweep no matter how many adapters come up. The alternative —
// start it once by hand — leaves the monitor off after any environment rebuild, and
// nothing would report that, because a staleness monitor that is not running looks
// identical to a workspace where nothing is stale.
//
// A failure here is logged, not fatal. The adapter's other work does not depend on the
// sweep, and refusing to boot the pipeline engine because a monitor could not start
// would turn a reporting gap into an outage.
func startModelFreshnessSweep(temporalClient client.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Zero means the workflow uses its own default. Read HERE rather than inside the
	// workflow because workflow code must be deterministic, and an environment read
	// would take a different path on replay than it did on the original execution.
	interval := 0
	if raw := strings.TrimSpace(os.Getenv("MODEL_FRESHNESS_SWEEP_SECONDS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			interval = n
		} else {
			log.Warnf("⚠️  MODEL_FRESHNESS_SWEEP_SECONDS=%q is not a positive integer; using the default interval", raw)
		}
	}

	run, err := temporalClient.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       workflows.ModelFreshnessWorkflowID,
		TaskQueue:                adapterTaskQueue,
		WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
	}, workflows.ModelFreshnessWorkflow, workflows.ModelFreshnessInput{IntervalSeconds: interval})
	if err != nil {
		log.WithError(err).Error("⚠️  could not start the model freshness sweep; stale models will not be reported")
		return
	}
	log.Infof("✅ Model freshness sweep running (workflow_id=%s run_id=%s)", run.GetID(), run.GetRunID())
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

// registerWorkflowsAndActivities puts every workflow and activity on the worker.
// A function of its own, taking the registry interface, so a test can run a
// workflow through exactly this registration: an activity missing here fails only
// at run time, when a workflow first schedules it.
func registerWorkflowsAndActivities(r worker.Registry) {
	// Register workflows - V2 ONLY
	r.RegisterWorkflow(workflows.NLPipelineWorkflowV2)         // V2 NL-driven workflow (deterministic, state machine)
	r.RegisterWorkflow(workflows.ScheduledPipelineRunWorkflow) // Scheduled run wrapper (creates execution, starts child)
	r.RegisterWorkflow(workflows.ScheduledModelRunWorkflow)    // Scheduled saved-query model rebuild (migration 085)
	r.RegisterWorkflow(workflows.ModelRefreshWorkflow)         // Event-triggered model rebuild, one long-lived run per model
	r.RegisterWorkflow(workflows.UpstreamFanOutWorkflow)       // One completion -> one child per downstream model
	r.RegisterWorkflow(workflows.ModelRefreshDispatchWorkflow) // One child: deliver one completion to one model
	r.RegisterWorkflow(workflows.ModelFreshnessWorkflow)       // Singleton durable timer: notices rebuilds that never came

	// Register shared activities (used by all workflows)
	r.RegisterActivity(workflows.EmitDomainEventActivity)
	r.RegisterActivity(workflows.SendToPipelineDLQ)
	r.RegisterActivity(workflows.SendToAgentDLQ)
	r.RegisterActivity(workflows.StateUpdateActivity)          // Architecture Phase 1: Authoritative state writer
	r.RegisterActivity(workflows.UpdatePipelineStatusActivity) // Keeps pipelines.status in sync with workflow outcome

	// Register V2 activities (Phase D: Request/Reply pattern)
	r.RegisterActivity(workflows.IntentActivityV2)
	r.RegisterActivity(workflows.ConnectorResolverActivityV2)
	r.RegisterActivity(workflows.ConnectorAvailabilityActivityV2)
	r.RegisterActivity(workflows.GenerateConnectorActivityV2)
	r.RegisterActivity(workflows.ConnectionValidationActivityV2)
	r.RegisterActivity(workflows.ConnectionValidatorActivityV2)
	r.RegisterActivity(workflows.FetchConnectionOAuthTokenIDActivity)
	r.RegisterActivity(workflows.RefreshOAuthTokenActivity)
	r.RegisterActivity(workflows.PlanSchemaDriftRepairActivity)
	r.RegisterActivity(workflows.ApplySchemaDDLActivity)
	r.RegisterActivity(workflows.ValidateSchemaRepairActivity)
	r.RegisterActivity(workflows.PlannerActivityV2)
	r.RegisterActivity(workflows.ValidatorActivityV2)
	r.RegisterActivity(workflows.CostEstimatorActivityV2)
	r.RegisterActivity(workflows.ExecutorActivityV2)
	r.RegisterActivity(workflows.CleanupPartialDataActivityV2)

	// DAG + HITL activities
	r.RegisterActivity(workflows.ExecuteGraphNodeActivityV2)
	r.RegisterActivity(workflows.InterpretNodeInputActivity)

	// Register scheduled run activities
	r.RegisterActivity(workflows.CheckActiveRunActivity)
	r.RegisterActivity(workflows.CreateScheduledExecutionActivity)
	r.RegisterActivity(workflows.FetchPipelineRunContextActivity)
	r.RegisterActivity(workflows.MarkExecutionFailedActivity)
	r.RegisterActivity(workflows.RunModelActivity)
	r.RegisterActivity(workflows.RecordModelRunFailureActivity) // Records a model run that never reported a result
	r.RegisterActivity(workflows.SignalModelRefreshActivity)
	r.RegisterActivity(workflows.SweepModelFreshnessActivity)
}
