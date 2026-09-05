package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/rsync-ai/shared/crypto"
	"github.com/rsync-ai/shared/kafkaclient"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"api-gateway/internal/cache"
	"api-gateway/internal/chat"
	"api-gateway/internal/config"
	"api-gateway/internal/db"
	"api-gateway/internal/handlers"
	"api-gateway/internal/kafka"
	"api-gateway/internal/metrics"
	"api-gateway/internal/notifier"
	"api-gateway/internal/projector"
	"api-gateway/internal/retention"
	"api-gateway/internal/telemetry"
	"api-gateway/internal/websocket"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"go.temporal.io/sdk/client"
)

// isProductionLikeEnv reports whether ENVIRONMENT means "production"
// (fail-closed). Mirrors the polarity used by AuthRequiredMiddleware:
// dev mode requires EXPLICIT opt-in via known dev values; everything
// else is production. See `handlers.isProductionLike` for the
// rationale.
func isProductionLikeEnv() bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
	switch v {
	case "development", "dev", "test", "local":
		return false
	default:
		return true
	}
}

// resolveKafkaBrokers turns the KAFKA_BROKERS bootstrap string into the list
// every Kafka client in this process is handed.
//
// kafkaclient.ParseBrokers rather than strings.Split: a multi-broker list is
// routinely written with spaces after the commas ("b1:9092, b2:9092"), and a
// raw split hands the clients a space-padded address that never resolves. The
// kafka-go paths launder the list through the shared security config, which
// trims it and drops blanks; the sarama consumer groups pass it to the broker
// verbatim, so the trimming has to happen here to reach all of them.
func resolveKafkaBrokers(raw string) []string {
	brokers := kafkaclient.ParseBrokers(raw)
	if len(brokers) == 0 {
		// Also covers a value that is nothing but separators (", ,"), which a
		// bare split would turn into a list of empty broker addresses.
		brokers = []string{"localhost:9092"}
	}
	return brokers
}

func requireProdSecret(name string, minLen int, forbiddenExact ...string) {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		log.Fatalf("❌ Missing required env var in production: %s", name)
	}
	if minLen > 0 && len(v) < minLen {
		log.Fatalf("❌ Env var %s must be at least %d characters in production", name, minLen)
	}
	for _, bad := range forbiddenExact {
		if v == bad {
			log.Fatalf("❌ Env var %s is set to an unsafe default in production; set a real secret", name)
		}
	}
}

func requireProdEncryptionKeys() {
	// Prefer ENCRYPTION_KEYS (keyring). Fall back to ENCRYPTION_KEY.
	keys := strings.TrimSpace(os.Getenv("ENCRYPTION_KEYS"))
	if keys != "" {
		parts := strings.Split(keys, ",")
		primary := ""
		for _, p := range parts {
			s := strings.TrimSpace(p)
			if s != "" {
				primary = s
				break
			}
		}
		if primary == "" {
			log.Fatalf("❌ ENCRYPTION_KEYS is set but empty/invalid in production")
		}
		if len(primary) < 32 {
			log.Fatalf("❌ Primary encryption key in ENCRYPTION_KEYS must be at least 32 characters in production")
		}
		// Reject known unsafe dev defaults
		if primary == "dev-encryption-key-32-bytes-long!!" || primary == "dev-only-key-please-change-me!!!" {
			log.Fatalf("❌ Primary encryption key is an unsafe dev default in production; set a real secret")
		}
		return
	}
	requireProdSecret("ENCRYPTION_KEY", 32, "dev-encryption-key-32-bytes-long!!", "dev-only-key-please-change-me!!!")
}

// localDatabaseHosts are hostnames that only ever point at an in-cluster dev
// Postgres (the docker-compose `postgres` service, or a loopback address). A
// staging or production deployment must use a real managed database (an Azure
// FQDN), never one of these.
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

// trustCloudflareClientIP reports whether ClientIP() should be derived from
// Cloudflare's CF-Connecting-IP header. Cloud (app.rsync.ai) runs behind
// Cloudflare -> Traefik, so this DEFAULTS to true (cloud behavior); the OSS
// self-host compose sets RSYNC_TRUST_CLOUDFLARE=false when NOT fronted by
// Cloudflare. Mirrors the default-cloud polarity used for billingEnforced().
func trustCloudflareClientIP() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("RSYNC_TRUST_CLOUDFLARE"))) {
	case "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

// cloudflareProxyCIDRs is Cloudflare's published edge IP ranges (IPv4 + IPv6).
// Source: https://www.cloudflare.com/ips/ — Cloudflare updates this list
// occasionally, so refresh it from that URL if edge IPs change.
//
// When api-gateway trusts CF-Connecting-IP (trustCloudflareClientIP), gin's
// X-Forwarded-For FALLBACK must trust ONLY these proxies. Otherwise a
// direct-to-origin request with a rotating XFF still bypasses the per-IP auth
// brute-force limiter (SEC-M-07).
var cloudflareProxyCIDRs = []string{
	// IPv4
	"173.245.48.0/20", "103.21.244.0/22", "103.22.200.0/22", "103.31.4.0/22",
	"141.101.64.0/18", "108.162.192.0/18", "190.93.240.0/20", "188.114.96.0/20",
	"197.234.240.0/22", "198.41.128.0/17", "162.158.0.0/15", "104.16.0.0/13",
	"104.24.0.0/14", "172.64.0.0/13", "131.0.72.0/22",
	// IPv6
	"2400:cb00::/32", "2606:4700::/32", "2803:f800::/32", "2405:b500::/32",
	"2405:8100::/32", "2a06:98c0::/29", "2c0f:f248::/32",
}

// trustedProxyCIDRs returns the CIDR ranges gin should trust for the
// X-Forwarded-For fallback. Defaults to Cloudflare's edge ranges; override the
// whole list with RSYNC_TRUSTED_PROXY_CIDRS (comma-separated) when fronted by a
// non-Cloudflare reverse proxy.
func trustedProxyCIDRs() []string {
	raw := strings.TrimSpace(os.Getenv("RSYNC_TRUSTED_PROXY_CIDRS"))
	if raw == "" {
		return cloudflareProxyCIDRs
	}
	out := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		if s := strings.TrimSpace(part); s != "" {
			out = append(out, s)
		}
	}
	if len(out) == 0 {
		return cloudflareProxyCIDRs
	}
	return out
}

// remoteDatabaseViolation returns a human-readable reason when a deployment
// that declared it requires a remote database (requireRemote, wired from
// RSYNC_REQUIRE_REMOTE_DB) is instead pointed at a local/in-cluster Postgres.
// An empty return means OK. It is pure + side-effect-free so it can be
// unit-tested; requireRemoteDatabase is the thin os.Exit wrapper. It
// deliberately never echoes the DATABASE_URL (it carries the DB password) —
// only the host.
func remoteDatabaseViolation(requireRemote bool, databaseURL string) string {
	if !requireRemote {
		return ""
	}
	raw := strings.TrimSpace(databaseURL)
	if raw == "" {
		return "DATABASE_URL is empty"
	}
	if u, err := url.Parse(raw); err == nil {
		if host := strings.ToLower(u.Hostname()); host != "" {
			if localDatabaseHosts[host] {
				return fmt.Sprintf("DATABASE_URL points at the local dev Postgres (host %q)", host)
			}
			return "" // a real remote host — fine
		}
	}
	// Host wasn't recoverable as a URL authority (e.g. a key=value DSN). Probe
	// for the known in-cluster signatures rather than guessing, and pass
	// anything else so an unusual-but-legitimate remote DSN is never rejected.
	low := strings.ToLower(raw)
	for _, sig := range []string{"@postgres:", "@postgres/", "@localhost", "@127.0.0.1", "@[::1]"} {
		if strings.Contains(low, sig) {
			return "DATABASE_URL points at the local dev Postgres"
		}
	}
	return ""
}

// requireRemoteDatabase crashes the gateway at startup when it is wired to the
// local dev Postgres but the deployment declared it must be remote
// (RSYNC_REQUIRE_REMOTE_DB, set by docker-compose.prod.yml and inherited by the
// staging overlay). This is the fail-loud backstop for the "silent dev-postgres
// fallback": docker-compose substitutes the in-cluster dev DB when DATABASE_URL
// is unset (e.g. the staging stack launched without `--env-file .env.staging`),
// so the gateway comes up healthy but pointed at a database that holds none of
// the real pipelines — surfacing only as confusing 403s / empty lists. Far
// better to never start.
//
// It is intentionally NOT gated on ENVIRONMENT: the staging overlay sets
// ENVIRONMENT=local (to enable the dev auth path) while still requiring a remote
// DB, so an ENVIRONMENT check would miss exactly the case that bit us.
func requireRemoteDatabase() {
	if reason := remoteDatabaseViolation(envIsTrue("RSYNC_REQUIRE_REMOTE_DB"), os.Getenv("DATABASE_URL")); reason != "" {
		log.Fatalf("❌ Refusing to start: %s, but this deployment requires a remote database "+
			"(RSYNC_REQUIRE_REMOTE_DB is set). The stack was almost certainly launched without "+
			"`--env-file .env.staging` — set DATABASE_URL to the real (Azure) database and relaunch.", reason)
	}
}

func main() {
	// Initialize trace-aware logging FIRST
	telemetry.InitLogging("api-gateway")

	// Load feature flags early
	config.LoadFeatures()

	// Fail fast on insecure defaults in production.
	if isProductionLikeEnv() {
		requireProdSecret("JWT_SECRET", 32, "dev_secret_key_change_in_prod")
		requireProdEncryptionKeys()
	} else {
		// Loud, unmissable warning when dev mode is active. The
		// X-User-ID fallback in AuthRequiredMiddleware is enabled,
		// meaning ANY request with a user UUID header impersonates
		// that user with no password. This must NEVER reach a
		// network anyone outside the dev machine can route to.
		envVal := strings.TrimSpace(os.Getenv("ENVIRONMENT"))
		log.Println("==============================================================")
		log.Printf("⚠️  DEV MODE ACTIVE (ENVIRONMENT=%q)", envVal)
		log.Println("⚠️  X-User-ID header fallback is ENABLED — auth is BYPASSABLE.")
		log.Println("⚠️  This is safe ONLY on a developer's local machine.")
		log.Println("⚠️  To run in production, set ENVIRONMENT=production.")
		log.Println("==============================================================")
	}

	// Independent of ENVIRONMENT, refuse to start when a remote-DB deployment
	// (staging/prod) is wired to the local dev Postgres — the silent
	// docker-compose fallback. See requireRemoteDatabase for the rationale.
	requireRemoteDatabase()

	// Independent of ENVIRONMENT, refuse to start with a missing/dev ENCRYPTION_KEY
	// on a real remote-DB deployment. The isProductionLikeEnv gate above misses
	// staging (which sets ENVIRONMENT=local for the dev auth path), so an unset
	// ENCRYPTION_KEY there silently fell back to the docker-compose dev key —
	// encrypting connection configs with a key the orchestrator/temporal-adapter
	// might not share, i.e. "invalid ciphertext" and SOURCE_UNREACHABLE. Gate on the
	// same deploy-intent signal as requireRemoteDatabase (RSYNC_REQUIRE_REMOTE_DB),
	// which staging inherits from docker-compose.prod.yml.
	if envIsTrue("RSYNC_REQUIRE_REMOTE_DB") {
		requireProdEncryptionKeys()
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// Initialize OpenTelemetry tracer
	cfg := telemetry.LoadTelemetryConfig("api-gateway")
	shutdownTracer, err := telemetry.InitTracerWithConfig(cfg)
	if err != nil {
		log.Warnf("⚠️  Failed to initialize telemetry: %v", err)
	} else {
		defer func() {
			if err := shutdownTracer(context.Background()); err != nil {
				log.Errorf("Error shutting down tracer: %v", err)
			}
		}()
	}

	// Initialize Database
	if err := db.Init(); err != nil {
		log.Warnf("⚠️  Database connection failed: %v (using mock data)", err)
	} else {
		// Run Migrations
		log.Info("🔄 Running database migrations...")
		if err := db.Migrate("migrations"); err != nil {
			log.Errorf("❌ Database migration failed: %v", err)
		}
		// Remove dev-only seed users in any non-dev (production-like) environment
		// to prevent known-credential access. Fail-closed: only the explicit dev
		// values (development/dev/test/local) keep the seed users; empty/typo/
		// "docker"/"prod" all treat as production-like and purge them.
		if isProductionLikeEnv() {
			if _, err := db.GetDB().Exec(
				`DELETE FROM users WHERE email IN ('default@rsync-ai.local', 'test@rsync-ai.local')`,
			); err != nil {
				log.Warnf("⚠️  Failed to remove dev seed users: %v", err)
			} else {
				log.Info("✅ Dev seed users removed (production mode)")
			}
		}
	}
	defer db.Close()

	// Initialize Kafka producer with Avro support
	brokerList := resolveKafkaBrokers(os.Getenv("KAFKA_BROKERS"))

	// Use UnifiedProducer which supports both Avro and JSON
	// Set KAFKA_USE_AVRO=true to enable Avro serialization (default: true)
	kafkaProducer := kafka.NewUnifiedProducer(brokerList)
	defer kafkaProducer.Close()

	handlers.SetKafkaProducer(kafkaProducer)
	if kafkaProducer.IsAvroEnabled() {
		log.Infof("✓ Kafka producer initialized with AVRO support, brokers: %v", brokerList)
	} else {
		log.Infof("✓ Kafka producer initialized with JSON format, brokers: %v", brokerList)
	}

	// Initialize Temporal client for workflow orchestration
	temporalAddress := os.Getenv("TEMPORAL_ADDRESS")
	if temporalAddress == "" {
		temporalAddress = "temporal:7233"
	}

	// Register connection params first so handlers can lazily (re)dial Temporal
	// on demand if the bounded startup retry below never succeeds (e.g.
	// api-gateway won the boot race against Temporal). Without lazy reconnect a
	// missing client silently hangs every pipeline run.
	handlers.SetTemporalConfig(temporalAddress, "default")

	// Bounded startup retry: keep dialing until Temporal accepts connections,
	// up to ~60s. This avoids the cold-boot race where api-gateway starts
	// before temporal:7233 is listening and would otherwise stay permanently
	// "workflows disabled" until a manual restart.
	var temporalClient client.Client
	temporalDeadline := time.Now().Add(60 * time.Second)
	for attempt := 1; ; attempt++ {
		temporalClient, err = client.Dial(client.Options{
			HostPort:  temporalAddress,
			Namespace: "default",
		})
		if err == nil {
			break
		}
		if time.Now().After(temporalDeadline) {
			log.Warnf("⚠️  Failed to create Temporal client after retries: %v (will lazily reconnect on first run)", err)
			break
		}
		log.Warnf("⏳ Temporal not reachable yet (attempt %d): %v — retrying in 3s", attempt, err)
		time.Sleep(3 * time.Second)
	}
	if temporalClient != nil {
		defer temporalClient.Close()
		handlers.SetTemporalClient(temporalClient)
		log.Infof("✅ Temporal client initialized: %s", temporalAddress)
		log.Info("   Pattern: API Gateway → Temporal → Kafka → Agents")
	}

	// Initialize Status Manager
	statusManager := handlers.NewStatusManager()
	log.Info("Status manager initialized")

	// Initialize Schema Cache (Redis)
	// Prefer a single address var; fall back to host/port; finally a dev-compose default.
	redisAddr := strings.TrimSpace(os.Getenv("REDIS_ADDRESS"))
	if redisAddr == "" {
		redisAddr = strings.TrimSpace(os.Getenv("REDIS_ADDR"))
	}
	if redisAddr == "" {
		redisHost := strings.TrimSpace(os.Getenv("REDIS_HOST"))
		if redisHost != "" {
			redisPort := strings.TrimSpace(os.Getenv("REDIS_PORT"))
			if redisPort == "" {
				redisPort = "6379"
			}
			redisAddr = redisHost + ":" + redisPort
		}
	}
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}
	redisPassword := strings.TrimSpace(os.Getenv("REDIS_PASSWORD"))

	// Retry Redis init up to 5 times with a 2s backoff to survive a brief startup race
	// where api-gateway becomes ready before Redis accepts connections.
	var schemaCache *cache.SchemaCache
	for attempt := 1; attempt <= 5; attempt++ {
		var cacheErr error
		schemaCache, cacheErr = cache.NewSchemaCache(redisAddr, redisPassword, 5*time.Minute)
		if cacheErr == nil {
			break
		}
		if attempt < 5 {
			log.Warnf("⚠️  Redis not ready (attempt %d/5): %v — retrying in 2s", attempt, cacheErr)
			time.Sleep(2 * time.Second)
		} else {
			log.Warnf("⚠️  Failed to initialize schema cache after 5 attempts: %v (proceeding without caching)", cacheErr)
		}
	}
	if schemaCache != nil {
		handlers.SetSchemaCache(schemaCache)
		log.Infof("✅ Schema cache initialized (Redis: %s, TTL: 5min)", redisAddr)

		// Initialize Explorer cache using the same Redis client
		explorerCache := cache.NewExplorerCache(schemaCache.GetClient(), 10*time.Minute)
		handlers.SetExplorerCache(explorerCache)
		log.Infof("✅ Explorer cache initialized (TTL: 10min)")
	}

	// Initialize Conversation Cache for multi-turn chat (uses same Redis client)
	// CRITICAL: Without this, every chat message creates a fresh StateIdle conversation
	// and multi-turn confirmation (sync mode selection) never works.
	var conversationCache *chat.ConversationCache
	if schemaCache != nil {
		conversationCache = chat.NewConversationCache(schemaCache.GetClient())
		log.Info("✅ Conversation cache initialized (TTL: 30min)")
	} else {
		log.Error("❌ Conversation cache NOT initialized — multi-turn chat will not work (Redis unavailable)")
	}

	// Initialize PII handler (needs producer for async scan publishing)
	piiHandler := handlers.NewPIIHandler(db.GetDB(), kafkaProducer)

	// Initialize Kafka Consumer for agent responses (multi-topic via per-reader fan-out).
	// The group id is spelled logically here; NewConsumer applies the
	// KAFKA_TOPIC_PREFIX namespace, so every caller is qualified by
	// construction rather than by remembering to wrap this literal.
	consumer := kafka.NewConsumer(
		brokerList,
		kafkaclient.Topics("agent.planner.responses", "pii.scan.response"),
		"api-gateway-consumer-group",
	)

	// Start consumer in background
	appCtx, appCancel := context.WithCancel(context.Background())
	defer appCancel()

	go consumer.Start(appCtx, func(ctx context.Context, response kafka.AgentResponse) error {
		// Route pii scan responses to the PII handler; everything else to status manager
		if response.Agent == "pii_scanner" {
			scanID, _ := response.Result["scan_id"].(string)
			piiHandler.HandlePIIScanResponse(ctx, scanID, response.Status, response.Result, response.Error)
			return nil
		}
		return statusManager.HandleAgentResponse(ctx, response)
	})

	log.Info("Kafka consumer started, listening to agent.planner.responses, pii.scan.response")

	hub := websocket.NewHub()
	go hub.Run()
	defer hub.Stop()

	// Initialize Kafka-WebSocket bridge for real-time updates
	kafkaBridge := websocket.NewKafkaBridge(hub, brokerList)
	go kafkaBridge.Start()
	defer kafkaBridge.Stop()
	log.Info("Kafka-WebSocket bridge initialized")

	// Initialize Event Projector for pipeline state updates (Architecture Phase 1)
	// This projects pipeline.domain.events to pipeline_progress table (best-effort)
	// Temporal's StateUpdateActivity writes authoritative state transitions
	eventProjector := projector.NewEventProjector(db.GetDB(), brokerList)

	// Wired here rather than inside the projector because the two packages must not
	// import each other: the projector is a consumer of pipeline events and knows
	// nothing about saved queries, while handlers knows nothing about Kafka. main is
	// the composition root and the only place that legitimately knows both.
	//
	// Assigned before Start(), so no event can arrive between the two and find the
	// hook nil.
	eventProjector.OnPipelineCompleted = handlers.FireModelsAfterPipeline

	if err := eventProjector.Start(); err != nil {
		log.Warnf("⚠️  Failed to start event projector: %v", err)
	} else {
		defer eventProjector.Stop()
		log.Info("✅ Event Projector started (consuming pipeline.domain.events)")
	}

	// Optional: retention cleanup for monitoring tables (best-effort; default off).
	// Enable with ENABLE_PIPELINE_RUN_RETENTION=true.
	retention.StartPipelineRunRetention(appCtx, db.GetDB())

	// Initialize Domain Event Manager for NL Pipeline HITL checkpoints
	//
	// Error, not Warn: nothing retries this, so a failure here is permanent for
	// the life of the process, and its only symptom is HITL checkpoints that
	// never reach the browser — the #1 false "pipelines don't work" report. The
	// process still comes up, because the rest of the API is unaffected and an
	// unreachable-at-boot Kafka self-heals while a crash loop would take the
	// whole gateway down with it; a security config that can never work has
	// already aborted startup inside the shared Kafka security path.
	if err := handlers.InitDomainEventManager(appCtx, brokerList); err != nil {
		log.Errorf("❌ Domain event manager NOT started: %v — NL pipeline HITL checkpoints will not reach the UI", err)
	} else {
		log.Info("✅ Domain event manager initialized")
	}

	// G1 / F-Obs-1: notifier consumer — subscribes to
	// rsync.notifications + rsync.healer.actions + rsync.healer.results
	// and persists each event into pipeline_notifications + delivers
	// via Slack/email per env. Pre-fix those topics had no consumer.
	//
	// Error, not Warn, for the same reason as the domain event manager above —
	// and one worse: this consumer is the delivery path for every Slack/email
	// alert, so a warning about it is the one log line nothing will page on.
	notifierSvc, err := notifier.Start(appCtx, db.GetDB(), brokerList)
	if err != nil {
		log.Errorf("❌ Notifier consumer NOT started: %v — no Slack or email alert will be delivered", err)
	} else {
		log.Info("✅ Notifier consumer started")
	}

	// Initialize OAuth, Schema Registry, and Auth handlers
	oauthHandler := handlers.NewOAuthHandler(db.GetDB())
	schemaHandler := handlers.NewSchemaRegistryHandler()
	authHandler := handlers.NewAuthHandler()
	log.Info("OAuth, Schema Registry, and Auth handlers initialized")
	log.Info(handlers.EmailConfigStatus())

	// Wire proactive token refresh into enrichConfigWithOAuthToken and start the
	// background refresher that keeps tokens warm before pipelines need them.
	handlers.SetTokenRefreshFunc(oauthHandler.RefreshTokenByID)
	bgTokenRefresher := handlers.NewBackgroundTokenRefresher(db.GetDB(), oauthHandler.RefreshTokenByID)
	bgTokenRefresher.Start(appCtx)

	// Initialize schema evolution handler with DB + Kafka deps
	handlers.SetSchemaEvolutionDeps(db.GetDB(), kafkaProducer)

	// Initialize Chat handler for agentic pipeline flow
	orchestratorURL := strings.TrimSpace(os.Getenv("ORCHESTRATOR_URL"))
	if orchestratorURL == "" {
		orchestratorURL = strings.TrimSpace(os.Getenv("BACKEND_ORCHESTRATOR_URL"))
	}
	if orchestratorURL == "" {
		orchestratorURL = "http://orchestrator:8080"
	}
	chatHandler := handlers.NewChatHandler(kafkaProducer, orchestratorURL)
	if conversationCache != nil {
		chatHandler.SetConversationCache(conversationCache)
	}
	log.Info("Chat handler initialized")

	r := gin.Default()

	// SECURITY (SEC-M-07): gin.Default() trusts ALL proxies and derives
	// c.ClientIP() from the client-controlled X-Forwarded-For header, which
	// lets an attacker rotate the header to bypass the per-IP auth
	// brute-force limiter (AuthRateLimitMiddleware). Cloud sits behind
	// Cloudflare -> Traefik: the real visitor IP is in CF-Connecting-IP,
	// which Cloudflare overwrites at its edge and clients cannot forge
	// (provided the origin only accepts Cloudflare traffic). Trust that
	// header for ClientIP. When the header is absent (local/dev), gin falls
	// through to its default logic, so dev is unaffected.
	if trustCloudflareClientIP() {
		r.TrustedPlatform = gin.PlatformCloudflare

		// SEC-M-07 completeness: TrustedPlatform only governs the CF-Connecting-IP
		// path. When that header is ABSENT, gin falls back to X-Forwarded-For and —
		// with gin.Default()'s trust-ALL-proxies default — would trust a forged XFF
		// from a direct-to-origin request, re-opening the per-IP auth-limiter bypass.
		// Restrict trusted proxies to the Cloudflare edge ranges (overridable via
		// RSYNC_TRUSTED_PROXY_CIDRS) so the XFF fallback is not attacker-trusted.
		// Only applied when we already trust Cloudflare, so local/dev (flag false)
		// keeps gin.Default()'s behavior and is unaffected.
		if err := r.SetTrustedProxies(trustedProxyCIDRs()); err != nil {
			log.Warnf("⚠️  Failed to set trusted proxies (continuing with gin defaults): %v", err)
		}
	}

	// Add OpenTelemetry tracing middleware
	r.Use(telemetry.TracingMiddleware("api-gateway"))

	// Domain Prometheus metrics: request count/latency by route+status,
	// auth-failure (401/403) and pipeline-run-trigger counters. Installed
	// right after tracing so it observes the final status of every handler.
	// Exposed on /metrics (below) and scraped by the otel-collector's
	// prometheus receiver → SigNoz. F-Obs-2.
	r.Use(metrics.HTTPMetricsMiddleware())

	// Prometheus /metrics — Go runtime + process metrics out of the box.
	// Scraped by the otel-collector / external Prometheus to track 4xx/5xx
	// rates, latency percentiles, and DB pool exhaustion.
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// Public Routes
	healthHandler := func(c *gin.Context) {
		traceID := telemetry.GetTraceIDFromContext(c)
		c.JSON(200, gin.H{"status": "ok", "service": "api-gateway", "trace_id": traceID})
	}

	// /health is what the container healthcheck hits, directly on :8080 inside the
	// compose network. It never traverses Traefik and is unaffected by anything below.
	r.GET("/health", healthHandler)

	// /api/health is the same probe as reached from OUTSIDE. It exists because the
	// two paths are not interchangeable through the edge: Traefik routes only
	// PathPrefix(`/api`) to this service (docker-compose.prod.yml:397) and hands
	// everything else to the frontend (:706). So https://app.rsync.ai/health is
	// answered by Next.js, which has no such page and returns its 404 document —
	// which is exactly what the admin Test Suite's "Backend /health" check was
	// reporting as a backend failure.
	//
	// Deliberately only /health is aliased. /version and /ready stay internal-only:
	// /version returns the build SHA, and there is no reason to publish that at the
	// edge just to make a diagnostic convenient.
	r.GET("/api/health", healthHandler)

	// /version is the canonical answer to "what's actually running here"
	// — used by the deployment-drift check to spot stale containers.
	r.GET("/version", handlers.GetVersion("api-gateway"))

	// Readiness: fail fast if core dependencies are unavailable.
	//
	// This is the probe the Helm chart points its readinessProbe at
	// (deploy/helm/rsync-ai/templates/apps/api-gateway.yaml). /health stays
	// static and stays on liveness: a database blip should drain traffic from
	// a replica, not restart every replica.
	r.GET("/ready", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()

		database := db.GetDB()
		pingOK := false
		if database != nil {
			pingOK = database.PingContext(ctx) == nil
		}

		code, reason := readinessVerdict(database != nil, pingOK, db.SchemaReady())
		if code == http.StatusOK {
			c.JSON(code, gin.H{"status": "ready"})
			return
		}
		c.JSON(code, gin.H{"status": "not_ready", "reason": reason})
	})

	r.GET("/ws", func(c *gin.Context) {
		// WebSocket auth: token via ?token= query param or auth_token cookie.
		// Browsers cannot send custom headers during WS upgrade, so we fall back to cookie.
		token := strings.TrimSpace(c.Query("token"))
		if token == "" {
			if cv, err := c.Cookie("auth_token"); err == nil {
				token = strings.TrimSpace(cv)
			}
		}
		if strings.HasPrefix(strings.ToLower(token), "bearer ") {
			token = strings.TrimSpace(token[7:])
		}

		database := db.GetDB()
		if database != nil && token != "" {
			var userID string
			err := database.QueryRow(`
				SELECT u.id FROM users u
				JOIN sessions s ON s.user_id = u.id
				WHERE s.token = $1 AND s.expires_at > NOW()
			`, crypto.HashSessionToken(token)).Scan(&userID)
			if err != nil {
				c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid_token"})
				return
			}
			c.Set("user_id", userID)
		} else if database != nil && isProductionLikeEnv() {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "token_required"})
			return
		}
		websocket.ServeWs(hub, c)
	})

	// API Routes (CORS)
	// - Dev default: allow http://localhost:3000
	// - Prod: allowlist via RSYNC_CORS_ORIGINS (comma-separated)
	//
	// NOTE: Allowing a header in CORS does NOT imply the backend trusts it.
	// We still reject dev identity headers in production at the auth layer.
	r.Use(func(c *gin.Context) {
		origin := strings.TrimSpace(c.GetHeader("Origin"))
		env := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))

		allowed := map[string]struct{}{}
		raw := strings.TrimSpace(os.Getenv("RSYNC_CORS_ORIGINS"))
		if raw != "" {
			for _, part := range strings.Split(raw, ",") {
				o := strings.TrimSpace(part)
				if o != "" {
					allowed[o] = struct{}{}
				}
			}
		} else if env != "production" && env != "prod" {
			allowed["http://localhost:3000"] = struct{}{}
		}

		// In dev, also accept any http://localhost:<port> or 127.0.0.1:<port>
		// so preview servers / CLI tools / autoPort dev servers work without
		// needing explicit env config every time the port changes.
		isDevLoopback := false
		if env != "production" && env != "prod" && origin != "" {
			if strings.HasPrefix(origin, "http://localhost:") || strings.HasPrefix(origin, "http://127.0.0.1:") {
				isDevLoopback = true
			}
		}

		if origin != "" {
			if _, ok := allowed[origin]; ok || isDevLoopback {
				c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
				c.Writer.Header().Set("Vary", "Origin")
			}
		}

		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
		allowHeaders := "Content-Type, Authorization, X-Trace-ID, Origin, Accept, traceparent, tracestate"
		if env != "production" && env != "prod" {
			// Dev-only compatibility (frontend tests still send this)
			allowHeaders += ", X-User-ID"
		}
		// Future-proof for cookie auth + CSRF
		allowHeaders += ", X-CSRF-Token"
		// Active-workspace selector (membership-verified server-side).
		allowHeaders += ", X-Workspace-ID"
		c.Writer.Header().Set("Access-Control-Allow-Headers", allowHeaders)
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Expose-Headers", "X-Trace-ID, traceparent, tracestate")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}
		c.Next()
	})

	// Public Auth Routes (no auth middleware required)
	auth := r.Group("/api/v1/auth")
	{
		auth.POST("/login", handlers.AuthRateLimitMiddleware(), authHandler.Login)
		auth.POST("/register", handlers.AuthRateLimitMiddleware(), authHandler.Register)
		auth.POST("/logout", authHandler.Logout)
		auth.GET("/me", authHandler.Me)
		auth.PATCH("/me", authHandler.UpdateMe)
		auth.PATCH("/password", authHandler.ChangePassword)
		auth.GET("/invite/:token", authHandler.ValidateInvite)
		// Email verification: token is one-time, no auth session needed
		auth.GET("/verify-email", authHandler.VerifyEmail)
		// Resend: requires a valid session (user must be logged in)
		auth.POST("/resend-verification", handlers.AuthRateLimitMiddleware(), authHandler.ResendVerification)
	}

	// Public workspace-invite preview (no auth): an invitee inspects what they're
	// joining before signing in. Rate-limited to blunt token enumeration.
	r.GET("/api/v1/workspace-invites/:token", handlers.AuthRateLimitMiddleware(), handlers.GetWorkspaceInvite)

	// Slack drift-approval interactivity callback. PUBLIC — Slack carries no rsync
	// session, so this route is intentionally OUTSIDE the authed api group:
	// authenticity is the Slack request signature (verified inside the handler),
	// identity is mapped Slack→verified-email→rsync user, and authorization is the
	// user's role in the pipeline's workspace. Disabled + inert unless
	// SLACK_SIGNING_SECRET is set. Rate-limited to blunt signature-spam.
	slackInteractions := handlers.NewSlackInteractionsHandler(
		db.GetDB(), kafkaProducer,
		strings.TrimSpace(os.Getenv("SLACK_SIGNING_SECRET")),
		strings.TrimSpace(os.Getenv("SLACK_BOT_TOKEN")),
	)
	r.POST("/api/v1/slack/interactions", handlers.AuthRateLimitMiddleware(), slackInteractions.HandleInteractions)

	api := r.Group("/api/v1")
	{
		// Feature flags endpoint (public, but under CORS middleware)
		api.GET("/features", handlers.GetFeatureFlags)

		// All remaining /api/v1 routes require authentication in production.
		// In development, AuthRequiredMiddleware provides a backward-compatible fallback.
		api.Use(handlers.AuthRequiredMiddleware())
		// Block unverified users from feature endpoints (no-op when RESEND_API_KEY is unset).
		api.Use(handlers.EmailVerifiedMiddleware())
		// CSRF protection for all state-changing routes (production by default).
		api.Use(handlers.CSRFMiddleware())
		// Per-user rate limiting for all authenticated API routes.
		api.Use(handlers.APIRateLimitMiddleware())
		// Resolve + pin the caller's active workspace, re-verifying membership
		// every request. Runs after auth so user_id is already in context.
		api.Use(handlers.WorkspaceContextMiddleware())

		// ========================================================================
		// PLAN - Pro upgrade request (auto-emails the team; no manual mail step)
		// ========================================================================
		api.POST("/plan/upgrade-request", authHandler.UpgradeRequest)

		// ========================================================================
		// USAGE - Active-workspace consumption vs plan limits (any member, read-only)
		// ========================================================================
		api.GET("/usage", handlers.GetWorkspaceUsage)

		// ========================================================================
		// CHAT - Main Entry Point (NL-Driven Pipeline Flow)
		// ========================================================================
		api.POST("/chat/message", handlers.ChatRateLimitMiddleware(), chatHandler.SendMessage)

		// ========================================================================
		// PIPELINES - Temporal Workflow Management
		// ========================================================================
		api.GET("/pipelines", handlers.ListPipelines)
		api.GET("/pipelines/stats", handlers.GetPipelineStats)
		api.GET("/pipelines/:id", handlers.GetPipeline)
		api.PATCH("/pipelines/:id", handlers.UpdatePipeline)
		api.POST("/pipelines", handlers.CreatePipeline)
		api.POST("/pipelines/:id/run", handlers.PipelineRunRateLimitMiddleware(), handlers.RunPipeline)
		api.POST("/pipelines/:id/assess", handlers.AssessPipeline)
		api.POST("/pipelines/:id/stop", handlers.StopPipeline)
		api.POST("/pipelines/:id/pause", handlers.PausePipeline)
		api.POST("/pipelines/:id/resume", handlers.ResumePipeline)
		api.GET("/pipelines/:id/events", handlers.GetPipelineEvents)
		api.POST("/pipelines/:id/events/raw", handlers.GetPipelineEventsRaw) // Requires power_user/admin + justification
		api.POST("/pipelines/:id/cdc/tables", handlers.UpdatePipelineCDCTables)
		api.POST("/pipelines/:id/cdc/backfill", handlers.BackfillPipelineCDCTables)
		api.POST("/pipelines/:id/tables", handlers.UpdatePipelineTables)
		api.GET("/pipelines/:id/table-stats", handlers.GetPipelineTableStats) // DMS-like per-table statistics
		api.GET("/pipelines/:id/compare", handlers.ComparePipelineRuns)
		api.GET("/pipelines/:id/trends", handlers.GetPipelineTrends)
		// Monitoring endpoints (feature-flagged)
		api.GET("/pipelines/:id/monitoring/overview", handlers.GetPipelineMonitoringOverview)
		// Checkpoints (advisory for UI)
		api.GET("/pipelines/:id/checkpoints", handlers.GetPipelineCheckpoints)
		// CDC controls (Debezium via Kafka Connect)
		api.GET("/pipelines/:id/cdc/status", handlers.GetPipelineCDCStatus)
		api.POST("/pipelines/:id/cdc/restart", handlers.RestartPipelineCDC)
		api.POST("/pipelines/:id/cdc/recover", handlers.RecoverPipelineCDC)
		api.POST("/pipelines/:id/cdc/pause", handlers.PausePipelineCDC)
		api.POST("/pipelines/:id/cdc/resume", handlers.ResumePipelineCDC)
		// HITL resume endpoints (signal Temporal workflow)
		api.POST("/pipelines/:id/hitl/connections", handlers.ResumePipelineConnections)
		api.POST("/pipelines/:id/hitl/connectors", handlers.ResumePipelineConnectors)
		api.POST("/pipelines/:id/hitl/tables", handlers.ResumePipelineTables)
		api.POST("/pipelines/:id/hitl/node-input", handlers.ResumePipelineNodeInput) // DAG node input
		api.DELETE("/pipelines/:id", handlers.DeletePipeline)

		// Schema evolution — pending DDL approvals from healer agent
		api.GET("/pipelines/:id/schema-changes", handlers.ListPipelineSchemaChanges)
		api.POST("/pipelines/:id/schema-changes/:changeId/approve", handlers.ApproveSchemaChange)
		api.POST("/pipelines/:id/schema-changes/:changeId/reject", handlers.RejectSchemaChange)
		// Per-pipeline schema-drift detector policy (pipelines.config JSONB)
		api.GET("/pipelines/:id/schema-drift-policy", handlers.GetPipelineSchemaDriftPolicy)
		api.PUT("/pipelines/:id/schema-drift-policy", handlers.UpdatePipelineSchemaDriftPolicy)

		// ========================================================================
		// NOTIFICATIONS - Per-user inbox (header bell). Rows written by the
		// notifier consumer (internal/notifier) from healer/executor events;
		// user-scoped, read-state tracked via read_at.
		// ========================================================================
		api.GET("/notifications", handlers.ListNotifications)
		api.GET("/notifications/unread-count", handlers.GetUnreadNotificationCount)
		api.POST("/notifications/mark-read", handlers.MarkNotificationRead)
		api.POST("/notifications/mark-all-read", handlers.MarkAllNotificationsRead)

		// Pipeline Schedules (Temporal Schedules)
		api.POST("/pipelines/:id/schedules", handlers.CreatePipelineSchedule)
		api.GET("/pipelines/:id/schedules", handlers.ListPipelineSchedules)
		api.PATCH("/pipelines/:id/schedules/:schedule_id", handlers.UpdatePipelineSchedule)
		api.PATCH("/pipelines/:id/schedules/:schedule_id/pause", handlers.PausePipelineSchedule)
		api.PATCH("/pipelines/:id/schedules/:schedule_id/resume", handlers.ResumePipelineSchedule)
		api.POST("/pipelines/:id/schedules/:schedule_id/trigger", handlers.TriggerPipelineSchedule)
		api.DELETE("/pipelines/:id/schedules/:schedule_id", handlers.DeletePipelineSchedule)

		// Real-time events and state management (HITL support)
		api.GET("/pipelines/:id/events/stream", handlers.SubscribePipelineEvents) // WebSocket
		api.GET("/pipelines/:id/state", handlers.GetPipelineState)

		// Canonical runtime view — single source of truth for "what is this pipeline
		// doing right now". Replaces UI-side state derivation (see pipeline_runtime.go).
		api.GET("/pipelines/:id/runtime", handlers.GetPipelineRuntime)
		// Nested REST execution routes for a pipeline (the flat /executions and
		// /executions/:id routes also work). Previously these 404'd, so callers
		// could only reach execution state via the flat routes.
		api.GET("/pipelines/:id/executions", handlers.ListExecutions)
		api.GET("/pipelines/:id/executions/:execId", handlers.GetExecution)

		// Diagnose: gather evidence about a misbehaving pipeline + ask llm-service
		// for a root-cause hypothesis. Read-only; never mutates state.
		api.POST("/pipelines/:id/diagnose", handlers.DiagnosePipeline)

		// ========================================================================
		// DRAFTS - Pipeline Draft Persistence (Right Panel)
		// ========================================================================
		api.POST("/drafts", handlers.CreateDraft)
		api.GET("/drafts/:id", handlers.GetDraft)
		api.PUT("/drafts/:id", handlers.UpdateDraft)
		api.DELETE("/drafts/:id", handlers.DeleteDraft)

		// ========================================================================
		// EXECUTIONS - Pipeline Run History
		// ========================================================================
		api.GET("/executions", handlers.ListExecutions)
		api.GET("/executions/:id", handlers.GetExecution)
		api.GET("/executions/:id/transforms", handlers.GetExecutionTransformLogs)
		api.POST("/executions/:id/cancel", handlers.CancelExecution)
		// Phase 3 + chat composition: run the Diagnoser against any
		// execution the calling user owns. Same code path the chat
		// assistant uses for "why did this fail?".
		api.GET("/executions/:id/diagnose", handlers.DiagnoseExecutionByID)

		// ========================================================================
		// CONNECTORS - MCP Connector Management
		// ========================================================================
		// Read-only catalog endpoints — any authed user can browse.
		api.GET("/connectors", handlers.ListMCPConnectors)
		api.GET("/connectors/:name", handlers.GetMCPConnector)
		api.GET("/connectors/:name/logo", handlers.GetMCPConnectorLogo)
		api.POST("/connectors/detect-category", handlers.DetectCategory)

		// Code-generation + validation endpoints write to shared
		// connector storage and drive LLM codegen. Gate behind
		// WorkspaceGeneratorMiddleware (workspace role >= admin) so a
		// member/viewer cannot persist arbitrary on-disk code, trigger
		// prompt-injection-driven endpoint planting, or burn LLM budget.
		// Generation is a TENANT capability (every self-serve signup owns
		// their personal workspace), so it is gated on the workspace axis
		// rather than the global user|power_user|admin staff tier — which
		// otherwise left paying owners (global role "user") locked out. The
		// group-level AuthRequired + WorkspaceContext .Use middleware run
		// first, so the caller's workspace role is already pinned in context.
		api.POST("/connectors/validate", handlers.WorkspaceGeneratorMiddleware(), handlers.ValidateConnector)
		api.POST("/connectors/generate", handlers.WorkspaceGeneratorMiddleware(), handlers.ConnectorGenRateLimitMiddleware(), handlers.GenerateConnector)

		// Tool-generator discovery proxy. The frontend used to hit port 5010
		// directly which broke under browser CORS preflight whenever the
		// preview origin wasn't in TG_ALLOWED_ORIGINS. Proxy through the
		// gateway so CORS is solved exactly once at the edge and
		// tool-generator stays internal.
		//
		// Gated by workspace role >= admin — same reasoning as
		// /connectors/generate (this proxy exposes /v1/discover and
		// /v1/generate on the llm-service).
		api.Any("/tool-generator/*proxyPath", handlers.WorkspaceGeneratorMiddleware(), handlers.ToolGeneratorProxy)

		// ========================================================================
		// WORKSPACES - Multi-tenant isolation
		// ========================================================================
		api.GET("/workspaces", handlers.ListWorkspaces)
		api.POST("/workspaces", handlers.CreateWorkspace)
		api.GET("/workspaces/:id", handlers.GetWorkspace)
		api.PATCH("/workspaces/:id", handlers.UpdateWorkspace)
		api.DELETE("/workspaces/:id", handlers.DeleteWorkspace)
		api.GET("/workspaces/:id/members", handlers.ListWorkspaceMembers)
		api.POST("/workspaces/:id/members/:userId/role", handlers.ChangeMemberRole)
		api.DELETE("/workspaces/:id/members/:userId", handlers.RemoveMember)
		api.POST("/workspaces/:id/invites", handlers.CreateWorkspaceInvite)
		api.GET("/workspaces/:id/invites", handlers.ListWorkspaceInvites)
		api.DELETE("/workspaces/:id/invites/:inviteId", handlers.RevokeWorkspaceInvite)
		// Authed invite acceptance (email-bound, single-use). Public preview of
		// the same token is registered above, outside this group.
		api.POST("/workspace-invites/:token/accept", handlers.AcceptWorkspaceInvite)

		// ========================================================================
		// CONNECTIONS - Data Source/Destination Management
		// ========================================================================
		api.GET("/connections", handlers.ListConnections)
		api.GET("/connections/:id", handlers.GetConnection)
		api.POST("/connections", handlers.CreateConnection)
		api.PUT("/connections/:id", handlers.UpdateConnection)
		api.PATCH("/connections/:id", handlers.UpdateConnection) // PATCH for version upgrades
		api.DELETE("/connections/:id", handlers.DeleteConnection)
		api.POST("/connections/test", handlers.TestConnection)            // Test before creating
		api.POST("/connections/:id/test", handlers.TestConnection)        // Test existing connection
		api.GET("/connections/:id/sample", handlers.SampleConnectionData) // Sample data for preview
		// Schema discovery (tables + columns + counts) via orchestrator agent
		api.GET("/connections/:id/metadata", handlers.GetConnectionMetadata)

		// Zero-credential try-it path. Both report unavailable unless
		// RSYNC_DEMO_DESTINATION_DSN is set, which only the quickstart compose
		// does — cloud is unaffected. Seeding replays CreateConnection, so it
		// inherits that handler's RBAC and workspace scoping rather than
		// introducing a second write path for connections.
		api.GET("/demo/status", handlers.GetDemoStatus)
		api.POST("/demo/seed", handlers.SeedDemoConnections)

		// ========================================================================
		// TRANSFORMS - Data Transformation & Preview
		// ========================================================================
		transformHandler := handlers.NewTransformHandler(db.GetDB(), "")
		transformHandler.RegisterRoutes(api)

		// ========================================================================
		// PII - Scan, Approvals, Policies, Hash Functions
		// ========================================================================
		piiHandler.RegisterRoutes(api)

		// ========================================================================
		// SUGGESTIONS - AI suggestions (PII + transforms + optimizations)
		// ========================================================================
		api.POST("/suggestions/generate", handlers.GenerateSuggestions)

		// ========================================================================
		// SCHEMA REGISTRY - Kafka Avro Schema Management
		// ========================================================================
		api.GET("/schemas", schemaHandler.ListSubjects)
		api.GET("/schemas/info", schemaHandler.GetRegistryInfo)
		api.GET("/schemas/config", schemaHandler.GetConfig)
		api.PUT("/schemas/config", handlers.AdminRoleMiddleware(), schemaHandler.SetConfig)
		api.GET("/schemas/:subject", schemaHandler.GetSubjectVersions)
		api.GET("/schemas/:subject/versions/:version", schemaHandler.GetSchema)
		api.POST("/schemas/:subject", handlers.PowerUserOrAdminMiddleware(), schemaHandler.RegisterSchema)
		api.POST("/schemas/:subject/compatibility", schemaHandler.CheckCompatibility)
		api.DELETE("/schemas/:subject", handlers.AdminRoleMiddleware(), schemaHandler.DeleteSubject)
		api.GET("/schemas/:subject/config", schemaHandler.GetConfig)
		api.PUT("/schemas/:subject/config", handlers.PowerUserOrAdminMiddleware(), schemaHandler.SetConfig)

		// ========================================================================
		// OAUTH - Authentication & Authorization
		// ========================================================================
		api.GET("/oauth/providers", oauthHandler.ListProviders)
		// BYO OAuth apps: per-user client_id/secret for providers without an
		// operator-set env app (e.g. spec-generated connectors). Registered
		// before /oauth/:provider/authorize; "apps" is a static sibling of the
		// :provider param, the same pattern as /oauth/providers and /oauth/tokens.
		api.POST("/oauth/apps", oauthHandler.SaveOAuthApp)
		api.GET("/oauth/apps/:provider", oauthHandler.GetOAuthApp)
		api.DELETE("/oauth/apps/:provider", oauthHandler.DeleteOAuthApp)
		api.GET("/oauth/:provider/authorize", oauthHandler.Authorize)
		api.GET("/oauth/tokens", oauthHandler.ListTokens)
		api.POST("/oauth/tokens/:token_id/refresh", oauthHandler.RefreshToken)
		api.DELETE("/oauth/tokens/:token_id", oauthHandler.RevokeToken)

		// ========================================================================
		// MONITORING - Sentinel Health & Issues (feature-flagged)
		// ========================================================================
		api.GET("/monitoring/sentinel/health", handlers.GetSentinelHealth)
		api.GET("/monitoring/sentinel/issues", handlers.GetSentinelIssues)

		// ========================================================================
		// ADMIN - Operations panel (role-gated: admin only)
		// ========================================================================
		admin := api.Group("/admin")
		admin.Use(handlers.AdminRoleMiddleware())
		{
			// Existing endpoints
			admin.GET("/overview", handlers.AdminOverview)
			admin.GET("/usage", handlers.AdminUsage) // per-workspace + per-user consumption (cross-tenant)
			admin.GET("/pipelines", handlers.AdminListPipelines)
			admin.GET("/executions", handlers.AdminListExecutions)
			admin.POST("/pipelines/:id/events/raw", handlers.AdminRawEventsRateLimitMiddleware(), handlers.AdminGetPipelineEventsRaw)

			// User management
			admin.GET("/users", handlers.AdminListUsers)
			admin.GET("/users/:id", handlers.AdminGetUser)
			admin.PATCH("/users/:id/role", handlers.AdminUpdateUserRole)
			admin.PATCH("/users/:id/status", handlers.AdminUpdateUserStatus)
			admin.DELETE("/users/:id", handlers.AdminDeleteUser)

			// Invitations
			admin.POST("/invitations", handlers.AdminCreateInvitation)
			admin.GET("/invitations", handlers.AdminListInvitations)
			admin.DELETE("/invitations/:id", handlers.AdminRevokeInvitation)

			// Audit logs
			admin.GET("/audit-logs", handlers.AdminListAuditLogs)

			// Instance settings
			admin.GET("/settings", handlers.AdminGetSettings)
			admin.PATCH("/settings", handlers.AdminUpdateSettings)

			// System health
			admin.GET("/health", handlers.AdminSystemHealth)

			// Deployment drift check — confirms every service is running the
			// same commit. Catches "fix in repo, not in container" outages.
			admin.GET("/drift", handlers.AdminDriftCheck)

			// Encryption key rotation (re-encrypt stored secrets with primary key)
			admin.POST("/encryption/rotate", handlers.AdminRotateEncryptionKeys)

			// Billing: manual plan set (interim until Stripe/P2). Platform-admin
			// only; sets a workspace's billing tier. See plan_quota.go.
			admin.POST("/workspaces/:id/plan", handlers.AdminSetWorkspacePlan)
		}

		// ========================================================================
		// EXPLORER - Natural Language SQL & Query Execution
		// ========================================================================
		api.POST("/sql/generate", handlers.GenerateSQL)                                                  // NL → SQL
		api.POST("/explorer/query", handlers.ExecuteExplorerQuery)                                       // Execute SQL: SELECT reads (all roles) + role-gated writes (admin=DML/DDL, owner=DROP/TRUNCATE)
		api.POST("/explorer/connections/:id/tables/recommend", handlers.GetRecommendedTablesForExplorer) // Table recommendations
		api.POST("/explorer/metabase/dashboard", handlers.CreateMetabaseDashboard)                       // Create Metabase dashboard from SQL

		// Schema Index (with caching + fingerprint)
		api.GET("/explorer/connections/:id/schema-index", handlers.GetSchemaIndex)              // Get cached schema index
		api.POST("/explorer/connections/:id/schema-index/refresh", handlers.RefreshSchemaIndex) // Force refresh schema index

		// Table Retrieval (for large schema handling)
		api.POST("/explorer/tables/retrieve", handlers.RetrieveTables) // Retrieve top-K tables matching question

		// LLM-Orchestrated NL Resolution
		api.POST("/explorer/nl/resolve-tables", handlers.ResolveExplorerTables)   // NL → table candidates (with HITL gating)
		api.POST("/explorer/nl/resolve-columns", handlers.ResolveExplorerColumns) // NL → column mapping (with HITL gating)
		api.POST("/explorer/nl/next-steps", handlers.GetExplorerNextSteps)        // Query results → next action suggestions

		// Action Execution (CSV, Slack, Email)
		api.GET("/explorer/export.csv", handlers.ExportCSVHandler) // Legacy: direct GET CSV export (kept for any deep links)
		api.POST("/explorer/export", handlers.ExportQueryHandler)  // D4: multi-format export (csv/tsv/json) for the Download dropdown
		api.POST("/explorer/share/slack", handlers.ShareToSlack)   // Share to Slack via webhook
		api.POST("/explorer/share/email", handlers.ShareViaEmail)  // Send via SMTP email

		// Saved Queries (migration 084) — replaces the per-browser localStorage
		// history with a workspace-scoped, shareable, versioned resource.
		// Saving is member-level and gates on NOTHING about the statement class:
		// storing SQL mutates no data. Running it stays gated at /explorer/query,
		// which re-classifies the CURRENT sql_text on every execution.
		api.GET("/explorer/saved", handlers.ListSavedQueries)                    // List (workspace-visible + own private)
		api.POST("/explorer/saved", handlers.CreateSavedQuery)                   // Create (member)
		api.GET("/explorer/saved/:id", handlers.GetSavedQuery)                   // Get one, with SQL
		api.PATCH("/explorer/saved/:id", handlers.UpdateSavedQuery)              // Edit (creator or workspace admin); snapshots prior version
		api.DELETE("/explorer/saved/:id", handlers.DeleteSavedQuery)             // Delete (creator or workspace admin)
		api.GET("/explorer/saved/:id/versions", handlers.ListSavedQueryVersions) // Edit history + any open proposal

		// How long that edit history survives (migration 097). Readable by any
		// member — people should be able to see how long their own history lasts —
		// and writable only by an admin, because it is the one Explorer setting
		// whose effect is deleting data. Default is keep forever, so these routes
		// change nothing until someone calls the PUT.
		api.GET("/explorer/version-retention", handlers.GetSavedQueryVersionRetention)
		api.PUT("/explorer/version-retention", handlers.SetSavedQueryVersionRetention)

		// Approval gate (migration 096). An edit to the SQL of a SCHEDULED query does
		// not take effect on PATCH above — it becomes a proposal, and saved_queries
		// .sql_text keeps its approved value until one of these two routes runs. Both
		// are admin-only: proposing is a member act, deciding what a scheduled table
		// means is not.
		api.POST("/explorer/saved/:id/pending/approve", handlers.ApproveSavedQueryEdit) // Apply the proposed SQL (admin)
		api.POST("/explorer/saved/:id/pending/reject", handlers.RejectSavedQueryEdit)   // Discard it, keeping the record (admin)

		// Models (migration 085) — a saved query that materializes to a table,
		// optionally on a schedule. Every mutation below requires workspace ADMIN,
		// unlike the saving routes above: storing SQL mutates nothing, while pointing
		// a query at a table and rebuilding it unattended is a DDL act. Reading a
		// schedule stays viewer-level.
		// Workspace-wide schedule inventory. Every other schedule route below is keyed
		// by a saved-query id, which means a user could only find a schedule they
		// already knew the id of — the reason a scheduled query was unfindable in the
		// UI. Viewer-level and workspace-scoped, like /explorer/saved.
		api.GET("/explorer/schedules", handlers.ListSavedQuerySchedules)

		api.PUT("/explorer/saved/:id/materialization", handlers.SetSavedQueryMaterialization) // Set/clear the target table
		api.POST("/explorer/saved/:id/run", handlers.RunSavedQueryModel)                      // Materialize once, now, as the caller
		api.GET("/explorer/saved/:id/runs", handlers.ListSavedQueryRuns)                      // Attempt history (migration 086)
		api.GET("/explorer/saved/:id/schedule", handlers.GetSavedQuerySchedule)               // Read the schedule
		api.POST("/explorer/saved/:id/schedule", handlers.CreateSavedQuerySchedule)           // Attach a schedule
		api.PUT("/explorer/saved/:id/schedule", handlers.UpdateSavedQuerySchedule)            // Change the cadence
		api.DELETE("/explorer/saved/:id/schedule", handlers.DeleteSavedQuerySchedule)         // Detach (the table survives)
		api.POST("/explorer/saved/:id/schedule/pause", handlers.PauseSavedQuerySchedule)
		api.POST("/explorer/saved/:id/schedule/resume", handlers.ResumeSavedQuerySchedule)

		// Which pipelines produce the tables this model reads. Read-only and
		// viewer-level: it names pipelines writing into the workspace's own warehouse,
		// which a viewer can already list. Its answer is a suggestion for the
		// "After a pipeline runs" dialog and is never applied on its own.
		api.GET("/explorer/saved/:id/upstreams", handlers.SuggestSavedQueryUpstreams)
	}

	// Internal service-to-service endpoints — no user auth, service secret required.
	internal := r.Group("/api/v1/internal")
	internal.Use(handlers.InternalServiceMiddleware())
	{
		// Allows the orchestrator's self-healer to re-run a pipeline after a
		// transient failure (ActionBackoffRetry) without a user session — the
		// user-facing POST /pipelines/:id/run fail-closes 401 in prod. Same
		// Temporal enqueue as /run, minus the per-user cost quota + CSRF; the
		// per-workspace plan gates + run cooldown are kept (KI-HEAL-RERUN-UNAUTH).
		internal.POST("/pipelines/:id/run", handlers.RunPipelineInternal)

		// Fires one scheduled model rebuild. Called by ScheduledModelRunWorkflow,
		// which has no user session — so this endpoint re-resolves the schedule's
		// run_as_user_id against workspace_members and re-classifies the current
		// sql_text on EVERY tick, and auto-pauses the schedule if either check now
		// fails. A stored permission would be one that outlives the person.
		internal.POST("/explorer/models/:id/run", handlers.RunSavedQueryModelInternal)

		// Lets the executor resolve + lock the destination namespace at the WRITE
		// boundary, once it knows the final table set. Before this existed the
		// probe hung off the table-selection HITL alone, so a pipeline whose
		// prompt named its tables never parked, never resumed, and therefore
		// never got probed at all (KI-NSLOCK-PROBE-UNREACHABLE-WITHOUT-HITL).
		// api-gateway owns the probe because it can decrypt the destination
		// connection; the executor owns the trigger because it knows the tables.
		internal.POST("/pipelines/:id/namespace/lock", handlers.LockPipelineNamespaceInternal)

		// Allows orchestrator to refresh OAuth tokens without a user session cookie.
		internal.POST("/oauth/tokens/:token_id/refresh", func(c *gin.Context) {
			tokenID := c.Param("token_id")
			if err := oauthHandler.RefreshTokenByID(c.Request.Context(), tokenID); err != nil {
				if errors.Is(err, handlers.ErrTokenFresh) {
					c.JSON(http.StatusOK, gin.H{"success": true, "message": "token_fresh"})
					return
				}
				c.JSON(http.StatusInternalServerError, gin.H{"success": false, "error": err.Error()})
				return
			}
			c.JSON(http.StatusOK, gin.H{"success": true})
		})
	}

	// OAuth Callback (outside /api/v1 for cleaner URLs)
	r.GET("/oauth/callback/:provider", oauthHandler.Callback)

	// Protected Routes (for future use with JWT)
	// protected := r.Group("/")
	// protected.Use(auth.AuthMiddleware())
	// {
	// 	protected.Any("/api/*path", func(c *gin.Context) {
	// 		proxy.ServeHTTP(c.Writer, c.Request)
	// 	})
	// }

	log.Infof("API Gateway starting on port %s", port)

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,  // prevent slowloris header attacks
		ReadTimeout:       60 * time.Second,  // max time to read full request body
		WriteTimeout:      120 * time.Second, // max time to write response (covers LLM streaming)
		IdleTimeout:       120 * time.Second, // keepalive idle connection timeout
	}

	// Setup graceful shutdown
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Info("Shutting down server...")

	// Stop background tasks (cancels appCtx, stops domain event consumer loop)
	appCancel()
	handlers.ShutdownDomainEventManager()
	notifierSvc.Stop()

	// Close consumer with a bounded wait (avoid shutdown hang).
	consumerDone := make(chan struct{})
	go func() {
		consumer.Close()
		close(consumerDone)
	}()
	select {
	case <-consumerDone:
	case <-time.After(5 * time.Second):
		log.Warn("Timed out waiting for Kafka consumer to close")
	}

	kafkaProducer.Close()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Warnf("HTTP server shutdown error: %v", err)
	}
	log.Info("Server stopped")
}
