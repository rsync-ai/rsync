package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	appmetrics "github.com/rsync-ai/backend-orchestrator/internal/metrics"
	"github.com/rsync-ai/backend-orchestrator/internal/telemetry"
	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	"github.com/rsync-ai/shared/kafkaclient/saramaauth"
)

// Config holds Kafka configuration
type Config struct {
	Brokers string // Comma-separated broker list
	GroupID string
}

// Manager manages Kafka consumer and producer
type Manager struct {
	Config Config
	// security carries the resolved SASL/TLS settings and the SPLIT broker
	// list. Everything in this service that opens its own sarama connection
	// reads it via SecurityConfig() rather than re-deriving brokers from
	// Config.Brokers, so a customer-managed cluster is configured in one place.
	security  kafkaclient.Config
	client    sarama.Client
	producer  sarama.SyncProducer
	consumers map[string]sarama.ConsumerGroup
	mu        sync.RWMutex
	connected bool
	tracer    trace.Tracer

	// topology is the admin view over the SAME broker connection, built on first
	// use so a manager that never creates a topic never opens an admin client.
	// It has its own mutex because mu is held across consumer/producer work and
	// topic creation must not queue behind it.
	topology   *TopologyManager
	topologyMu sync.Mutex
}

// topologyFor returns the single TopologyManager layered over this manager's
// existing client, creating it once.
//
// The Manager and the TopologyManager were two unrelated ways to reach the same
// broker, and topic creation existed on both — which is exactly how the CDC
// pre-creation path came to sit outside the RF/min.insync.replicas policy that the
// TopologyManager routes apply. Giving the Manager a TopologyManager instead of its
// own creation code removes the second implementation rather than keeping the two
// in sync by hand.
//
// NewClusterAdminFromClient shares the underlying client, so the admin must NOT be
// Closed here (that closes the shared client). m.client is assigned once at
// construction and never replaced, so caching the wrapper cannot strand it.
func (m *Manager) topologyFor() (*TopologyManager, error) {
	m.topologyMu.Lock()
	defer m.topologyMu.Unlock()

	if m.topology != nil {
		return m.topology, nil
	}
	admin, err := sarama.NewClusterAdminFromClient(m.client)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster admin: %w", err)
	}
	m.topology = &TopologyManager{
		client:     m.client,
		admin:      admin,
		brokers:    m.Config.Brokers,
		topicCache: make(map[string]*TopicInfo),
		cacheTTL:   30 * time.Second,
	}
	return m.topology, nil
}

func envBool(name string, defaultVal bool) bool {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return defaultVal
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return defaultVal
	}
}

// NewManager creates a new Kafka manager
func NewManager(config Config) (*Manager, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V3_3_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest // Read all messages from beginning
	// COOPERATIVE REBALANCING: Incremental rebalancing (only affected consumers pause)
	// This protects against accidental replica scaling and reduces blast radius during upgrades
	// Note: Sarama's "Sticky" strategy is cooperative by default (Kafka 2.4+)
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategySticky(),
	}
	saramaConfig.Consumer.Group.Rebalance.Timeout = 30 * time.Second // Reduced for fast agent tasks

	// AGGRESSIVE TIMEOUTS for fast, stateless agent workflows
	// Static membership + long timeouts = zombie consumers (messages sit unprocessed)
	// Rule: Heartbeat interval should be 1/10 of session timeout
	saramaConfig.Consumer.Group.Session.Timeout = 30 * time.Second   // Max time between heartbeats (was 120s)
	saramaConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second // Heartbeat frequency (1/10 of session timeout)

	// STATIC MEMBERSHIP (optional): Prevents full rebalances on restarts when InstanceId is stable.
	//
	// IMPORTANT:
	// - Static membership DOES NOT inherently require replicas=1.
	// - Horizontal scaling still depends on topic partitioning (and appropriate message keys).
	//
	// Enable with:
	// - KAFKA_STATIC_MEMBERSHIP=true, or
	// - KAFKA_CONSUMER_INSTANCE_ID=<stable-unique-id>
	staticEnabled := envBool("KAFKA_STATIC_MEMBERSHIP", false)
	instanceID := strings.TrimSpace(os.Getenv("KAFKA_CONSUMER_INSTANCE_ID"))
	if instanceID != "" {
		staticEnabled = true
	}
	if staticEnabled {
		if instanceID == "" {
			instanceID = os.Getenv("HOSTNAME")
			if instanceID == "" {
				hostname, err := os.Hostname()
				if err != nil {
					hostname = "orchestrator-default"
				}
				instanceID = hostname
			}
			// Make instance ID unique and stable (same across pod restarts in K8s if HOSTNAME is stable)
			instanceID = fmt.Sprintf("orchestrator-%s", instanceID)
		}
		saramaConfig.Consumer.Group.InstanceId = instanceID
		log.Infof("🔒 Kafka static membership enabled (instance_id=%s, session_timeout=30s)", instanceID)
	} else {
		log.Info("🔓 Kafka static membership disabled (KAFKA_STATIC_MEMBERSHIP=false)")
	}

	// CRITICAL: max.poll.interval.ms equivalent
	// This is the maximum time between poll() calls before Kafka kicks the consumer
	// With async worker pool, handlers return quickly, so this is less critical but set conservatively
	saramaConfig.Consumer.MaxProcessingTime = 300 * time.Second // 5 minutes (was 120s)

	// Fetch configuration for real-time updates
	saramaConfig.Consumer.Fetch.Min = 1                        // Fetch immediately, don't wait
	saramaConfig.Consumer.Fetch.Default = 1024 * 1024          // 1MB fetch size for batching
	saramaConfig.Consumer.MaxWaitTime = 500 * time.Millisecond // Quick fetch for real-time updates

	// SAFE OFFSET COMMIT STRATEGY: Auto-commit + MarkMessage
	// This is the recommended safe pattern:
	//   1. Consumer reads message
	//   2. Handler processes message (starts Temporal workflow)
	//   3. On success: session.MarkMessage(msg, "")
	//   4. Auto-commit flushes marked offsets every 5s
	//   5. On failure: Don't mark → message redelivered
	//
	// Result: At-least-once delivery, no message loss, safe restart semantics
	saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
	saramaConfig.Consumer.Offsets.AutoCommit.Interval = 5 * time.Second

	// Producer config - OPTIMIZED FOR PERFORMANCE
	// WaitForAll: wait for all in-sync replicas — prevents silent message loss on leader crash.
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 3 // Reduced retries for faster failure
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Compression = sarama.CompressionSnappy
	saramaConfig.Producer.Timeout = 10 * time.Second // Increased from 2s - Docker network overhead
	saramaConfig.Producer.MaxMessageBytes = 10485760 // 10MB max message size

	// Network timeouts - dramatically increased for Docker environment stability
	// Docker adds latency: container networking, DNS resolution, load balancing
	saramaConfig.Net.DialTimeout = 30 * time.Second  // Allow time for Docker DNS + connection
	saramaConfig.Net.ReadTimeout = 60 * time.Second  // Allow for network latency + broker processing
	saramaConfig.Net.WriteTimeout = 60 * time.Second // Allow for network latency + broker processing

	// Resolve SASL/TLS from the environment, but let Config.Brokers stay
	// authoritative for WHERE we connect — adopting kafkaclient must change
	// only how the connection is secured, never its address.
	//
	// The broker string is SPLIT here. It used to be wrapped as
	// []string{config.Brokers}, which turned a customer's multi-broker
	// bootstrap list into a single unresolvable hostname; this client is the
	// parent of the producer, every consumer group and the cluster admin, so
	// that collapse took the whole data path with it.
	security, err := serviceSecurityConfig(config.Brokers)
	if err != nil {
		return nil, err
	}
	for _, warning := range security.Warnings() {
		log.Warnf("⚠️  Kafka config: %s", warning)
	}
	log.Infof("🔌 Kafka client config: %s", security)
	// Print the namespace every topic will be minted under. Producer and
	// consumer disagreeing about it is a silent failure -- nobody errors, the
	// reader just waits on a topic nobody writes -- so an operator debugging
	// "the pipeline hangs" needs to be able to compare this line across
	// services and against `kafka-topics --list`.
	if prefix := kafkaclient.TopicPrefix(); prefix != "" {
		log.Infof("🏷️  Kafka topic namespace: %q (set %s to change; empty disables)", prefix, kafkaclient.EnvTopicPrefix)
	} else {
		log.Warnf("⚠️  Kafka topic namespace disabled (%s=\"\"): topics are created unprefixed and are indistinguishable from other applications' topics on a shared cluster", kafkaclient.EnvTopicPrefix)
	}

	// Create client
	client, err := saramaauth.NewClient(security, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Create producer
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	manager := &Manager{
		Config:    config,
		security:  security,
		client:    client,
		producer:  producer,
		consumers: make(map[string]sarama.ConsumerGroup),
		connected: true,
		tracer:    otel.Tracer("kafka-manager"),
	}

	log.Info("✅ Kafka client and producer initialized")
	return manager, nil
}

// SecurityConfig returns the resolved Kafka connection settings: the SPLIT
// broker list plus whatever SASL/TLS the environment asked for.
//
// Any component that opens its own sarama connection — the sentinel healers,
// the CDC stats agents — must build it from this rather than re-splitting
// Config.Brokers itself. Those call sites each carried their own copy of the
// split loop and their own hardcoded "kafka:29092" fallback, which on a
// customer-managed cluster meant silently dialing a hostname that only exists
// inside the bundled compose stack.
func (m *Manager) SecurityConfig() kafkaclient.Config {
	return m.security
}

// IsConnected returns the connection status
func (m *Manager) IsConnected() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connected
}

// Ping performs a real round trip to the Kafka cluster and reports whether it answered.
//
// Deliberately NOT IsConnected() and NOT ListTopics(), which are the two obvious
// candidates and are both useless as liveness probes:
//
//   - `connected` is a bool set once in NewManager and cleared only by Close(). It says
//     "we were constructed and not shut down", never "the broker is up".
//   - ListTopics() calls sarama's client.Topics(), which iterates the client's cached
//     metadata map under a read lock and never touches the network. It returns the same
//     list, successfully, for a cluster that died minutes ago.
//
// RefreshMetadata() issues an actual metadata request and returns an error when no broker
// answers it, so it is the only one of the three that can tell a live cluster from a dead
// one. It costs one metadata request per call (sarama's default config is Metadata.Full,
// so the response covers every topic) — cheap at the health monitor's 30s cadence, which
// is why the caller, not this method, owns the interval.
func (m *Manager) Ping() error {
	m.mu.RLock()
	connected := m.connected
	client := m.client
	m.mu.RUnlock()

	if !connected || client == nil {
		return fmt.Errorf("kafka manager not connected")
	}
	if err := client.RefreshMetadata(); err != nil {
		return fmt.Errorf("kafka metadata refresh failed: %w", err)
	}
	// A refresh that succeeds against a cluster reporting zero brokers is not a healthy
	// cluster; producing to it would block rather than fail.
	if len(client.Brokers()) == 0 {
		return fmt.Errorf("kafka cluster returned no brokers")
	}
	return nil
}

// Produce sends a message to a Kafka topic
func (m *Manager) Produce(topic string, key, value []byte) error {
	return m.ProduceWithContext(context.Background(), topic, key, value)
}

// ProduceWithContext sends a message with trace context propagation
func (m *Manager) ProduceWithContext(ctx context.Context, topic string, key, value []byte) error {
	// Qualify here rather than at the ~60 call sites: every Produce/Consume
	// variant funnels through these three, so a topic named anywhere in the
	// orchestrator -- including one added later -- lands in the platform
	// namespace without the author having to remember. Idempotent, so an
	// already-qualified name from generateTopicName passes through unchanged.
	topic = kafkaclient.Topic(topic)
	// Start a span for the produce operation
	ctx, span := m.tracer.Start(ctx, fmt.Sprintf("kafka.produce.%s", topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
			attribute.String("messaging.destination_kind", "topic"),
		),
	)
	defer span.End()

	// Inject trace context into headers
	headers := m.injectTraceHeaders(ctx)

	// Add explicit trace_id header for debugging
	traceID := telemetry.TraceIDFromContext(ctx)
	if traceID != "" {
		headers["trace_id"] = traceID
	}

	// Convert to sarama headers
	saramaHeaders := make([]sarama.RecordHeader, 0, len(headers))
	for k, v := range headers {
		saramaHeaders = append(saramaHeaders, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.ByteEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: saramaHeaders,
	}

	partition, offset, err := m.producer.SendMessage(msg)
	if err != nil {
		appmetrics.KafkaMessagesPublishedTotal.WithLabelValues(topic, "failure").Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to produce message")
		log.Errorf("Failed to produce message to topic %s: %v", topic, err)
		return fmt.Errorf("failed to produce message: %w", err)
	}
	appmetrics.KafkaMessagesPublishedTotal.WithLabelValues(topic, "success").Inc()

	span.SetAttributes(
		attribute.Int64("messaging.kafka.partition", int64(partition)),
		attribute.Int64("messaging.kafka.offset", offset),
	)
	span.SetStatus(codes.Ok, "")

	log.WithFields(log.Fields{
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
		"trace_id":  traceID,
	}).Debug("Message produced to Kafka")

	return nil
}

// ProduceWithHeaders sends a message to a Kafka topic with custom headers
func (m *Manager) ProduceWithHeaders(topic string, key, value []byte, headers map[string]string) error {
	return m.ProduceWithHeadersAndContext(context.Background(), topic, key, value, headers)
}

// ProduceWithHeadersAndContext sends a message with custom headers and trace context
func (m *Manager) ProduceWithHeadersAndContext(ctx context.Context, topic string, key, value []byte, customHeaders map[string]string) error {
	// Qualify here rather than at the ~60 call sites: every Produce/Consume
	// variant funnels through these three, so a topic named anywhere in the
	// orchestrator -- including one added later -- lands in the platform
	// namespace without the author having to remember. Idempotent, so an
	// already-qualified name from generateTopicName passes through unchanged.
	topic = kafkaclient.Topic(topic)
	// Start a span for the produce operation
	ctx, span := m.tracer.Start(ctx, fmt.Sprintf("kafka.produce.%s", topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
			attribute.String("messaging.destination_kind", "topic"),
		),
	)
	defer span.End()

	// Inject trace context into headers
	headers := m.injectTraceHeaders(ctx)

	// Merge custom headers (custom headers take precedence)
	for k, v := range customHeaders {
		headers[k] = v
	}

	// Add explicit trace_id if not already present
	traceID := telemetry.TraceIDFromContext(ctx)
	if _, exists := headers["trace_id"]; !exists && traceID != "" {
		headers["trace_id"] = traceID
	}

	// Convert to sarama headers
	saramaHeaders := make([]sarama.RecordHeader, 0, len(headers))
	for k, v := range headers {
		saramaHeaders = append(saramaHeaders, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Key:     sarama.ByteEncoder(key),
		Value:   sarama.ByteEncoder(value),
		Headers: saramaHeaders,
	}

	partition, offset, err := m.producer.SendMessage(msg)
	if err != nil {
		appmetrics.KafkaMessagesPublishedTotal.WithLabelValues(topic, "failure").Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to produce message")
		log.Errorf("Failed to produce message to topic %s: %v", topic, err)
		return fmt.Errorf("failed to produce message: %w", err)
	}
	appmetrics.KafkaMessagesPublishedTotal.WithLabelValues(topic, "success").Inc()

	span.SetAttributes(
		attribute.Int64("messaging.kafka.partition", int64(partition)),
		attribute.Int64("messaging.kafka.offset", offset),
	)
	span.SetStatus(codes.Ok, "")

	log.WithFields(log.Fields{
		"topic":     topic,
		"partition": partition,
		"offset":    offset,
		"trace_id":  traceID,
		"headers":   len(headers),
	}).Debug("Message produced to Kafka with headers")

	return nil
}

// injectTraceHeaders injects OTel trace context into a header map
func (m *Manager) injectTraceHeaders(ctx context.Context) map[string]string {
	headers := make(map[string]string)
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(headers)
	propagator.Inject(ctx, carrier)
	return headers
}

// extractTraceContext extracts OTel trace context from Kafka message headers
func (m *Manager) extractTraceContext(headers []*sarama.RecordHeader) (context.Context, map[string]string) {
	headerMap := make(map[string]string)
	for _, h := range headers {
		headerMap[string(h.Key)] = string(h.Value)
	}

	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(headerMap)
	ctx := propagator.Extract(context.Background(), carrier)

	return ctx, headerMap
}

// ConsumeHandler is a function that processes consumed messages
type ConsumeHandler func(message *sarama.ConsumerMessage) error

// ConsumeHandlerWithContext is a function that processes consumed messages with context
type ConsumeHandlerWithContext func(ctx context.Context, message *sarama.ConsumerMessage) error

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	topic          string
	handler        ConsumeHandler
	handlerWithCtx ConsumeHandlerWithContext
	producer       sarama.SyncProducer // For DLQ
	maxRetries     int
	tracer         trace.Tracer
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Infof("Kafka consumer group session started for topic: %s", h.topic)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Infof("Kafka consumer group session ended for topic: %s", h.topic)
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	log.Infof("🔧 [%s] ConsumeClaim() ENTERED - Starting poll loop for partition %d", h.topic, claim.Partition())

	for {
		log.Debugf("🔧 [%s] Polling Kafka for messages...", h.topic)
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// Extract trace context from message headers
			ctx, headerMap := h.extractTraceContext(message.Headers)

			// Start a consumer span as child of extracted context
			ctx, span := h.tracer.Start(ctx, fmt.Sprintf("kafka.consume.%s", h.topic),
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "kafka"),
					attribute.String("messaging.destination", message.Topic),
					attribute.Int64("messaging.kafka.partition", int64(message.Partition)),
					attribute.Int64("messaging.kafka.offset", message.Offset),
				),
			)

			// Get trace_id for logging
			traceID := telemetry.TraceIDFromContext(ctx)
			if traceID == "" {
				// Fallback to header
				traceID = headerMap["trace_id"]
			}

			log.WithFields(log.Fields{
				"topic":     message.Topic,
				"partition": message.Partition,
				"offset":    message.Offset,
				"trace_id":  traceID,
			}).Info("📩 Message received from Kafka")

			// Process message with retry logic
			var lastErr error
			maxRetries := h.maxRetries
			if maxRetries == 0 {
				maxRetries = 3 // Default
			}

			for attempt := 1; attempt <= maxRetries; attempt++ {
				var err error
				if h.handlerWithCtx != nil {
					err = h.handlerWithCtx(ctx, message)
				} else {
					err = h.handler(message)
				}

				if err != nil {
					lastErr = err
					span.RecordError(err)
					log.WithFields(log.Fields{
						"attempt":  attempt,
						"max":      maxRetries,
						"trace_id": traceID,
						"error":    err.Error(),
					}).Warn("⚠️  Message processing attempt failed")

					if attempt < maxRetries {
						// Exponential backoff
						sleepDuration := time.Duration(attempt*attempt) * 100 * time.Millisecond
						time.Sleep(sleepDuration)
					}
				} else {
					lastErr = nil
					span.SetStatus(codes.Ok, "")
					log.WithField("trace_id", traceID).Info("✅ Message processed successfully")
					break
				}
			}

			// CRITICAL OFFSET COMMIT LOGIC:
			// Only commit offset if processing succeeded OR DLQ send succeeded
			// This prevents data loss on handler failure + DLQ failure
			shouldCommit := false

			if lastErr != nil {
				span.SetStatus(codes.Error, "All retries failed")
				log.WithFields(log.Fields{
					"retries":  maxRetries,
					"trace_id": traceID,
					"error":    lastErr.Error(),
				}).Error("❌ All retries failed, sending to DLQ")

				// Try to send to DLQ, only commit offset if DLQ send succeeds
				dlqSuccess := h.sendToDLQ(ctx, message, lastErr)
				if dlqSuccess {
					shouldCommit = true
					log.WithField("trace_id", traceID).Info("✅ Message sent to DLQ, committing offset")
				} else {
					shouldCommit = false
					log.WithField("trace_id", traceID).Error("❌ DLQ send failed, NOT committing offset (message will be redelivered)")
				}
			} else {
				// Handler succeeded
				shouldCommit = true
			}

			span.End()

			// Commit offset only if processing succeeded OR DLQ succeeded
			if shouldCommit {
				session.MarkMessage(message, "")
				log.WithField("trace_id", traceID).Debug("✅ Offset committed")
			}

		case <-session.Context().Done():
			return nil
		}
	}
}

// SmartDeserialize deserializes Kafka message data that may be either Avro or JSON
// It automatically detects the format based on the magic byte
func SmartDeserialize(data []byte, target interface{}) error {
	if len(data) == 0 {
		return fmt.Errorf("empty message data")
	}

	// Check for Avro magic byte (Confluent wire format starts with 0x00)
	if data[0] == 0x00 {
		if len(data) < 5 {
			return fmt.Errorf("Avro message too short: %d bytes", len(data))
		}
		schemaID := int(binary.BigEndian.Uint32(data[1:5]))
		payload := data[5:]
		if len(payload) == 0 {
			return fmt.Errorf("empty Avro payload (schema_id=%d)", schemaID)
		}

		decoded, err := decodeConfluentAvro(schemaID, payload)
		if err != nil {
			allowJSON := strings.TrimSpace(os.Getenv("AVRO_ALLOW_JSON_PAYLOAD")) != ""
			if allowJSON {
				// Legacy compatibility: older producers wrote JSON after the header.
				if jerr := json.Unmarshal(payload, target); jerr == nil {
					return nil
				}
			}
			return err
		}

		// Normalize union wrappers, then unmarshal into the target type.
		plain := normalizeAvroNative(decoded)
		b, err := json.Marshal(plain)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, target); err != nil {
			return fmt.Errorf("failed to unmarshal decoded Avro into target: %w", err)
		}
		return nil
	}

	// Skip any leading whitespace or null bytes that might remain
	start := 0
	for start < len(data) && (data[start] == 0x00 || data[start] == ' ' || data[start] == '\t' || data[start] == '\n' || data[start] == '\r') {
		start++
	}

	if start >= len(data) {
		return fmt.Errorf("no valid content found after skipping whitespace/null bytes")
	}

	data = data[start:]

	// Parse as JSON
	if err := json.Unmarshal(data, target); err != nil {
		// Log first bytes for debugging
		preview := data
		if len(preview) > 100 {
			preview = preview[:100]
		}
		return fmt.Errorf("JSON parse failed (first %d bytes: %q): %w", len(preview), preview, err)
	}

	return nil
}

type avroDecodeHelper struct {
	registryURL string
	client      *http.Client

	mu         sync.RWMutex
	schemaByID map[int]string
	codecByID  map[int]*goavro.Codec
}

var globalAvroDecoder struct {
	once sync.Once
	dec  *avroDecodeHelper
}

func getAvroDecoder() *avroDecodeHelper {
	globalAvroDecoder.once.Do(func() {
		u := strings.TrimSpace(os.Getenv("SCHEMA_REGISTRY_URL"))
		if u == "" {
			u = "http://schema-registry:8080"
		}
		globalAvroDecoder.dec = &avroDecodeHelper{
			registryURL: strings.TrimRight(u, "/"),
			client:      &http.Client{Timeout: 5 * time.Second},
			schemaByID:  make(map[int]string),
			codecByID:   make(map[int]*goavro.Codec),
		}
	})
	return globalAvroDecoder.dec
}

func decodeConfluentAvro(schemaID int, payload []byte) (interface{}, error) {
	dec := getAvroDecoder()

	dec.mu.RLock()
	if c, ok := dec.codecByID[schemaID]; ok && c != nil {
		dec.mu.RUnlock()
		native, _, err := c.NativeFromBinary(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to decode avro binary (schema_id=%d): %w", schemaID, err)
		}
		return native, nil
	}
	dec.mu.RUnlock()

	schema, err := dec.getSchemaByID(schemaID)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to compile avro codec (schema_id=%d): %w", schemaID, err)
	}

	dec.mu.Lock()
	dec.codecByID[schemaID] = codec
	dec.mu.Unlock()

	native, _, err := codec.NativeFromBinary(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to decode avro binary (schema_id=%d): %w", schemaID, err)
	}
	return native, nil
}

// normalizeAvroNative unwraps union wrappers and recursively normalizes nested structures.
func normalizeAvroNative(v interface{}) interface{} {
	switch tv := v.(type) {
	case map[string]interface{}:
		// Union wrappers are maps with exactly one key.
		if len(tv) == 1 {
			for _, inner := range tv {
				return normalizeAvroNative(inner)
			}
		}
		out := make(map[string]interface{}, len(tv))
		for k, val := range tv {
			out[k] = normalizeAvroNative(val)
		}
		return out
	case []interface{}:
		out := make([]interface{}, 0, len(tv))
		for _, it := range tv {
			out = append(out, normalizeAvroNative(it))
		}
		return out
	default:
		return v
	}
}

func (d *avroDecodeHelper) getSchemaByID(schemaID int) (string, error) {
	d.mu.RLock()
	if s, ok := d.schemaByID[schemaID]; ok && strings.TrimSpace(s) != "" {
		d.mu.RUnlock()
		return s, nil
	}
	d.mu.RUnlock()

	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/schemas/ids/%d", d.registryURL, schemaID), nil)
	if err != nil {
		return "", err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("schema registry request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("schema registry error (status=%d): %s", resp.StatusCode, string(body))
	}

	var parsed struct {
		Schema string `json:"schema"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return "", err
	}

	d.mu.Lock()
	d.schemaByID[schemaID] = parsed.Schema
	d.mu.Unlock()
	return parsed.Schema, nil
}

// extractTraceContext extracts OTel trace context from Kafka message headers
func (h *ConsumerGroupHandler) extractTraceContext(headers []*sarama.RecordHeader) (context.Context, map[string]string) {
	headerMap := make(map[string]string)
	for _, header := range headers {
		headerMap[string(header.Key)] = string(header.Value)
	}

	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(headerMap)
	ctx := propagator.Extract(context.Background(), carrier)

	return ctx, headerMap
}

// sendToDLQ sends a failed message to the Dead Letter Queue with trace context
func (h *ConsumerGroupHandler) sendToDLQ(ctx context.Context, message *sarama.ConsumerMessage, err error) bool {
	if h.producer == nil {
		log.Warn("⚠️  No producer available for DLQ, message will be lost")
		return false
	}

	// Start DLQ span
	_, span := h.tracer.Start(ctx, "kafka.dlq.send",
		trace.WithSpanKind(trace.SpanKindProducer),
	)
	defer span.End()

	dlqTopic := h.topic + ".dlq"

	// Copy original headers and add error details
	headers := make([]sarama.RecordHeader, 0, len(message.Headers)+4)
	for _, header := range message.Headers {
		headers = append(headers, *header)
	}
	headers = append(headers, sarama.RecordHeader{
		Key:   []byte("dlq_error"),
		Value: []byte(err.Error()),
	})
	headers = append(headers, sarama.RecordHeader{
		Key:   []byte("dlq_original_topic"),
		Value: []byte(message.Topic),
	})
	headers = append(headers, sarama.RecordHeader{
		Key:   []byte("dlq_timestamp"),
		Value: []byte(time.Now().UTC().Format(time.RFC3339)),
	})

	// Inject trace context
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(make(map[string]string))
	propagator.Inject(ctx, carrier)
	for k, v := range carrier {
		headers = append(headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: []byte(v),
		})
	}

	dlqMsg := &sarama.ProducerMessage{
		Topic:   dlqTopic,
		Key:     sarama.ByteEncoder(message.Key),
		Value:   sarama.ByteEncoder(message.Value),
		Headers: headers,
	}

	_, _, sendErr := h.producer.SendMessage(dlqMsg)
	if sendErr != nil {
		span.RecordError(sendErr)
		log.Errorf("❌ Failed to send message to DLQ topic %s: %v", dlqTopic, sendErr)
		return false // DLQ send failed
	} else {
		appmetrics.DLQMessagesTotal.WithLabelValues(message.Topic).Inc()
		span.SetStatus(codes.Ok, "")
		log.Infof("📤 Message sent to DLQ topic %s", dlqTopic)
		return true // DLQ send succeeded
	}
}

// Consume starts consuming messages from a topic
func (m *Manager) Consume(topic string, handler ConsumeHandler) error {
	return m.ConsumeWithContext(topic, func(ctx context.Context, msg *sarama.ConsumerMessage) error {
		return handler(msg)
	})
}

// ConsumeWithContext starts consuming messages with trace context support
func (m *Manager) ConsumeWithContext(topic string, handler ConsumeHandlerWithContext) error {
	// Qualify here rather than at the ~60 call sites: every Produce/Consume
	// variant funnels through these three, so a topic named anywhere in the
	// orchestrator -- including one added later -- lands in the platform
	// namespace without the author having to remember. Idempotent, so an
	// already-qualified name from generateTopicName passes through unchanged.
	topic = kafkaclient.Topic(topic)
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if already consuming this topic
	if _, exists := m.consumers[topic]; exists {
		return fmt.Errorf("already consuming topic: %s", topic)
	}

	// Create topic-specific consumer group to avoid rebalancing conflicts
	// Each topic gets its own consumer group for independent consumption
	topicGroupID := fmt.Sprintf("%s-%s", m.Config.GroupID, topic)

	consumerGroup, err := sarama.NewConsumerGroupFromClient(topicGroupID, m.client)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	log.Infof("📡 Consumer group '%s' created for topic: %s", topicGroupID, topic)

	m.consumers[topic] = consumerGroup

	// Start consuming in goroutine
	go func() {
		groupHandler := &ConsumerGroupHandler{
			topic:          topic,
			handlerWithCtx: handler,
			producer:       m.producer, // Pass producer for DLQ support
			maxRetries:     3,          // Default 3 retries before DLQ
			tracer:         m.tracer,
		}

		ctx := context.Background()
		for {
			// This will block until session ends
			if err := consumerGroup.Consume(ctx, []string{topic}, groupHandler); err != nil {
				log.Errorf("Error from consumer group for topic %s: %v", topic, err)
				time.Sleep(5 * time.Second) // Wait before retrying
			}

			// Check if context is done (shutdown)
			select {
			case <-ctx.Done():
				log.Infof("Consumer for topic %s shutting down", topic)
				return
			default:
			}
		}
	}()

	log.Infof("✅ Started consuming from topic: %s", topic)
	return nil
}

// StopConsuming stops consuming from a specific topic
func (m *Manager) StopConsuming(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	consumer, exists := m.consumers[topic]
	if !exists {
		// Not consuming this topic, nothing to do
		return nil
	}

	log.Infof("🛑 Stopping consumer for topic: %s", topic)

	if err := consumer.Close(); err != nil {
		log.Errorf("Error closing consumer for topic %s: %v", topic, err)
		// Still remove from map even if close failed
		delete(m.consumers, topic)
		return fmt.Errorf("failed to close consumer for topic %s: %w", topic, err)
	}

	delete(m.consumers, topic)
	log.Infof("✅ Stopped consuming from topic: %s", topic)
	return nil
}

// Close closes all Kafka connections
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Info("Closing Kafka connections...")

	// Close all consumers
	for topic, consumer := range m.consumers {
		if err := consumer.Close(); err != nil {
			log.Errorf("Error closing consumer for topic %s: %v", topic, err)
		}
	}

	// Close producer
	if err := m.producer.Close(); err != nil {
		log.Errorf("Error closing producer: %v", err)
	}

	// Close client
	if err := m.client.Close(); err != nil {
		log.Errorf("Error closing client: %v", err)
	}

	m.connected = false
	log.Info("✅ Kafka connections closed")
	return nil
}

// TopicMetadata represents metadata about a Kafka topic
type TopicMetadata struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int
}

// ConsumerGroupLag represents lag information for a consumer group
type ConsumerGroupLag struct {
	GroupID       string
	Topic         string
	Partition     int32
	CurrentOffset int64
	LogEndOffset  int64
	Lag           int64
}

// GetConsumerGroupLag retrieves lag information for a specific consumer group
func (m *Manager) GetConsumerGroupLag(groupID string) (map[string]int64, error) {
	if !m.connected {
		return nil, fmt.Errorf("kafka manager not connected")
	}

	// Create a coordinator client to fetch offsets
	coordinator, err := m.client.Coordinator(groupID)
	if err != nil {
		return nil, fmt.Errorf("failed to get coordinator for group %s: %w", groupID, err)
	}

	// Fetch offsets for the consumer group
	offsetsRequest := &sarama.OffsetFetchRequest{
		Version:       1,
		ConsumerGroup: groupID,
	}

	// Get all topics for this group
	topics, err := m.client.Topics()
	if err != nil {
		return nil, fmt.Errorf("failed to get topics: %w", err)
	}
	for _, topic := range topics {
		partitions, err := m.client.Partitions(topic)
		if err != nil {
			log.WithError(err).WithField("topic", topic).Warn("Failed to get partitions")
			continue
		}
		for _, partition := range partitions {
			offsetsRequest.AddPartition(topic, partition)
		}
	}

	offsetsResponse, err := coordinator.FetchOffset(offsetsRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch offsets: %w", err)
	}

	// Collect the group's committed offset per topic/partition. Kafka reports -1 for a
	// partition the group has NEVER committed to.
	committed := make(map[string]map[int32]int64)
	for topic, partitions := range offsetsResponse.Blocks {
		for partition, block := range partitions {
			if committed[topic] == nil {
				committed[topic] = make(map[int32]int64)
			}
			committed[topic][partition] = block.Offset
		}
	}

	// Fetch the log-end (high-watermark) offset ONLY for partitions this group has actually
	// committed to. A per-pipeline sink group commits only to the topics it consumes, so
	// skipping never-committed (-1) partitions both scopes the result to the pipeline's own
	// topics and avoids a GetOffset round-trip for every foreign cluster topic.
	logEnd := make(map[string]map[int32]int64)
	for topic, partitions := range committed {
		for partition, offset := range partitions {
			if offset < 0 {
				continue
			}
			logEndOffset, err := m.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.WithError(err).WithFields(log.Fields{
					"topic":     topic,
					"partition": partition,
				}).Warn("Failed to get log end offset")
				continue
			}
			if logEnd[topic] == nil {
				logEnd[topic] = make(map[int32]int64)
			}
			logEnd[topic][partition] = logEndOffset
		}
	}

	return computeConsumerGroupLag(committed, logEnd), nil
}

// computeConsumerGroupLag is the pure core of GetConsumerGroupLag, split out so the
// topic-scoping invariant can be unit-tested without a live broker.
//
// committed[topic][partition] is the group's committed offset (Kafka reports -1 when the
// group has NEVER committed that partition); logEnd[topic][partition] is the partition
// high-watermark. Partitions the group never committed are SKIPPED: a per-pipeline sink
// group only commits to the topics it actually consumes, so a never-committed partition
// belongs to some OTHER pipeline's topic and its high-watermark must not be counted as this
// sink's backlog. A topic with no committed partition is omitted entirely, keeping the
// result scoped to the pipeline's own topics — this is what kills the cluster-wide phantom
// where every foreign topic's whole size was summed as lag (the 14.8M false alarm). A racy
// or missing high-watermark (below the committed offset) clamps to 0 rather than going
// negative.
func computeConsumerGroupLag(committed, logEnd map[string]map[int32]int64) map[string]int64 {
	lagByTopic := make(map[string]int64)
	for topic, partitions := range committed {
		var topicLag int64
		hasCommitted := false
		for partition, offset := range partitions {
			if offset < 0 {
				continue // never committed by this group → foreign topic, not our backlog
			}
			hasCommitted = true
			if lag := logEnd[topic][partition] - offset; lag > 0 {
				topicLag += lag
			}
		}
		if hasCommitted {
			lagByTopic[topic] = topicLag
		}
	}
	return lagByTopic
}

// consumerGroupLister is the slice of sarama.ClusterAdmin that ListConsumerGroups
// needs, narrowed so the aggregation contract can be exercised without a cluster.
type consumerGroupLister interface {
	ListConsumerGroups() (map[string]string, error)
}

// ListConsumerGroups lists all consumer groups known to the cluster.
//
// Kafka partitions group metadata across brokers: each group lives on the broker
// coordinating its __consumer_offsets partition, so a ListGroups request answers only
// for the broker that received it. This used to ask Brokers()[0] and return its answer
// as if it were the whole cluster. On the bundled single-broker stack that is the same
// thing, which is why it never showed; on a customer's multi-broker cluster it is a
// silent partial result, and partial is worse than an error here — a lag-based
// autoscaler or the sentinel's wedge detector reads a missing group as "that group has
// no lag" rather than as "I could not see it".
//
// sarama's ClusterAdmin queries every broker in parallel and returns an error if ANY of
// them failed (admin.go ListConsumerGroups), which is exactly the contract this needs:
// aggregate, and refuse to answer at all rather than answer partially.
func (m *Manager) ListConsumerGroups() ([]string, error) {
	if !m.connected || m.client == nil {
		return nil, fmt.Errorf("kafka manager not connected")
	}

	admin, err := sarama.NewClusterAdminFromClient(m.client)
	if err != nil {
		return nil, fmt.Errorf("failed to create cluster admin: %w", err)
	}
	// NewClusterAdminFromClient shares the underlying client; do NOT Close() it here
	// (admin.Close() closes the shared client). It is GC'd when it goes out of scope.
	return listConsumerGroups(admin)
}

// listConsumerGroups is the pure core of ListConsumerGroups, split out so the
// "never return a partial list" rule is unit-testable without a live broker.
//
// The nil result on error is the whole point: sarama populates the group map AND
// returns an error when only some brokers answered, so returning both would hand the
// caller a plausible-looking list that is missing whatever lived on the dead broker.
func listConsumerGroups(admin consumerGroupLister) ([]string, error) {
	groups, err := admin.ListConsumerGroups()
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	names := make([]string, 0, len(groups))
	for groupID := range groups {
		names = append(names, groupID)
	}
	return names, nil
}

// GetTopicMetadata retrieves metadata for a specific topic
func (m *Manager) GetTopicMetadata(topic string) (*TopicMetadata, error) {
	if !m.connected {
		return nil, fmt.Errorf("kafka manager not connected")
	}

	// Get partitions for the topic
	partitions, err := m.client.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions for topic %s: %w", topic, err)
	}

	// Get replication factor (from the first partition)
	replicationFactor := 1
	if len(partitions) > 0 {
		replicas, err := m.client.Replicas(topic, partitions[0])
		if err == nil {
			replicationFactor = len(replicas)
		}
	}

	metadata := &TopicMetadata{
		Name:              topic,
		NumPartitions:     len(partitions),
		ReplicationFactor: replicationFactor,
	}

	return metadata, nil
}

// ListTopics returns all topic names known to the broker.
func (m *Manager) ListTopics() ([]string, error) {
	if !m.connected {
		return nil, fmt.Errorf("kafka manager not connected")
	}
	return m.client.Topics()
}

// EnsureTopicExists creates the topic with the given partition count if it does not
// already exist, reusing the manager's existing broker connection. Idempotent and
// safe to call concurrently (an "already exists" race is treated as success).
//
// This is used to pre-create a Debezium CDC topic ("cdc-<id>.<db>.<table>") BEFORE
// starting the kafka-mcp-sink consumer for it. Debezium only creates that topic when
// it emits its first change event; with snapshot.mode=recovery/no_data (hybrid CDC,
// no snapshot) the first event may not arrive for a while, so a sink that starts
// against a non-existent topic gets 0 assigned partitions and never consumes. Unlike
// the batch topic we cannot pre-create it with a bootstrap marker (the CDC sink would
// reject the non-Debezium message), so we create it empty via the admin API instead.
func (m *Manager) EnsureTopicExists(topic string, partitions int32) error {
	return m.EnsureTopicExistsWithConfig(topic, partitions, nil)
}

// EnsureTopicExistsWithConfig is EnsureTopicExists for a topic whose per-topic
// configuration is part of its correctness rather than a tuning preference.
//
// The Debezium schema-history topic is the reason this exists. Debezium replays that
// topic in full on every connector restart to rebuild the source DDL, so it must be
// 1 partition, uncompacted, and retained forever. Inheriting the broker's defaults
// instead — log.retention.hours=168 on the bundled broker, cleanup.policy=compact on
// several managed offerings — deletes DDL entries that are still needed, and the
// connector then fails on its FIRST RESTART, not on first run: the pipeline works for
// days and then dies with a schema-history error naming nothing about retention.
//
// configEntries may be nil, which is exactly the 2-arg behaviour: no per-topic
// overrides beyond the durability floor applyReplicationPolicy pins on every path.
func (m *Manager) EnsureTopicExistsWithConfig(topic string, partitions int32, configEntries map[string]string) error {
	if !m.connected || m.client == nil {
		return fmt.Errorf("kafka manager not connected")
	}
	if strings.TrimSpace(topic) == "" {
		return fmt.Errorf("topic name is required")
	}
	// Fast path: already present.
	if existing, err := m.client.Topics(); err == nil {
		for _, t := range existing {
			if t == topic {
				return nil
			}
		}
	}
	tm, err := m.topologyFor()
	if err != nil {
		return err
	}
	return ensureAuthoritativeTopic(tm, topic, partitions, configEntries)
}

// ensureAuthoritativeTopic is the TopicConfig that EnsureTopicExists asks for, split
// from the client plumbing so a test can assert what actually reaches Kafka on this
// path without standing up a broker. The flags below are the load-bearing part of
// this function; a test that constructed its own TopicConfig would be asserting its
// own opinion rather than the production one.
func ensureAuthoritativeTopic(tm *TopologyManager, topic string, partitions int32, configEntries map[string]string) error {
	// This used to be a hand-rolled sarama.TopicDetail right here —
	// ReplicationFactor: 1, no ConfigEntries at all — sitting entirely outside the
	// policy the TopologyManager routes apply, on the path that runs on every CDC
	// start. Against a broker whose default min.insync.replicas is 2 (MSK's, and most
	// managed clusters') that produced a topic created RF=1, inheriting misr=2, born
	// permanently unwritable: ListTopics shows it, the sink subscribes to it, and
	// every acks=all produce returns NOT_ENOUGH_REPLICAS with nothing in any log
	// naming the replication factor. The pipeline reports running and streams zero
	// rows. Routing through EnsureTopic makes that unrepresentable.
	// Copied rather than aliased: normalizeTopicConfig -> applyReplicationPolicy WRITES
	// min.insync.replicas into this map, and the caller's literal is built once per CDC
	// start. Handing the caller's own map to a mutating policy is how a per-topic
	// override acquires a value nobody wrote.
	var cfgEntries map[string]string
	if len(configEntries) > 0 {
		cfgEntries = make(map[string]string, len(configEntries))
		for k, v := range configEntries {
			cfgEntries[k] = v
		}
	}
	return tm.EnsureTopic(context.Background(), TopicConfig{
		Name:       topic,
		Partitions: partitions,
		// The caller's per-topic configuration, when it has one. Empty for the CDC and
		// batch data topics, which want exactly the broker's defaults; non-empty for
		// the Debezium schema history, whose retention and cleanup policy are a
		// correctness requirement rather than a preference.
		Config: cfgEntries,
		// These topics carry KEYED CDC records. Growing the partition count of a
		// topic Debezium already auto-created would rehash every key onto a
		// different partition and break per-key ordering — a corruption that no
		// error reports. Leave an existing topic exactly as found, which is also
		// precisely what this path did before it had a partition opinion at all.
		KeepExistingPartitions: true,
		// The name came from Debezium's topic.prefix or from an already-qualified
		// signal topic. Confining it again would create a topic under a name the
		// sink does not subscribe to.
		NameIsAuthoritative: true,
	})
}

// GetAllConsumerGroupsLag retrieves lag information for all consumer groups
func (m *Manager) GetAllConsumerGroupsLag() (map[string]map[string]int64, error) {
	groups, err := m.ListConsumerGroups()
	if err != nil {
		return nil, err
	}

	allLags := make(map[string]map[string]int64)

	for _, groupID := range groups {
		lag, err := m.GetConsumerGroupLag(groupID)
		if err != nil {
			log.WithError(err).WithField("group_id", groupID).Warn("Failed to get lag for consumer group")
			continue
		}
		allLags[groupID] = lag
	}

	return allLags, nil
}

// RestartConsumerGroup stops and restarts a consumer group for a specific topic
// This is used by Sentinel agent for auto-healing closed consumer groups
func (m *Manager) RestartConsumerGroup(topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.WithField("topic", topic).Info("🔄 Restarting consumer group for topic")

	// Check if consumer exists
	consumer, exists := m.consumers[topic]
	if !exists {
		return fmt.Errorf("no consumer found for topic: %s", topic)
	}

	// Close the existing consumer
	if err := consumer.Close(); err != nil {
		log.WithError(err).WithField("topic", topic).Warn("Error closing consumer during restart")
		// Continue anyway - we'll try to create a new one
	}

	// Remove from map
	delete(m.consumers, topic)
	log.WithField("topic", topic).Info("✅ Closed old consumer group")

	// Create new consumer group with same config
	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V3_3_0_0
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest // Continue from where we left off
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}
	saramaConfig.Consumer.Group.Rebalance.Timeout = 60 * time.Second
	saramaConfig.Consumer.MaxProcessingTime = 120 * time.Second
	saramaConfig.Consumer.Fetch.Min = 1
	saramaConfig.Consumer.Fetch.Default = 1024 * 1024
	saramaConfig.Consumer.MaxWaitTime = 500 * time.Millisecond

	// Create new consumer group.
	//
	// This is a second, independent sarama.Config — it copies nothing from the
	// one NewManager builds — so the security settings have to be applied here
	// too, and the brokers come from the already-split list.
	if err := saramaauth.Apply(saramaConfig, m.security); err != nil {
		return fmt.Errorf("failed to apply Kafka security config for %s: %w", topic, err)
	}
	newConsumer, err := sarama.NewConsumerGroup(m.security.Brokers, m.Config.GroupID, saramaConfig)
	if err != nil {
		return fmt.Errorf("failed to create new consumer group for %s: %w", topic, err)
	}

	// Store new consumer
	m.consumers[topic] = newConsumer
	log.WithField("topic", topic).Info("✅ Created new consumer group - restart complete")

	return nil
}

// IsConsumerActive checks if a consumer group is active and not closed
func (m *Manager) IsConsumerActive(topic string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	consumer, exists := m.consumers[topic]
	if !exists {
		return false
	}

	// Check if consumer errors channel is closed (indicates consumer is dead)
	select {
	case _, ok := <-consumer.Errors():
		return ok // If channel is closed, ok will be false
	default:
		return true // Channel is open and no error available
	}
}
