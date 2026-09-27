package websocket

import (
	"context"
	"encoding/json"
	"github.com/rsync-ai/shared/kafkaclient"
	log "github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"

	rsynckafka "api-gateway/internal/kafka"
	"api-gateway/internal/security"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// KafkaBridge bridges Kafka events to WebSocket clients
type KafkaBridge struct {
	hub       *Hub
	brokers   []string
	consumers map[string]*kafka.Reader
	mu        sync.RWMutex // Protects consumers map from concurrent access
	ctx       context.Context
	cancel    context.CancelFunc
	tracer    trace.Tracer
}

// NewKafkaBridge creates a new Kafka to WebSocket bridge
func NewKafkaBridge(hub *Hub, brokers []string) *KafkaBridge {
	ctx, cancel := context.WithCancel(context.Background())
	return &KafkaBridge{
		hub:       hub,
		brokers:   brokers,
		consumers: make(map[string]*kafka.Reader),
		ctx:       ctx,
		cancel:    cancel,
		tracer:    otel.Tracer("kafka-ws-bridge"),
	}
}

// Start begins consuming from Kafka topics and bridging to WebSocket
func (b *KafkaBridge) Start() {
	topics := bridgeTopics()
	for _, topic := range topics {
		go b.consumeTopic(topic)
	}

	log.Printf("✅ Kafka-WebSocket bridge started (consuming %d topic(s): %v)", len(topics), topics)
}

// bridgeTopics is the complete list of topics the bridge forwards to WebSocket
// clients, already qualified with KAFKA_TOPIC_PREFIX.
//
// Every name here MUST have an in-repo producer AND an in-repo creator.
// pipeline.domain.events is the only one that does: backend-temporal-adapter
// produces it (workflows/activities.go) and every topic creator provisions it.
//
// The bridge used to subscribe to pipeline.agent.telemetry and to the
// agent.planner/agent.executor response topics as well, and before that to
// eleven more agent.*, task.* and *.status.updates names. They are gone: the
// agent control plane was removed, so either nothing produced to them or the
// only reader of what was produced was this debug relay. Each subscription was
// one more topic auto-created on first join (the kafka-go reader creates what it
// subscribes to), one more consumer group to grant ACLs for, and on a broker with
// auto.create.topics.enable off a permanent UNKNOWN_TOPIC_OR_PARTITION retry
// loop. If a new producer lands, add its topic here AND to every topic creator in
// the same change.
func bridgeTopics() []string {
	return kafkaclient.Topics("pipeline.domain.events")
}

// bridgeGroupID is the consumer group id this bridge joins for one topic.
//
// The bridge runs one group per topic, and like every other group the platform
// joins it is namespaced under KAFKA_TOPIC_PREFIX, so a customer's operator can
// cover every group this service uses with one PREFIXED grant rather than
// enumerating them. An unqualified id under a PREFIXED grant does not crash:
// the broker refuses the JoinGroup and the WebSocket simply goes quiet.
//
// The prefix is trimmed back off the topic before Group() re-applies it,
// because topic arrives here already qualified (Start builds the list with
// kafkaclient.Topics). Without the trim the id reads
// "rsync.websocket-bridge-rsync.pipeline.domain.events" — still correct for
// ACLs, but unreadable in a `kafka-consumer-groups --list`, which is half of
// what an owned namespace is for.
//
// With KAFKA_TOPIC_PREFIX="" both the trim and Group() are no-ops, so the
// migration lever yields exactly the pre-namespacing id and the bridge keeps
// its committed offsets.
func bridgeGroupID(topic string) string {
	return kafkaclient.Group("websocket-bridge-" + strings.TrimPrefix(topic, kafkaclient.TopicPrefix()))
}

// consumeTopic consumes messages from a single topic
func (b *KafkaBridge) consumeTopic(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        b.brokers,
		Dialer:         rsynckafka.Dialer(b.brokers),
		Topic:          topic,
		GroupID:        bridgeGroupID(topic),
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        3 * time.Second,
		StartOffset:    kafka.LastOffset,
		CommitInterval: time.Second,
	})

	// Thread-safe map write
	b.mu.Lock()
	b.consumers[topic] = reader
	b.mu.Unlock()

	defer reader.Close()

	log.Printf("📡 Kafka bridge: Consuming from topic '%s'", topic)

	for {
		select {
		case <-b.ctx.Done():
			log.Printf("Kafka bridge: Stopping consumer for '%s'", topic)
			return
		default:
			msg, err := reader.ReadMessage(b.ctx)
			if err != nil {
				if b.ctx.Err() != nil {
					return // Context cancelled
				}
				// Log and continue on temporary errors
				time.Sleep(time.Second)
				continue
			}

			b.processMessage(topic, msg)
		}
	}
}

// processMessage processes a Kafka message and broadcasts to WebSocket
func (b *KafkaBridge) processMessage(topic string, msg kafka.Message) {
	// TRACE FIX: Extract full W3C TraceContext from Kafka headers
	// This links the bridge span to the original trace tree
	carrier := extractKafkaHeaders(msg.Headers)
	propagator := otel.GetTextMapPropagator()
	ctx := propagator.Extract(b.ctx, propagation.MapCarrier(carrier))

	// Create a span for the bridge processing
	_, span := b.tracer.Start(ctx, "bridge.forward."+topic,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
			attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
			attribute.Int64("messaging.kafka.offset", msg.Offset),
		),
	)
	defer span.End()

	// Determine trace ID for WebSocket events:
	// - Prefer real W3C trace context when present (traceparent/tracestate).
	// - Otherwise, use the explicit trace_id header (stable correlation id for traces/logs).
	// - Finally, fall back to the new span's trace ID.
	traceID := span.SpanContext().TraceID().String()
	hasW3C := carrier["traceparent"] != "" || carrier["tracestate"] != ""
	if !hasW3C {
		if customTraceID, ok := carrier["trace_id"]; ok && customTraceID != "" {
			if sanitized, ok := sanitizeTraceID(customTraceID); ok {
				traceID = sanitized
			}
		}
	}

	// ============================================================================
	// NEW ARCHITECTURE: Handle pipeline.domain.events (primary source of truth)
	// ============================================================================
	if topic == kafkaclient.Topic("pipeline.domain.events") {
		var domainEvent map[string]interface{}
		if err := json.Unmarshal(msg.Value, &domainEvent); err != nil {
			log.Printf("Failed to parse domain event: %v", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to parse domain event")
			return
		}

		pipelineID, _ := domainEvent["pipeline_id"].(string)
		if pipelineID == "" && len(msg.Key) > 0 {
			pipelineID = string(msg.Key)
		}

		eventType, _ := domainEvent["event_type"].(string)
		stage, _ := domainEvent["stage"].(string)
		stageGroup, _ := domainEvent["stage_group"].(string)

		// Redact sensitive data before broadcasting to any UI client.
		// This prevents leaking secrets (tokens/passwords) via event payloads.
		redacted := security.RedactMap(domainEvent)

		// Broadcast as domain_event (new format)
		b.hub.BroadcastDomainEvent(pipelineID, redacted, traceID)

		// Additionally, emit data_plane_metrics for fast UI counters.
		// This is additive (clients can ignore) and keeps payload shape stable.
		if strings.EqualFold(eventType, "DATA_PLANE_METRICS") {
			b.hub.BroadcastDataPlaneMetrics(map[string]interface{}{
				"pipeline_id":  pipelineID,
				"event_type":   eventType,
				"execution_id": redacted["execution_id"],
				"trace_id":     redacted["trace_id"],
				"data":         redacted,
			}, traceID)
		}

		span.SetStatus(codes.Ok, "")
		displayPipeline := pipelineID
		if len(displayPipeline) > 8 {
			displayPipeline = displayPipeline[:8]
		}
		log.Printf("🎯 Kafka→WS: domain event (type=%s, stage=%s, group=%s, pipeline=%s)",
			eventType, stage, stageGroup, displayPipeline)
		return
	}

	// bridgeTopics subscribes to nothing else, so this is unreachable today. It is
	// kept as a loud guard rather than a silent drop: a topic added to bridgeTopics
	// without a handler here would otherwise vanish without a trace.
	log.Printf("Kafka bridge: no handler for topic %q, message dropped", topic)
	span.SetStatus(codes.Error, "no handler for topic")
}

// Stop stops the Kafka bridge
func (b *KafkaBridge) Stop() {
	b.cancel()

	for topic, reader := range b.consumers {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing Kafka consumer for %s: %v", topic, err)
		}
	}

	log.Println("Kafka-WebSocket bridge stopped")
}

// Helper functions

// extractKafkaHeaders converts Kafka headers to a map for trace propagation
func extractKafkaHeaders(headers []kafka.Header) map[string]string {
	result := make(map[string]string)
	for _, h := range headers {
		result[h.Key] = string(h.Value)
	}
	return result
}

func sanitizeTraceID(raw string) (string, bool) {
	s := strings.ToLower(strings.TrimSpace(raw))
	// Limit length to reduce header abuse.
	if len(s) == 0 || len(s) > 64 {
		return "", false
	}
	// Prefer OpenTelemetry/W3C trace-id shape: 16-byte (32 hex) lowercase.
	if len(s) != 32 {
		return "", false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') {
			continue
		}
		return "", false
	}
	return s, true
}
