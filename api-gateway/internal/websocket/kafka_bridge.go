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

// AgentEvent represents an event from the agent system
type AgentEvent struct {
	TraceID    string                 `json:"trace_id"`
	Agent      string                 `json:"agent"`
	Status     string                 `json:"status"`
	PipelineID string                 `json:"pipeline_id,omitempty"`
	TaskID     string                 `json:"task_id,omitempty"`
	Result     map[string]interface{} `json:"result,omitempty"`
	Error      string                 `json:"error,omitempty"`
	Timestamp  string                 `json:"timestamp"`
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
	// Topics to consume for real-time updates.
	//
	// Every name here MUST have an in-repo producer AND an in-repo creator. These four
	// are the complete set that does:
	//
	//	pipeline.domain.events    backend-temporal-adapter workflows/activities.go
	//	pipeline.agent.telemetry  orchestrator workers/{intent,planner}.go emitTelemetry
	//	agent.planner.responses   llm-service planner/kafka_consumer.py OUTPUT_TOPIC
	//	agent.executor.responses  orchestrator agents/executor/executor.go
	//
	// all four created by EnsureAgentControlTopics (orchestrator internal/kafka/topology.go).
	//
	// Eleven more used to be listed here — task.results, agent.intent.responses,
	// agent.{resolver,discovery,planner}.{responses,progress}, agent.orchestrator.progress,
	// agent.validator.responses, pipeline.status.updates and cdc.status.updates. A
	// repo-wide search finds no producer for any of them, in any language. Subscribing
	// to a topic nobody writes is indistinguishable from a channel that is broken, so
	// their silence was never diagnosable; and on a broker with auto.create.topics.enable
	// off — the setting a customer-managed cluster usually ships — each one is also a
	// permanent UNKNOWN_TOPIC_OR_PARTITION retry loop and one more consumer group to
	// grant ACLs for. If a producer for any of them lands later, re-add the name here
	// AND to EnsureAgentControlTopics in the same change.
	topics := kafkaclient.Topics(
		"pipeline.domain.events",
		"pipeline.agent.telemetry", // Optional, for debug mode only
		"agent.planner.responses",
		"agent.executor.responses",
	)

	for _, topic := range topics {
		go b.consumeTopic(topic)
	}

	log.Printf("✅ Kafka-WebSocket bridge started (consuming %d topics, including pipeline.domain.events)", len(topics))
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
	// - Otherwise, use the explicit trace_id header (stable correlation id for SigNoz/logs).
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

	// ============================================================================
	// NEW ARCHITECTURE: Handle pipeline.agent.telemetry (optional, debug only)
	// ============================================================================
	if topic == kafkaclient.Topic("pipeline.agent.telemetry") {
		var telemetryEvent map[string]interface{}
		if err := json.Unmarshal(msg.Value, &telemetryEvent); err != nil {
			log.Printf("Failed to parse telemetry event: %v", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to parse telemetry event")
			return
		}

		pipelineID, _ := telemetryEvent["pipeline_id"].(string)
		if pipelineID == "" && len(msg.Key) > 0 {
			pipelineID = string(msg.Key)
		}

		// Even though telemetry is debug-only, still redact to avoid accidental secret exposure.
		redacted := security.RedactMap(telemetryEvent)

		// Broadcast as telemetry_event (for debug/developer consoles only)
		b.hub.BroadcastTelemetryEvent(pipelineID, redacted, traceID)

		span.SetStatus(codes.Ok, "")
		return // Don't log telemetry events (too noisy)
	}
	// ============================================================================

	// REMOVED: the agent.resolver.progress and agent.orchestrator.progress special
	// cases used to live here. Both topics were unsubscribed in Start() because no
	// component in this repo produces to them, in any language — the two handlers were
	// therefore unreachable, and had been for as long as they were their topics' only
	// reader. Deleting them keeps the surviving handlers honest: what remains here is
	// exactly the set of message shapes that can actually arrive.

	// Parse standard agent response messages
	var event AgentEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		log.Printf("Failed to parse Kafka message from %s: %v", topic, err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to parse message")
		return
	}

	// If pipeline_id is missing, fall back to Kafka key (we key messages by pipeline_id in several producers)
	if event.PipelineID == "" && len(msg.Key) > 0 {
		event.PipelineID = string(msg.Key)
	}

	// Use trace ID from event if still empty
	if traceID == "" || traceID == "00000000000000000000000000000000" {
		traceID = event.TraceID
	}

	// Normalize agent name (and recover missing agent from topic)
	event.Agent = normalizeAgentName(event.Agent, topic)
	// Normalize status into UI-friendly agent status
	normalizedAgentStatus := normalizeAgentStatus(event.Status, event.Error)

	// Add event attributes to span
	span.SetAttributes(
		attribute.String("agent.name", event.Agent),
		attribute.String("agent.status", event.Status),
		attribute.String("trace_id", traceID),
	)
	if event.PipelineID != "" {
		span.SetAttributes(attribute.String("pipeline.id", event.PipelineID))
	}

	// Determine event type based on topic and content
	switch {
	case strings.Contains(topic, "cdc"):
		b.hub.BroadcastCDCUpdate(
			event.TaskID, // Use task ID as connector name for CDC
			event.Status,
			event.Result,
		)

	case event.Agent != "":
		// Always emit agent_activity for agent events so the UI can render stage-by-stage progress
		// Extract message from result
		resultMessage := getResultMessage(event)

		b.hub.BroadcastAgentActivity(
			event.Agent,
			normalizedAgentStatus,
			event.PipelineID,
			map[string]interface{}{
				"task_id":    event.TaskID,
				"error":      event.Error,
				"message":    resultMessage,
				"trace_id":   traceID, // Include trace_id in agent activity for correlation
				"data":       event.Result,
				"event_type": determineEventType(event.Status, event.Error), // Add event type for frontend state updates
			},
			traceID,
		)

		// Also emit pipeline_status for pipeline-scoped states (esp. needs_connection) with richer data.
		if event.PipelineID != "" {
			// Base payload
			payload := map[string]interface{}{
				"pipeline_id": event.PipelineID,
				"status":      event.Status,
				"message":     resultMessage,
			}

			// If agent provided structured result (e.g. resolver needs_connection), forward it for UI actions
			for k, v := range event.Result {
				payload[k] = v
			}

			b.hub.BroadcastEvent(EventPipelineStatus, payload, traceID)

			// Preferred: emit normalized pipeline_progress for stable frontend UI.
			stage := stageFromAgentOrTopic(event.Agent, topic)
			req := extractRequirements(event.Status, event.Result)
			b.hub.BroadcastPipelineProgress(map[string]interface{}{
				"pipeline_id":  event.PipelineID,
				"stage":        stage,
				"status":       pipelineStatusFromRaw(event.Status, event.Error),
				"agent":        event.Agent,
				"agent_status": normalizedAgentStatus,
				"message":      resultMessage,
				"requirements": req,
				"details":      payload, // include merged fields for debugging/UX
			}, traceID)
		}

	default:
		// Generic broadcast
		b.hub.BroadcastEvent(EventSystemHealth, map[string]interface{}{
			"topic":   topic,
			"status":  event.Status,
			"message": string(msg.Value),
		}, traceID)
	}

	span.SetStatus(codes.Ok, "")

	// Safe logging with trace ID truncation
	displayTraceID := traceID
	if len(displayTraceID) > 8 {
		displayTraceID = displayTraceID[:8]
	}
	log.Printf("🔄 Kafka→WS: %s event (agent=%s, status=%s, trace=%s)",
		topic, event.Agent, event.Status, displayTraceID)
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

func getResultMessage(event AgentEvent) string {
	if event.Error != "" {
		return event.Error
	}
	if msg, ok := event.Result["message"].(string); ok {
		return msg
	}
	// Intent agent usually returns structured intent without a message. Emit a user-friendly line
	// that doesn't incorrectly claim we're already checking connections.
	if strings.ToLower(strings.TrimSpace(event.Agent)) == "intent" {
		if intentAny, ok := event.Result["intent"]; ok && intentAny != nil {
			if m, ok := intentAny.(map[string]interface{}); ok {
				action, _ := m["action"].(string)
				src, _ := m["source_category"].(string)
				dst, _ := m["destination_category"].(string)
				if action == "" {
					action = "sync"
				}
				if src != "" && dst != "" {
					return "✅ Understood your request. Preparing connectors and validating setup..."
				}
			}
			return "✅ Understood your request. Preparing connectors and validating setup..."
		}
	}
	return event.Status
}

func normalizeAgentName(agent string, topic string) string {
	a := strings.TrimSpace(agent)
	if a == "" {
		// Recover missing agent name from topic
		switch {
		case strings.Contains(topic, "agent.intent"):
			a = "intent"
		case strings.Contains(topic, "agent.resolver"):
			a = "resolver"
		case strings.Contains(topic, "agent.discovery"):
			a = "discovery"
		case strings.Contains(topic, "agent.planner"):
			a = "planner"
		case strings.Contains(topic, "agent.validator"):
			a = "validator"
		case strings.Contains(topic, "agent.executor"):
			a = "executor"
		}
	}
	a = strings.ToLower(a)
	a = strings.TrimSuffix(a, "_agent")
	a = strings.TrimSuffix(a, " agent")
	return a
}

func normalizeAgentStatus(status string, errMsg string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	if errMsg != "" || s == "error" || s == "failed" || s == "failure" {
		return "error"
	}
	switch s {
	case "success", "completed", "done", "valid":
		return "success"
	case "processing", "progress", "running", "checking", "generating", "deploying":
		return "processing"
	case "needs_connection", "needs_confirmation", "needs_tables", "needs_pii_handling", "action_required":
		return "waiting"
	default:
		// Unknown statuses should not keep spinners running forever
		return "processing"
	}
}

func stageFromAgentOrTopic(agent string, topic string) string {
	a := strings.ToLower(agent)
	switch a {
	case "intent":
		return "understanding"
	case "resolver":
		return "connecting"
	case "discovery":
		return "discovering"
	case "planner":
		return "planning"
	case "validator":
		return "validating"
	case "executor":
		return "executing"
	default:
		// fallback from topic name
		switch {
		case strings.Contains(topic, "intent"):
			return "understanding"
		case strings.Contains(topic, "resolver"):
			return "connecting"
		case strings.Contains(topic, "discovery"):
			return "discovering"
		case strings.Contains(topic, "planner"):
			return "planning"
		case strings.Contains(topic, "validator"):
			return "validating"
		case strings.Contains(topic, "executor"):
			return "executing"
		}
	}
	return "understanding"
}

func pipelineStatusFromRaw(rawStatus string, errMsg string) string {
	s := strings.ToLower(strings.TrimSpace(rawStatus))
	if errMsg != "" || s == "error" || s == "failed" {
		return "error"
	}
	switch s {
	case "needs_connection", "needs_confirmation", "needs_tables", "needs_pii_handling", "action_required":
		return "blocked"
	case "success", "completed", "done", "valid":
		return "completed"
	default:
		return "running"
	}
}

func extractRequirements(rawStatus string, result map[string]interface{}) map[string]interface{} {
	s := strings.ToLower(strings.TrimSpace(rawStatus))
	if !strings.HasPrefix(s, "needs_") && s != "action_required" {
		return nil
	}
	req := map[string]interface{}{
		"type": s,
	}
	// Common fields (best-effort)
	if v, ok := result["missing"]; ok {
		req["missing"] = v
	}
	if v, ok := result["connector_type"]; ok {
		req["connector_type"] = v
	}
	if v, ok := result["source_type"]; ok {
		req["source_type"] = v
	}
	if v, ok := result["destination_type"]; ok {
		req["destination_type"] = v
	}
	if v, ok := result["direction"]; ok {
		req["direction"] = v
	}
	return req
}

// determineEventType determines the event type for frontend state updates
func determineEventType(status string, errMsg string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	if errMsg != "" || s == "failed" || s == "error" {
		return "error"
	}
	if s == "success" || s == "completed" || s == "valid" || s == "done" {
		return "agent_complete"
	}
	if s == "processing" || s == "running" || s == "checking" || s == "generating" || s == "deploying" {
		return "agent_start"
	}
	// "needs_input" should show as progress but not complete
	if s == "needs_input" {
		return "progress"
	}
	return "progress"
}
