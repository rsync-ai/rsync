package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	log "github.com/sirupsen/logrus"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Consumer handles reading messages from one or more Kafka topics.
// Each topic gets its own kafka.Reader sharing the same consumer group ID.
type Consumer struct {
	readers []*kafka.Reader
	tracer  trace.Tracer
}

// AgentResponse represents a response from an agent
type AgentResponse struct {
	TraceID       string                 `json:"trace_id"`
	PipelineID    string                 `json:"pipeline_id,omitempty"`
	CorrelationID string                 `json:"correlation_id,omitempty"`
	Status        string                 `json:"status"`
	Agent         string                 `json:"agent"`
	Result        map[string]interface{} `json:"result,omitempty"`
	Error         string                 `json:"error,omitempty"`
	Timestamp     string                 `json:"timestamp"`
}

// MessageHandler is a function that processes incoming messages
// Now includes context for trace propagation
type MessageHandler func(ctx context.Context, response AgentResponse) error

// MessageHandlerLegacy is the old handler signature for backwards compatibility
type MessageHandlerLegacy func(AgentResponse) error

// NewConsumer creates a new Kafka consumer that reads from all provided topics.
// A separate kafka.Reader (sharing the same GroupID) is created per topic so
// that every topic receives messages — kafka-go's Reader only supports a single
// Topic field, not a topic list.
//
// The caller passes a LOGICAL group id ("api-gateway-consumer-group"); the
// namespace is applied here, not at the call site. Putting it at the
// constructor is the point: a future caller that adds a fourth consumer, or
// that derives its group id from configuration, is namespaced by construction
// rather than by remembering to be.
//
// DECISION — an operator-supplied group id is namespaced too.
//
// Nothing in api-gateway reads a group id from the environment today (the
// orchestrator's KAFKA_GROUP_ID is the platform's one example), but this
// function is where such a value would arrive, so the rule is settled here
// rather than the first time someone adds the variable.
//
// The rule is: qualify it. The whole reason Group() exists is that a customer
// granting ACLs on a shared cluster wants ONE PREFIXED grant to cover every
// group this product joins. A group id that skips qualification because it came
// from the environment is exactly the one that falls outside that grant, and
// Kafka does not fail loudly for it — the broker answers the JoinGroup with an
// authorization error and the consumer simply never receives a record. A
// half-covered grant is worse than none, because the deployment looks healthy.
//
// The argument against — an operator who typed KAFKA_GROUP_ID=acme-ingest may
// not expect to see rsync.acme-ingest on the broker — is real but is answered
// by the same lever topics use: setting KAFKA_TOPIC_PREFIX="" disables
// qualification for topics AND groups together, so an operator who genuinely
// wants their exact string has a supported way to get it, and gets it
// consistently across both instead of a group id that silently disagrees with
// the topics it reads. Qualification is idempotent, so an operator who spells
// the prefix into the variable themselves is not double-prefixed either.
func NewConsumer(brokers []string, topics []string, groupID string) *Consumer {
	if len(topics) == 0 {
		log.Fatalf("No topics provided to Kafka consumer")
	}

	// One dialer, shared by every per-topic Reader: it carries the SASL/TLS
	// this service needs to reach a customer-managed cluster.
	dialer := Dialer(brokers)

	// Resolved once and logged, so the startup line reports the id the broker
	// will actually see rather than the logical name the caller wrote.
	group := kafkaclient.Group(groupID)

	readers := make([]*kafka.Reader, 0, len(topics))
	for _, topic := range topics {
		cfg := kafka.ReaderConfig{
			Brokers:        brokers,
			Dialer:         dialer,
			GroupID:        group,
			Topic:          topic,
			MinBytes:       10e3, // 10KB
			MaxBytes:       10e6, // 10MB
			CommitInterval: time.Second,
			StartOffset:    kafka.LastOffset,
		}
		readers = append(readers, kafka.NewReader(cfg))
		log.Printf("📡 Kafka reader registered for topic: %s (GroupID: %s)", topic, group)
	}

	return &Consumer{
		readers: readers,
		tracer:  otel.Tracer("kafka-consumer"),
	}
}

// Start begins consuming messages from all registered topics concurrently.
// Each topic is handled by a dedicated goroutine; all goroutines share the
// same handler function and are cancelled together via ctx.
func (c *Consumer) Start(ctx context.Context, handler MessageHandler) {
	log.Printf("Kafka consumer started, listening on %d topic(s)...", len(c.readers))
	propagator := otel.GetTextMapPropagator()

	var wg sync.WaitGroup
	for _, r := range c.readers {
		wg.Add(1)
		go func(reader *kafka.Reader) {
			defer wg.Done()
			c.readLoop(ctx, reader, handler, propagator)
		}(r)
	}
	wg.Wait()
}

// readLoop is the per-reader consume loop.
func (c *Consumer) readLoop(ctx context.Context, reader *kafka.Reader, handler MessageHandler, propagator propagation.TextMapPropagator) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("Consumer context cancelled, stopping reader for topic: %s", reader.Config().Topic)
			return
		default:
			readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			msg, err := reader.ReadMessage(readCtx)
			cancel()

			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
					continue
				}
				log.Printf("Error reading message from topic %s: %v", reader.Config().Topic, err)
				time.Sleep(time.Second)
				continue
			}

			carrier := extractHeadersToMap(msg.Headers)
			msgCtx := propagator.Extract(ctx, propagation.MapCarrier(carrier))

			msgCtx, span := c.tracer.Start(msgCtx, "kafka.consume.agent-response",
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					attribute.String("messaging.system", "kafka"),
					attribute.String("messaging.destination", msg.Topic),
					attribute.Int64("messaging.kafka.partition", int64(msg.Partition)),
					attribute.Int64("messaging.kafka.offset", msg.Offset),
				),
			)

			var response AgentResponse
			if err := json.Unmarshal(msg.Value, &response); err != nil {
				log.Printf("Error unmarshaling message from topic %s: %v", msg.Topic, err)
				span.RecordError(err)
				span.SetStatus(codes.Error, "Failed to unmarshal message")
				span.End()
				continue
			}

			span.SetAttributes(
				attribute.String("agent.name", response.Agent),
				attribute.String("agent.status", response.Status),
				attribute.String("trace_id", response.TraceID),
			)
			if response.PipelineID != "" {
				span.SetAttributes(attribute.String("pipeline.id", response.PipelineID))
			}

			log.Printf("Received response from agent '%s' for trace_id '%s' (status: %s, topic: %s)",
				response.Agent, response.TraceID, response.Status, msg.Topic)

			if err := handler(msgCtx, response); err != nil {
				log.Printf("Error handling message from topic %s: %v", msg.Topic, err)
				span.RecordError(err)
				span.SetStatus(codes.Error, "Handler error")
			} else {
				span.SetStatus(codes.Ok, "")
			}

			span.End()
		}
	}
}

// extractHeadersToMap converts Kafka headers to a map for trace propagation
func extractHeadersToMap(headers []kafka.Header) map[string]string {
	result := make(map[string]string)
	for _, h := range headers {
		result[h.Key] = string(h.Value)
	}
	return result
}

// Close closes all Kafka readers managed by this consumer.
func (c *Consumer) Close() error {
	var firstErr error
	for _, r := range c.readers {
		if err := r.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
