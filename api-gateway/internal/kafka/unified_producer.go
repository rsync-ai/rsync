package kafka

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"time"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// UnifiedProducer handles sending messages in both Avro and JSON formats
// This provides a migration path from JSON to Avro
type UnifiedProducer struct {
	jsonWriter   *kafkago.Writer
	avroProducer *AvroProducer
	tracer       trace.Tracer
	useAvro      bool
	brokers      []string
}

// UnifiedConfig holds unified producer configuration
type UnifiedConfig struct {
	Brokers           []string
	UseAvro           bool
	SchemaRegistryURL string
}

// DefaultUnifiedConfig returns default configuration
func DefaultUnifiedConfig() *UnifiedConfig {
	useAvro := getEnvBool("KAFKA_USE_AVRO", true) // Default to Avro

	schemaRegistry := os.Getenv("SCHEMA_REGISTRY_URL")
	if schemaRegistry == "" {
		schemaRegistry = "http://schema-registry:8080"
	}

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:29092"
	}

	return &UnifiedConfig{
		Brokers:           splitComma(brokers),
		UseAvro:           useAvro,
		SchemaRegistryURL: schemaRegistry,
	}
}

// NewUnifiedProducer creates a new unified producer
func NewUnifiedProducer(brokers []string) *UnifiedProducer {
	config := DefaultUnifiedConfig()
	config.Brokers = brokers

	return NewUnifiedProducerWithConfig(config)
}

// NewUnifiedProducerWithConfig creates a producer with explicit config
func NewUnifiedProducerWithConfig(config *UnifiedConfig) *UnifiedProducer {
	p := &UnifiedProducer{
		tracer:  otel.Tracer("unified-kafka-producer"),
		useAvro: config.UseAvro,
		brokers: config.Brokers,
	}

	// Always create JSON writer as fallback
	p.jsonWriter = &kafkago.Writer{
		Addr:                   kafkago.TCP(config.Brokers...),
		Transport:              Transport(config.Brokers),
		Balancer:               &kafkago.LeastBytes{},
		MaxAttempts:            3,
		WriteTimeout:           10 * time.Second,
		AllowAutoTopicCreation: true,
	}

	// Create Avro producer if enabled
	if config.UseAvro {
		avroConfig := &AvroConfig{
			Brokers:             config.Brokers,
			SchemaRegistryURL:   config.SchemaRegistryURL,
			AutoRegisterSchemas: true,
		}
		p.avroProducer = NewAvroProducer(avroConfig)
		p.avroProducer.LoadPipelineSchema()
		log.Printf("✓ Unified producer initialized with Avro support (Schema Registry: %s)", config.SchemaRegistryURL)
	} else {
		log.Printf("✓ Unified producer initialized with JSON format")
	}

	return p
}

// SendPipelineRequest sends a pipeline request (JSON mode for backward compatibility)
func (p *UnifiedProducer) SendPipelineRequest(topic string, traceID string, request map[string]interface{}) error {
	return p.SendPipelineRequestWithContext(context.Background(), topic, traceID, request)
}

// SendPipelineRequestWithContext sends a pipeline request with context
func (p *UnifiedProducer) SendPipelineRequestWithContext(ctx context.Context, topic string, traceID string, request map[string]interface{}) error {
	if p.useAvro && p.avroProducer != nil {
		return p.avroProducer.SendPipelineRequestAvro(ctx, topic, traceID, request)
	}
	return p.sendJSON(ctx, topic, traceID, request)
}

// SendAgentMessage sends an agent-to-agent message
func (p *UnifiedProducer) SendAgentMessage(ctx context.Context, topic string, traceID string, request map[string]interface{}) error {
	if p.useAvro && p.avroProducer != nil {
		return p.avroProducer.SendAgentMessage(ctx, topic, traceID, request)
	}
	return p.sendJSON(ctx, topic, traceID, request)
}

// SendIntentTask sends an Intent Task using the proper schema
func (p *UnifiedProducer) SendIntentTask(ctx context.Context, topic string, traceID string, request map[string]interface{}) error {
	if p.useAvro && p.avroProducer != nil {
		return p.avroProducer.SendIntentTask(ctx, topic, traceID, request)
	}
	return p.sendJSON(ctx, topic, traceID, request)
}

// sendJSON sends a message using JSON serialization (fallback)
func (p *UnifiedProducer) sendJSON(ctx context.Context, topic string, traceID string, request map[string]interface{}) error {
	ctx, span := p.tracer.Start(ctx, "kafka.send.json",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
			attribute.String("trace_id", traceID),
		),
	)
	defer span.End()

	value, err := json.Marshal(request)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to marshal message")
		return err
	}

	// Inject trace context into headers
	headers := make([]kafkago.Header, 0)
	propagator := otel.GetTextMapPropagator()
	carrier := make(propagation.MapCarrier)
	propagator.Inject(ctx, carrier)

	for k, v := range carrier {
		headers = append(headers, kafkago.Header{Key: k, Value: []byte(v)})
	}

	headers = append(headers,
		kafkago.Header{Key: "trace_id", Value: []byte(traceID)},
		kafkago.Header{Key: "X-Trace-ID", Value: []byte(traceID)},
		kafkago.Header{Key: "content-type", Value: []byte("application/json")},
	)

	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = p.jsonWriter.WriteMessages(sendCtx,
		kafkago.Message{
			Topic:   topic,
			Key:     []byte(traceID),
			Value:   value,
			Headers: headers,
		},
	)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to send to Kafka")
		log.Printf("Failed to send JSON to Kafka topic %s: %v", topic, err)
		return err
	}

	span.SetStatus(codes.Ok, "")
	log.Printf("✓ Sent JSON message to Kafka topic '%s' with trace_id: %s", topic, traceID)
	return nil
}

// IsAvroEnabled returns whether Avro is enabled
func (p *UnifiedProducer) IsAvroEnabled() bool {
	return p.useAvro
}

// Close closes the producer
func (p *UnifiedProducer) Close() error {
	var err error
	if p.jsonWriter != nil {
		if e := p.jsonWriter.Close(); e != nil {
			err = e
		}
	}
	if p.avroProducer != nil {
		if e := p.avroProducer.Close(); e != nil {
			err = e
		}
	}
	return err
}

// Helper functions
func getEnvBool(key string, defaultVal bool) bool {
	if val := os.Getenv(key); val != "" {
		b, err := strconv.ParseBool(val)
		if err == nil {
			return b
		}
	}
	return defaultVal
}

// splitComma splits a bootstrap string into broker addresses.
//
// It delegates to the shared parser rather than splitting here: the hand-rolled
// version dropped empty entries but kept surrounding spaces, so a list written
// as "b1:9092, b2:9092" produced a broker address with a leading space that
// never resolves.
func splitComma(s string) []string {
	return kafkaclient.ParseBrokers(s)
}
