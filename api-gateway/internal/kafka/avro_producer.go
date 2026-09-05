package kafka

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/linkedin/goavro/v2"
	kafkago "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// AvroProducer handles sending Avro-encoded messages to Kafka with Schema Registry
type AvroProducer struct {
	writer              *kafkago.Writer
	tracer              trace.Tracer
	schemaRegistryURL   string
	schemaCache         map[string]int // schema name -> schema ID
	schemaCacheMu       sync.RWMutex
	httpClient          *http.Client
	autoRegisterSchemas bool
	schemas             map[string]string // schema name -> schema JSON

	codecMu    sync.RWMutex
	codecCache map[int]*goavro.Codec // schema id -> compiled codec

	parsedMu    sync.RWMutex
	parsedCache map[int]*avroParsedSchema // schema id -> parsed schema index
}

// AvroConfig holds Avro producer configuration
type AvroConfig struct {
	Brokers             []string
	SchemaRegistryURL   string
	AutoRegisterSchemas bool
}

// DefaultAvroConfig returns default Avro configuration
func DefaultAvroConfig() *AvroConfig {
	schemaRegistry := os.Getenv("SCHEMA_REGISTRY_URL")
	if schemaRegistry == "" {
		schemaRegistry = "http://schema-registry:8080"
	}

	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:29092"
	}

	return &AvroConfig{
		// Split, not wrapped: []string{brokers} turned a customer's
		// "b1:9093,b2:9093" into one unresolvable hostname.
		Brokers:             splitComma(brokers),
		SchemaRegistryURL:   schemaRegistry,
		AutoRegisterSchemas: true,
	}
}

// NewAvroProducer creates a new Avro-enabled Kafka producer
func NewAvroProducer(config *AvroConfig) *AvroProducer {
	return &AvroProducer{
		writer: &kafkago.Writer{
			Addr:                   kafkago.TCP(config.Brokers...),
			Transport:              Transport(config.Brokers),
			Balancer:               &kafkago.LeastBytes{},
			MaxAttempts:            3,
			WriteTimeout:           10 * time.Second,
			AllowAutoTopicCreation: true,
			RequiredAcks:           kafkago.RequireAll,
			Compression:            kafkago.Snappy,
		},
		tracer:              otel.Tracer("avro-kafka-producer"),
		schemaRegistryURL:   config.SchemaRegistryURL,
		schemaCache:         make(map[string]int),
		httpClient:          &http.Client{Timeout: 5 * time.Second},
		autoRegisterSchemas: config.AutoRegisterSchemas,
		schemas:             make(map[string]string),
		codecCache:          make(map[int]*goavro.Codec),
		parsedCache:         make(map[int]*avroParsedSchema),
	}
}

// RegisterSchema registers a schema for a given name
func (p *AvroProducer) RegisterSchema(name, schemaJSON string) {
	p.schemaCacheMu.Lock()
	defer p.schemaCacheMu.Unlock()
	p.schemas[name] = schemaJSON
}

// LoadPipelineSchema loads the pipeline request schema
func (p *AvroProducer) LoadPipelineSchema() {
	// Pipeline request schema - matches shared/avro-schemas/core/pipeline.avsc
	pipelineSchema := `{
		"namespace": "com.rsync.pipeline",
		"type": "record",
		"name": "PipelineRequest",
		"fields": [
			{"name": "trace_id", "type": "string"},
			{"name": "pipeline_id", "type": "string"},
			{"name": "pipeline_name", "type": ["null", "string"], "default": null},
			{"name": "source", "type": {
				"type": "record",
				"name": "ConnectionRef",
				"fields": [
					{"name": "connection_id", "type": "string"},
					{"name": "connector_type", "type": "string"},
					{"name": "config", "type": ["null", {"type": "map", "values": "string"}], "default": null}
				]
			}},
			{"name": "destination", "type": "ConnectionRef"},
			{"name": "sync_mode", "type": {"type": "enum", "name": "SyncMode", "symbols": ["FULL", "INCREMENTAL", "CDC", "APPEND", "UPSERT"]}, "default": "FULL"},
			{"name": "entities", "type": ["null", {"type": "array", "items": {
				"type": "record",
				"name": "EntitySelection",
				"fields": [
					{"name": "name", "type": "string"},
					{"name": "namespace", "type": ["null", "string"], "default": null},
					{"name": "filter", "type": ["null", "string"], "default": null},
					{"name": "columns", "type": ["null", {"type": "array", "items": "string"}], "default": null}
				]
			}}], "default": null},
			{"name": "pii_handling", "type": ["null", {
				"type": "record",
				"name": "PIIConfig",
				"fields": [
					{"name": "mode", "type": {"type": "enum", "name": "PIIMode", "symbols": ["NONE", "DETECT", "MASK", "HASH", "ENCRYPT", "REDACT"]}, "default": "NONE"},
					{"name": "fields", "type": ["null", {"type": "array", "items": "string"}], "default": null}
				]
			}], "default": null},
			{"name": "schedule", "type": ["null", {
				"type": "record",
				"name": "Schedule",
				"fields": [
					{"name": "cron", "type": ["null", "string"], "default": null},
					{"name": "interval_minutes", "type": ["null", "int"], "default": null},
					{"name": "timezone", "type": "string", "default": "UTC"}
				]
			}], "default": null},
			{"name": "metadata", "type": ["null", {"type": "map", "values": "string"}], "default": null},
			{"name": "created_at", "type": "long"},
			{"name": "user_id", "type": ["null", "string"], "default": null}
		]
	}`
	p.RegisterSchema("com.rsync.pipeline.PipelineRequest", pipelineSchema)

	// Intent Task schema - matches backend-orchestrator IntentTask structure
	intentTaskSchema := `{
		"namespace": "com.rsync.agent",
		"type": "record",
		"name": "IntentTask",
		"fields": [
			{"name": "task_id", "type": "string"},
			{"name": "pipeline_id", "type": ["null", "string"], "default": null},
			{"name": "execution_id", "type": ["null", "string"], "default": null},
			{"name": "request", "type": "string"},
			{"name": "user_id", "type": ["null", "string"], "default": null},
			{"name": "session_id", "type": ["null", "string"], "default": null},
			{"name": "context", "type": ["null", {"type": "map", "values": "string"}], "default": null},
			{"name": "history", "type": ["null", {"type": "array", "items": "string"}], "default": null},
			{"name": "timestamp", "type": "string"}
		]
	}`
	p.RegisterSchema("com.rsync.agent.IntentTask", intentTaskSchema)

	// Agent message schema
	agentSchema := `{
		"namespace": "com.rsync.agent",
		"type": "record",
		"name": "AgentMessage",
		"fields": [
			{"name": "trace_id", "type": "string"},
			{"name": "message_id", "type": "string"},
			{"name": "correlation_id", "type": ["null", "string"], "default": null},
			{"name": "source_agent", "type": "string"},
			{"name": "target_agent", "type": ["null", "string"], "default": null},
			{"name": "message_type", "type": {"type": "enum", "name": "MessageType", "symbols": ["REQUEST", "RESPONSE", "EVENT", "ERROR", "HEARTBEAT", "COMMAND"]}},
			{"name": "action", "type": "string"},
			{"name": "payload", "type": ["null", "bytes"], "default": null},
			{"name": "payload_schema", "type": ["null", "string"], "default": null},
			{"name": "status", "type": ["null", {"type": "enum", "name": "Status", "symbols": ["PENDING", "PROCESSING", "SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"]}], "default": null},
			{"name": "error", "type": ["null", {
				"type": "record",
				"name": "ErrorInfo",
				"fields": [
					{"name": "code", "type": "string"},
					{"name": "message", "type": "string"},
					{"name": "details", "type": ["null", "string"], "default": null},
					{"name": "retryable", "type": "boolean", "default": false}
				]
			}], "default": null},
			{"name": "context", "type": ["null", {"type": "map", "values": "string"}], "default": null},
			{"name": "timestamp", "type": "long"},
			{"name": "expires_at", "type": ["null", "long"], "default": null},
			{"name": "priority", "type": "int", "default": 0},
			{"name": "retry_count", "type": "int", "default": 0}
		]
	}`
	p.RegisterSchema("com.rsync.agent.AgentMessage", agentSchema)
}

// SendPipelineRequestAvro sends a pipeline request as Avro
func (p *AvroProducer) SendPipelineRequestAvro(ctx context.Context, topic string, traceID string, request map[string]interface{}) error {
	schemaName := "com.rsync.pipeline.PipelineRequest"

	// Ensure required fields
	if request["trace_id"] == nil {
		request["trace_id"] = traceID
	}
	if request["created_at"] == nil {
		request["created_at"] = time.Now().UnixMilli()
	}

	return p.sendAvro(ctx, topic, schemaName, traceID, request)
}

// SendAgentMessage sends an agent message as Avro
func (p *AvroProducer) SendAgentMessage(ctx context.Context, topic string, traceID string, request map[string]interface{}) error {
	schemaName := "com.rsync.agent.AgentMessage"

	if request["trace_id"] == nil {
		request["trace_id"] = traceID
	}
	if request["timestamp"] == nil {
		request["timestamp"] = time.Now().UnixMilli()
	}

	return p.sendAvro(ctx, topic, schemaName, traceID, request)
}

// SendIntentTask sends an Intent Task message using the proper Avro schema
func (p *AvroProducer) SendIntentTask(ctx context.Context, topic string, traceID string, request map[string]interface{}) error {
	schemaName := "com.rsync.agent.IntentTask"

	// Ensure timestamp is a string (not int64)
	if request["timestamp"] == nil {
		request["timestamp"] = time.Now().Format(time.RFC3339)
	}

	return p.sendAvro(ctx, topic, schemaName, traceID, request)
}

// sendAvro is the core Avro sending function
func (p *AvroProducer) sendAvro(ctx context.Context, topic, schemaName, key string, data map[string]interface{}) error {
	ctx, span := p.tracer.Start(ctx, "kafka.send.avro",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", topic),
			attribute.String("avro.schema", schemaName),
			attribute.String("trace_id", key),
		),
	)
	defer span.End()

	// Get schema
	p.schemaCacheMu.RLock()
	schema, ok := p.schemas[schemaName]
	p.schemaCacheMu.RUnlock()

	if !ok {
		err := fmt.Errorf("schema not found: %s", schemaName)
		span.RecordError(err)
		span.SetStatus(codes.Error, "Schema not found")
		return err
	}

	// Get or register schema ID
	schemaID, err := p.getOrRegisterSchema(topic, schemaName, schema)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Schema registration failed")
		return fmt.Errorf("failed to get schema ID: %w", err)
	}

	// Serialize to Avro wire format
	valueBytes, err := p.serializeWithSchemaID(schemaID, schema, data)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Serialization failed")
		return fmt.Errorf("failed to serialize: %w", err)
	}

	// Inject trace context into Kafka headers
	headers := make([]kafkago.Header, 0)
	propagator := otel.GetTextMapPropagator()
	carrier := make(propagation.MapCarrier)
	propagator.Inject(ctx, carrier)

	for k, v := range carrier {
		headers = append(headers, kafkago.Header{Key: k, Value: []byte(v)})
	}

	// Add Avro-specific headers
	headers = append(headers,
		kafkago.Header{Key: "trace_id", Value: []byte(key)},
		kafkago.Header{Key: "X-Trace-ID", Value: []byte(key)},
		kafkago.Header{Key: "content-type", Value: []byte("application/avro")},
		kafkago.Header{Key: "avro.schema.id", Value: []byte(fmt.Sprintf("%d", schemaID))},
		kafkago.Header{Key: "avro.schema.name", Value: []byte(schemaName)},
	)

	sendCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = p.writer.WriteMessages(sendCtx,
		kafkago.Message{
			Topic:   topic,
			Key:     []byte(key),
			Value:   valueBytes,
			Headers: headers,
		},
	)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to send to Kafka")
		log.Printf("Failed to send Avro message to Kafka topic %s: %v", topic, err)
		return err
	}

	span.SetStatus(codes.Ok, "")
	log.Printf("✓ Sent Avro message to Kafka topic '%s' with trace_id: %s, schema: %s", topic, key, schemaName)
	return nil
}

// getOrRegisterSchema gets or registers a schema with Schema Registry
func (p *AvroProducer) getOrRegisterSchema(topic, schemaName, schema string) (int, error) {
	cacheKey := topic + "-" + schemaName

	// Check cache
	p.schemaCacheMu.RLock()
	if id, ok := p.schemaCache[cacheKey]; ok {
		p.schemaCacheMu.RUnlock()
		return id, nil
	}
	p.schemaCacheMu.RUnlock()

	// Subject name using TopicRecordNameStrategy
	subject := topic + "-" + schemaName

	if p.autoRegisterSchemas {
		// Register with Schema Registry
		id, err := p.registerWithRegistry(subject, schema)
		if err != nil {
			return 0, err
		}

		p.schemaCacheMu.Lock()
		p.schemaCache[cacheKey] = id
		p.schemaCacheMu.Unlock()

		return id, nil
	}

	// If not auto-registering, try to get latest
	id, err := p.getLatestSchemaID(subject)
	if err != nil {
		return 0, err
	}

	p.schemaCacheMu.Lock()
	p.schemaCache[cacheKey] = id
	p.schemaCacheMu.Unlock()

	return id, nil
}

// registerWithRegistry registers a schema with Schema Registry
func (p *AvroProducer) registerWithRegistry(subject, schema string) (int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", p.schemaRegistryURL, subject)

	reqBody := map[string]string{
		"schema":     schema,
		"schemaType": "AVRO",
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("schema registry request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registry error: %s - %s", resp.Status, string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	log.Printf("✓ Registered schema '%s' with ID: %d", subject, result.ID)
	return result.ID, nil
}

// getLatestSchemaID gets the latest schema ID for a subject
func (p *AvroProducer) getLatestSchemaID(subject string) (int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions/latest", p.schemaRegistryURL, subject)

	resp, err := p.httpClient.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("schema registry error: %s - %s", resp.Status, string(body))
	}

	var result struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, err
	}

	return result.ID, nil
}

// serializeWithSchemaID creates Confluent wire format
// Format: [magic byte 0][4-byte schema ID big-endian][Avro binary payload]
//
// NOTE: Previous versions wrote JSON after the header (NOT Avro). This implementation writes
// real Avro binary so schema evolution + type fidelity work end-to-end.
func (p *AvroProducer) serializeWithSchemaID(schemaID int, schema string, data map[string]interface{}) ([]byte, error) {
	codec, err := p.getCodec(schemaID, schema)
	if err != nil {
		return nil, err
	}
	parsed, err := p.getParsedSchema(schemaID, schema)
	if err != nil {
		return nil, err
	}

	native, err := parsed.toNative(data)
	if err != nil {
		return nil, err
	}

	avroBytes, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		return nil, fmt.Errorf("failed to encode avro: %w", err)
	}

	buf := new(bytes.Buffer)
	buf.WriteByte(0) // Magic byte
	if err := binary.Write(buf, binary.BigEndian, int32(schemaID)); err != nil {
		return nil, err
	}
	buf.Write(avroBytes)
	return buf.Bytes(), nil
}

func (p *AvroProducer) getCodec(schemaID int, schema string) (*goavro.Codec, error) {
	p.codecMu.RLock()
	if c, ok := p.codecCache[schemaID]; ok && c != nil {
		p.codecMu.RUnlock()
		return c, nil
	}
	p.codecMu.RUnlock()

	codec, err := goavro.NewCodec(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to compile avro codec: %w", err)
	}

	p.codecMu.Lock()
	p.codecCache[schemaID] = codec
	p.codecMu.Unlock()
	return codec, nil
}

func (p *AvroProducer) getParsedSchema(schemaID int, schema string) (*avroParsedSchema, error) {
	p.parsedMu.RLock()
	if s, ok := p.parsedCache[schemaID]; ok && s != nil {
		p.parsedMu.RUnlock()
		return s, nil
	}
	p.parsedMu.RUnlock()

	ps, err := parseAvroSchema(schema)
	if err != nil {
		return nil, err
	}

	p.parsedMu.Lock()
	p.parsedCache[schemaID] = ps
	p.parsedMu.Unlock()
	return ps, nil
}

type avroParsedSchema struct {
	root interface{}
	// named holds both full and short names to schema nodes (record/enum).
	named map[string]map[string]interface{}
}

func parseAvroSchema(schema string) (*avroParsedSchema, error) {
	var root interface{}
	if err := json.Unmarshal([]byte(schema), &root); err != nil {
		return nil, fmt.Errorf("failed to parse avro schema json: %w", err)
	}
	ps := &avroParsedSchema{
		root:  root,
		named: make(map[string]map[string]interface{}),
	}
	ps.indexNamed(root, "")
	return ps, nil
}

func (s *avroParsedSchema) indexNamed(node interface{}, currentNamespace string) {
	switch n := node.(type) {
	case map[string]interface{}:
		typ, _ := n["type"]
		switch tv := typ.(type) {
		case string:
			if tv == "record" || tv == "enum" {
				name, _ := n["name"].(string)
				ns, _ := n["namespace"].(string)
				if ns == "" {
					ns = currentNamespace
				}
				full := strings.TrimSpace(name)
				if ns != "" && name != "" && !strings.Contains(name, ".") {
					full = ns + "." + name
				}
				if name != "" {
					s.named[name] = n
				}
				if full != "" {
					s.named[full] = n
				}
				if ns != "" {
					currentNamespace = ns
				}
			}
			// Walk children
			if tv == "record" {
				if fields, ok := n["fields"].([]interface{}); ok {
					for _, f := range fields {
						if fm, ok := f.(map[string]interface{}); ok {
							s.indexNamed(fm["type"], currentNamespace)
						}
					}
				}
			} else if tv == "array" {
				s.indexNamed(n["items"], currentNamespace)
			} else if tv == "map" {
				s.indexNamed(n["values"], currentNamespace)
			}
		case map[string]interface{}:
			s.indexNamed(tv, currentNamespace)
		case []interface{}:
			for _, b := range tv {
				s.indexNamed(b, currentNamespace)
			}
		}
	case []interface{}:
		for _, it := range n {
			s.indexNamed(it, currentNamespace)
		}
	}
}

func (s *avroParsedSchema) toNative(value interface{}) (interface{}, error) {
	return s.convert(s.root, value)
}

func (s *avroParsedSchema) convert(schema interface{}, value interface{}) (interface{}, error) {
	switch sch := schema.(type) {
	case string:
		if isPrimitiveType(sch) {
			return coercePrimitive(sch, value)
		}
		// Named type reference
		if def, ok := s.named[sch]; ok {
			return s.convert(def, value)
		}
		// Unknown named type; best-effort pass through
		return value, nil
	case []interface{}:
		// Union
		if value == nil {
			for _, b := range sch {
				if bs, ok := b.(string); ok && bs == "null" {
					return goavro.Union("null", nil), nil
				}
			}
			return nil, fmt.Errorf("union has no null branch")
		}
		for _, b := range sch {
			branchName, branchSchema := unionBranchNameAndSchema(b, s)
			if branchName == "null" {
				continue
			}
			if unionValueMatches(branchSchema, value, s) {
				conv, err := s.convert(branchSchema, value)
				if err != nil {
					return nil, err
				}
				return goavro.Union(branchName, conv), nil
			}
		}
		// Fallback: pick first non-null branch
		for _, b := range sch {
			branchName, branchSchema := unionBranchNameAndSchema(b, s)
			if branchName == "null" {
				continue
			}
			conv, err := s.convert(branchSchema, value)
			if err != nil {
				continue
			}
			return goavro.Union(branchName, conv), nil
		}
		return nil, fmt.Errorf("unable to match union branch for value")
	case map[string]interface{}:
		typAny, ok := sch["type"]
		if !ok {
			return value, nil
		}
		switch t := typAny.(type) {
		case string:
			switch t {
			case "record":
				mv, ok := value.(map[string]interface{})
				if !ok || mv == nil {
					// allow struct-like via json roundtrip
					b, _ := json.Marshal(value)
					_ = json.Unmarshal(b, &mv)
				}
				out := make(map[string]interface{})
				fields, _ := sch["fields"].([]interface{})
				for _, f := range fields {
					fm, ok := f.(map[string]interface{})
					if !ok {
						continue
					}
					fname, _ := fm["name"].(string)
					fschema := fm["type"]
					var fv interface{}
					if mv != nil {
						fv = mv[fname]
					}
					conv, err := s.convert(fschema, fv)
					if err != nil {
						return nil, fmt.Errorf("field %s: %w", fname, err)
					}
					out[fname] = conv
				}
				return out, nil
			case "enum":
				// enums are native strings
				if value == nil {
					return "", nil
				}
				return fmt.Sprint(value), nil
			case "array":
				itemsSchema := sch["items"]
				switch vv := value.(type) {
				case []interface{}:
					out := make([]interface{}, 0, len(vv))
					for _, it := range vv {
						cv, err := s.convert(itemsSchema, it)
						if err != nil {
							return nil, err
						}
						out = append(out, cv)
					}
					return out, nil
				case []string:
					out := make([]interface{}, 0, len(vv))
					for _, it := range vv {
						cv, err := s.convert(itemsSchema, it)
						if err != nil {
							return nil, err
						}
						out = append(out, cv)
					}
					return out, nil
				default:
					return []interface{}{}, nil
				}
			case "map":
				valuesSchema := sch["values"]
				mv, ok := value.(map[string]interface{})
				if !ok || mv == nil {
					// attempt map[string]string
					if ms, ok := value.(map[string]string); ok {
						mv = make(map[string]interface{}, len(ms))
						for k, v := range ms {
							mv[k] = v
						}
					}
				}
				out := make(map[string]interface{})
				for k, v := range mv {
					cv, err := s.convert(valuesSchema, v)
					if err != nil {
						return nil, err
					}
					out[k] = cv
				}
				return out, nil
			default:
				if isPrimitiveType(t) {
					return coercePrimitive(t, value)
				}
				// named type nested in map
				if def, ok := s.named[t]; ok {
					return s.convert(def, value)
				}
				return value, nil
			}
		case []interface{}:
			return s.convert(t, value)
		case map[string]interface{}:
			return s.convert(t, value)
		default:
			return value, nil
		}
	default:
		return value, nil
	}
}

func isPrimitiveType(t string) bool {
	switch t {
	case "null", "boolean", "int", "long", "double", "float", "bytes", "string":
		return true
	default:
		return false
	}
}

func coercePrimitive(t string, value interface{}) (interface{}, error) {
	switch t {
	case "null":
		return nil, nil
	case "string":
		if value == nil {
			return "", nil
		}
		return fmt.Sprint(value), nil
	case "boolean":
		if b, ok := value.(bool); ok {
			return b, nil
		}
		return false, nil
	case "int":
		switch v := value.(type) {
		case int:
			return int32(v), nil
		case int32:
			return v, nil
		case int64:
			return int32(v), nil
		case float64:
			return int32(v), nil
		case json.Number:
			i, _ := v.Int64()
			return int32(i), nil
		case string:
			if v == "" {
				return int32(0), nil
			}
			n, _ := strconv.ParseInt(v, 10, 32)
			return int32(n), nil
		default:
			return int32(0), nil
		}
	case "long":
		switch v := value.(type) {
		case int:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		case float64:
			return int64(v), nil
		case json.Number:
			i, _ := v.Int64()
			return i, nil
		case string:
			if v == "" {
				return int64(0), nil
			}
			n, _ := strconv.ParseInt(v, 10, 64)
			return n, nil
		default:
			return int64(0), nil
		}
	case "double":
		switch v := value.(type) {
		case float64:
			return v, nil
		case float32:
			return float64(v), nil
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		case json.Number:
			f, _ := v.Float64()
			return f, nil
		case string:
			if v == "" {
				return float64(0), nil
			}
			f, _ := strconv.ParseFloat(v, 64)
			return f, nil
		default:
			return float64(0), nil
		}
	case "bytes":
		switch v := value.(type) {
		case []byte:
			return v, nil
		case string:
			// JSON marshaling of []byte produces base64 strings; decode if possible.
			if b, err := base64.StdEncoding.DecodeString(v); err == nil {
				return b, nil
			}
			return []byte(v), nil
		default:
			return []byte{}, nil
		}
	default:
		return value, nil
	}
}

func unionBranchNameAndSchema(branch interface{}, idx *avroParsedSchema) (string, interface{}) {
	switch b := branch.(type) {
	case string:
		if isPrimitiveType(b) {
			return b, b
		}
		if idx != nil {
			if def, ok := idx.named[b]; ok {
				// record/enum name
				if bt, ok := def["type"].(string); ok && (bt == "record" || bt == "enum") {
					if nm, ok := def["name"].(string); ok && nm != "" {
						return nm, def
					}
				}
				return b, def
			}
		}
		return b, b
	case map[string]interface{}:
		if t, ok := b["type"].(string); ok {
			switch t {
			case "record", "enum":
				if nm, ok := b["name"].(string); ok && nm != "" {
					return nm, b
				}
				return t, b
			default:
				return t, b
			}
		}
		return "unknown", b
	default:
		return "unknown", branch
	}
}

func unionValueMatches(branchSchema interface{}, value interface{}, idx *avroParsedSchema) bool {
	switch bs := branchSchema.(type) {
	case string:
		switch bs {
		case "string":
			_, ok := value.(string)
			return ok
		case "boolean":
			_, ok := value.(bool)
			return ok
		case "int", "long", "double", "float":
			switch value.(type) {
			case int, int32, int64, float64, float32, json.Number:
				return true
			default:
				return false
			}
		case "bytes":
			_, okB := value.([]byte)
			_, okS := value.(string)
			return okB || okS
		default:
			if idx != nil {
				_, ok := idx.named[bs]
				return ok
			}
			return false
		}
	case map[string]interface{}:
		t, _ := bs["type"].(string)
		switch t {
		case "record", "map":
			_, ok := value.(map[string]interface{})
			return ok
		case "array":
			switch value.(type) {
			case []interface{}, []string:
				return true
			default:
				return false
			}
		case "enum":
			_, ok := value.(string)
			return ok
		default:
			return true
		}
	default:
		return false
	}
}

// Close closes the Avro producer
func (p *AvroProducer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
