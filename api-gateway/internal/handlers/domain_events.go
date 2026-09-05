package handlers

import (
	"context"
	"encoding/json"
	"github.com/rsync-ai/shared/kafkaclient"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	rsynckafka "api-gateway/internal/kafka"
	"api-gateway/internal/security"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		env := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))

		raw := strings.TrimSpace(os.Getenv("RSYNC_CORS_ORIGINS"))
		allowed := map[string]struct{}{}
		if raw != "" {
			for _, part := range strings.Split(raw, ",") {
				o := strings.TrimSpace(part)
				o = strings.TrimRight(o, "/")
				if o != "" {
					allowed[o] = struct{}{}
				}
			}
		}

		// If no explicit allowlist is configured, allow all only in non-prod.
		if len(allowed) == 0 && env != "production" && env != "prod" {
			return true
		}
		if origin == "" {
			// Non-browser clients may omit Origin. Only allow that in non-prod.
			return env != "production" && env != "prod"
		}

		origin = strings.TrimRight(origin, "/")
		if len(allowed) > 0 {
			_, ok := allowed[origin]
			return ok
		}

		// Prod default: only allow same-host.
		u, err := url.Parse(origin)
		if err != nil || strings.TrimSpace(u.Host) == "" {
			return false
		}
		return strings.EqualFold(strings.TrimSpace(u.Host), strings.TrimSpace(r.Host))
	},
}

// DomainEventManager manages domain event subscriptions
type DomainEventManager struct {
	subscribers   map[string]map[*websocket.Conn]bool // pipeline_id -> connections
	mu            sync.RWMutex
	kafkaConsumer sarama.ConsumerGroup
	cancel        context.CancelFunc
	done          chan struct{}
}

var eventManager *DomainEventManager

// InitDomainEventManager initializes the domain event manager.
// The provided ctx (typically appCtx) controls the consumer loop lifetime.
func InitDomainEventManager(ctx context.Context, kafkaBrokers []string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V3_4_0_0
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Same SASL/TLS and client.id as every other Kafka client in this service.
	// Without it this consumer is silently PLAINTEXT on an authenticated
	// cluster, and the only symptom is NL-pipeline HITL checkpoints that never
	// reach the browser — which presents as "the pipeline hangs forever".
	if err := rsynckafka.ApplySarama(config, kafkaBrokers); err != nil {
		return err
	}

	// Namespaced under the same KAFKA_TOPIC_PREFIX as the topic this group
	// reads (see consumeDomainEvents), so a customer-managed cluster can cover
	// both with one PREFIXED grant. An unqualified group id under a PREFIXED
	// grant does not crash: the broker rejects the JoinGroup and this consumer
	// simply never delivers a HITL checkpoint to the browser.
	consumer, err := sarama.NewConsumerGroup(kafkaBrokers, kafkaclient.Group("api-gateway-domain-events"), config)
	if err != nil {
		return err
	}

	consumerCtx, cancel := context.WithCancel(ctx)
	eventManager = &DomainEventManager{
		subscribers:   make(map[string]map[*websocket.Conn]bool),
		kafkaConsumer: consumer,
		cancel:        cancel,
		done:          make(chan struct{}),
	}

	go eventManager.consumeDomainEvents(consumerCtx)
	return nil
}

// ShutdownDomainEventManager stops the consumer loop and closes the Kafka consumer.
// Call this during graceful shutdown before the process exits.
func ShutdownDomainEventManager() {
	if eventManager == nil {
		return
	}
	eventManager.cancel()
	select {
	case <-eventManager.done:
	case <-time.After(5 * time.Second):
		log.Warn("Timed out waiting for domain event consumer to stop")
	}
	if err := eventManager.kafkaConsumer.Close(); err != nil {
		log.WithError(err).Warn("Error closing domain event Kafka consumer")
	}
}

// SubscribePipelineEvents handles WebSocket connections for pipeline events
// GET /api/v1/pipelines/:pipeline_id/events/stream
//
// Authorization is enforced BEFORE the WebSocket upgrade: without this check any
// authenticated user could stream another user's pipeline events (config,
// schemas, error messages) simply by guessing the pipeline UUID.
//
// The gate is the ACTIVE workspace, not created_by. A creator-only check let the
// creator hold an open event stream on a pipeline belonging to a workspace they
// had since switched away from — the stream keeps pushing after the switch, so
// it leaks for as long as the socket stays open — while denying a teammate in
// the pipeline's own workspace the live view of a run they share.
func SubscribePipelineEvents(c *gin.Context) {
	pipelineID, ok := requireUUIDParam(c, "id", "invalid_pipeline_id", "Invalid pipeline ID format")
	if !ok {
		return
	}

	if _, ok := requirePipelineWorkspaceRole(c, pipelineID, security.WSViewer); !ok {
		return
	}

	// Upgrade to WebSocket only after authorization is proven.
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("Failed to upgrade to WebSocket: %v", err)
		return
	}
	defer conn.Close()

	// Register subscriber
	eventManager.subscribe(pipelineID, conn)
	defer eventManager.unsubscribe(pipelineID, conn)

	log.Printf("✅ WebSocket connected for pipeline: %s", pipelineID)

	// Send initial connection confirmation
	conn.WriteJSON(map[string]interface{}{
		"type":        "connected",
		"pipeline_id": pipelineID,
		"timestamp":   time.Now(),
	})

	// Keep connection alive and handle pings
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error for pipeline %s: %v", pipelineID, err)
			}
			break
		}
	}

	log.Printf("❌ WebSocket disconnected for pipeline: %s", pipelineID)
}

// subscribe adds a WebSocket connection to pipeline subscribers
func (m *DomainEventManager) subscribe(pipelineID string, conn *websocket.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.subscribers[pipelineID] == nil {
		m.subscribers[pipelineID] = make(map[*websocket.Conn]bool)
	}
	m.subscribers[pipelineID][conn] = true
}

// unsubscribe removes a WebSocket connection from pipeline subscribers
func (m *DomainEventManager) unsubscribe(pipelineID string, conn *websocket.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if subscribers, ok := m.subscribers[pipelineID]; ok {
		delete(subscribers, conn)
		if len(subscribers) == 0 {
			delete(m.subscribers, pipelineID)
		}
	}
}

// consumeDomainEvents consumes domain events from Kafka and broadcasts to subscribers.
// Exits cleanly when ctx is cancelled (e.g. on SIGTERM via appCancel).
func (m *DomainEventManager) consumeDomainEvents(ctx context.Context) {
	defer close(m.done)
	handler := &domainEventHandler{manager: m}

	for {
		if err := ctx.Err(); err != nil {
			log.Info("Domain event consumer stopping (context cancelled)")
			return
		}
		err := m.kafkaConsumer.Consume(ctx, kafkaclient.Topics("pipeline.domain.events"), handler)
		if err != nil {
			if ctx.Err() != nil {
				return // clean shutdown
			}
			log.WithError(err).Error("Domain event consumer error — retrying in 5s")
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

// domainEventHandler implements sarama.ConsumerGroupHandler
type domainEventHandler struct {
	manager *DomainEventManager
}

func (h *domainEventHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *domainEventHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *domainEventHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		var event map[string]interface{}
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("Failed to unmarshal domain event: %v", err)
			session.MarkMessage(message, "")
			continue
		}

		// Extract pipeline_id from event
		pipelineID, ok := event["pipeline_id"].(string)
		if !ok {
			log.Printf("Domain event missing pipeline_id: %v", event)
			session.MarkMessage(message, "")
			continue
		}

		log.Printf("📡 Received domain event for pipeline %s: %s", pipelineID, event["event_type"])

		// Broadcast to all subscribers for this pipeline
		h.manager.broadcast(pipelineID, event)

		session.MarkMessage(message, "")
	}
	return nil
}

// broadcast sends an event to all subscribers of a pipeline
func (m *DomainEventManager) broadcast(pipelineID string, event map[string]interface{}) {
	m.mu.RLock()
	subscribers, ok := m.subscribers[pipelineID]
	m.mu.RUnlock()

	if !ok || len(subscribers) == 0 {
		return
	}

	// Send to all subscribers
	for conn := range subscribers {
		err := conn.WriteJSON(event)
		if err != nil {
			log.Printf("Failed to send event to WebSocket: %v", err)
			// Connection is broken, will be cleaned up by ReadMessage loop
		}
	}
}
