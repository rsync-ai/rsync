package websocket

import (
	"context"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"api-gateway/internal/db"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		origin := strings.TrimSpace(r.Header.Get("Origin"))
		if origin == "" {
			// Non-browser clients may omit Origin. Only allow that in non-prod.
			return isNonProductionEnv()
		}

		allowed := allowedOriginsFromEnv()
		if len(allowed) == 0 {
			// If no explicit allowlist is configured:
			// - allow in non-prod
			// - otherwise only allow same-host origins
			if isNonProductionEnv() {
				return true
			}
			return isSameHostOrigin(origin, r)
		}
		_, ok := allowed[normalizeOrigin(origin)]
		return ok
	},
}

// EventType represents the type of real-time event
type EventType string

const (
	EventPipelineStatus EventType = "pipeline_status"
	// Normalized, UI-friendly pipeline progress event (preferred by frontend)
	EventPipelineProgress EventType = "pipeline_progress"
	// High-signal data-plane metric updates (throughput/lag/cost rollups, etc.)
	EventDataPlaneMetrics EventType = "data_plane_metrics"
	EventCDCStatus        EventType = "cdc_status"
	EventConnectionStatus EventType = "connection_status"
	EventAgentActivity    EventType = "agent_activity"
	EventDecisionUpdate   EventType = "decision_update"
	EventSystemHealth     EventType = "system_health"

	// Agent-triggered UI events
	EventTriggerConnectionModal EventType = "trigger_connection_modal"
	EventAgentClarification     EventType = "agent_clarification"
	EventAgentProgress          EventType = "agent_progress"
	EventAgentComplete          EventType = "agent_complete"
	EventSyncModeQuestion       EventType = "sync_mode_question"
)

// WebSocketEvent represents a typed event sent to clients
type WebSocketEvent struct {
	Type      EventType              `json:"type"`
	Timestamp string                 `json:"timestamp"`
	Data      map[string]interface{} `json:"data"`
	TraceID   string                 `json:"trace_id,omitempty"`
}

// outboundMessage carries the marshaled WebSocket payload and an optional
// owner user id. If ownerUserID is non-empty, the hub will only deliver the
// payload to clients whose userID matches; if empty, the hub falls back to
// global delivery (used for genuinely cross-tenant events such as system
// health and for legacy broadcasters that do not yet carry a pipeline_id).
type outboundMessage struct {
	payload     []byte
	ownerUserID string
}

// pipelineOwnerCacheMax bounds the in-process pipeline_id → owner map.
// pipelines.created_by is immutable, so cached entries never go stale; the
// cap exists only to prevent unbounded growth in a long-running process.
const pipelineOwnerCacheMax = 10000

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan outboundMessage
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
	done       chan struct{}
	stopOnce   sync.Once

	// pipelineOwners caches pipeline_id → owner user_id for fan-out scoping.
	// See PipelineOwner for the lookup + cache strategy.
	ownerMu        sync.RWMutex
	pipelineOwners map[string]string
}

type Client struct {
	hub       *Hub
	conn      *websocket.Conn
	send      chan []byte
	userID    string // set at upgrade time from the authenticated session
	closeOnce sync.Once
}

func normalizeOrigin(origin string) string {
	o := strings.TrimSpace(origin)
	o = strings.TrimRight(o, "/")
	return o
}

func isNonProductionEnv() bool {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
	return env != "production" && env != "prod"
}

func isSameHostOrigin(origin string, r *http.Request) bool {
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	originHost := strings.TrimSpace(u.Host)
	if originHost == "" {
		return false
	}
	// r.Host may include port. Require exact match to avoid surprises.
	return strings.EqualFold(originHost, strings.TrimSpace(r.Host))
}

func allowedOriginsFromEnv() map[string]struct{} {
	raw := strings.TrimSpace(os.Getenv("RSYNC_CORS_ORIGINS"))
	if raw == "" {
		return nil
	}
	out := map[string]struct{}{}
	for _, part := range strings.Split(raw, ",") {
		o := normalizeOrigin(part)
		if o == "" {
			continue
		}
		out[o] = struct{}{}
	}
	return out
}

func (c *Client) closeSend() {
	c.closeOnce.Do(func() {
		close(c.send)
	})
}

func NewHub() *Hub {
	return &Hub{
		broadcast:      make(chan outboundMessage),
		register:       make(chan *Client),
		unregister:     make(chan *Client),
		clients:        make(map[*Client]bool),
		done:           make(chan struct{}),
		pipelineOwners: make(map[string]string),
	}
}

// PipelineOwner returns the user_id that created the given pipeline, looking
// it up in an in-process cache backed by Postgres. Returns false when the
// pipeline cannot be resolved (unknown id, DB unavailable, deleted row), in
// which case callers MUST fail closed and drop the message rather than
// fall back to global delivery.
func (h *Hub) PipelineOwner(ctx context.Context, pipelineID string) (string, bool) {
	if pipelineID == "" {
		return "", false
	}
	h.ownerMu.RLock()
	owner, ok := h.pipelineOwners[pipelineID]
	h.ownerMu.RUnlock()
	if ok {
		return owner, owner != ""
	}

	database := db.GetDB()
	if database == nil {
		return "", false
	}
	var ownerID string
	err := database.QueryRowContext(ctx, `SELECT created_by FROM pipelines WHERE id = $1`, pipelineID).Scan(&ownerID)
	if err != nil || ownerID == "" {
		return "", false
	}

	h.ownerMu.Lock()
	// Hard-cap eviction: clear when full. Acceptable because misses repopulate
	// from the immutable pipelines.created_by column with one cheap PK lookup.
	if len(h.pipelineOwners) >= pipelineOwnerCacheMax {
		h.pipelineOwners = make(map[string]string, pipelineOwnerCacheMax)
	}
	h.pipelineOwners[pipelineID] = ownerID
	h.ownerMu.Unlock()
	return ownerID, true
}

// cachePipelineOwner is a test seam: it lets unit tests prime the owner cache
// without standing up a database. Production code resolves owners via
// PipelineOwner (which falls back to Postgres).
func (h *Hub) cachePipelineOwner(pipelineID, userID string) {
	if pipelineID == "" || userID == "" {
		return
	}
	h.ownerMu.Lock()
	h.pipelineOwners[pipelineID] = userID
	h.ownerMu.Unlock()
}

// sendScopedToPipeline marshals + delivers a payload that must only be visible
// to the owner of pipelineID. If the owner cannot be resolved we fail closed.
func (h *Hub) sendScopedToPipeline(pipelineID string, payload []byte, traceID string) {
	if pipelineID == "" {
		log.WithField("trace_id", traceID).Warn("dropping pipeline-scoped WS event: missing pipeline_id")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	owner, ok := h.PipelineOwner(ctx, pipelineID)
	if !ok {
		log.WithFields(log.Fields{
			"trace_id":    traceID,
			"pipeline_id": pipelineID,
		}).Warn("dropping pipeline-scoped WS event: owner unresolved")
		return
	}
	h.broadcast <- outboundMessage{payload: payload, ownerUserID: owner}
}

func (h *Hub) Stop() {
	h.stopOnce.Do(func() {
		close(h.done)
		h.mu.Lock()
		for client := range h.clients {
			delete(h.clients, client)
			client.closeSend()
		}
		h.mu.Unlock()
	})
}

// BroadcastEvent sends a typed event to connected clients. If the event's
// data map contains a non-empty "pipeline_id" we treat it as pipeline-scoped
// and only deliver to the owner of that pipeline (fail-closed on unresolved
// owner). Events without a pipeline_id are still delivered globally to
// preserve behaviour for genuinely cross-tenant events such as system_health,
// and for legacy chat-driven events (agent_clarification, sync_mode_question,
// trigger_connection_modal) that do not yet carry an owner correlation id.
// Scoping those is a follow-up — see PR description.
func (h *Hub) BroadcastEvent(eventType EventType, data map[string]interface{}, traceID string) {
	event := WebSocketEvent{
		Type:      eventType,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data:      data,
		TraceID:   traceID,
	}

	log.Printf("📤 Broadcasting WebSocket event: type=%s, data=%+v", eventType, data)

	eventJSON, err := json.Marshal(event)
	if err != nil {
		log.Printf("Failed to marshal WebSocket event: %v", err)
		return
	}

	if pipelineID, ok := extractPipelineID(data); ok {
		h.sendScopedToPipeline(pipelineID, eventJSON, traceID)
		return
	}
	h.broadcast <- outboundMessage{payload: eventJSON}
}

// extractPipelineID pulls a non-empty pipeline_id out of an event payload so
// BroadcastEvent can route pipeline-scoped events through the owner filter.
func extractPipelineID(data map[string]interface{}) (string, bool) {
	if data == nil {
		return "", false
	}
	raw, ok := data["pipeline_id"]
	if !ok || raw == nil {
		return "", false
	}
	s, ok := raw.(string)
	if !ok {
		return "", false
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	return s, true
}

// BroadcastPipelineProgress sends a normalized pipeline progress event.
// This is intended to be stable across all connectors/services.
func (h *Hub) BroadcastPipelineProgress(data map[string]interface{}, traceID string) {
	h.BroadcastEvent(EventPipelineProgress, data, traceID)
}

// BroadcastDataPlaneMetrics broadcasts a high-signal data-plane metrics update.
// These events are expected to be low-frequency (e.g., every 5–10s) and safe for UI consumption.
func (h *Hub) BroadcastDataPlaneMetrics(data map[string]interface{}, traceID string) {
	h.BroadcastEvent(EventDataPlaneMetrics, data, traceID)
}

// BroadcastCDCUpdate sends a CDC connector status update
func (h *Hub) BroadcastCDCUpdate(connectorName, status string, details map[string]interface{}) {
	data := map[string]interface{}{
		"connector_name": connectorName,
		"status":         status,
	}
	for k, v := range details {
		data[k] = v
	}
	h.BroadcastEvent(EventCDCStatus, data, "")
}

// BroadcastAgentActivity sends an agent activity update
func (h *Hub) BroadcastAgentActivity(agent, status, pipelineID string, details map[string]interface{}, traceID string) {
	data := map[string]interface{}{
		"agent":       agent,
		"status":      status, // Changed from "action" to "status" to match frontend expectations
		"pipeline_id": pipelineID,
	}
	for k, v := range details {
		data[k] = v
	}
	h.BroadcastEvent(EventAgentActivity, data, traceID)
}

// ============================================================================
// NEW ARCHITECTURE: Domain Events & Telemetry
// ============================================================================

// BroadcastDomainEvent broadcasts a canonical domain event (new architecture)
// Domain events are the source of truth for pipeline state transitions and
// can contain pipeline config, schemas and error messages — they must only
// be visible to the owner of pipelineID.
func (h *Hub) BroadcastDomainEvent(pipelineID string, event map[string]interface{}, traceID string) {
	wsEvent := WebSocketEvent{
		Type:      "domain_event",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data:      event,
		TraceID:   traceID,
	}
	message, err := json.Marshal(wsEvent)
	if err != nil {
		log.Printf("Failed to marshal domain event: %v", err)
		return
	}
	h.sendScopedToPipeline(pipelineID, message, traceID)
}

// BroadcastTelemetryEvent broadcasts agent telemetry (debug only). Even
// though telemetry is developer-facing, the payload still references a
// specific pipeline so we scope delivery to that pipeline's owner.
func (h *Hub) BroadcastTelemetryEvent(pipelineID string, event map[string]interface{}, traceID string) {
	wsEvent := WebSocketEvent{
		Type:      "telemetry_event",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Data:      event,
		TraceID:   traceID,
	}
	message, err := json.Marshal(wsEvent)
	if err != nil {
		log.Printf("Failed to marshal telemetry event: %v", err)
		return
	}
	h.sendScopedToPipeline(pipelineID, message, traceID)
}

// ============================================================================

func (h *Hub) Run() {
	// OPTIMIZATION: Batch messages to reduce WebSocket overhead
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	messageBatch := make([]outboundMessage, 0, 10)
	var batchMu sync.Mutex

	for {
		select {
		case <-h.done:
			return
		case client := <-h.register:
			h.mu.Lock()
			h.clients[client] = true
			h.mu.Unlock()
		case client := <-h.unregister:
			h.mu.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				client.closeSend()
			}
			h.mu.Unlock()
		case message := <-h.broadcast:
			batchMu.Lock()
			messageBatch = append(messageBatch, message)
			batchMu.Unlock()
		case <-ticker.C:
			batchMu.Lock()
			if len(messageBatch) > 0 {
				toDrop := make([]*Client, 0)
				h.mu.RLock()
				for client := range h.clients {
					for _, msg := range messageBatch {
						// Scope filter: pipeline-owned events are delivered
						// only to clients authenticated as the owner. An
						// empty ownerUserID means "deliver to all" and is
						// used by genuinely cross-tenant events plus legacy
						// chat-driven broadcasts that don't yet carry an
						// owner correlation id.
						if msg.ownerUserID != "" && client.userID != msg.ownerUserID {
							continue
						}
						select {
						case client.send <- msg.payload:
						default:
							// Client buffer full: disconnect to prevent silent UI desync.
							toDrop = append(toDrop, client)
							goto nextClient
						}
					}
				nextClient:
					continue
				}
				h.mu.RUnlock()
				if len(toDrop) > 0 {
					h.mu.Lock()
					for _, client := range toDrop {
						if _, ok := h.clients[client]; ok {
							delete(h.clients, client)
							client.closeSend()
						}
					}
					h.mu.Unlock()
				}
				messageBatch = messageBatch[:0]
			}
			batchMu.Unlock()
		}
	}
}

func (c *Client) writePump() {
	defer func() {
		c.conn.Close()
	}()
	for {
		message, ok := <-c.send
		if !ok {
			c.conn.WriteMessage(websocket.CloseMessage, []byte{})
			return
		}
		if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
			return
		}
	}
}

func ServeWs(hub *Hub, c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Println(err)
		return
	}
	// Tag the client with the authenticated user so the hub can deliver
	// pipeline-scoped events only to the pipeline's owner. The caller is
	// expected to populate user_id on the gin context before invoking
	// ServeWs (the /ws route in main.go does this via its session lookup).
	userID := strings.TrimSpace(c.GetString("user_id"))
	client := &Client{hub: hub, conn: conn, send: make(chan []byte, 256), userID: userID}
	client.hub.register <- client

	go client.writePump()
	// Read pump is minimal for now as we mostly push to clients
	go func() {
		defer func() {
			client.hub.unregister <- client
			client.conn.Close()
		}()
		for {
			_, _, err := client.conn.ReadMessage()
			if err != nil {
				break
			}
		}
	}()
}
