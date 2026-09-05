package handlers

import (
	"github.com/gin-gonic/gin"

	"api-gateway/internal/chat"
	"api-gateway/internal/kafka"
)

// ChatHandler handles chat/conversational pipeline creation
type ChatHandler struct {
	kafkaProducer     *kafka.UnifiedProducer
	orchestratorURL   string
	conversationCache *chat.ConversationCache
}

// NewChatHandler creates a new chat handler
func NewChatHandler(kafkaProducer *kafka.UnifiedProducer, orchestratorURL string) *ChatHandler {
	return &ChatHandler{
		kafkaProducer:   kafkaProducer,
		orchestratorURL: orchestratorURL,
	}
}

// SetConversationCache sets the conversation cache for the chat handler
func (h *ChatHandler) SetConversationCache(cache *chat.ConversationCache) {
	h.conversationCache = cache
}

// ChatMessageRequest represents a chat message from the frontend.
//
// Session continuity: the server echoes the session_id in
// response.metadata.session_id every turn. Clients must send it back to
// keep multi-turn state (intent → awaiting_confirmation → pipeline_started).
// SessionID at the top level is supported as a defensive convenience for
// API callers; the frontend uses the nested context.session_id form which
// is also still accepted.
type ChatMessageRequest struct {
	Message   string                 `json:"message" binding:"required"`
	SessionID string                 `json:"session_id,omitempty"`
	Context   map[string]interface{} `json:"context"`
	History   []map[string]string    `json:"history"`
	// Autosend, when true, instructs the server to skip the interactive
	// confirmation step and immediately start the pipeline if the intent is
	// unambiguous (both source and destination resolve to known connectors).
	// This is used by non-interactive API harnesses (e.g. scripts/e2e-*.sh)
	// that cannot type "yes" in a follow-up turn. The UI does not need this:
	// it surfaces a "Start pipeline" button that posts the confirmation
	// message itself.
	Autosend bool `json:"autosend,omitempty"`
}

// ChatMessageResponse represents the agent's response
type ChatMessageResponse struct {
	Message     string                 `json:"message"`
	Type        string                 `json:"type"` // text, connector_missing, connection_missing, pipeline_started, etc.
	Data        map[string]interface{} `json:"data,omitempty"`
	TraceID     string                 `json:"trace_id,omitempty"`
	Timestamp   string                 `json:"timestamp,omitempty"`
	Agent       string                 `json:"agent,omitempty"`
	Suggestions []string               `json:"suggestions,omitempty"` // Suggested actions for user
	Metadata    map[string]interface{} `json:"metadata,omitempty"`    // Additional metadata (redirect URLs, etc.)
}

// SendMessage handles POST /api/v1/chat/message
// @Summary Send chat message to create pipeline
// @Description Sends a natural language message to create a pipeline via Temporal workflow
// @Tags Chat
// @Accept json
// @Produce json
// @Param message body ChatMessageRequest true "Chat Message"
// @Success 200 {object} ChatMessageResponse
// @Router /api/v1/chat/message [post]
func (h *ChatHandler) SendMessage(c *gin.Context) {
	// Use NL Pipeline flow with HITL checkpoints
	h.SendMessageNLPipeline(c)
}

