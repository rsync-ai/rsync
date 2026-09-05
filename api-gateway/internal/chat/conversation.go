package chat

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

// =============================================================================
// CONVERSATION STATE MODEL
// =============================================================================

// ConversationState represents the current state in the conversation state machine
type ConversationState string

const (
	// StateIdle - no pending intent, ready for new user message
	StateIdle ConversationState = "idle"
	// StateAwaitingSource - waiting for user to specify source connector
	StateAwaitingSource ConversationState = "awaiting_source"
	// StateAwaitingDestination - waiting for user to specify destination connector
	StateAwaitingDestination ConversationState = "awaiting_destination"
	// StateAwaitingRole - exactly one connector was named but its role (source vs
	// destination) couldn't be inferred from the phrasing (e.g. a bare "mysql").
	// We hold the named connector in PendingIntent.UnresolvedConnector and ask the
	// user which side it is before continuing slot-filling.
	StateAwaitingRole ConversationState = "awaiting_role"
	// StateAwaitingConfirmation - both source and dest known, waiting for user to confirm
	StateAwaitingConfirmation ConversationState = "awaiting_confirmation"
)

// PendingIntent stores the partially-filled intent during slot-filling
type PendingIntent struct {
	Action          string   `json:"action,omitempty"`           // e.g., "sync", "transfer", "replicate"
	SourceType      string   `json:"source_type,omitempty"`      // e.g., "mysql", "postgresql"
	DestinationType string   `json:"destination_type,omitempty"` // e.g., "s3", "snowflake"
	Tables          []string `json:"tables,omitempty"`           // specific tables if mentioned
	SyncMode        string   `json:"sync_mode,omitempty"`        // "batch", "cdc", "auto"
	Confidence      float64  `json:"confidence,omitempty"`       // confidence score from LLM
	// OriginalRequest preserves the user's initial NL request across slot-filling + confirmation.
	// Without this, confirmation messages like "Yes sync_mode=batch" would overwrite the intent,
	// causing pipelines to be created with generic requests (e.g., "Sync mysql to postgresql").
	OriginalRequest string `json:"original_request,omitempty"`
	// UnresolvedConnector holds a single named connector whose role (source vs
	// destination) is still ambiguous, while we ask the user to clarify in the
	// StateAwaitingRole step. It is cleared once the role is chosen.
	UnresolvedConnector string `json:"unresolved_connector,omitempty"`
}

// IsValid checks if the intent has all required fields
func (p *PendingIntent) IsValid() bool {
	return p.SourceType != "" && p.DestinationType != ""
}

// ConversationContext stores multi-turn conversation state for a user session
type ConversationContext struct {
	mu sync.RWMutex

	// Identifiers
	ID        string `json:"id"` // conversation_id (same as session_id)
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`

	// State machine
	State         ConversationState `json:"state"`
	PendingIntent *PendingIntent    `json:"pending_intent,omitempty"`

	// Last interaction
	LastMessage   string    `json:"last_message,omitempty"`
	LastResponse  string    `json:"last_response,omitempty"`
	LastUpdatedAt time.Time `json:"last_updated_at"`

	// Metadata
	CreatedAt    time.Time `json:"created_at"`
	MessageCount int       `json:"message_count"`
}

// NewConversationContext creates a new conversation context
func NewConversationContext(userID, sessionID string) *ConversationContext {
	now := time.Now()
	return &ConversationContext{
		ID:            sessionID,
		UserID:        userID,
		SessionID:     sessionID,
		State:         StateIdle,
		PendingIntent: nil,
		CreatedAt:     now,
		LastUpdatedAt: now,
		MessageCount:  0,
	}
}

// SetState updates the conversation state
func (c *ConversationContext) SetState(state ConversationState) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.State = state
	c.LastUpdatedAt = time.Now()
}

// SetPendingIntent sets or updates the pending intent
func (c *ConversationContext) SetPendingIntent(intent *PendingIntent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.PendingIntent = clonePendingIntent(intent)
	c.LastUpdatedAt = time.Now()
}

// Reset clears the conversation state to idle
func (c *ConversationContext) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.State = StateIdle
	c.PendingIntent = nil
	c.LastUpdatedAt = time.Now()
}

func clonePendingIntent(p *PendingIntent) *PendingIntent {
	if p == nil {
		return nil
	}
	cp := *p
	if p.Tables != nil {
		cp.Tables = append([]string(nil), p.Tables...)
	}
	return &cp
}

func (c *ConversationContext) GetState() ConversationState {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.State
}

func (c *ConversationContext) GetPendingIntent() *PendingIntent {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return clonePendingIntent(c.PendingIntent)
}

func (c *ConversationContext) IncMessageCount() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.MessageCount++
	c.LastUpdatedAt = time.Now()
}

func (c *ConversationContext) SetLastMessage(message string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LastMessage = message
	c.LastUpdatedAt = time.Now()
}

func (c *ConversationContext) SetLastResponse(response string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LastResponse = response
	c.LastUpdatedAt = time.Now()
}

func (c *ConversationContext) GetLastResponse() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.LastResponse
}

func (c *ConversationContext) Touch(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LastUpdatedAt = at
}

// Snapshot returns a deep copy suitable for safe JSON marshaling.
// It does NOT copy the internal mutex state.
func (c *ConversationContext) Snapshot() *ConversationContext {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &ConversationContext{
		ID:            c.ID,
		UserID:        c.UserID,
		SessionID:     c.SessionID,
		State:         c.State,
		PendingIntent: clonePendingIntent(c.PendingIntent),
		LastMessage:   c.LastMessage,
		LastResponse:  c.LastResponse,
		LastUpdatedAt: c.LastUpdatedAt,
		CreatedAt:     c.CreatedAt,
		MessageCount:  c.MessageCount,
	}
}

// =============================================================================
// CONNECTOR VALIDATION
// =============================================================================

// ConnectorStopwords are words that should NOT be treated as connector names
var ConnectorStopwords = map[string]bool{
	"want":     true,
	"create":   true,
	"setup":    true,
	"make":     true,
	"build":    true,
	"new":      true,
	"pipeline": true,
	"sync":     true,
	"transfer": true,
	"move":     true,
	"copy":     true,
	"data":     true,
	"help":     true,
	"please":   true,
	"need":     true,
	"like":     true,
	"would":    true,
}

// IsValidConnectorName checks if a name is a valid connector (not a stopword)
func IsValidConnectorName(name string) bool {
	normalized := strings.ToLower(strings.TrimSpace(name))
	if normalized == "" {
		return false
	}
	return !ConnectorStopwords[normalized]
}

// LevenshteinDistance computes the edit distance between two strings.
func LevenshteinDistance(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}
	row := make([]int, lb+1)
	for j := 0; j <= lb; j++ {
		row[j] = j
	}
	for i := 1; i <= la; i++ {
		prev := i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			val := row[j-1] + cost
			if row[j]+1 < val {
				val = row[j] + 1
			}
			if prev+1 < val {
				val = prev + 1
			}
			row[j-1] = prev
			prev = val
		}
		row[lb] = prev
	}
	return row[lb]
}

// FuzzyMatchConnector finds the closest connector name within maxDist edits.
// Returns the best match and whether one was found.
func FuzzyMatchConnector(input string, knownConnectors []string, maxDist int) (string, bool) {
	input = strings.ToLower(strings.TrimSpace(input))
	best := ""
	bestDist := maxDist + 1
	for _, c := range knownConnectors {
		d := LevenshteinDistance(input, c)
		if d < bestDist {
			bestDist = d
			best = c
		}
	}
	if bestDist <= maxDist {
		return best, true
	}
	return "", false
}

// NormalizeConnectorName normalizes common connector name variations
func NormalizeConnectorName(name string) string {
	normalized := strings.ToLower(strings.TrimSpace(name))

	// Common aliases. Short user-facing names map to the canonical connector
	// id used on disk and in connector_catalog.
	aliases := map[string]string{
		"s3":              "aws-s3",
		"amazon-s3":       "aws-s3",
		"postgres":        "postgresql",
		"postgesql":       "postgresql", // common typo
		"pg":              "postgresql",
		"mongo":           "mongodb",
		"gcs":             "google-cloud-storage",
		"azure-blob":      "azure-blob-storage",
		"bq":              "bigquery",
		"elasticsearch":   "elasticsearch",
		"es":              "elasticsearch",
		"mssql":           "sqlserver",
		"sql-server":      "sqlserver",
		"oracledb":        "oracle",
		"oracle-db":       "oracle",
		"oracle-database": "oracle",
		"shopify":         "shopify-admin-graphql",
	}

	if alias, ok := aliases[normalized]; ok {
		return alias
	}
	return normalized
}

// =============================================================================
// REDIS-BACKED CONVERSATION CACHE
// =============================================================================

const (
	// ConversationTTL is the default TTL for conversation context (30 minutes)
	ConversationTTL = 30 * time.Minute
	// ConversationKeyPrefix is the Redis key prefix for conversation contexts
	ConversationKeyPrefix = "chat:conversation:"
)

// ConversationCache provides Redis-backed storage for conversation contexts
type ConversationCache struct {
	client *redis.Client
	ttl    time.Duration
}

// NewConversationCache creates a new conversation cache using an existing Redis client
func NewConversationCache(client *redis.Client) *ConversationCache {
	return &ConversationCache{
		client: client,
		ttl:    ConversationTTL,
	}
}

// buildKey constructs the Redis key for a conversation
func (c *ConversationCache) buildKey(userID, conversationID string) string {
	return fmt.Sprintf("%s%s:%s", ConversationKeyPrefix, userID, conversationID)
}

// Get retrieves a conversation context by user ID and conversation ID
func (c *ConversationCache) Get(ctx context.Context, userID, conversationID string) (*ConversationContext, error) {
	key := c.buildKey(userID, conversationID)

	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil // Not found - start new conversation
	}
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"user_id":         userID,
			"conversation_id": conversationID,
		}).Warn("Redis GET conversation failed")
		return nil, err
	}

	var conv ConversationContext
	if err := json.Unmarshal(val, &conv); err != nil {
		log.WithError(err).Warn("Failed to unmarshal conversation context")
		return nil, err
	}

	log.WithFields(log.Fields{
		"user_id":         userID,
		"conversation_id": conversationID,
		"state":           conv.State,
		"message_count":   conv.MessageCount,
	}).Debug("Conversation context loaded from cache")

	return &conv, nil
}

// Set stores a conversation context
func (c *ConversationCache) Set(ctx context.Context, conv *ConversationContext) error {
	now := time.Now()
	conv.Touch(now)
	snap := conv.Snapshot()
	key := c.buildKey(snap.UserID, snap.ID)

	data, err := json.Marshal(snap)
	if err != nil {
		return fmt.Errorf("failed to marshal conversation context: %w", err)
	}

	err = c.client.Set(ctx, key, data, c.ttl).Err()
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"user_id":         conv.UserID,
			"conversation_id": conv.ID,
		}).Warn("Redis SET conversation failed")
		return err
	}

	log.WithFields(log.Fields{
		"user_id":         snap.UserID,
		"conversation_id": snap.ID,
		"state":           snap.State,
		"ttl":             c.ttl.String(),
	}).Debug("Conversation context cached")

	return nil
}

// GetOrCreate retrieves an existing conversation or creates a new one
func (c *ConversationCache) GetOrCreate(ctx context.Context, userID, conversationID string) (*ConversationContext, error) {
	conv, err := c.Get(ctx, userID, conversationID)
	if err != nil {
		return nil, err
	}

	if conv == nil {
		// Create new conversation
		conv = NewConversationContext(userID, conversationID)
		if err := c.Set(ctx, conv); err != nil {
			log.WithError(err).Warn("Failed to cache new conversation")
			// Continue even if caching fails - we have an in-memory copy
		}
		log.WithFields(log.Fields{
			"user_id":         userID,
			"conversation_id": conversationID,
		}).Info("Created new conversation context")
	}

	return conv, nil
}

// =============================================================================
// SLOT FILLING RESULT
// =============================================================================

// SlotFillingResult represents the response from the slot-filling LLM prompt
type SlotFillingResult struct {
	// IsAnsweringPrevious indicates if the user's message is answering a previous question
	IsAnsweringPrevious bool `json:"answering_previous"`
	// ExtractedSlot is which slot was filled (e.g., "source", "destination")
	ExtractedSlot string `json:"extracted_slot,omitempty"`
	// ExtractedValue is the value for the filled slot
	ExtractedValue string `json:"extracted_value,omitempty"`
	// StillMissing lists slots that are still not filled
	StillMissing []string `json:"still_missing,omitempty"`
	// NextQuestion is the question to ask for the next missing slot
	NextQuestion string `json:"next_question,omitempty"`
	// Confidence score for the extraction (0-1)
	Confidence float64 `json:"confidence"`
}

// =============================================================================
// CONFIRMATION PARSING
// =============================================================================

// ConfirmationResult represents the parsed yes/no response
type ConfirmationResult string

const (
	ConfirmationYes     ConfirmationResult = "yes"
	ConfirmationNo      ConfirmationResult = "no"
	ConfirmationUnclear ConfirmationResult = "unclear"
)

// ParseConfirmation determines if a user message is a yes/no confirmation
func ParseConfirmation(message string) ConfirmationResult {
	normalized := strings.ToLower(strings.TrimSpace(message))

	// Remove punctuation for better matching
	normalized = strings.Map(func(r rune) rune {
		if r == ',' || r == '!' || r == '.' || r == '?' {
			return ' '
		}
		return r
	}, normalized)
	normalized = strings.TrimSpace(normalized)
	// Collapse multiple spaces
	for strings.Contains(normalized, "  ") {
		normalized = strings.ReplaceAll(normalized, "  ", " ")
	}

	// A mid-confirmation edit ("change the destination to bigquery", "use bigquery
	// instead") is neither yes nor no. Defer to the handler (which updates the
	// pending pair in place) by returning Unclear — this must run BEFORE the yes/no
	// prefix checks so "okay change …" isn't miscreated and "no, change …" isn't
	// treated as an outright cancel.
	padded := " " + normalized + " "
	for _, ev := range []string{"change", "switch", "edit", "modify", "replace", "instead"} {
		if strings.Contains(padded, " "+ev+" ") {
			return ConfirmationUnclear
		}
	}

	// Check longer patterns FIRST (order matters!)
	// No patterns (check multi-word patterns first)
	// NB: edit verbs (change/edit/modify/switch/replace) are intentionally NOT here —
	// they are handled above as an in-place edit, not an outright cancel.
	noPatterns := []string{
		"start over", "hold on", "not yet", "that's wrong", "that is wrong",
		"do not", "don't", "no", "n", "nope", "nah", "cancel", "stop", "abort",
		"wait", "wrong", "incorrect", "different",
		"restart", "new",
	}

	for _, pattern := range noPatterns {
		if normalized == pattern || strings.HasPrefix(normalized, pattern+" ") {
			return ConfirmationNo
		}
	}

	// Yes patterns (check multi-word patterns first)
	yesPatterns := []string{
		"go ahead", "do it", "create it", "run it", "start it", "looks good",
		"that's right", "that is right", "yes", "y", "yeah", "yep", "yup",
		"sure", "ok", "okay", "go", "confirm", "confirmed", "proceed",
		"continue", "start", "run", "lgtm", "correct", "right",
		"absolutely", "definitely", "affirmative",
	}

	for _, pattern := range yesPatterns {
		if normalized == pattern || strings.HasPrefix(normalized, pattern+" ") {
			return ConfirmationYes
		}
	}

	return ConfirmationUnclear
}
