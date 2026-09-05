package correlation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

// ==============================================================================
// CORRELATION STORE (Phase D - Request/Reply Pattern)
// ==============================================================================
// This implements a Redis-based correlation store for Activity request/reply.
// Activities write requests with a correlation ID, then block waiting for the
// correlated response. Workers write responses to Redis.
//
// This replaces the anti-pattern of KafkaAdapter signaling workflows.

const (
	// Redis key prefixes
	requestPrefix  = "agent:request:"
	responsePrefix = "agent:response:"

	// Default timeout for waiting for responses
	defaultTimeout = 5 * time.Minute
)

// Store manages request/response correlation
type Store struct {
	redis *redis.Client
}

// NewStore creates a new correlation store
func NewStore(redisAddr, redisPassword string) (*Store, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     redisPassword,
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.WithField("addr", redisAddr).Info("✅ Connected to Redis correlation store")
	return &Store{redis: client}, nil
}

// Request represents an agent request
type Request struct {
	CorrelationID string                 `json:"correlation_id"`
	AgentType     string                 `json:"agent_type"`
	Task          map[string]interface{} `json:"task"`
	Timeout       time.Duration          `json:"timeout,omitempty"`
	SentAt        time.Time              `json:"sent_at"`
}

// Response represents an agent response
type Response struct {
	CorrelationID string                 `json:"correlation_id"`
	Status        string                 `json:"status"`
	Output        map[string]interface{} `json:"output,omitempty"`
	Error         string                 `json:"error,omitempty"`
	ReceivedAt    time.Time              `json:"received_at"`
}

// WriteRequest writes a request and returns the correlation ID
func (s *Store) WriteRequest(ctx context.Context, req Request) error {
	// Defense-in-depth: a nil store (Redis never initialized) must surface a clean,
	// retriable error instead of a nil-pointer panic on s.redis below. The adapter
	// now fails loud at startup if init fails (cmd/adapter/main.go), so this should be
	// unreachable in practice — but a panic here is opaque (it manifests deep inside a
	// Temporal activity), so we guard the exact dereference site.
	if s == nil || s.redis == nil {
		return fmt.Errorf("correlation store not initialized (nil Redis client)")
	}
	hasUserRequest := false
	if req.Task != nil {
		_, hasUserRequest = req.Task["user_request"]
	}
	hasParsedIntent := false
	if req.Task != nil {
		_, hasParsedIntent = req.Task["parsed_intent"]
	}
	log.WithFields(log.Fields{
		"correlation_id":    req.CorrelationID,
		"agent_type":        req.AgentType,
		"has_user_request":  hasUserRequest,
		"has_parsed_intent": hasParsedIntent,
	}).Debug("correlation WriteRequest called")

	if req.CorrelationID == "" {
		return fmt.Errorf("correlation_id is required")
	}

	data, err := json.Marshal(req)
	if err != nil {
		log.WithError(err).WithFields(log.Fields{
			"correlation_id": req.CorrelationID,
			"agent_type":     req.AgentType,
		}).Error("failed to marshal correlation request")
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	key := requestPrefix + req.CorrelationID

	// Write request with TTL (2x timeout for safety)
	ttl := defaultTimeout * 2
	if req.Timeout > 0 {
		ttl = req.Timeout * 2
	}
	log.WithFields(log.Fields{
		"key": key,
		"ttl": ttl,
	}).Debug("writing correlation request to redis")

	if err := s.redis.Set(ctx, key, data, ttl).Err(); err != nil {
		log.WithError(err).WithField("key", key).Error("failed to write correlation request to redis")
		return fmt.Errorf("failed to write request: %w", err)
	}
	log.WithFields(log.Fields{
		"correlation_id": req.CorrelationID,
		"agent_type":     req.AgentType,
		"key":            key,
		"ttl":            ttl,
	}).Debug("correlation request written to redis")

	return nil
}

// WaitForResponse blocks until a response is received or timeout
// This is the core blocking primitive used by Activities
func (s *Store) WaitForResponse(ctx context.Context, correlationID string, timeout time.Duration) (*Response, error) {
	if s == nil || s.redis == nil {
		return nil, fmt.Errorf("correlation store not initialized (nil Redis client)")
	}
	if timeout == 0 {
		timeout = defaultTimeout
	}

	responseKey := responsePrefix + correlationID
	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	log.WithFields(log.Fields{
		"correlation_id": correlationID,
		"timeout":        timeout,
	}).Debug("⏳ Waiting for correlated response")

	// Use BRPOP for blocking pop with timeout
	// BRPOP key timeout_seconds
	result, err := s.redis.BRPop(ctxTimeout, timeout, responseKey).Result()
	if err != nil {
		if err == redis.Nil || err == context.DeadlineExceeded {
			return nil, fmt.Errorf("timeout waiting for response: correlation_id=%s", correlationID)
		}
		return nil, fmt.Errorf("failed to wait for response: %w", err)
	}

	// BRPOP returns [key, value]
	if len(result) != 2 {
		return nil, fmt.Errorf("unexpected BRPOP result length: %d", len(result))
	}

	responseData := result[1]
	var response Response
	if err := json.Unmarshal([]byte(responseData), &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	log.WithFields(log.Fields{
		"correlation_id": correlationID,
		"status":         response.Status,
	}).Info("✅ Received correlated response")

	// Clean up request key
	s.redis.Del(context.Background(), requestPrefix+correlationID)

	return &response, nil
}

// WriteResponse writes a response (called by workers)
func (s *Store) WriteResponse(ctx context.Context, resp Response) error {
	if resp.CorrelationID == "" {
		return fmt.Errorf("correlation_id is required")
	}

	resp.ReceivedAt = time.Now()
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	responseKey := responsePrefix + resp.CorrelationID

	// Use LPUSH to push response (BRPOP waits on the other end)
	if err := s.redis.LPush(ctx, responseKey, data).Err(); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	// Set expiry on response key (in case BRPOP already timed out)
	s.redis.Expire(ctx, responseKey, 10*time.Minute)

	log.WithFields(log.Fields{
		"correlation_id": resp.CorrelationID,
		"status":         resp.Status,
	}).Debug("📝 Wrote correlated response")

	return nil
}

// GetRequest retrieves a request by correlation ID (for workers)
func (s *Store) GetRequest(ctx context.Context, correlationID string) (*Request, error) {
	key := requestPrefix + correlationID
	data, err := s.redis.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("request not found: correlation_id=%s", correlationID)
		}
		return nil, fmt.Errorf("failed to get request: %w", err)
	}

	var req Request
	if err := json.Unmarshal([]byte(data), &req); err != nil {
		return nil, fmt.Errorf("failed to unmarshal request: %w", err)
	}

	return &req, nil
}

// Close closes the Redis connection
func (s *Store) Close() error {
	return s.redis.Close()
}
