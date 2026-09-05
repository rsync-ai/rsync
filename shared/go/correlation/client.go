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
// CORRELATION CLIENT (For Workers/Agents)
// ==============================================================================
// This is used by workers to write responses to the correlation store
// Activities block waiting for these responses (request/reply pattern)

// Client is a lightweight client for workers to write responses
type Client struct {
	redis *redis.Client
}

// NewClient creates a new correlation client for workers
func NewClient(redisAddr, redisPassword string) (*Client, error) {
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

	log.WithField("addr", redisAddr).Info("✅ Connected to Redis correlation client")
	return &Client{redis: client}, nil
}

// WorkerResponse represents a worker's response
type WorkerResponse struct {
	CorrelationID string                 `json:"correlation_id"`
	Status        string                 `json:"status"`
	Output        map[string]interface{} `json:"output,omitempty"`
	Error         string                 `json:"error,omitempty"`
	ReceivedAt    time.Time              `json:"received_at"`
}

// WriteResponse writes a response to the correlation store
// This unblocks the waiting Activity
func (c *Client) WriteResponse(ctx context.Context, resp WorkerResponse) error {
	if resp.CorrelationID == "" {
		// No correlation ID means this is a V1 workflow - skip
		return nil
	}

	resp.ReceivedAt = time.Now()
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("failed to marshal response: %w", err)
	}

	responseKey := "agent:response:" + resp.CorrelationID

	// Use LPUSH to push response (BRPOP waits on the other end)
	if err := c.redis.LPush(ctx, responseKey, data).Err(); err != nil {
		return fmt.Errorf("failed to write response: %w", err)
	}

	// Set expiry on response key (in case BRPOP already timed out)
	c.redis.Expire(ctx, responseKey, 10*time.Minute)

	log.WithFields(log.Fields{
		"correlation_id": resp.CorrelationID,
		"status":         resp.Status,
	}).Debug("📝 Wrote correlated response")

	return nil
}

// Close closes the Redis connection
func (c *Client) Close() error {
	return c.redis.Close()
}
