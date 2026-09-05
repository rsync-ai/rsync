package correlation

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

func claimTTLFromEnv() time.Duration {
	// Default is intentionally short so that if a worker crashes mid-task,
	// another worker can pick up the request quickly (prevents pipelines stuck "running").
	//
	// Long-running tasks MUST renew their claim periodically.
	claimTTL := 2 * time.Minute
	if raw := os.Getenv("CORRELATION_CLAIM_TTL"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			claimTTL = d
		} else {
			log.WithField("value", raw).Warn("Invalid CORRELATION_CLAIM_TTL; using default 2m")
		}
	}
	return claimTTL
}

// PendingRequest represents a request waiting for processing
type PendingRequest struct {
	CorrelationID string                 `json:"correlation_id"`
	RequestType   string                 `json:"request_type"` // "intent", "plan", "execute", etc.
	Payload       map[string]interface{} `json:"payload"`
	Timestamp     int64                  `json:"timestamp"`
}

// PollPendingRequests polls Redis for pending requests of given type
func (c *Client) PollPendingRequests(ctx context.Context, requestType string) ([]*PendingRequest, error) {
	// Use Redis SCAN to find pending request keys
	// Pattern: agent:request:* (all requests, we filter by type after fetching)
	pattern := "agent:request:*"

	var cursor uint64
	var requests []*PendingRequest

	for {
		// SCAN COUNT is a hint for how many keys to examine per round-trip.
		// At the old value of 10, a single poll over a 32k-key keyspace took
		// ~3,200 round-trips — and with 6 worker pollers each doing this on a
		// ticker, Redis saw ~74k SCAN ops/sec (pinning the orchestrator at
		// ~200% CPU and Redis at ~60%). A larger COUNT collapses that to a few
		// dozen round-trips per poll with no behavioural change (SCAN still
		// returns every matching key across the full cursor walk).
		keys, nextCursor, err := c.redis.Scan(ctx, cursor, pattern, 1000).Result()
		if err != nil {
			return nil, fmt.Errorf("redis scan failed: %w", err)
		}

		// Fetch each request
		for _, key := range keys {
			data, err := c.redis.Get(ctx, key).Result()
			if err == redis.Nil {
				continue // Key expired/deleted
			}
			if err != nil {
				return nil, fmt.Errorf("redis get failed: %w", err)
			}

			// Parse as generic request to check agent_type
			var genericReq struct {
				CorrelationID string                 `json:"correlation_id"`
				AgentType     string                 `json:"agent_type"`
				Task          map[string]interface{} `json:"task"`
				SentAt        string                 `json:"sent_at"`
			}

			if err := json.Unmarshal([]byte(data), &genericReq); err != nil {
				continue // Skip malformed requests
			}

			// Filter by agent type
			if genericReq.AgentType != requestType {
				continue
			}

			// Convert to PendingRequest
			req := &PendingRequest{
				CorrelationID: genericReq.CorrelationID,
				RequestType:   genericReq.AgentType,
				Payload:       genericReq.Task,
			}

			requests = append(requests, req)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}

	return requests, nil
}

// ClaimRequest atomically claims a request for processing
func (c *Client) ClaimRequest(ctx context.Context, correlationID string, workerID string) (bool, error) {
	// Use Redis SET NX to claim
	claimKey := fmt.Sprintf("claim:%s", correlationID)

	// IMPORTANT:
	// - Claim TTL is short by default (see claimTTLFromEnv()) to avoid stuck claims after crashes.
	// - Long-running tasks must renew their claim periodically while processing.
	claimTTL := claimTTLFromEnv()
	success, err := c.redis.SetNX(ctx, claimKey, workerID, claimTTL).Result()
	if err != nil {
		return false, err
	}

	return success, nil
}

// RenewClaim extends the claim TTL for a correlation request if this worker still owns the claim.
// Returns (true,nil) when renewed; (false,nil) if claim is missing or owned by someone else.
func (c *Client) RenewClaim(ctx context.Context, correlationID string, workerID string) (bool, error) {
	claimKey := fmt.Sprintf("claim:%s", correlationID)
	current, err := c.redis.Get(ctx, claimKey).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current != workerID {
		return false, nil
	}
	return c.redis.Expire(ctx, claimKey, claimTTLFromEnv()).Result()
}

// DeleteRequest removes a processed request from Redis
func (c *Client) DeleteRequest(ctx context.Context, correlationID string, requestType string) error {
	key := fmt.Sprintf("agent:request:%s", correlationID)
	return c.redis.Del(ctx, key).Err()
}
