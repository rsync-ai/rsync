package cache

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

// SchemaCache provides Redis-backed caching for schema discovery
type SchemaCache struct {
	client *redis.Client
	ttl    time.Duration
}

// NewSchemaCache creates a new schema cache instance
func NewSchemaCache(redisAddr, redisPassword string, ttl time.Duration) (*SchemaCache, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     redisPassword,
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	})

	// Instrument Redis client with OpenTelemetry (traces + metrics for every command)
	if err := redisotel.InstrumentTracing(client); err != nil {
		log.WithError(err).Warn("redisotel: failed to instrument Redis client — continuing without Redis spans")
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	log.WithField("redis_addr", redisAddr).Info("✅ Schema cache initialized")

	return &SchemaCache{
		client: client,
		ttl:    ttl,
	}, nil
}

// Get retrieves cached schema
func (c *SchemaCache) Get(ctx context.Context, connectionID string) ([]byte, error) {
	key := fmt.Sprintf("schema:%s", connectionID)
	
	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil // Cache miss
	}
	if err != nil {
		log.WithError(err).Warn("Redis GET failed")
		return nil, err
	}

	log.WithField("connection_id", connectionID).Debug("Schema cache hit")
	return val, nil
}

// Set caches schema discovery result
func (c *SchemaCache) Set(ctx context.Context, connectionID string, data []byte) error {
	key := fmt.Sprintf("schema:%s", connectionID)
	
	err := c.client.Set(ctx, key, data, c.ttl).Err()
	if err != nil {
		log.WithError(err).Warn("Redis SET failed")
		return err
	}

	log.WithFields(log.Fields{
		"connection_id": connectionID,
		"ttl":           c.ttl.String(),
		"size_bytes":    len(data),
	}).Debug("Schema cached")
	
	return nil
}

// Invalidate removes cached schema
func (c *SchemaCache) Invalidate(ctx context.Context, connectionID string) error {
	key := fmt.Sprintf("schema:%s", connectionID)
	
	err := c.client.Del(ctx, key).Err()
	if err != nil {
		log.WithError(err).Warn("Redis DEL failed")
		return err
	}

	log.WithField("connection_id", connectionID).Info("Schema cache invalidated")
	return nil
}

// Close closes the Redis client
func (c *SchemaCache) Close() error {
	return c.client.Close()
}

// GetClient returns the underlying Redis client for reuse
func (c *SchemaCache) GetClient() *redis.Client {
	return c.client
}
