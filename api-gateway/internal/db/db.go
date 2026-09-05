package db

import (
	"context"
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/rsync-ai/shared/pgdriver"
)

var DB *sql.DB

// schemaReady records whether Migrate completed successfully in THIS process.
//
// It exists because a healthy connection pool is not the same thing as a
// usable database. main() runs migrations only inside the Init success
// branch, so an Init that fails once leaves the schema uncreated forever --
// and database/sql then redials lazily the moment Postgres comes up, so the
// pool looks perfectly healthy while every query fails with
// `relation "..." does not exist`. A readiness probe that only pings cannot
// tell those two states apart. This flag can.
var schemaReady atomic.Bool

// connectRetryInterval is the pause between startup ping attempts.
const connectRetryInterval = 3 * time.Second

// connectTimeout bounds the startup connect retry (DB_CONNECT_TIMEOUT,
// default 60s). Zero means "one attempt, fail immediately" -- what a
// fail-fast caller or a unit test wants.
func connectTimeout() time.Duration {
	if v := strings.TrimSpace(os.Getenv("DB_CONNECT_TIMEOUT")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			return d
		}
	}
	return 60 * time.Second
}

// SchemaReady reports whether migrations were applied successfully in this
// process. False means the gateway must not be routed traffic: either the
// database never came up, or it came up after the one-shot migration step
// had already been skipped.
func SchemaReady() bool { return schemaReady.Load() }

// markSchemaReady is called by Migrate on success.
func markSchemaReady() { schemaReady.Store(true) }

// Init initializes the database connection
func Init() error {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		env := strings.ToLower(strings.TrimSpace(os.Getenv("ENVIRONMENT")))
		if env == "production" || env == "prod" {
			return fmt.Errorf("DATABASE_URL is required in production")
		}
		return fmt.Errorf("DATABASE_URL is not set")
	}

	var err error
	DB, err = sql.Open("postgres", dbURL)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Connection pool settings (configurable)
	maxOpen := 25
	if v := strings.TrimSpace(os.Getenv("DB_MAX_OPEN_CONNS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxOpen = n
		}
	}

	// Default idle is intentionally smaller than open to reduce DB idle load.
	maxIdle := 10
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	if v := strings.TrimSpace(os.Getenv("DB_MAX_IDLE_CONNS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxIdle = n
		}
		if maxIdle > maxOpen {
			maxIdle = maxOpen
		}
	}

	connMaxLifetime := 5 * time.Minute
	if v := strings.TrimSpace(os.Getenv("DB_CONN_MAX_LIFETIME")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			connMaxLifetime = d
		}
	}
	connMaxIdleTime := 1 * time.Minute
	if v := strings.TrimSpace(os.Getenv("DB_CONN_MAX_IDLE_TIME")); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			connMaxIdleTime = d
		}
	}

	DB.SetMaxOpenConns(maxOpen)
	DB.SetMaxIdleConns(maxIdle)
	DB.SetConnMaxLifetime(connMaxLifetime)
	DB.SetConnMaxIdleTime(connMaxIdleTime)

	// Bounded connect retry, mirroring the Temporal startup loop in
	// cmd/server/main.go. A single attempt loses the cold-boot race against
	// Postgres -- routine under Kubernetes, where pods start unordered -- and
	// losing it is not self-correcting: main() only migrates inside the
	// success branch, so the process runs on with an empty schema while
	// /health answers 200. Retrying here is what keeps that state from
	// existing in the first place.
	deadline := time.Now().Add(connectTimeout())
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := DB.PingContext(ctx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("failed to ping database after %d attempt(s): %w", attempt, err)
		}
		log.Warnf("⏳ Database not reachable yet (attempt %d): %v — retrying in %s", attempt, err, connectRetryInterval)
		time.Sleep(connectRetryInterval)
	}

	log.Println("✅ Database connection established")
	return nil
}

// Close closes the database connection
func Close() error {
	if DB != nil {
		return DB.Close()
	}
	return nil
}

// GetDB returns the database connection
func GetDB() *sql.DB {
	return DB
}

