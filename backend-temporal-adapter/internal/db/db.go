package db

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"

	_ "github.com/rsync-ai/shared/pgdriver"
	log "github.com/sirupsen/logrus"
)

var db *sql.DB

// Init initializes the database connection
func Init() error {
	host := getEnv("POSTGRES_HOST", "postgres")
	port := getEnv("POSTGRES_PORT", "5432")
	user := getEnv("POSTGRES_USER", "rsync")
	password := getEnv("POSTGRES_PASSWORD", "rsyncpass")
	dbname := getEnv("POSTGRES_DB", "rsyncdb")

	sslmode := getEnv("POSTGRES_SSLMODE", "disable")
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslmode)

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	// Set connection pool settings. Configurable so a shared managed DB
	// (Azure/RDS) with a low max_connections can be sized across all services.
	maxOpen := 25
	if v := os.Getenv("POSTGRES_MAX_OPEN_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxOpen = n
		}
	}
	maxIdle := 5
	if v := os.Getenv("POSTGRES_MAX_IDLE_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxIdle = n
		}
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)

	log.WithFields(log.Fields{
		"host":   host,
		"port":   port,
		"dbname": dbname,
	}).Info("✅ Database connection established")

	return nil
}

// GetDB returns the database connection
func GetDB() *sql.DB {
	return db
}

// Close closes the database connection
func Close() error {
	if db != nil {
		return db.Close()
	}
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
