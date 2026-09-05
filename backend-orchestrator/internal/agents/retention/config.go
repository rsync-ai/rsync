package retention

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all configuration for the Retention Manager Agent
type Config struct {
	Kafka      KafkaConfig
	Retention  RetentionConfig
	Safety     SafetyConfig
	Archive    ArchiveConfig
	Monitoring MonitoringConfig
}

// KafkaConfig holds Kafka connection configuration
type KafkaConfig struct {
	BootstrapServers string
	AdminTimeout     time.Duration
}

// RetentionConfig holds retention management configuration
type RetentionConfig struct {
	// Default retention (7 days in ms)
	DefaultRetentionMs int64

	// Reduced retention for cleanup (1 hour in ms)
	CleanupRetentionMs int64

	// Minimum messages to trigger bulk detection
	BulkLoadThreshold int64

	// Whether to archive before cleanup
	ArchiveBeforeCleanup bool

	// Restore retention after cleanup
	RestoreRetentionAfterCleanup bool
}

// SafetyConfig holds safety check configuration
type SafetyConfig struct {
	// Minimum time before cleanup (after bulk load registered)
	MinWaitAfterLoadMins int

	// Grace period after all consumers caught up
	GracePeriodMins int

	// Maximum lag allowed for any consumer before cleanup
	MaxLagBeforeCleanup int64

	// Require all known consumers caught up
	RequireAllConsumersCaughtUp bool

	// Minimum consumers that must be caught up
	MinConsumersCaughtUp int
}

// ArchiveConfig holds S3 archival configuration
type ArchiveConfig struct {
	Enabled         bool
	S3Bucket        string
	S3Prefix        string
	S3Region        string
	CompressionType string // "gzip", "snappy", "none"
}

// MonitoringConfig holds monitoring configuration
type MonitoringConfig struct {
	// How often to check consumer progress
	CheckIntervalSecs int

	// How often to check for cleanup opportunities
	CleanupCheckIntervalSecs int

	// How often to restore modified retentions
	RestoreCheckIntervalSecs int
}

// DefaultConfig returns configuration with default values
func DefaultConfig() *Config {
	return &Config{
		Kafka: KafkaConfig{
			BootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
			AdminTimeout:     30 * time.Second,
		},
		Retention: RetentionConfig{
			DefaultRetentionMs:           7 * 24 * 60 * 60 * 1000, // 7 days
			CleanupRetentionMs:           1 * 60 * 60 * 1000,      // 1 hour
			BulkLoadThreshold:            100000,                  // 100K messages
			ArchiveBeforeCleanup:         false,
			RestoreRetentionAfterCleanup: true,
		},
		Safety: SafetyConfig{
			MinWaitAfterLoadMins:        30,
			GracePeriodMins:             15,
			MaxLagBeforeCleanup:         1000,
			RequireAllConsumersCaughtUp: true,
			MinConsumersCaughtUp:        1,
		},
		Archive: ArchiveConfig{
			Enabled:         false,
			S3Bucket:        getEnv("RETENTION_S3_BUCKET", ""),
			S3Prefix:        getEnv("RETENTION_S3_PREFIX", "kafka-archive"),
			S3Region:        getEnv("AWS_REGION", "us-east-1"),
			CompressionType: "gzip",
		},
		Monitoring: MonitoringConfig{
			CheckIntervalSecs:        60,
			CleanupCheckIntervalSecs: 300,
			RestoreCheckIntervalSecs: 3600,
		},
	}
}

// FromEnv creates configuration from environment variables
func FromEnv() *Config {
	config := DefaultConfig()

	// Kafka
	if val := os.Getenv("KAFKA_BOOTSTRAP_SERVERS"); val != "" {
		config.Kafka.BootstrapServers = val
	}

	// Retention
	if val := os.Getenv("RETENTION_DEFAULT_MS"); val != "" {
		if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			config.Retention.DefaultRetentionMs = v
		}
	}

	if val := os.Getenv("RETENTION_CLEANUP_MS"); val != "" {
		if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			config.Retention.CleanupRetentionMs = v
		}
	}

	if val := os.Getenv("RETENTION_BULK_THRESHOLD"); val != "" {
		if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			config.Retention.BulkLoadThreshold = v
		}
	}

	if val := os.Getenv("RETENTION_ARCHIVE_BEFORE_CLEANUP"); val != "" {
		config.Retention.ArchiveBeforeCleanup = strings.ToLower(val) == "true"
	}

	// Safety
	if val := os.Getenv("RETENTION_MIN_WAIT_MINS"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			config.Safety.MinWaitAfterLoadMins = v
		}
	}

	if val := os.Getenv("RETENTION_GRACE_PERIOD_MINS"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			config.Safety.GracePeriodMins = v
		}
	}

	if val := os.Getenv("RETENTION_MAX_LAG"); val != "" {
		if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			config.Safety.MaxLagBeforeCleanup = v
		}
	}

	// Archive
	if val := os.Getenv("RETENTION_ARCHIVE_ENABLED"); val != "" {
		config.Archive.Enabled = strings.ToLower(val) == "true"
	}

	if val := os.Getenv("RETENTION_S3_BUCKET"); val != "" {
		config.Archive.S3Bucket = val
		if config.Archive.S3Bucket != "" {
			config.Archive.Enabled = true
		}
	}

	// Monitoring
	if val := os.Getenv("RETENTION_CHECK_INTERVAL"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			config.Monitoring.CheckIntervalSecs = v
		}
	}

	if val := os.Getenv("RETENTION_CLEANUP_CHECK_INTERVAL"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			config.Monitoring.CleanupCheckIntervalSecs = v
		}
	}

	return config
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
