package config

import (
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config holds all application configuration
type Config struct {
	// Server configuration
	Server ServerConfig `mapstructure:"server"`

	// Database configuration
	Database DatabaseConfig `mapstructure:"database"`

	// Kafka configuration
	Kafka KafkaConfig `mapstructure:"kafka"`

	// Telemetry configuration
	Telemetry TelemetryConfig `mapstructure:"telemetry"`

	// Redis configuration
	Redis RedisConfig `mapstructure:"redis"`

	// Feature flags
	Features FeatureFlags `mapstructure:"features"`

	// MCP Connectors
	MCP MCPConfig `mapstructure:"mcp"`
}

// ServerConfig holds HTTP server settings
type ServerConfig struct {
	Port        string        `mapstructure:"port"`
	Environment string        `mapstructure:"environment"`
	Debug       bool          `mapstructure:"debug"`
	LogFormat   string        `mapstructure:"log_format"` // "json" or "text"
	LogLevel    string        `mapstructure:"log_level"`  // "debug", "info", "warn", "error"
	Timeout     time.Duration `mapstructure:"timeout"`
}

// DatabaseConfig holds PostgreSQL connection settings
type DatabaseConfig struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Name     string `mapstructure:"name"`
	SSLMode  string `mapstructure:"sslmode"`
}

// ConnectionString returns the PostgreSQL connection string
func (d *DatabaseConfig) ConnectionString() string {
	return "host=" + d.Host +
		" port=" + d.Port +
		" user=" + d.User +
		" password=" + d.Password +
		" dbname=" + d.Name +
		" sslmode=" + d.SSLMode
}

// KafkaConfig holds Kafka connection settings
type KafkaConfig struct {
	Brokers string `mapstructure:"brokers"`
	// GroupID is a consumer-group PREFIX, not a literal group id, and it arrives
	// here already qualified with the deployment's Kafka namespace — see
	// kafkaGroupID in kafka_identity.go for what that means and why it applies to
	// an operator-supplied value.
	GroupID string `mapstructure:"group_id"`
}

// BrokerList returns brokers as a slice
func (k *KafkaConfig) BrokerList() []string {
	return strings.Split(k.Brokers, ",")
}

// TelemetryConfig holds OpenTelemetry settings
type TelemetryConfig struct {
	// OTLP Endpoint (typically the sidecar at localhost:4317)
	OTLPEndpoint string `mapstructure:"otlp_endpoint"`

	// Service name for tracing
	ServiceName string `mapstructure:"service_name"`

	// Service version
	ServiceVersion string `mapstructure:"service_version"`

	// Enable/disable telemetry
	Enabled bool `mapstructure:"enabled"`

	// Sampling rate (0.0 to 1.0, 1.0 = always sample)
	SamplingRate float64 `mapstructure:"sampling_rate"`

	// Use insecure connection (for local dev)
	Insecure bool `mapstructure:"insecure"`
}

// RedisConfig holds Redis connection settings
type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

// Address returns the Redis address
func (r *RedisConfig) Address() string {
	return r.Host + ":" + r.Port
}

// FeatureFlags holds feature toggles
type FeatureFlags struct {
	EnableScheduler                 bool `mapstructure:"enable_scheduler"`
	EnableConsumerAgent             bool `mapstructure:"enable_consumer_agent"`
	EnableRetentionAgent            bool `mapstructure:"enable_retention_agent"`
	ConsumerAutoScale               bool `mapstructure:"consumer_auto_scale"`
	ConsumerAutoRestart             bool `mapstructure:"consumer_auto_restart"`
	EnableInfraPreflightSweep       bool `mapstructure:"enable_infra_preflight_sweep"`
	InfraPreflightSweepIntervalSecs int  `mapstructure:"infra_preflight_sweep_interval_secs"`
}

// MCPConfig holds MCP connector settings
type MCPConfig struct {
	ToolsDir string `mapstructure:"tools_dir"`
}

// LoadConfig loads configuration from environment variables and .env file
func LoadConfig() (*Config, error) {
	v := viper.New()

	// Set config file (optional .env)
	v.SetConfigName(".env")
	v.SetConfigType("env")
	v.AddConfigPath(".")
	v.AddConfigPath("./backend-orchestrator")
	v.AddConfigPath("/app")

	// Read .env file if it exists (don't error if not found)
	_ = v.ReadInConfig()

	// Enable automatic environment variable binding
	v.AutomaticEnv()

	// Replace dots with underscores for env var names
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Set defaults
	setDefaults(v)

	// Build config struct
	cfg := &Config{
		Server: ServerConfig{
			Port:        v.GetString("PORT"),
			Environment: v.GetString("ENVIRONMENT"),
			Debug:       v.GetBool("DEBUG"),
			LogFormat:   v.GetString("LOG_FORMAT"),
			LogLevel:    v.GetString("LOG_LEVEL"),
			Timeout:     v.GetDuration("SERVER_TIMEOUT"),
		},
		Database: DatabaseConfig{
			Host:     v.GetString("DB_HOST"),
			Port:     v.GetString("DB_PORT"),
			User:     v.GetString("DB_USER"),
			Password: v.GetString("DB_PASSWORD"),
			Name:     v.GetString("DB_NAME"),
			SSLMode:  v.GetString("DB_SSLMODE"),
		},
		Kafka: KafkaConfig{
			Brokers: v.GetString("KAFKA_BROKERS"),
			GroupID: kafkaGroupID(v.GetString("KAFKA_GROUP_ID")),
		},
		Telemetry: TelemetryConfig{
			OTLPEndpoint:   v.GetString("OTEL_EXPORTER_OTLP_ENDPOINT"),
			ServiceName:    v.GetString("OTEL_SERVICE_NAME"),
			ServiceVersion: v.GetString("OTEL_SERVICE_VERSION"),
			Enabled:        v.GetBool("OTEL_ENABLED"),
			SamplingRate:   v.GetFloat64("OTEL_SAMPLING_RATE"),
			Insecure:       v.GetBool("OTEL_INSECURE"),
		},
		Redis: RedisConfig{
			Host:     v.GetString("REDIS_HOST"),
			Port:     v.GetString("REDIS_PORT"),
			Password: v.GetString("REDIS_PASSWORD"),
			DB:       v.GetInt("REDIS_DB"),
		},
		Features: FeatureFlags{
			EnableScheduler:                 v.GetBool("ENABLE_SCHEDULER"),
			EnableConsumerAgent:             v.GetBool("ENABLE_CONSUMER_AGENT"),
			EnableRetentionAgent:            v.GetBool("ENABLE_RETENTION_AGENT"),
			ConsumerAutoScale:               v.GetBool("CONSUMER_AUTO_SCALE"),
			ConsumerAutoRestart:             v.GetBool("CONSUMER_AUTO_RESTART"),
			EnableInfraPreflightSweep:       v.GetBool("ENABLE_INFRA_PREFLIGHT_SWEEP"),
			InfraPreflightSweepIntervalSecs: v.GetInt("INFRA_PREFLIGHT_SWEEP_INTERVAL_SECS"),
		},
		MCP: MCPConfig{
			ToolsDir: v.GetString("TOOLS_DIR"),
		},
	}

	return cfg, nil
}

// setDefaults sets default values for all configuration options
func setDefaults(v *viper.Viper) {
	// Server defaults
	v.SetDefault("PORT", "8080")
	v.SetDefault("ENVIRONMENT", "development")
	v.SetDefault("DEBUG", false)
	v.SetDefault("LOG_FORMAT", "text")
	v.SetDefault("LOG_LEVEL", "info")
	v.SetDefault("SERVER_TIMEOUT", "30s")

	// Database defaults
	v.SetDefault("DB_HOST", "localhost")
	v.SetDefault("DB_PORT", "5432")
	v.SetDefault("DB_USER", "user")
	v.SetDefault("DB_PASSWORD", "password")
	v.SetDefault("DB_NAME", "pipeline_db")
	v.SetDefault("DB_SSLMODE", "disable")

	// Kafka defaults
	v.SetDefault("KAFKA_BROKERS", "localhost:9092")
	v.SetDefault("KAFKA_GROUP_ID", "go-orchestrator-group")

	// Telemetry defaults (sidecar pattern: localhost:4317)
	v.SetDefault("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
	v.SetDefault("OTEL_SERVICE_NAME", "orchestrator")
	v.SetDefault("OTEL_SERVICE_VERSION", "1.0.0")
	v.SetDefault("OTEL_ENABLED", true)
	v.SetDefault("OTEL_SAMPLING_RATE", 1.0) // Sample all in dev
	v.SetDefault("OTEL_INSECURE", true)     // Sidecar is local

	// Redis defaults
	v.SetDefault("REDIS_HOST", "localhost")
	v.SetDefault("REDIS_PORT", "6379")
	v.SetDefault("REDIS_PASSWORD", "")
	v.SetDefault("REDIS_DB", 0)

	// Feature flags defaults
	v.SetDefault("ENABLE_SCHEDULER", true)
	v.SetDefault("ENABLE_CONSUMER_AGENT", true)
	// Retention agent defaults OFF: it is currently non-functional (its
	// BootstrapServers is never used by any Kafka client; all ops go through a
	// hardcoded, nonexistent localhost:8082 REST proxy; the bulk-load registry
	// has zero callers; failures silently "simulate" success). Starting it only
	// emits a misleading "started" log and runs dead goroutines. Opt in
	// explicitly once it has a real Kafka admin client. See CAPABILITIES.md.
	v.SetDefault("ENABLE_RETENTION_AGENT", false)
	v.SetDefault("CONSUMER_AUTO_SCALE", true)
	v.SetDefault("CONSUMER_AUTO_RESTART", true)
	// Infra preflight sweep: warm the core MCP connectors (minio, kafka-mcp-sink,
	// debezium) at orchestrator boot, and every N secs if interval > 0. Off by
	// default — the per-pipeline preflight already ensures connectors on demand;
	// this is an opt-in warm-up to avoid a cold-start on the first pipeline.
	v.SetDefault("ENABLE_INFRA_PREFLIGHT_SWEEP", false)
	v.SetDefault("INFRA_PREFLIGHT_SWEEP_INTERVAL_SECS", 0)

	// MCP defaults
	v.SetDefault("TOOLS_DIR", "../../shared/mcp-connectors")
}
