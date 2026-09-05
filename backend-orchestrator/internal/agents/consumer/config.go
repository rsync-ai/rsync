package consumer

import (
	"os"
	"strconv"
	"strings"

	kafkaclient "github.com/rsync-ai/shared/kafkaclient"
)

// DefaultBootstrapServers is the address this agent falls back to when the
// deployment names no brokers at all — the standalone/dev shape, where the
// process and the broker share a host.
const DefaultBootstrapServers = "localhost:9092"

// kafkaServiceName names the PROCESS this agent runs inside, not the agent, so
// the customer's broker logs and quota metrics attribute the connection to
// "rsync-orchestrator". KAFKA_CLIENT_ID still overrides it.
const kafkaServiceName = "orchestrator"

// Config holds all configuration for the Consumer Registry Agent
type Config struct {
	Kafka   KafkaConfig
	Scaling ScalingConfig
	Health  HealthConfig
	Docker  DockerConfig

	// Consumer group naming
	ConsumerGroupPrefix string

	// Topics to auto-manage
	ManagedTopicPatterns []string

	// Topics to exclude
	ExcludedTopics []string
}

// KafkaConfig holds Kafka connection configuration
type KafkaConfig struct {
	BootstrapServers    string
	AutoOffsetReset     string
	EnableAutoCommit    bool
	MaxPollRecords      int
	MaxPollIntervalMs   int
	SessionTimeoutMs    int
	HeartbeatIntervalMs int
}

// ScalingConfig holds auto-scaling configuration
type ScalingConfig struct {
	// Lag thresholds
	LagScaleUpThreshold   int64
	LagScaleDownThreshold int64

	// Consumer limits
	MinConsumersPerTopic int
	MaxConsumersPerTopic int

	// Scaling behavior
	ScaleUpIncrement   int
	ScaleDownIncrement int
	ScaleCooldownSecs  int

	// Throughput-based scaling
	MessagesPerConsumer int

	// Partition-aware scaling
	MatchConsumerToPartitions bool
}

// HealthConfig holds health monitoring configuration
type HealthConfig struct {
	// Monitoring intervals
	HealthCheckIntervalSecs       int
	MetricsCollectionIntervalSecs int

	// Health thresholds
	MaxConsecutiveFailures int
	HeartbeatTimeoutSecs   int

	// Auto-restart
	AutoRestartEnabled       bool
	RestartDelaySecs         int
	MaxRestartAttempts       int
	RestartBackoffMultiplier float64
}

// DockerConfig holds Docker spawner configuration
type DockerConfig struct {
	NetworkName     string
	ConsumerImage   string
	ContainerPrefix string
	MemoryLimit     string
	CPULimit        string
	Labels          map[string]string
}

// DefaultConfig returns configuration with default values
func DefaultConfig() *Config {
	return &Config{
		Kafka: KafkaConfig{
			BootstrapServers:    bootstrapServersFromEnv(),
			AutoOffsetReset:     "earliest",
			EnableAutoCommit:    false,
			MaxPollRecords:      500,
			MaxPollIntervalMs:   300000,
			SessionTimeoutMs:    30000,
			HeartbeatIntervalMs: 10000,
		},
		Scaling: ScalingConfig{
			LagScaleUpThreshold:       50000,
			LagScaleDownThreshold:     1000,
			MinConsumersPerTopic:      1,
			MaxConsumersPerTopic:      10,
			ScaleUpIncrement:          1,
			ScaleDownIncrement:        1,
			ScaleCooldownSecs:         300,
			MessagesPerConsumer:       10000,
			MatchConsumerToPartitions: true,
		},
		Health: HealthConfig{
			HealthCheckIntervalSecs:       30,
			MetricsCollectionIntervalSecs: 60,
			MaxConsecutiveFailures:        3,
			HeartbeatTimeoutSecs:          60,
			AutoRestartEnabled:            true,
			RestartDelaySecs:              10,
			MaxRestartAttempts:            5,
			RestartBackoffMultiplier:      2.0,
		},
		Docker: DockerConfig{
			NetworkName:     getEnv("DOCKER_NETWORK", "rsync-ai-mcp"),
			ConsumerImage:   getEnv("CONSUMER_IMAGE", "rsync-ai/consumer:latest"),
			ContainerPrefix: "rsync-consumer",
			MemoryLimit:     "512m",
			CPULimit:        "0.5",
			Labels: map[string]string{
				"managed-by": "consumer-registry",
				"app":        "rsync-ai",
			},
		},
		ConsumerGroupPrefix: "rsync-pipeline",
		ManagedTopicPatterns: []string{
			"cdc.*",
			"conn.*",
			"transformed.*",
		},
		ExcludedTopics: []string{
			"__consumer_offsets",
			"_schemas",
		},
	}
}

// FromEnv creates configuration from environment variables
func FromEnv() *Config {
	config := DefaultConfig()

	// Override from environment
	if val := os.Getenv("CONSUMER_LAG_SCALE_UP"); val != "" {
		if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			config.Scaling.LagScaleUpThreshold = v
		}
	}

	if val := os.Getenv("CONSUMER_LAG_SCALE_DOWN"); val != "" {
		if v, err := strconv.ParseInt(val, 10, 64); err == nil {
			config.Scaling.LagScaleDownThreshold = v
		}
	}

	if val := os.Getenv("CONSUMER_MAX_PER_TOPIC"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			config.Scaling.MaxConsumersPerTopic = v
		}
	}

	if val := os.Getenv("CONSUMER_MIN_PER_TOPIC"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			config.Scaling.MinConsumersPerTopic = v
		}
	}

	if val := os.Getenv("CONSUMER_HEALTH_INTERVAL"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			config.Health.HealthCheckIntervalSecs = v
		}
	}

	if val := os.Getenv("CONSUMER_AUTO_RESTART"); val != "" {
		config.Health.AutoRestartEnabled = strings.ToLower(val) == "true"
	}

	if val := os.Getenv("CONSUMER_MAX_RESTARTS"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			config.Health.MaxRestartAttempts = v
		}
	}

	if val := os.Getenv("CONSUMER_SCALE_COOLDOWN"); val != "" {
		if v, err := strconv.Atoi(val); err == nil {
			config.Scaling.ScaleCooldownSecs = v
		}
	}

	if val := os.Getenv("CONSUMER_GROUP_PREFIX"); val != "" {
		config.ConsumerGroupPrefix = val
	}

	return config
}

// bootstrapServersFromEnv resolves the broker list under exactly the precedence
// kafkaclient.FromEnv applies: KAFKA_BROKERS first, then
// KAFKA_BOOTSTRAP_SERVERS, then the standalone default.
//
// Only the second name was read here, and no shipped compose file gives the
// orchestrator that variable — it is handed KAFKA_BROKERS (docker-compose.yml:713).
// This agent is on by default (ENABLE_CONSUMER_AGENT, config.go:239), so in every
// deployment including prod it resolved localhost:9092: a Kafka client pointed at
// an address the container can never reach, whose lag readings and auto-restart
// decisions therefore describe a phantom cluster, and whose address spawner.go
// stamps into every consumer container it launches.
func bootstrapServersFromEnv() string {
	if v := strings.TrimSpace(os.Getenv(kafkaclient.EnvBrokers)); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv(kafkaclient.EnvBootstrapServers)); v != "" {
		return v
	}
	return DefaultBootstrapServers
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
