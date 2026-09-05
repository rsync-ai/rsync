package handlers

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"api-gateway/internal/db"
	rsynckafka "api-gateway/internal/kafka"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/rsync-ai/shared/kafkaclient"
)

type serviceHealth struct {
	Service   string `json:"service"`
	Status    string `json:"status"`
	LatencyMs int64  `json:"latency_ms"`
	Error     string `json:"error,omitempty"`
}

// AdminSystemHealth checks connectivity to core infrastructure services.
// GET /api/v1/admin/health
func AdminSystemHealth(c *gin.Context) {
	services := make([]serviceHealth, 0, 4)

	// PostgreSQL
	services = append(services, checkPostgres())

	// Redis
	services = append(services, checkRedis())

	// Kafka
	services = append(services, checkKafka())

	// Temporal
	services = append(services, checkTemporal())

	c.JSON(http.StatusOK, gin.H{"services": services})
}

func checkPostgres() serviceHealth {
	start := time.Now()
	database := db.GetDB()
	if database == nil {
		return serviceHealth{Service: "postgresql", Status: "down", LatencyMs: 0, Error: "not connected"}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := database.PingContext(ctx)
	latency := time.Since(start).Milliseconds()

	if err != nil {
		return serviceHealth{Service: "postgresql", Status: "down", LatencyMs: latency, Error: err.Error()}
	}
	return serviceHealth{Service: "postgresql", Status: "up", LatencyMs: latency}
}

func checkRedis() serviceHealth {
	start := time.Now()

	redisAddr := os.Getenv("REDIS_HOST")
	if redisAddr == "" {
		redisAddr = "redis"
	}
	redisPort := os.Getenv("REDIS_PORT")
	if redisPort == "" {
		redisPort = "6379"
	}
	addr := redisAddr + ":" + redisPort

	client := redis.NewClient(&redis.Options{
		Addr:        addr,
		Password:    os.Getenv("REDIS_PASSWORD"),
		DialTimeout: 3 * time.Second,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := client.Ping(ctx).Err()
	latency := time.Since(start).Milliseconds()

	if err != nil {
		return serviceHealth{Service: "redis", Status: "down", LatencyMs: latency, Error: err.Error()}
	}
	return serviceHealth{Service: "redis", Status: "up", LatencyMs: latency}
}

// kafkaProbeBudget bounds the whole Kafka check, however many bootstrap
// entries there are, so this endpoint's latency does not grow with the size of
// the customer's cluster. With a single broker each attempt still gets the same
// 3s the probe has always had.
const kafkaProbeBudget = 3 * time.Second

func checkKafka() serviceHealth {
	brokers := kafkaProbeBrokers()

	// Dial through the same dialer the real clients use. A plain DialContext
	// against a SASL-required cluster reports "kafka: down" for a broker that
	// is perfectly healthy — the probe would be measuring its own missing
	// credentials rather than the broker.
	dialer := rsynckafka.Dialer(brokers)

	return probeKafkaBrokers(brokers, func(ctx context.Context, addr string) error {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err != nil {
			return err
		}
		defer conn.Close()

		if deadline, ok := ctx.Deadline(); ok {
			conn.SetDeadline(deadline)
		}
		_, err = conn.Brokers()
		return err
	})
}

// kafkaProbeBrokers resolves the bootstrap list the probe should try.
// ParseBrokers, not strings.Split: "b1:9092, b2:9092" is a list of two brokers,
// not one broker and one address with a leading space.
func kafkaProbeBrokers() []string {
	brokers := kafkaclient.ParseBrokers(os.Getenv("KAFKA_BROKERS"))
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	return brokers
}

// probeKafkaBrokers reports the cluster up if ANY bootstrap entry answers.
//
// A bootstrap list is a list of entry points into one cluster, so a probe that
// only ever dials brokers[0] reports "kafka: down" for a fully healthy cluster
// whenever its first entry happens to be the one being restarted — an
// observability lie that sends an operator chasing an outage that is not
// happening. Same shape as dialBroker in the kafka-mcp-sink worker
// (cmd/kafka-sink-worker/kafka_security.go:108): try each, first success wins,
// report the last error only if all of them failed.
func probeKafkaBrokers(brokers []string, probe func(ctx context.Context, addr string) error) serviceHealth {
	start := time.Now()
	if len(brokers) == 0 {
		return serviceHealth{Service: "kafka", Status: "down", Error: "no kafka brokers configured"}
	}

	// Split the budget evenly rather than sharing one deadline: a single
	// unreachable entry that hangs until timeout would otherwise consume it and
	// every remaining broker would be reported down without being tried.
	perBroker := kafkaProbeBudget / time.Duration(len(brokers))

	var lastErr error
	for _, addr := range brokers {
		ctx, cancel := context.WithTimeout(context.Background(), perBroker)
		err := probe(ctx, addr)
		cancel()
		if err == nil {
			return serviceHealth{Service: "kafka", Status: "up", LatencyMs: time.Since(start).Milliseconds()}
		}
		// Name the address: with several brokers a bare "connection refused"
		// does not say which one, and that is the whole question being asked.
		lastErr = fmt.Errorf("%s: %w", addr, err)
	}
	return serviceHealth{
		Service:   "kafka",
		Status:    "down",
		LatencyMs: time.Since(start).Milliseconds(),
		Error:     lastErr.Error(),
	}
}

func checkTemporal() serviceHealth {
	start := time.Now()

	temporalAddr := os.Getenv("TEMPORAL_ADDRESS")
	if temporalAddr == "" {
		temporalAddr = "temporal:7233"
	}

	// Simple TCP connectivity check (avoids needing grpc health package)
	conn, err := net.DialTimeout("tcp", temporalAddr, 3*time.Second)
	latency := time.Since(start).Milliseconds()

	if err != nil {
		return serviceHealth{Service: "temporal", Status: "down", LatencyMs: latency, Error: err.Error()}
	}
	conn.Close()
	return serviceHealth{Service: "temporal", Status: "up", LatencyMs: latency}
}
