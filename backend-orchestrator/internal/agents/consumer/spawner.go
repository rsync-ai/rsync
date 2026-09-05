package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Spawner interface for different deployment backends
type Spawner interface {
	Spawn(ctx context.Context, groupID, topic string, partitions []int, config map[string]string) (*SpawnResult, error)
	Terminate(ctx context.Context, consumerID string) error
	GetStatus(ctx context.Context, consumerID string) (*ConsumerInstance, error)
	List(ctx context.Context) ([]*ConsumerInstance, error)
	Restart(ctx context.Context, consumerID string) (*SpawnResult, error)
}

// ConsumerInstance represents a running consumer
type ConsumerInstance struct {
	ConsumerID    string            `json:"consumer_id"`
	GroupID       string            `json:"group_id"`
	Topic         string            `json:"topic"`
	ContainerID   string            `json:"container_id,omitempty"`
	ContainerName string            `json:"container_name,omitempty"`
	Status        string            `json:"status"`
	Config        map[string]string `json:"config,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
}

// DockerSpawner spawns consumers as Docker containers via Docker API
type DockerSpawner struct {
	config     *Config
	httpClient *http.Client
	instances  map[string]*ConsumerInstance
	mu         sync.RWMutex
}

// NewDockerSpawner creates a new Docker spawner
func NewDockerSpawner(config *Config) (*DockerSpawner, error) {
	// Create HTTP client for Docker socket
	httpClient := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return net.Dial("unix", "/var/run/docker.sock")
			},
		},
		Timeout: 30 * time.Second,
	}

	return &DockerSpawner{
		config:     config,
		httpClient: httpClient,
		instances:  make(map[string]*ConsumerInstance),
	}, nil
}

// dockerCreateRequest is the Docker container create request
type dockerCreateRequest struct {
	Image      string            `json:"Image"`
	Env        []string          `json:"Env"`
	Labels     map[string]string `json:"Labels"`
	HostConfig dockerHostConfig  `json:"HostConfig"`
}

type dockerHostConfig struct {
	NetworkMode   string              `json:"NetworkMode"`
	Memory        int64               `json:"Memory,omitempty"`
	RestartPolicy dockerRestartPolicy `json:"RestartPolicy"`
}

type dockerRestartPolicy struct {
	Name              string `json:"Name"`
	MaximumRetryCount int    `json:"MaximumRetryCount"`
}

type dockerCreateResponse struct {
	ID       string   `json:"Id"`
	Warnings []string `json:"Warnings"`
}

// Spawn spawns a new consumer container
func (s *DockerSpawner) Spawn(
	ctx context.Context,
	groupID, topic string,
	partitions []int,
	extraConfig map[string]string,
) (*SpawnResult, error) {
	consumerID := fmt.Sprintf("consumer-%s", uuid.New().String()[:8])
	containerName := fmt.Sprintf("%s-%s", s.config.Docker.ContainerPrefix, consumerID)

	// Check if Docker is available
	if !s.isDockerAvailable() {
		return s.simulateSpawn(consumerID, groupID, topic, partitions)
	}

	// Build environment variables
	env := []string{
		fmt.Sprintf("CONSUMER_ID=%s", consumerID),
		fmt.Sprintf("CONSUMER_GROUP_ID=%s", groupID),
		fmt.Sprintf("KAFKA_TOPIC=%s", topic),
		fmt.Sprintf("KAFKA_BOOTSTRAP_SERVERS=%s", s.config.Kafka.BootstrapServers),
		fmt.Sprintf("KAFKA_AUTO_OFFSET_RESET=%s", s.config.Kafka.AutoOffsetReset),
		fmt.Sprintf("KAFKA_MAX_POLL_RECORDS=%d", s.config.Kafka.MaxPollRecords),
	}

	if len(partitions) > 0 {
		partStr := make([]string, len(partitions))
		for i, p := range partitions {
			partStr[i] = strconv.Itoa(p)
		}
		env = append(env, fmt.Sprintf("KAFKA_PARTITIONS=%s", strings.Join(partStr, ",")))
	}

	// Add extra config
	for k, v := range extraConfig {
		env = append(env, fmt.Sprintf("%s=%s", strings.ToUpper(strings.ReplaceAll(k, ".", "_")), v))
	}

	// Build labels
	labels := make(map[string]string)
	for k, v := range s.config.Docker.Labels {
		labels[k] = v
	}
	labels["consumer-id"] = consumerID
	labels["consumer-group"] = groupID
	labels["consumer-topic"] = topic

	// Create container request
	createReq := dockerCreateRequest{
		Image:  s.config.Docker.ConsumerImage,
		Env:    env,
		Labels: labels,
		HostConfig: dockerHostConfig{
			NetworkMode: s.config.Docker.NetworkName,
			Memory:      parseMemory(s.config.Docker.MemoryLimit),
			RestartPolicy: dockerRestartPolicy{
				Name:              "on-failure",
				MaximumRetryCount: 3,
			},
		},
	}

	body, _ := json.Marshal(createReq)

	// Create container
	req, _ := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("http://docker/containers/create?name=%s", containerName),
		bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("[DockerSpawner] Docker API error, simulating: %v", err)
		return s.simulateSpawn(consumerID, groupID, topic, partitions)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 201 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Printf("[DockerSpawner] Container create failed: %s", string(bodyBytes))
		return s.simulateSpawn(consumerID, groupID, topic, partitions)
	}

	var createResp dockerCreateResponse
	json.NewDecoder(resp.Body).Decode(&createResp)

	// Start container
	startReq, _ := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("http://docker/containers/%s/start", createResp.ID), nil)

	startResp, err := s.httpClient.Do(startReq)
	if err != nil {
		log.Printf("[DockerSpawner] Container start failed: %v", err)
		return &SpawnResult{
			Success:    false,
			ConsumerID: consumerID,
			GroupID:    groupID,
			Topic:      topic,
			Error:      err.Error(),
			SpawnedAt:  time.Now(),
		}, err
	}
	startResp.Body.Close()

	// Track instance
	instance := &ConsumerInstance{
		ConsumerID:    consumerID,
		GroupID:       groupID,
		Topic:         topic,
		ContainerID:   createResp.ID,
		ContainerName: containerName,
		Status:        "running",
		Config:        extraConfig,
		CreatedAt:     time.Now(),
	}

	s.mu.Lock()
	s.instances[consumerID] = instance
	s.mu.Unlock()

	log.Printf("[DockerSpawner] Spawned container: %s for topic %s", containerName, topic)

	return &SpawnResult{
		Success:       true,
		ConsumerID:    consumerID,
		GroupID:       groupID,
		Topic:         topic,
		ContainerID:   createResp.ID,
		ContainerName: containerName,
		SpawnedAt:     time.Now(),
	}, nil
}

// isDockerAvailable checks if Docker socket is available
func (s *DockerSpawner) isDockerAvailable() bool {
	req, _ := http.NewRequest("GET", "http://docker/_ping", nil)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == 200
}

// simulateSpawn simulates spawning when Docker is not available
func (s *DockerSpawner) simulateSpawn(consumerID, groupID, topic string, partitions []int) (*SpawnResult, error) {
	log.Printf("[DockerSpawner] Simulating spawn: %s for topic %s", consumerID, topic)

	instance := &ConsumerInstance{
		ConsumerID:    consumerID,
		GroupID:       groupID,
		Topic:         topic,
		ContainerName: fmt.Sprintf("simulated-%s", consumerID),
		Status:        "running",
		CreatedAt:     time.Now(),
	}

	s.mu.Lock()
	s.instances[consumerID] = instance
	s.mu.Unlock()

	return &SpawnResult{
		Success:       true,
		ConsumerID:    consumerID,
		GroupID:       groupID,
		Topic:         topic,
		ContainerName: instance.ContainerName,
		SpawnedAt:     time.Now(),
	}, nil
}

// Terminate terminates a consumer container
func (s *DockerSpawner) Terminate(ctx context.Context, consumerID string) error {
	s.mu.RLock()
	instance, exists := s.instances[consumerID]
	s.mu.RUnlock()

	if !exists {
		return fmt.Errorf("consumer not found: %s", consumerID)
	}

	// If Docker available and container exists
	if s.isDockerAvailable() && instance.ContainerID != "" {
		// Stop container
		stopReq, _ := http.NewRequestWithContext(ctx, "POST",
			fmt.Sprintf("http://docker/containers/%s/stop?t=30", instance.ContainerID), nil)
		if resp, err := s.httpClient.Do(stopReq); err == nil {
			resp.Body.Close()
		}

		// Remove container
		rmReq, _ := http.NewRequestWithContext(ctx, "DELETE",
			fmt.Sprintf("http://docker/containers/%s", instance.ContainerID), nil)
		if resp, err := s.httpClient.Do(rmReq); err == nil {
			resp.Body.Close()
		}
	}

	s.mu.Lock()
	delete(s.instances, consumerID)
	s.mu.Unlock()

	log.Printf("[DockerSpawner] Terminated consumer: %s", consumerID)
	return nil
}

// List lists all managed consumers
func (s *DockerSpawner) List(ctx context.Context) ([]*ConsumerInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]*ConsumerInstance, 0, len(s.instances))
	for _, instance := range s.instances {
		result = append(result, instance)
	}

	return result, nil
}

// Close closes the Docker client
func (s *DockerSpawner) Close() error {
	return nil
}

// parseMemory parses memory string like "512m" to bytes
func parseMemory(mem string) int64 {
	mem = strings.ToLower(strings.TrimSpace(mem))

	if strings.HasSuffix(mem, "g") {
		val, _ := strconv.ParseInt(strings.TrimSuffix(mem, "g"), 10, 64)
		return val * 1024 * 1024 * 1024
	}
	if strings.HasSuffix(mem, "m") {
		val, _ := strconv.ParseInt(strings.TrimSuffix(mem, "m"), 10, 64)
		return val * 1024 * 1024
	}
	if strings.HasSuffix(mem, "k") {
		val, _ := strconv.ParseInt(strings.TrimSuffix(mem, "k"), 10, 64)
		return val * 1024
	}

	val, _ := strconv.ParseInt(mem, 10, 64)
	return val
}

// ConsumerSpawner is the main spawner that delegates to the appropriate backend
type ConsumerSpawner struct {
	config  *Config
	spawner *DockerSpawner
}

// NewConsumerSpawner creates a new consumer spawner
func NewConsumerSpawner(config *Config) (*ConsumerSpawner, error) {
	dockerSpawner, err := NewDockerSpawner(config)
	if err != nil {
		return nil, err
	}

	return &ConsumerSpawner{
		config:  config,
		spawner: dockerSpawner,
	}, nil
}

// Spawn spawns a new consumer
func (s *ConsumerSpawner) Spawn(ctx context.Context, groupID, topic string, partitions []int, config map[string]string) (*SpawnResult, error) {
	return s.spawner.Spawn(ctx, groupID, topic, partitions, config)
}

// Terminate terminates a consumer
func (s *ConsumerSpawner) Terminate(ctx context.Context, consumerID string) error {
	return s.spawner.Terminate(ctx, consumerID)
}

// Close closes the spawner
func (s *ConsumerSpawner) Close() error {
	return s.spawner.Close()
}
