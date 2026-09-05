package sentinel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/rsync-ai/shared/kafkaclient"
	"net/http"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"

	"github.com/rsync-ai/backend-orchestrator/internal/connectorpaths"
	"github.com/rsync-ai/backend-orchestrator/internal/kafka"
	"github.com/rsync-ai/backend-orchestrator/internal/mcp"
)

// kafkaBrokerProbe is the one method of *kafka.Manager the broker health check needs.
// Narrow on purpose: it keeps the check unit-testable without a live cluster, and it makes
// the dependency a single round trip instead of the whole manager.
type kafkaBrokerProbe interface {
	Ping() error
}

// HealthMonitor monitors the health of all system components
type HealthMonitor struct {
	kafkaManager *kafka.Manager
	// kafkaProbe is kafkaManager again, narrowed. Nil when this process has no manager —
	// see NewHealthMonitor for why it is not simply assigned.
	kafkaProbe kafkaBrokerProbe
	db         *sql.DB
	config     *SentinelConfig
	logger     *AuditLogger

	// Component tracking
	componentHealth map[string]*ComponentHealth
	mu              sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// HTTP client for connector health checks
	httpClient *http.Client
}

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(kafkaManager *kafka.Manager, db *sql.DB, config *SentinelConfig, logger *AuditLogger) *HealthMonitor {
	h := &HealthMonitor{
		kafkaManager:    kafkaManager,
		db:              db,
		config:          config,
		logger:          logger,
		componentHealth: make(map[string]*ComponentHealth),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
	// Assigned only when non-nil. A nil *kafka.Manager put straight into an interface
	// field yields a NON-nil interface holding a nil pointer, so `h.kafkaProbe != nil`
	// would pass and the check would call Ping() on a nil receiver.
	if kafkaManager != nil {
		h.kafkaProbe = kafkaManager
	}
	return h
}

// Start starts the health monitor
func (h *HealthMonitor) Start(ctx context.Context) error {
	h.ctx, h.cancel = context.WithCancel(ctx)

	// Start background monitoring loops
	h.wg.Add(4)
	go h.monitorKafkaConsumers()
	go h.monitorMCPConnectors()
	go h.monitorInfrastructure()
	go h.pruneStaleComponentsLoop()

	return nil
}

// Stop stops the health monitor
func (h *HealthMonitor) Stop() {
	if h.cancel != nil {
		h.cancel()
	}
	h.wg.Wait()
}

// RecordHeartbeat records a heartbeat from a component
func (h *HealthMonitor) RecordHeartbeat(componentID string, health *ComponentHealth) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.componentHealth[componentID] = health

	// Persist to database
	go h.persistHealthToDB(health)
}

// persistHealthToDB persists component health to the database.
//
// last_error and metadata are written even though the original upsert omitted them: both
// are columns GET /api/v1/monitoring/sentinel/health selects
// (api-gateway/internal/handlers/monitoring.go:394), so leaving them out returned a status
// with no explanation attached to it. They are written unconditionally, blank included, so
// a component that recovers stops reporting the failure it recovered from.
func (h *HealthMonitor) persistHealthToDB(health *ComponentHealth) {
	if h.db == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	metadataJSON := []byte("{}")
	if len(health.Metadata) > 0 {
		if encoded, err := json.Marshal(health.Metadata); err == nil {
			metadataJSON = encoded
		} else {
			log.WithError(err).WithField("component_id", health.ComponentID).
				Debug("Failed to encode component health metadata; persisting empty object")
		}
	}

	_, err := h.db.ExecContext(ctx, `
		INSERT INTO sentinel_component_health (
			component_id, component_type, status, last_heartbeat,
			messages_processed, error_count, consumer_lag, last_error, metadata, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
		ON CONFLICT (component_id) DO UPDATE SET
			status = EXCLUDED.status,
			last_heartbeat = EXCLUDED.last_heartbeat,
			messages_processed = EXCLUDED.messages_processed,
			error_count = EXCLUDED.error_count,
			consumer_lag = EXCLUDED.consumer_lag,
			last_error = EXCLUDED.last_error,
			metadata = EXCLUDED.metadata,
			updated_at = NOW()
	`,
		health.ComponentID, health.ComponentType, health.Status,
		health.LastHeartbeat, health.MessagesProcessed, health.ErrorCount,
		health.ConsumerLag, health.LastError, string(metadataJSON),
	)

	if err != nil {
		log.WithError(err).WithField("component_id", health.ComponentID).Debug("Failed to persist component health")
	}
}

// recordInfraHealth stores one infrastructure component's verdict and publishes it.
//
// Publishing is the point. Before this existed, all three infrastructure checks wrote only
// to h.componentHealth — a map that no code outside this file reads — so an unhealthy
// PostgreSQL, Kafka or Kafka Connect was detected correctly and then told nobody. The row
// is what the monitoring API and its infrastructure summary
// (api-gateway/internal/handlers/monitoring.go:394 and :722) read.
//
// The write is synchronous, unlike RecordHeartbeat's `go h.persistHealthToDB(...)`: this
// runs on a 30s ticker over three components, so there is nothing to gain from a goroutine
// and a caller can be sure the row landed.
func (h *HealthMonitor) recordInfraHealth(componentID string, status HealthStatus, lastErr string, metadata map[string]interface{}) {
	h.mu.Lock()
	health, exists := h.componentHealth[componentID]
	if !exists {
		health = &ComponentHealth{
			ComponentID:   componentID,
			ComponentType: ComponentTypeInfrastructure,
			Metadata:      make(map[string]interface{}),
		}
		h.componentHealth[componentID] = health
	}
	health.Status = status
	// Assigned unconditionally, empty string included. Leaving the previous text in place
	// on recovery would leave a healthy component permanently reporting a failure it has
	// already come back from.
	health.LastError = lastErr
	health.UpdatedAt = time.Now()
	// These checks ARE the component's heartbeat — nothing else reports for
	// infrastructure, and last_heartbeat is NOT NULL.
	health.LastHeartbeat = health.UpdatedAt
	for k, v := range metadata {
		health.Metadata[k] = v
	}

	// Copied, map included, so the persist below runs outside the lock without aliasing
	// state a concurrent check may be mutating.
	snapshot := *health
	snapshot.Metadata = make(map[string]interface{}, len(health.Metadata))
	for k, v := range health.Metadata {
		snapshot.Metadata[k] = v
	}
	h.mu.Unlock()

	h.persistHealthToDB(&snapshot)
}

// RecordHealthChange records a health status change and persists it.
//
// The persist is the point. This function used to write only to h.componentHealth — a map
// no code outside this file reads — so its three callers each detected a real failure
// correctly and then told nobody: a closed consumer group (checkKafkaConsumerLag), consumer
// lag (same loop), and the widest one, the heartbeat-timeout sweep that marks a component
// dead (sentinel.go performHealthCheck). The row in sentinel_component_health is what
// GET /api/v1/monitoring/sentinel/health reads; without it "the component died" reached a
// log line and nothing else. This is the same omission #731 T9 fixed for the three
// infrastructure checks by introducing recordInfraHealth — these callers were left behind
// on the old route, which is why health_monitor_persist_census_test.go now enforces the
// rule against the source rather than trusting the next reader to notice.
//
// The argument is copied rather than stored. performHealthCheck passes a *ComponentHealth
// owned by the Sentinel agent's own map and guarded by the agent's mutex; storing that
// pointer here published one struct into two maps under two different locks. Copying also
// means the value handed to persistHealthToDB cannot be mutated underneath it by the
// owning agent while the write is in flight.
func (h *HealthMonitor) RecordHealthChange(componentID string, health *ComponentHealth) {
	if health == nil {
		return
	}

	snapshot := *health
	snapshot.ComponentID = componentID
	snapshot.Metadata = make(map[string]interface{}, len(health.Metadata))
	for k, v := range health.Metadata {
		snapshot.Metadata[k] = v
	}
	if snapshot.UpdatedAt.IsZero() {
		snapshot.UpdatedAt = time.Now()
	}
	// last_heartbeat is NOT NULL (migration 011). The two consumer call sites never set it,
	// so persisting them unmodified would record a component that last reported in year 1.
	// Only the zero value is filled in: on the heartbeat-timeout path the stale timestamp
	// IS the evidence, and overwriting it would erase the reason the component was
	// declared dead.
	if snapshot.LastHeartbeat.IsZero() {
		snapshot.LastHeartbeat = snapshot.UpdatedAt
	}

	stored := snapshot
	h.mu.Lock()
	h.componentHealth[componentID] = &stored
	h.mu.Unlock()

	log.WithFields(log.Fields{
		"component_id":   componentID,
		"status":         snapshot.Status,
		"last_heartbeat": snapshot.LastHeartbeat,
	}).Info("Component health changed")

	h.persistHealthToDB(&snapshot)
}

// monitorKafkaConsumers monitors Kafka consumer health and lag
func (h *HealthMonitor) monitorKafkaConsumers() {
	defer h.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.checkKafkaConsumerLag()
		}
	}
}

// orchestratorConsumedTopics is the set of topics THIS process consumes, and is
// the only valid input to a consumer-liveness check.
//
// The invariant that matters: every entry must have a matching
// kafkaManager.ConsumeWithContext(<topic>, …) call inside the orchestrator
// binary, because IsConsumerActive answers from this Manager's own `consumers`
// map — a topic the orchestrator merely *produces* to has no entry there and so
// reports "inactive" forever.
//
// `pipeline.domain.events` used to be in this list and was exactly that mistake:
// the orchestrator only produces to it (cmd/orchestrator/main.go ProduceWithHeaders,
// sentinel/cdc_wal_watchdog.go, sentinel/cdc_sentinel.go), while its consumers all
// live in *other* processes — api-gateway's event projector, WebSocket bridge and
// domain-event handler, plus the Kafka sink worker. The check therefore logged
// "⚠️ Consumer group is closed or inactive" and pinned a permanently Unhealthy
// component on every tick, at the poll interval, forever. Do not re-add a topic
// here just because the orchestrator touches it — produce ≠ consume.
var orchestratorConsumedTopics = []string{
	"agent.control.commands.intent",
	"agent.control.commands.resolver",
	"agent.control.commands.discovery",
	"agent.control.commands.planner",
	"agent.control.commands.validator",
	"agent.control.commands.executor",
	"agent.control.commands.cost_estimator",
	"agent.control.commands.capability_resolver",
	"agent.control.commands.connection_validator",
}

// checkKafkaConsumerLag checks consumer lag and status for the per-agent
// command topics that drive the orchestrator. The list mirrors the workers
// in cmd/orchestrator/main.go — keep in sync when adding/removing agents.
//
// Lag is emitted both as health-system input (drives healing) and as an OTel
// gauge (sentinel.kafka.consumer_lag, labelled by topic + group). Sustained
// non-zero lag on a topic with active consumers usually indicates a slow
// worker; sudden growth on a previously-zero topic indicates a producer/
// consumer topic-name mismatch (the failure mode that left 2892 messages
// stranded on agent.control.commands before the publishAgentCommand fix).
func (h *HealthMonitor) checkKafkaConsumerLag() {
	ctx, span := sentinelTracer.Start(h.ctx, "check_consumer_lag")
	defer span.End()

	// Qualified here, not in the list above: the list is paired entry-by-entry
	// against the ConsumeWithContext literals in this module by
	// consumed_topics_test.go, and that invariant is about logical identity.
	// The broker lookup is the only place that needs the wire name.
	agentTopics := kafkaclient.Topics(orchestratorConsumedTopics...)

	// One lag fetch per topic — manager scopes the consumer-group name as
	// "<base-group>-<topic>", matching how ConsumeWithContext registers them.
	baseGroup := h.kafkaManager.Config.GroupID

	for _, topic := range agentTopics {
		isActive := h.kafkaManager.IsConsumerActive(topic)

		if !isActive {
			log.WithField("topic", topic).Warn("⚠️  Consumer group is closed or inactive")

			h.RecordHealthChange(topic, &ComponentHealth{
				ComponentID:   topic,
				ComponentType: ComponentTypeKafkaConsumer,
				Status:        HealthStatusUnhealthy,
				LastError:     "Consumer group closed",
				UpdatedAt:     time.Now(),
				Metadata: map[string]interface{}{
					"topic":      topic,
					"is_active":  false,
					"issue_type": "consumer_group_closed",
				},
			})
			continue
		}

		topicGroup := fmt.Sprintf("%s-%s", baseGroup, topic)
		lag, err := h.kafkaManager.GetConsumerGroupLag(topicGroup)
		if err != nil {
			log.WithError(err).WithField("topic", topic).Debug("Could not get consumer lag")
			continue
		}

		// GetConsumerGroupLag returns a topic→lag map; for our per-topic
		// groups there's only one entry but iterating is cheap and robust.
		topicLag := lag[topic]

		// Always emit the OTel gauge — including zero lag — so SigNoz can
		// distinguish "consumer healthy at zero" from "consumer not reporting".
		if h.logger != nil {
			h.logger.RecordConsumerLag(ctx, topic, topicGroup, topicLag)
		}

		if topicLag > 0 {
			log.WithFields(log.Fields{
				"topic": topic,
				"group": topicGroup,
				"lag":   topicLag,
			}).Debug("Consumer lag detected")

			h.RecordHealthChange(topic, &ComponentHealth{
				ComponentID:   topic,
				ComponentType: ComponentTypeKafkaConsumer,
				Status:        HealthStatusHealthy,
				ConsumerLag:   topicLag,
				UpdatedAt:     time.Now(),
			})
		}
	}

	span.SetAttributes(attribute.Int("topics_checked", len(agentTopics)))
}

// monitorMCPConnectors monitors MCP connector health
func (h *HealthMonitor) monitorMCPConnectors() {
	defer h.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.checkMCPConnectorHealth()
		}
	}
}

// mcpConnectorPort is the internal port every MCP connector listens on.
// scripts/mcp_generate_compose.py hardcodes PORT=8000/MCP_PORT=8000 and a
// healthcheck against localhost:8000/health for every generated service, so
// there is nothing per-connector to look up. The old query selected a `port`
// column from a table nobody wrote, which meant this check would have probed
// port 0 even if it had ever found a row.
const mcpConnectorPort = 8000

// checkMCPConnectorHealth checks health of all MCP connectors
func (h *HealthMonitor) checkMCPConnectorHealth() {
	ctx, span := sentinelTracer.Start(h.ctx, "check_mcp_connector_health")
	defer span.End()

	// Enumerate the connector tree, NOT the database.
	//
	// This used to read `SELECT container_name, port, status FROM
	// connector_instances WHERE status = 'running'`. That table has four indexes,
	// this one reader, and no writer anywhere in the repo — nothing has ever
	// inserted a row into it. So the loop below iterated zero times on every tick,
	// forever, and the only visible symptom was an absence: prod ran 25 MCP
	// connector containers and `sentinel_component_health` held zero
	// `mcp_connector:*` rows. Nothing errored. A monitor that checks nothing and a
	// monitor that finds everything healthy look identical from the outside, which
	// is why this survived so long.
	//
	// The tree is the right source because it is the same source
	// scripts/mcp_generate_compose.py reads to decide which containers exist at
	// all: a latest.json marks a connector root, versions/<current_version>/
	// carries its metadata and Dockerfile, and the container is named
	// <STACK_PREFIX>-<id>-vX-Y-Z-mcp. Enumerating anything else would describe a
	// deployment that was never generated.
	//
	// Internal connectors and roots without a Dockerfile are skipped for exactly
	// that reason — the generator skips them, so no container of theirs exists and
	// probing one would manufacture a permanently-unhealthy component.
	toolsDir := connectorpaths.ToolsDir()
	if toolsDir == "" {
		log.Warn("MCP connector health: connector tree not found (set MCP_CONNECTORS_PATH); skipping check")
		return
	}
	roots := connectorpaths.IterConnectorRoots(toolsDir)
	span.SetAttributes(attribute.Int("connector_roots_found", len(roots)))

	connectorCount := 0
	for _, cr := range roots {
		if cr.Internal || !cr.HasDockerfile {
			continue
		}

		// Every MCP connector listens on 8000 inside the network: the generator
		// hardcodes PORT=8000 and healthchecks localhost:8000/health, and
		// mcp.checkDockerContainer reaches live containers the same way.
		name := mcp.MCPContainerName(cr.ID, cr.CurrentVersion)
		if name == "" {
			continue
		}

		connectorCount++
		componentID := fmt.Sprintf("mcp_connector:%s", name)

		// Check HTTP health endpoint
		healthy := h.checkConnectorHTTPHealth(ctx, name, mcpConnectorPort)

		h.mu.Lock()
		health, exists := h.componentHealth[componentID]
		if !exists {
			health = &ComponentHealth{
				ComponentID:   componentID,
				ComponentType: ComponentTypeMCPConnector,
				Metadata:      make(map[string]interface{}),
			}
			h.componentHealth[componentID] = health
		}

		if healthy {
			health.Status = HealthStatusHealthy
		} else {
			health.Status = HealthStatusUnhealthy
		}
		health.UpdatedAt = time.Now()
		// These checks ARE the connector's heartbeat — nothing else reports for MCP
		// connectors, and last_heartbeat is NOT NULL.
		health.LastHeartbeat = health.UpdatedAt
		health.Metadata["connector_id"] = cr.ID
		health.Metadata["connector_version"] = cr.CurrentVersion
		health.Metadata["port"] = mcpConnectorPort

		// Copied so the persist below runs outside the lock without aliasing state a
		// concurrent check may be mutating — the recordInfraHealth pattern.
		snapshot := *health
		snapshot.Metadata = make(map[string]interface{}, len(health.Metadata))
		for k, v := range health.Metadata {
			snapshot.Metadata[k] = v
		}
		h.mu.Unlock()

		// Both breaks in KI-SENTINEL-MCP-CONNECTOR-HEALTH-DEAD-TWO-WAYS are closed
		// now: #818 added this persist, and the enumeration above gives it rows to
		// persist. Either half alone is invisible — a persist with no rows and a
		// row that is never persisted produce the same empty table.
		h.persistHealthToDB(&snapshot)

		log.WithFields(log.Fields{
			"connector": name,
			"healthy":   healthy,
			"port":      mcpConnectorPort,
		}).Debug("Checked MCP connector health")
	}

	span.SetAttributes(attribute.Int("connectors_checked", connectorCount))
}

// checkConnectorHTTPHealth checks HTTP health endpoint for a connector
func (h *HealthMonitor) checkConnectorHTTPHealth(ctx context.Context, name string, port int) bool {
	if port == 0 {
		return false
	}

	// MCP connectors are accessed internally via Docker network
	healthURL := fmt.Sprintf("http://%s:%d/health", name, port)

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", healthURL, nil)
	if err != nil {
		return false
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		log.WithError(err).WithField("connector", name).Debug("HTTP health check failed")
		return false
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// monitorInfrastructure monitors infrastructure components (Kafka, Redis, PostgreSQL)
func (h *HealthMonitor) monitorInfrastructure() {
	defer h.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.checkInfrastructureHealth()
		}
	}
}

// checkInfrastructureHealth checks health of infrastructure components
func (h *HealthMonitor) checkInfrastructureHealth() {
	ctx, span := sentinelTracer.Start(h.ctx, "check_infrastructure_health")
	defer span.End()

	// Check PostgreSQL
	h.checkPostgreSQLHealth(ctx)

	// Check Kafka connectivity (via kafka manager)
	h.checkKafkaHealth(ctx)

	// Check Kafka Connect (Debezium) for CDC pipelines
	h.checkKafkaConnectHealth(ctx)

	// TODO: Check Redis health
}

// checkPostgreSQLHealth checks PostgreSQL connectivity
func (h *HealthMonitor) checkPostgreSQLHealth(ctx context.Context) {
	componentID := "infrastructure:postgresql"

	if h.db == nil {
		h.recordInfraHealth(componentID, HealthStatusUnknown, "no database handle configured for this process", nil)
		return
	}

	if err := h.db.PingContext(ctx); err != nil {
		log.WithError(err).Error("PostgreSQL health check failed")
		h.recordInfraHealth(componentID, HealthStatusUnhealthy, err.Error(), nil)
		return
	}
	h.recordInfraHealth(componentID, HealthStatusHealthy, "", nil)
}

// probeKafkaBroker turns one broker probe into a health verdict.
//
// Three outcomes, not two. "No manager configured" is NOT healthy: this process has
// observed nothing about the cluster, and recording that as healthy is the same collapse
// of "could not find out" into "everything is fine" that made the sink-presence check
// delete its own escalations. HealthStatusUnknown says exactly what happened, and the
// monitoring API's infrastructure summary counts it as neither healthy nor unhealthy.
func probeKafkaBroker(probe kafkaBrokerProbe) (HealthStatus, string) {
	if probe == nil {
		return HealthStatusUnknown, "kafka manager not configured for this process"
	}
	if err := probe.Ping(); err != nil {
		return HealthStatusUnhealthy, err.Error()
	}
	return HealthStatusHealthy, ""
}

// checkKafkaHealth checks Kafka connectivity with a real round trip to the cluster.
//
// This used to be `healthy := true // Assume healthy for now`, which meant
// infrastructure:kafka reported healthy for the whole life of the process regardless of
// what the brokers were doing — the check could not produce a negative result at all.
// kafka.Manager.Ping() issues a metadata request; see its doc comment for why the cheaper
// IsConnected()/ListTopics() would have reproduced the same always-healthy answer.
func (h *HealthMonitor) checkKafkaHealth(ctx context.Context) {
	status, lastErr := probeKafkaBroker(h.kafkaProbe)
	if status == HealthStatusUnhealthy {
		log.WithField("error", lastErr).Error("Kafka health check failed")
	}
	h.recordInfraHealth("infrastructure:kafka", status, lastErr, nil)
}

// checkKafkaConnectHealth checks Kafka Connect REST API health.
// This is a critical dependency for CDC pipelines (Debezium).
func (h *HealthMonitor) checkKafkaConnectHealth(ctx context.Context) {
	componentID := "infrastructure:kafka-connect"

	// Resolve the same way every other Kafka Connect caller in the orchestrator
	// does — KAFKA_CONNECT_URL, falling back to the compose service name. This
	// used to be a hardcoded "http://kafka-connect:8083/" while the eight other
	// call sites honoured the env var, so on any deployment that does not name
	// the service literally "kafka-connect" (Kubernetes, where the Service
	// carries the Helm release prefix) this one probe resolved nothing and
	// pinned infrastructure:kafka-connect to unhealthy forever. The failure is
	// worse than a false alarm: the CDC health surface is what the sentinel and
	// the healer read, so a permanently-red component there is indistinguishable
	// from a real Connect outage.
	healthURL := strings.TrimRight(kafkaConnectURLFromEnv(), "/") + "/"

	req, err := http.NewRequestWithContext(ctx, "GET", healthURL, nil)
	if err != nil {
		return
	}

	resp, err := h.httpClient.Do(req)
	healthy := err == nil && resp != nil && resp.StatusCode == http.StatusOK
	lastErr := ""
	if err != nil {
		lastErr = err.Error()
	} else if resp != nil {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Sprintf("unexpected status: %s", resp.Status)
		}
	}

	status := HealthStatusHealthy
	if !healthy {
		status = HealthStatusUnhealthy
		// Helpful hint: frequent exit code 137 indicates OOM.
		if lastErr == "" {
			lastErr = "kafka-connect not healthy"
		}
		lastErr = fmt.Sprintf("%s (CDC requires Kafka Connect; if it keeps restarting, check memory/KAFKA_HEAP_OPTS)", lastErr)
	} else {
		lastErr = ""
	}

	h.recordInfraHealth(componentID, status, lastErr, map[string]interface{}{"url": healthURL})
}

// EvictStaleComponents removes specific component IDs from the health monitor's map and
// from sentinel_component_health.
//
// Called by the sentinel agent after it has already determined these are stale-dead.
func (h *HealthMonitor) EvictStaleComponents(ids []string) {
	h.mu.Lock()
	for _, id := range ids {
		delete(h.componentHealth, id)
	}
	h.mu.Unlock()

	h.deleteHealthFromDB(ids)
}

// deleteHealthFromDB removes evicted components from sentinel_component_health.
//
// Eviction has to reach both stores or it reaches neither usefully. While RecordHealthChange
// was silent, a dead component never had a row, so evicting it from the map alone was
// consistent; now that the dead verdict is published, dropping it from the map only would
// leave GET /api/v1/monitoring/sentinel/health reporting a component the sentinel has
// already collected as garbage — permanently, because nothing else in this repo deletes
// from this table. That would turn the Total this change just made honest into a count of
// accumulated ghosts.
func (h *HealthMonitor) deleteHealthFromDB(ids []string) {
	if h.db == nil || len(ids) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Placeholders rather than a driver array type: the PostgreSQL driver is only
	// blank-imported in this package (registered, not used as an API), and an IN list
	// needs nothing from it.
	placeholders := make([]string, len(ids))
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(
		`DELETE FROM sentinel_component_health WHERE component_id IN (%s)`,
		strings.Join(placeholders, ", "),
	)

	if _, err := h.db.ExecContext(ctx, query, args...); err != nil {
		log.WithError(err).WithField("component_ids", ids).
			Debug("Failed to delete evicted component health rows")
	}
}

// pruneStaleComponentsLoop periodically evicts components that have been dead
// longer than StaleComponentTTL from the health monitor's own map.
func (h *HealthMonitor) pruneStaleComponentsLoop() {
	defer h.wg.Done()

	// Run at half the TTL so eviction is timely
	interval := h.config.StaleComponentTTL / 2
	if interval < time.Minute {
		interval = time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.pruneStaleComponents()
		}
	}
}

func (h *HealthMonitor) pruneStaleComponents() {
	now := time.Now()
	var staleIDs []string

	h.mu.RLock()
	for id, c := range h.componentHealth {
		if c.Status == HealthStatusDead && now.Sub(c.UpdatedAt) > h.config.StaleComponentTTL {
			staleIDs = append(staleIDs, id)
		}
	}
	h.mu.RUnlock()

	if len(staleIDs) == 0 {
		return
	}

	h.mu.Lock()
	for _, id := range staleIDs {
		delete(h.componentHealth, id)
	}
	h.mu.Unlock()

	h.deleteHealthFromDB(staleIDs)

	log.WithField("evicted", len(staleIDs)).Info("Pruned stale dead components from health monitor")
}
