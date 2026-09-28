package sentinel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
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
	// kafkaProbe is kafkaManager again, narrowed. It stays unset when this process has
	// no manager — see NewHealthMonitor for why it is not simply assigned.
	kafkaProbe kafkaBrokerProbe
	db         *sql.DB
	config     *SentinelConfig
	logger     *AuditLogger

	// Component tracking
	componentHealth map[string]*ComponentHealth
	// absentConnectors remembers which connector containers this host does not run,
	// so the row eviction below happens once per name instead of on every 30s tick.
	// Guarded by mu like componentHealth.
	absentConnectors map[string]struct{}
	mu               sync.RWMutex

	// Control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// HTTP client for connector health checks
	httpClient *http.Client

	// dockerClient is kept separate from httpClient on purpose: the connector probe's
	// transport is replaced wholesale in tests, and a census sharing that client would
	// answer with whatever the connector stub was told to return.
	dockerClient *http.Client

	// containerCensus answers "which containers exist on this host?" — a field rather
	// than a direct call so a sweep can be tested against a known host without a
	// Docker daemon. Defaults to dockerContainerCensus.
	containerCensus func(ctx context.Context) (containerCensus, error)

	// censusFailing is the previous sweep's census outcome, so a socket proxy that goes
	// away logs once instead of every 30 seconds. Touched only by checkMCPConnectorHealth,
	// which runs on the single monitorMCPConnectors goroutine, so it needs no lock.
	censusFailing bool

	// kafkaConnectForgotten and cdcDemandFailing belong to checkKafkaConnectHealth, which
	// runs only on the infrastructure goroutine, so they need no lock. The first records
	// that Kafka Connect's rows have been cleared since it was last recorded; the second
	// is the previous tick's CDC-pipeline query outcome, so a failing query logs once.
	kafkaConnectForgotten bool
	cdcDemandFailing      bool
	// infraFailures counts each infrastructure probe's consecutive misses for
	// debounceInfraFailure. Touched only on the infrastructure goroutine.
	infraFailures map[string]int

	// onComponentsEvicted lets the Agent drop its in-memory issues for components this
	// monitor has just collected as garbage. Without it, handleDetectedIssue's
	// "already active, return early" branch would suppress the re-insert of an issue
	// whose row eviction has just deleted, leaving a real fault invisible in the table
	// the UI reads. nil is valid — a monitor with no Agent attached.
	onComponentsEvicted func(componentIDs []string)

	// serviceProbes are the core rsync services checked alongside PostgreSQL, Kafka and
	// Kafka Connect on the infrastructure tick (service_probes.go). Set by NewAgent from
	// the environment; empty for a monitor built directly, as the tests do.
	serviceProbes []*serviceProbe
}

// NewHealthMonitor creates a new health monitor
func NewHealthMonitor(kafkaManager *kafka.Manager, db *sql.DB, config *SentinelConfig, logger *AuditLogger) *HealthMonitor {
	h := &HealthMonitor{
		kafkaManager:     kafkaManager,
		db:               db,
		config:           config,
		logger:           logger,
		componentHealth:  make(map[string]*ComponentHealth),
		absentConnectors: make(map[string]struct{}),
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
		dockerClient: &http.Client{
			Timeout: dockerCensusTimeout,
		},
	}
	h.containerCensus = h.dockerContainerCensus
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

	// Start background monitoring loops.
	//
	// There is no Kafka consumer-lag loop. The orchestrator runs no always-on consumer of
	// its own — its workers poll the Redis correlation store, and the schema-drift healer's
	// consumers exist only behind RSYNC_SCHEMA_DRIFT_ENABLED — so a liveness check keyed
	// on this Manager's consumers would have nothing to watch. CDC sink groups are counted
	// by CDCSentinel's consumer census instead (cdc_consumer_census.go).
	h.wg.Add(3)
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
	h.closeServiceProbes()
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
// The write is synchronous: this runs on a 30s ticker over a handful of components, so
// there is nothing to gain from a goroutine and a caller can be sure the row landed.
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

// connectorReachability is the three-way answer to "is this connector there?".
//
// The probe used to return a bool, which forced two unrelated facts through one
// bit: "this host never deployed this connector" and "this connector is deployed
// and broken" both came back false, and both were written down as unhealthy. The
// tree is the set of connectors this repo can BUILD, while a deployment is a
// subset chosen per host, so on any host that runs a subset — which is every host
// except a full-catalogue one — the most alarming number on /admin/health was an
// artifact of the enumeration rather than a fault
// (KI-HEALTH-COUNTS-UNDEPLOYED-CONNECTORS).
type connectorReachability int

const (
	// connectorReachable: /health answered 200.
	connectorReachable connectorReachability = iota

	// connectorFailing: there is something there to fail. Either the census says a
	// container of this name exists on this host, or the probe failed in a way that
	// implies one — connection refused, a TCP timeout, a non-200. This is a fault
	// and stays unhealthy.
	connectorFailing

	// connectorUnknown: the probe could not tell absence from fault. Only the
	// resolver fallback produces this — a DNS error that is not IsNotFound says
	// nothing either way, and the sweep writes nothing down for it.
	//
	// It exists because the alternative was tried and was wrong. This used to fold
	// into connectorFailing on the reasoning that a component "keeps its row and
	// stays a fault" when the resolver does not answer; on a host whose resolv.conf
	// carries search domains that is every undeployed connector, which is how prod
	// came to show 18 faults for containers it had never deployed. An unknown that
	// is silent costs a fault this lane would have reported second (the pipeline lane
	// reports the connectors a pipeline actually uses); an unknown reported as a
	// fault costs every reading of the board.
	connectorUnknown

	// connectorAbsent: no container of this name exists on this host.
	//
	// Since the container census (dockerContainerCensus) this is a direct answer from
	// the Docker API rather than an inference from DNS, and it is the census that is
	// authoritative whenever one is available. The resolver fallback below still
	// produces it from a clean NXDOMAIN, for a deployment with no socket proxy.
	//
	// In the fallback, Docker's embedded DNS publishes a record per RUNNING container
	// and withdraws it when the container stops, so the fallback cannot distinguish
	// "never deployed" from "deployed and currently stopped". The census can, and
	// calls a stopped container a fault. Where the fallback cannot, it is safe in this
	// direction for two reasons: connectors are deployed on demand
	// (mcp.tryDeployConnectorContainer), so an unresolvable name is overwhelmingly
	// "this host has never needed this connector"; and the case it does hide — a
	// connector a pipeline is actually using going away — is alarmed on by the
	// pipeline lane, which watches that pipeline's own connector and publishes
	// CDC_CONNECTOR_DOWN (cdc_sentinel.go emitCDCIssue -> notify.go).
	connectorAbsent
)

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
	//
	// An undeployed connector manufactured one the same way, and that half took a
	// second fix: the set enumerated here is still every connector this repo could
	// build, because narrowing it is what made the old query blind, so the verdict
	// is what changed. A name that does not resolve on the Docker network is
	// reported as not deployed and gets no row at all — see connectorReachability.
	toolsDir := connectorpaths.ToolsDir()
	if toolsDir == "" {
		log.Warn("MCP connector health: connector tree not found (set MCP_CONNECTORS_PATH); skipping check")
		return
	}
	roots := connectorpaths.IterConnectorRoots(toolsDir)
	span.SetAttributes(attribute.Int("connector_roots_found", len(roots)))

	// One census per sweep, not one lookup per connector: the answer is the same for
	// every name in the tree, and it is the authority on which of them this host runs.
	census, censusErr := h.containerCensus(ctx)
	if censusErr != nil && !h.censusFailing {
		h.censusFailing = true
		if errors.Is(censusErr, errNoDockerAPI) {
			// Not a fault: this deployment runs no socket proxy (the quickstart/self-host
			// compose does not), so the sweep uses the resolver and records only what the
			// resolver can answer for.
			log.Debug("Connector health: no container census configured; falling back to name resolution")
		} else {
			log.WithError(censusErr).
				Warn("Connector health: container census unavailable; connectors this host has not been seen to run will go unreported until it returns")
		}
	} else if censusErr == nil && h.censusFailing {
		h.censusFailing = false
		log.WithField("containers", len(census)).Info("Connector health: container census available again")
	}

	connectorCount := 0
	notDeployed := 0
	undetermined := 0
	var evicted []string
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

		componentID := fmt.Sprintf("mcp_connector:%s", name)

		switch reach := h.classifyConnector(ctx, name, census, censusErr); reach {
		case connectorAbsent:
			notDeployed++
			// No row, rather than a fourth status. A status only this enumeration can
			// produce would have to be taught to the panel, to the counts and to the
			// issue detector; an absent row is already understood by all three, and a
			// connector nobody deployed is not a component of this deployment.
			if id := h.forgetConnector(componentID); id != "" {
				evicted = append(evicted, id)
			}
		case connectorUnknown:
			// Nothing conclusive, so nothing written down — and no eviction either,
			// because an unknown is not evidence that an existing row is wrong.
			undetermined++
		default:
			connectorCount++
			h.recordConnectorVerdict(componentID, cr.ID, cr.CurrentVersion, reach == connectorReachable)
		}
	}

	// One DELETE per sweep, and only while there is something to delete:
	// forgetConnector answers "" once it has reconciled a name, so a host running 7
	// of 21 connectors issues this once after start and not again. It has to run at
	// least once per process, though — the rows outlive the process, so this is also
	// what clears rows an older build wrote.
	if len(evicted) > 0 {
		h.forgetComponents(evicted)
		log.WithField("components", evicted).
			Info("Connector health: dropped components with no container deployed on this host")
	}

	span.SetAttributes(
		attribute.Int("connectors_checked", connectorCount),
		attribute.Int("connectors_not_deployed", notDeployed),
		attribute.Int("connectors_undetermined", undetermined),
		attribute.Bool("census_available", censusErr == nil),
		attribute.Int("census_containers", len(census)),
	)
}

// classifyConnector answers "is this connector there?" from the census when there is
// one, and from name resolution when there is not.
//
// The census is the authority on existence, so a container it lists is never an
// absence no matter what the resolver says. The two can disagree in both directions:
// Docker withdraws a stopped container's DNS record while the container still exists,
// and on a host with search domains the resolver can fail to answer conclusively for
// a name that is plainly in the list.
func (h *HealthMonitor) classifyConnector(
	ctx context.Context, name string, census containerCensus, censusErr error,
) connectorReachability {
	if censusErr != nil {
		return h.checkConnectorReachability(ctx, name, mcpConnectorPort)
	}

	state, exists := census[name]
	if !exists {
		return connectorAbsent
	}
	if state != "running" {
		// Deployed here and not up. The resolver cannot report this at all — there is
		// no DNS record for a stopped container — so it read as an absence before.
		log.WithFields(log.Fields{"connector": name, "state": state}).
			Debug("MCP connector container exists but is not running")
		return connectorFailing
	}

	if reach := h.checkConnectorReachability(ctx, name, mcpConnectorPort); reach == connectorReachable {
		return connectorReachable
	}
	// Running, and it did not answer 200. Whatever the probe made of the error, the
	// census has already settled that there is something here to be broken.
	return connectorFailing
}

// forgetComponents removes evicted components from both tables that outlive this
// process: the health row, and any issue the detector filed against the component.
//
// Both halves are needed. The two rows are written by different lanes and only the
// health row has ever been deleted, so an issue filed against a component that is
// later collected as garbage outlived it indefinitely — which is what prod's 18
// connector_down rows are. api-gateway's monitoring endpoints read that table
// directly, so a ghost there is a ghost on /admin/health and in the bell's count.
func (h *HealthMonitor) forgetComponents(componentIDs []string) {
	if len(componentIDs) == 0 {
		return
	}
	h.deleteHealthFromDB(componentIDs)
	h.deleteIssuesFromDB(componentIDs)
	// The Agent keeps its own map of active issues and returns early for one it has
	// already seen, so without this the row just deleted would not be re-inserted if
	// the component came back and failed again.
	if h.onComponentsEvicted != nil {
		h.onComponentsEvicted(componentIDs)
	}
}

// recordConnectorVerdict writes one connector's verdict to the in-memory map and
// to sentinel_component_health. Split out of the sweep so the persist can be
// tested for a verdict the probe cannot reach without a Docker network.
func (h *HealthMonitor) recordConnectorVerdict(componentID, connectorID, version string, healthy bool) {
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
	// It answered, so any earlier conclusion that this host does not run it is stale.
	delete(h.absentConnectors, componentID)

	if healthy {
		health.Status = HealthStatusHealthy
	} else {
		health.Status = HealthStatusUnhealthy
	}
	health.UpdatedAt = time.Now()
	// These checks ARE the connector's heartbeat — nothing else reports for MCP
	// connectors, and last_heartbeat is NOT NULL.
	health.LastHeartbeat = health.UpdatedAt
	health.Metadata["connector_id"] = connectorID
	health.Metadata["connector_version"] = version
	health.Metadata["port"] = mcpConnectorPort

	// Copied so the persist below runs outside the lock without aliasing state a
	// concurrent check may be mutating — the recordInfraHealth pattern.
	snapshot := copyComponentHealth(health)
	h.mu.Unlock()

	// Both breaks in KI-SENTINEL-MCP-CONNECTOR-HEALTH-DEAD-TWO-WAYS are closed
	// now: #818 added this persist, and the enumeration above gives it rows to
	// persist. Either half alone is invisible — a persist with no rows and a
	// row that is never persisted produce the same empty table.
	h.persistHealthToDB(snapshot)

	log.WithFields(log.Fields{
		"connector": componentID,
		"healthy":   healthy,
		"port":      mcpConnectorPort,
	}).Debug("Checked MCP connector health")
}

// forgetConnector drops a connector this host does not run from the in-memory map,
// and answers with the component id while there is still a row to delete with it —
// "" once the name has been reconciled, so the DELETE and the log line happen once
// per transition rather than every 30 seconds.
func (h *HealthMonitor) forgetConnector(componentID string) string {
	h.mu.Lock()
	defer h.mu.Unlock()

	_, inMap := h.componentHealth[componentID]
	_, reconciled := h.absentConnectors[componentID]
	if reconciled && !inMap {
		return ""
	}
	delete(h.componentHealth, componentID)
	h.absentConnectors[componentID] = struct{}{}
	return componentID
}

// copyComponentHealth returns a deep-enough copy of a component's health: the
// struct plus its metadata map, which is the only reference type in it. Callers
// read these fields without holding the lock the writers take.
func copyComponentHealth(c *ComponentHealth) *ComponentHealth {
	if c == nil {
		return nil
	}
	cp := *c
	cp.Metadata = make(map[string]interface{}, len(c.Metadata))
	for k, v := range c.Metadata {
		cp.Metadata[k] = v
	}
	return &cp
}

// snapshotComponents returns a copy of every component this monitor polls, for a
// reader that does not hold mu — the issue detector, which reads each field
// directly while the polling loops are writing them.
func (h *HealthMonitor) snapshotComponents() []*ComponentHealth {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make([]*ComponentHealth, 0, len(h.componentHealth))
	for _, c := range h.componentHealth {
		if c == nil {
			continue
		}
		out = append(out, copyComponentHealth(c))
	}
	return out
}

// checkConnectorReachability probes a connector's /health endpoint and separates
// "not deployed here" from "deployed and failing".
func (h *HealthMonitor) checkConnectorReachability(ctx context.Context, name string, port int) connectorReachability {
	if port == 0 {
		return connectorFailing
	}

	// MCP connectors are accessed internally via Docker network
	healthURL := fmt.Sprintf("http://%s:%d/health", name, port)

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", healthURL, nil)
	if err != nil {
		return connectorFailing
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		// errors.As walks *url.Error -> *net.OpError -> *net.DNSError, each of which
		// unwraps to the next. IsNotFound is the resolver saying the name does not
		// exist, which is an absence this can act on.
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) {
			if dnsErr.IsNotFound {
				log.WithField("connector", name).
					Debug("MCP connector name does not resolve; not deployed on this host")
				return connectorAbsent
			}
			// A resolver that timed out, refused or SERVFAILed has told us nothing:
			// the name may be a container that is down, or a container that was never
			// here. This used to return connectorFailing, which is what put 18
			// never-deployed connectors on prod's board — see connectorUnknown.
			log.WithError(err).WithField("connector", name).
				Debug("MCP connector name did not resolve conclusively; no verdict recorded")
			return connectorUnknown
		}
		log.WithError(err).WithField("connector", name).Debug("HTTP health check failed")
		return connectorFailing
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return connectorReachable
	}
	return connectorFailing
}

// monitorInfrastructure monitors infrastructure components: PostgreSQL, Kafka, Kafka
// Connect, and the core rsync services in service_probes.go.
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

	// Redis, Temporal and the rsync services pipelines depend on
	h.checkServiceProbes(ctx)
}

// debounceInfraFailure turns one failed infrastructure probe into a verdict with the
// same debounce the core services use (serviceFailureThreshold, service_probes.go):
// degraded for the misses before the threshold, which the detector does not alert on,
// and unhealthy from the threshold on. An unhealthy infrastructure component is a
// CRITICAL INFRASTRUCTURE_DOWN for every admin, so one refused dial during a restart,
// or a probe that lands while a fresh install's service is still starting, must not
// be one.
func (h *HealthMonitor) debounceInfraFailure(componentID, lastErr string) (HealthStatus, string) {
	if h.infraFailures == nil {
		h.infraFailures = make(map[string]int)
	}
	h.infraFailures[componentID]++
	n := h.infraFailures[componentID]
	if n < serviceFailureThreshold {
		return HealthStatusDegraded, fmt.Sprintf("%s (failed %d of %d checks before it is reported down)",
			lastErr, n, serviceFailureThreshold)
	}
	return HealthStatusUnhealthy, lastErr
}

// resetInfraFailures starts a component's miss count over after a success.
func (h *HealthMonitor) resetInfraFailures(componentID string) {
	delete(h.infraFailures, componentID)
}

// checkPostgreSQLHealth checks PostgreSQL connectivity
func (h *HealthMonitor) checkPostgreSQLHealth(ctx context.Context) {
	componentID := "infrastructure:postgresql"

	if h.db == nil {
		h.recordInfraHealth(componentID, HealthStatusUnknown, "no database handle configured for this process", nil)
		return
	}

	if err := h.db.PingContext(ctx); err != nil {
		status, lastErr := h.debounceInfraFailure(componentID, err.Error())
		if status == HealthStatusUnhealthy {
			log.WithError(err).Error("PostgreSQL health check failed")
		}
		h.recordInfraHealth(componentID, status, lastErr, nil)
		return
	}
	h.resetInfraFailures(componentID)
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
	const componentID = "infrastructure:kafka"
	status, lastErr := probeKafkaBroker(h.kafkaProbe)
	if status == HealthStatusUnhealthy {
		status, lastErr = h.debounceInfraFailure(componentID, lastErr)
	} else {
		h.resetInfraFailures(componentID)
	}
	if status == HealthStatusUnhealthy {
		log.WithField("error", lastErr).Error("Kafka health check failed")
	}
	h.recordInfraHealth(componentID, status, lastErr, nil)
}

// cdcPipelinesExistQuery is the predicate the CDC sentinel selects its pipelines by
// (cdc_sentinel.go activeCDCPipelinesQuery) without its status filter. A pipeline that
// failed because Kafka Connect went down still needs Connect back, so counting only
// running ones would stop the check at the moment it matters. Pipelines are
// hard-deleted (workers/cdc_reconciler.go), so any row is one somebody still has.
const cdcPipelinesExistQuery = `SELECT EXISTS (SELECT 1 FROM pipelines WHERE sync_mode = 'cdc' OR cdc_mode IS NOT NULL)`

// kafkaConnectExpected reports whether this deployment is meant to be running Kafka
// Connect.
//
// Kafka Connect is optional. The quickstart runs it only under the cdc profile, and the
// chart only with connectors.cdc.enabled. Probing it unconditionally put
// infrastructure:kafka-connect down on every install without CDC. Once the detector
// read infrastructure components, that became an INFRASTRUCTURE_DOWN finding alerted
// to every admin that no recovery could close, because the service was never coming.
//
// Two things say it is expected. The first is an explicit KAFKA_CONNECT_URL: the chart
// sets it only when CDC is enabled, and an operator who brings their own Connect sets
// it. The second, under the compose default, is a CDC pipeline for it to serve. With
// none, Connect being down stops nothing.
//
// The container census and name resolution cannot answer this. The quickstart runs no
// socket proxy, so it has no census. And Docker withdraws a stopped container's DNS
// record, so a crashed Connect would read as one that was never deployed.
//
// When it cannot find out, it answers expected, which is how the probe always behaved.
func (h *HealthMonitor) kafkaConnectExpected(ctx context.Context) bool {
	if strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL")) != "" || h.db == nil {
		return true
	}
	qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var exists bool
	if err := h.db.QueryRowContext(qctx, cdcPipelinesExistQuery).Scan(&exists); err != nil {
		if !h.cdcDemandFailing {
			h.cdcDemandFailing = true
			log.WithError(err).Warn("Kafka Connect health: could not tell whether any CDC pipeline exists; probing Kafka Connect anyway")
		}
		return true
	}
	h.cdcDemandFailing = false
	return exists
}

// forgetKafkaConnect takes Kafka Connect off the board while nothing needs it. It gets
// no row, rather than a status, as with a connector this host never deployed. Its
// health row and any finding filed against it are deleted, which closes a finding an
// earlier tick or an older build left. That happens once, and again only if Connect
// has been recorded since.
func (h *HealthMonitor) forgetKafkaConnect(componentID string) {
	h.resetInfraFailures(componentID)
	h.mu.Lock()
	_, inMap := h.componentHealth[componentID]
	delete(h.componentHealth, componentID)
	h.mu.Unlock()
	if inMap || !h.kafkaConnectForgotten {
		h.kafkaConnectForgotten = true
		h.forgetComponents([]string{componentID})
	}
}

// checkKafkaConnectHealth checks Kafka Connect REST API health.
// This is a critical dependency for CDC pipelines (Debezium).
func (h *HealthMonitor) checkKafkaConnectHealth(ctx context.Context) {
	componentID := "infrastructure:kafka-connect"

	if !h.kafkaConnectExpected(ctx) {
		h.forgetKafkaConnect(componentID)
		return
	}

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

	// A fresh install's first probes land while Connect is still starting, and each
	// used to file a CRITICAL INFRASTRUCTURE_DOWN for a service seconds from coming up.
	status := HealthStatusHealthy
	if !healthy {
		if lastErr == "" {
			lastErr = "kafka-connect not healthy"
		}
		// Helpful hint: frequent exit code 137 indicates OOM.
		status, lastErr = h.debounceInfraFailure(componentID,
			fmt.Sprintf("%s (CDC requires Kafka Connect; if it keeps restarting, check memory/KAFKA_HEAP_OPTS)", lastErr))
	} else {
		h.resetInfraFailures(componentID)
		lastErr = ""
	}

	h.recordInfraHealth(componentID, status, lastErr, map[string]interface{}{"url": healthURL})
}

// deleteHealthFromDB removes evicted components from sentinel_component_health.
//
// Eviction has to reach both stores or it reaches neither usefully. Every verdict is
// published, so dropping a component from the map only would leave
// GET /api/v1/monitoring/sentinel/health reporting a component the sentinel has already
// collected as garbage — permanently, because nothing else in this repo deletes from this
// table.
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

// deleteIssuesFromDB removes the IssueDetector's findings for components that no
// longer exist.
//
// Nothing else in this repo deletes from this table for the generic detector lane.
// The CDC and batch lanes each resolve their own ids by prefix, and a successful heal
// clears only the Agent's in-memory map, so a finding persisted by persistIssueToDB
// has no path out of the table at all. Combined with an enumeration that could invent
// components, that turned the issue list into an append-only log of things that were
// once believed.
func (h *HealthMonitor) deleteIssuesFromDB(componentIDs []string) {
	if h.db == nil || len(componentIDs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	placeholders := make([]string, len(componentIDs))
	args := make([]interface{}, len(componentIDs))
	for i, id := range componentIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = id
	}

	query := fmt.Sprintf(
		`DELETE FROM sentinel_active_issues WHERE component_id IN (%s)`,
		strings.Join(placeholders, ", "),
	)

	if _, err := h.db.ExecContext(ctx, query, args...); err != nil {
		log.WithError(err).WithField("component_ids", componentIDs).
			Debug("Failed to delete active issues for evicted components")
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

	h.forgetComponents(staleIDs)

	log.WithField("evicted", len(staleIDs)).Info("Pruned stale dead components from health monitor")
}
