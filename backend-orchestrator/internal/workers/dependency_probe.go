package workers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/rsync-ai/backend-orchestrator/internal/mcp"

	log "github.com/sirupsen/logrus"
)

// DependencyProbe periodically verifies that the runtime dependencies declared
// in pipeline_dependencies (destination MCP, source MCP, etc.) are actually
// alive, and writes the observed state into pipeline_dependency_health.
//
// This is the observation half of the "stop trusting assertions, start trusting
// observations" fix for the state-fragmentation bug class. Without it, a CDC
// pipeline whose destination MCP container died will continue to report
// "streaming" forever — Temporal already exited and nothing else watches it.
//
// Deliberately simple: no auto-recovery here, no event emission — those are
// separate concerns. Just observe, write to the health table, and let the
// canonical /api/pipelines/{id}/runtime endpoint surface the truth.
type DependencyProbe struct {
	db          *sql.DB
	mcpManager  *mcp.ServerManager
	httpClient  *http.Client
	tickEvery   time.Duration
	stopCh      chan struct{}
}

// NewDependencyProbe constructs a probe. Caller is expected to call Start in a
// goroutine and Stop on shutdown.
func NewDependencyProbe(db *sql.DB, mcpManager *mcp.ServerManager) *DependencyProbe {
	return &DependencyProbe{
		db:         db,
		mcpManager: mcpManager,
		httpClient: &http.Client{Timeout: 3 * time.Second},
		tickEvery:  15 * time.Second,
		stopCh:     make(chan struct{}),
	}
}

func (p *DependencyProbe) Start(ctx context.Context) {
	if p.db == nil || p.mcpManager == nil {
		log.Warn("dependency_probe: missing db or mcpManager, not starting")
		return
	}
	log.Infof("dependency_probe: starting (tick every %s)", p.tickEvery)
	t := time.NewTicker(p.tickEvery)
	defer t.Stop()
	// First sweep immediately so /runtime has data on first request.
	p.sweep(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-t.C:
			p.sweep(ctx)
		}
	}
}

func (p *DependencyProbe) Stop() {
	select {
	case <-p.stopCh:
		// already closed
	default:
		close(p.stopCh)
	}
}

// sweep walks every dependency for pipelines that are currently active (not
// completed/failed/paused for batch; for CDC we always probe so we catch
// "streaming-but-dead" cases).
func (p *DependencyProbe) sweep(ctx context.Context) {
	// Only probe dependencies of pipelines in a NON-terminal state. The
	// previous "sync_mode = 'cdc' OR <non-terminal>" probed CDC pipelines
	// unconditionally — so a failed/cancelled/archived CDC pipeline (e.g. one
	// pinned to a connector version that was later removed) kept getting probed
	// forever. Actively-streaming CDC pipelines are still covered because their
	// status ('running'/'streaming_active') is non-terminal.
	rows, err := p.db.QueryContext(ctx, `
		SELECT d.id, d.pipeline_id, d.kind, d.identifier, COALESCE(d.metadata, '{}'::jsonb)
		FROM pipeline_dependencies d
		JOIN pipelines p ON p.id = d.pipeline_id
		WHERE COALESCE(p.status, '') NOT IN ('completed', 'failed', 'cancelled', 'paused', 'archived')
	`)
	if err != nil {
		log.Warnf("dependency_probe: list query failed: %v", err)
		return
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		var depID, pipelineID, kind, identifier string
		var metaRaw []byte
		if err := rows.Scan(&depID, &pipelineID, &kind, &identifier, &metaRaw); err != nil {
			log.Warnf("dependency_probe: row scan failed: %v", err)
			continue
		}
		var meta map[string]interface{}
		_ = json.Unmarshal(metaRaw, &meta)
		status, lastError, details := p.probeOne(ctx, pipelineID, kind, identifier, meta)
		p.writeHealth(ctx, depID, status, lastError, details)
		if status == "unhealthy" {
			p.maybeRecover(ctx, depID, pipelineID, kind, identifier)
		}
	}
	// Demoted to Debug: the previous Info-level emission fired on every
	// tick (every 5–15s) and drowned out real error signals in logs. We
	// only want a heads-up when something is actually unhealthy — which
	// the writeHealth path already logs at Warn for status transitions.
	if count > 0 {
		log.Debugf("dependency_probe: swept %d dependencies", count)
	}
}

// maybeRecover attempts to bring a dead dependency back. We only auto-recover
// destination MCPs (the failure mode that silently breaks CDC streaming) and
// only after consecutive_failures crosses a threshold (~60s with the 15s tick),
// to avoid thrashing on transient blips. We also rate-limit by stamping
// last_recovery_at in the details JSON so we don't restart more than once a
// minute even if the probe interval changes.
func (p *DependencyProbe) maybeRecover(ctx context.Context, depID, pipelineID, kind, identifier string) {
	if kind != "mcp_dest" {
		return // source MCP is best-effort; only the destination breaks streaming
	}

	var consecFailures int
	var detailsRaw []byte
	if err := p.db.QueryRowContext(ctx, `
		SELECT consecutive_failures, COALESCE(details, '{}'::jsonb)
		FROM pipeline_dependency_health WHERE dependency_id = $1
	`, depID).Scan(&consecFailures, &detailsRaw); err != nil {
		return
	}

	var details map[string]interface{}
	_ = json.Unmarshal(detailsRaw, &details)
	if details == nil {
		details = map[string]interface{}{}
	}

	const recoveryFloor = 4     // wait ~60s before intervening
	const recoveryCeiling = 20  // circuit breaker: stop hammering a permanently-dead dependency

	if consecFailures < recoveryFloor {
		return // wait at least ~60s before intervening
	}

	// Circuit breaker. After this many consecutive failures the dependency is
	// clearly not coming back on its own (e.g. the pinned connector version was
	// removed/consolidated). Endless StartServer + tool-generator 404 attempts
	// just spam logs and burn CPU, and the per-minute rate limit doesn't bound
	// total attempts. Stop auto-recovery and log ONCE so an operator can act.
	if consecFailures > recoveryCeiling {
		if _, logged := details["circuit_open_logged"].(bool); !logged {
			log.Warnf("dependency_probe: circuit breaker OPEN for %s (pipeline=%s, consec_failures=%d) — giving up auto-recovery until the dependency is restored", identifier, pipelineID, consecFailures)
			details["circuit_open_logged"] = true
			if patched, err := json.Marshal(details); err == nil {
				_, _ = p.db.ExecContext(ctx, `
					UPDATE pipeline_dependency_health SET details = $1::jsonb, updated_at = NOW()
					WHERE dependency_id = $2
				`, patched, depID)
			}
		}
		return
	}

	// Rate limit: at most one recovery attempt per minute per dependency.
	if lastStr, ok := details["last_recovery_at"].(string); ok && lastStr != "" {
		if last, err := time.Parse(time.RFC3339, lastStr); err == nil && time.Since(last) < time.Minute {
			return
		}
	}

	name, version := splitIdentifier(identifier)
	if name == "" {
		return
	}
	log.Warnf("dependency_probe: auto-recovering dead %s %s (pipeline=%s, consec_failures=%d)",
		kind, identifier, pipelineID, consecFailures)

	cfg := mcp.ServerConfig{Name: name, Version: version, RequireHTTP: true}
	server, err := p.mcpManager.StartServer(cfg)
	recoveryTime := time.Now().UTC().Format(time.RFC3339)
	details["last_recovery_at"] = recoveryTime
	if err != nil || server == nil {
		details["last_recovery_error"] = fmt.Sprintf("%v", err)
		log.Errorf("dependency_probe: recovery failed for %s: %v", identifier, err)
	} else {
		details["last_recovery_ok"] = true
		details["recovered_to_host"] = server.Host
		details["recovered_to_port"] = server.Port
		log.Infof("dependency_probe: recovery OK for %s — now at %s:%d", identifier, server.Host, server.Port)
	}
	if patched, err := json.Marshal(details); err == nil {
		_, _ = p.db.ExecContext(ctx, `
			UPDATE pipeline_dependency_health SET details = $1::jsonb, updated_at = NOW()
			WHERE dependency_id = $2
		`, patched, depID)
	}
}

// probeOne returns ("healthy" | "degraded" | "unhealthy", error string, details).
//
// pipelineID is carried in for the one kind that needs a fact about the pipeline
// rather than about a process: `debezium_task`, whose "is the connector RUNNING"
// answer stays true after a selected source table is dropped at the origin. See
// droppedSourceTables.
func (p *DependencyProbe) probeOne(ctx context.Context, pipelineID, kind, identifier string, meta map[string]interface{}) (string, string, map[string]interface{}) {
	details := map[string]interface{}{}
	switch kind {
	case "mcp_source", "mcp_dest":
		name, version := splitIdentifier(identifier)
		if name == "" {
			return "unknown", "malformed identifier", details
		}
		// Resolve "latest" or empty version to the concrete vX.Y.Z that the
		// orchestrator's MCP registry actually keys on. Without this, dependencies
		// stored as `mysql@latest` perpetually report "no MCP server registered"
		// because GetServer is keyed on the concrete version string.
		if version == "" || version == "latest" {
			if resolved, err := p.mcpManager.ResolveConcreteVersion(name, "latest"); err == nil && resolved != "" {
				version = resolved
				details["resolved_version"] = resolved
			}
		}
		// Step 1: ask the orchestrator's server manager what it knows.
		server, ok := p.mcpManager.GetServer(name, version)
		if (!ok || server == nil) && version != "" && version != "latest" {
			// The MCP registry keys on the "v"-prefixed concrete version
			// (e.g. "mysql:v1.0.0"), but a dependency hydrated from a connection
			// often carries a bare "1.0.0" (no "v"). Retry the toggled-prefix
			// variant so a healthy, running connector isn't falsely reported as
			// "no MCP server registered" purely on a version-string mismatch.
			alt := "v" + version
			if strings.HasPrefix(version, "v") {
				alt = strings.TrimPrefix(version, "v")
			}
			if s2, ok2 := p.mcpManager.GetServer(name, alt); ok2 && s2 != nil {
				server, ok = s2, true
				details["resolved_version"] = alt
			}
		}
		if !ok || server == nil {
			return "unhealthy", "no MCP server registered with orchestrator", details
		}
		details["transport"] = server.ConnType
		details["status"] = server.Status

		if server.Status != "running" {
			return "unhealthy", fmt.Sprintf("MCP server status: %s", server.Status), details
		}

		// Step 2: for HTTP-mode servers (the only ones reachable by other containers),
		// hit /health to confirm the container is actually responding.
		if server.ConnType == "http" {
			url := fmt.Sprintf("http://%s:%d/health", server.Host, server.Port)
			details["probe_url"] = url
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			resp, err := p.httpClient.Do(req)
			if err != nil {
				return "unhealthy", fmt.Sprintf("HTTP probe failed: %v", err), details
			}
			_ = resp.Body.Close()
			details["http_status"] = resp.StatusCode
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return "healthy", "", details
			}
			// Some MCP HTTP servers don't expose /health; treat any non-5xx as "responding".
			if resp.StatusCode < 500 {
				return "degraded", fmt.Sprintf("/health returned %d", resp.StatusCode), details
			}
			return "unhealthy", fmt.Sprintf("/health returned %d", resp.StatusCode), details
		}

		// stdio is in-process; reaching this point means the orchestrator at least
		// thinks it's alive. We can't deeply probe without making an MCP call here,
		// which would race with real workloads. Mark degraded if this dep was meant
		// to be HTTP (kafka-mcp-sink relies on it) — caller can tell from required_phases.
		return "healthy", "", details

	case "debezium_task":
		// The CDC source is a Debezium connector running in Kafka Connect. Health
		// = the connector AND all its tasks are RUNNING. identifier is the Kafka
		// Connect connector name (e.g. "cdc-<pipelineid8>").
		connectURL := strings.TrimRight(kafkaConnectBaseURL(), "/")
		url := fmt.Sprintf("%s/connectors/%s/status", connectURL, identifier)
		details["probe_url"] = url
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := p.httpClient.Do(req)
		if err != nil {
			return "unhealthy", fmt.Sprintf("kafka connect unreachable: %v", err), details
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return "unhealthy", "debezium connector not found in kafka connect", details
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return "unhealthy", fmt.Sprintf("kafka connect status %d", resp.StatusCode), details
		}
		var st struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
			Tasks []struct {
				ID    int    `json:"id"`
				State string `json:"state"`
			} `json:"tasks"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			return "degraded", fmt.Sprintf("unparseable connector status: %v", err), details
		}
		connState := strings.ToUpper(strings.TrimSpace(st.Connector.State))
		details["connector_state"] = connState
		details["task_count"] = len(st.Tasks)
		running, failed := 0, []string{}
		for _, t := range st.Tasks {
			ts := strings.ToUpper(strings.TrimSpace(t.State))
			if ts == "RUNNING" {
				running++
			} else {
				failed = append(failed, fmt.Sprintf("task%d=%s", t.ID, ts))
			}
		}
		details["tasks_running"] = running
		if connState == "RUNNING" && len(st.Tasks) > 0 && running == len(st.Tasks) {
			// Everything this probe could see is fine — and that was the bug
			// (KI-CDC-DROPPED-SOURCE-TABLE-REPORTS-HEALTHY). A connector whose
			// selected source table was dropped at the origin stays RUNNING with all
			// tasks RUNNING forever, capturing nothing, and every instrument the
			// product offers said "healthy" because each one asked whether the
			// process was up. It is. The table is not.
			//
			// The cdcstats DDL consumer records that fact (schema_changes.go
			// trackSelectedTableDrops); it cannot write the verdict itself, because
			// writeHealth below overwrites status on every 15s tick and would erase
			// it. So the degrade is computed here, on the healthy path only:
			// PAUSED and the connector=<state> branches below are more specific
			// answers about the same stream and keep priority.
			if names := p.droppedSourceTables(ctx, pipelineID); len(names) > 0 {
				details["dropped_source_tables"] = names
				return "degraded", fmt.Sprintf("source table(s) dropped at origin: %s", strings.Join(names, ", ")), details
			}
			return "healthy", "", details
		}
		if connState == "PAUSED" {
			return "degraded", "debezium connector paused", details
		}
		msg := fmt.Sprintf("connector=%s", connState)
		if len(failed) > 0 {
			msg += " " + strings.Join(failed, ",")
		}
		return "unhealthy", msg, details

	case "kafka_sink_worker":
		// The kafka-mcp-sink is a long-running container the orchestrator reaches
		// as an MCP server. Resolve it the same way as mcp_* and hit /health for
		// liveness. (Per-pipeline freshness/stall lives in the sink's own /status
		// on a container-internal port the orchestrator can't reach; liveness here
		// is enough for the health panel + Diagnose to know the sink is up.)
		const sinkName = "kafka-mcp-sink"
		ver := ""
		if resolved, rerr := p.mcpManager.ResolveConcreteVersion(sinkName, "latest"); rerr == nil && resolved != "" {
			ver = resolved
		}
		server, ok := p.mcpManager.GetServer(sinkName, ver)
		if (!ok || server == nil) && ver != "" {
			alt := "v" + ver
			if strings.HasPrefix(ver, "v") {
				alt = strings.TrimPrefix(ver, "v")
			}
			if s2, ok2 := p.mcpManager.GetServer(sinkName, alt); ok2 && s2 != nil {
				server, ok = s2, true
			}
		}
		if !ok || server == nil {
			return "unknown", "kafka-mcp-sink not registered with orchestrator", details
		}
		details["transport"] = server.ConnType
		if server.ConnType != "http" {
			return "healthy", "", details
		}
		url := fmt.Sprintf("http://%s:%d/health", server.Host, server.Port)
		details["probe_url"] = url
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		resp, err := p.httpClient.Do(req)
		if err != nil {
			return "unhealthy", fmt.Sprintf("sink /health failed: %v", err), details
		}
		_ = resp.Body.Close()
		details["http_status"] = resp.StatusCode
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return "healthy", "", details
		}
		if resp.StatusCode < 500 {
			return "degraded", fmt.Sprintf("/health returned %d", resp.StatusCode), details
		}
		return "unhealthy", fmt.Sprintf("/health returned %d", resp.StatusCode), details
	}
	return "unknown", "unknown dependency kind", details
}

// droppedSourceTables returns the selected source tables this pipeline's CDC stream
// has been told are gone from the origin and not yet seen again — the open rows in
// cdc_source_table_drops, written by the cdcstats DDL consumer.
//
// Fail-soft in every direction, because this runs on a 15s sweep and its only job is
// to make a green badge honest:
//
//   - no db, or no pipeline id: nothing to say, keep the caller's verdict;
//   - query error (the commonest being an un-migrated deploy where the relation does
//     not exist yet): logged at Debug and treated as "no drops", so a missing
//     migration costs the new signal and never the existing health report;
//   - no rows: the normal case on a healthy deployment, served by the partial index
//     idx_cdc_source_table_drops_open.
//
// The names are schema metadata (table names only, no values), which is what keeps
// the resulting last_error inside the metadata-only rule when Diagnose forwards it
// to an LLM.
func (p *DependencyProbe) droppedSourceTables(ctx context.Context, pipelineID string) []string {
	if p.db == nil || strings.TrimSpace(pipelineID) == "" {
		return nil
	}
	rows, err := p.db.QueryContext(ctx, `
		SELECT table_name
		FROM cdc_source_table_drops
		WHERE pipeline_id = $1::uuid AND restored_at IS NULL
		ORDER BY table_name
	`, pipelineID)
	if err != nil {
		log.Debugf("dependency_probe: dropped source table lookup failed (ignored, pipeline=%s): %v", pipelineID, err)
		return nil
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			log.Debugf("dependency_probe: dropped source table scan failed (ignored): %v", err)
			return nil
		}
		if name = strings.TrimSpace(name); name != "" {
			names = append(names, name)
		}
	}
	if err := rows.Err(); err != nil {
		log.Debugf("dependency_probe: dropped source table iteration failed (ignored): %v", err)
		return nil
	}
	return names
}

// kafkaConnectBaseURL returns the Kafka Connect REST base URL (env override or
// the in-cluster default). Mirrors handlers.getKafkaConnectURL, duplicated here
// because that helper is unexported in a different package.
func kafkaConnectBaseURL() string {
	if u := strings.TrimSpace(os.Getenv("KAFKA_CONNECT_URL")); u != "" {
		return u
	}
	return "http://kafka-connect:8083"
}

func (p *DependencyProbe) writeHealth(ctx context.Context, depID, status, lastError string, details map[string]interface{}) {
	detailsJSON, _ := json.Marshal(details)
	var lastHealthyClause string
	if status == "healthy" {
		lastHealthyClause = "NOW()"
	} else {
		lastHealthyClause = "pipeline_dependency_health.last_healthy_at"
	}
	// Cast $2 to text explicitly — Postgres can't deduce its type when it's
	// referenced in multiple CASE branches with different return types
	// (timestamp vs integer), and reports `inconsistent types deduced for
	// parameter $2`. Casting once at the top resolves the ambiguity.
	q := fmt.Sprintf(`
		INSERT INTO pipeline_dependency_health
			(dependency_id, status, last_checked_at, last_healthy_at, last_error, consecutive_failures, details, updated_at)
		VALUES ($1, $2::text, NOW(),
		        CASE WHEN $2::text = 'healthy' THEN NOW() ELSE NULL END,
		        $3,
		        CASE WHEN $2::text = 'healthy' THEN 0 ELSE 1 END,
		        $4::jsonb, NOW())
		ON CONFLICT (dependency_id) DO UPDATE SET
			status               = EXCLUDED.status,
			last_checked_at      = NOW(),
			last_healthy_at      = %s,
			last_error           = EXCLUDED.last_error,
			consecutive_failures = CASE WHEN EXCLUDED.status = 'healthy' THEN 0 ELSE pipeline_dependency_health.consecutive_failures + 1 END,
			details              = EXCLUDED.details,
			updated_at           = NOW()
	`, lastHealthyClause)
	if _, err := p.db.ExecContext(ctx, q, depID, status, lastError, detailsJSON); err != nil {
		log.Warnf("dependency_probe: writeHealth failed (dep=%s status=%s): %v", depID, status, err)
	}
}

// splitIdentifier turns "postgresql@v1.0.14" into ("postgresql", "v1.0.14").
// Tolerates missing version (returns empty version, caller handles).
func splitIdentifier(id string) (string, string) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", ""
	}
	if i := strings.Index(id, "@"); i >= 0 {
		return strings.TrimSpace(id[:i]), strings.TrimSpace(id[i+1:])
	}
	return id, ""
}
